"""Follow "diagnose the upstream task" until it reaches the task that actually raised.

Naming the upstream task is where the old verdict stopped, and it left the reader holding the same
question one rung higher: that task's log is usually another stub, or another wrapper, and on-call
opens three tabs to reach the exception. The chain is walkable from the API, so the debugger walks
it and reports the real root plus the path it took.

Two hop kinds cover every case in the corpus. Inside one run, an `upstream_failed` task points at
the run's own failed tasks. Across DAGs, an ExternalTaskSensor points at a task in a different DAG
and run. Both end the same way: fetch that task's log, classify it, and stop as soon as something
carries a cause.

The walk is bounded and it announces where it stopped. An unbounded chase costs one API round trip
per rung and can loop on a self-referential sensor, and a chain that quietly gave up looks exactly
like a chain that ended.
"""

from __future__ import annotations

from .external_task_rca import _api, _resolve_base, _run_holding
from .parse import parse_log
from .signatures import classify

MAX_HOPS = 4


class Client:
    """The four API calls the walk needs, so the bundle can swap its own REST client in."""

    def __init__(self) -> None:
        self.api = _api()
        self.token = self.api.resolve_bearer()
        self.base = _resolve_base()

    def tis_in_run(self, dag_id: str, run_id: str) -> list:
        """Every task instance in one run."""
        return self.api.list_task_instances_in_run(self.base, self.token, dag_id, run_id) or []

    def failed_try(self, ti: dict) -> dict:
        """The try that actually failed. The newest can be a green retry carrying no cause."""
        try:
            tries = self.api.expand_tries(self.base, self.token, ti) or [ti]
        except Exception:
            return ti
        failed = [t for t in tries if t.get("state") == "failed"]
        return (failed or tries)[-1]

    def log_text(self, ti: dict) -> str:
        """One task instance's log, flattened."""
        return self.api.fetch_log(self.base, self.token, ti) or ""

    def find_run(
        self, dag_id: str, task_id: str, on_date: str | None, ti_state: str | None
    ) -> str | None:
        """The run this task ran in, when only its day is known."""
        return _run_holding(self.api, self.base, self.token, dag_id, task_id, on_date, ti_state)


def _diagnose_text(text: str) -> tuple[dict | None, str | None]:
    """(signature, error text) for one fetched log, without a second network hop."""
    parsed = parse_log(text)
    if parsed.airflow_signature:
        return parsed.airflow_signature, None
    m = classify(text)
    if m:
        return {
            "key": m.key,
            "sig_class": m.sig_class,
            "likely_cause": m.likely_cause,
            "programmatic_fix": m.programmatic_fix,
            "matched_on": m.matched_on,
            "remedy": m.remedy,
        }, None
    return None, text.strip()[-400:] or None


_CLIENT = Client  # the bundle swaps this for one built on its own REST client


def _next_in_run(
    client: Client, dag_id: str, run_id: str, task_id: str
) -> tuple[list[dict], str | None]:
    """The failed task instances in this run, excluding the one we came from."""
    try:
        tis = client.tis_in_run(dag_id, run_id)
    except Exception as e:
        return [], f"Airflow API unavailable ({e})"
    failed = [t for t in tis if t.get("state") == "failed" and t.get("task_id") != task_id]
    if not failed:
        return [], "no failed task in this run, so the cause is outside it"
    # Earliest start is the one that broke first; the rest are its consequences.
    return sorted(failed, key=lambda t: str(t.get("start_date") or "")), None


def _external_target(diag: dict) -> tuple[str, str, str] | None:
    """(dag_id, run_id, task_id) of an ExternalTaskSensor's target, when the diagnosis has one."""
    spark = diag.get("spark") or {}
    if spark.get("engine") != "external_task":
        return None
    tasks = spark.get("task_ids") or []
    if not (spark.get("dag_id") and spark.get("run_id") and tasks):
        return None
    return spark["dag_id"], spark["run_id"], tasks[0]


def walk(diag: dict, on_date: str | None = None, max_hops: int = MAX_HOPS) -> dict | None:
    """Follow the chain to the task that raised. None when this diagnosis is not a pointer.

    Returns the hops taken, the root it reached, and the reason it stopped. The reason is never
    omitted: "no root found" and "stopped at the hop limit" call for different next actions.
    """
    ident = diag.get("identity") or {}
    dag_id, task_id = ident.get("dag_id"), ident.get("task_id")
    run_id = ident.get("run_id")
    if not (dag_id and task_id):
        return None

    is_stub = bool(diag.get("no_error_text")) and diag.get("ti_state") == "upstream_failed"
    external = _external_target(diag)
    if not (is_stub or external):
        return None

    try:
        client = _CLIENT()
    except Exception as e:
        return {"hops": [], "root": None, "note": f"Airflow API unavailable ({e})"}

    if not run_id and not external:
        run_id = client.find_run(dag_id, task_id, on_date, diag.get("ti_state"))
        if not run_id:
            return {"hops": [], "root": None, "note": "could not identify the run this task ran in"}

    hops: list[dict] = []
    seen = {(dag_id, run_id, task_id)}
    cur_dag, cur_run, cur_task = dag_id, run_id, task_id
    # An external target names the exact next task; everything else scans the run it came from.
    target = external

    for _ in range(max_hops):
        if target:
            candidates = [{"dag_id": target[0], "dag_run_id": target[1], "task_id": target[2]}]
            note, target = None, None
        else:
            candidates, note = _next_in_run(client, cur_dag, cur_run, cur_task)
        if not candidates:
            return {"hops": hops, "root": None, "note": note or "nothing further upstream"}

        ti = candidates[0]
        also = [t["task_id"] for t in candidates[1:] if t.get("task_id")]
        key = (ti["dag_id"], ti.get("dag_run_id"), ti["task_id"])
        if key in seen:
            return {"hops": hops, "root": None, "note": f"the chain loops back to {ti['task_id']}"}
        seen.add(key)

        ti = client.failed_try(ti)
        try:
            text = client.log_text(ti)
        except Exception as e:
            return {
                "hops": hops,
                "root": None,
                "note": f"could not read {ti['task_id']}'s log ({e})",
            }

        sig, err = _diagnose_text(text)
        hops.append(
            {
                "dag_id": ti["dag_id"],
                "task_id": ti["task_id"],
                "state": ti.get("state"),
                "try_number": ti.get("try_number"),
                "signature": sig,
                "siblings": also,
            }
        )
        if sig or err:
            return {
                "hops": hops,
                "root": {
                    "dag_id": ti["dag_id"],
                    "task_id": ti["task_id"],
                    "signature": sig,
                    "error": err,
                },
                "note": None,
            }
        cur_dag, cur_run, cur_task = ti["dag_id"], ti.get("dag_run_id"), ti["task_id"]

    return {"hops": hops, "root": None, "note": f"stopped at the {max_hops}-hop limit"}


def chain_text(walked: dict) -> str | None:
    """One line naming the root and the path taken, or why the walk stopped."""
    if not walked:
        return None
    hops = walked.get("hops") or []
    root = walked.get("root")
    if not root:
        note = walked.get("note") or "the chain could not be followed"
        seen = " -> ".join(h["task_id"] for h in hops)
        return f"Upstream walk stopped: {note}." + (f" Reached {seen}." if seen else "")
    path = " -> ".join(f"{h['task_id']}" for h in hops)
    who = f"{root['dag_id']}.{root['task_id']}"
    sig = (root.get("signature") or {}).get("key")
    what = f"{who} ({sig})" if sig else who
    tail = f" via {path}." if len(hops) > 1 else "."
    siblings = hops[0].get("siblings") if hops else None
    extra = f" {len(siblings)} other task(s) failed in the same run." if siblings else ""
    return f"Root cause: {what}{tail}{extra}"
