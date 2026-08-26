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

MAX_HOPS = 4
_ROOT_LOG_KEEP = 200_000  # the root's own log, so a resolver never reads the wrong task's


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

    def task_graph(self, dag_id: str) -> dict:
        """task_id -> downstream task ids, for proving an edge rather than assuming one."""
        return self.api.dag_task_graph(self.base, self.token, dag_id) or {}

    def task_timeout(self, dag_id: str, task_id: str) -> float | None:
        """The task's declared execution_timeout in seconds, or None if it has none."""
        meta = (self.api.dag_task_meta(self.base, self.token, dag_id) or {}).get(task_id) or {}
        et = meta.get("execution_timeout") or {}
        if not isinstance(et, dict):
            return None
        secs = (et.get("days") or 0) * 86400 + (et.get("seconds") or 0)
        return float(secs) or None

    def task_history(self, dag_id: str, task_id: str, limit: int = 100) -> list:
        """Recent instances of one task, newest first, for a runtime trend."""
        rows = self.api.list_task_instances_for_task(self.base, self.token, dag_id, task_id, limit)
        return rows or []

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
    return None, text.strip()[-400:] or None


_CLIENT = Client  # the bundle swaps this for one built on its own REST client


def _ancestors(graph: dict, task_id: str) -> set:
    """Every task that can reach `task_id`, from the DAG's own edges."""
    upstream: dict = {}
    for src, dests in graph.items():
        for d in dests:
            upstream.setdefault(d, set()).add(src)
    seen, queue = set(), list(upstream.get(task_id, ()))
    while queue:
        cur = queue.pop()
        if cur in seen:
            continue
        seen.add(cur)
        queue += list(upstream.get(cur, ()))
    return seen


def _next_in_run(
    client: Client, dag_id: str, run_id: str, task_id: str
) -> tuple[list[dict], str | None]:
    """The failed tasks in this run that are genuinely upstream of the one we came from.

    Start time does not prove a dependency. A DAG with parallel branches fails two unrelated
    tasks in one run, and the earlier one is not upstream of anything: naming it root cause sends
    on-call at a fault that is not theirs, under a label that claims the answer was read off the
    API. So the edge is read off the API too, and without it there is no root-cause claim.
    """
    try:
        tis = client.tis_in_run(dag_id, run_id)
    except Exception as e:
        return [], f"Airflow API unavailable ({e})"
    failed = [t for t in tis if t.get("state") == "failed" and t.get("task_id") != task_id]
    if not failed:
        return [], "no failed task in this run, so the cause is outside it"
    try:
        graph = client.task_graph(dag_id)
    except Exception:
        graph = {}
    if not graph:
        return (
            [],
            f"the DAG structure is unavailable, so none of {len(failed)} failed task(s) can be proved upstream",
        )
    ancestors = _ancestors(graph, task_id)
    upstream_failed = [t for t in failed if t.get("task_id") in ancestors]
    if not upstream_failed:
        names = ", ".join(sorted(t["task_id"] for t in failed)[:3])
        return [], f"no FAILED task is upstream of this one; the run also failed {names}"
    return sorted(upstream_failed, key=lambda t: str(t.get("start_date") or "")), None


_TARGET_WORTH_WALKING = ("failed", "upstream_failed")


def _external_target(diag: dict) -> tuple[str, str, str] | None:
    """The sensor target worth walking to, or None when the target did not fail.

    A sensor can trip on a target that SUCCEEDED or was SKIPPED by design, and those verdicts are
    about the sensor, not the target. Walking anyway reads a green log, calls it the root cause,
    and prints "Fix <task>" directly under the signature's own "do not backfill" - two sentences
    that contradict each other, one of them false. With several poked tasks the one to follow is
    the one holding the failure, not whichever came first in the list.
    """
    spark = diag.get("spark") or {}
    if spark.get("engine") != "external_task":
        return None
    if (spark.get("state") or "").lower() not in _TARGET_WORTH_WALKING:
        return None
    states = spark.get("states") or {}
    failed = [t for t, st in states.items() if st in _TARGET_WORTH_WALKING]
    task = failed[0] if failed else (spark.get("task_ids") or [None])[0]
    if not (spark.get("dag_id") and spark.get("run_id") and task):
        return None
    return spark["dag_id"], spark["run_id"], task


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
                    "run_id": ti.get("dag_run_id"),
                    "signature": sig,
                    "error": err,
                    "log": text[-_ROOT_LOG_KEEP:],
                },
                "note": None,
            }
        cur_dag, cur_run, cur_task = ti["dag_id"], ti.get("dag_run_id"), ti["task_id"]

    return {"hops": hops, "root": None, "note": f"stopped at the {max_hops}-hop limit"}
