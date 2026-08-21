"""Which active DAGs the sweep can actually profile, and which it is blind to.

The optimizer reads Spark event logs. A BigQuery operator, a sensor or a plain Python
callable emits nothing to read, so a backlog built only from the logs that happen to
exist looks complete while silently omitting most of the fleet.

This enumerates every unpaused DAG from the Airflow API, classifies each task by whether
it can produce an event log, and reports the gap by name.

Two auth paths, because the sweep runs in two places. On a laptop the bearer comes from
`.claude/scripts/airflow_api.py` (the astro CLI context, refreshed by SSO). In the
automations container that file does not exist and there is no CLI, so the token arrives
as AIRFLOW_TI_API_TOKEN from an ExternalSecret. The env var wins when both are present.
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

# Local-developer fallback only. Absent in any deployed copy, which uses the env token.
AIRFLOW_API = os.environ.get("AIRFLOW_API_HELPER", ".claude/scripts/airflow_api.py")
REPORT = os.path.join(os.environ.get("OPTIMIZER_OUTDIR", "optimizer_out"),
                      "optimizer_coverage_{date}.md")

# Operators that submit a Spark job and therefore leave an event log behind.
SPARK_OPERATORS = {
    "ModelPysparkBatchOperator",
    "DataprocCreateBatchOperator",
    "DataprocSubmitJobOperator",
    "DataprocInstantiateWorkflowTemplateOperator",
}
# Operators that run Spark somewhere we cannot read an event log from today.
OPAQUE_OPERATORS = {
    "DbxDbtOperator": "Databricks job cluster, no cluster_log_conf",
    "DatabricksSubmitRunOperator": "Databricks job cluster, no cluster_log_conf",
    "CustomVertexAIPipelineJobOperator": "Vertex pipeline, Spark runs in another project",
}


@dataclass
class DagCoverage:
    """One DAG's profileability."""

    dag_id: str
    owners: str = ""
    tags: list = field(default_factory=list)
    spark_tasks: list = field(default_factory=list)
    opaque_tasks: list = field(default_factory=list)  # (task_id, why)
    other_tasks: list = field(default_factory=list)

    @property
    def profilable(self) -> bool:
        """True when at least one task submits a Spark job we can read a log from."""
        return bool(self.spark_tasks)


@dataclass
class Coverage:
    """The fleet split into what the sweep can read and what it cannot."""

    date: str
    dags: list = field(default_factory=list)
    error: str = ""
    report_path: str = ""

    @property
    def profilable(self) -> list:
        """Active DAGs with at least one readable Spark task."""
        return [d for d in self.dags if d.profilable]

    @property
    def unprofiled(self) -> list:
        """Active DAGs the optimizer is structurally blind to."""
        return [d for d in self.dags if not d.profilable]

    def unprofiled_line(self) -> str:
        """One clause for the digest headline."""
        if self.error:
            return f"DAG coverage unknown ({self.error})."
        n = len(self.unprofiled)
        return f"{n} active DAG{'s' if n != 1 else ''} had no Spark task to profile."


def _airflow(base: str, token: str, path: str, params: dict) -> dict:
    """One authenticated GET against the Airflow REST API."""
    url = f"{base.rstrip('/')}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}",
                                               "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} on {path}") from e


def _bearer() -> str:
    """The deployment token if one was injected, else the laptop's astro CLI context."""
    env = os.environ.get("AIRFLOW_TI_API_TOKEN", "").strip()
    if env:
        return env
    if not os.path.exists(AIRFLOW_API):
        raise RuntimeError("no AIRFLOW_TI_API_TOKEN and no airflow_api.py to resolve one")
    helper_dir = os.path.dirname(AIRFLOW_API) or "."
    r = subprocess.run(
        ["python3", "-c",
         f"import sys;sys.path.insert(0,{helper_dir!r});"
         "import airflow_api as a;print(a.resolve_bearer())"],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode != 0:
        raise RuntimeError((r.stderr or "no astro bearer").strip()[:200])
    return r.stdout.strip()


def _active_dags(base: str, token: str) -> list[dict]:
    out, offset = [], 0
    while True:
        body = _airflow(base, token, "/dags",
                        {"paused": "false", "limit": 100, "offset": offset})
        page = body.get("dags", [])
        out += page
        offset += len(page)
        if not page or offset >= body.get("total_entries", len(out)):
            break
    return out


def collect(base: str, date: str, token: str | None = None) -> Coverage:
    """Enumerate active DAGs and classify every task. Never raises - errors land on the report."""
    cov = Coverage(date=date, report_path=REPORT.format(date=date))
    try:
        token = token or _bearer()
        dags = _active_dags(base, token)
    except Exception as e:  # a coverage failure must not sink the sweep
        cov.error = str(e)[:160]
        return cov

    for d in dags:
        dag_id = d.get("dag_id", "")
        dc = DagCoverage(dag_id=dag_id, owners=", ".join(d.get("owners") or []),
                         tags=[t.get("name") for t in (d.get("tags") or [])])
        try:
            tasks = _airflow(base, token, f"/dags/{dag_id}/tasks", {}).get("tasks", [])
        except Exception as e:
            dc.other_tasks.append(("<tasks unreadable>", str(e)[:80]))
            cov.dags.append(dc)
            continue
        for t in tasks:
            op = t.get("operator_name") or t.get("class_ref", {}).get("class_name", "")
            tid = t.get("task_id", "")
            if op in SPARK_OPERATORS:
                dc.spark_tasks.append(tid)
            elif op in OPAQUE_OPERATORS:
                dc.opaque_tasks.append((tid, OPAQUE_OPERATORS[op]))
            else:
                dc.other_tasks.append((tid, op or "unknown operator"))
        cov.dags.append(dc)
    return cov


def _load_bag_and_paused(dag_folder: str | None) -> tuple[dict, set]:
    """Parse the DAG bundle and read which DAGs are paused. Airflow-only; seam for tests.

    The paused query runs FIRST and deliberately. Parsing the bundle executes the module-level
    code of every DAG in the deployment, including their `Variable.get()` calls, which is
    hundreds of MB and a burst of API-server RPCs from inside one worker slot. Doing that and
    then discovering the DB is unreachable pays the whole cost for nothing, which is what the
    original order did. If the session fails, this raises before the parse.

    A worker in a deployment that denies direct metadata-DB access will raise here every run;
    that is intended. `collect_local` turns it into a stated gap on the report rather than a
    silent one, and `sweep` then declines to write ledger rows it cannot key correctly.
    """
    from airflow.models import DagModel
    from airflow.models.dagbag import DagBag
    from airflow.utils.session import create_session

    with create_session() as session:
        paused = {row[0] for row in session.query(DagModel.dag_id).filter(
            DagModel.is_paused.is_(True)).all()}
    bag = DagBag(dag_folder=dag_folder, include_examples=False)
    return bag.dags, paused


def collect_local(date: str, dag_folder: str | None = None) -> Coverage:
    """Same Coverage, built by parsing the DAG bundle instead of calling the REST API.

    When the sweep runs as an Airflow task it is already inside the deployment, so the DAG
    files are on disk and no deployment token is needed at all. This is the preferred path:
    a token is a credential to store, rotate and leak, and this needs none.

    Paused DAGs are excluded to match the API path, which filters `paused=false`.
    """
    cov = Coverage(date=date, report_path=REPORT.format(date=date))
    try:
        dags, paused = _load_bag_and_paused(dag_folder)
    except Exception as e:
        cov.error = str(e)[:160]
        return cov

    for dag_id, dag in sorted(dags.items()):
        if dag_id in paused:
            continue
        owner = getattr(dag, "owner", "") or ""
        dc = DagCoverage(dag_id=dag_id, owners=owner, tags=sorted(getattr(dag, "tags", []) or []))
        for t in dag.tasks:
            op, tid = type(t).__name__, t.task_id
            if op in SPARK_OPERATORS:
                dc.spark_tasks.append(tid)
            elif op in OPAQUE_OPERATORS:
                dc.opaque_tasks.append((tid, OPAQUE_OPERATORS[op]))
            else:
                dc.other_tasks.append((tid, op))
        cov.dags.append(dc)
    return cov


def render(cov: Coverage, profiled_dags: set | None = None) -> str:
    """The coverage report: what was read, what was not, and why not."""
    profiled = profiled_dags or set()
    lines = [f"# Optimizer coverage — {cov.date}", ""]
    if cov.error:
        lines += [f"Could not enumerate DAGs: {cov.error}", "",
                  "The backlog for this sweep covers only the event logs that were present.",
                  "Treat its completeness as unknown.", ""]
        return "\n".join(lines)

    spark = cov.profilable
    seen = [d for d in spark if d.dag_id in profiled]
    missed = [d for d in spark if d.dag_id not in profiled]
    lines += [
        f"{len(cov.dags)} active DAGs. {len(spark)} have a Spark task; "
        f"{len(cov.unprofiled)} do not and are invisible to this tool.", "",
        f"- profiled this sweep: {len(seen)}",
        f"- Spark DAGs with no log this sweep: {len(missed)}",
        f"- no Spark task at all: {len(cov.unprofiled)}", "",
    ]

    if missed:
        lines += ["## Spark DAGs that produced no log", "",
                  "Either they did not run in the window, or the log has not landed.", ""]
        lines += [f"- `{d.dag_id}` ({', '.join(d.spark_tasks[:4])})" for d in sorted(
            missed, key=lambda d: d.dag_id)]
        lines.append("")

    opaque = [(d, t, why) for d in cov.dags for (t, why) in d.opaque_tasks]
    if opaque:
        lines += ["## Spark we cannot read", "",
                  "These run Spark on an engine whose plan or metrics are not reachable yet.", ""]
        lines += [f"- `{d.dag_id}` / `{t}` — {why}" for (d, t, why) in sorted(
            opaque, key=lambda x: x[0].dag_id)]
        lines.append("")

    if cov.unprofiled:
        lines += ["## No Spark task", "",
                  "Nothing to profile. Listed so the backlog is not mistaken for the fleet.", ""]
        for d in sorted(cov.unprofiled, key=lambda d: d.dag_id):
            ops = sorted({op for (_, op) in d.other_tasks})[:3]
            lines.append(f"- `{d.dag_id}` — {', '.join(ops) or 'no tasks'}")
        lines.append("")
    return "\n".join(lines)


def write(cov: Coverage, profiled_dags: set | None = None, root: str = ".") -> str:
    """Write the coverage report next to the backlog. Returns its path."""
    path = os.path.join(root, cov.report_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(render(cov, profiled_dags))
    return path


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("usage: python -m airflow_optimizer.coverage <api_base_url> <YYYY-MM-DD>")
        raise SystemExit(2)
    c = collect(sys.argv[1], sys.argv[2])
    print(render(c))
