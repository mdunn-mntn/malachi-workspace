"""Which active DAGs the sweep can actually profile, and which it is blind to.

The optimizer reads Spark event logs. A BigQuery operator, a sensor or a plain Python
callable emits nothing to read, so a backlog built only from the logs that happen to
exist looks complete while silently omitting most of the fleet.

This enumerates every DAG the deployment knows, classifies each unpaused one's tasks by
whether they can produce an event log, and reports the gap by name.

Two ways in. Running as an Airflow task, `collect_local` parses the DAG bundle already on
disk and needs no credential at all; that is the path the DAG uses. Running outside Airflow,
`collect` calls the REST API with a bearer from AIRFLOW_TI_API_TOKEN, or from the local astro
CLI helper when one is present.
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

# Local-developer fallback only. Absent in any deployed copy, which uses the env token.
AIRFLOW_API = os.environ.get("AIRFLOW_API_HELPER", ".claude/scripts/airflow_api.py")
REPORT = os.path.join(os.environ.get("OPTIMIZER_OUTDIR", "optimizer_out"),
                      "optimizer_coverage_{date}.md")

# Operators that submit a Spark job and therefore leave an event log behind.
SPARK_OPERATORS = {
    "ModelPysparkBatchOperator",
    "TiPysparkBatchOperator",
    "RetrySafeDataprocCreateBatchOperator",
    "DataprocCreateBatchOperator",
    "DataprocSubmitJobOperator",
    "DataprocInstantiateWorkflowTemplateOperator",
}
# Operators that run Spark somewhere we cannot read an event log from today.
OPAQUE_OPERATORS = {
    "DbxDbtOperator": "Databricks job cluster, no cluster_log_conf",
    "DatabricksSubmitRunOperator": "Databricks job cluster, no cluster_log_conf",
    "CustomVertexAIPipelineJobOperator": "Vertex pipeline, Spark runs in another project",
    "ModelPysparkWorkflowOperator": "managed cluster, no spark.eventLog.dir and not a batch",
    "DataprocInstantiateInlineWorkflowTemplateOperator":
        "managed cluster, no spark.eventLog.dir and not a batch",
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
    dag_ids_including_paused: set = field(default_factory=set)
    error: str = ""
    warning: str = ""
    unparsed_files: list = field(default_factory=list)
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
        line = f"{n} DAG{'s' if n != 1 else ''} had no Spark task to profile."
        if self.warning:
            line += " Paused DAGs are counted as active this run."
        bad = len(self.unparsed_files)
        if bad:
            line += (f" {bad} DAG file{'s' if bad != 1 else ''} failed to import, so this "
                     "count is short.")
        return line


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


def _all_dags(base: str, token: str) -> list[dict]:
    out, offset = [], 0
    while True:
        body = _airflow(base, token, "/dags", {"limit": 100, "offset": offset})
        page = body.get("dags", [])
        out += page
        offset += len(page)
        if not page or offset >= body.get("total_entries", len(out)):
            break
    return out


def collect(base: str, date: str, token: str | None = None) -> Coverage:
    """Enumerate every DAG, classify the unpaused ones. Never raises - errors land on the report."""
    cov = Coverage(date=date, report_path=REPORT.format(date=date))
    try:
        token = token or _bearer()
        dags = _all_dags(base, token)
    except Exception as e:  # a coverage failure must not sink the sweep
        cov.error = _first_line(e)
        return cov
    if not dags:
        cov.error = f"{base} returned no DAGs; the base URL or the token's scope is wrong"
        return cov

    for d in dags:
        dag_id = d.get("dag_id", "")
        cov.dag_ids_including_paused.add(dag_id)
        if d.get("is_paused"):
            continue
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


def _first_line(e: Exception) -> str:
    """Airflow 3 raises multi-paragraph diagnostics; a headline needs one line."""
    return str(e).strip().partition("\n")[0].rstrip(" :")[:160] or type(e).__name__


def _paused_dag_ids() -> set:
    """DAG ids currently paused. Airflow-only; seam for tests."""
    from airflow.models import DagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        return {row[0] for row in session.query(DagModel.dag_id).filter(
            DagModel.is_paused.is_(True)).all()}


def _bundle_dag_folder() -> str | None:
    """The folder this task's own DAG was parsed from, which on Astro is a bundle under /tmp."""
    try:
        from airflow.sdk import get_current_context

        fileloc = getattr(get_current_context().get("dag"), "fileloc", "")
    except Exception:
        return None
    if not fileloc:
        return None
    path = Path(fileloc).resolve()
    for parent in path.parents:
        if parent.name == "dags":
            return str(parent)
    return str(path.parent)


def _load_bag(dag_folder: str | None) -> tuple[dict, dict]:
    """Parse the DAG bundle off disk, returning the DAGs and the files that failed to import."""
    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=dag_folder, include_examples=False)
    return bag.dags, bag.import_errors


def _known_operator(task: object) -> str:
    """The nearest classified ancestor, so an in-repo subclass is not read as a non-Spark task."""
    for cls in type(task).__mro__:
        if cls.__name__ in SPARK_OPERATORS or cls.__name__ in OPAQUE_OPERATORS:
            return cls.__name__
    return type(task).__name__


def collect_local(date: str, dag_folder: str | None = None) -> Coverage:
    """Same Coverage from the bundle; a subclass resolves to the base the REST path cannot see."""
    cov = Coverage(date=date, report_path=REPORT.format(date=date))
    try:
        paused = _paused_dag_ids()
    except Exception as e:
        paused = set()
        cov.warning = _first_line(e)
    try:
        dags, import_errors = _load_bag(dag_folder or _bundle_dag_folder())
    except Exception as e:
        cov.error = f"DAG bundle unreadable: {_first_line(e)}"
        return cov
    if not dags and not import_errors:
        cov.error = "DAG bundle held no DAGs; the folder it was parsed from is not the deployed one"
        return cov
    cov.unparsed_files = sorted(os.path.basename(f) for f in import_errors)
    cov.dag_ids_including_paused = set(dags)

    for dag_id, dag in sorted(dags.items()):
        if dag_id in paused:
            continue
        owner = getattr(dag, "owner", "") or ""
        dc = DagCoverage(dag_id=dag_id, owners=owner, tags=sorted(getattr(dag, "tags", []) or []))
        for t in dag.tasks:
            op, tid = _known_operator(t), t.task_id
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
    if cov.warning:
        lines += [f"Paused DAGs could not be excluded ({cov.warning}), so every DAG in the "
                  "bundle is counted as active below.", ""]
    if cov.unparsed_files:
        lines += ["These DAG files failed to import, so the DAGs they define are missing from "
                  "every count below:", ""]
        lines += [f"- `{f}`" for f in cov.unparsed_files]
        lines.append("")

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
            empty = "Spark we cannot read" if d.opaque_tasks else "no tasks"
            lines.append(f"- `{d.dag_id}` — {', '.join(ops) or empty}")
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
