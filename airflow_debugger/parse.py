"""Airflow failure parser + operator->engine router.

Input is a failed-task Airflow log (the file `airflow_pull.sh --watch` drops into
`on-call/`). Extracts identity (dag / task / run / try), routes by operator to
the Spark engine, and pulls the downstream job id out of the log:
- Dataproc: `Batch job <batch_id>`
- Databricks: the dbt-databricks adapter's `Job submission response={"run_id":<N>}`

`route()` dispatches to the matching analyzer and returns its evidence bundle.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

from .signatures import classify

# Substrings in op_classpath (or the log body) that pin the Spark engine.
_DATABRICKS_OPS = ("DbxDbtOperator", "DatabricksSubmitRun", "ModelPysparkDbxJob", ".dbx.")
_DATAPROC_OPS = (
    "DataprocCreateBatch",
    "ModelPysparkBatch",
    "ModelPysparkWorkflow",
    "TiPysparkBatch",
    "DataprocInstantiate",
)


@dataclass
class ParsedFailure:
    """Identity + engine + downstream job id extracted from an Airflow log."""

    dag_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None
    try_number: int | None = None
    operator: str | None = None
    engine: str = "unknown"  # dataproc | databricks | other | unknown
    batch_id: str | None = None  # dataproc
    dbx_run_id: int | None = None  # databricks
    airflow_signature: dict | None = None  # signature of the Airflow-task-level failure
    notes: list = field(default_factory=list)


def _first(pattern: str, text: str, flags: int = 0) -> str | None:
    m = re.search(pattern, text, flags)
    return m.group(1) if m else None


def parse_log(text: str) -> ParsedFailure:
    """Parse a failed-task Airflow log into identity + engine + job id."""
    p = ParsedFailure()
    p.dag_id = _first(r"dag_id=([A-Za-z0-9_.-]+)", text)
    p.task_id = _first(r"task_id=([A-Za-z0-9_.-]+)", text)
    p.run_id = _first(r"dagrun_id=(\S+)", text) or _first(
        r"run_id['\"]?[=:]\s*['\"]?(scheduled__[^'\"\s]+)", text
    )
    tn = _first(r"try_number=(\d+)", text)
    p.try_number = int(tn) if tn else None

    opc = _first(r"op_classpath=(\[[^\]]*\])", text)
    p.operator = opc
    hay = opc or text
    if any(o in hay for o in _DATABRICKS_OPS):
        p.engine = "databricks"
    elif any(o in hay for o in _DATAPROC_OPS):
        p.engine = "dataproc"
    elif opc and "Operator" in opc:
        p.engine = "other"  # sensor / python / pod — not a Spark job

    if p.engine == "dataproc":
        p.batch_id = _first(r"Batch job (\S+)", text)
        if not p.batch_id:
            p.notes.append("dataproc engine but no 'Batch job <id>' line found")
    elif p.engine == "databricks":
        rid = (
            _first(r'Job submission response=[^\n]*?"run_id":\s*(\d+)', text)
            or _first(r'"run_id":\s*(\d{10,})', text)
            or _first(r"/runs/(\d{10,})", text)
        )
        p.dbx_run_id = int(rid) if rid else None
        if not p.dbx_run_id:
            p.notes.append(
                "databricks engine but no numeric run_id found (needs XCom/lakeflow lookup)"
            )

    asig = classify(text)
    p.airflow_signature = asdict(asig) if asig else None
    return p


def _spark_succeeded(spark: dict | None) -> bool:
    """True if the downstream Spark job itself finished OK (Airflow failure is orchestration-only)."""
    if not spark:
        return False
    if spark.get("engine") == "databricks":
        return (spark.get("state") or {}).get("result_state") == "SUCCESS" and not spark.get(
            "failed_tasks"
        )
    if spark.get("engine") == "dataproc":
        return spark.get("state") == "SUCCEEDED"
    return False


def diagnose(parsed: ParsedFailure) -> dict:
    """Synthesize the root cause across the Airflow-task and Spark-engine layers.

    If the Spark job itself failed with a signature, that is the root cause. If the
    Spark job SUCCEEDED but Airflow still failed, the failure is orchestration-only
    (e.g. a pod eviction) and the Airflow-log signature is the root cause.
    """
    spark = None
    if parsed.engine in ("dataproc", "databricks") and (parsed.batch_id or parsed.dbx_run_id):
        spark = route(parsed)
    spark_sig = (spark or {}).get("signature")
    spark_ok = _spark_succeeded(spark)
    orchestration_only = bool(spark_ok and parsed.airflow_signature)

    if spark_sig and not spark_ok:
        root = spark_sig
    elif orchestration_only:
        root = parsed.airflow_signature
    else:
        root = spark_sig or parsed.airflow_signature

    return {
        "identity": {
            "dag_id": parsed.dag_id,
            "task_id": parsed.task_id,
            "run_id": parsed.run_id,
            "try_number": parsed.try_number,
        },
        "engine": parsed.engine,
        "airflow_signature": parsed.airflow_signature,
        "spark": spark,
        "spark_outcome": "succeeded" if spark_ok else ("failed" if spark else "none"),
        "orchestration_only": orchestration_only,
        "root_signature": root,
        "root_error": (spark or {}).get("root_error"),
        "batch_id": parsed.batch_id,
        "dbx_run_id": parsed.dbx_run_id,
        "job_id": (spark or {}).get("job_id"),
    }


def parse_log_file(path: str) -> ParsedFailure:
    """Parse a log file on disk."""
    with open(path, encoding="utf-8", errors="replace") as f:
        return parse_log(f.read())


def route(parsed: ParsedFailure) -> dict:
    """Dispatch to the matching analyzer; return its evidence bundle (dict)."""
    from .databricks_rca import analyze_run
    from .dataproc_rca import analyze_batch

    if parsed.engine == "dataproc" and parsed.batch_id:
        return asdict(analyze_batch(parsed.batch_id))
    if parsed.engine == "databricks" and parsed.dbx_run_id:
        return asdict(analyze_run(parsed.dbx_run_id))
    return {
        "engine": parsed.engine,
        "note": "no downstream Spark job id; Airflow-only failure (sensor/python/pod or unresolved)",
        "parsed": asdict(parsed),
    }


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        print("usage: python -m airflow_debugger.parse <airflow_log_file> [--route]")
        raise SystemExit(2)
    parsed = parse_log_file(sys.argv[1])
    print(json.dumps(asdict(parsed), indent=2, default=str))
    if "--route" in sys.argv:
        print("--- routed analysis ---")
        print(json.dumps(route(parsed), indent=2, default=str))
