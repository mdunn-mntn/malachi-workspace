"""Net-new Databricks job-run failure analyzer (key-free).

Drives the Databricks CLI (U2M OAuth profile) via subprocess — no stored
tokens. Given a Databricks job run_id, returns a small deterministic evidence
bundle: run state, per-failed-task root-cause error (+ trace tail), cluster
termination reason when available, and a signature match. No LLM.

The parent run_id comes from the Airflow DatabricksSubmitRunOperator XCom (or is
discovered from the task log). Task-level output MUST be read with the TASK
run_id, not the parent job run_id (else INVALID_PARAMETER_VALUE).
"""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import asdict, dataclass, field

from .signatures import classify

PROFILE = "malachi@mountain.com"  # U2M OAuth; the DEFAULT profile is invalid
_TRACE_TAIL = 2000  # chars of error_trace kept (tail-first)
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _dbx(*args: str, timeout: int = 90) -> dict | list | None:
    """Run a `databricks ... -o json` command; return parsed JSON or an error dict."""
    cmd = ["databricks", *args, "-p", PROFILE, "-o", "json"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        return {"_cli_error": str(e)}
    if out.returncode != 0:
        return {"_cli_error": (out.stderr or out.stdout).strip()[:500]}
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        return {"_cli_error": "non-json output", "_raw": out.stdout[:500]}


def _strip_ansi(s: str | None) -> str:
    return _ANSI.sub("", s or "")


@dataclass
class TaskFailure:
    """One failed task's extracted evidence."""

    task_key: str
    run_id: int
    error: str | None = None
    error_trace_tail: str | None = None
    cluster_id: str | None = None
    termination: dict | None = None
    signature: dict | None = None


@dataclass
class DatabricksEvidence:
    """Deterministic evidence bundle for one Databricks job run."""

    engine: str = "databricks"
    run_id: int | None = None
    run_name: str | None = None
    state: dict = field(default_factory=dict)
    failed_tasks: list = field(default_factory=list)
    root_error: str | None = None
    signature: dict | None = None
    notes: list = field(default_factory=list)


def analyze_run(run_id: int) -> DatabricksEvidence:
    """Deterministic RCA for one Databricks job run. Never raises for a CLI error."""
    ev = DatabricksEvidence(run_id=run_id)
    run = _dbx("jobs", "get-run", str(run_id))
    if not isinstance(run, dict) or run.get("_cli_error"):
        ev.notes.append(
            f"get-run failed: {run.get('_cli_error') if isinstance(run, dict) else run}"
        )
        return ev
    ev.run_name = run.get("run_name")
    ev.state = run.get("state", {})
    tasks = run.get("tasks") or [run]  # single-task runs carry state at the top level

    for t in tasks:
        result = (t.get("state") or {}).get("result_state")
        if result and result != "FAILED":
            continue
        trun = t.get("run_id", run_id)
        tf = TaskFailure(task_key=t.get("task_key", "(single)"), run_id=trun)
        tf.cluster_id = (t.get("cluster_instance") or {}).get("cluster_id")

        out = _dbx("jobs", "get-run-output", str(trun))
        if isinstance(out, dict) and not out.get("_cli_error"):
            tf.error = out.get("error")
            trace = _strip_ansi(out.get("error_trace"))
            tf.error_trace_tail = trace[-_TRACE_TAIL:] or None
        elif isinstance(out, dict):
            ev.notes.append(f"get-run-output({trun}) failed: {out['_cli_error']}")

        if tf.cluster_id:
            cl = _dbx("clusters", "get", tf.cluster_id)
            if isinstance(cl, dict) and not cl.get("_cli_error"):
                tf.termination = cl.get("termination_reason")

        blob = " ".join(
            filter(None, [tf.error, tf.error_trace_tail, json.dumps(tf.termination or {})])
        )
        m = classify(blob, engine="databricks")
        if m:
            tf.signature = asdict(m)
        ev.failed_tasks.append(asdict(tf))

    if ev.failed_tasks:
        first = ev.failed_tasks[0]
        ev.root_error = first.get("error") or ev.state.get("state_message")
        ev.signature = first.get("signature")
    else:
        ev.root_error = ev.state.get("state_message")
        m = classify(ev.root_error or "", engine="databricks")
        ev.signature = asdict(m) if m else None
    return ev


if __name__ == "__main__":
    import sys

    rid = int(sys.argv[1]) if len(sys.argv) > 1 else 459011294807453  # INC-009
    print(json.dumps(asdict(analyze_run(rid)), indent=2, default=str))
