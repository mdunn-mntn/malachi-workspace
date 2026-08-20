"""ExternalTaskSensor failure resolver (key-free).

An `ExternalTaskSensor` that trips its `failed_states` raises with no message, so the
Airflow log ends at `Poking for tasks ['x'] in dag y on <date> ...` and then dies. The
poke line is present on every SUCCESSFUL poke too, so a signature over it would fire on
green runs. The answer is not in this log at all: it is the target task's real state,
which only the Airflow API knows.

Three outcomes read completely differently to an on-call, and only the API separates them:
    failed    -> a real upstream failure; diagnose the target, not this sensor
    skipped   -> the target was skipped by design; the sensor is mis-configured, not broken
    running   -> the sensor timed out waiting; a duration problem, not a fault

Auth reuses the on-call puller's bearer resolution (`.claude/scripts/airflow_api.py`,
`--token` / `$AIRFLOW_BEARER` / the active `astro` CLI context). No secret is stored here.
"""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess
from dataclasses import asdict, dataclass, field

from .signatures import Match

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_API_PATH = os.path.join(_ROOT, ".claude", "scripts", "airflow_api.py")
_CONFIG = os.path.join(_ROOT, ".claude", "scripts", "config.env")

# What the target's state means for the SENSOR's failure. Only 'failed' points elsewhere.
_VERDICT = {
    "failed": "real upstream failure: diagnose the target task, not this sensor",
    "upstream_failed": "real upstream failure: the target never ran either",
    "skipped": "the target was SKIPPED by design; the sensor should allow it "
    "(skipped_states / soft_fail), not page",
    "running": "the target was still running: the sensor ran out of time, not a fault",
    "queued": "the target had not started: the sensor ran out of time, not a fault",
    "success": "the target SUCCEEDED: the sensor failed on its own (window or logical-date "
    "mismatch), not on the target",
}

# The verdict comes from the API's answer, not from matching log text, so it earns a real
# signature rather than leaving the report at "unclassified".
_SIG_CLASS = {
    "failed": ("external_task_target_failed", "upstream/external-task-failed", "no"),
    "upstream_failed": ("external_task_target_failed", "upstream/external-task-failed", "no"),
    "skipped": ("external_task_target_skipped", "sensor/target-skipped-by-design", "yes"),
    "running": ("external_task_target_unfinished", "sensor/target-still-running", "sometimes"),
    "queued": ("external_task_target_unfinished", "sensor/target-not-started", "sometimes"),
    "success": ("external_task_window_mismatch", "sensor/window-mismatch", "yes"),
}
_LATE_SIG = (
    "external_task_target_unfinished",
    "sensor/target-unfinished-at-poke",
    "no",
)

# The API returns the target's state NOW, which for a resolved incident is usually success.
# Same defect that hid nine failures from the daily pull (IMP-053): compare against the
# moment the sensor gave up, not against the moment we asked.
_STALE_VERDICT = (
    "state {state} is from AFTER the sensor gave up (target ended {end}, sensor failed "
    "{failed}). At poke time the target had not succeeded, so this was a real wait, not a "
    "sensor bug. Diagnose the target's own failure."
)


@dataclass
class ExternalTaskEvidence:
    """Deterministic evidence bundle for one ExternalTaskSensor target."""

    engine: str = "external_task"
    dag_id: str | None = None
    task_ids: list = field(default_factory=list)
    logical_date: str | None = None
    run_id: str | None = None
    state: str | None = None  # the target's state AT QUERY TIME, not at poke time
    states: dict = field(default_factory=dict)  # task_id -> state, when several were poked
    state_is_later: bool = False  # True = the target moved on after the sensor gave up
    target_end_date: str | None = None
    sensor_failed_at: str | None = None
    error_text: str | None = None
    signature: dict | None = None
    notes: list = field(default_factory=list)


def _api() -> object:
    """Import the on-call puller as a module; it is a script, not a package."""
    spec = importlib.util.spec_from_file_location("airflow_api", _API_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {_API_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _config_value(key: str) -> str:
    """Read one exported default out of config.env without sourcing the shell."""
    try:
        with open(_CONFIG, encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return ""
    m = re.search(rf'{key}:-([^}}"]*)', text)
    return (m.group(1) if m else "").strip()


def _resolve_base() -> str:
    """The Airflow API base, same order airflow_pull.sh uses: env, then astro inspect."""
    base = os.environ.get("AIRFLOW_TI_API_URL") or ""
    if not base:
        dep = os.environ.get("AIRFLOW_TI_DEPLOYMENT_ID") or _config_value(
            "AIRFLOW_TI_DEPLOYMENT_ID"
        )
        out = subprocess.run(
            ["astro", "deployment", "inspect", dep, "--key", "metadata.airflow_api_url"],
            capture_output=True, text=True, timeout=60,
        )  # fmt: skip
        base = (out.stdout or "").strip().strip('"')
    if not base or base == "null":
        raise RuntimeError("could not resolve the Airflow API base URL")
    if not base.startswith("http"):
        base = f"https://{base}"
    return base if base.endswith("/api/v2") else f"{base.rstrip('/')}/api/v2"


def _target_run(api: object, base: str, token: str, dag_id: str, logical_date: str) -> dict | None:
    """The target DAG's run for a logical date. The API filters on it directly."""
    status, obj = api._get_json(  # noqa: SLF001
        base,
        token,
        f"/dags/{dag_id}/dagRuns",
        {"logical_date_gte": logical_date, "logical_date_lte": logical_date, "limit": 5},
    )
    if status != 200:
        return None
    for r in (obj or {}).get("dag_runs", []) or []:
        return r
    return None


def analyze_external_task(
    dag_id: str, task_ids: list, logical_date: str | None, failed_at: str | None = None
) -> ExternalTaskEvidence:
    """Resolve what the poked task actually did. Never raises for an API error."""
    ev = ExternalTaskEvidence(
        dag_id=dag_id,
        task_ids=list(task_ids),
        logical_date=logical_date,
        sensor_failed_at=failed_at,
    )
    if not logical_date:
        ev.notes.append("no logical date in the poke line; cannot identify the target run")
        return ev
    try:
        api = _api()
        token = api.resolve_bearer()
        base = _resolve_base()
    except Exception as e:  # a stale astro session must degrade, never kill the diagnosis
        ev.notes.append(f"Airflow API unavailable ({e}); run 'astro login' and re-run")
        return ev

    try:
        run = _target_run(api, base, token, dag_id, logical_date)
        if not run:
            ev.notes.append(
                f"no {dag_id} run at logical date {logical_date}: the target run does not "
                "exist, so the sensor was waiting on something that was never scheduled"
            )
            return ev
        ev.run_id = run.get("dag_run_id")
        tis = api.list_task_instances_in_run(base, token, dag_id, ev.run_id) or []
    except Exception as e:
        ev.notes.append(f"Airflow API call failed: {e}")
        return ev

    wanted = set(ev.task_ids)
    hits = [t for t in tis if t.get("task_id") in wanted]
    ev.states = {t.get("task_id"): t.get("state") for t in hits}
    ev.target_end_date = max((t.get("end_date") or "" for t in hits), default="") or None
    missing = wanted - set(ev.states)
    if missing:
        ev.notes.append(f"target task(s) not in the run: {', '.join(sorted(missing))}")
    # Worst state wins: one failed target explains the sensor regardless of its siblings.
    for state in ("failed", "upstream_failed", "skipped", "running", "queued", "success"):
        if state in ev.states.values():
            ev.state = state
            break
    if not ev.state:
        return ev
    who = _first_with(ev.states, ev.state)
    ev.error_text = f"{dag_id}.{who} is {ev.state}"
    ev.state_is_later = _moved_on_after(ev.target_end_date, failed_at)
    if ev.state_is_later:
        cause = _STALE_VERDICT.format(state=ev.state, end=ev.target_end_date, failed=failed_at)
        key, sig_class, fix = _LATE_SIG
    else:
        cause = _VERDICT[ev.state]
        key, sig_class, fix = _SIG_CLASS[ev.state]
    ev.notes.append(cause)
    ev.signature = asdict(
        Match(
            key=key,
            sig_class=sig_class,
            likely_cause=f"{ev.error_text}. {cause}",
            programmatic_fix=fix,
            matched_on=f"Airflow API state of {dag_id}.{who}",
        )
    )
    return ev


def _moved_on_after(end_date: str | None, failed_at: str | None) -> bool:
    """True when the target reached its current state only AFTER the sensor gave up."""
    if not failed_at:
        return False
    if not end_date:
        return True  # never finished, so whatever it reads now, it had not finished then
    return _iso(end_date) > _iso(failed_at)


def _iso(value: str) -> str:
    """Comparable UTC key; both sources print RFC-3339, so lexical order is chronological."""
    return value.replace("+00:00", "Z").rstrip()


def _first_with(states: dict, state: str) -> str:
    """The first task id holding the given state."""
    return next((k for k, v in states.items() if v == state), "?")


if __name__ == "__main__":
    import json
    import sys
    from dataclasses import asdict

    argv = sys.argv[1:]
    if len(argv) < 3:
        print(
            "usage: python -m airflow_debugger.external_task_rca "
            "<dag_id> <task_id[,task_id]> <logical_date> [<sensor_failed_at>]"
        )
        raise SystemExit(2)
    ev = analyze_external_task(
        argv[0], argv[1].split(","), argv[2], argv[3] if len(argv) > 3 else None
    )
    print(json.dumps(asdict(ev), indent=2, default=str))
