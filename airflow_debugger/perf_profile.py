"""Perf-class handoff: when a failure signature is perf-shaped, profile the event log.

The debugger names WHAT killed the batch (ttl_exceeded, driver_oom, fetch_failed); for those
classes the WHY usually lives in the Spark event log (spill, skew, straggler, recompute -
INC-005 is the proof case). Escalation-only bridge into airflow_optimizer: fires only for
perf signatures with a reachable event log, never on other failure classes, and degrades to
a note on any error (logs are often absent or PAM-gated). IMP-032.
"""

from __future__ import annotations

import os
import subprocess
import tempfile

PERF_SIGNATURES = {"ttl_exceeded", "driver_oom", "fetch_failed"}
_TOP_N = 5
_GSUTIL_OPTS = [
    "-o", "GSUtil:check_hashes=never",  # crc32c gatekeeper corrupts .zstd
    "-o", "GSUtil:sliced_object_download_threshold=0",  # -m/sliced hangs on this Mac
]
_FETCH_TIMEOUT_S = 300


def should_profile(diag: dict) -> bool:
    """Escalation gate: dataproc + perf-shaped root signature + an event log to read."""
    spark = diag.get("spark") or {}
    key = (diag.get("root_signature") or {}).get("key") or ""
    return (
        spark.get("engine") == "dataproc"
        and key in PERF_SIGNATURES
        and bool(spark.get("has_event_log"))
    )


def _candidates(spark: dict) -> list[str]:
    """Event-log locations to try, most likely first (fleet logs are batch-uuid rolling dirs)."""
    out = []
    uri = spark.get("event_log_uri")
    log_dir = uri.rsplit("/", 1)[0] if uri else None
    if log_dir and spark.get("batch_uuid"):
        out.append(f"{log_dir}/eventlog_v2_batch-{spark['batch_uuid']}")
    if uri:
        out += [f"{uri}.zstd", uri]
    return out


def _fetch(uri: str, dest: str) -> str | None:
    """Bring one candidate local; local paths (tests) pass through. None if unreachable."""
    if not uri.startswith("gs://"):
        return uri if os.path.exists(uri) else None
    name = uri.rstrip("/").rsplit("/", 1)[-1]
    local = os.path.join(dest, name)
    try:
        if name.startswith("eventlog_v2_"):
            os.makedirs(local, exist_ok=True)
            r = subprocess.run(
                ["gsutil", *_GSUTIL_OPTS, "cp", f"{uri}/events_*", local + "/"],
                capture_output=True, timeout=_FETCH_TIMEOUT_S,
            )
            return local if r.returncode == 0 and os.listdir(local) else None
        r = subprocess.run(
            ["gsutil", *_GSUTIL_OPTS, "cp", uri, local],
            capture_output=True, timeout=_FETCH_TIMEOUT_S,
        )
        return local if r.returncode == 0 and os.path.exists(local) else None
    except (subprocess.TimeoutExpired, OSError):
        return None


def profile(diag: dict, workdir: str | None = None) -> dict | None:
    """Run the optimizer's event-log analysis for a perf-shaped failure. Never raises."""
    if not should_profile(diag):
        return None
    spark = diag.get("spark") or {}
    dest = workdir or tempfile.mkdtemp(prefix="perf_profile_")
    for uri in _candidates(spark):
        local = _fetch(uri, dest)
        if not local:
            continue
        try:
            from airflow_optimizer.optimize import analyze_eventlog

            _, findings = analyze_eventlog(local)
        except Exception as e:  # a profiler crash must never kill the diagnosis
            return {"source": uri, "error": f"profile failed: {e}", "findings": []}
        return {
            "source": uri,
            "error": None,
            "findings": [
                {"key": f.key, "impact": f.impact, "title": f.title,
                 "evidence": f.evidence, "fix": f.fix, "rec_type": f.rec_type}
                for f in findings[:_TOP_N]
            ],
        }
    return {"source": None, "error": "event log not reachable (absent or access-gated)",
            "findings": []}
