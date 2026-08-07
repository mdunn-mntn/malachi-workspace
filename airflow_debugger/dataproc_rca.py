"""Dataproc Serverless batch failure analyzer (key-free).

Harvested pattern from data-eng-assistant (gcloud CLI, no stored tokens):
describe the batch (state / ttl / config / timing) and read its driver log via
**Cloud Logging** -> decode the `MCP_EVENT_LOGGING_CONFIG_BASE64` breadcrumb for
the Spark application_id -> extract the error text -> match a signature.

Driver logs come from Cloud Logging, NOT the GCS staging bucket: that bucket is
403 for on-call user creds (no `storage.objects.list`), while Cloud Logging is
readable and carries the same driver messages + MCP breadcrumbs.

TTL kills are detected structurally (state CANCELLED + runtime ~= ttl), which is
more reliable than any log string. The `.zstd` Spark event-log deep-dive
(spill / skew / uncached recompute) stays optional and is unavailable when the
History Server peripheral is omitted (common in this DAG family).
"""

from __future__ import annotations

import base64
import json
import re
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime

from .signatures import Match, classify

PROJECT_DEFAULT = "mntn-prj-prod-00"
REGION = "us-central1"
_LOG_LIMIT = 80
_LOG_FRESHNESS = "45d"
_TTL_TOLERANCE = 0.05  # runtime within 5% of ttl => TTL kill
_ERR_MARKERS = (
    "Traceback (most recent call last):",
    "RECEIVED SIGNAL TERM",
    "exited with code",
    "Exit code",
    "Py4JJavaError",
    "Caused by:",
    "Exception",
    "ERROR",
    "CANCELLED",
)
_PERF_KEYS = (
    "spark:spark.sql.shuffle.partitions",
    "spark:spark.dynamicAllocation.maxExecutors",
    "spark:spark.executor.memory",
    "spark:spark.executor.cores",
    "spark:spark.executor.instances",
    "spark:spark.driver.memory",
)


def _run(cmd: list[str], timeout: int = 90) -> tuple[str | None, str | None]:
    """Run a command; return (stdout, None) or (None, error)."""
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        return None, str(e)
    if out.returncode != 0:
        err = (out.stderr or out.stdout).strip()[:400]
        return None, err or f"exit code {out.returncode} with no output"
    return out.stdout, None


def _describe(batch_id: str, project: str, region: str) -> tuple[dict | None, str | None]:
    stdout, err = _run(
        ["gcloud", "dataproc", "batches", "describe", batch_id,
         "--region", region, "--project", project, "--format", "json"]
    )  # fmt: skip
    if err:
        return None, err
    try:
        return json.loads(stdout or "{}"), None
    except json.JSONDecodeError:
        return None, "describe: non-json output"


def _logging_messages(batch_id: str, project: str) -> tuple[str, str | None]:
    """Driver log lines via Cloud Logging (key-free; the GCS staging bucket is 403)."""
    filt = f'resource.type="cloud_dataproc_batch" AND resource.labels.batch_id="{batch_id}"'
    stdout, err = _run(
        ["gcloud", "logging", "read", filt, "--project", project,
         "--limit", str(_LOG_LIMIT), "--freshness", _LOG_FRESHNESS,
         "--order", "desc", "--format", "value(jsonPayload.message)"]
    )  # fmt: skip
    return stdout or "", err


def _ttl_seconds(ttl: str | None) -> int | None:
    if ttl and ttl.endswith("s"):
        try:
            return int(float(ttl[:-1]))
        except ValueError:
            return None
    return None


def _runtime_seconds(create: str | None, end: str | None) -> int | None:
    if not create or not end:
        return None
    try:
        c = datetime.fromisoformat(create.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end.replace("Z", "+00:00"))
        return int((e - c).total_seconds())
    except ValueError:
        return None


def _decode_app_id(text: str) -> str | None:
    for line in text.splitlines():
        _, _, tail = line.partition("MCP_EVENT_LOGGING_CONFIG_BASE64:")
        if tail:
            try:
                meta = json.loads(base64.b64decode(tail.strip()))
                if isinstance(meta, dict) and meta.get("application_id"):
                    return meta["application_id"]
            except (ValueError, json.JSONDecodeError):
                pass
    m = re.search(r"app-\d{13,18}-\d{4,}|application_\d{13}_\d+", text)
    return m.group(0) if m else None


def _error_region(text: str) -> str | None:
    idx = text.rfind("Traceback (most recent call last):")
    if idx >= 0:
        return text[idx : idx + 2000]
    seen, hits = set(), []
    for ln in text.splitlines():
        s = ln.strip()
        if s and s not in seen and any(k in ln for k in _ERR_MARKERS):
            seen.add(s)
            hits.append(s[:200])
    return "\n".join(hits[:10]) or None


@dataclass
class DataprocEvidence:
    """Deterministic evidence bundle for one Dataproc Serverless batch."""

    engine: str = "dataproc"
    batch_id: str | None = None
    state: str | None = None
    state_message: str | None = None
    application_id: str | None = None
    ttl: str | None = None
    runtime_s: int | None = None
    has_event_log: bool = False
    event_log_uri: str | None = None
    spark_config: dict = field(default_factory=dict)
    error_text: str | None = None
    signature: dict | None = None
    notes: list = field(default_factory=list)


def analyze_batch(
    batch_id: str, project: str = PROJECT_DEFAULT, region: str = REGION
) -> DataprocEvidence:
    """Deterministic RCA for one Dataproc batch. Never raises for a CLI error."""
    ev = DataprocEvidence(batch_id=batch_id)
    d, err = _describe(batch_id, project, region)
    if err or d is None:
        ev.notes.append(f"describe failed: {err}")
        return ev

    ev.state = d.get("state")
    ev.state_message = d.get("stateMessage")
    ev.ttl = (d.get("environmentConfig", {}).get("executionConfig", {}) or {}).get("ttl")
    ev.runtime_s = _runtime_seconds(d.get("createTime"), d.get("stateTime"))
    props = (d.get("runtimeConfig", {}) or {}).get("properties", {}) or {}
    ev.spark_config = {
        k.replace("spark:spark.", ""): v for k, v in props.items() if k in _PERF_KEYS
    }

    event_log_dir = props.get("spark:spark.eventLog.dir")
    hs = (d.get("environmentConfig", {}).get("peripheralsConfig", {}) or {}).get(
        "sparkHistoryServerConfig"
    )
    ev.has_event_log = bool(event_log_dir) or bool(hs)
    if not ev.has_event_log:
        ev.notes.append(
            "no persistent event log (eventLog.dir unset); deep spill/skew profile unavailable"
        )

    logs, log_err = _logging_messages(batch_id, project)
    if logs:
        ev.application_id = _decode_app_id(logs)
        ev.error_text = _error_region(logs)
        if ev.has_event_log and ev.application_id and event_log_dir:
            ev.event_log_uri = f"{event_log_dir}/{ev.application_id}"
    elif log_err:
        ev.notes.append(f"driver log fetch failed: {log_err}")
    else:
        ev.notes.append("no driver log via Cloud Logging (check freshness window)")

    # Structural TTL detection: CANCELLED + ran ~= its ttl => wall-clock kill.
    ttl_s = _ttl_seconds(ev.ttl)
    if (
        ev.state == "CANCELLED"
        and ttl_s
        and ev.runtime_s
        and abs(ev.runtime_s - ttl_s) / ttl_s < _TTL_TOLERANCE
    ):
        ev.signature = asdict(
            Match(
                "ttl_exceeded",
                "ttl/wall-clock",
                f"Cancelled at its {ev.ttl} TTL (ran {ev.runtime_s}s). Usually a perf regression. "
                "Profile the Spark event log for spill/skew/uncached recompute; a TTL bump alone "
                "rarely fixes it.",
                "sometimes",
                f"CANCELLED at ttl {ev.ttl}",
            )
        )
    else:
        m = classify(" ".join(filter(None, [ev.state_message, ev.error_text])), engine="dataproc")
        ev.signature = asdict(m) if m else None
    return ev


if __name__ == "__main__":
    import sys

    bid = sys.argv[1] if len(sys.argv) > 1 else "tpa-mntn-id-20260729-3"  # INC-005 try-3
    print(json.dumps(asdict(analyze_batch(bid)), indent=2, default=str))
