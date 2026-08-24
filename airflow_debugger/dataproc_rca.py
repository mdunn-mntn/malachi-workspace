"""Dataproc Serverless batch failure analyzer (key-free).

Harvested pattern from data-eng-assistant (gcloud CLI, no stored tokens):
describe the batch (state / ttl / config / timing) and read its driver log via
**Cloud Logging** -> decode the `MCP_EVENT_LOGGING_CONFIG_BASE64` breadcrumb for
the Spark application_id -> extract the error text -> match a signature.

Driver text comes from Cloud Logging first, falling back to the staging-bucket
`driveroutput.*` glob named in the batch stateMessage when Logging returns no
error text (egress flake, thin logging). The staging bucket denies everything
(even a direct object get reports `storage.objects.list` 403) without the
`dataproc-debug` PAM grant, so the fallback degrades to an actionable note.

TTL kills are detected structurally (state CANCELLED + runtime ~= ttl), which is
more reliable than any log string. The `.zstd` Spark event-log deep-dive
(spill / skew / uncached recompute) stays optional and is unavailable when the
History Server peripheral is omitted (common in this DAG family).
"""

from __future__ import annotations

import base64
import ipaddress
import json
import os
import re
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime

from .signatures import Match, classify

PROJECT_DEFAULT = "mntn-prj-prod-00"
REGION = "us-central1"
_LOG_LIMIT = 80
_LOGGING_HOST = "logging.googleapis.com"
_OAUTH_HOST = "oauth2.googleapis.com"
_ADC_PATH = "~/.config/gcloud/application_default_credentials.json"
_PUBLIC_RESOLVER = "8.8.8.8"
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
_DRIVEROUTPUT_RE = re.compile(r"gs://\S+/driveroutput(?:\.\*|\.\d+)?")
_DRIVER_HEAD_CHARS = 20_000  # keep the head: the MCP breadcrumb prints at startup
_DRIVER_TAIL_CHARS = 180_000  # keep the tail: the failure sits at the end
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


_DNS_BLOCK_MARKERS = (
    "0.0.0.0", "Name or service not known", "nodename nor servname",
    "Temporary failure in name resolution", "Failed to establish a new connection",
    "Connection refused", "getaddrinfo", "ServiceUnavailable", "Unable to find the server",
)


def _is_public_v4(addr: str) -> bool:
    """True only for a routable public IPv4 address.

    A router that transparently redirects port 53 answers @8.8.8.8 from the local
    blocker anyway. In IP-blocking mode that answer is the blocker's own LAN address,
    which pins curl at the blocker and surfaces as a confusing parse error instead of
    an honest resolution failure.
    """
    try:
        ip = ipaddress.IPv4Address(addr)
    except ipaddress.AddressValueError:
        return False
    return ip.is_global


def _public_ip(host: str) -> str | None:
    """Resolve a host against a public resolver, bypassing a local DNS sinkhole."""
    stdout, err = _run(["dig", "+short", f"@{_PUBLIC_RESOLVER}", "A", host], timeout=15)
    if err:
        return None
    for line in (stdout or "").splitlines():
        line = line.strip()
        if line and line[0].isdigit() and _is_public_v4(line):
            return line
    return None


def _token_via_pinned_refresh() -> tuple[str | None, str | None]:
    """Mint an access token over a pinned IP, when the sinkhole also eats the OAuth host.

    `gcloud auth print-access-token` goes through the system resolver, so a token that
    needs refreshing dies in the very sinkhole this fallback exists to route around. The
    refresh is a plain form POST, so it can be pinned the same way as the log read.
    """
    ip = _public_ip(_OAUTH_HOST)
    if not ip:
        return None, f"could not resolve {_OAUTH_HOST} via {_PUBLIC_RESOLVER}"
    try:
        with open(os.path.expanduser(_ADC_PATH), encoding="utf-8") as f:
            adc = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        return None, f"application default credentials unreadable: {e}"
    if adc.get("type") != "authorized_user":
        return None, f"ADC type {adc.get('type')!r} has no refresh_token to exchange"
    stdout, err = _run([
        "curl", "-s", "--max-time", "30",
        "--resolve", f"{_OAUTH_HOST}:443:{ip}",
        f"https://{_OAUTH_HOST}/token",
        "-d", f"client_id={adc.get('client_id', '')}",
        "-d", f"client_secret={adc.get('client_secret', '')}",
        "-d", f"refresh_token={adc.get('refresh_token', '')}",
        "-d", "grant_type=refresh_token",
    ])  # fmt: skip
    if err:
        return None, err
    try:
        body = json.loads(stdout or "{}")
    except json.JSONDecodeError:
        return None, "pinned token refresh: non-json response"
    token = body.get("access_token")
    if not token:
        return None, f"pinned token refresh: {body.get('error_description') or body.get('error')}"
    return token, None


def _access_token() -> tuple[str | None, str | None]:
    """A short-lived access token, falling back to a pinned refresh when DNS is sinkholed."""
    token, err = _run(["gcloud", "auth", "print-access-token"], timeout=30)
    if token and token.strip():
        return token.strip(), None
    if err and not any(m in err for m in _DNS_BLOCK_MARKERS):
        return None, err
    pinned, pin_err = _token_via_pinned_refresh()
    if pinned:
        return pinned, None
    return None, f"{err or 'no token'} | pinned refresh: {pin_err}"


def _api_error(stdout: str) -> str | None:
    """The API's own error message, which a bare `curl -s` would discard as `no entries`."""
    try:
        body = json.loads(stdout or "{}")
    except json.JSONDecodeError:
        return None
    err = body.get("error")
    if isinstance(err, dict):
        return f"HTTP {err.get('code')}: {err.get('message')}"
    return None


def _logging_via_curl(filt: str, project: str) -> tuple[str, str | None]:
    """Cloud Logging over a pinned IP, for when local DNS sinkholes the API host."""
    ip = _public_ip(_LOGGING_HOST)
    if not ip:
        return "", f"could not resolve {_LOGGING_HOST} via {_PUBLIC_RESOLVER}"
    token, err = _access_token()
    if err:
        return "", f"access token: {err}"
    body = json.dumps({
        "resourceNames": [f"projects/{project}"],
        "filter": filt,
        "orderBy": "timestamp desc",
        "pageSize": _LOG_LIMIT,
    })
    stdout, err = _run([
        "curl", "-s", "--max-time", "60",
        "--resolve", f"{_LOGGING_HOST}:443:{ip}",
        f"https://{_LOGGING_HOST}/v2/entries:list",
        "-H", f"Authorization: Bearer {token}",
        "-H", "Content-Type: application/json",
        "-d", body,
    ])
    if err:
        return "", err
    api_err = _api_error(stdout or "")
    if api_err:
        return "", f"pinned curl: {api_err}"
    try:
        entries = json.loads(stdout or "{}").get("entries", [])
    except json.JSONDecodeError:
        return "", "pinned curl: non-json response"
    lines = [e.get("jsonPayload", {}).get("message", "") for e in entries]
    return "\n".join(m for m in lines if m), None


def logging_messages(
    filt: str, project: str, limit: int = _LOG_LIMIT, field: str = "jsonPayload.message"
) -> tuple[str, str | None]:
    """Lines of one field for any Cloud Logging filter, with the pinned-IP fallback."""
    stdout, err = _run(
        ["gcloud", "logging", "read", filt, "--project", project,
         "--limit", str(limit), "--freshness", _LOG_FRESHNESS,
         "--order", "desc", "--format", f"value({field})"]
    )  # fmt: skip
    if stdout or not err:
        return stdout or "", err
    if not any(m in err for m in _DNS_BLOCK_MARKERS):
        return "", err
    text, curl_err = _logging_via_curl(filt, project)
    if text:
        return text, None
    return "", f"{err} | pinned-curl fallback: {curl_err or 'no entries'}"


def _logging_messages(batch_id: str, project: str) -> tuple[str, str | None]:
    """Driver log lines via Cloud Logging (key-free; the GCS staging bucket is 403)."""
    return logging_messages(
        f'resource.type="cloud_dataproc_batch" AND resource.labels.batch_id="{batch_id}"', project
    )


def driveroutput_uri(state_message: str | None) -> str | None:
    """The staging-bucket driver-output glob a failed batch names in its stateMessage."""
    m = _DRIVEROUTPUT_RE.search(state_message or "")
    return m.group(0) if m else None


def driveroutput_text(uri: str) -> tuple[str | None, str | None]:
    """Read driver output from the staging bucket; return (text, None) or (None, why)."""
    try:
        out = subprocess.run(
            ["gsutil", "-o", "GSUtil:check_hashes=never", "cat", uri],
            capture_output=True,
            text=True,
            timeout=180,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        return None, str(e)
    if out.returncode != 0:
        err = out.stderr or out.stdout or ""
        if "403" in err or "AccessDenied" in err:
            return None, (
                "403 on the staging bucket; request the dataproc-debug PAM grant, then re-run"
            )
        return None, err.strip()[-400:] or f"exit code {out.returncode}"
    text = out.stdout
    if len(text) > _DRIVER_HEAD_CHARS + _DRIVER_TAIL_CHARS:
        text = (
            text[:_DRIVER_HEAD_CHARS]
            + "\n...[driveroutput truncated]...\n"
            + text[-_DRIVER_TAIL_CHARS:]
        )
    return text, None


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


def error_region(text: str) -> str | None:
    """The failure region of a driver log: the last traceback, else the distinct error lines."""
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
    batch_uuid: str | None = None
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
    ev.batch_uuid = d.get("uuid")
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
        ev.error_text = error_region(logs)
        if ev.has_event_log and ev.application_id and event_log_dir:
            ev.event_log_uri = f"{event_log_dir}/{ev.application_id}"
    elif log_err:
        ev.notes.append(f"driver log fetch failed: {log_err}")
    else:
        ev.notes.append("no driver log via Cloud Logging (check freshness window)")

    # Fallback: no Cloud Logging error text -> the driveroutput the stateMessage names.
    if not ev.error_text:
        uri = driveroutput_uri(ev.state_message)
        if uri:
            text, do_err = driveroutput_text(uri)
            if text:
                ev.application_id = ev.application_id or _decode_app_id(text)
                ev.error_text = error_region(text)
                ev.notes.append(
                    "driver text read from staging driveroutput (Cloud Logging had none)"
                )
                if (
                    ev.has_event_log
                    and ev.application_id
                    and event_log_dir
                    and not ev.event_log_uri
                ):
                    ev.event_log_uri = f"{event_log_dir}/{ev.application_id}"
            else:
                ev.notes.append(f"driveroutput fallback failed: {do_err}")

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
