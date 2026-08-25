"""Offline tests for the dataproc analyzer's driveroutput fallback (IMP-028).

Run: python3 -m airflow_debugger.tests.test_dataproc_rca  (or via pytest).

Fixture inc012_driveroutput.log is the REAL INC-012 try-1 driver output
(materialize_mntn_select 2026-08-06, batch mntn-select-2026-08-06-1786049114),
read from the staging bucket under the dataproc-debug PAM grant.
"""

from __future__ import annotations

from pathlib import Path

from airflow_debugger import dataproc_rca
from airflow_debugger.dataproc_rca import analyze_batch, driveroutput_uri

_FIXTURE = Path(__file__).parent / "fixtures" / "inc012_driveroutput.log"

# The live stateMessage of the real failed batch (gcloud describe, 2026-08-06).
_INC012_STATE_MESSAGE = (
    "Google Cloud Dataproc Agent reports job failure. If logs are available, they can be found at:\n"
    "https://console.cloud.google.com/dataproc/batches/us-central1/mntn-select-2026-08-06-1786049114"
    "?project=mntn-prj-prod-00\n"
    "gcloud dataproc batches wait 'mntn-select-2026-08-06-1786049114' --region 'us-central1'"
    " --project 'mntn-prj-prod-00'\n"
    "https://console.cloud.google.com/storage/browser/dataproc-staging-us-central1-995798185124-d8mf0cme/"
    "google-cloud-dataproc-metainfo/97804124-642c-434b-9511-567be172416e/jobs/"
    "srvls-batch-e8a9d9e3-7dae-4b7d-af5f-7694fdaad71a/\n"
    "gs://dataproc-staging-us-central1-995798185124-d8mf0cme/google-cloud-dataproc-metainfo/"
    "97804124-642c-434b-9511-567be172416e/jobs/srvls-batch-e8a9d9e3-7dae-4b7d-af5f-7694fdaad71a/"
    "driveroutput.*"
)

_DESCRIBE = {
    "state": "FAILED",
    "stateMessage": _INC012_STATE_MESSAGE,
    "createTime": "2026-08-06T21:04:24Z",
    "stateTime": "2026-08-06T21:23:05Z",
    "environmentConfig": {"executionConfig": {"ttl": "14400s"}},
    "runtimeConfig": {"properties": {}},
}


def _patched(describe=None, logging=None, driveroutput=None) -> tuple:  # noqa: ANN001
    """Swap the module's CLI-touching functions; return the originals for restore."""
    orig = (dataproc_rca._describe, dataproc_rca._logging_messages, dataproc_rca.driveroutput_text)
    if describe is not None:
        dataproc_rca._describe = describe
    if logging is not None:
        dataproc_rca._logging_messages = logging
    if driveroutput is not None:
        dataproc_rca.driveroutput_text = driveroutput
    return orig


def _restore(orig: tuple) -> None:
    dataproc_rca._describe, dataproc_rca._logging_messages, dataproc_rca.driveroutput_text = orig


def test_uri_parses_real_state_message() -> None:
    """The exact glob the failed batch names is extracted verbatim."""
    uri = driveroutput_uri(_INC012_STATE_MESSAGE)
    assert uri is not None and uri.startswith("gs://dataproc-staging-")
    assert uri.endswith("/driveroutput.*")


def test_uri_absent_returns_none() -> None:
    """A stateMessage without a driveroutput URI (or None) yields None."""
    assert driveroutput_uri(None) is None
    assert driveroutput_uri("Batch was CANCELLED as ttl exceeded") is None


def test_fallback_reads_driveroutput_and_classifies_inc012() -> None:
    """Logging egress dead -> the fixture text yields the gcs_list_timeout verdict."""
    text = _FIXTURE.read_text()
    orig = _patched(
        describe=lambda *a: (_DESCRIBE, None),
        logging=lambda *a: ("", "gcloud crashed (ConnectionError): logging.googleapis.com refused"),
        driveroutput=lambda uri: (text, None),
    )
    try:
        ev = analyze_batch("mntn-select-2026-08-06-1786049114")
    finally:
        _restore(orig)
    assert ev.error_text and "Error listing gs://" in ev.error_text
    assert ev.signature and ev.signature["key"] == "gcs_list_timeout"
    assert any("staging driveroutput" in n for n in ev.notes)


def test_fallback_403_degrades_to_actionable_note() -> None:
    """No PAM grant -> no crash, no error_text, a note that says how to unblock."""
    orig = _patched(
        describe=lambda *a: (_DESCRIBE, None),
        logging=lambda *a: ("", None),
        driveroutput=lambda uri: (
            None,
            "403 on the staging bucket; request the dataproc-debug PAM grant, then re-run",
        ),
    )
    try:
        ev = analyze_batch("mntn-select-2026-08-06-1786049114")
    finally:
        _restore(orig)
    assert ev.error_text is None
    assert any("driveroutput fallback failed" in n and "PAM" in n for n in ev.notes)


def test_no_fallback_when_logging_carries_error_text() -> None:
    """Cloud Logging already produced error text -> the staging bucket is not touched."""

    def _must_not_run(uri: str) -> None:
        raise AssertionError("driveroutput fallback fired despite Logging error text")

    logs = "Traceback (most recent call last):\n  File x\nValueError: boom"
    orig = _patched(
        describe=lambda *a: (_DESCRIBE, None),
        logging=lambda *a: (logs, None),
        driveroutput=_must_not_run,
    )
    try:
        ev = analyze_batch("mntn-select-2026-08-06-1786049114")
    finally:
        _restore(orig)
    assert ev.error_text and "ValueError" in ev.error_text


def test_dns_sinkhole_falls_back_to_pinned_curl() -> None:
    """A sinkholed DNS error retries over a pinned IP instead of returning nothing."""
    calls = []

    def fake_run(cmd: list[str], timeout: int = 90) -> tuple[str | None, str | None]:
        calls.append(cmd[:3])
        if cmd[:3] == ["gcloud", "logging", "read"]:
            return None, "ServiceUnavailable: 503 failed to connect to all addresses; 0.0.0.0:443"
        if cmd[:2] == ["dig", "+short"]:
            return "142.251.46.74\n", None
        if cmd[:2] == ["gcloud", "auth"]:
            return "ya29.token\n", None
        if cmd[0] == "curl":
            return '{"entries":[{"jsonPayload":{"message":"OutOfMemoryError: Java heap space"}}]}', None
        raise AssertionError(f"unexpected command {cmd}")

    orig = dataproc_rca._run
    dataproc_rca._run = fake_run
    try:
        text, err = dataproc_rca._logging_messages("some-batch-1", "mntn-prj-prod-00")
    finally:
        dataproc_rca._run = orig
    assert err is None, err
    assert "OutOfMemoryError" in text
    assert [c[:1][0] for c in calls if c[0] == "curl"][:1] == ["curl"]


def test_non_dns_error_does_not_reach_the_fallback() -> None:
    """A permissions failure is a real answer, not something a pinned IP can fix."""

    def fake_run(cmd: list[str], timeout: int = 90) -> tuple[str | None, str | None]:
        if cmd[:3] == ["gcloud", "logging", "read"]:
            return None, "PERMISSION_DENIED: caller lacks logging.logEntries.list"
        raise AssertionError("fallback fired on a non-DNS error")

    orig = dataproc_rca._run
    dataproc_rca._run = fake_run
    try:
        text, err = dataproc_rca._logging_messages("some-batch-1", "mntn-prj-prod-00")
    finally:
        dataproc_rca._run = orig
    assert text == ""
    assert "PERMISSION_DENIED" in err


def test_sinkholed_resolver_answer_is_rejected() -> None:
    """dig returning the sinkhole address is not a usable IP."""

    def fake_run(cmd: list[str], timeout: int = 90) -> tuple[str | None, str | None]:
        if cmd[:2] == ["dig", "+short"]:
            return "0.0.0.0\n", None
        raise AssertionError(f"unexpected command {cmd}")

    orig = dataproc_rca._run
    dataproc_rca._run = fake_run
    try:
        assert dataproc_rca._public_ip("logging.googleapis.com") is None
    finally:
        dataproc_rca._run = orig


def test_lan_sinkhole_answer_is_rejected() -> None:
    """An IP-blocking sinkhole answering with its own LAN address must not be pinned."""
    for addr in ("192.168.10.177", "10.0.0.53", "127.0.0.1", "172.16.4.4", "0.0.0.0"):
        assert not dataproc_rca._is_public_v4(addr), addr
    for addr in ("142.250.73.106", "216.239.34.174", "8.8.8.8"):
        assert dataproc_rca._is_public_v4(addr), addr


def test_token_refresh_is_pinned_too(tmp_adc: str = "") -> None:
    """IMP-051: a token that needs refreshing must not go through the sinkhole being routed around."""
    import json as _json
    import tempfile

    adc = {"type": "authorized_user", "client_id": "cid", "client_secret": "sec",
           "refresh_token": "rt"}  # fmt: skip
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        _json.dump(adc, fh)
        path = fh.name

    def fake_run(cmd: list[str], timeout: int = 90) -> tuple[str | None, str | None]:
        if cmd[:2] == ["gcloud", "auth"]:
            return None, "getaddrinfo: Name or service not known"
        if cmd[:2] == ["dig", "+short"]:
            return "142.251.46.74\n", None
        if cmd[0] == "curl":
            assert "--resolve" in cmd, "the token refresh went through the system resolver"
            assert any("oauth2.googleapis.com/token" in a for a in cmd)
            return '{"access_token":"ya29.pinned","expires_in":3599}', None
        raise AssertionError(f"unexpected command {cmd}")

    orig_run, orig_adc = dataproc_rca._run, dataproc_rca._ADC_PATH
    dataproc_rca._run, dataproc_rca._ADC_PATH = fake_run, path
    try:
        token, err = dataproc_rca._access_token()
    finally:
        dataproc_rca._run, dataproc_rca._ADC_PATH = orig_run, orig_adc
    assert (token, err) == ("ya29.pinned", None)


def test_a_non_dns_token_failure_is_not_retried_over_a_pinned_ip() -> None:
    """A revoked credential is a real answer; pinning an IP cannot fix it and must not mask it."""
    def fake_run(cmd: list[str], timeout: int = 90) -> tuple[str | None, str | None]:
        if cmd[:2] == ["gcloud", "auth"]:
            return None, "Reauthentication required"
        raise AssertionError("pinned refresh fired on a non-DNS token failure")

    orig = dataproc_rca._run
    dataproc_rca._run = fake_run
    try:
        token, err = dataproc_rca._access_token()
    finally:
        dataproc_rca._run = orig
    assert token is None and "Reauthentication" in err


def test_an_http_error_body_is_surfaced_not_read_as_no_entries() -> None:
    """IMP-052: `curl -s` without --fail parses a 403 body fine and finds zero entries."""
    def fake_run(cmd: list[str], timeout: int = 90) -> tuple[str | None, str | None]:
        if cmd[:3] == ["gcloud", "logging", "read"]:
            return None, "getaddrinfo failed"
        if cmd[:2] == ["dig", "+short"]:
            return "142.251.46.74\n", None
        if cmd[:2] == ["gcloud", "auth"]:
            return "ya29.token\n", None
        if cmd[0] == "curl":
            return ('{"error":{"code":403,"message":"Permission logging.logEntries.list denied.",'
                    '"status":"PERMISSION_DENIED"}}'), None
        raise AssertionError(f"unexpected command {cmd}")

    orig = dataproc_rca._run
    dataproc_rca._run = fake_run
    try:
        text, err = dataproc_rca._logging_messages("some-batch-1", "mntn-prj-prod-00")
    finally:
        dataproc_rca._run = orig
    assert text == ""
    assert "403" in err and "logging.logEntries.list denied" in err
    assert "no entries" not in err, "the real API error was discarded"


def test_public_ip_skips_sinkhole_and_takes_the_real_answer() -> None:
    """dig output mixing a CNAME, a sinkhole answer and a real A record yields the real one."""
    orig = dataproc_rca._run
    dataproc_rca._run = lambda *a, **k: (
        "logging-alv.googleapis.com.\n192.168.10.177\n216.239.34.174\n",
        None,
    )
    try:
        assert dataproc_rca._public_ip("logging.googleapis.com") == "216.239.34.174"
    finally:
        dataproc_rca._run = orig


_REAL_NOT_FOUND = (
    "ERROR: (gcloud.dataproc.batches.describe) NOT_FOUND: Not found: Batch "
    "projects/mntn-prj-prod-00/locations/us-central1/batches/tpa-export-2026-08-15-1786992151-2. "
    "This command is authenticated as malachi@mountain.com which is the active account."
)


def test_an_expired_batch_reads_as_expired_not_as_an_error() -> None:
    """Dataproc ages batches out; a historical failure is expected, not a fault."""
    note = dataproc_rca.describe_failure_note("tpa-export-2026-08-15-1786992151-2", _REAL_NOT_FOUND)
    assert "expired" in note
    assert "unrecoverable" in note
    assert "gcloud" not in note


def test_no_account_reaches_a_published_report() -> None:
    """The report lands in a shared bucket, so a CLI error must not carry whose account ran it."""
    for err in (_REAL_NOT_FOUND, "quota exceeded for malachi@mountain.com on project x"):
        note = dataproc_rca.describe_failure_note("b", err)
        assert "@mountain.com" not in note, note
        assert "malachi" not in note, note


def test_a_permission_error_is_named_as_one() -> None:
    """A missing grant and an expired batch need different actions, so name which one it is."""
    note = dataproc_rca.describe_failure_note("b", "PERMISSION_DENIED: caller does not have access")
    assert "lacks dataproc.batches.get" in note


def test_an_unknown_error_survives_scrubbed_rather_than_dropped() -> None:
    """Losing the error entirely would be worse than publishing a redacted one."""
    note = dataproc_rca.describe_failure_note("b", "some novel failure mode")
    assert "some novel failure mode" in note


if __name__ == "__main__":
    test_uri_parses_real_state_message()
    test_uri_absent_returns_none()
    test_fallback_reads_driveroutput_and_classifies_inc012()
    test_fallback_403_degrades_to_actionable_note()
    test_no_fallback_when_logging_carries_error_text()
    test_dns_sinkhole_falls_back_to_pinned_curl()
    test_non_dns_error_does_not_reach_the_fallback()
    test_sinkholed_resolver_answer_is_rejected()
    test_lan_sinkhole_answer_is_rejected()
    test_public_ip_skips_sinkhole_and_takes_the_real_answer()
    test_token_refresh_is_pinned_too()
    test_a_non_dns_token_failure_is_not_retried_over_a_pinned_ip()
    test_an_http_error_body_is_surfaced_not_read_as_no_entries()
    print("OK - dataproc_rca driveroutput + dns-fallback tests passed")
