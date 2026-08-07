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
    orig = (dataproc_rca._describe, dataproc_rca._logging_messages, dataproc_rca._driveroutput_text)
    if describe is not None:
        dataproc_rca._describe = describe
    if logging is not None:
        dataproc_rca._logging_messages = logging
    if driveroutput is not None:
        dataproc_rca._driveroutput_text = driveroutput
    return orig


def _restore(orig: tuple) -> None:
    dataproc_rca._describe, dataproc_rca._logging_messages, dataproc_rca._driveroutput_text = orig


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


if __name__ == "__main__":
    test_uri_parses_real_state_message()
    test_uri_absent_returns_none()
    test_fallback_reads_driveroutput_and_classifies_inc012()
    test_fallback_403_degrades_to_actionable_note()
    test_no_fallback_when_logging_carries_error_text()
    print("OK - dataproc_rca driveroutput fallback tests passed")
