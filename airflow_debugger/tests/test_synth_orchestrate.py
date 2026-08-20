"""Offline tests for the LLM synth fallback + orchestrator guards (no network).

Run: python3 -m airflow_debugger.tests.test_synth_orchestrate  (or via pytest).
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import types
from collections.abc import Callable

from airflow_debugger import orchestrate
from airflow_debugger import synth as synth_mod
from airflow_debugger.parse import ParsedFailure
from airflow_debugger.synth import _MAX_INPUT, _build_payload, synthesize

# Real prod shape (2026-08-06 set_gaclid_enabled_flag/send_notification): unclassified
# failure whose root cause exists only in the raw log text (SlackApiError not_in_channel).
_SLACK_LOG = """\
2026-08-06T01:08:16.873042Z [info] astronomer.runtime.listener TaskInstance Details:
2026-08-06T01:08:17.430913Z [error] task Task failed with exception
2026-08-06T01:08:17.476952Z [info] include.job_config.slack_messages id=UUID('019fd498') task_id='send_notification' dag_id='set_gaclid_enabled_flag' run_id='scheduled__2026-08-05T01:00:00+00:00' try_number=2 'exception': SlackApiError("The request to the Slack API failed. The server responded with: {'ok': False, 'error': 'not_in_channel'}")
2026-08-06T01:08:17.610566Z [error] airflow.providers.slack.notifications.slack.SlackNotifier Failed to send notification (sync): The request to the Slack API failed.
The server responded with: {'ok': False, 'error': 'channel_not_found'}
"""


class _Block:
    def __init__(self, type_: str, text: str = "") -> None:
        self.type = type_
        self.text = text


class _Resp:
    def __init__(self, content: list, stop_reason: str = "end_turn") -> None:
        self.content = content
        self.stop_reason = stop_reason


def _fake_anthropic(create: Callable) -> types.ModuleType:
    mod = types.ModuleType("anthropic")

    class Anthropic:
        def __init__(self) -> None:
            self.messages = types.SimpleNamespace(create=create)

    mod.Anthropic = Anthropic
    return mod


def _with_anthropic(module: types.ModuleType | None, fn: Callable) -> object:
    had, prev = "anthropic" in sys.modules, sys.modules.get("anthropic")
    sys.modules["anthropic"] = module
    try:
        return fn()
    finally:
        if had:
            sys.modules["anthropic"] = prev
        else:
            del sys.modules["anthropic"]


# Deliberately UNCLASSIFIED: the deterministic-vs-LLM test needs the LLM path to be
# reached. Any signature added here would silently turn that test into a no-op.
_UNKNOWN_LOG = """\
2026-08-06T01:08:16.873042Z [info] astronomer.runtime.listener TaskInstance Details:
2026-08-06T01:08:17.430913Z [error] task Task failed with exception
2026-08-06T01:08:17.476952Z [info] include.job_config.slack_messages id=UUID('019fd498') task_id='send_notification' dag_id='set_gaclid_enabled_flag' run_id='scheduled__2026-08-05T01:00:00+00:00' try_number=2 'exception': DiskFullException("No space left on device while staging output")
"""


def _unknown_log_file() -> str:
    fd, path = tempfile.mkstemp(suffix=".log", text=True)
    with os.fdopen(fd, "w") as f:
        f.write(_UNKNOWN_LOG)
    return path


def _slack_log_file() -> str:
    fd, path = tempfile.mkstemp(suffix=".log", text=True)
    with os.fdopen(fd, "w") as f:
        f.write(_SLACK_LOG)
    return path


def test_api_error_returns_none_not_stub() -> None:
    """An API error yields (None, note), never a truthy stub the caller mistakes for an RCA."""

    def _raise(**kwargs: object) -> None:
        raise RuntimeError("no ANTHROPIC_API_KEY")

    rca, note = _with_anthropic(
        _fake_anthropic(_raise), lambda: synthesize({"engine": "unknown"}, [])
    )
    assert rca is None
    assert "RuntimeError" in note and "no ANTHROPIC_API_KEY" in note


def test_import_missing_returns_note() -> None:
    """A missing anthropic package yields (None, note), not a silent None."""
    rca, note = _with_anthropic(None, lambda: synthesize({}, []))
    assert rca is None
    assert "not installed" in note


def test_payload_truncation_is_structure_aware() -> None:
    """A huge root_error shrinks to valid JSON that still carries similar_past_incidents."""
    evidence = {"spark": {"root_error": "x" * 13000}, "engine": "databricks"}
    payload = _build_payload(evidence, [{"inc": "INC-42", "signature": "driver-oom"}])
    assert len(payload) <= _MAX_INPUT
    doc = json.loads(payload)
    assert doc["similar_past_incidents"][0]["inc"] == "INC-42"
    assert doc["evidence"]["spark"]["root_error"].endswith("...[truncated]")


def test_output_capped_at_500() -> None:
    """A long model reply is capped at 500 chars, matching the docstring contract."""
    resp = _Resp([_Block("text", "a" * 700)])
    rca, note = _with_anthropic(_fake_anthropic(lambda **kw: resp), lambda: synthesize({}, []))
    assert len(rca) == 500
    assert note is None


def test_max_tokens_thinking_only_returns_note() -> None:
    """A max_tokens-exhausted turn with only a thinking block yields (None, cap note)."""
    resp = _Resp([_Block("thinking")], stop_reason="max_tokens")
    rca, note = _with_anthropic(_fake_anthropic(lambda **kw: resp), lambda: synthesize({}, []))
    assert rca is None
    assert "max_tokens" in note


def test_orchestrate_prefers_deterministic_on_llm_failure() -> None:
    """An LLM error never replaces the valid deterministic report or fakes llm confidence."""

    def _raise(**kwargs: object) -> None:
        raise RuntimeError("FakeAuthError")

    path = _unknown_log_file()
    try:
        res = _with_anthropic(_fake_anthropic(_raise), lambda: orchestrate.investigate(path))
    finally:
        os.unlink(path)
    assert res["report"].startswith("RCA [low]: set_gaclid_enabled_flag/send_notification")
    assert res["confidence"] == "low"
    assert res["llm_used"] is False
    assert "LLM synthesis unavailable" in res["diagnosis"]["llm_note"]


def test_slack_failure_classifies_without_an_llm_call() -> None:
    """A Slack notify failure resolves deterministically; the LLM is never reached."""

    def _raise(**kwargs: object) -> None:
        raise AssertionError("LLM called for a classified failure")

    path = _slack_log_file()
    try:
        res = _with_anthropic(_fake_anthropic(_raise), lambda: orchestrate.investigate(path))
    finally:
        os.unlink(path)
    assert res["diagnosis"]["root_signature"]["key"] == "slack_notify_failed"
    assert res["confidence"] == "high"
    assert res["llm_used"] is False


def test_orchestrate_matcher_crash_degrades_to_no_matches() -> None:
    """A matcher crash degrades to empty matches instead of killing the run."""

    def _boom(*args: object) -> None:
        raise json.JSONDecodeError("Expecting ':' delimiter", '{"inc"', 6)

    path = _unknown_log_file()
    orig = orchestrate.match_incidents
    orchestrate.match_incidents = _boom
    try:
        res = orchestrate.investigate(path, use_llm=False)
    finally:
        orchestrate.match_incidents = orig
        os.unlink(path)
    assert res["report"].startswith("RCA [low]")
    assert res["similar_incidents"] == []


def test_evidence_includes_log_tail_and_parse_notes() -> None:
    """The LLM evidence bundle carries the raw log tail and parse notes."""
    captured: dict = {}

    def _capture(evidence: dict, matches: list | None = None, model: str = "") -> tuple:
        captured.update(evidence)
        return None, None

    path = _unknown_log_file()
    orig = synth_mod.synthesize
    synth_mod.synthesize = _capture
    try:
        orchestrate.investigate(path)
    finally:
        synth_mod.synthesize = orig
        os.unlink(path)
    assert "No space left on device" in captured["log_tail"]
    assert "parse_notes" in captured and "root_error" in captured


def test_dataproc_error_text_reaches_incident_query() -> None:
    """Dataproc error_text feeds the incident-match query despite root_error being None."""
    parsed = ParsedFailure(
        dag_id="url_pattern_identification",
        task_id="run_spark_pattern_identification",
        engine="dataproc",
        batch_id="url-pat-id-1",
    )
    diag = {
        "identity": {"dag_id": parsed.dag_id, "task_id": parsed.task_id},
        "engine": "dataproc",
        "airflow_signature": None,
        "spark": {
            "engine": "dataproc",
            "state": "CANCELLED",
            "state_message": None,
            "error_text": "java.lang.OutOfMemoryError: Java heap space",
            "notes": [],
        },
        "spark_outcome": "failed",
        "orchestration_only": False,
        "root_signature": None,
        "root_error": None,
        "batch_id": parsed.batch_id,
        "dbx_run_id": None,
        "job_id": None,
    }
    queries: list[str] = []

    def _capture_match(dag: object, task: object, query: str) -> list:
        queries.append(query)
        return []

    orig = (orchestrate.parse_log_file, orchestrate.diagnose, orchestrate.match_incidents)
    orchestrate.parse_log_file = lambda p: parsed
    orchestrate.diagnose = lambda p: diag
    orchestrate.match_incidents = _capture_match
    try:
        orchestrate.investigate("unused.log", use_llm=False)
    finally:
        orchestrate.parse_log_file, orchestrate.diagnose, orchestrate.match_incidents = orig
    assert "OutOfMemoryError" in queries[0]


if __name__ == "__main__":
    for fn in [
        test_api_error_returns_none_not_stub,
        test_import_missing_returns_note,
        test_payload_truncation_is_structure_aware,
        test_output_capped_at_500,
        test_max_tokens_thinking_only_returns_note,
        test_orchestrate_prefers_deterministic_on_llm_failure,
        test_slack_failure_classifies_without_an_llm_call,
        test_orchestrate_matcher_crash_degrades_to_no_matches,
        test_evidence_includes_log_tail_and_parse_notes,
        test_dataproc_error_text_reaches_incident_query,
    ]:
        fn()
    print("OK - synth + orchestrate tests passed")
