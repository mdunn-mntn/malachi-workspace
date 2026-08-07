"""IMP-032 perf-class handoff: escalation gating, local profiling, report rendering."""

from __future__ import annotations

import os

from airflow_debugger.perf_profile import PERF_SIGNATURES, profile, should_profile
from airflow_debugger.report import build_troubleshooting

FIXTURE = os.path.join(
    os.path.dirname(__file__), "..", "..", "airflow_optimizer", "tests", "fixtures",
    "eventlog.zstd",
)


def _diag(sig_key: str, has_log: bool = True, uri: str | None = None) -> dict:
    return {
        "root_signature": {"key": sig_key, "sig_class": "x", "likely_cause": "y"},
        "spark": {"engine": "dataproc", "has_event_log": has_log, "event_log_uri": uri},
        "engine": "dataproc",
    }


def test_gate_fires_only_on_perf_signatures_with_a_log() -> None:
    """The escalation gate fires only for perf-shaped signatures that have an event log."""
    for key in PERF_SIGNATURES:
        assert should_profile(_diag(key))
    assert not should_profile(_diag("gcs_list_timeout"))  # non-perf class: never escalate
    assert not should_profile(_diag("ttl_exceeded", has_log=False))
    assert not should_profile({"root_signature": {"key": "ttl_exceeded"}, "spark": None})


def test_profile_runs_optimizer_on_local_event_log() -> None:
    """A perf failure with a local event log yields optimizer findings."""
    res = profile(_diag("ttl_exceeded", uri=FIXTURE))
    assert res is not None and res["error"] is None
    assert res["findings"], "the skewed fixture must yield findings"
    assert {"key", "impact", "title", "evidence", "fix", "rec_type"} <= set(res["findings"][0])


def test_profile_degrades_to_note_when_log_unreachable() -> None:
    """An unreachable log degrades to an error note, never an exception."""
    res = profile(_diag("driver_oom", uri="/nonexistent/path/app-123"))
    assert res is not None and res["findings"] == []
    assert "not reachable" in res["error"]


def test_troubleshooting_renders_perf_section() -> None:
    """The troubleshooting pack renders the perf section (findings or the unavailable note)."""
    diag = _diag("ttl_exceeded", uri=FIXTURE)
    diag["perf_profile"] = profile(diag)
    pack = build_troubleshooting(diag)
    assert "Perf profile (event log)" in pack

    diag2 = _diag("ttl_exceeded", uri="/nonexistent/x")
    diag2["perf_profile"] = profile(diag2)
    pack2 = build_troubleshooting(diag2)
    assert "Perf profile unavailable" in pack2


def test_non_perf_failure_gets_no_profile() -> None:
    """Non-perf failure classes never trigger the profiler."""
    assert profile(_diag("executor_lost")) is None
