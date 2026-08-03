"""Offline unit tests for the deterministic signature classifier (no network).

Run: python3 -m airflow_debugger.tests.test_signatures  (or via pytest).
The live INC-009 / INC-005 integration checks live in the ticket, not here.
"""

from __future__ import annotations

from airflow_debugger.signatures import classify

CASES = [
    # (label, error_text, expected_key)
    (
        "inc009_table_exists",
        "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view "
        "`prod`.`mntn_matched_reporting`.`targeted_signal` because it already exists. SQLSTATE: 42P07",
        "table_or_view_already_exists",
    ),
    (
        "executor_oom",
        "Container killed by YARN for exceeding memory limits. 10.4 GB of 10 GB physical memory used",
        "executor_oom_yarn",
    ),
    ("exit_137", "Container killed on request. Exit code is 137", "executor_oom_yarn"),
    (
        "shuffle_fetch",
        "org.apache.spark.shuffle.FetchFailedException: Failed to connect to host/1.2.3.4:7337",
        "shuffle_fetch_failure",
    ),
    (
        "generic_analysis",
        "AnalysisException: [UNRESOLVED_COLUMN] cannot resolve `foo` given input columns",
        "analysis_exception",
    ),
    (
        "pod_404",
        "(404) Reason: Not Found ... pods 'x' not found during istio check",
        "pod_evicted_404",
    ),
    (
        "spot",
        "Cluster terminated. Reason: PREEMPTIBLE_WITH_FALLBACK_GCP instance was preempted",
        "spot_preemption",
    ),
]


def test_classifier_cases() -> None:
    """Each taxonomy case matches its expected signature key."""
    for label, text, expected in CASES:
        m = classify(text)
        assert m is not None, f"{label}: expected a match, got None"
        assert m.key == expected, f"{label}: expected {expected}, got {m.key}"


def test_empty_returns_none() -> None:
    """Empty/None input yields no match."""
    assert classify("") is None
    assert classify(None) is None  # type: ignore[arg-type]


def test_table_exists_beats_generic_analysis() -> None:
    """The specific 42P07 fingerprint wins over the generic AnalysisException."""
    m = classify("AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] ... SQLSTATE: 42P07")
    assert m is not None and m.key == "table_or_view_already_exists"


if __name__ == "__main__":
    test_classifier_cases()
    test_empty_returns_none()
    test_table_exists_beats_generic_analysis()
    print(f"OK — {len(CASES)} classifier cases + edge cases passed")
