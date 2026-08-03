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
    # --- corpus gap classes (INC-001..INC-008) ---
    (
        "inc004_late_data",
        "AnalysisException [PATH_NOT_FOUND]: gs://mntn-data-archive-prod/ipdsc_geo/dt=2026-07-29",
        "path_not_found_late_data",
    ),
    (
        "inc003_vertex_param",
        "ValueError: The pipeline parameter reference_date is not found in the pipeline "
        "job input definitions",
        "vertex_param_contract",
    ),
    (
        "inc008_stockout",
        "RuntimeError: Job failed with: code: 14 ... the zone does not have enough resources "
        "available to fulfill the request",
        "cluster_create_stockout",
    ),
    (
        "inc008_quota",
        "Insufficient N2_CPUS quota. Requested 4672 but only 328 available in us-central1",
        "quota_exhaustion",
    ),
    (
        "inc007_openai_quota",
        "invalid_request_error: You have exceeded your file storage quota. Projects are "
        "limited to 2.5TB of files.",
        "openai_file_quota",
    ),
    (
        "inc001_sensor_timeout",
        "airflow.exceptions.AirflowSensorTimeout: Snap. Time is up. precondition_bombora poked 216x",
        "sensor_timeout",
    ),
    (
        "inc007_external_task",
        "ExternalTaskFailedError: The external task product_categorization in state upstream_failed",
        "external_task_failed",
    ),
    (
        # First live-fire catch (2026-08-02, vertical_classification_api): a dbt data-quality test.
        "live_dbt_test_fail",
        "Completed with 1 error ... Failure in test ddp_vertical_classification_api__failure_rate "
        "... Got 5580 results, configured to fail if >5000",
        "dbt_test_failure",
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


def test_path_not_found_beats_generic_analysis() -> None:
    """A PATH_NOT_FOUND AnalysisException is late-data, not a generic query error."""
    m = classify("AnalysisException [PATH_NOT_FOUND]: gs://bucket/ipdsc_geo/dt=2026-07-29")
    assert m is not None and m.key == "path_not_found_late_data"


def test_pod_evict_not_mistaken_for_sensor_timeout() -> None:
    """A pod-evict 'served logs timed out' stays pod_evicted_404, not sensor_timeout."""
    m = classify("Could not read served logs: timed out ... pods 'x' not found during istio check")
    assert m is not None and m.key == "pod_evicted_404"


if __name__ == "__main__":
    test_classifier_cases()
    test_empty_returns_none()
    test_table_exists_beats_generic_analysis()
    test_path_not_found_beats_generic_analysis()
    test_pod_evict_not_mistaken_for_sensor_timeout()
    print(f"OK — {len(CASES)} classifier cases + edge cases passed")
