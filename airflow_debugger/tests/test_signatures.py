"""Offline unit tests for the deterministic signature classifier (no network).

Run: python3 -m airflow_debugger.tests.test_signatures  (or via pytest).
The live INC-009 / INC-005 integration checks live in the ticket, not here.
"""

from __future__ import annotations

import re

from airflow_debugger.signatures import SIGNATURES, classify

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
        # Real prod (2026-08-06, materialize_mntn_select): driver-side GCS list of the full
        # augmentor_log prefix timed out; both tries died ~19 min in (INC-012).
        "inc012_gcs_list_timeout",
        "Caused by: java.io.IOException: Error listing gs://mntn-data-archive-prod/augmentor_log/"
        "region=\nCaused by: java.net.SocketTimeoutException: Read timed out",
        "gcs_list_timeout",
    ),
    (
        "inc007_external_task",
        "ExternalTaskFailedError: The external task product_categorization in state upstream_failed",
        "external_task_failed",
    ),
    (
        # Real prod (2026-08-05, hashed_email_ds_26_signals/wait_fpa): the external task was SKIPPED
        # (producer short-circuited on a missing Predactiv/DS26 file) yet Airflow emits the identical
        # "... failed." message. Same signature key; disambiguation needs the external task's state.
        "inc011_external_task_skipped",
        "ExternalTaskFailedError: Some of the external tasks ['dsid26_predactiv_processing'] in DAG "
        "fpa_site_visit_batch_serverless failed.",
        "external_task_failed",
    ),
    (
        # Real prod (2026-08-02, tpa_ipdsc_export/ipdsc_ds_67): DS67 model code bug - a bound
        # method passed instead of its call result, so the bucket name is the method repr.
        "prod_ds67_bound_method",
        "IllegalArgumentException: Invalid GCS bucket name '<bound method BaseModel.write_location "
        "of <class __main__.DS67>>': bucket name must contain only 'a-z0-9_.-' characters.",
        "invalid_output_path_config",
    ),
    (
        # Real prod downstream symptom (2026-08-03, tpa_mntn_id_export): missing ipdsc/ds67 partition.
        "prod_missing_partition",
        "FileNotFoundError: Missing required ipdsc partition at "
        "gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67",
        "path_not_found_late_data",
    ),
    (
        # First live-fire catch (2026-08-02, vertical_classification_api): a dbt data-quality test.
        "live_dbt_test_fail",
        "Completed with 1 error ... Failure in test ddp_vertical_classification_api__failure_rate "
        "... Got 5580 results, configured to fail if >5000",
        "dbt_test_failure",
    ),
    (
        # INC-012 mixed driver blob (desc log order): benign scale-down decommissions must not
        # steal the GCS-list-timeout verdict.
        "inc012_mixed_driver_blob",
        "Caused by: java.net.SocketTimeoutException: Read timed out\n"
        "Caused by: java.io.IOException: Error listing "
        "gs://mntn-data-archive-prod/augmentor_log/region=\n"
        "ERROR TaskSchedulerImpl: Lost executor 7 (10.128.0.23:35249): "
        "Executor decommission finished: spark scale down\n"
        "ERROR TaskSchedulerImpl: Lost executor 3 (10.128.0.11:41213): "
        "Executor decommission finished: spark scale down",
        "gcs_list_timeout",
    ),
    (
        # A preemption diagnostic inside an ExecutorLostFailure line is the specific cause.
        "preempted_executor_lost",
        "ExecutorLostFailure (executor 1 exited caused by one of the running tasks) "
        "Reason: Container container_1 on host wk-2 was preempted.",
        "spot_preemption",
    ),
    (
        "real_executor_lost",
        "ERROR TaskSchedulerImpl: Lost executor 5 (10.0.0.5): worker lost",
        "executor_lost",
    ),
    (
        # Canonical stage-failure relay: executor-side shuffle OOM, not a driver OOM.
        "executor_shuffle_oom_stage_failure",
        "Job aborted due to stage failure ... SparkOutOfMemoryError: Unable to acquire "
        "65536 bytes of memory\nDriver stacktrace:\n"
        " at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages\n"
        "Caused by: org.apache.spark.memory.SparkOutOfMemoryError",
        "shuffle_oom",
    ),
    ("driver_heap_oom", "java.lang.OutOfMemoryError: Java heap space", "driver_oom"),
    ("ttl_cancelling_batch", "Cancelling batch as ttl exceeded", "ttl_exceeded"),
    ("ttl_deadline_with_batch_context", "Batch expired: DEADLINE_EXCEEDED", "ttl_exceeded"),
    (
        # SocketTimeout bound to a nearby gs:// reference still reads as a GCS list timeout.
        "gcs_socket_timeout_with_context",
        "WARN GoogleCloudStorageFileSystem: list of gs://mntn-data-archive-prod/augmentor_log/ "
        "failed\nCaused by: java.net.SocketTimeoutException: Read timed out",
        "gcs_list_timeout",
    ),
    (
        # Airflow-3 shape (INC-015 drift logs): the [error] line is empty and the payload
        # survives only in the slack_messages dict repr, where \n is two literal characters.
        "vertex_code9_repr_escapes",
        "'exception': RuntimeError('Job failed with:\\ncode: 9\\nmessage: \" The DAG failed "
        "because some tasks failed. The failed tasks are: [submit-daily-drift-job].; Job "
        "(project_id = mntn-targeting-prj-prod, job_id = 3145311409647124480) is failed due "
        "to the above error.\"\\n')",
        "vertex_pipeline_task_failed",
    ),
    (
        # Older incident capture (INC-002): same wrapper on real newlines.
        "vertex_code9_real_newlines",
        "RuntimeError: Job failed with:\ncode: 9\nmessage: \" The DAG failed because some "
        "tasks failed. The failed tasks are: [create-dataproc-cluster].; Job (project_id = "
        "mntn-targeting-prj-prod, job_id = 951702149350293504) is failed due to the above "
        "error.\"",
        "vertex_pipeline_task_failed",
    ),
    (
        # INC-016/017: the retry reattached to the already-failed batch (id cached in XCom).
        "batch_id_already_exists",
        "airflow.providers.google.cloud.operators.dataproc.DataprocCreateBatchOperator "
        "Batch with given id already exists.",
        "batch_id_attach_trap",
    ),
    (
        # INC-020: IAM 503 before submission, so no batch was ever created.
        "impersonated_credentials_503",
        "'exception': ServiceUnavailable('Getting metadata from plugin failed with error: "
        "(\\'Unable to acquire impersonated credentials\\', \\'{\\\\n  \"error\": {\\\\n    "
        "\"code\": 503,\\\\n    \"status\": \"UNAVAILABLE\"}}\\')')",
        "impersonation_unavailable",
    ),
    (
        # The TASK's own exception is the Slack error (set_gaclid_enabled_flag/send_notification).
        "slack_channel_not_found",
        "'exception': SlackApiError(\"The request to the Slack API failed. "
        "(url: https://slack.com/api/chat.postMessage)\\nThe server responded with: "
        "{'ok': False, 'error': 'not_in_channel'}\")}",
        "slack_notify_failed",
    ),
    (
        "airflow_execution_timeout",
        "2026-08-04T17:15:15.351435Z [error] task Process timed out\n"
        "2026-08-04T17:15:15.654932Z [error] task Task failed with exception",
        "task_execution_timeout",
    ),
    (
        # The wrapper names no cause: it lives in the batch's own driver output.
        "dataproc_agent_boilerplate_only",
        "'exception': AirflowException(\"Batch job mntn-select-2026-08-06-1786049114 failed "
        "with error: Google Cloud Dataproc Agent reports job failure. If logs are available, "
        "they can be found at:\\nhttps://console.cloud.google.com/dataproc/batches/us-central1"
        "/mntn-select-2026\")",
        "downstream_job_no_local_cause",
    ),
    (
        # Same class via KubernetesPodOperator; \n here is two literal characters.
        "pod_returned_a_failure",
        "'exception': AirflowException('Pod pre-cache-verticals-w9e9fd2v returned a failure."
        "\\nremote_pod: {\\'api_version\\': None,\\n \\'kind\\': None}')",
        "downstream_job_no_local_cause",
    ),
    (
        # dbt model raised at runtime; the real exception is in the traceback below it.
        "dbt_model_runtime_crash",
        "Completed with 1 error, 0 partial successes, and 0 warnings:\n"
        "  Runtime Error in model ddp_vertical_classification_api "
        "(models/vertical_categorization/ddp_vertical_classification_api.py)\n"
        "  Python model failed with traceback as:\n"
        "  ValueError: Too many signals to process 176052364",
        "dbt_model_runtime_error",
    ),
]

# Real prod log shape (2026-08-06 ddp_vertical_classification_api): a dbt python model
# RUNTIME crash, whose summary still says 'Completed with 1 error'. Must NOT classify as
# dbt_test_failure (wrong class, wrong fixability).
DBT_RUNTIME_CRASH = (
    "01:41:07  1 of 1 ERROR creating python table model ml.ddp_vertical_classification_api "
    "[ERROR in 559.22s]\n"
    "01:41:08  Completed with 1 error, 0 partial successes, and 0 warnings:\n"
    "01:41:08    Runtime Error in model ddp_vertical_classification_api "
    "(models/vertical_categorization/ddp_vertical_classification_api.py)\n"
    "  Python model failed with traceback as:\n"
    "  ValueError: Too many signals to process 169643477 for period between "
    "2026-08-06T00:30:00+00:00 and 2026-08-06T01:30:00+00:00\n"
    "01:41:08  Done. PASS=0 WARN=0 ERROR=1 SKIP=0 TOTAL=1"
)


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


def test_order_integrity() -> None:
    """Every case's expected key is the FIRST match across the full ordered list."""
    for label, text, expected in CASES:
        hits = [s.key for s in SIGNATURES if re.search(s.pattern, text, re.IGNORECASE | re.DOTALL)]
        assert hits, f"{label}: no signature matched"
        assert hits[0] == expected, f"{label}: {hits[0]} steals the match from {expected} ({hits})"


def test_benign_scale_down_decommission_no_match() -> None:
    """A blob of only benign 'spark scale down' decommissions is not executor_lost."""
    blob = (
        "ERROR TaskSchedulerImpl: Lost executor 7 (10.128.0.23:35249): "
        "Executor decommission finished: spark scale down\n"
        "ERROR TaskSchedulerImpl: Lost executor 3 (10.128.0.11:41213): "
        "Executor decommission finished: spark scale down"
    )
    assert classify(blob) is None


def test_dbt_runtime_crash_not_test_failure() -> None:
    """A dbt model runtime crash ('Completed with 1 error') is not a data-quality test."""
    m = classify(DBT_RUNTIME_CRASH)
    assert m is not None and m.key == "dbt_model_runtime_error", f"got {m and m.key}"


def test_slack_callback_noise_does_not_steal_the_real_cause() -> None:
    """The Slack notifier error in a failure CALLBACK must not become the root cause.

    Any DAG that posts to Slack emits this after the task has already failed; on
    ga4 and url_pattern_identification it was outranking the real exception.
    """
    blob = (
        "[error] task Task failed with exception\n"
        "'exception': AirflowException('Pod ga4-pod-h7hjg1kb returned a failure."
        "\\nremote_pod: {}')\n"
        "[error] airflow.providers.slack.notifications.slack.SlackNotifier Failed to send "
        "notification (sync): The request to the Slack API failed.\n"
        "The server responded with: {'ok': False, 'error': 'channel_not_found'}\n"
        "[error] task Failed to run task callback\n"
    )
    m = classify(blob)
    assert m is not None and m.key == "downstream_job_no_local_cause", f"got {m and m.key}"


def test_generic_socket_timeout_not_gcs() -> None:
    """A JDBC/network read timeout with no GCS context is not gcs_list_timeout."""
    blob = (
        "java.sql.SQLException: could not read response\n"
        "Caused by: java.net.SocketTimeoutException: Read timed out\n"
        " at java.net.SocketInputStream.socketRead0"
    )
    assert classify(blob, engine="databricks") is None
    assert classify(blob) is None


def test_grpc_deadline_not_ttl() -> None:
    """A client-side gRPC DEADLINE_EXCEEDED poll timeout is not a TTL kill."""
    blob = (
        "grpc._channel._InactiveRpcError: status = StatusCode.DEADLINE_EXCEEDED, "
        'details = "Deadline Exceeded" while calling google.cloud.dataproc get_batch'
    )
    assert classify(blob) is None


def test_gcp_capacity_signatures_reachable_from_databricks() -> None:
    """GCP quota/stockout also terminates Databricks-on-GCP clusters (engine='any')."""
    m = classify(
        "Cluster terminated. Reason: GCP_QUOTA_EXCEEDED. "
        "Insufficient CPU quota in region us-central1.",
        engine="databricks",
    )
    assert m is not None and m.key == "quota_exhaustion"
    m = classify(
        "Cluster terminated. Reason: ZONE_RESOURCE_POOL_EXHAUSTED in us-central1-a.",
        engine="databricks",
    )
    assert m is not None and m.key == "cluster_create_stockout"


if __name__ == "__main__":
    test_classifier_cases()
    test_empty_returns_none()
    test_table_exists_beats_generic_analysis()
    test_path_not_found_beats_generic_analysis()
    test_pod_evict_not_mistaken_for_sensor_timeout()
    test_order_integrity()
    test_benign_scale_down_decommission_no_match()
    test_dbt_runtime_crash_not_test_failure()
    test_slack_callback_noise_does_not_steal_the_real_cause()
    test_generic_socket_timeout_not_gcs()
    test_grpc_deadline_not_ttl()
    test_gcp_capacity_signatures_reachable_from_databricks()
    print(f"OK — {len(CASES)} classifier cases + edge cases passed")
