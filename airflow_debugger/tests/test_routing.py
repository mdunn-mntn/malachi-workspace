"""Routing to the system that owns the cause (IMP-055): Vertex pipelines + ExternalTaskSensor.

Both log shapes are verbatim from the corpus. Neither carries its cause, and both print
their one distinguishing line on SUCCESSFUL runs too, so the regression that matters is
the negative one: extracting a handle must never become a verdict on a green run.
"""

from __future__ import annotations

from dataclasses import asdict

from airflow_debugger import external_task_rca, vertex_rca
from airflow_debugger.parse import diagnose, parse_log
from airflow_debugger.signatures import classify

# 2026-08-20 fangorn_hhid_inference_pipeline_run/challenger_inference_pipeline, try 1.
VERTEX_LOG = """2026-08-20T21:52:44.765123Z [info] task.stdout Submitting Vertex AI Pipeline:
2026-08-20T21:52:44.765284Z [info] task.stdout   Pipeline: fangorn_hhid_challenger_inference_pipeline
2026-08-20T21:52:44.765380Z [info] task.stdout   Reference Date: 2026-08-20
2026-08-20T21:52:53.899761Z [info] task.stdout Pipeline Run URL: https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-hhid-challenger-inference-pipeline-20260820215253?project=mntn-targeting-prj-prod
2026-08-20T22:24:40.992548Z [error] task Task failed with exception
"""

# The same DAG on a day it SUCCEEDED. Identical up to the outcome.
VERTEX_GREEN = VERTEX_LOG.replace(
    "2026-08-20T22:24:40.992548Z [error] task Task failed with exception",
    "2026-08-20T22:24:40.992548Z [info] task Task succeeded",
)

# 2026-08-19 keyword_ddp_reporting/wait_for_product_categorization, try 1.
SENSOR_LOG = """2026-08-19T15:00:11.965872Z [info] airflow.task.operators.airflow.providers.standard.sensors.external_task.ExternalTaskSensor Poking for tasks ['batch_post.product_categorization'] in dag mntn_match_incrementals_fetch on 2026-08-18T09:00:00+00:00 ...
2026-08-19T15:00:12.045826Z [error] task Task failed with exception
"""

# The real INC-024 chain: the leaf component error names the ml_job replica, percent-encoded.
LEAF_ERROR = (
    "The replica workerpool0-0 exited with a non-zero status of 1. To find out more about "
    "why your job exited please check the logs: https://console.cloud.google.com/logs/viewer"
    "?project=401977096985&resource=ml_job%2Fjob_id%2F4569671626135699456&advancedFilter="
    "resource.type%3D%22ml_job%22%0Aresource.labels.job_id%3D%224569671626135699456%22"
)

ML_JOB_MESSAGES = "\n".join(
    [
        '{"attrs": {"tag": "workerpool0-0"}, "levelname": "ERROR", "message": '
        '"RuntimeError: Job index 0 (last job_id: b929b772-c546-476f-939f-1526b5b523a1) '
        'failed 3 times. Aborting."}',
        '{"attrs": {"tag": "workerpool0-0"}, "levelname": "ERROR", "message": '
        '"Traceback (most recent call last):"}',
        '{"attrs": {"tag": "workerpool0-0"}, "levelname": "INFO", "message": '
        '"[KFP Executor INFO]: Resubmitted job index 0 as 4a3e804a-82dc-4587-aa2d-dc6c2b1c2d66"}',
        '{"attrs": {"tag": "workerpool0-0"}, "levelname": "INFO", "message": '
        '"[KFP Executor INFO]: Submitting 1 parallel jobs to cluster fangorn-hhid-challenger-7bea6d2b"}',
    ]
)


def test_vertex_run_url_yields_a_handle_not_a_verdict() -> None:
    """The Run URL is the routing handle. It must never classify anything by itself."""
    p = parse_log(VERTEX_LOG)
    assert p.engine == "vertex"
    assert p.vertex_run_id == "fangorn-hhid-challenger-inference-pipeline-20260820215253"
    assert (p.vertex_project, p.vertex_location) == ("mntn-targeting-prj-prod", "us-central1")
    assert p.airflow_signature is None, "the Airflow log carries no cause; it must stay unclassified"


def test_the_green_run_prints_the_same_url_and_is_still_unclassified() -> None:
    """325 false positives came from a line that was true of the LOG, not of the FAILURE."""
    green = parse_log(VERTEX_GREEN)
    assert green.vertex_run_id == parse_log(VERTEX_LOG).vertex_run_id
    assert green.airflow_signature is None


def test_leaf_error_names_the_replica_to_fetch_next() -> None:
    """The next hop is percent-encoded inside the component error; both shapes must parse."""
    assert vertex_rca._ml_job_id(LEAF_ERROR) == "4569671626135699456"
    assert vertex_rca._ml_job_id("no link here") is None


def test_the_aborting_attempt_is_tried_first() -> None:
    """The executor retries an index 3x; only the last attempt's driver output has the cause."""
    jobs = vertex_rca._dataproc_jobs(ML_JOB_MESSAGES)
    assert jobs[0] == "b929b772-c546-476f-939f-1526b5b523a1"
    assert "4a3e804a-82dc-4587-aa2d-dc6c2b1c2d66" in jobs


def test_failed_leaf_excludes_the_root_dag_node() -> None:
    """The root node only restates its children; treating it as the leaf loses the real step."""
    run = "fangorn-hhid-challenger-inference-pipeline-20260820215253"
    job = {
        "displayName": run,
        "jobDetail": {
            "taskDetails": [
                {"taskName": run, "state": "FAILED", "error": {"message": "the DAG failed"}},
                {"taskName": "create-dataproc-cluster", "state": "SUCCEEDED"},
                {"taskName": "submit-parallel-inference-jobs", "state": "FAILED",
                 "error": {"message": LEAF_ERROR}},
            ]
        },
    }  # fmt: skip
    leaves = vertex_rca._failed_leaves(job, run)
    assert [t["taskName"] for t in leaves] == ["submit-parallel-inference-jobs"]


def test_the_driver_output_error_classifies_as_the_missing_alias() -> None:
    """INC-024's real cause, five layers down, must land on a signature and not a shrug."""
    driver = (
        'File "/tmp/x/run_challenger_inference.py", line 35, in resolve_model_uri\n'
        "    raise ValueError(f\"No version found with alias pattern '{alias}-v*' for model\")\n"
        "ValueError: No version found with alias pattern 'challenger-v*' for model "
        "'fangorn-hhid-xgboost'"
    )
    m = classify(driver)
    assert m is not None and m.key == "model_alias_not_found"
    assert m.programmatic_fix == "no", "a retry cannot recreate a registry alias"


def test_the_alias_error_never_fires_on_the_airflow_log() -> None:
    """The signature belongs to the driver output; the Airflow log must not pick it up."""
    assert classify(VERTEX_LOG) is None
    assert classify(VERTEX_GREEN) is None


def test_sensor_poke_line_yields_the_target_identity() -> None:
    """Target dag, tasks, logical date and the moment the sensor gave up all come from the log."""
    p = parse_log(SENSOR_LOG)
    assert p.external_dag_id == "mntn_match_incrementals_fetch"
    assert p.external_task_ids == ["batch_post.product_categorization"]
    assert p.external_logical_date == "2026-08-18T09:00:00+00:00"
    assert p.failed_at == "2026-08-19T15:00:12.045826Z"


def test_a_target_that_recovered_later_is_not_reported_as_healthy() -> None:
    """IMP-053 again: the API answers with the state NOW, and the sensor failed hours ago."""
    assert external_task_rca._moved_on_after("2026-08-19T22:11:22.851593Z", "2026-08-19T15:00:12Z")
    assert not external_task_rca._moved_on_after(
        "2026-08-19T14:00:00.000000Z", "2026-08-19T15:00:12Z"
    )
    # never finished at all -> it certainly had not finished when the sensor gave up
    assert external_task_rca._moved_on_after(None, "2026-08-19T15:00:12Z")
    # no failure timestamp -> cannot claim staleness either way
    assert not external_task_rca._moved_on_after("2026-08-19T22:11:22Z", None)


def test_a_routed_verdict_is_high_confidence_not_unclassified() -> None:
    """Routing resolves the failure, so the report must not still call it a taxonomy gap."""
    ev = external_task_rca.ExternalTaskEvidence(
        dag_id="mntn_match_incrementals_fetch",
        task_ids=["batch_post.product_categorization"],
        state="failed",
        states={"batch_post.product_categorization": "failed"},
        signature={
            "key": "external_task_target_failed",
            "sig_class": "upstream/external-task-failed",
            "likely_cause": "diagnose the target task",
            "programmatic_fix": "no",
            "matched_on": "Airflow API state",
        },
    )
    p = parse_log(SENSOR_LOG)
    p.airflow_signature = None
    diag = {
        "identity": {"dag_id": p.dag_id, "task_id": p.task_id},
        "engine": p.engine,
        "spark": asdict(ev),
        "root_signature": ev.signature,
    }
    from airflow_debugger.report import build_report

    report = build_report(diag)
    assert "upstream/external-task-failed" in report
    assert "unclassified" not in report


def test_diagnose_does_not_route_a_log_with_no_handle() -> None:
    """No handle means no network call: an offline sweep over 1000 logs must stay offline."""
    diag = diagnose(parse_log("2026-08-19T01:00:00Z [error] task Task failed with exception\n"))
    assert diag["spark"] is None
    assert diag["spark_outcome"] == "none"


# Verbatim from INC-025's ml_job replica log.
_MASKING_404 = (
    "google.api_core.exceptions.NotFound: 404 Not found: Cluster "
    "projects/mntn-targeting-prj-prod/regions/us-central1/clusters/fangorn-challenger-a483e22d\n"
    "    raise exceptions.from_grpc_error(exc) from exc\n"
)


def test_the_cleanup_404_is_recognised_as_a_mask_not_a_cause() -> None:
    """A missing cluster at the replica layer is a symptom; the real fault is one hop deeper."""
    m = vertex_rca._DELETE_404_RE.search(_MASKING_404)
    assert m
    assert m.group(1) == "fangorn-challenger-a483e22d"


def test_the_create_refusal_beats_the_cleanup_404() -> None:
    """The audit log's CreateCluster status is what classifies, and it classifies as quota."""
    calls = []

    def _fake(filt: str, project: str, limit: int = 0, field: str = "") -> tuple[str, None]:
        calls.append((filt, field))
        return (
            "Multiple validation errors:\n"
            " - Insufficient 'N2_CPUS' quota. Requested 4672.0, available 328.0.",
            None,
        )

    orig = vertex_rca.logging_messages
    vertex_rca.logging_messages = _fake
    try:
        text, err = vertex_rca._cluster_create_error("fangorn-challenger-a483e22d", "p")
    finally:
        vertex_rca.logging_messages = orig

    assert err is None
    assert "N2_CPUS" in text
    assert calls[0][1] == "protoPayload.status.message"
    assert "CreateCluster" in calls[0][0]
    assert classify(text).key == "quota_exhaustion"


def test_a_replica_404_with_no_audit_entry_stays_at_the_replica_layer() -> None:
    """No audit entry must leave the honest symptom in place, never a blank verdict."""
    orig = vertex_rca.logging_messages
    vertex_rca.logging_messages = lambda *a, **k: ("", "permission denied")
    try:
        text, err = vertex_rca._cluster_create_error("nope", "p")
    finally:
        vertex_rca.logging_messages = orig
    assert text is None
    assert err == "permission denied"


if __name__ == "__main__":
    for name, fn in sorted(dict(globals()).items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("OK - vertex + external-task routing tests passed")
