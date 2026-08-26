"""Routing to the system that owns the cause (IMP-055): Vertex pipelines + ExternalTaskSensor.

Both log shapes are verbatim from the corpus. Neither carries its cause, and both print
their one distinguishing line on SUCCESSFUL runs too, so the regression that matters is
the negative one: extracting a handle must never become a verdict on a green run.
"""

from __future__ import annotations

from dataclasses import asdict

from airflow_debugger import external_task_rca, masks, vertex_rca
from airflow_debugger.parse import diagnose, parse_log
from airflow_debugger.report import build_report
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
    assert p.airflow_signature is None, (
        "the Airflow log carries no cause; it must stay unclassified"
    )


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
    mask = masks.detect(_MASKING_404)
    assert mask is not None
    assert mask.key == "dataproc_cleanup_delete_404"
    assert mask.resolver == "vertex_rca._cluster_create_error"


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


def test_every_mask_declares_what_it_hides_and_where_to_look() -> None:
    """A mask with no next hop is worse than no mask: it stops the chain and says nothing."""
    assert masks.MASKS
    keys = [m.key for m in masks.MASKS]
    assert len(keys) == len(set(keys))
    for m in masks.MASKS:
        assert m.hides and m.next_hop, m.key
        assert masks.note(m).startswith("This is not the cause")
        assert m.next_hop in masks.note(m)


def test_the_notifier_failure_is_a_mask_not_the_task_failure() -> None:
    """INC corpus: 7 logs where the on-failure Slack callback is the last thing that raises."""
    mask = masks.detect("slack_sdk.errors.SlackApiError: channel_not_found")
    assert mask is not None
    assert mask.key == "slack_notifier_failed"


def test_a_reattached_batch_is_a_mask_not_a_fresh_fault() -> None:
    """The retry inherits the first attempt's error; reporting it as new hides the real one."""
    mask = masks.detect("AlreadyExists: 409 Batch with given id already exists")
    assert mask is not None
    assert mask.key == "dataproc_batch_reattach"


def test_a_real_error_is_not_a_mask() -> None:
    """The registry must stay narrow: a genuine cause has to pass straight through."""
    assert masks.detect("java.lang.OutOfMemoryError: Java heap space") is None
    assert masks.detect("Insufficient 'N2_CPUS' quota. Requested 4672.0") is None
    assert masks.detect(None) is None
    assert masks.detect("") is None


def test_the_report_refuses_to_present_a_mask_as_the_verdict() -> None:
    """The whole point: a masked verdict says so in the report, never stands alone."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "root_error": _MASKING_404,
        "root_signature": {},
    }
    out = build_report(diag)
    assert "This is not the cause" in out
    assert "audit log" in out


def test_an_empty_stub_names_the_task_that_actually_failed() -> None:
    """ "Diagnose the upstream task" is correct and useless; the reader wants to know which one."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "no_error_text": True,
        "ti_state": "upstream_failed",
        "upstream_failed_tasks": ["tpa_export"],
        "root_signature": {},
    }
    out = build_report(diag)
    assert "`tpa_export`" in out
    assert "never ran" in out


def test_many_culprits_are_capped_and_counted() -> None:
    """A fan-in can fail on twenty tasks; a report that lists all of them is unreadable."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "no_error_text": True,
        "ti_state": "upstream_failed",
        "upstream_failed_tasks": [f"task_{i}" for i in range(9)],
        "root_signature": {},
    }
    out = build_report(diag)
    assert "+6 more" in out
    assert "task_8" not in out


def test_the_stub_still_reports_when_the_lookup_fails() -> None:
    """An unreachable API must degrade to the old wording, never blank the verdict."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "no_error_text": True,
        "ti_state": "upstream_failed",
        "upstream_failed_tasks": [],
        "root_signature": {},
    }
    out = build_report(diag)
    assert "diagnose the upstream task that failed" in out


def test_the_lookup_excludes_the_asking_task_itself() -> None:
    """A task cannot be its own upstream cause."""
    calls = {}

    class _Api:
        @staticmethod
        def resolve_bearer() -> str:
            return "tok"

        @staticmethod
        def list_task_instances_in_run(
            base: str, token: str, dag_id: str, run_id: str
        ) -> list[dict]:
            calls["run_id"] = run_id
            return [
                {"task_id": "me", "state": "upstream_failed"},
                {"task_id": "producer", "state": "failed"},
                {"task_id": "other", "state": "success"},
            ]

    orig_api, orig_base = external_task_rca._api, external_task_rca._resolve_base
    external_task_rca._api = lambda: _Api
    external_task_rca._resolve_base = lambda: "https://x/api/v2"
    try:
        failed, note = external_task_rca.upstream_failures("d", "run-1", "me")
    finally:
        external_task_rca._api, external_task_rca._resolve_base = orig_api, orig_base

    assert failed == ["producer"]
    assert note is None
    assert calls["run_id"] == "run-1"


def test_the_run_is_matched_on_state_not_merely_on_containing_the_task() -> None:
    """An hourly DAG's day is mostly green; matching the first run found reports a SUCCESS run.

    Verbatim shape from 2026-08-21 `vertical_classification_api`, which had 21 runs that day and
    only one failure. Matching on presence alone picked a green run and answered confidently wrong.
    """
    runs = [
        {"dag_run_id": "green-1", "state": "success", "start_date": "2026-08-21T01:30:00Z"},
        {"dag_run_id": "bad-1", "state": "failed", "start_date": "2026-08-21T02:30:00Z"},
        {"dag_run_id": "green-2", "state": "success", "start_date": "2026-08-21T03:30:00Z"},
    ]
    per_run = {
        "green-1": [{"task_id": "response_tests", "state": "success"}],
        "bad-1": [
            {"task_id": "ddp_vertical_classification_api", "state": "failed"},
            {"task_id": "response_tests", "state": "upstream_failed"},
        ],
        "green-2": [{"task_id": "response_tests", "state": "success"}],
    }

    class _Api:
        @staticmethod
        def resolve_bearer() -> str:
            return "tok"

        @staticmethod
        def day_window(day: str) -> tuple[str, str]:
            return (f"{day}T00:00:00Z", f"{day}T23:59:59Z")

        @staticmethod
        def list_runs_for_day(base: str, token: str, dag_id: str, s: str, e: str) -> list[dict]:
            return runs

        @staticmethod
        def list_task_instances_in_run(base: str, token: str, dag_id: str, rid: str) -> list[dict]:
            return per_run[rid]

    orig_api, orig_base = external_task_rca._api, external_task_rca._resolve_base
    external_task_rca._api = lambda: _Api
    external_task_rca._resolve_base = lambda: "https://x/api/v2"
    try:
        failed, note = external_task_rca.upstream_failures(
            "vertical_classification_api", None, "response_tests", "2026-08-21", "upstream_failed"
        )
    finally:
        external_task_rca._api, external_task_rca._resolve_base = orig_api, orig_base

    assert failed == ["ddp_vertical_classification_api"], failed
    assert note is None


# Verbatim shape from fangorn_inference_pipeline_run/wait_for_challenger_features, 2026-08-08:
# a 216 KB log of poke lines with no exception anywhere in it.
_POKE_LOOP = (
    "2026-08-08T17:27:28.901651Z [info] airflow.task.operators...GCSObjectExistenceSensor "
    "Sensor checks existence of : mntn-data-archive-prod, "
    "feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07/_SUCCESS\n"
    "2026-08-08T17:27:29.190161Z [info] task Rescheduling task, marking task as UP_FOR_RESCHEDULE\n"
) * 3


def test_a_poke_loop_names_what_the_sensor_waited_on() -> None:
    """The log looks healthy and the task is failed; the answer is the target, not the log."""
    p = parse_log(_POKE_LOOP)
    assert p.poke_count == 3
    assert p.reschedule_count == 3
    assert p.poke_target.endswith("dt=2026-08-07/_SUCCESS")
    assert p.poke_target.startswith("gs://mntn-data-archive-prod/")


def test_a_failed_sensor_with_no_timeout_line_is_explained_not_left_blank() -> None:
    """The AirflowSensorTimeout lands in a different try, so this try has nothing to classify."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "wait_for_x"},
        "no_error_text": True,
        "ti_state": "failed",
        "poke_target": "gs://b/o/_SUCCESS",
        "poke_count": 60,
        "reschedule_count": 60,
        "root_signature": {},
    }
    out = build_report(diag)
    assert "gs://b/o/_SUCCESS" in out
    assert "60 time(s)" in out
    assert "different try" in out


def test_an_empty_failed_log_without_pokes_still_reports_the_worker_death() -> None:
    """A sensor verdict must not swallow the plain empty-log case."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "no_error_text": True,
        "ti_state": "failed",
        "poke_target": None,
        "poke_count": 0,
        "root_signature": {},
    }
    out = build_report(diag)
    assert "worker died" in out


def test_a_process_killed_mid_poke_is_not_read_as_a_sensor_giving_up() -> None:
    """audience_intent/wait_for_ipdsc_geo, 2026-08-19 06:39Z, inside Astronomer's DB maintenance.

    22 pokes and ZERO reschedules: the sensor was polling in-process, so it never handed control
    back. The log stopping there means the process was killed, not that the sensor timed out, and
    the two need opposite actions.
    """
    diag = {
        "identity": {"dag_id": "audience_intent", "task_id": "wait_for_ipdsc_geo"},
        "no_error_text": True,
        "ti_state": "failed",
        "poke_target": "gs://mntn-data-archive-prod/ipdsc_geo/dt=2026-08-18/_SUCCESS",
        "poke_count": 22,
        "reschedule_count": 0,
        "root_signature": {},
    }
    out = build_report(diag)
    assert "killed mid-poke" in out
    assert "Nothing here is the cause" in out
    assert "control-plane" in out
    assert "time(s) and never saw it" not in out, "must not read as a sensor timeout"


def test_a_reschedule_sensor_is_not_read_as_a_killed_process() -> None:
    """The mirror image: reschedules present means the sensor DID hand control back."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "wait_for_x"},
        "no_error_text": True,
        "ti_state": "failed",
        "poke_target": "gs://b/o/_SUCCESS",
        "poke_count": 60,
        "reschedule_count": 60,
        "root_signature": {},
    }
    out = build_report(diag)
    assert "different try" in out
    assert "killed mid-poke" not in out


# Verbatim shape from databricks_guid_geos/run_databricks_job, 2026-08-20. The operator announces
# a startup budget, the pod never logs anything, and the task raises with an empty message.
_POD_TIMEOUT = (
    "2026-08-20T01:01:12.453165Z [info] DbxDbtOperator Building pod run-databricks-job-xrc0t925\n"
    "2026-08-20T01:01:12.772400Z [info] PodManager Waiting up to 120s to get the POD scheduled...\n"
    "2026-08-20T01:01:12.808300Z [info] PodManager Waiting 120s to get the POD running...\n"
    "2026-08-20T01:03:13.560881Z [info] DbxDbtOperator Deleting pod: run-databricks-job-xrc0t925\n"
    "2026-08-20T01:03:13.636975Z [error] task Task failed with exception\n"
)


def test_a_pod_that_never_started_is_parsed_from_the_wait_and_delete_pair() -> None:
    """There is no error text to match, so the evidence has to be structural."""
    p = parse_log(_POD_TIMEOUT)
    assert p.pod_name == "run-databricks-job-xrc0t925"
    assert p.pod_wait_seconds == 120
    assert p.pod_deleted is True


def test_the_empty_exception_is_explained_instead_of_left_unclassified() -> None:
    """`Task failed with exception` with no payload is the log's whole content."""
    diag = {
        "identity": {"dag_id": "databricks_guid_geos", "task_id": "run_databricks_job"},
        "pod_name": "run-databricks-job-xrc0t925",
        "pod_wait_seconds": 120,
        "pod_deleted": True,
        "root_error": "",
        "root_signature": {},
    }
    out = build_report(diag)
    assert "run-databricks-job-xrc0t925" in out
    assert "120s budget" in out
    assert "Nothing in this log is the cause" in out


def test_a_pod_note_never_overrides_a_real_error() -> None:
    """A pod that was deleted AFTER a genuine failure must not steal the verdict."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "pod_name": "p-123",
        "pod_wait_seconds": 120,
        "pod_deleted": True,
        "root_error": "java.lang.OutOfMemoryError: Java heap space",
        "root_signature": {},
    }
    out = build_report(diag)
    assert "did not reach Running" not in out


def test_a_pod_that_started_fine_produces_no_note() -> None:
    """No delete means the operator never gave up waiting."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "pod_name": "p-123",
        "pod_wait_seconds": 120,
        "pod_deleted": False,
        "root_error": "",
        "root_signature": {},
    }
    assert "did not reach Running" not in build_report(diag)


def _stale_evidence(state: str) -> object:
    """One sensor whose target reached `state` AFTER the sensor gave up, through the real path."""
    from unittest import mock

    from airflow_debugger import external_task_rca as rca

    hit = {"task_id": "build_features", "state": state, "end_date": "2026-08-25T06:10:00+00:00"}
    api = mock.Mock(
        resolve_bearer=lambda: "t",
        list_task_instances_in_run=lambda *a, **k: [hit],
    )
    with (
        mock.patch.object(rca, "_resolve_base", lambda: "http://x"),
        mock.patch.object(rca, "_api", lambda: api),
        mock.patch.object(rca, "_target_run", lambda *a, **k: {"dag_run_id": "r1"}),
    ):
        return rca.analyze_external_task(
            "producer_dag", ["build_features"], "2026-08-25T05:00:00+00:00",
            failed_at="2026-08-25T05:30:00+00:00",
        )  # fmt: skip


def test_a_stale_target_state_never_prescribes_the_remedy_it_just_ruled_out() -> None:
    """The remedy was keyed on the target's CURRENT state, which this branch has just declared
    stale. For a target that ended green that printed "the sensor looked at the wrong run"
    under a cause saying the sensor was right."""
    ev = _stale_evidence("success")
    cause = " ".join(ev.notes)
    remedy = ev.signature["remedy"]
    assert "not a sensor bug" in cause, cause
    assert "wrong run" not in remedy, remedy
    assert "covers the target's real runtime" in remedy, remedy


def test_a_stale_failed_target_still_routes_to_the_target() -> None:
    """The other half of the same branch: a target that ended failed is the thing to diagnose."""
    remedy = _stale_evidence("failed").signature["remedy"]
    assert "Diagnose the target's own failure" in remedy, remedy


if __name__ == "__main__":
    for name, fn in sorted(dict(globals()).items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("OK - vertex + external-task routing tests passed")
