"""Offline unit tests for the parser/router + report synthesis (no network).

Run: python3 -m airflow_debugger.tests.test_parse  (or via pytest).
"""

from __future__ import annotations

import base64
import os
import subprocess
import tempfile
from unittest import mock

from airflow_debugger import databricks_rca, report
from airflow_debugger.dataproc_rca import _decode_app_id, _run, analyze_batch
from airflow_debugger.parse import _spark_succeeded, parse_log, parse_log_file
from airflow_debugger.report import build_report
from airflow_debugger.signatures import SIGNATURES


def _sig(key: str) -> dict:
    from dataclasses import asdict

    return next(asdict(s) for s in SIGNATURES if s.key == key)


_DBX_LOG = """
[2026-07-31T15:47:47Z] INFO - TaskInstance Details: dag_id=keyword_ddp_reporting task_id=write_targeted_signal_ds_19 try_number=2 op_classpath=["include.dbx.kube_operators.DbxDbtOperator","airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator"]
[2026-07-31T15:52:03Z] INFO - [base] Databricks adapter: Job submission response=b'{"run_id":65237255325756}'
"""

_DATAPROC_LOG = """
[2026-07-29T14:40Z] INFO - TaskInstance Details: dag_id=tpa_mntn_id_export task_id=tpa_mntn_id_export try_number=3 op_classpath=["include.models.operators.ModelPysparkBatchOperator","airflow.providers.google.cloud.operators.dataproc.DataprocCreateBatchOperator"]
[2026-07-29T14:40Z] INFO - Batch job tpa-mntn-id-20260729-3 submitted
"""

_SENSOR_LOG = """
[2026-07-30Z] INFO - TaskInstance Details: dag_id=x task_id=wait_for_y try_number=1 op_classpath=["airflow.providers.standard.sensors.external_task.ExternalTaskSensor","airflow.sdk.bases.sensor.BaseSensorOperator"]
"""


def test_parse_databricks() -> None:
    """DbxDbtOperator log routes to databricks with the adapter run_id."""
    p = parse_log(_DBX_LOG)
    assert p.engine == "databricks"
    assert p.dbx_run_id == 65237255325756
    assert p.dag_id == "keyword_ddp_reporting"
    assert p.try_number == 2


def test_parse_dataproc() -> None:
    """ModelPysparkBatch log routes to dataproc with the batch id."""
    p = parse_log(_DATAPROC_LOG)
    assert p.engine == "dataproc"
    assert p.batch_id == "tpa-mntn-id-20260729-3"


def test_parse_sensor_is_other() -> None:
    """A sensor/python task is 'other' (no Spark job)."""
    p = parse_log(_SENSOR_LOG)
    assert p.engine == "other"
    assert p.batch_id is None and p.dbx_run_id is None


# Real prod shape (INC-012): identity appears only QUOTED inside the RuntimeTaskInstance repr
# (dag_id='materialize_mntn_select'); there is no unquoted "TaskInstance Details" line.
_QUOTED_IDENTITY_LOG = """
[2026-08-06T21:04:16Z] INFO - {'task_instance': RuntimeTaskInstance(task_id='materialize', dag_id='materialize_mntn_select', run_id='scheduled__2026-08-06T19:45:00+00:00', try_number=1)}
[2026-08-06T21:04:16Z] ERROR - AirflowException("Batch job mntn-select-2026-08-06-1786049114 failed with error: Google Cloud Dataproc Agent reports job failure")
"""


def test_parse_quoted_identity() -> None:
    """dag_id/task_id parse when they only appear quoted (INC-012 log shape)."""
    p = parse_log(_QUOTED_IDENTITY_LOG)
    assert p.dag_id == "materialize_mntn_select"
    assert p.task_id == "materialize"
    assert p.try_number == 1


# Real failed-run dataproc shape (2026-08-05 tpa_ipdsc_export): 'Starting batch <id>' and
# lowercase 'batch job <id>' only; the capital 'Batch job <id>' line is success-only.
_DATAPROC_FAILED_LOG = """
2026-08-05T04:53:00Z [info] airflow.task.operators.include.models.operators.ModelPysparkBatchOperator Current env: prod
2026-08-05T04:53:00Z [info] airflow.task.operators.include.models.operators.ModelPysparkBatchOperator Starting batch ipd-ds-17-x5m-20260804-023500-1
2026-08-05T04:53:02Z [info] airflow.task.operators.include.models.operators.ModelPysparkBatchOperator Waiting for the completion of batch job ipd-ds-17-x5m-20260804-023500-1
2026-08-05T04:54:32Z [error] task Task failed with exception
"""


def test_parse_batch_id_from_failed_run_wording() -> None:
    """batch_id parses from 'Starting batch'/lowercase 'batch job' (no success line)."""
    p = parse_log(_DATAPROC_FAILED_LOG)
    assert p.engine == "dataproc"
    assert p.batch_id == "ipd-ds-17-x5m-20260804-023500-1"
    assert p.notes == []


# Real DbxDbtOperator shape (2026-08-05 vertical_classification_api): the k8s pod-label dict
# carries a sanitized run_id EARLIER in the log than the real one in the TI repr.
_POD_LABEL_LOG = """
2026-08-05T16:30:08Z [info] airflow.task.operators.include.dbx.kube_operators.DbxDbtOperator Building pod ddp-vertical-classification-api-6mige0v3 with labels: {'dag_id': 'vertical_classification_api', 'task_id': 'ddp_vertical_classification_api', 'run_id': 'scheduled__2026-08-05T1530000000-1182d226a', 'try_number': '1'}
2026-08-05T17:15:08Z [info] include.job_config.slack_messages {'task_instance': RuntimeTaskInstance(task_id='ddp_vertical_classification_api', dag_id='vertical_classification_api', run_id='scheduled__2026-08-05T15:30:00+00:00', try_number=1, map_index=-1)}
"""


def test_parse_run_id_prefers_real_over_pod_label() -> None:
    """The real run_id (colons / +00:00) wins over the earlier k8s-sanitized label value."""
    p = parse_log(_POD_LABEL_LOG)
    assert p.run_id == "scheduled__2026-08-05T15:30:00+00:00"
    assert p.engine == "databricks"


def test_parse_run_id_manual_and_backfill_prefix() -> None:
    """manual__/backfill__ run_ids parse (a manual clear/re-run is the standard on-call action)."""
    tmpl = "RuntimeTaskInstance(task_id='t', dag_id='d', run_id='{rid}', try_number=2)"
    for rid in ("manual__2026-08-06T12:34:56+00:00", "backfill__2026-08-06T00:00:00+00:00"):
        assert parse_log(tmpl.format(rid=rid)).run_id == rid


def test_parse_operator_from_real_log_shapes() -> None:
    """Operator detection works without op_classpath: logger name and <Task(...)> repr."""
    # GKEStartPodOperator via the operator logger name (real ga4 shape) -> confirmed non-Spark.
    p = parse_log(
        "2026-08-06Z [info] airflow.task.operators.airflow.providers.google.cloud.operators"
        ".kubernetes_engine.GKEStartPodOperator Starting pod\n"
    )
    assert p.engine == "other"
    assert p.operator and "GKEStartPodOperator" in p.operator
    # _PythonDecoratedOperator via the Task repr only (real set_gaclid shape).
    p = parse_log("{'task': <Task(_PythonDecoratedOperator): send_notification>}")
    assert p.engine == "other"
    assert p.operator == "_PythonDecoratedOperator"
    # ExternalTaskSensor logger name -> 'other' too (matches the context tier).
    p = parse_log(
        "[info] airflow.task.operators.airflow.providers.standard.sensors.external_task"
        ".ExternalTaskSensor poking\n"
    )
    assert p.engine == "other"


def test_parse_log_file_filename_fallback() -> None:
    """Identity falls back to <HHMMSS>__<dag>__<task>[__mapN]__try<N>__<state>.log naming."""
    # Real ModelPysparkBatchOperator family: no dag_id/task_id/try_number in the body at all.
    body = _DATAPROC_FAILED_LOG
    with tempfile.TemporaryDirectory() as d:
        f1 = os.path.join(d, "045256__tpa_ipdsc_export__ipdsc_ds_17__try1__failed.log")
        with open(f1, "w") as fh:
            fh.write(body)
        p = parse_log_file(f1)
        assert p.dag_id == "tpa_ipdsc_export"
        assert p.task_id == "ipdsc_ds_17"
        assert p.try_number == 1
        # Mapped-task + upstream_failed state variant.
        f2 = os.path.join(
            d, "110652__url_pattern_identification__run_spark__map0__try2__upstream_failed.log"
        )
        with open(f2, "w") as fh:
            fh.write("no identity here\n")
        p = parse_log_file(f2)
        assert p.dag_id == "url_pattern_identification"
        assert p.task_id == "run_spark"
        assert p.map_index == 0
        assert p.try_number == 2
        # Body identity still wins over the filename when present.
        f3 = os.path.join(d, "120000__wrong_dag__wrong_task__try9__failed.log")
        with open(f3, "w") as fh:
            fh.write(_QUOTED_IDENTITY_LOG)
        p = parse_log_file(f3)
        assert p.dag_id == "materialize_mntn_select"
        assert p.try_number == 1


def test_spark_succeeded() -> None:
    """A SUCCESS Databricks run with no failed tasks reads as succeeded."""
    assert _spark_succeeded(
        {"engine": "databricks", "state": {"result_state": "SUCCESS"}, "failed_tasks": []}
    )
    assert not _spark_succeeded(
        {"engine": "databricks", "state": {"result_state": "FAILED"}, "failed_tasks": [{}]}
    )
    assert _spark_succeeded({"engine": "dataproc", "state": "SUCCEEDED"})
    assert not _spark_succeeded({"engine": "dataproc", "state": "CANCELLED"})


def test_report_orchestration_only_no_emdash() -> None:
    """Orchestration-only report names the succeeded Spark job and has no em-dash."""
    diag = {
        "identity": {"dag_id": "keyword_ddp_reporting", "task_id": "write_targeted_signal_ds_19"},
        "engine": "databricks",
        "orchestration_only": True,
        "dbx_run_id": 65237255325756,
        "job_id": 85436725717072,
        "root_signature": {
            "sig_class": "orchestration/pod-evicted",
            "likely_cause": "K8s pod evicted or lost mid-run.",
            "programmatic_fix": "no",
        },
    }
    r = build_report(diag)
    assert "orchestration-only failure" in r
    assert "SUCCEEDED" in r
    assert "—" not in r  # standing no-em-dash rule
    assert len(r) <= 500


# Real truncation shape (2026-08-06 vertical_classification_api log): 301-char
# dbt_test_failure cause + identity + fix line + full Databricks deep link > 500.
def test_report_truncation_keeps_url_whole() -> None:
    """Over-budget report shrinks the cause; the deep link and remedy survive whole."""
    diag = {
        "identity": {
            "dag_id": "vertical_classification_api",
            "task_id": "ddp_vertical_classification_api",
        },
        "engine": "databricks",
        "dbx_run_id": 485768712345678,
        "job_id": 794948123456789,
        "root_signature": _sig("dbt_test_failure"),
    }
    r = build_report(diag)
    link = "https://1262887251702944.4.gcp.databricks.com/jobs/794948123456789/runs/485768712345678"
    assert len(r) <= 500
    assert link in r  # whole URL, never cut mid-number
    assert "Route to the model owner" in r  # the remedy, not the fix category
    assert "…" in r  # the cause carries the truncation, not the link


def test_the_deep_link_is_dropped_whole_never_trimmed() -> None:
    """`_fit` trims the longest line, and the link was a candidate. Once trimmed, the guard that
    drops it whole could not fire, so the report shipped a URL that 404s."""
    link = "https://1262887251702944.4.gcp.databricks.com/jobs/794948123456789/runs/485768712345678"
    lines = ["x" * 90 for _ in range(5)] + [link]
    out = report._fit(lines, link, 300)
    assert len(out) <= 300
    assert link in out or link.split("/runs/")[0] not in out, out


def test_report_long_cause_leaves_room_for_fix_line() -> None:
    """A 599-char external_task_failed cause no longer swallows the remedy line."""
    diag = {
        "identity": {"dag_id": "hashed_email_ds_26_signals", "task_id": "wait_fpa"},
        "engine": "unknown",
        "root_signature": _sig("external_task_failed"),
    }
    r = build_report(diag)
    assert len(r) <= 500
    assert "Resolve the external task's real state first" in r
    assert "…" in r


def test_a_settled_answer_keeps_its_numbers_and_its_options() -> None:
    """The gauntlet blocker. A 500-char cap gutted the cause to 15 characters, so the shortfall the
    resolver exists to compute never reached the reader. No line may lose its fact."""
    diag = {
        "identity": {"dag_id": "fangorn_inference_pipeline_run", "task_id": "challenger"},
        "root_signature": _sig("quota_exhaustion"),
        "resolution": {
            "verdict": "The request needed 4672 N2_CPUS and 328 were free, short by 4344.",
            "evidence": "N2_CPUS: requested 4672, available 328",
            "solutions": [
                "Now: list what is consuming N2_CPUS in this region. A single idle cluster holding "
                "the headroom looks exactly like a ceiling that is too low, and deleting it is "
                "faster than a quota request.",
                "If nothing is holding it, the ceiling really is too low: raise N2_CPUS for the "
                "region. That is the AUDI-1217 work.",
                "To unblock this one run without waiting for either, shrink the request below 328.",
            ],
        },
    }
    r = build_report(diag)
    assert "4672" in r and "328" in r and "4344" in r, r
    assert "1. Now:" in r and "2." in r and "3." in r, r


# Real no-signature shape (2026-08-05 tpa_ipdsc_export logs): dataproc engine,
# no batch id, the parser's note is the only explanation available.
def test_report_no_signature_surfaces_parser_notes() -> None:
    """The fallback reads parser notes from the diag dict (defensively via .get)."""
    note = "dataproc engine but no 'Batch job <id>' line found"
    diag = {
        "identity": {"dag_id": "tpa_ipdsc_export", "task_id": "ipdsc_ds_17"},
        "engine": "dataproc",
        "spark": None,
        "root_signature": None,
        "notes": [note],
    }
    assert note in build_report(diag)
    # legacy diag without a notes key must not crash
    assert build_report({"identity": {}, "spark": None}).startswith("RCA [low]")


def test_short_cause_dotted_error_class() -> None:
    """Spark 3.4+/Databricks dotted error classes surface in the headline."""
    diag = {
        "identity": {"dag_id": "d", "task_id": "t"},
        "root_signature": {"sig_class": "query/schema-error"},
        "root_error": "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column `x` cannot be resolved.",
    }
    assert "UNRESOLVED_COLUMN.WITH_SUGGESTION" in build_report(diag).splitlines()[0]


def _proc(rc: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=rc, stdout=stdout, stderr=stderr)


def test_run_nonzero_empty_output_is_error() -> None:
    """rc!=0 with empty stdout+stderr must still return a non-empty error."""
    stdout, err = _run(["python3", "-c", "import sys; sys.exit(3)"])
    assert stdout is None
    assert err == "exit code 3 with no output"


def test_analyze_batch_phantom_describe() -> None:
    """A silent describe failure is reported as such, never as a phantom empty batch."""
    with mock.patch("airflow_debugger.dataproc_rca.subprocess.run", return_value=_proc(1)):
        ev = analyze_batch("phantom-batch")
    assert any("phantom-batch" in n and "describe failed" in n for n in ev.notes), ev.notes


def test_analyze_batch_expired_reads_as_expired() -> None:
    """Dataproc ages batches out, so a historical NOT_FOUND is expected rather than a fault."""
    not_found = _proc(
        1,
        stderr=(
            "ERROR: (gcloud.dataproc.batches.describe) NOT_FOUND: Not found: Batch "
            "projects/p/locations/us-central1/batches/old-batch. This command is "
            "authenticated as someone@mountain.com which is the active account."
        ),
    )
    with mock.patch("airflow_debugger.dataproc_rca.subprocess.run", return_value=not_found):
        ev = analyze_batch("old-batch")
    note = " ".join(ev.notes)
    assert "expired" in note
    assert "@example.com" not in note, "a published report must not carry an account"


def test_analyze_batch_surfaces_logging_stderr() -> None:
    """A failed gcloud logging read surfaces its stderr instead of blaming freshness."""
    describe = _proc(0, stdout='{"state": "CANCELLED"}')
    log_fail = _proc(1, stderr="HTTPSConnectionPool: Max retries exceeded / connection refused")
    with mock.patch(
        "airflow_debugger.dataproc_rca.subprocess.run", side_effect=[describe, log_fail]
    ):
        ev = analyze_batch("mntn-select-2026-08-06-1786049114")
    assert any(
        n.startswith("driver log fetch failed:") and "connection refused" in n for n in ev.notes
    )
    assert not any("freshness" in n for n in ev.notes)


def test_decode_app_id_marker_without_colon() -> None:
    """Marker line using '=' (conf/env echo) must not raise, falls to regex/None."""
    line = "spark.executorEnv.MCP_EVENT_LOGGING_CONFIG_BASE64=eyJub3QiOiJqc29uIn0="
    assert _decode_app_id(line) is None


def test_decode_app_id_non_dict_payload() -> None:
    """A base64 payload decoding to a non-dict must not raise AttributeError."""
    line = "MCP_EVENT_LOGGING_CONFIG_BASE64: " + base64.b64encode(b"[1, 2]").decode()
    assert _decode_app_id(line) is None


def test_decode_app_id_valid_breadcrumb() -> None:
    """The well-formed colon+dict breadcrumb still decodes."""
    payload = base64.b64encode(b'{"application_id": "app-2026080620512221-0001"}').decode()
    assert _decode_app_id("MCP_EVENT_LOGGING_CONFIG_BASE64: " + payload) == (
        "app-2026080620512221-0001"
    )


def _fake_dbx(responses: dict, calls: list) -> object:
    """Build a _dbx stand-in keyed on the leading CLI args; logs every call."""

    def fake(*args: str, timeout: int = 90) -> object:
        calls.append(args)
        return responses.get(args[:3], responses.get(args[:2]))

    return fake


def test_dbx_rca_timedout_and_canceled_tasks_collected() -> None:
    """TIMEDOUT/CANCELED result_states are failures, not skipped (review 2026-08-06)."""
    for result, msg in [("TIMEDOUT", "Run timed out after 7200s"), ("CANCELED", "Run cancelled")]:
        run = {
            "run_id": 900,
            "job_id": 1,
            "state": {"result_state": result, "state_message": msg},
            "tasks": [
                {
                    "task_key": "model",
                    "run_id": 901,
                    "state": {"result_state": result, "life_cycle_state": "TERMINATED"},
                }
            ],
        }
        calls: list = []
        responses = {
            ("jobs", "get-run", "900"): run,
            ("jobs", "get-run-output"): {"error": msg},
        }
        with mock.patch.object(databricks_rca, "_dbx", _fake_dbx(responses, calls)):
            ev = databricks_rca.analyze_run(900)
        assert [t["task_key"] for t in ev.failed_tasks] == ["model"], result
        assert ev.root_error == msg
        assert ("jobs", "get-run-output", "901") in calls


def test_dbx_rca_running_tasks_not_reported_failed() -> None:
    """Tasks with no result_state (RUNNING/PENDING, the INC-009 shape) are skipped."""
    run = {
        "run_id": 700,
        "state": {"life_cycle_state": "RUNNING"},
        "tasks": [
            {"task_key": "model", "run_id": 701, "state": {"life_cycle_state": "RUNNING"}},
            {"task_key": "post", "run_id": 702, "state": {"life_cycle_state": "PENDING"}},
        ],
    }
    calls: list = []
    with mock.patch.object(
        databricks_rca, "_dbx", _fake_dbx({("jobs", "get-run", "700"): run}, calls)
    ):
        ev = databricks_rca.analyze_run(700)
    assert ev.failed_tasks == []
    assert not any(c[:2] == ("jobs", "get-run-output") for c in calls)
    assert any("not terminal" in n for n in ev.notes)


def test_dbx_rca_follows_next_page_token() -> None:
    """A failed task on page 2 of the paginated task list is found."""
    page1 = {
        "run_id": 800,
        "state": {"result_state": "FAILED", "state_message": "Task seg_101 failed"},
        "tasks": [
            {"task_key": f"seg_{i}", "run_id": i, "state": {"result_state": "SUCCESS"}}
            for i in range(100)
        ],
        "next_page_token": "tok1",
    }
    page2 = {"tasks": [{"task_key": "seg_101", "run_id": 801, "state": {"result_state": "FAILED"}}]}
    calls: list = []

    def fake(*args: str, timeout: int = 90) -> object:
        calls.append(args)
        if args[:5] == ("jobs", "get-run", "800", "--page-token", "tok1"):
            return page2
        if args[:2] == ("jobs", "get-run"):
            return page1
        return {"error": "boom"}

    with mock.patch.object(databricks_rca, "_dbx", fake):
        ev = databricks_rca.analyze_run(800)
    assert [t["task_key"] for t in ev.failed_tasks] == ["seg_101"]
    assert ("jobs", "get-run", "800", "--page-token", "tok1") in calls
    assert ev.root_error == "boom"


def test_dbx_rca_non_dict_run_output_noted() -> None:
    """A non-dict get-run-output payload (json 'null') is surfaced in notes, not dropped."""
    run = {
        "run_id": 600,
        "state": {"result_state": "FAILED", "state_message": "fail"},
        "tasks": [{"task_key": "model", "run_id": 601, "state": {"result_state": "FAILED"}}],
    }
    responses = {("jobs", "get-run", "600"): run, ("jobs", "get-run-output"): None}
    with mock.patch.object(databricks_rca, "_dbx", _fake_dbx(responses, [])):
        ev = databricks_rca.analyze_run(600)
    assert any("non-dict payload" in n for n in ev.notes)


def test_bogus_none_batch_id_is_rejected() -> None:
    """'Starting batch None-1' means no batch was submitted; never query GCP for it."""
    log = (
        "[info] airflow.task.operators.include.dataproc.serverless_operators."
        "RetrySafeDataprocCreateBatchOperator Starting batch None-1\n"
        "[error] task Task failed with exception\n"
    )
    p = parse_log(log)
    assert p.engine == "dataproc"
    assert p.batch_id is None, f"queried GCP for a bogus id: {p.batch_id}"
    assert any("id-minting" in n for n in p.notes), p.notes


def test_real_batch_id_still_parses() -> None:
    """The None-guard must not reject a legitimate batch id."""
    p = parse_log(
        "[info] ...DataprocCreateBatchOperator Starting batch tpa-export-2026-08-15-1786992151-1"
    )
    assert p.batch_id == "tpa-export-2026-08-15-1786992151-1"
    assert p.notes == []


if __name__ == "__main__":
    for fn in [
        test_parse_databricks,
        test_parse_dataproc,
        test_parse_sensor_is_other,
        test_parse_quoted_identity,
        test_parse_batch_id_from_failed_run_wording,
        test_parse_run_id_prefers_real_over_pod_label,
        test_parse_run_id_manual_and_backfill_prefix,
        test_parse_operator_from_real_log_shapes,
        test_parse_log_file_filename_fallback,
        test_spark_succeeded,
        test_report_orchestration_only_no_emdash,
        test_report_truncation_keeps_url_whole,
        test_the_deep_link_is_dropped_whole_never_trimmed,
        test_report_long_cause_leaves_room_for_fix_line,
        test_report_no_signature_surfaces_parser_notes,
        test_short_cause_dotted_error_class,
        test_run_nonzero_empty_output_is_error,
        test_analyze_batch_phantom_describe,
        test_analyze_batch_surfaces_logging_stderr,
        test_decode_app_id_marker_without_colon,
        test_decode_app_id_non_dict_payload,
        test_decode_app_id_valid_breadcrumb,
        test_dbx_rca_timedout_and_canceled_tasks_collected,
        test_dbx_rca_running_tasks_not_reported_failed,
        test_dbx_rca_follows_next_page_token,
        test_dbx_rca_non_dict_run_output_noted,
    ]:
        fn()
    test_bogus_none_batch_id_is_rejected()
    test_real_batch_id_still_parses()
    print("OK — parse + synthesis tests passed")
