"""Offline unit tests for the parser/router + report synthesis (no network).

Run: python3 -m airflow_debugger.tests.test_parse  (or via pytest).
"""

from __future__ import annotations

from airflow_debugger.parse import _spark_succeeded, parse_log
from airflow_debugger.report import build_report

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


if __name__ == "__main__":
    for fn in [
        test_parse_databricks,
        test_parse_dataproc,
        test_parse_sensor_is_other,
        test_parse_quoted_identity,
        test_spark_succeeded,
        test_report_orchestration_only_no_emdash,
    ]:
        fn()
    print("OK — parse + synthesis tests passed")
