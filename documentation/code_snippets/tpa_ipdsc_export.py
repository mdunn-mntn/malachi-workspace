import logging
from datetime import datetime
from functools import reduce
from typing import Any

import boto3
import pendulum
from job_config import JobTeamConfig
from tpa_export.spark.data_source import all_data_source_ids
from tpa_export.spark.data_source import data_source_executor_memory
from tpa_export.spark.data_source import data_source_shuffle_partitions
from tpa_export.spark.data_source import ds_id_spark_step
from tpa_export.spark.data_source import tpa_export_cluster_config
from tpa_export.spark.data_source import tpa_export_spark_step
from tpa_export.spark.data_source.ipdsc_emr_cluster import geo_spark_step

from airflow import DAG
from airflow import AirflowException
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)
TEAM = JobTeamConfig.TPA_EXPORT.value


def fetch_output_prefix(**kwargs: Any) -> None:
    s3_client = boto3.client("s3", region_name="us-west-2")
    env = Variable.get("ENV")
    execution_date = datetime.now().strftime("%Y-%m-%d")

    bucket = f"sh-dw-external-tables-{env}"
    prefix_metadata_path = f"tpa_audit/prefix/{execution_date}/output_prefix.txt"

    response = s3_client.get_object(Bucket=bucket, Key=prefix_metadata_path)
    output_prefix = response["Body"].read().decode("utf-8").strip()

    if not output_prefix:
        raise ValueError("Output prefix not found in S3.")

    task_instance = kwargs["task_instance"]
    task_instance.xcom_push(key="output_prefix", value=output_prefix)


with DAG(
    dag_id="tpa_ipdsc_export",
    # start the DAG at 12:45 AM, UTC
    schedule_interval="45 2 * * *",
    start_date=pendulum.datetime(2024, 6, 8, tz="UTC"),
    catchup=False,
    concurrency=1,
    **TEAM.make_dag_args(severity=0, tags=["targeting", "ipdsc"], default_args={}),
) as dag:
    # yesterday's date in YYYY-MM-DD format
    DT = "{{ ds }}"
    # current airflow environment
    ENV = "{{ var.value.get('ENV') }}"
    # current bucket
    AIRFLOW_BUCKET = "{{ var.value.get('AIRFLOW_S3_BUCKET') }}"
    # spill path for Athena queries
    ATHENA_SPILL_PATH = "/_athena_spill/tpa_ipdsc_export/"

    begin_task = DummyOperator(task_id="begin_task")

    dump_icloud_ips = SqlToS3Operator(
        task_id="dump_icloud_ips",
        query=(
            """
            select ip from summarydata.icloud_ipv4
            """
        ),
        s3_bucket="{{ var.value.get('ARCHIVE_S3_BUCKET') }}",
        s3_key="ipdsc_icloud_ips/icloud.parquet",
        sql_conn_id="prod_cloudberry",
        file_format="parquet",
        replace=True,
    )

    # EMR cluster management tasks
    create_ipdsc_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_ipdsc_emr_cluster",
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        region_name="us-west-2",
        wait_for_completion=True,
        waiter_max_attempts=600,
        waiter_delay=60,
        job_flow_overrides=tpa_export_cluster_config(
            ENV,
            DT,
            [
                ds_id_spark_step(
                    bucket=AIRFLOW_BUCKET,
                    date=DT,
                    env=ENV,
                    data_source_id=data_source_id,
                    executor_memory=data_source_executor_memory.get(data_source_id, 28),
                    partitions=data_source_shuffle_partitions.get(data_source_id, 5000),
                )
                for data_source_id in all_data_source_ids
            ]
            + [
                geo_spark_step(
                    bucket=AIRFLOW_BUCKET,
                    date=DT,
                    env=ENV,
                )
            ]
            + [
                tpa_export_spark_step(
                    bucket=AIRFLOW_BUCKET,
                    date=DT,
                    env=ENV,
                )
            ],
        ),
    )

    wait_ipdsc_emr_cluster = EmrJobFlowSensor(
        task_id="wait_ipdsc_emr_cluster",
        job_flow_id=create_ipdsc_emr_cluster.output,
        aws_conn_id="aws_default",
        poke_interval=300,
        max_attempts=150,
        mode="reschedule",
        deferrable=True,
    )

    with TaskGroup(group_id="update_idsc_athena") as update_idsc_athena:
        [
            AthenaOperator(
                task_id=f"update_ipdsc_athena_ds_{ds}",
                query=f"ALTER TABLE data_archive_{ENV}.ipdsc ADD IF NOT EXISTS PARTITION (dt='{DT}', data_source_id={ds}) LOCATION 's3://mntn-data-archive-{ENV}/ipdsc/dt={DT}/data_source_id={ds}/';",
                database=f"data_archive_{ENV}",
                output_location="s3://mntn-data-curated-prod/msck_repair_log/ipdsc",
                aws_conn_id="aws_default",
                retries=3,
                workgroup="primary",
            )
            for ds in all_data_source_ids
        ] + [
            AthenaOperator(
                task_id="update_ipdsc_athena_geo",
                query=f"ALTER TABLE data_archive_{ENV}.ipdsc_geo ADD IF NOT EXISTS PARTITION (dt='{DT}') LOCATION 's3://mntn-data-archive-{ENV}/ipdsc_geo/dt={DT}/';",
                database=f"data_archive_{ENV}",
                output_location="s3://mntn-data-curated-prod/msck_repair_log/ipdsc_geo",
                aws_conn_id="aws_default",
                retries=3,
                workgroup="primary",
            )
        ]

    fetch_prefix = PythonOperator(
        task_id="fetch_output_prefix",
        python_callable=fetch_output_prefix,
        provide_context=True,
    )

    # we need to trigger TPA audit pipeline after this DAG completes
    trigger_tpa_file_persist = TriggerDagRunOperator(
        task_id="trigger_tpa_file_persist",
        trigger_dag_id="persist_tpa_export_files",
        conf={
            "bucket": f"sh-dw-external-tables-{ENV}",
            "prefix": "{{ task_instance.xcom_pull(task_ids='fetch_output_prefix', key='output_prefix') }}",
        },
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    trigger_tpa_daily_metrics = TriggerDagRunOperator(
        task_id="trigger_tpa_daily_metrics",
        trigger_dag_id="tpa_export_daily_metric_monitor_workflow",
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end_task = DummyOperator(
        task_id="end_task",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # implementation of the Airflow "watcher pattern"
    # https://airflow.apache.org/docs/apache-airflow/2.7.2/best-practices.html#example-of-watcher-pattern-with-trigger-rules
    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher():  # type: ignore[no-untyped-def]
        raise AirflowException("Failing task because one or more upstream tasks failed.")

    # setup the DAG
    (
        begin_task
        >> dump_icloud_ips
        >> create_ipdsc_emr_cluster
        >> wait_ipdsc_emr_cluster
        >> update_idsc_athena
        >> fetch_prefix
        >> trigger_tpa_file_persist
        >> trigger_tpa_daily_metrics
        >> end_task
    )

    # implementation of the Airflow "watcher pattern" -- only watch what we don't handle
    [
        dump_icloud_ips,
        create_ipdsc_emr_cluster,
        wait_ipdsc_emr_cluster,
        update_idsc_athena,
        trigger_tpa_file_persist,
    ] >> watcher()
