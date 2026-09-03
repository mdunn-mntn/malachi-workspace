from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import TaskGroup

from dag_utils.google import config, run_dataproc_serverless
from dag_utils.pagerduty import pagerduty_failure_callback_camperbid

with DAG(
    dag_id="bos__hourly",
    start_date=datetime(2026, 4, 1),
    schedule="0 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["bos", "hourly"],
    default_args={
        "retries": 2,
        "on_failure_callback": pagerduty_failure_callback_camperbid("critical"),
    },
) as dag:
    TABLES = (
        "campaign_avg_cpm",
        "campaign_audience_size",
    )

    COREDB_CONN_ID = "coredb_prod_camperbid"
    COREDB_DB = "coredb"
    COREDB_STAGING_SCHEMA = "camperbid"
    COREDB_STAGING_TABLE_PREFIX = f"bos__{config.env}"

    PATH_BASE = f"{config.table_path}/bos"
    PATH_TEMP = f"{PATH_BASE}/_tmp"

    for table_name in TABLES:
        with TaskGroup(group_id=table_name[:30]) as tg:
            drop_coredb_staging_table = SQLExecuteQueryOperator(
                task_id="drop_coredb_staging_table",
                conn_id=COREDB_CONN_ID,
                database="coredb",
                sql="DROP TABLE IF EXISTS {{ params.staging_schema }}.{{ params.staging_table }}",
                split_statements=False,
                autocommit=True,
                params={
                    "staging_schema": COREDB_STAGING_SCHEMA,
                    "staging_table": f"{COREDB_STAGING_TABLE_PREFIX}__{table_name}",
                },
            )

            create_coredb_staging_table = run_dataproc_serverless(
                task_id="create_coredb_staging_table",
                script_path=f"{config.scripts_path}/bos/{table_name}.py",
                script_args=[
                    "--env",
                    config.env,
                    "--ts",
                    "{{ data_interval_end }}",
                    "--staging-schema",
                    "camperbid",
                    "--staging-table",
                    f"{COREDB_STAGING_TABLE_PREFIX}__{table_name}",
                    "--path-temp",
                    PATH_TEMP,
                ],
                max_executors=20,
                shuffle_partitions=200,
                enable_postgres=True,
                retries=2,
            )

            drop_coredb_staging_table >> create_coredb_staging_table

            if config.env == "prod":
                sync_coredb_target_table = SQLExecuteQueryOperator(
                    task_id="sync_coredb_target_table",
                    conn_id=COREDB_CONN_ID,
                    database=COREDB_DB,
                    sql=f"sql/{table_name}.sql",
                    split_statements=False,
                    autocommit=False,
                    params={
                        "staging_schema": COREDB_STAGING_SCHEMA,
                        "staging_table": f"{COREDB_STAGING_TABLE_PREFIX}__{table_name}",
                    },
                )
                create_coredb_staging_table >> sync_coredb_target_table
