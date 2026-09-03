from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLExecuteQueryOperator, SQLTableCheckOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator, BigQueryDeleteTableOperator, BigQueryInsertJobOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.sdk import TaskGroup, get_current_context

from dag_utils.google import config, run_dataproc_serverless
from dag_utils.pagerduty import pagerduty_failure_callback_camperbid
from dag_utils.testing import SQLTestOperator
from dags.intent_score_threshold_v4.tests import NONBLOCKING_TESTS

with open(Path(__file__).parent.joinpath("README.md")) as f:
    doc_md = f.read()

with open(Path(__file__).parent.joinpath("sql", "population_histogram.sql")) as f:
    population_histogram_sql = f.read()

with DAG(
    dag_id="intent_score_threshold_v4",
    doc_md=doc_md,
    description="?",
    start_date=datetime(2025, 6, 23),
    schedule="0 0 * * *",  # At midnight
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    default_args={
        "retries": 2,
        "on_failure_callback": pagerduty_failure_callback_camperbid("critical"),
    },
) as dag:
    PYSPARK_SCRIPT_PATH = f"{config.scripts_path}/intent_score_threshold_v4/pipeline.py"
    HOME_PATH = f"{config.table_path}/intent_score_threshold_v4"
    TABLES = [
        "campaign",
        "campaign_budget",
        "campaign_impression_cap",
        "campaign_cost_and_impression",
        "campaign_bucket_population",
        "campaign_threshold",
    ]
    BIG_TABLES = [
        "campaign_cost_and_impression",
    ]
    JDBC_TABLES = [
        "campaign",
        "campaign_budget",
        "campaign_impression_cap",
    ]

    wait = TimeDeltaSensorAsync(
        task_id="wait",
        delta=timedelta(hours=1),
    )

    with TaskGroup(group_id="v4_pipeline") as pipeline:
        task_dict = {}
        for table in TABLES:
            task_dict[table] = run_dataproc_serverless(
                task_id=table,
                script_path=PYSPARK_SCRIPT_PATH,
                script_args=[
                    "etl",
                    "--env",
                    config.env,
                    "--data-interval-end",
                    "{{ data_interval_end }}",
                    "--home-path",
                    HOME_PATH,
                    "--table-name",
                    table,
                ],
                retries=2,
                enable_postgres=True if table in JDBC_TABLES else False,
                shuffle_partitions=10_000 if table in BIG_TABLES else 1,
                initial_executors=200 if table in BIG_TABLES else 2,
                max_executors=200 if table in BIG_TABLES else 2,
                runtime_version="3.0",
            )

        task_dict["campaign"] >> task_dict["campaign_budget"] >> task_dict["campaign_threshold"]
        task_dict["campaign"] >> task_dict["campaign_impression_cap"] >> task_dict["campaign_threshold"]
        task_dict["campaign"] >> task_dict["campaign_cost_and_impression"] >> task_dict["campaign_threshold"]
        task_dict["campaign"] >> task_dict["campaign_bucket_population"] >> task_dict["campaign_threshold"]

    wait >> pipeline

    # PACE-6989: one shared bid-log scan feeds both v3's and v4's bucketing (halves the
    # nightly scan and shuffle; see sql/population_histogram.sql). It filters campaigns on
    # BOTH campaign_bucket tables, so it must run after v4's campaign task (writes ours)
    # AND v3's (writes theirs) — hence the sensor. v3's campaign_bucket_population in turn
    # waits on this task. The resulting cross-DAG ordering is linear, no cycle:
    # v4.campaign + v3.campaign -> population_histogram -> v3 pipeline/tests -> v4 sync.
    wait_for_v3_campaign = ExternalTaskSensor(
        task_id="wait_for_v3_campaign",
        external_dag_id="intent_score_threshold_v3",
        external_task_id="v3_pipeline.campaign",
        timeout=timedelta(hours=8),
        deferrable=True,
    )

    population_histogram = BigQueryInsertJobOperator(
        task_id="population_histogram",
        project_id="dw-main-bronze",
        location=config.region,
        cancel_on_kill=True,
        params={"env": config.env},
        configuration={
            # https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
            "query": {
                "query": population_histogram_sql,
                "useLegacySql": False,
            }
        },
    )

    [task_dict["campaign"], wait_for_v3_campaign] >> population_histogram >> task_dict["campaign_bucket_population"]

    with TaskGroup(group_id="create_external_tables") as create_external_tables:
        for table in TABLES:
            table_id = f"camperbid_{config.env}__hhst_v4__{table}"
            drop_table = BigQueryDeleteTableOperator(
                task_id=f"drop_{table}",
                ignore_if_missing=True,
                deletion_dataset_table=f"dw-main-bronze.external.{table_id}",
            )
            create_table = BigQueryCreateTableOperator(
                task_id=f"create_{table}",
                project_id=config.project_id,
                dataset_id="external",
                table_id=table_id,
                if_exists="log",
                table_resource={  # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
                    "tableReference": {
                        "projectId": "dw-main-bronze",
                        "datasetId": "external",
                        "tableId": table_id,
                    },
                    "description": "External table from the intent_score_threshold DAG",
                    "requirePartitionFilter": False,
                    "type": "EXTERNAL",
                    "externalDataConfiguration": {
                        "sourceFormat": "PARQUET",
                        "autodetect": True,
                        "sourceUris": [
                            f"{HOME_PATH}/{table}/*.parquet",
                        ],
                        "hivePartitioningOptions": {
                            "mode": "CUSTOM",
                            "sourceUriPrefix": f"{HOME_PATH}/{table}" + r"/{dt:STRING}/{hh:STRING}",
                            "requirePartitionFilter": True,
                        },
                    },
                },
            )
            drop_table >> create_table

    pipeline >> create_external_tables

    with TaskGroup(group_id="sync_to_coredb") as sync_to_coredb:
        CONN_ID_COREDB = "coredb_prod_camperbid"
        COREDB_SCHEMA = "camperbid"
        COREDB_TABLE_PREFIX = f"hhst_v4__{config.env}__"

        drop_tables = SQLExecuteQueryOperator(
            task_id="drop_tables",
            conn_id=CONN_ID_COREDB,
            sql="\n".join(
                [
                    "BEGIN;",
                    *[f"DROP TABLE IF EXISTS {COREDB_SCHEMA}.{COREDB_TABLE_PREFIX}{t};" for t in TABLES],
                    "COMMIT;",
                ]
            ),
            split_statements=False,
            autocommit=False,
        )

        copy_tables = run_dataproc_serverless(
            task_id="copy_tables",
            script_path=PYSPARK_SCRIPT_PATH,
            script_args=[
                "copy_to_coredb",
                "--env",
                config.env,
                "--data-interval-end",
                "{{ data_interval_end }}",
                "--home-path",
                HOME_PATH,
                "--schema",
                COREDB_SCHEMA,
                "--table-prefix",
                COREDB_TABLE_PREFIX,
                "--tables",
                *TABLES,
            ],
            enable_postgres=True,
            shuffle_partitions=1,
            max_executors=2,
            retries=2,
            runtime_version="3.0",
        )
        drop_tables >> copy_tables

        with TaskGroup(group_id="blocking_test_tables") as blocking_test_tables:
            SQLTableCheckOperator(
                task_id="table_checks_campaign_bucket_population",
                conn_id=CONN_ID_COREDB,
                table=f"{COREDB_SCHEMA}.{COREDB_TABLE_PREFIX}campaign_bucket_population",
                checks={
                    "row_count_check": {"check_statement": "COUNT(*) BETWEEN 100 AND 500000"},
                    # all-zero populations (empty shared histogram, 2026-08-21) must block
                    # the publish - row counts alone can't catch it
                    "population_sum_check": {"check_statement": "SUM(population) > 0"},
                },
            )
            SQLTableCheckOperator(
                task_id="table_checks_campaign_threshold",
                conn_id=CONN_ID_COREDB,
                table=f"{COREDB_SCHEMA}.{COREDB_TABLE_PREFIX}campaign_threshold",
                checks={
                    "row_count_check": {
                        "check_statement": "COUNT(*) BETWEEN 1000 AND 10000",
                    },
                    "percent_null_check_pacing_pct": {
                        "check_statement": "SUM(CASE WHEN pacing_pct IS NULL THEN 1 ELSE 0 END) / SUM(1) < 0.1",  # No more than 10% NULL
                    },
                },
            )
            SQLColumnCheckOperator(
                task_id="column_checks_campaign_bucket_population",
                conn_id=CONN_ID_COREDB,
                table=f"{COREDB_SCHEMA}.{COREDB_TABLE_PREFIX}campaign_bucket_population",
                column_mapping={
                    "campaign_id": {"null_check": {"equal_to": 0, "tolerance": 0}},
                    "lower": {"null_check": {"equal_to": 0, "tolerance": 0}},
                    "upper": {"null_check": {"equal_to": 0, "tolerance": 0}},
                },
                accept_none=False,
            )
            SQLColumnCheckOperator(
                task_id="column_checks_campaign_threshold",
                conn_id=CONN_ID_COREDB,
                table=f"{COREDB_SCHEMA}.{COREDB_TABLE_PREFIX}campaign_threshold",
                column_mapping={
                    "campaign_id": {
                        "null_check": {"equal_to": 0, "tolerance": 0},
                        "unique_check": {"equal_to": 0, "tolerance": 0},
                    },
                    "next_threshold": {
                        "null_check": {"equal_to": 0, "tolerance": 0},
                    },
                },
                accept_none=False,
            )
        copy_tables >> blocking_test_tables

        nonblocking_test_tables = SQLTestOperator.partial(
            task_id="nonblocking_test_tables",
            conn_id=CONN_ID_COREDB,
            map_index_template="{{ task.description }}",
        ).expand_kwargs([{"test": t} for t in NONBLOCKING_TESTS])
        copy_tables >> nonblocking_test_tables

        if config.env == "prod":
            wait_for_v3 = ExternalTaskSensor(
                task_id="wait_for_v3",
                external_dag_id="intent_score_threshold_v3",
                external_task_group_id="sync_to_coredb.test_tables",
                # bucket_population alone can run 2-4h at Aug 2026 bid-log volumes;
                # 4h regularly expired before v3 finished.
                timeout=timedelta(hours=8),
                deferrable=True,
            )

            with TaskGroup(group_id="sync_tables") as sync_tables:
                SQLExecuteQueryOperator(
                    task_id="sync_intent_threshold_buckets",
                    conn_id=CONN_ID_COREDB,
                    sql="sql/sync_intent_threshold_buckets.sql",
                    split_statements=False,
                    autocommit=False,
                )
                SQLExecuteQueryOperator(
                    task_id="sync_optimized_intent_thresholds",
                    conn_id=CONN_ID_COREDB,
                    sql="sql/sync_optimized_intent_thresholds.sql",
                    split_statements=False,
                    autocommit=False,
                )
            wait_for_v3 >> sync_tables
            blocking_test_tables >> sync_tables

        pipeline >> sync_to_coredb

    # PACE-6846: off-critical-path signal for the MNTN ID rollout. The grid row for
    # mntn_id_ingestion_active stays "skipped" until bid events with
    # household_id_source = 'mntn_id' appear, then flips green — making the flip date
    # visible per-run without opening logs. Never pages, never blocks the pipeline.
    with TaskGroup(group_id="mntn_id_observability"):

        def _probe_bid_event_sources() -> bool:
            sql = """
                SELECT
                    household_id_source
                    , COUNT(*) AS events
                    , COUNT(DISTINCT campaign_group_id) AS cgids
                FROM `dw-main-bronze.raw.bidder_bid_events`
                WHERE
                    _PARTITIONTIME >= TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL 2 HOUR)
                    AND _PARTITIONTIME < TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL 1 HOUR)
                GROUP BY 1
                ORDER BY 2 DESC
            """
            try:
                hook = BigQueryHook(gcp_conn_id=config.conn_id, use_legacy_sql=False)
                # The connection's default project lacks bigquery.jobs.create in prod, so pin it
                job = hook.insert_job(
                    configuration={"query": {"query": sql, "useLegacySql": False}},
                    project_id=config.project_id,
                    location=config.region,
                )
                rows = list(job.result())
                summary = " | ".join(f"{source or 'null'}: {events:,} events, {cgids} cgids" for source, events, cgids in rows) or "no rows"
                print(f"Bid-event household id sources, hour beginning 2h ago (1h landing buffer): {summary}")
                get_current_context()["ti"].xcom_push(key="summary", value=summary)
                return any(row[0] == "mntn_id" for row in rows)
            except Exception as e:
                # Surface the error in the task's XCom so a persistently broken probe is
                # distinguishable from "no mntn_id traffic yet" without opening logs.
                print(f"Probe failed (not paging, not blocking): {e}")
                try:
                    get_current_context()["ti"].xcom_push(key="summary", value=f"probe error: {e}")
                except Exception as xcom_error:
                    print(f"Could not push probe error to XCom: {xcom_error}")
                return False

        probe = ShortCircuitOperator(
            task_id="probe_bid_event_sources",
            python_callable=_probe_bid_event_sources,
            on_failure_callback=None,
            retries=1,
        )
        probe >> EmptyOperator(task_id="mntn_id_ingestion_active")
