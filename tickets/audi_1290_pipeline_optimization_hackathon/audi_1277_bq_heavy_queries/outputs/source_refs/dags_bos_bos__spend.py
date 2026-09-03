from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator, BigQueryInsertJobOperator
from airflow.sdk import Param, TaskGroup

from dag_utils.google import config, run_dataproc_serverless
from dag_utils.pagerduty import pagerduty_failure_callback_camperbid
from dag_utils.testing import IntegrationTest, SQLTestOperator

with DAG(
    dag_id="bos__spend",
    start_date=datetime(2026, 4, 1),
    schedule="0,15,30,45 * * * *",
    max_active_runs=1,
    max_active_tasks=20,
    catchup=False,
    tags=["bos", "spend"],
    default_args={
        "retries": 2,
        "on_failure_callback": pagerduty_failure_callback_camperbid("critical"),
    },
    params={
        "backfill_days": Param(5, type="integer", minimum=1, maximum=1000, description="Number of previous days to backfill"),
    },
) as dag:
    TABLES = [
        "campaign_performance",
        "campaign_group_performance",
        "campaign_flight_end_cost",
        "campaign_group_flight_end_cost",
        "sum_by_private_marketplace_by_hour",
        "campaign_utc_yesterday_costs_impressions_by_hour",
    ]
    TABLES_THAT_DEPEND_ON_CAMPAIGN_SUMMARY_HOURLY = [
        "campaign_performance",
        "campaign_group_performance",
    ]
    TABLES_THAT_DEPEND_ON_FLIGHT_METRICS_PER2388 = [
        "campaign_performance",
    ]

    COREDB_CONN_ID = "coredb_prod_camperbid"
    COREDB_DB = "coredb"
    COREDB_STAGING_SCHEMA = "camperbid"
    COREDB_STAGING_TABLE_PREFIX = f"bos__{config.env}"

    PATH_BASE = f"{config.table_path}/bos"
    PATH_TEMP = f"{PATH_BASE}/_tmp"

    # campaign_summary_hourly
    # ---------------------------------------------------------------------------------
    with TaskGroup(group_id="campaign_summary_hourly") as campaign_summary_hourly:
        TABLE_ID_CAMPAIGN_SUMMARY_HOURLY = f"camperbid_{config.env}__bos__campaign_summary_hourly"
        create_table = BigQueryInsertJobOperator(
            task_id="create",
            project_id="dw-main-bronze",
            cancel_on_kill=True,
            configuration={
                # https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
                "query": {
                    # https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
                    "useLegacySql": False,
                    "query": """
                        DELETE FROM {dataset_id}
                        WHERE created_at >= CAST('{start_time}' AS TIMESTAMP);

                        INSERT INTO {dataset_id} (
                            created_at
                            , campaign_id
                            , campaign_group_id
                            , advertiser_id
                            , objective_id
                            , channel_id
                            , impressions
                            , media_cost
                            , media_spend
                            , data_spend
                            , platform_spend
                            , total_cost
                            , total_spend
                        )
                        WITH temp_after_6_hours_ago AS (
                            SELECT
                                DATE_TRUNC(sp.time, MINUTE) AS created_at
                                , sp.campaign_id
                                , c.campaign_group_id
                                , c.advertiser_id
                                , c.objective_id
                                , c.channel_id
                                , SUM(1) AS impressions
                                , CAST(SUM(COALESCE(sp.media_cost     , 0)) AS NUMERIC) AS media_cost
                                , CAST(SUM(COALESCE(sp.media_spend    , 0)) AS NUMERIC) AS media_spend
                                , CAST(SUM(COALESCE(sp.data_spend     , 0)) AS NUMERIC) AS data_spend
                                , CAST(SUM(COALESCE(sp.platform_spend , 0)) AS NUMERIC) AS platform_spend
                            FROM `dw-main-silver.logdata.spend_pacing` sp
                            INNER JOIN `dw-main-gold.public.campaigns` c
                                ON sp.campaign_id = c.campaign_id
                            WHERE
                                sp.time >= CAST('{end_time}' AS TIMESTAMP) - INTERVAL 24 HOUR
                                AND sp.time < CAST('{end_time}' AS TIMESTAMP)
                            GROUP BY 1, 2, 3, 4, 5, 6
                        )
                        , temp_before_6_hours_ago AS (
                            SELECT
                                DATE_TRUNC(cil.time, MINUTE) AS created_at
                                , cil.campaign_id
                                , c.campaign_group_id
                                , c.advertiser_id
                                , c.objective_id
                                , c.channel_id
                                , SUM(1) AS impressions
                                , CAST(SUM(COALESCE(cil.media_cost     , 0)) AS NUMERIC) AS media_cost
                                , CAST(SUM(COALESCE(cil.media_spend    , 0)) AS NUMERIC) AS media_spend
                                , CAST(SUM(COALESCE(cil.data_spend     , 0)) AS NUMERIC) AS data_spend
                                , CAST(SUM(COALESCE(cil.platform_spend , 0)) AS NUMERIC) AS platform_spend
                            FROM `dw-main-silver.logdata.cost_impression_log` cil
                            INNER JOIN `dw-main-gold.public.campaigns` c
                                ON cil.campaign_id = c.campaign_id
                            WHERE
                                cil.time >= CAST('{start_time}' AS TIMESTAMP)
                                AND cil.time < CAST('{end_time}' AS TIMESTAMP) - INTERVAL 24 HOUR
                                AND NOT cil.unlinked
                                AND cil.ad_served_id IS NOT NULL
                            GROUP BY 1, 2, 3, 4, 5, 6
                        )
                        , temp_stack AS (
                            SELECT * FROM temp_after_6_hours_ago
                            UNION ALL
                            SELECT * FROM temp_before_6_hours_ago
                        )
                        SELECT
                            created_at
                            , campaign_id
                            , campaign_group_id
                            , advertiser_id
                            , objective_id
                            , channel_id
                            , SUM(impressions)    AS impressions
                            , SUM(media_cost)     AS media_cost
                            , SUM(media_spend)    AS media_spend
                            , SUM(data_spend)     AS data_spend
                            , SUM(platform_spend) AS platform_spend
                            , SUM(media_cost)     AS total_cost
                            , SUM(media_spend + data_spend + platform_spend) AS total_spend
                        FROM temp_stack
                        GROUP BY 1, 2, 3, 4, 5, 6
                    """.format(
                        dataset_id=f"dw-main-bronze.external.{TABLE_ID_CAMPAIGN_SUMMARY_HOURLY}",
                        start_time="{{ data_interval_end.date().subtract(days=params.backfill_days) }}",
                        end_time="{{ data_interval_end }}",
                    ),
                }
            },
        )
        # test_regression DISABLED (drift test off). Re-enable: uncomment block + `create_table >> test_regression`.
        # test_regression = BigQueryTestOperator(
        # task_id="test_regression",
        # retries=2,
        # test=IntegrationTest(
        # description="Is there drift in spend between BOS and CIL?",
        # sql=f"""
        # WITH bos AS (
        # SELECT
        # campaign_group_id,
        # SUM(COALESCE(total_spend, 0)) AS bos_total_spend
        # FROM `dw-main-bronze.external.camperbid_{config.env}__bos__campaign_summary_hourly`
        # WHERE
        # created_at >= CAST('{{{{ data_interval_end }}}}' AS TIMESTAMP) - INTERVAL '30' DAY
        # AND created_at <  CAST('{{{{ data_interval_end }}}}' AS TIMESTAMP) - INTERVAL '2' DAY
        # GROUP BY campaign_group_id
        # ),

        # logdata AS (
        # SELECT
        # c.campaign_group_id,
        # SUM(
        # COALESCE(l.data_spend, 0)
        # + COALESCE(l.media_spend, 0)
        # + COALESCE(l.platform_spend, 0)
        # ) AS logdata_total_spend
        # FROM `dw-main-silver.logdata.cost_impression_log` l
        # JOIN `dw-main-silver.public.campaigns` c
        # ON c.campaign_id = l.campaign_id
        # WHERE
        # l.time >= CAST('{{{{ data_interval_end }}}}' AS TIMESTAMP) - INTERVAL '30' DAY
        # AND l.time < CAST('{{{{ data_interval_end }}}}' AS TIMESTAMP) - INTERVAL '2' DAY
        # AND l.unlinked = FALSE
        # AND l.ad_served_id IS NOT NULL
        # GROUP BY c.campaign_group_id
        # )
        # , temp_metric AS (
        # SELECT
        # COALESCE(b.campaign_group_id, l.campaign_group_id) AS campaign_group_id,
        # COALESCE(b.bos_total_spend, 0) AS bos_total_spend,
        # COALESCE(l.logdata_total_spend, 0) AS logdata_total_spend,
        # COALESCE(b.bos_total_spend, 0) - COALESCE(l.logdata_total_spend, 0) AS diff,
        # ROUND(
        # SAFE_DIVIDE(
        # COALESCE(b.bos_total_spend, 0) - COALESCE(l.logdata_total_spend, 0),
        # NULLIF(COALESCE(l.logdata_total_spend, 0), 0)
        # ) * 100,
        # 2
        # ) AS diff_pct
        # FROM bos b
        # FULL OUTER JOIN logdata l
        # ON b.campaign_group_id = l.campaign_group_id
        # )
        # SELECT *, ABS(diff) < 1.0 AS success
        # FROM temp_metric
        # WHERE NOT ABS(diff) < 1.0
        # ORDER BY ABS(diff) DESC;
        # """,
        # pagerduty_severity="critical",
        # column_alias_metric="diff_pct",
        # ),
        # )
        # create_table >> test_regression

    # flight_metrics_per2388
    # Copied from
    # https://github.com/SteelHouse/db_repo/blob/ff8eb8a4bb9d59180c9910506eead698786c8576/coredw/lds/functions/populate_campaign_performance_v2.sql
    # ---------------------------------------------------------------------------------
    with TaskGroup(group_id="flight_metrics_per2388") as flight_metrics_per2388:
        TABLE_ID_FLIGHT_METRICS_PER2388 = f"camperbid_{config.env}__bos__flight_metrics_per2388"
        drop_table = BigQueryDeleteTableOperator(
            task_id="drop",
            ignore_if_missing=True,
            deletion_dataset_table=f"dw-main-bronze.external.{TABLE_ID_FLIGHT_METRICS_PER2388}",
        )
        create_table = BigQueryInsertJobOperator(
            task_id="create",
            project_id="dw-main-bronze",
            cancel_on_kill=True,
            configuration={
                # https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
                "query": {
                    # https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
                    "destinationTable": {
                        "projectId": "dw-main-bronze",
                        "datasetId": "external",
                        "tableId": TABLE_ID_FLIGHT_METRICS_PER2388,
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "useLegacySql": False,
                    "query": """
                        WITH temp_flight_metrics_per2388 AS (
                            SELECT
                                af.campaign_id
                                , af.channel_id
                                , COALESCE(hll_count.MERGE(uniques), 0)                                                                         AS uniques
                                , SUM(COALESCE(click_conversions, 0) + COALESCE(view_conversions, 0) + COALESCE(competing_view_conversions, 0)) AS conversions
                                , SUM(COALESCE(clicks, 0) + COALESCE(views, 0) + COALESCE(competing_views, 0))                                  AS visits
                                , SUM(COALESCE(click_order_value, 0) + COALESCE(view_order_value, 0) + COALESCE(competing_view_order_value, 0)) AS order_value
                                , SUM(COALESCE(media_spend, 0) + COALESCE(data_spend, 0) + COALESCE(platform_spend, 0))                         AS spend
                                , CAST(SUM(COALESCE(vast_complete, 0)) AS INTEGER)                                                              AS vast_complete
                            FROM `dw-main-silver.summarydata.all_facts` af
                            INNER JOIN `dw-main-gold.dso.campaign_group_flight` cgf
                                ON af.campaign_group_id = cgf.campaign_group_id
                            WHERE
                                af.hour >= CAST('2024-01-01' AS DATETIME)
                                AND af.hour >= cgf.flight_start_time_local
                                and af.hour <= CURRENT_DATE + interval '2' day
                            GROUP BY 1, 2
                        )
                        , temp_data AS (
                            SELECT
                                campaign_id
                                , COALESCE(CAST(conversions AS NUMERIC) / NULLIF(visits, 0), 0)               AS conversion_rate
                                , COALESCE(CAST(visits AS NUMERIC) / NULLIF(CAST(uniques AS NUMERIC), 0), 0)  AS visit_rate
                                , COALESCE(order_value / NULLIF(spend, 0), 0)                                 AS roas
                                , COALESCE(spend / NULLIF(conversions, 0), 0)                                 AS cpa
                                , COALESCE(spend / NULLIF(visits, 0), 0)                                      AS cpv
                                , COALESCE(case when channel_id = 8 then coalesce(spend, 0) else 0 end / NULLIF(vast_complete, 0), 0) AS cpcv
                            FROM temp_flight_metrics_per2388
                        )
                        SELECT
                            CAST(campaign_id        AS INTEGER) AS campaign_id
                            , CAST(conversion_rate  AS NUMERIC) AS conversion_rate
                            , CAST(visit_rate       AS NUMERIC) AS visit_rate
                            , CAST(roas             AS NUMERIC) AS roas
                            , CAST(cpa              AS NUMERIC) AS cpa
                            , CAST(cpv              AS NUMERIC) AS cpv
                            , CAST(cpcv             AS NUMERIC) AS cpcv
                        FROM temp_data
                    """,
                }
            },
        )
        drop_table >> create_table

    # BOS Tables
    # ---------------------------------------------------------------------------------
    with TaskGroup(group_id="tables") as tg_tables:
        for table_name in TABLES:
            with TaskGroup(group_id=table_name) as tg:
                drop_table = SQLExecuteQueryOperator(
                    task_id="drop",
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

                script_args = [
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
                ]
                if table_name in TABLES_THAT_DEPEND_ON_CAMPAIGN_SUMMARY_HOURLY:
                    script_args += ["--table-id-campaign-summary-hourly", f"dw-main-bronze.external.{TABLE_ID_CAMPAIGN_SUMMARY_HOURLY}"]
                if table_name in TABLES_THAT_DEPEND_ON_FLIGHT_METRICS_PER2388:
                    script_args += ["--table-id-flight-metrics-per2388", f"dw-main-bronze.external.{TABLE_ID_FLIGHT_METRICS_PER2388}"]

                create_table = run_dataproc_serverless(
                    task_id="create",
                    script_path=f"{config.scripts_path}/bos/{table_name}.py",
                    script_args=script_args,
                    max_executors=20,
                    shuffle_partitions=200,
                    enable_postgres=True,
                    retries=2,
                )

                drop_table >> create_table

                if config.env == "prod":
                    sync = SQLExecuteQueryOperator(
                        task_id="sync",
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
                    create_table >> sync

                    test_staleness = SQLTestOperator(
                        task_id="test_staleness",
                        retries=2,
                        conn_id=COREDB_CONN_ID,
                        database=COREDB_DB,
                        test=IntegrationTest(
                            description=f"Ensure data in {table_name} is not stale",
                            sql=f"""
                                WITH temp_min_dates AS (
                                    SELECT
                                        MIN(create_time) AS min_create_time
                                        , INTERVAL '30' MINUTE AS time_until_stale
                                        , CURRENT_TIMESTAMP - INTERVAL '30' MINUTE AS stale_at
                                    FROM performance.{table_name}
                                )
                                SELECT
                                    *
                                    , CURRENT_TIMESTAMP - min_create_time AS staleness
                                    , min_create_time > stale_at AS success
                                FROM temp_min_dates
                            """,
                            pagerduty_severity="critical",
                            column_alias_metric="staleness",
                        ),
                    )
                    sync >> test_staleness

                    if table_name == "campaign_group_performance":
                        test_regression = SQLTestOperator(
                            task_id="test_regression",
                            retries=2,
                            conn_id=COREDB_CONN_ID,
                            database=COREDB_DB,
                            test=IntegrationTest(
                                description="Ensure values in performance.campaign_group_performance are at least 95% as big as the previous value.",
                                sql="""
                                    WITH temp_diff AS (
                                        SELECT
                                            n.campaign_group_id
                                            , n.flight_id
                                            , COALESCE(o.eod_utc_spend              , 0)::NUMERIC AS eod_utc_spend__old
                                            , COALESCE(n.eod_utc_spend              , 0)::NUMERIC AS eod_utc_spend__new
                                            , COALESCE(o.utc_yesterday_media_cost   , 0)::NUMERIC AS utc_yesterday_media_cost__old
                                            , COALESCE(n.utc_yesterday_media_cost   , 0)::NUMERIC AS utc_yesterday_media_cost__new
                                            , COALESCE(o.eod_utc_cost               , 0)::NUMERIC AS eod_utc_cost__old
                                            , COALESCE(n.eod_utc_cost               , 0)::NUMERIC AS eod_utc_cost__new
                                            , COALESCE(o.flight_impressions         , 0)::NUMERIC AS flight_impressions__old
                                            , COALESCE(n.flight_impressions         , 0)::NUMERIC AS flight_impressions__new
                                            , COALESCE(o.eod_utc_impressions        , 0)::NUMERIC AS eod_utc_impressions__old
                                            , COALESCE(n.eod_utc_impressions        , 0)::NUMERIC AS eod_utc_impressions__new
                                            , COALESCE(o.utc_yesterday_impressions  , 0)::NUMERIC AS utc_yesterday_impressions__old
                                            , COALESCE(n.utc_yesterday_impressions  , 0)::NUMERIC AS utc_yesterday_impressions__new
                                            , COALESCE(o.flight_cost                , 0)::NUMERIC AS flight_cost__old
                                            , COALESCE(n.flight_cost                , 0)::NUMERIC AS flight_cost__new
                                        FROM performance.campaign_group_performance n
                                        JOIN archives.campaign_group_performance_archives o
                                            ON n.campaign_group_id = o.campaign_group_id
                                            AND n.flight_id = o.flight_id
                                            AND o.archive_time = (SELECT MAX(archive_time) FROM archives.campaign_group_performance_archives)
                                    )
                                    , temp_analysis AS (
                                        SELECT
                                            *
                                            , CASE WHEN eod_utc_spend__old > 0 THEN
                                                eod_utc_spend__new / eod_utc_spend__old
                                            END AS eod_utc_spend__diff_pct
                                            , CASE WHEN utc_yesterday_media_cost__old > 0 THEN
                                                utc_yesterday_media_cost__new / utc_yesterday_media_cost__old
                                            END AS utc_yesterday_media_cost__diff_pct
                                            , CASE WHEN eod_utc_cost__old > 0 THEN
                                                eod_utc_cost__new / eod_utc_cost__old
                                            END AS eod_utc_cost__diff_pct
                                            , CASE WHEN flight_impressions__old > 0 THEN
                                                flight_impressions__new / flight_impressions__old
                                            END AS flight_impressions__diff_pct
                                            , CASE WHEN eod_utc_impressions__old > 0 THEN
                                                eod_utc_impressions__new / eod_utc_impressions__old
                                            END AS eod_utc_impressions__diff_pct
                                            , CASE WHEN utc_yesterday_impressions__old > 0 THEN
                                                utc_yesterday_impressions__new / utc_yesterday_impressions__old
                                            END AS utc_yesterday_impressions__diff_pct
                                            , CASE WHEN flight_cost__old > 0 THEN
                                                flight_cost__new / flight_cost__old
                                            END AS flight_cost__diff_pct
                                            , NULLIF(
                                                ARRAY_TO_STRING(
                                                    ARRAY_REMOVE(
                                                        ARRAY[
                                                            CASE WHEN
                                                                eod_utc_spend__old > 0
                                                                AND eod_utc_spend__new / eod_utc_spend__old < 0.95
                                                                THEN 'eod_utc_spend: new less than 95%% of old'
                                                            END,
                                                            CASE WHEN
                                                                utc_yesterday_media_cost__old > 0
                                                                AND utc_yesterday_media_cost__new / utc_yesterday_media_cost__old < 0.95
                                                                THEN 'utc_yesterday_media_cost: new less than 95%% of old'
                                                            END,
                                                            CASE WHEN
                                                                eod_utc_cost__old > 0
                                                                AND eod_utc_cost__new / eod_utc_cost__old < 0.95
                                                                THEN 'eod_utc_cost: new less than 95%% of old'
                                                            END,
                                                            CASE WHEN
                                                                flight_impressions__old > 0
                                                                AND flight_impressions__new / flight_impressions__old < 0.95
                                                                THEN 'flight_impressions: new less than 95%% of old'
                                                            END,
                                                            CASE WHEN
                                                                eod_utc_impressions__old > 0
                                                                AND eod_utc_impressions__new / eod_utc_impressions__old < 0.95
                                                                THEN 'eod_utc_impressions: new less than 95%% of old'
                                                            END,
                                                            CASE WHEN
                                                                utc_yesterday_impressions__old > 0
                                                                AND utc_yesterday_impressions__new / utc_yesterday_impressions__old < 0.95
                                                                THEN 'utc_yesterday_impressions: new less than 95%% of old'
                                                            END,
                                                            CASE WHEN
                                                                flight_cost__old > 0
                                                                AND flight_cost__new / flight_cost__old < 0.95
                                                                THEN 'flight_cost: new less than 95%% of old'
                                                            END
                                                        ],
                                                        NULL
                                                    ),
                                                    ', '
                                                ),
                                                ''
                                            ) AS tests
                                        FROM temp_diff
                                    )
                                    SELECT *, tests IS NULL AS success
                                    FROM temp_analysis
                                    WHERE tests IS NOT NULL
                                """,
                                pagerduty_severity="critical",
                                column_alias_metric="tests",
                            ),
                        )
                        sync >> test_regression

            if table_name in TABLES_THAT_DEPEND_ON_CAMPAIGN_SUMMARY_HOURLY:
                campaign_summary_hourly >> tg
            if table_name in TABLES_THAT_DEPEND_ON_FLIGHT_METRICS_PER2388:
                flight_metrics_per2388 >> tg
