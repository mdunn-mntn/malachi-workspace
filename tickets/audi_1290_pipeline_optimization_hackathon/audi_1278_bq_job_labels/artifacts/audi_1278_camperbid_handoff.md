# AUDI-1278 hand-off: label camperbid's Spark-BigQuery jobs

Owners per `.github/CODEOWNERS` on `SteelHouse/airflow-camperbid`: `@SteelHouse/pacing` and `@SteelHouse/performance-ml`. Diff is against main `707d739`.

## Slack draft (send as is)

Two asks on airflow-camperbid, both small.

1. Can one of you add the two Spark properties below to `dag_utils/google.py` and merge? It's a one-file change, no script edits. It puts `airflow-dag` and `airflow-task` labels on every BigQuery query the Spark-BigQuery connector runs from your Dataproc jobs, which is about 1,100 slot-hours a day that the BigQuery cost report currently shows as unattributed. 97% of it is `bos__spend`.

2. Before merging, can you run one `bos__spend` `tables.*.create` task on the camperbid dev deployment and confirm the job in BigQuery carries the labels? I can run the check query on my side once you tell me it ran, or you can use the one at the bottom.

Why it's this change and not a script edit: the connector submits the query job itself, so the operator's automatic labels never reach it. The connector reads any `bigQueryJobLabel.<key>` option from Spark conf when it's prefixed with `spark.datasource.bigquery.`, and the batch and workflow dicts are already Jinja-templated (the `labels` block renders `{{ dag.dag_id }}` today). After it lands, `bos__spend` will read about 2,600 slot-hours a day on the optimizer report instead of about 1,630. That's attribution moving, not new spend.

## The diff

Two property blocks, one per Dataproc path. Values go through `lower` and `replace('.', '-')` so they match what `BigQueryInsertJobOperator` writes and the optimizer groups both under the same key. Every current task id fits the 63-character label limit; the longest, `tables-campaign_utc_yesterday_costs_impressions_by_hour-create`, is 62.

### `run_dataproc_serverless` (Dataproc Serverless batches: bos, intent_score_threshold_v3 and v4, media_plan_analytics, win_rate_hourly, campaign_avg_cpm)

```diff
@@ def run_dataproc_serverless(
             "runtime_config": {
                 # https://cloud.google.com/dataproc-serverless/docs/reference/rest/v1/RuntimeConfig
                 "version": runtime_version,
                 "properties": {
                     # https://cloud.google.com/dataproc-serverless/docs/concepts/properties
                     # https://spark.apache.org/docs/latest/configuration.html
                     "spark.driver.maxResultSize": "-1",
                     "spark.sql.shuffle.partitions": f"{shuffle_partitions}",
                     "spark.sql.mapKeyDedupPolicy": "LAST_WIN",
+                    "spark.datasource.bigquery.bigQueryJobLabel.airflow-dag": "{{ dag.dag_id | lower }}",
+                    "spark.datasource.bigquery.bigQueryJobLabel.airflow-task": "{{ task.task_id | lower | replace('.', '-') }}",
                     "spark.dataproc.executor.disk.size": "250g",  # this is per executor, default is 100g per core (so 400g here)
```

### `DataprocConfig.asJson` (cluster workflow templates: win_rate, win_rate_bq, intent_score, bid_price_log_aggregation, media_plan_change_log_sync, media_plan_regeneration, network_performance_metrics_sync, tmul_unnested_intent_scores_7day, ml_scores_bidder_sync_verification)

```diff
@@ class DataprocConfig:
                     "pyspark_job": {  # https://cloud.google.com/dataproc/docs/reference/rest/v1/PySparkJob
                         "main_python_file_uri": f"{self.script_path}",
                         "args": self.script_args,
                         ...
                         "properties": {
                             "executor-cores": f"{self.executor_instance_info.count_vCPU}",
                             "num-executors": f"{self.executor_instance_count}",
                             ...
                             "spark.sql.shuffle.partitions": f"{self.shuffle_partitions}",
                             "spark.sql.mapKeyDedupPolicy": "LAST_WIN",
+                            "spark.datasource.bigquery.bigQueryJobLabel.airflow-dag": "{{ dag.dag_id | lower }}",
+                            "spark.datasource.bigquery.bigQueryJobLabel.airflow-task": "{{ task.task_id | lower | replace('.', '-') }}",
                             "spark.jars": ",".join(
```

Both `batch` (DataprocCreateBatchOperator) and `template` (DataprocInstantiateInlineWorkflowTemplateOperator) are template fields, so the nested strings render at run time exactly like the existing `labels` entries.

## What it covers and what it does not

Covered with no script edits: every connector read and write from `spark_scripts/utils/util_spark.py` (`bigquery_load_query`, `bigquery_load_query_v2`, `bigquery_load_table`, `bigquery_save_table`) and the private `_read_bigquery` in `spark_scripts/win_rate_hourly/spark_pipeline.py`, because the connector reads the label options from Spark conf regardless of which helper builds the reader.

Not covered, python-client jobs on the same service account (about 20 jobs and under 1 slot-hour a day):
- `spark_scripts/initial_bvp_V7/bvp_data_refresh_v7.py::load_bq`: pass `labels=` on its `QueryJobConfig`.
- The `autotof_morning_pass` kedro job (olympus repo): the `-- AUTOTOF` script, the `perml.campaign_*` `WHERE run_date = @run_date` reads and the `perml.campaign_frequency_presets` merge. The SQL lives in olympus, so the label goes on the kedro job's BigQuery client.
- The `external.camperbid_prod__hhst_v#__campaign_bucket` merge and 7 load jobs a day: source file not found by code search.

## Owner validation on dev

1. Trigger one `bos__spend` `tables.<table>.create` task on the camperbid dev deployment (service account `airflow-camperbid-dev@mntn-prj-dev-00`).
2. Run this in BigQuery (dev jobs also bill in dw-main-bronze; swap the timestamps for the run window):

```sql
SELECT job_id, statement_type, labels, ROUND(total_slot_ms / 3600000, 2) AS slot_h
FROM `dw-main-bronze`.`region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP('2026-09-03 00:00:00')
  AND creation_time < TIMESTAMP('2026-09-04 00:00:00')
  AND user_email = 'airflow-camperbid-dev@mntn-prj-dev-00.iam.gserviceaccount.com'
ORDER BY creation_time DESC
LIMIT 100
```

3. Pass: every SELECT job from the batch carries `airflow-dag = bos__spend` and `airflow-task = tables-<table>-create`.

## After merge

The daily optimizer report (`gs://mntn-data-archive-prod/optimizer/optimizer_bq_<date>.md`) reads labels straight from the job history, so the unattributed row drops the next day. Four new `bq_heavy_task` rows appear for `bos__spend` (the flight_end_cost pair is about 525 slot-hours a day each). Expected; that is the cost becoming visible, and it feeds AUDI-1277.
