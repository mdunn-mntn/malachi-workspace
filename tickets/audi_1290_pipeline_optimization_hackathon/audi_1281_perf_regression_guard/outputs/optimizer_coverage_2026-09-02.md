# Optimizer coverage — 2026-09-02

## 19 scanned jobs could not be tied to a DAG

Any finding they raise appears in the backlog without an Airflow link.

- `Auction Log Augmentor (GCS: pure parquet)` — no DAG in the bundle defines a task with this name
- `CalculateCRMMatchRate` — no DAG in the bundle defines a task with this name
- `T-Mobile Blocked GUIDs (BQ → GCS)` — no DAG in the bundle defines a task with this name
- `T-Mobile Blocked GUIDs Export` — no DAG in the bundle defines a task with this name
- `T-Mobile Blocked IPs (BQ → GCS)` — no DAG in the bundle defines a task with this name
- `adv_score_live_cg_monitor` — no DAG in the bundle defines a task with this name
- `audience_intent_scoring_staging_ds46` — named by 2 DAGs: audience_intent_scoring_14day_lookback, audience_intent_scoring_staging
- `data_source_id 4 for 2026-08-26` — no DAG in the bundle defines a task with this name
- `data_source_id 4 for 2026-08-27` — no DAG in the bundle defines a task with this name
- `data_source_id 4 for 2026-08-30` — no DAG in the bundle defines a task with this name
- `geo_data in prod for 2026-08-26` — no DAG in the bundle defines a task with this name
- `geo_data in prod for 2026-08-27` — no DAG in the bundle defines a task with this name
- `geo_data in prod for 2026-08-28` — no DAG in the bundle defines a task with this name
- `geo_data in prod for 2026-08-29` — no DAG in the bundle defines a task with this name
- `geo_data in prod for 2026-08-30` — no DAG in the bundle defines a task with this name
- `geo_data in prod for 2026-08-31` — no DAG in the bundle defines a task with this name
- `guid_log_ip_advertiser_id` — no DAG in the bundle defines a task with this name
- `intent_score_household_map` — no DAG in the bundle defines a task with this name
- `ipdsc_third_party_audience_builder` — no DAG in the bundle defines a task with this name

65 active DAGs. 29 have a Spark task; 36 do not, of which 1 are cost-profiled via BigQuery or Databricks and 35 are invisible to this tool.

- profiled this sweep: 14
- Spark DAGs with no log this sweep: 15
- no Spark task, cost-profiled: 1
- no Spark task, invisible: 35

## Spark DAGs that produced no log

Either they did not run in the window, or the log has not landed.

- `conversion_signal_backfill_workflow` (submit_batch_dsid_21)
- `crm_match_rate` (run_match_rate_spark)
- `feature_store_snapshot` (feature_group_2_derived.guid_and_conv_log_derived_advertiser_id_dsc_id_snap, feature_group_2_derived.guid_log_derived_ip_vertical_id_snap, feature_group_2_derived.conv_log_derived_ip_snap, feature_group_2_derived.core_derived_advertiser_id_snap)
- `gcp_mntn_global_data` (mntn_global_data)
- `gcp_tpa_daily_metrics` (tpa_daily_metrics)
- `guid_geos_summary_to_integration` (build_tables.build_guid_geos_summary)
- `hashed_email_conversion_log_signals` (populate_hem_data_ds_21)
- `hashed_email_deepsync_signals_ds29` (populate_hem_data_ds_29)
- `hashed_email_ds_26_signals` (populate_hem_data_ds_26)
- `hashed_email_experian_signals` (populate_hem_data_ds_22)
- `hashed_email_guid_log_signals` (populate_hem_data_ds_23)
- `hashed_phone_backfill_workflow` (backfill_phone_signal_experian_dsid22.backfill_phone_signal_dsid22, backfill_phone_signal_deepsync_dsid29.backfill_phone_signal_dsid29)
- `hhdsc_build` (hhdsc_ds_13, hhdsc_ds_19)
- `pixel_page_view_signal_backfill_workflow` (submit_batch_dsid_33, submit_batch_dsid_24, submit_batch_dsid_40, submit_batch_dsid_39)
- `run_test_models` (data_set_snapshot, data_set_snapshot_weekly, data_set_snapshot_monthly, data_set_b)

## Spark we cannot read

These run Spark on an engine whose plan or metrics are not reachable yet.

- `advertiser_scores_monitor` / `adv_score_live_cg_monitor` — managed cluster, no spark.eventLog.dir and not a batch
- `create_ip_verticals` / `ddp_url_classification` — Databricks job cluster, no cluster_log_conf
- `create_ip_verticals` / `ddp_url_classification_filtered` — Databricks job cluster, no cluster_log_conf
- `create_ip_verticals` / `domain_vertical_mappings` — Databricks job cluster, no cluster_log_conf
- `create_ip_verticals` / `write_verticals` — Databricks job cluster, no cluster_log_conf
- `databricks_guid_geos` / `run_databricks_job` — Databricks job cluster, no cluster_log_conf
- `fetch_common_crawl` / `common_crawl_index` — Databricks job cluster, no cluster_log_conf
- `fetch_common_crawl` / `common_crawl_home_page_index` — Databricks job cluster, no cluster_log_conf
- `fetch_common_crawl` / `common_crawl_home_page_content` — Databricks job cluster, no cluster_log_conf
- `fetch_common_crawl` / `common_crawl_website_home_pages` — Databricks job cluster, no cluster_log_conf
- `keyword_ddp_reporting` / `write_targeted_signal_ds_19` — Databricks job cluster, no cluster_log_conf
- `keyword_ddp_reporting` / `write_targeted_signal_ds_13` — Databricks job cluster, no cluster_log_conf
- `keyword_ddp_reporting` / `write_targeted_signal_ds_19_domain` — Databricks job cluster, no cluster_log_conf
- `missing_domains` / `missing_domains` — Databricks job cluster, no cluster_log_conf
- `mntn_match_audience_sizes` / `batch_prep` — Databricks job cluster, no cluster_log_conf
- `mntn_match_audience_sizes` / `taxonomy_vector_source` — Databricks job cluster, no cluster_log_conf
- `mntn_match_audience_sizes` / `taxonomy_vector_index` — Databricks job cluster, no cluster_log_conf
- `mntn_match_ddp_audit` / `run_ddp_audit` — Databricks job cluster, no cluster_log_conf
- `mntn_match_ddp_audit` / `run_ddp_domains_added_per_vertical` — Databricks job cluster, no cluster_log_conf
- `mntn_match_ddp_audit` / `run_ddp_ips_added_per_vertical` — Databricks job cluster, no cluster_log_conf
- `mntn_match_ddp_rolling_audit` / `run_ddp_rolling_audit` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.openai_batch_joined` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.taxonomy_vector` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.categorization_temp` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.mm_taxonomy_update` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.product_categorization` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.mm_taxonomy_update_bq` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_test.test_mm_taxonomy` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_test.test_product_categorization` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_test.test_categorization_temp` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_submit` / `batch_prep.product_uniques` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_submit` / `batch_prep.openai_batch_input_raw` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_submit` / `batch_prep.openai_batch_input_formatted` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_submit` / `batch_validate` — Databricks job cluster, no cluster_log_conf
- `mntn_match_tpa_export_prep` / `batch_prep` — Databricks job cluster, no cluster_log_conf
- `mntn_match_verticals_precache_v1_1` / `pre_cache_verticals` — Databricks job cluster, no cluster_log_conf
- `mntn_match_verticals_precache_v1_1` / `auto_assign_verticals` — Databricks job cluster, no cluster_log_conf
- `run_test_models` / `data_set_a` — managed cluster, no spark.eventLog.dir and not a batch
- `vertical_classification_api` / `ddp_vertical_classification_api` — Databricks job cluster, no cluster_log_conf
- `vertical_classification_api` / `response_tests` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `advertiser_verticals_base` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `ip_vertical_visits` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `ip_vertical_matrix_binary` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `jaccard_similarity_matrix` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `ip_vertical_similarity_scores` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `vertical_correlation_model` — Databricks job cluster, no cluster_log_conf

## No Spark task

No event log to read. A cost tag names the surface that measures the DAG instead; untagged rows are the remaining blind spot.

- `advertiser_scores_monitor` — Spark we cannot read
- `airflow_debugger_daily` — _PythonDecoratedOperator
- `airflow_debugger_rapid` — _PythonDecoratedOperator
- `augmentor_daily_gcs` — DecoratedMappedOperator, _PythonDecoratedOperator
- `blocked_guids_export` — _PythonDecoratedOperator
- `blocked_ip_addresses_export` — _PythonDecoratedOperator
- `bottom_up_keywords_pipeline_run` — PythonOperator
- `category_taxonomy` — BigQueryInsertJobOperator, SQLExecuteQueryOperator, _PythonDecoratedOperator — cost-profiled (bq)
- `dag_run_duration_watchdog` — PythonOperator
- `databricks_guid_geos` — Spark we cannot read
- `dlv_pattern_identification` — DecoratedMappedOperator, _PythonDecoratedOperator
- `fangorn_hhid_inference_pipeline_run` — GCSObjectExistenceSensor, PythonOperator
- `fangorn_hhid_training_pipeline_run` — GCSObjectExistenceSensor, PythonOperator
- `fangorn_inference_pipeline_run` — GCSObjectExistenceSensor, PythonOperator
- `fangorn_training_pipeline_run` — GCSObjectExistenceSensor, PythonOperator
- `fetch_common_crawl` — GCSListObjectsOperator, _ShortCircuitDecoratedOperator
- `ga4` — GKEStartPodOperator
- `keyword_ddp_reporting` — ExternalTaskSensor
- `marketo_data_export` — GKEStartPodOperator
- `missing_domains` — Spark we cannot read
- `mntn_match_audience_sizes` — Spark we cannot read
- `mntn_match_ddp_audit` — Spark we cannot read
- `mntn_match_ddp_rolling_audit` — Spark we cannot read
- `mntn_match_incrementals_fetch` — MntnKubePodOperator
- `mntn_match_incrementals_submit` — MntnKubePodOperator
- `mntn_match_tpa_export_prep` — Spark we cannot read
- `mntn_match_verticals_precache_v1_1` — Spark we cannot read
- `monitor_memdb_batch_output` — _PythonDecoratedOperator
- `set_gaclid_enabled_flag` — _PythonDecoratedOperator
- `spark_optimizer_daily` — _PythonDecoratedOperator
- `storage_transfer` — DecoratedMappedOperator, MappedOperator, _PythonDecoratedOperator
- `tmobile_blocked_guids_export_dataproc` — DecoratedMappedOperator, _PythonDecoratedOperator
- `tmobile_blocked_ip_export_dataproc` — DecoratedMappedOperator, _PythonDecoratedOperator
- `url_pattern_identification` — DecoratedMappedOperator, _PythonDecoratedOperator
- `vertical_classification_api` — Spark we cannot read
- `vertical_correlation_pipeline` — Spark we cannot read
