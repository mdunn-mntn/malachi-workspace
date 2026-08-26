# Optimizer coverage — 2026-08-25

65 active DAGs. 25 have a Spark task; 40 do not and are invisible to this tool.

- profiled this sweep: 3
- Spark DAGs with no log this sweep: 22
- no Spark task at all: 40

## Spark DAGs that produced no log

Either they did not run in the window, or the log has not landed.

- `audience_intent` (advertiser_score_distribution_monitor, data_aggregation.advertiser_vertical_scoring_eligible, data_aggregation.advertiser_verticals, data_aggregation.page_views)
- `audience_intent_scoring_14day_lookback` (audience_intent_scoring_staging, fangorn_predictions_vertical)
- `audience_intent_scoring_household_14day_lookback` (fangorn_household_14day_lookback, fangorn_household_predictions_vertical)
- `conversion_signal_backfill_workflow` (submit_batch_dsid_21)
- `create_ip_verticals` (vertical_size_monitor)
- `crm_match_rate` (run_match_rate_spark)
- `feature_store_hourly` (feature_group_1_source.aug_log_ip_hourly, feature_group_1_source.aug_log_ip_vertical_id_hourly)
- `feature_store_setup_model` (feature_group_1_source.aug_log_ip, feature_group_1_source.aug_log_ip_vertical_id, feature_group_1_source.cil_ip, feature_group_1_source.conv_log_ip)
- `feature_store_snapshot` (feature_group_2_derived.conv_log_derived_ip_snap, feature_group_2_derived.conv_log_derived_ip_vertical_id_snap, feature_group_2_derived.core_derived_advertiser_id_snap, feature_group_2_derived.core_derived_campaign_group_id_snap)
- `fpa_site_visit_batch_serverless` (dsid23_guid_log_processing, dsid25_5x5_processing, dsid26_predactiv_processing, dsid28_33across_processing)
- `gcp_mntn_global_data` (mntn_global_data)
- `gcp_tpa_daily_metrics` (tpa_daily_metrics)
- `guid_geos_summary_to_integration` (build_tables.build_guid_geos_summary)
- `hashed_phone_backfill_workflow` (backfill_phone_signal_deepsync_dsid29.backfill_phone_signal_dsid29, backfill_phone_signal_experian_dsid22.backfill_phone_signal_dsid22)
- `hhdsc_build` (hhdsc_ds_13, hhdsc_ds_19)
- `ipdsc_monitor` (monitor_ipdsc_14, monitor_ipdsc_42, monitor_ipdsc_46, monitor_ipdsc_49)
- `materialize_mntn_first_party` (materialize)
- `pixel_page_view_signal_backfill_workflow` (submit_batch_dsid_24, submit_batch_dsid_33, submit_batch_dsid_39, submit_batch_dsid_40)
- `run_test_models` (data_set_b, data_set_bc1, data_set_c, data_set_c1)
- `targeted_signal_crm` (identity_targeted_signal)
- `tpa_ipdsc_export` (ipdsc_bombora, ipdsc_ds_13, ipdsc_ds_14, ipdsc_ds_16)
- `tpa_mntn_id_export` (tpa_mntn_id_export)

## Spark we cannot read

These run Spark on an engine whose plan or metrics are not reachable yet.

- `create_ip_verticals` / `ddp_url_classification` — Databricks job cluster, no cluster_log_conf
- `create_ip_verticals` / `ddp_url_classification_filtered` — Databricks job cluster, no cluster_log_conf
- `create_ip_verticals` / `domain_vertical_mappings` — Databricks job cluster, no cluster_log_conf
- `create_ip_verticals` / `write_verticals` — Databricks job cluster, no cluster_log_conf
- `databricks_guid_geos` / `run_databricks_job` — Databricks job cluster, no cluster_log_conf
- `fetch_common_crawl` / `common_crawl_home_page_content` — Databricks job cluster, no cluster_log_conf
- `fetch_common_crawl` / `common_crawl_home_page_index` — Databricks job cluster, no cluster_log_conf
- `fetch_common_crawl` / `common_crawl_index` — Databricks job cluster, no cluster_log_conf
- `fetch_common_crawl` / `common_crawl_website_home_pages` — Databricks job cluster, no cluster_log_conf
- `keyword_ddp_reporting` / `write_targeted_signal_ds_13` — Databricks job cluster, no cluster_log_conf
- `keyword_ddp_reporting` / `write_targeted_signal_ds_19` — Databricks job cluster, no cluster_log_conf
- `keyword_ddp_reporting` / `write_targeted_signal_ds_19_domain` — Databricks job cluster, no cluster_log_conf
- `missing_domains` / `missing_domains` — Databricks job cluster, no cluster_log_conf
- `mntn_match_audience_sizes` / `batch_prep` — Databricks job cluster, no cluster_log_conf
- `mntn_match_audience_sizes` / `taxonomy_vector_index` — Databricks job cluster, no cluster_log_conf
- `mntn_match_audience_sizes` / `taxonomy_vector_source` — Databricks job cluster, no cluster_log_conf
- `mntn_match_ddp_audit` / `run_ddp_audit` — Databricks job cluster, no cluster_log_conf
- `mntn_match_ddp_audit` / `run_ddp_domains_added_per_vertical` — Databricks job cluster, no cluster_log_conf
- `mntn_match_ddp_audit` / `run_ddp_ips_added_per_vertical` — Databricks job cluster, no cluster_log_conf
- `mntn_match_ddp_rolling_audit` / `run_ddp_rolling_audit` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.categorization_temp` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.mm_taxonomy_update` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.mm_taxonomy_update_bq` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.openai_batch_joined` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.product_categorization` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_post.taxonomy_vector` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_test.test_categorization_temp` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_test.test_mm_taxonomy` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_fetch` / `batch_test.test_product_categorization` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_submit` / `batch_prep.openai_batch_input_formatted` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_submit` / `batch_prep.openai_batch_input_raw` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_submit` / `batch_prep.product_uniques` — Databricks job cluster, no cluster_log_conf
- `mntn_match_incrementals_submit` / `batch_validate` — Databricks job cluster, no cluster_log_conf
- `mntn_match_tpa_export_prep` / `batch_prep` — Databricks job cluster, no cluster_log_conf
- `mntn_match_verticals_precache_v1_1` / `pre_cache_verticals` — Databricks job cluster, no cluster_log_conf
- `vertical_classification_api` / `ddp_vertical_classification_api` — Databricks job cluster, no cluster_log_conf
- `vertical_classification_api` / `response_tests` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `advertiser_verticals_base` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `ip_vertical_matrix_binary` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `ip_vertical_similarity_scores` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `ip_vertical_visits` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `jaccard_similarity_matrix` — Databricks job cluster, no cluster_log_conf
- `vertical_correlation_pipeline` / `vertical_correlation_model` — Databricks job cluster, no cluster_log_conf

## No Spark task

Nothing to profile. Listed so the backlog is not mistaken for the fleet.

- `advertiser_scores_monitor` — ModelPysparkWorkflowOperator
- `airflow_debugger_daily` — @task
- `augmentor_daily_gcs` — @task
- `blocked_guids_export` — @task
- `blocked_ip_addresses_export` — @task
- `bottom_up_keywords_pipeline_run` — PythonOperator
- `category_taxonomy` — @task, BigQueryInsertJobOperator, SQLExecuteQueryOperator
- `dag_run_duration_watchdog` — PythonOperator
- `databricks_guid_geos` — no tasks
- `dlv_pattern_identification` — @task
- `fangorn_hhid_inference_pipeline_run` — GCSObjectExistenceSensor, PythonOperator
- `fangorn_hhid_training_pipeline_run` — GCSObjectExistenceSensor, PythonOperator
- `fangorn_inference_pipeline_run` — GCSObjectExistenceSensor, PythonOperator
- `fangorn_training_pipeline_run` — GCSObjectExistenceSensor, PythonOperator
- `fetch_common_crawl` — @task.short_circuit, GCSListObjectsOperator
- `ga4` — GKEStartPodOperator
- `hashed_email_conversion_log_signals` — TiPysparkBatchOperator
- `hashed_email_deepsync_signals_ds29` — @task, TiPysparkBatchOperator
- `hashed_email_ds_26_signals` — ExternalTaskSensor, TiPysparkBatchOperator
- `hashed_email_experian_signals` — @task, TiPysparkBatchOperator
- `hashed_email_guid_log_signals` — ExternalTaskSensor, TiPysparkBatchOperator
- `keyword_ddp_reporting` — ExternalTaskSensor
- `marketo_data_export` — GKEStartPodOperator
- `missing_domains` — no tasks
- `mntn_match_audience_sizes` — no tasks
- `mntn_match_ddp_audit` — no tasks
- `mntn_match_ddp_rolling_audit` — no tasks
- `mntn_match_incrementals_fetch` — MntnKubePodOperator
- `mntn_match_incrementals_submit` — MntnKubePodOperator
- `mntn_match_tpa_export_prep` — no tasks
- `mntn_match_verticals_precache_v1_1` — ModelPysparkDbxJobOperator
- `monitor_memdb_batch_output` — @task
- `set_gaclid_enabled_flag` — @task
- `spark_optimizer_daily` — @task
- `storage_transfer` — @task, CloudDataTransferServiceS3ToGCSOperator
- `tmobile_blocked_guids_export_dataproc` — @task
- `tmobile_blocked_ip_export_dataproc` — @task
- `url_pattern_identification` — @task
- `vertical_classification_api` — no tasks
- `vertical_correlation_pipeline` — no tasks
