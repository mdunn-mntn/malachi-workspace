# AUDI-1280 tag audit

Source: 78 DAG files (dags/ minus .airflowignore). Live: 75 DAGs fetched 2026-09-03T03:22:48.709631+00:00.
Alerting files: 67. Unwatched at origin/main: 32. Unwatched on this branch: 7.
Watch list at origin/main: ['tpa', 'Machine Learning', 'ml', 'ml_inference', 'ml_training', 'mntn_matched', 'mntn_match', 'audience_intent_scoring', 'vertical_categorization', 'common_crawl_content']
Watch list on this branch: ['tpa', 'Machine Learning', 'ml', 'ml_inference', 'ml_training', 'mntn_matched', 'mntn_match', 'audience_intent_scoring', 'vertical_categorization', 'common_crawl_content', 'Targeting']
Unwatched at main by team config: {'ATTRIBUTION': 7, 'TARGETING': 3, 'TGT': 22}
Files with no live DAG: ['dags/attribution/dlv_parse.py', 'dags/attribution/url_pattern_discovery.py', 'dags/attribution/url_pattern_pipeline.py']
Live DAGs with no source file: []
Source tags missing live (resolver over-resolves): []
Live tags missing from source (resolver under-resolves, P-tags excluded): []

| dag_id | file | team_config | alert_route | tags | watched_at_main | watched | debugger_reply_location | is_paused | live_match | live_only_tags | source_only_tags |
|---|---|---|---|---|---|---|---|---|---|---|---|
| blocked_guids_export | dags/attribution/blocked_guids_export.py | ATTRIBUTION | slack:#monitor-attribution | Attribution P1 icloud | no | no | digest | no | yes |  |  |
| blocked_ip_addresses_export | dags/attribution/blocked_ip_addresses_export.py | ATTRIBUTION | slack:#monitor-attribution | Attribution P1 icloud | no | no | digest | no | yes |  |  |
| dlv_pattern_identification | dags/attribution/dlv_pattern_identification.py | ATTRIBUTION | slack:#monitor-attribution | Attribution P3 attribution dlv url_patterns | no | no | digest | no | yes |  |  |
| ga4 | dags/attribution/ga4.py | ATTRIBUTION | slack:#monitor-attribution | Attribution ga4 | no | no | digest | no | yes |  |  |
| marketo_data_export | dags/attribution/marketo_data_export.py | ATTRIBUTION | slack:#monitor-attribution | Attribution marketo_data_export | no | no | digest | no | yes |  |  |
| set_gaclid_enabled_flag | dags/attribution/set_gaclid_enabled_flag.py | ATTRIBUTION | slack:#monitor-attribution | Attribution P1 | no | no | digest | no | yes |  |  |
| url_pattern_identification | dags/attribution/url_pattern_identification.py | ATTRIBUTION | slack:#monitor-attribution | Attribution P3 attribution url_patterns | no | no | digest | no | yes |  |  |
| ddp_url_verticals_filtered_backfill | dags/ddp_url_verticals_filtered_backfill.py | ML | slack:#monitor-emr | Machine Learning vertical_categorization | yes | yes | digest | yes | yes |  |  |
| domain_vertical_mappings_backfill | dags/domain_vertical_mappings_backfill.py | ML | slack:#monitor-emr | Machine Learning vertical_categorization | yes | yes | digest | yes | yes |  |  |
| vertical_classification_api | dags/machine_learning/ddp_vertical_classification_api.py | ML | slack:#monitor-emr | Machine Learning vertical_categorization | yes | yes | digest | no | yes |  |  |
| mntn_match_audience_sizes | dags/machine_learning/mntn_match_audience_sizes.py | ML | slack:#monitor-emr | Machine Learning mntn_matched | yes | yes | digest | no | yes |  |  |
| mntn_match_incrementals_fetch | dags/machine_learning/mntn_match_incrementals_fetch.py | ML | slack:#monitor-emr | Machine Learning vertical_categorization | yes | yes | digest | no | yes |  |  |
| mntn_match_incrementals_submit | dags/machine_learning/mntn_match_incrementals_submit.py | ML | slack:#monitor-emr | Machine Learning P1 mntn_match openai site_visit_signal | yes | yes | digest | no | yes |  |  |
| mntn_match_tpa_export_prep | dags/machine_learning/mntn_match_tpa_export_prep.py | ML | slack:#monitor-emr | Machine Learning P1 ds_19 mntn_matched | yes | yes | digest | no | yes |  |  |
| mntn_match_verticals_precache_v1_1 | dags/machine_learning/mntn_match_verticals_precache_v1_1.py | ML | slack:#monitor-emr | Machine Learning mntn_matched targeting | yes | yes | digest | no | yes |  |  |
| databricks_guid_geos | dags/targeting/databricks_guid_geos.py | ML | slack:#monitor-emr | Machine Learning P2 guid_geos | yes | yes | digest | no | yes |  |  |
| airflow_debugger_daily | dags/airflow_debugger_daily.py | TARGETING | slack:#monitor-targeting | Targeting audi debugger oncall targeting | no | yes | digest | no | yes |  |  |
| airflow_debugger_rapid | dags/airflow_debugger_rapid.py | TARGETING | slack:#monitor-targeting | Targeting audi debugger oncall targeting | no | yes | digest | no | yes |  |  |
| spark_optimizer_daily | dags/spark_optimizer_daily.py | TARGETING | slack:#monitor-targeting | Targeting audi optimizer spark targeting | no | yes | digest | no | yes |  |  |
| hh_audience_intent | dags/audience_intent/hh_audience_intent.py | TGT | slack:#monitor-tpa | Targeting audience_intent household mntn_id targeting tgt | no | yes | thread | no | yes |  |  |
| conversion_signal_backfill_workflow | dags/conversion_signal/conversion_workflow.py | TGT | slack:#monitor-tpa | P2 Targeting pixel pipeline targeting tgt | no | yes | thread | no | yes |  |  |
| create_ip_verticals | dags/create_ip_vertical_assocations.py | TGT | slack:#monitor-tpa+pagerduty:pagerduty_tgt_events | P0 Targeting tgt | no | yes | thread | no | yes |  |  |
| crm_match_rate | dags/crm/crm_match_rate_dag.py | TGT | slack:#monitor-tpa | P1 Targeting crm match_rate targeting tgt | no | yes | thread | no | yes |  |  |
| fpa_site_visit_batch_serverless | dags/fpa/fpa_vendor_log_batch_ingestion_consolidated.py | TGT | slack:#monitor-tpa | P1 Targeting ddp fpa site_visit_signal targeting tgt | no | yes | thread | no | yes |  |  |
| hashed_phone_backfill_workflow | dags/gcp_hashed_phone_backfill_workflow.py | TGT | slack:#monitor-tpa | P1 Targeting dataproc hashed_phone tgt | no | yes | thread | no | yes |  |  |
| pixel_page_view_signal_backfill_workflow | dags/gcp_page_view_signal_backfill_workflow.py | TGT | slack:#monitor-tpa | P2 Targeting pixel pipeline targeting tgt | no | yes | thread | no | yes |  |  |
| gcp_tpa_daily_metrics | dags/gcp_tpa_daily_metrics_workflow.py | TGT | slack:#monitor-tpa | P2 Targeting gcp targeting tgt tpa tpa metrics | yes | yes | thread | no | yes |  |  |
| audience_intent_scoring_staging | dags/machine_learning/audience_intent_scoring_staging.py | TGT | slack:#monitor-tpa | P1 Targeting audience_intent_scoring fangorn ipdsc ml tgt | yes | yes | thread | yes | yes |  |  |
| bottom_up_keywords_pipeline_run | dags/machine_learning/bottom_up_keywords_pipeline_run.py | TGT | slack:#monitor-tpa | Targeting bottom_up_keywords ml_training tgt vertex_ai | yes | yes | thread | no | yes |  |  |
| audience_intent_scoring_14day_lookback | dags/machine_learning/fangorn_14day_lookback_dag.py | TGT | slack:#monitor-tpa | P1 Targeting audience_intent_scoring fangorn ipdsc ml tgt | yes | yes | thread | no | yes |  |  |
| audience_intent_conversions_scoring_14day_lookback | dags/machine_learning/fangorn_conversions_14day_lookback_dag.py | TGT | slack:#monitor-tpa | P1 Targeting audience_intent_scoring fangorn_conversions ipdsc ml tgt | yes | yes | thread | yes | yes |  |  |
| fangorn_hhid_training_pipeline_run | dags/machine_learning/fangorn_hhid_training_pipeline_run.py | TGT | slack:#monitor-tpa | Targeting dataproc fangorn_hhid ml_training tgt vertex_ai | yes | yes | thread | no | yes |  |  |
| audience_intent_scoring_household_14day_lookback | dags/machine_learning/fangorn_household_14day_lookback_dag.py | TGT | slack:#monitor-tpa | P1 Targeting audience_intent_scoring fangorn ipdsc ml tgt | yes | yes | thread | no | yes |  |  |
| fangorn_training_pipeline_run | dags/machine_learning/fangorn_training_pipeline_run.py | TGT | slack:#monitor-tpa | Targeting dataproc fangorn ml_training tgt vertex_ai | yes | yes | thread | no | yes |  |  |
| mntn_match_ddp_audit | dags/machine_learning/mntn_match_ddp_audit.py | TGT | slack:#monitor-tpa | Targeting audit tgt vertical_categorization | yes | yes | thread | no | yes |  |  |
| mntn_match_ddp_rolling_audit | dags/machine_learning/mntn_match_ddp_rolling_audit.py | TGT | slack:#monitor-tpa | Targeting audit tgt vertical_categorization | yes | yes | thread | no | yes |  |  |
| segment_quality_scoring_weekly | dags/machine_learning/segment_quality_scoring_dag.py | TGT | slack:#monitor-tpa | P2 Targeting household_scoring ml segment_quality tgt ti-956 weekly | yes | yes | thread | yes | yes |  |  |
| vertical_correlation_pipeline | dags/machine_learning/vertical_correlation_pipeline.py | TGT | slack:#monitor-tpa | Targeting behavioral_targeting ml_training tgt vertical_correlation | yes | yes | thread | no | yes |  |  |
| gcp_mntn_global_data | dags/mntn_global_data.py | TGT | slack:#monitor-tpa+pagerduty:pagerduty_tgt_events | P0 Targeting ipdsc mntn_global_data targeting tgt tpa_export | no | yes | thread | no | yes |  |  |
| run_test_models | dags/models/run_test_models.py | TGT | slack:#monitor-tpa | P1 Targeting targeting test_models tgt ti | no | yes | thread | no | yes |  |  |
| monitor_memdb_batch_output | dags/monitor_memdb_batch_output.py | TGT | slack:#monitor-tpa | Targeting auditing monitoring tgt tpa | yes | yes | thread | no | yes |  |  |
| advertiser_scores_monitor | dags/monitoring/advertiser_scores_monitor.py | TGT | slack:#monitor-tpa | P2 Targeting audience_intent monitoring targeting tgt ti | no | yes | thread | no | yes |  |  |
| dag_run_duration_watchdog | dags/monitoring/dag_run_duration_watchdog.py | TGT | slack:#monitor-tpa | P1 Targeting airflow monitoring pagerduty tgt | no | yes | thread | no | yes |  |  |
| fetch_common_crawl | dags/targeting/fetch_common_crawl.py | TGT | slack:#monitor-tpa | P2 Targeting common_crawl_content tgt vertical_categorization | yes | yes | thread | no | yes |  |  |
| guid_geos_summary_to_integration | dags/targeting/guid_geos_summary_to_integration.py | TGT | slack:#monitor-tpa | P3 Targeting guid_geos targeting tgt | no | yes | thread | no | yes |  |  |
| hashed_email_conversion_log_signals | dags/targeting/hashed_email_conversion_log_signals.py | TGT | slack:#monitor-tpa | P1 Targeting hashed_email hashed_email_signal signal targeting tgt | no | yes | thread | no | yes |  |  |
| hashed_email_deepsync_signals_ds29 | dags/targeting/hashed_email_deepsync_signals.py | TGT | slack:#monitor-tpa | P1 Targeting hashed_email hashed_email_signal signal targeting tgt | no | yes | thread | no | yes |  |  |
| hashed_email_ds_26_signals | dags/targeting/hashed_email_ds_26_signals.py | TGT | slack:#monitor-tpa | P1 Targeting hashed_email hashed_email_signal signal targeting tgt | no | yes | thread | no | yes |  |  |
| hashed_email_experian_signals | dags/targeting/hashed_email_experian_signals.py | TGT | slack:#monitor-tpa | P1 Targeting hashed_email hashed_email_signal signal targeting tgt | no | yes | thread | no | yes |  |  |
| hashed_email_guid_log_signals | dags/targeting/hashed_email_guid_log_signals.py | TGT | slack:#monitor-tpa | P1 Targeting hashed_email hashed_email_signal signal targeting tgt | no | yes | thread | no | yes |  |  |
| missing_domains | dags/tpa/missing_domains.py | TGT | slack:#monitor-tpa | Targeting tgt | no | yes | thread | no | yes |  |  |
| hhdsc_build | dags/tpa_export/hhdsc_build.py | TGT | slack:#monitor-tpa+pagerduty:pagerduty_tgt_events | P0 Targeting hhdsc mntn_id targeting tgt | no | yes | thread | no | yes |  |  |
| targeted_signal_crm | dags/tpa_export/targeted_signal_crm.py | TGT | slack:#monitor-tpa | P1 Targeting ipdsc targeted_signal targeting tgt | no | yes | thread | no | yes |  |  |
| vertical_classification_fetch | dags/vertical_classification/vertical_classification_fetch.py | TGT | slack:#monitor-tpa | P1 Targeting openai targeting tgt vertical_classification | no | yes | thread | yes | yes |  |  |
| vertical_classification_submit | dags/vertical_classification/vertical_classification_submit.py | TGT | slack:#monitor-tpa | P1 Targeting openai targeting tgt vertical_classification | no | yes | thread | yes | yes |  |  |
| audience_intent | dags/audience_intent/audience_intent.py | TPA_EXPORT | slack:#alerts-tpa-pipeline+pagerduty:pagerduty_tgt_events | P0 Targeting audience_intent targeting tpa | yes | yes | thread | no | yes |  |  |
| fangorn_conversions_inference_pipeline_run | dags/machine_learning/fangorn_conversions_inference_pipeline_run.py | TPA_EXPORT | slack:#alerts-tpa-pipeline+pagerduty:pagerduty_tgt_events | P0 Targeting dataproc fangorn_conversions ml_inference tpa vertex_ai | yes | yes | thread | no | yes |  |  |
| fangorn_hhid_inference_pipeline_run | dags/machine_learning/fangorn_hhid_inference_pipeline_run.py | TPA_EXPORT | slack:#alerts-tpa-pipeline+pagerduty:pagerduty_tgt_events | P0 Targeting dataproc fangorn_hhid ml_inference tpa vertex_ai | yes | yes | thread | no | yes |  |  |
| fangorn_inference_pipeline_run | dags/machine_learning/fangorn_inference_pipeline_run.py | TPA_EXPORT | slack:#alerts-tpa-pipeline+pagerduty:pagerduty_tgt_events | P0 Targeting dataproc fangorn ml_inference tpa vertex_ai | yes | yes | thread | no | yes |  |  |
| keyword_ddp_reporting | dags/machine_learning/keyword_ddp_reporting.py | TPA_EXPORT | slack:#alerts-tpa-pipeline | Targeting targeted_signal tpa | yes | yes | thread | no | yes |  |  |
| site_network_hourly | dags/models/bidstream_hourly/site_network_hourly.py | TPA_EXPORT | slack:#alerts-tpa-pipeline | P1 Targeting ds-network ipdsc targeting tpa | yes | yes | thread | no | yes |  |  |
| ipdsc_monitor | dags/monitoring/ipdsc_monitor.py | TPA_EXPORT | slack:#alerts-tpa-pipeline | P1 Targeting ipdsc monitoring targeting tpa | yes | yes | thread | no | yes |  |  |
| materialize_mntn_first_party | dags/targeting/materialize_mntn_first_party_dag.py | TPA_EXPORT | slack:#alerts-tpa-pipeline | P1 Targeting ipdsc segment-updates targeting tpa | yes | yes | thread | no | yes |  |  |
| category_taxonomy | dags/tpa/category_taxonomy.py | TPA_EXPORT | slack:#alerts-tpa-pipeline | P1 Targeting targeting taxonomy tpa | yes | yes | thread | no | yes |  |  |
| materialize_mntn_select | dags/tpa_export/materialize_mntn_select.py | TPA_EXPORT | slack:#alerts-tpa-pipeline | P1 Targeting ipdsc targeting tpa | yes | yes | thread | no | yes |  |  |
| tpa_ipdsc_export | dags/tpa_export/tpa_ipdsc_export.py | TPA_EXPORT | slack:#alerts-tpa-pipeline+pagerduty:pagerduty_tgt_events | P0 Targeting ipdsc targeting tpa | yes | yes | thread | no | yes |  |  |
| tpa_mntn_id_export | dags/tpa_export/tpa_mntn_id_export.py | TPA_EXPORT | slack:#alerts-tpa-pipeline | Targeting ipdsc targeting tpa | yes | yes | thread | no | yes |  |  |
| augmentor_daily_gcs | dags/attribution/augmentor_daily_gcs.py |  | email:weiang@mountain.com | attribution augmentor dataproc | no | no | none | no | yes |  |  |
| <dynamic> | dags/attribution/dlv_parse.py |  | none |  | no | no | none |  | no live dag |  |  |
| tmobile_blocked_guids_export_dataproc | dags/attribution/tmobile_blocked_guids_workflow.py |  | email:weiang@mountain.com | attribution dataproc tmobile | no | no | none | no | yes |  |  |
| tmobile_blocked_ip_export_dataproc | dags/attribution/tmobile_blocked_ip_workflow.py |  | email:weiang@mountain.com | attribution dataproc tmobile | no | no | none | no | yes |  |  |
| <dynamic> | dags/attribution/url_pattern_discovery.py |  | none |  | no | no | none |  | no live dag |  |  |
| <dynamic> | dags/attribution/url_pattern_pipeline.py |  | none |  | no | no | none |  | no live dag |  |  |
| create_persistent_history_cluster | dags/create_persistent_history_cluster.py |  | none |  | no | no | none | yes | yes |  |  |
| feature_store_hourly | dags/models/feature_store_hourly.py |  | none | feature_store hourly | no | no | none | no | yes |  |  |
| feature_store_snapshot | dags/models/feature_store_snapshot.py |  | none | feature_store snapshot | no | no | none | no | yes |  |  |
| storage_transfer | dags/storage_transfer.py |  | none |  | no | no | none | no | yes |  |  |
| feature_store_setup_model | dags/models/feature_store_setup_model.py | TGT | none | feature_store | no | no | none | no | yes |  |  |
