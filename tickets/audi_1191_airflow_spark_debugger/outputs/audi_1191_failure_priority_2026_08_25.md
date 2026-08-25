# AUDI-1191 — what to fix, ranked

From the 2026-08-25 replay of 211 failed-state logs (23 days on disk, 2026-07-28 to 2026-08-24).

**Ranked by how much of a DAG's noise is ACTIONABLE**, not by raw failure count. A DAG that fails
20 times on a GCP stockout needs capacity work, not debugging; one that fails 5 times on a broken
notifier is a one-line fix nobody has made. `days` is how many distinct days it failed on — a high
count with few days is one bad episode, a low count across many days is a persistent defect.

| Rank | DAG | Logs | Days | Actionable | Weather | No cause | Lead signature |
|---:|---|---:|---:|---:|---:|---:|---|
| 1 | `vertical_classification_api` | 86 | 16 | **18** | 26 | 42 | `UNCLASSIFIED` |
| 2 | `tpa_ipdsc_export` | 37 | 7 | **9** | 0 | 28 | `UNCLASSIFIED` |
| 3 | `set_gaclid_enabled_flag` | 6 | 6 | **6** | 0 | 0 | `slack_notify_failed` |
| 4 | `keyword_ddp_reporting` | 7 | 2 | **5** | 0 | 2 | `analysis_exception` |
| 5 | `ga4` | 5 | 5 | **5** | 0 | 0 | `auth_error` |
| 6 | `mntn_match_incrementals_fetch` | 20 | 1 | **4** | 16 | 0 | `cluster_create_stockout` |
| 7 | `fangorn_inference_pipeline_run` | 12 | 5 | **4** | 7 | 1 | `path_not_found_late_data` |
| 8 | `fangorn_hhid_inference_pipeline_run` | 4 | 2 | **4** | 0 | 0 | `model_alias_not_found` |
| 9 | `bottom_up_keywords_pipeline_run` | 2 | 1 | **2** | 0 | 0 | `db_credential_rejected` |
| 10 | `mntn_match_verticals_precache_v1_1` | 9 | 5 | **1** | 1 | 7 | `UNCLASSIFIED` |
| 11 | `site_network_hourly` | 2 | 2 | **1** | 0 | 1 | `UNCLASSIFIED` |
| 12 | `tpa_mntn_id_export` | 1 | 1 | **1** | 0 | 0 | `path_not_found_late_data` |
| 13 | `audience_intent` | 4 | 2 | **0** | 1 | 3 | `UNCLASSIFIED` |
| 14 | `fpa_site_visit_batch_serverless` | 3 | 1 | **0** | 0 | 3 | `downstream_job_no_local_cause` |
| 15 | `hashed_email_ds_26_signals` | 3 | 2 | **0** | 0 | 3 | `UNCLASSIFIED` |
| 16 | `augmentor_daily_gcs` | 2 | 1 | **0** | 0 | 2 | `downstream_job_no_local_cause` |
| 17 | `databricks_guid_geos` | 2 | 2 | **0** | 0 | 2 | `UNCLASSIFIED` |
| 18 | `hashed_email_guid_log_signals` | 2 | 1 | **0** | 0 | 2 | `UNCLASSIFIED` |
| 19 | `url_pattern_identification` | 2 | 2 | **0** | 2 | 0 | `ttl_exceeded` |
| 20 | `materialize_mntn_select` | 1 | 1 | **0** | 0 | 1 | `downstream_job_no_local_cause` |
| 21 | `mntn_match_audience_sizes` | 1 | 1 | **0** | 1 | 0 | `cluster_create_stockout` |

---

## The actionable work, by cause

| Cause | Logs | What it means | Where |
|---|---:|---|---|
| `task_execution_timeout` | 14 | raise or investigate the timeout budget | vertical_classification_api/ddp_vertical_classification_api, vertical_classification_api/response_tests |
| `path_not_found_late_data` | 10 | an upstream partition was late or absent | fangorn_inference_pipeline_run/challenger_inference_pipeline, fangorn_inference_pipeline_run/daily_drift_pipeline +3 |
| `dbt_model_runtime_error` | 9 | a dbt model raises at runtime | mntn_match_incrementals_fetch/batch_post.taxonomy_vector, mntn_match_verticals_precache_v1_1/pre_cache_verticals +1 |
| `slack_notify_failed` | 6 | the on-failure notifier is broken, not the task | set_gaclid_enabled_flag/send_notification |
| `auth_error` | 5 | a grant or token is missing | ga4/fetch_transaction_conversion_report |
| `analysis_exception` | 5 | a query references something missing | keyword_ddp_reporting/write_targeted_signal_ds_13, keyword_ddp_reporting/write_targeted_signal_ds_19 |
| `model_alias_not_found` | 4 | a model alias was dropped by a re-registration | fangorn_hhid_inference_pipeline_run/challenger_inference_pipeline |
| `db_credential_rejected` | 2 | a database credential is stale | bottom_up_keywords_pipeline_run/training_pipeline |
| `dag_not_found_at_startup` | 2 | a DAG import or bundle problem | tpa_ipdsc_export/get_dt, tpa_ipdsc_export/remove_icloud_ip |
| `impersonation_unavailable` | 1 | impersonation failed | site_network_hourly/site_network_hourly |
| `invalid_output_path_config` | 1 | a path is misconfigured | tpa_ipdsc_export/ipdsc_ds_67 |
| `batch_id_attach_trap` | 1 | the retry reattached to the failed batch | tpa_ipdsc_export/tpa_export |

---

## The split that matters

| | Logs | Share |
|---|---:|---:|
| Actionable — someone can fix this | 60 | 28% |
| Weather — capacity, quota, preemption | 54 | 25% |
| No cause in the log | 97 | 45% |

**Most on-call pages are weather.** That is the argument for AUDI-1217: the quota and stockout work
removes more alert volume than any amount of DAG debugging. The actionable column is where code
changes pay, and it is much smaller than the raw failure count suggests.

