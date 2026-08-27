# AUDI-1191 — every failed task in the window, and what the debugger says about it

Generated 2026-08-27 from `on-call/airflow_logs/` — 25 days pulled, failures on 19 of them, 2026-08-02 to 2026-08-25.
Harness: `artifacts/audi_1191_replay30.py`. Every distinct failure ran the full chain (`orchestrate.investigate`) — the same code path the prod DAG runs — and was rendered through `slack_block.render`, so each block below is what Slack would carry.

**216 failed-state logs collapse to 67 distinct failures**, keyed by `(dag_id, task_id, signature)`.

| | Distinct | Logs |
|---|---:|---:|
| Root-caused, high confidence | 47 | 139 |
| Named condition, low confidence | 20 | 77 |
| Chain errors / crashes | 0 | 0 |

**Nothing crashed.** All 67 ran parse, route, engine RCA, signature, incident match and render without an exception.

---

## 1. Signatures that fired

| Signature | Distinct | Logs | Class |
|---|---:|---:|---|
| `cluster_create_stockout` | 5 | 45 | weather |
| `task_execution_timeout` | 2 | 16 | actionable |
| `downstream_job_no_local_cause` | 7 | 10 | actionable |
| `path_not_found_late_data` | 5 | 10 | actionable |
| `dbt_model_runtime_error` | 3 | 9 | actionable |
| `slack_notify_failed` | 1 | 6 | actionable |
| `analysis_exception` | 2 | 5 | actionable |
| `auth_error` | 1 | 5 | actionable |
| `model_alias_not_found` | 1 | 4 | actionable |
| `quota_exhaustion` | 1 | 4 | weather |
| `dbt_test_failure` | 1 | 3 | actionable |
| `sensor_timeout` | 3 | 3 | actionable |
| `ttl_exceeded` | 2 | 3 | weather |
| `batch_id_missing` | 1 | 2 | actionable |
| `dag_not_found_at_startup` | 2 | 2 | actionable |
| `db_credential_rejected` | 1 | 2 | actionable |
| `task_externally_terminated` | 1 | 2 | weather |
| `batch_id_attach_trap` | 1 | 1 | actionable |
| `external_task_failed` | 1 | 1 | actionable |
| `external_task_target_skipped` | 1 | 1 | actionable |
| `external_task_target_unfinished` | 1 | 1 | actionable |
| `impersonation_unavailable` | 1 | 1 | actionable |
| `invalid_output_path_config` | 1 | 1 | actionable |
| `pod_evicted_404` | 1 | 1 | weather |
| `spot_preemption` | 1 | 1 | weather |

---

## 2. Low-confidence results, by the condition each one names

Every one of these carries a stated reason there is no root cause, not a bare `unclassified`.

| DAG / task | Logs | What the debugger says |
|---|---:|---|
| `vertical_classification_api/response_tests` | 39 | Root cause is vertical_classification_api.ddp_vertical_classification_api: A dbt model raised at runtime (not a data-quality test). The real exception is in the Python traceback printed under the Runt |
| `mntn_match_verticals_precache_v1_1/pre_cache_verticals` | 6 | The pod pre-cache-verticals-hjd4sxyi did not reach Running inside its 120s budget, so the operator deleted it and raised with an empty message. Nothing in this log is the cause: check node capacity an |
| `tpa_ipdsc_export/tpa_export` | 5 | The task never ran; diagnose the upstream task that failed. |
| `tpa_ipdsc_export/insert_file_audits` | 3 | The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed. |
| `tpa_ipdsc_export/trigger_crm_match_rate` | 3 | The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed. |
| `tpa_ipdsc_export/trigger_tpa_daily_metrics` | 3 | The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed. |
| `databricks_guid_geos/run_databricks_job` | 2 | The pod run-databricks-job-xrc0t925 did not reach Running inside its 120s budget, so the operator deleted it and raised with an empty message. Nothing in this log is the cause: check node capacity and |
| `tpa_ipdsc_export/ipdsc_ds_67` | 2 | The task never ran; diagnose the upstream task that failed. |
| `tpa_ipdsc_export/ipdsc_geo` | 2 | The task never ran; diagnose the upstream task that failed. |
| `tpa_ipdsc_export/trigger_mntn_id_export` | 2 | The task never ran; diagnose the upstream task that failed. |
| `audience_intent/intent_score_map` | 1 | Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything. |
| `audience_intent/wait_for_ipdsc_geo` | 1 | The process was killed mid-poke while watching gs://mntn-data-archive-prod/ipdsc_geo/dt=2026-08-18/_SUCCESS; the log stops with no exception and no reschedule. Nothing here is the cause. Check for a c |
| `augmentor_daily_gcs/merge_day_d0` | 1 | Root cause is augmentor_daily_gcs.augment_hour_d0, which raised: ing OpenLineage CompositeTransport emission after the first successful delivery because `continue_on_success=False`. Transport that emi |
| `fangorn_inference_pipeline_run/wait_for_challenger_features` | 1 | A reschedule-mode sensor polled gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07/_SUCCESS 60 time(s) and never saw it. This try holds no ti |
| `hashed_email_ds_26_signals/populate_hem_data_ds_26` | 1 | The task never ran; diagnose the upstream task that failed. |
| `hashed_email_guid_log_signals/populate_hem_data_ds_23` | 1 | The task never ran; diagnose the upstream task that failed. |
| `site_network_hourly/site_network_hourly` | 1 | Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything. |
| `tpa_ipdsc_export/ipdsc_ds_17` | 1 | Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything. |
| `tpa_ipdsc_export/ipdsc_ds_35` | 1 | Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything. |
| `tpa_ipdsc_export/tpa_export_enrich` | 1 | The task never ran; diagnose the upstream task that failed. |

---

## 3. What to fix, ranked by how much of a DAG's noise is actionable

A DAG that fails 25 times on a GCP stockout needs capacity work, not debugging. `days` is how many distinct days it failed on: a high count over few days is one bad episode, a low count across many days is a persistent defect.

| Rank | DAG | Logs | Days | Actionable | Weather | No cause in log |
|---:|---|---:|---:|---:|---:|---:|
| 1 | `vertical_classification_api` | 89 | 17 | **24** | 26 | 39 |
| 2 | `tpa_ipdsc_export` | 37 | 7 | **14** | 0 | 23 |
| 3 | `keyword_ddp_reporting` | 7 | 2 | **7** | 0 | 0 |
| 4 | `set_gaclid_enabled_flag` | 6 | 6 | **6** | 0 | 0 |
| 5 | `ga4` | 5 | 5 | **5** | 0 | 0 |
| 6 | `mntn_match_incrementals_fetch` | 20 | 1 | **4** | 16 | 0 |
| 7 | `fangorn_inference_pipeline_run` | 14 | 5 | **4** | 9 | 1 |
| 8 | `fangorn_hhid_inference_pipeline_run` | 4 | 2 | **4** | 0 | 0 |
| 9 | `fpa_site_visit_batch_serverless` | 3 | 1 | **3** | 0 | 0 |
| 10 | `mntn_match_verticals_precache_v1_1` | 9 | 5 | **2** | 1 | 6 |
| 11 | `hashed_email_ds_26_signals` | 3 | 2 | **2** | 0 | 1 |
| 12 | `bottom_up_keywords_pipeline_run` | 2 | 1 | **2** | 0 | 0 |
| 13 | `audience_intent` | 4 | 2 | **1** | 1 | 2 |
| 14 | `augmentor_daily_gcs` | 2 | 1 | **1** | 0 | 1 |
| 15 | `hashed_email_guid_log_signals` | 2 | 1 | **1** | 0 | 1 |
| 16 | `site_network_hourly` | 2 | 2 | **1** | 0 | 1 |
| 17 | `materialize_mntn_select` | 1 | 1 | **1** | 0 | 0 |
| 18 | `tpa_mntn_id_export` | 1 | 1 | **1** | 0 | 0 |
| 19 | `databricks_guid_geos` | 2 | 2 | **0** | 0 | 2 |
| 20 | `url_pattern_identification` | 2 | 2 | **0** | 2 | 0 |
| 21 | `mntn_match_audience_sizes` | 1 | 1 | **0** | 1 | 0 |

| | Logs | Share |
|---|---:|---:|
| Actionable, someone can fix this | 83 | 38% |
| Weather: capacity, quota, preemption | 56 | 26% |
| No cause in the log, next hop named | 77 | 36% |

**Most on-call pages are weather or a pointer elsewhere.** That is the argument for AUDI-1217: quota and stockout work removes more alert volume than any amount of DAG debugging.

---

## 4. Every distinct failure, with its output

Ordered by how many logs it accounts for.

### `vertical_classification_api` / `response_tests` — UNCLASSIFIED

**39 log(s)** on 2026-08-03, 2026-08-04, 2026-08-05, 2026-08-06, 2026-08-07, 2026-08-10, 2026-08-16, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-21, 2026-08-25 · confidence **low** · representative `on-call/airflow_logs/2026-08-03/214146__vertical_classification_api__response_tests__try1__upstream_failed.log`

```
RCA [low]: vertical_classification_api/response_tests - dbt/model-runtime-error (upstream)
Root cause is vertical_classification_api.ddp_vertical_classification_api: A dbt model raised at runtime (not a data-quality test). The real exception is in the Python traceback printed under the Runtime Error line; dbt's own line numbers are templated and do not match the source file.
Read the Python traceback under the Runtime Error line and fix it in the model's source. dbt's own line numbers are templated and point at the wrong place.
```

Slack block:

```
*What failed*  *vertical_classification_api/response_tests* — dbt/model-runtime-error
*Why*  (walked upstream) Root cause is vertical_classification_api.ddp_vertical_classification_api: A dbt model raised at runtime (not a data-quality test). The real exception is in the Python traceback printed under the Runtime Error line; dbt's own line numbers are templated and do not match the source file.
*Where*  `vertical_classification_api/response_tests`
*How it failed*  the failure is vertical_classification_api.ddp_vertical_classification_api
*Fix*  Read the Python traceback under the Runtime Error line and fix it in the model's source. dbt's own line numbers are templated and point at the wrong place.
```

### `vertical_classification_api` / `ddp_vertical_classification_api` — cluster_create_stockout

**25 log(s)** on 2026-08-10, 2026-08-16, 2026-08-18, 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-10/203840__vertical_classification_api__ddp_vertical_classification_api__try1__failed.log`

Similar incidents: INC-022, INC-008

```
RCA [high]: vertical_classification_api/ddp_vertical_classification_api - infra/zonal-stockout
GCE had no capacity in zone us-central1-c for the requested machine type.
1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-c on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
https://1262887251702944.4.gcp.databricks.com/jobs/492494377764260/runs/187567562119171
```

Slack block:

```
*What failed*  *vertical_classification_api/ddp_vertical_classification_api* — infra/zonal-stockout
*Why*  (settled from evidence) GCE had no capacity in zone us-central1-c for the requested machine type.
*Where*  `vertical_classification_api/ddp_vertical_classification_api` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/vertical_classification_api/runs/scheduled__2026-08-10T19:30:00+00:00|Airflow run> · <https://1262887251702944.4.gcp.databricks.com/jobs/492494377764260/runs/187567562119171|databricks job>
*How it failed*  the refusal names zone us-central1-c
*Fix*  1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-c on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
```

### `mntn_match_incrementals_fetch` / `batch_post.taxonomy_vector` — cluster_create_stockout

**14 log(s)** on 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-19/103045__mntn_match_incrementals_fetch__batch_post.taxonomy_vector__try1__failed.log`

Similar incidents: INC-022, INC-008

```
RCA [high]: mntn_match_incrementals_fetch/batch_post.taxonomy_vector - infra/zonal-stockout
GCE had no capacity in zone us-central1-c for the requested machine type.
1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-c on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
https://1262887251702944.4.gcp.databricks.com/jobs/442654081725019/runs/54170738735327
```

Slack block:

```
*What failed*  *mntn_match_incrementals_fetch/batch_post.taxonomy_vector* — infra/zonal-stockout
*Why*  (settled from evidence) GCE had no capacity in zone us-central1-c for the requested machine type.
*Where*  `mntn_match_incrementals_fetch/batch_post.taxonomy_vector` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/mntn_match_incrementals_fetch/runs/scheduled__2026-08-18T0900000000-adb7b4bf9|Airflow run> · <https://1262887251702944.4.gcp.databricks.com/jobs/442654081725019/runs/54170738735327|databricks job>
*How it failed*  the refusal names zone us-central1-c
*Fix*  1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-c on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
```

### `vertical_classification_api` / `ddp_vertical_classification_api` — task_execution_timeout

**10 log(s)** on 2026-08-04, 2026-08-05, 2026-08-06, 2026-08-07, 2026-08-17, 2026-08-18, 2026-08-21, 2026-08-25 · confidence **high** · representative `on-call/airflow_logs/2026-08-04/163001__vertical_classification_api__ddp_vertical_classification_api__try1__failed.log`

Similar incidents: INC-009, INC-010, INC-011

```
RCA [high]: vertical_classification_api/ddp_vertical_classification_api - timeout/execution
The task outgrew its time limit. Successful runs already use most of the time allowed, so it ran out of time doing real work rather than hanging.
1. Now: raise execution_timeout from 45m to 68m. At the current +11% drift that holds for hundreds of runs. 2. Then find out why it got slower: runtime rose +11% across these runs. Compare the input row count or file count for the same period. 3. If the input did not grow, the task itself got slower: profile the longest stage of a recent successful run against an older one.
https://1262887251702944.4.gcp.databricks.com/jobs/1076089900365300/runs/875822783949209
```

Slack block:

```
*What failed*  *vertical_classification_api/ddp_vertical_classification_api* — timeout/execution
*Why*  (settled from evidence) The task outgrew its time limit. Successful runs already use most of the time allowed, so it ran out of time doing real work rather than hanging.
*Where*  `vertical_classification_api/ddp_vertical_classification_api` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/vertical_classification_api/runs/scheduled__2026-08-04T15:30:00+00:00|Airflow run> · <https://1262887251702944.4.gcp.databricks.com/jobs/1076089900365300/runs/875822783949209|databricks job>
*How it failed*  hit its 45m limit; the last 97 successful runs took 31m rising to 34m (+11%)
*Fix*  1. Now: raise execution_timeout from 45m to 68m. At the current +11% drift that holds for hundreds of runs. 2. Then find out why it got slower: runtime rose +11% across these runs. Compare the input row count or file count for the same period. 3. If the input did not grow, the task itself got slower: profile the longest stage of a recent successful run against an older one.
```

### `mntn_match_verticals_precache_v1_1` / `pre_cache_verticals` — UNCLASSIFIED

**6 log(s)** on 2026-08-20, 2026-08-21, 2026-08-23, 2026-08-24 · confidence **low** · representative `on-call/airflow_logs/2026-08-20/040003__mntn_match_verticals_precache_v1_1__pre_cache_verticals__try1__failed.log`

```
RCA [low]: mntn_match_verticals_precache_v1_1/pre_cache_verticals - unclassified
The pod pre-cache-verticals-hjd4sxyi did not reach Running inside its 120s budget, so the operator deleted it and raised with an empty message. Nothing in this log is the cause: check node capacity and image-pull time for that pod, not the task's code.
```

Slack block:

```
*What failed*  *mntn_match_verticals_precache_v1_1/pre_cache_verticals* — no-cause-in-log
*Why*  (no cause in this log) The pod pre-cache-verticals-hjd4sxyi did not reach Running inside its 120s budget, so the operator deleted it and raised with an empty message. Nothing in this log is the cause: check node capacity and image-pull time for that pod, not the task's code.
*Where*  `mntn_match_verticals_precache_v1_1/pre_cache_verticals` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/mntn_match_verticals_precache_v1_1/runs/scheduled__2026-08-20T0330000000-f7e0bb3d3|Airflow run>
*How it failed*  the pod was deleted after 120s without reaching Running
*Fix*  Check node capacity and image-pull time for that pod, not the task's code.
```

### `set_gaclid_enabled_flag` / `send_notification` — slack_notify_failed

**6 log(s)** on 2026-08-04, 2026-08-05, 2026-08-06, 2026-08-15, 2026-08-18, 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-04/010544__set_gaclid_enabled_flag__send_notification__try2__failed.log`

```
RCA [high]: set_gaclid_enabled_flag/send_notification - config/slack-channel
The Slack notification call failed: the bot is not in the target channel, or the channel id is wrong or renamed.
Invite the app to the channel, or correct the channel id in the DAG config. The task's own work may well have succeeded.
This is not the cause: it hides the task failure the on-failure callback was trying to announce. Read the task's own error, above the callback frames.
```

Slack block:

```
*What failed*  *set_gaclid_enabled_flag/send_notification* — config/slack-channel
*Why*  (matched signature) The Slack notification call failed: the bot is not in the target channel, or the channel id is wrong or renamed.
*Where*  `set_gaclid_enabled_flag/send_notification` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/set_gaclid_enabled_flag/runs/scheduled__2026-08-03T01:00:00+00:00|Airflow run>
*How it failed*  matched on "'exception': SlackApiError"
*Fix*  Invite the app to the channel, or correct the channel id in the DAG config. The task's own work may well have succeeded. Verify against the linked lines before changing anything.
```

### `vertical_classification_api` / `response_tests` — task_execution_timeout

**6 log(s)** on 2026-08-20, 2026-08-23, 2026-08-24, 2026-08-25 · confidence **high** · representative `on-call/airflow_logs/2026-08-20/171620__vertical_classification_api__response_tests__try1__failed.log`

Similar incidents: INC-009, INC-010, INC-011

```
RCA [high]: vertical_classification_api/response_tests - timeout/execution
The task outgrew its time limit. Successful runs already use most of the time allowed, so it ran out of time doing real work rather than hanging.
1. Now: raise execution_timeout from 45m to 68m. At the current +4% drift that holds for hundreds of runs. 2. Then find out why it got slower: runtime rose +4% across these runs. Compare the input row count or file count for the same period. 3. If the input did not grow, the task itself got slower: profile the longest stage of a recent successful run against an older one.
```

Slack block:

```
*What failed*  *vertical_classification_api/response_tests* — timeout/execution
*Why*  (settled from evidence) The task outgrew its time limit. Successful runs already use most of the time allowed, so it ran out of time doing real work rather than hanging.
*Where*  `vertical_classification_api/response_tests` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/vertical_classification_api/runs/scheduled__2026-08-20T15:30:00+00:00|Airflow run>
*How it failed*  hit its 45m limit; the last 90 successful runs took 36m rising to 37m (+4%)
*Fix*  1. Now: raise execution_timeout from 45m to 68m. At the current +4% drift that holds for hundreds of runs. 2. Then find out why it got slower: runtime rose +4% across these runs. Compare the input row count or file count for the same period. 3. If the input did not grow, the task itself got slower: profile the longest stage of a recent successful run against an older one.
```

### `ga4` / `fetch_transaction_conversion_report` — auth_error

**5 log(s)** on 2026-08-04, 2026-08-05, 2026-08-06, 2026-08-15, 2026-08-18 · confidence **high** · representative `on-call/airflow_logs/2026-08-04/113640__ga4__fetch_transaction_conversion_report__try2__failed.log`

Similar incidents: INC-020

```
RCA [high]: ga4/fetch_transaction_conversion_report - auth
The call was refused: User does not have sufficient permissions for this property. To learn more about Property ID, see https://developers.google.com/analytics/devguides/reporting/data/v1/property-id.
1. Now: grant the identity the access that message names, on the resource it names. At MNTN that is a Crossplane change, not a console edit. 2. Then confirm the grant landed before re-running; a retry cannot clear a refusal. 3. If the grant is already in place, the job is running as a different identity than you think: check which service account it actually uses.
```

Slack block:

```
*What failed*  *ga4/fetch_transaction_conversion_report* — auth
*Why*  (settled from evidence) The call was refused: User does not have sufficient permissions for this property. To learn more about Property ID, see https://developers.google.com/analytics/devguides/reporting/data/v1/property-id.
*Where*  `ga4/fetch_transaction_conversion_report` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/ga4/runs/scheduled__2026-08-03T11:00:00+00:00|Airflow run>
*How it failed*  the refusal message the service returned
*Fix*  1. Now: grant the identity the access that message names, on the resource it names. At MNTN that is a Crossplane change, not a console edit. 2. Then confirm the grant landed before re-running; a retry cannot clear a refusal. 3. If the grant is already in place, the job is running as a different identity than you think: check which service account it actually uses.
```

### `tpa_ipdsc_export` / `tpa_export` — UNCLASSIFIED

**5 log(s)** on 2026-08-05, 2026-08-08, 2026-08-17 · confidence **low** · representative `on-call/airflow_logs/2026-08-05/033825__tpa_ipdsc_export__tpa_export__try1__upstream_failed.log`

Similar incidents: INC-016, INC-010, INC-014

```
RCA [low]: tpa_ipdsc_export/tpa_export - no error text in log
https://console.cloud.google.com/dataproc/batches/us-central1/tpa-export-2026-08-04-1785907338?project=mntn-prj-prod-00
The task never ran; diagnose the upstream task that failed.
no driver log via Cloud Logging (check freshness window)
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/tpa_export* — no-cause-in-log
*Why*  (no cause in this log) The task never ran; diagnose the upstream task that failed.
*Where*  `tpa_ipdsc_export/tpa_export` · <https://console.cloud.google.com/dataproc/batches/us-central1/tpa-export-2026-08-04-1785907338?project=mntn-prj-prod-00|dataproc job>
*How it failed*  could not follow the chain: could not identify the run this task ran in
*Fix*  Diagnose the upstream task named above; this one never started.
```

### `fangorn_hhid_inference_pipeline_run` / `challenger_inference_pipeline` — model_alias_not_found

**4 log(s)** on 2026-08-20, 2026-08-21 · confidence **high** · representative `on-call/airflow_logs/2026-08-20/215241__fangorn_hhid_inference_pipeline_run__challenger_inference_pipeline__try1__failed.log`

Similar incidents: INC-024, INC-025

```
RCA [high]: fangorn_hhid_inference_pipeline_run/challenger_inference_pipeline - vertex/model-alias-missing
The inference job resolves its model by alias pattern (e.g. challenger-v*) and the registry has…
Re-apply the alias to the intended model version in the registry. A retry cannot recreate an alias, so every…
https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-hhid-challenger-inference-pipeline-20260820215253?project=mntn-targeting-prj-prod
```

Slack block:

```
*What failed*  *fangorn_hhid_inference_pipeline_run/challenger_inference_pipeline* — vertex/model-alias-missing
*Why*  (matched signature) The inference job resolves its model by alias pattern (e.g. challenger-v*) and the registry has no version carrying it. Re-registering a model drops the aliases it replaces, so this fires on every run until the owner re-applies the alias.
*Where*  `fangorn_hhid_inference_pipeline_run/challenger_inference_pipeline` · <https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-hhid-challenger-inference-pipeline-20260820215253?project=mntn-targeting-prj-prod|vertex job>
*How it failed*  matched on "No version found with alias pattern '{alias}-v*' for model"
*Fix*  Re-apply the alias to the intended model version in the registry. A retry cannot recreate an alias, so every run fails until someone does.
```

### `fangorn_inference_pipeline_run` / `challenger_inference_pipeline` — quota_exhaustion

**4 log(s)** on 2026-08-24 · confidence **high** · representative `on-call/airflow_logs/2026-08-24/180000__fangorn_inference_pipeline_run__challenger_inference_pipeline__try1__failed.log`

Similar incidents: INC-025, INC-008, INC-012

```
RCA [high]: fangorn_inference_pipeline_run/challenger_inference_pipeline - infra/quota
The request needed 145500 DISKS_TOTAL_GB and 74280 were free, short by 71220.
1. Now: list what is consuming DISKS_TOTAL_GB in this region. A single idle cluster holding the headroom looks exactly like a ceiling that is too low (INC-025), and deleting it is faster than a quota request. 2. If nothing is holding it, the ceiling really is too low: raise DISKS_TOTAL_GB for the region. That is the AUDI-1217 work. 3. To unblock this one run without waiting for either, shrink the request below 74280 DISKS_TOTAL_GB.
https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-challenger-inference-pipeline-20260824223801?project=mntn-targeting-prj-prod
```

Slack block:

```
*What failed*  *fangorn_inference_pipeline_run/challenger_inference_pipeline* — infra/quota
*Why*  (settled from evidence) The request needed 145500 DISKS_TOTAL_GB and 74280 were free, short by 71220.
*Where*  `fangorn_inference_pipeline_run/challenger_inference_pipeline` · <https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-challenger-inference-pipeline-20260824223801?project=mntn-targeting-prj-prod|vertex job>
*How it failed*  DISKS_TOTAL_GB: requested 145500, available 74280
*Fix*  1. Now: list what is consuming DISKS_TOTAL_GB in this region. A single idle cluster holding the headroom looks exactly like a ceiling that is too low (INC-025), and deleting it is faster than a quota request. 2. If nothing is holding it, the ceiling really is too low: raise DISKS_TOTAL_GB for the region. That is the AUDI-1217 work. 3. To unblock this one run without waiting for either, shrink the request below 74280 DISKS_TOTAL_GB.
```

### `fangorn_inference_pipeline_run` / `inference_pipeline` — cluster_create_stockout

**4 log(s)** on 2026-08-25 · confidence **high** · representative `on-call/airflow_logs/2026-08-25/180000__fangorn_inference_pipeline_run__inference_pipeline__try1__failed.log`

Similar incidents: INC-008, INC-002, INC-025

```
RCA [high]: fangorn_inference_pipeline_run/inference_pipeline - infra/zonal-stockout
GCE had no capacity in zone us-central1-a for the requested machine type.
1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-a on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-inference-dataproc-pipeline-20260825192427?project=mntn-targeting-prj-prod
```

Slack block:

```
*What failed*  *fangorn_inference_pipeline_run/inference_pipeline* — infra/zonal-stockout
*Why*  (settled from evidence) GCE had no capacity in zone us-central1-a for the requested machine type.
*Where*  `fangorn_inference_pipeline_run/inference_pipeline` · <https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-inference-dataproc-pipeline-20260825192427?project=mntn-targeting-prj-prod|vertex job>
*How it failed*  the refusal names zone us-central1-a
*Fix*  1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-a on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
```

### `mntn_match_incrementals_fetch` / `batch_post.taxonomy_vector` — dbt_model_runtime_error

**4 log(s)** on 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-19/185606__mntn_match_incrementals_fetch__batch_post.taxonomy_vector__try16__failed.log`

Similar incidents: INC-022, INC-006, INC-009

```
RCA [high]: mntn_match_incrementals_fetch/batch_post.taxonomy_vector - dbt/model-runtime-error
A dbt model raised at runtime (not a data-quality test). The real exception is in the Python traceback printed under the Runtime Error line; dbt's own line numbers are templated and do not match the source file.
Read the Python traceback under the Runtime Error line and fix it in the model's source. dbt's own line numbers are templated and point at the wrong place.
```

Slack block:

```
*What failed*  *mntn_match_incrementals_fetch/batch_post.taxonomy_vector* — dbt/model-runtime-error
*Why*  (matched signature) A dbt model raised at runtime (not a data-quality test). The real exception is in the Python traceback printed under the Runtime Error line; dbt's own line numbers are templated and do not match the source file.
*Where*  `mntn_match_incrementals_fetch/batch_post.taxonomy_vector` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/mntn_match_incrementals_fetch/runs/scheduled__2026-08-18T09:00:00+00:00|Airflow run>
*How it failed*  matched on "Runtime Error in model"
*Fix*  Read the Python traceback under the Runtime Error line and fix it in the model's source. dbt's own line numbers are templated and point at the wrong place.
```

### `vertical_classification_api` / `ddp_vertical_classification_api` — dbt_model_runtime_error

**4 log(s)** on 2026-08-03, 2026-08-06, 2026-08-07 · confidence **high** · representative `on-call/airflow_logs/2026-08-03/213002__vertical_classification_api__ddp_vertical_classification_api__try1__failed.log`

Similar incidents: INC-022, INC-009, INC-012

```
RCA [high]: vertical_classification_api/ddp_vertical_classification_api - dbt/model-runtime-error
The model raised ValueError in model ddp_vertical_classification_api: Too many signals to process 176052364 for period between 2026-08-03T20:30:00+00:00 and 2026-08-03T21:30:00+00:00.
1. Now: open the model's source and fix the ValueError. dbt's line numbers are templated, so they point at the wrong line; search for the call in the message. 2. Then re-run this model alone before the full selector, so a second failure is not confused with the first. 3. If the message names a value rather than a bug, the input data changed: check the upstream table for the same period.
https://1262887251702944.4.gcp.databricks.com/jobs/229342401411383/runs/826780216536763
```

Slack block:

```
*What failed*  *vertical_classification_api/ddp_vertical_classification_api* — dbt/model-runtime-error
*Why*  (settled from evidence) The model raised ValueError in model ddp_vertical_classification_api: Too many signals to process 176052364 for period between 2026-08-03T20:30:00+00:00 and 2026-08-03T21:30:00+00:00
*Where*  `vertical_classification_api/ddp_vertical_classification_api` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/vertical_classification_api/runs/scheduled__2026-08-03T20:30:00+00:00|Airflow run> · <https://1262887251702944.4.gcp.databricks.com/jobs/229342401411383/runs/826780216536763|databricks job>
*How it failed*  deepest exception in the traceback under dbt's Runtime Error line
*Fix*  1. Now: open the model's source and fix the ValueError. dbt's line numbers are templated, so they point at the wrong line; search for the call in the message. 2. Then re-run this model alone before the full selector, so a second failure is not confused with the first. 3. If the message names a value rather than a bug, the input data changed: check the upstream table for the same period.
```

### `fangorn_inference_pipeline_run` / `daily_drift_pipeline` — path_not_found_late_data

**3 log(s)** on 2026-08-08, 2026-08-10 · confidence **high** · representative `on-call/airflow_logs/2026-08-08/182808__fangorn_inference_pipeline_run__daily_drift_pipeline__try2__failed.log`

Similar incidents: INC-015, INC-003, INC-002

```
RCA [high]: fangorn_inference_pipeline_run/daily_drift_pipeline - late-data/missing-partition
The job read gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07 before it existed.
1. Now: check whether gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-daily-feature-drift-pipeline-20260808182829?project=mntn-targeting-prj-prod
```

Slack block:

```
*What failed*  *fangorn_inference_pipeline_run/daily_drift_pipeline* — late-data/missing-partition
*Why*  (settled from evidence) The job read gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07 before it existed.
*Where*  `fangorn_inference_pipeline_run/daily_drift_pipeline` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/fangorn_inference_pipeline_run/runs/scheduled__2026-08-06T18:00:00+00:00|Airflow run> · <https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-daily-feature-drift-pipeline-20260808182829?project=mntn-targeting-prj-prod|vertex job>
*How it failed*  the read that failed names gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07
*Fix*  1. Now: check whether gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
```

### `fpa_site_visit_batch_serverless` / `dsid30_augmentor_log_processing` — downstream_job_no_local_cause

**3 log(s)** on 2026-08-07 · confidence **high** · representative `on-call/airflow_logs/2026-08-07/082821__fpa_site_visit_batch_serverless__dsid30_augmentor_log_processing__try2__failed.log`

Similar incidents: INC-012

```
RCA [high]: fpa_site_visit_batch_serverless/dsid30_augmentor_log_processing - boilerplate/cause-one-layer-down
The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
https://console.cloud.google.com/dataproc/batches/us-central1/fpa-dsid30-20260807-20260807t070000-3296?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *fpa_site_visit_batch_serverless/dsid30_augmentor_log_processing* — boilerplate/cause-one-layer-down
*Why*  (matched signature) The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
*Where*  `fpa_site_visit_batch_serverless/dsid30_augmentor_log_processing` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/fpa_site_visit_batch_serverless/runs/scheduled__2026-08-07T07:00:00+00:00|Airflow run> · <https://console.cloud.google.com/dataproc/batches/us-central1/fpa-dsid30-20260807-20260807t070000-3296?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "Dataproc Agent reports job failure"
*Fix*  Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

### `keyword_ddp_reporting` / `write_targeted_signal_ds_13` — analysis_exception

**3 log(s)** on 2026-08-20 · confidence **high** · representative `on-call/airflow_logs/2026-08-20/000612__keyword_ddp_reporting__write_targeted_signal_ds_13__try1__failed.log`

Similar incidents: INC-023, INC-009, INC-006

```
RCA [high]: keyword_ddp_reporting/write_targeted_signal_ds_13 - TABLE_OR_VIEW_NOT_FOUND
The query references `prod.ml.ddp_url_verticals`, which does not resolve.
1. Now: check whether `prod.ml.ddp_url_verticals` exists. If it does, the job's role cannot see it and the fix is a grant. 2. If it does not exist, it was renamed or dropped upstream. Find the new name and update the reference to it. 3. If nothing replaced it, the read itself is stale: remove it, or restore the object with its owner.
https://1262887251702944.4.gcp.databricks.com/jobs/813174683340963/runs/49162563474601
```

Slack block:

```
*What failed*  *keyword_ddp_reporting/write_targeted_signal_ds_13* — query/schema-error
*Why*  (settled from evidence) The query references `prod.ml.ddp_url_verticals`, which does not resolve.
*Where*  `keyword_ddp_reporting/write_targeted_signal_ds_13` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/keyword_ddp_reporting/runs/scheduled__2026-08-18T1500000000-fd145430d|Airflow run> · <https://1262887251702944.4.gcp.databricks.com/jobs/813174683340963/runs/49162563474601|databricks job>
*How it failed*  matched on the unresolved identifier `prod.ml.ddp_url_verticals`
*Fix*  1. Now: check whether `prod.ml.ddp_url_verticals` exists. If it does, the job's role cannot see it and the fix is a grant. 2. If it does not exist, it was renamed or dropped upstream. Find the new name and update the reference to it. 3. If nothing replaced it, the read itself is stale: remove it, or restore the object with its owner.
```

### `tpa_ipdsc_export` / `insert_file_audits` — UNCLASSIFIED

**3 log(s)** on 2026-08-04, 2026-08-05, 2026-08-08 · confidence **low** · representative `on-call/airflow_logs/2026-08-04/051647__tpa_ipdsc_export__insert_file_audits__try1__upstream_failed.log`

Similar incidents: INC-010, INC-014, INC-016

```
RCA [low]: tpa_ipdsc_export/insert_file_audits - late-data/missing-partition (upstream)
The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
The failure is tpa_ipdsc_export.tpa_export, not this task.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/insert_file_audits* — late-data/missing-partition
*Why*  (settled from evidence) The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
*Where*  `tpa_ipdsc_export/insert_file_audits`
*How it failed*  the read that failed names gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67; the failure is tpa_ipdsc_export.tpa_export; 1 other task(s) failed in the same run
*Fix*  1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
```

### `tpa_ipdsc_export` / `ipdsc_ds_17` — path_not_found_late_data

**3 log(s)** on 2026-08-05, 2026-08-08 · confidence **high** · representative `on-call/airflow_logs/2026-08-05/033824__tpa_ipdsc_export__ipdsc_ds_17__try1__upstream_failed.log`

Similar incidents: INC-014, INC-010, INC-016

```
RCA [high]: tpa_ipdsc_export/ipdsc_ds_17 - late-data/missing-partition
The job read gs://mntn-data-partners/partners/sharethis/segments/date=20260803 before it existed.
1. Now: check whether gs://mntn-data-partners/partners/sharethis/segments/date=20260803 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
https://console.cloud.google.com/dataproc/batches/us-central1/ipd-ds-17-x5m-20260804-023500-1?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/ipdsc_ds_17* — late-data/missing-partition
*Why*  (settled from evidence) The job read gs://mntn-data-partners/partners/sharethis/segments/date=20260803 before it existed.
*Where*  `tpa_ipdsc_export/ipdsc_ds_17` · <https://console.cloud.google.com/dataproc/batches/us-central1/ipd-ds-17-x5m-20260804-023500-1?project=mntn-prj-prod-00|dataproc job>
*How it failed*  the read that failed names gs://mntn-data-partners/partners/sharethis/segments/date=20260803
*Fix*  1. Now: check whether gs://mntn-data-partners/partners/sharethis/segments/date=20260803 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
```

### `tpa_ipdsc_export` / `trigger_crm_match_rate` — UNCLASSIFIED

**3 log(s)** on 2026-08-04, 2026-08-05, 2026-08-08 · confidence **low** · representative `on-call/airflow_logs/2026-08-04/051648__tpa_ipdsc_export__trigger_crm_match_rate__try1__upstream_failed.log`

Similar incidents: INC-010, INC-014, INC-016

```
RCA [low]: tpa_ipdsc_export/trigger_crm_match_rate - late-data/missing-partition (upstream)
The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
The failure is tpa_ipdsc_export.tpa_export, not this task.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/trigger_crm_match_rate* — late-data/missing-partition
*Why*  (settled from evidence) The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
*Where*  `tpa_ipdsc_export/trigger_crm_match_rate`
*How it failed*  the read that failed names gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67; the failure is tpa_ipdsc_export.tpa_export; 1 other task(s) failed in the same run
*Fix*  1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
```

### `tpa_ipdsc_export` / `trigger_tpa_daily_metrics` — UNCLASSIFIED

**3 log(s)** on 2026-08-04, 2026-08-05, 2026-08-08 · confidence **low** · representative `on-call/airflow_logs/2026-08-04/051648__tpa_ipdsc_export__trigger_tpa_daily_metrics__try1__upstream_failed.log`

Similar incidents: INC-010, INC-014, INC-016

```
RCA [low]: tpa_ipdsc_export/trigger_tpa_daily_metrics - late-data/missing-partition (upstream)
The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
The failure is tpa_ipdsc_export.tpa_export, not this task.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/trigger_tpa_daily_metrics* — late-data/missing-partition
*Why*  (settled from evidence) The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
*Where*  `tpa_ipdsc_export/trigger_tpa_daily_metrics`
*How it failed*  the read that failed names gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67; the failure is tpa_ipdsc_export.tpa_export; 1 other task(s) failed in the same run
*Fix*  1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
```

### `vertical_classification_api` / `response_tests` — dbt_test_failure

**3 log(s)** on 2026-08-02, 2026-08-03, 2026-08-20 · confidence **high** · representative `on-call/airflow_logs/2026-08-02/105603__vertical_classification_api__response_tests__try1__failed.log`

Similar incidents: INC-005, INC-009, INC-010

```
RCA [high]: vertical_classification_api/response_tests - dbt-test/data-quality
A dbt data-quality test tripped its threshold (the test query returned more failing rows than allowed, e.g. 'Got N results, configured to fail if >M'). The upstream data violated an expectation - route to the model owner to fix the source…
Route to the model owner: either the source data is wrong or the test bound is. Do not re-run, the test trips again on the same rows.
```

Slack block:

```
*What failed*  *vertical_classification_api/response_tests* — dbt-test/data-quality
*Why*  (matched signature) A dbt data-quality test tripped its threshold (the test query returned more failing rows than allowed, e.g. 'Got N results, configured to fail if >M'). The upstream data violated an expectation - route to the model owner to fix the source data or adjust the test bound; not an auto-fixable code crash.
*Where*  `vertical_classification_api/response_tests` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/vertical_classification_api/runs/scheduled__2026-08-02T09:30:00+00:00|Airflow run>
*How it failed*  matched on "2 of 4 FAIL 5580"
*Fix*  Route to the model owner: either the source data is wrong or the test bound is. Do not re-run, the test trips again on the same rows.
```

### `bottom_up_keywords_pipeline_run` / `training_pipeline` — db_credential_rejected

**2 log(s)** on 2026-08-25 · confidence **high** · representative `on-call/airflow_logs/2026-08-25/193145__bottom_up_keywords_pipeline_run__training_pipeline__try1__failed.log`

```
RCA [high]: bottom_up_keywords_pipeline_run/training_pipeline - auth/database-credential
The database rejected the credential itself, which is not the same as a missing grant: the password is wrong, rotated, or t…
Compare the secret's last rotation against the last green run, then repoint the job at the current s…
https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/bottom-up-keywords-training-pipeline-20260825193521?project=mntn-targeting-prj-prod
```

Slack block:

```
*What failed*  *bottom_up_keywords_pipeline_run/training_pipeline* — auth/database-credential
*Why*  (matched signature) The database rejected the credential itself, which is not the same as a missing grant: the password is wrong, rotated, or the secret the job reads is stale.
*Where*  `bottom_up_keywords_pipeline_run/training_pipeline` · <https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/bottom-up-keywords-training-pipeline-20260825193521?project=mntn-targeting-prj-prod|vertex job>
*How it failed*  matched on "PSQLException: FATAL: password authentication failed"
*Fix*  Compare the secret's last rotation against the last green run, then repoint the job at the current secret. Re-running with the same credential fails identically.
```

### `databricks_guid_geos` / `run_databricks_job` — UNCLASSIFIED

**2 log(s)** on 2026-08-20, 2026-08-24 · confidence **low** · representative `on-call/airflow_logs/2026-08-20/010108__databricks_guid_geos__run_databricks_job__try1__failed.log`

```
RCA [low]: databricks_guid_geos/run_databricks_job - unclassified
The pod run-databricks-job-xrc0t925 did not reach Running inside its 120s budget, so the operator deleted it and raised with an empty message. Nothing in this log is the cause: check node capacity and image-pull time for that pod, not the task's code.
```

Slack block:

```
*What failed*  *databricks_guid_geos/run_databricks_job* — no-cause-in-log
*Why*  (no cause in this log) The pod run-databricks-job-xrc0t925 did not reach Running inside its 120s budget, so the operator deleted it and raised with an empty message. Nothing in this log is the cause: check node capacity and image-pull time for that pod, not the task's code.
*Where*  `databricks_guid_geos/run_databricks_job` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/databricks_guid_geos/runs/scheduled__2026-08-20T0001000000-ae514fc5d|Airflow run>
*How it failed*  the pod was deleted after 120s without reaching Running
*Fix*  Check node capacity and image-pull time for that pod, not the task's code.
```

### `keyword_ddp_reporting` / `write_targeted_signal_ds_19` — analysis_exception

**2 log(s)** on 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-19/150519__keyword_ddp_reporting__write_targeted_signal_ds_19__try1__upstream_failed.log`

Similar incidents: INC-009, INC-006, INC-007

```
RCA [high]: keyword_ddp_reporting/write_targeted_signal_ds_19 - query/schema-error
Invalid SQL, missing column/table.
Fix the query: the named table or column does not resolve. Check whether an upstream rename landed before editing the SQL.
```

Slack block:

```
*What failed*  *keyword_ddp_reporting/write_targeted_signal_ds_19* — query/schema-error
*Why*  (matched signature) Invalid SQL, missing column/table.
*Where*  `keyword_ddp_reporting/write_targeted_signal_ds_19` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/keyword_ddp_reporting/runs/scheduled__2026-08-18T1500000000-fd145430d|Airflow run>
*How it failed*  matched on "TABLE_OR_VIEW_NOT_FOUND"
*Fix*  Fix the query: the named table or column does not resolve. Check whether an upstream rename landed before editing the SQL. Verify against the linked lines before changing anything.
```

### `mntn_match_incrementals_fetch` / `batch_post.taxonomy_vector` — task_externally_terminated

**2 log(s)** on 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-19/184052__mntn_match_incrementals_fetch__batch_post.taxonomy_vector__try15__failed.log`

Similar incidents: INC-022, INC-006, INC-005

```
RCA [high]: mntn_match_incrementals_fetch/batch_post.taxonomy_vector - orchestration/externally-killed
Airflow terminated the process; the task did not fail on its own. Usually a clear, a DAG-run reset, or the scheduler adopting the instance. This tr…
Read the earlier try; this one holds no cause. If no earlier try failed, the kill was a clear or a scheduler adoption and nothing is broken.
https://1262887251702944.4.gcp.databricks.com/jobs/388721542088647/runs/467910066307046
```

Slack block:

```
*What failed*  *mntn_match_incrementals_fetch/batch_post.taxonomy_vector* — orchestration/externally-killed
*Why*  (matched signature) Airflow terminated the process; the task did not fail on its own. Usually a clear, a DAG-run reset, or the scheduler adopting the instance. This try's log holds no cause: if the task really is broken, the reason is in an EARLIER try.
*Where*  `mntn_match_incrementals_fetch/batch_post.taxonomy_vector` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/mntn_match_incrementals_fetch/runs/scheduled__2026-08-18T0900000000-adb7b4bf9|Airflow run> · <https://1262887251702944.4.gcp.databricks.com/jobs/388721542088647/runs/467910066307046|databricks job>
*How it failed*  matched on "Server indicated the task shouldn't be running anymore"
*Fix*  Read the earlier try; this one holds no cause. If no earlier try failed, the kill was a clear or a scheduler adoption and nothing is broken.
```

### `tpa_ipdsc_export` / `ipdsc` — batch_id_missing

**2 log(s)** on 2026-08-17 · confidence **high** · representative `on-call/airflow_logs/2026-08-17/203357__tpa_ipdsc_export__ipdsc__try1__failed.log`

Similar incidents: INC-016, INC-005, INC-017

```
RCA [high]: tpa_ipdsc_export/ipdsc - dag_bug/no-batch-id
Airflow logged the batch id as literally 'None': the upstream id-minting task returned nothing, so no batch was ever submitted. The missing id IS the fault.
Fix the upstream task that returns the batch id, whose XCom came back empty. The Spark job never ran, so there is nothing wrong with it.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/ipdsc* — dag_bug/no-batch-id
*Why*  (matched signature) Airflow logged the batch id as literally 'None': the upstream id-minting task returned nothing, so no batch was ever submitted. The missing id IS the fault.
*Where*  `tpa_ipdsc_export/ipdsc`
*How it failed*  matched on "Starting batch None-1"
*Fix*  Fix the upstream task that returns the batch id, whose XCom came back empty. The Spark job never ran, so there is nothing wrong with it. Verify against the linked lines before changing anything.
```

### `tpa_ipdsc_export` / `ipdsc_ds_49` — downstream_job_no_local_cause

**2 log(s)** on 2026-08-21, 2026-08-24 · confidence **high** · representative `on-call/airflow_logs/2026-08-21/032801__tpa_ipdsc_export__ipdsc_ds_49__try1__failed.log`

Similar incidents: INC-012, INC-021, INC-009

```
RCA [high]: tpa_ipdsc_export/ipdsc_ds_49 - boilerplate/cause-one-layer-down
The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
https://console.cloud.google.com/dataproc/batches/us-central1/ipd-ds-49-1q1-20260820-023500-1?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/ipdsc_ds_49* — boilerplate/cause-one-layer-down
*Why*  (matched signature) The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
*Where*  `tpa_ipdsc_export/ipdsc_ds_49` · <https://console.cloud.google.com/dataproc/batches/us-central1/ipd-ds-49-1q1-20260820-023500-1?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "Dataproc Agent reports job failure"
*Fix*  Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

### `tpa_ipdsc_export` / `ipdsc_ds_67` — UNCLASSIFIED

**2 log(s)** on 2026-08-05, 2026-08-08 · confidence **low** · representative `on-call/airflow_logs/2026-08-05/033824__tpa_ipdsc_export__ipdsc_ds_67__try1__upstream_failed.log`

Similar incidents: INC-010, INC-014, INC-016

```
RCA [low]: tpa_ipdsc_export/ipdsc_ds_67 - no error text in log
https://console.cloud.google.com/dataproc/batches/us-central1/ipd-ds-67-ylg-20260804-023500-1?project=mntn-prj-prod-00
The task never ran; diagnose the upstream task that failed.
no driver log via Cloud Logging (check freshness window)
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/ipdsc_ds_67* — no-cause-in-log
*Why*  (no cause in this log) The task never ran; diagnose the upstream task that failed.
*Where*  `tpa_ipdsc_export/ipdsc_ds_67` · <https://console.cloud.google.com/dataproc/batches/us-central1/ipd-ds-67-ylg-20260804-023500-1?project=mntn-prj-prod-00|dataproc job>
*How it failed*  could not follow the chain: could not identify the run this task ran in
*Fix*  Diagnose the upstream task named above; this one never started.
```

### `tpa_ipdsc_export` / `ipdsc_geo` — UNCLASSIFIED

**2 log(s)** on 2026-08-05, 2026-08-08 · confidence **low** · representative `on-call/airflow_logs/2026-08-05/033824__tpa_ipdsc_export__ipdsc_geo__try1__upstream_failed.log`

Similar incidents: INC-010, INC-014, INC-016

```
RCA [low]: tpa_ipdsc_export/ipdsc_geo - no error text in log
https://console.cloud.google.com/dataproc/batches/us-central1/ipdsc-geo-2026-08-04-1785905567?project=mntn-prj-prod-00
The task never ran; diagnose the upstream task that failed.
no driver log via Cloud Logging (check freshness window)
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/ipdsc_geo* — no-cause-in-log
*Why*  (no cause in this log) The task never ran; diagnose the upstream task that failed.
*Where*  `tpa_ipdsc_export/ipdsc_geo` · <https://console.cloud.google.com/dataproc/batches/us-central1/ipdsc-geo-2026-08-04-1785905567?project=mntn-prj-prod-00|dataproc job>
*How it failed*  could not follow the chain: could not identify the run this task ran in
*Fix*  Diagnose the upstream task named above; this one never started.
```

### `tpa_ipdsc_export` / `tpa_export` — path_not_found_late_data

**2 log(s)** on 2026-08-04, 2026-08-17 · confidence **high** · representative `on-call/airflow_logs/2026-08-04/051639__tpa_ipdsc_export__tpa_export__try3__failed.log`

Similar incidents: INC-016, INC-010, INC-014

```
RCA [high]: tpa_ipdsc_export/tpa_export - late-data/missing-partition
The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
https://console.cloud.google.com/dataproc/batches/us-central1/tpa-export-2026-08-03-1785818317?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/tpa_export* — late-data/missing-partition
*Why*  (settled from evidence) The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
*Where*  `tpa_ipdsc_export/tpa_export` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/tpa_ipdsc_export/runs/scheduled__2026-08-03T02:35:00+00:00|Airflow run> · <https://console.cloud.google.com/dataproc/batches/us-central1/tpa-export-2026-08-03-1785818317?project=mntn-prj-prod-00|dataproc job>
*How it failed*  the read that failed names gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67
*Fix*  1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
```

### `tpa_ipdsc_export` / `trigger_mntn_id_export` — UNCLASSIFIED

**2 log(s)** on 2026-08-05, 2026-08-08 · confidence **low** · representative `on-call/airflow_logs/2026-08-05/033824__tpa_ipdsc_export__trigger_mntn_id_export__try1__upstream_failed.log`

Similar incidents: INC-010, INC-014, INC-016

```
RCA [low]: tpa_ipdsc_export/trigger_mntn_id_export - no error text in log
The task never ran; diagnose the upstream task that failed.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/trigger_mntn_id_export* — no-cause-in-log
*Why*  (no cause in this log) The task never ran; diagnose the upstream task that failed.
*Where*  `tpa_ipdsc_export/trigger_mntn_id_export`
*How it failed*  could not follow the chain: could not identify the run this task ran in
*Fix*  Diagnose the upstream task named above; this one never started.
```

### `url_pattern_identification` / `run_spark_pattern_identification` — ttl_exceeded

**2 log(s)** on 2026-08-05, 2026-08-06 · confidence **high** · representative `on-call/airflow_logs/2026-08-05/110652__url_pattern_identification__run_spark_pattern_identification__map0__try2__failed.log`

Similar incidents: INC-005, INC-004, INC-006

```
RCA [high]: url_pattern_identification/run_spark_pattern_identification - ttl/wall-clock
Cancelled at its 14400s TTL (ran 14400s). Usually a perf regression. Profile the Spark event log for spill/skew/uncached recompute;…
Profile the event log for spill, skew or uncached recompute and fix that. Raise the TTL only once the runtime trend explains why the job needs longer.
https://console.cloud.google.com/dataproc/batches/us-central1/url-pat-id-63736-202608051106?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *url_pattern_identification/run_spark_pattern_identification* — ttl/wall-clock
*Why*  (matched signature) Cancelled at its 14400s TTL (ran 14400s). Usually a perf regression. Profile the Spark event log for spill/skew/uncached recompute; a TTL bump alone rarely fixes it.
*Where*  `url_pattern_identification/run_spark_pattern_identification` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/url_pattern_identification/runs/scheduled__2026-08-04T07:00:00+00:00|Airflow run> · <https://console.cloud.google.com/dataproc/batches/us-central1/url-pat-id-63736-202608051106?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "CANCELLED at ttl 14400s"
*Fix*  Profile the event log for spill, skew or uncached recompute and fix that. Raise the TTL only once the runtime trend explains why the job needs longer.
```

### `audience_intent` / `data_aggregation.prospecting_active_campaign_categories` — downstream_job_no_local_cause

**1 log(s)** on 2026-08-24 · confidence **high** · representative `on-call/airflow_logs/2026-08-24/035516__audience_intent__data_aggregation.prospecting_active_campaign_categories__try1__failed.log`

Similar incidents: INC-012, INC-021

```
RCA [high]: audience_intent/data_aggregation.prospecting_active_campaign_categories - boilerplate/cause-one-layer-down
The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
https://console.cloud.google.com/dataproc/batches/us-central1/aud-int-pro-act-20260823-20260824-035519-1?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *audience_intent/data_aggregation.prospecting_active_campaign_categories* — boilerplate/cause-one-layer-down
*Why*  (matched signature) The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
*Where*  `audience_intent/data_aggregation.prospecting_active_campaign_categories` · <https://console.cloud.google.com/dataproc/batches/us-central1/aud-int-pro-act-20260823-20260824-035519-1?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "Dataproc Agent reports job failure"
*Fix*  Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

### `audience_intent` / `household_score_distribution_monitor` — ttl_exceeded

**1 log(s)** on 2026-08-24 · confidence **high** · representative `on-call/airflow_logs/2026-08-24/073909__audience_intent__household_score_distribution_monitor__try1__failed.log`

Similar incidents: INC-004, INC-009, INC-012

```
RCA [high]: audience_intent/household_score_distribution_monitor - ttl/wall-clock
Cancelled at its 3600s TTL (ran 3603s). Usually a perf regression. Profile the Spark event log for spill/skew/uncached recompute;…
Profile the event log for spill, skew or uncached recompute and fix that. Raise the TTL only once the runtime trend explains why the job needs longer.
https://console.cloud.google.com/dataproc/batches/us-central1/aud-int-hou-sco-mon-20260823-20260824-073912-1?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *audience_intent/household_score_distribution_monitor* — ttl/wall-clock
*Why*  (matched signature) Cancelled at its 3600s TTL (ran 3603s). Usually a perf regression. Profile the Spark event log for spill/skew/uncached recompute; a TTL bump alone rarely fixes it.
*Where*  `audience_intent/household_score_distribution_monitor` · <https://console.cloud.google.com/dataproc/batches/us-central1/aud-int-hou-sco-mon-20260823-20260824-073912-1?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "CANCELLED at ttl 3600s"
*Fix*  Profile the event log for spill, skew or uncached recompute and fix that. Raise the TTL only once the runtime trend explains why the job needs longer.
```

### `audience_intent` / `intent_score_map` — UNCLASSIFIED

**1 log(s)** on 2026-08-19 · confidence **low** · representative `on-call/airflow_logs/2026-08-19/061806__audience_intent__intent_score_map__try1__failed.log`

Similar incidents: INC-021, INC-004

```
RCA [low]: audience_intent/intent_score_map - no error text in log
Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything.
```

Slack block:

```
*What failed*  *audience_intent/intent_score_map* — no-cause-in-log
*Why*  (no cause in this log) Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything.
*Where*  `audience_intent/intent_score_map`
*How it failed*  no cause in this log
*Fix*  Check whether it already retried; the worker died before the task could raise.
```

### `audience_intent` / `wait_for_ipdsc_geo` — UNCLASSIFIED

**1 log(s)** on 2026-08-19 · confidence **low** · representative `on-call/airflow_logs/2026-08-19/061806__audience_intent__wait_for_ipdsc_geo__try1__failed.log`

Similar incidents: INC-021, INC-004

```
RCA [low]: audience_intent/wait_for_ipdsc_geo - no error text in log
The process was killed mid-poke while watching gs://mntn-data-archive-prod/ipdsc_geo/dt=2026-08-18/_SUCCESS; the log stops with no exception and no reschedule. Nothing here is the cause. Check for a control-plane or infra event at the last log timestamp before looking at the DAG.
```

Slack block:

```
*What failed*  *audience_intent/wait_for_ipdsc_geo* — no-cause-in-log
*Why*  (no cause in this log) The process was killed mid-poke while watching gs://mntn-data-archive-prod/ipdsc_geo/dt=2026-08-18/_SUCCESS; the log stops with no exception and no reschedule. Nothing here is the cause. Check for a control-plane or infra event at the last log timestamp before looking at the DAG.
*Where*  `audience_intent/wait_for_ipdsc_geo`
*How it failed*  22 poke(s), 0 reschedule(s)
*Fix*  Check whether the awaited object landed, and for a control-plane event at the last timestamp.
```

### `augmentor_daily_gcs` / `augment_hour_d0` — downstream_job_no_local_cause

**1 log(s)** on 2026-08-07 · confidence **high** · representative `on-call/airflow_logs/2026-08-07/030058__augmentor_daily_gcs__augment_hour_d0__map13__try2__failed.log`

Similar incidents: INC-012, INC-008

```
RCA [high]: augmentor_daily_gcs/augment_hour_d0 - boilerplate/cause-one-layer-down
The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
https://console.cloud.google.com/dataproc/batches/us-central1/auction-log-augment-gcs-20260806-h13-260807030101?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *augmentor_daily_gcs/augment_hour_d0* — boilerplate/cause-one-layer-down
*Why*  (matched signature) The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
*Where*  `augmentor_daily_gcs/augment_hour_d0` · <https://console.cloud.google.com/dataproc/batches/us-central1/auction-log-augment-gcs-20260806-h13-260807030101?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "Dataproc Agent reports job failure"
*Fix*  Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

### `augmentor_daily_gcs` / `merge_day_d0` — UNCLASSIFIED

**1 log(s)** on 2026-08-07 · confidence **low** · representative `on-call/airflow_logs/2026-08-07/031945__augmentor_daily_gcs__merge_day_d0__try1__upstream_failed.log`

```
RCA [low]: augmentor_daily_gcs/merge_day_d0 - no error text in log
Root cause is augmentor_daily_gcs.augment_hour_d0, which raised: ing OpenLineage CompositeTransport emission after the first successful delivery because `continue_on_success=False`. Transport that emitted the event: <HttpTransport(name=astro_primary, kind=http, priority=2)> 2026-08-07T03:19:45.271156Z [i
Fix augmentor_daily_gcs.augment_hour_d0. This task never ran, so nothing here needs changing.
```

Slack block:

```
*What failed*  *augmentor_daily_gcs/merge_day_d0* — upstream/root-cause-walked
*Why*  (walked upstream) Root cause is augmentor_daily_gcs.augment_hour_d0, which raised: ing OpenLineage CompositeTransport emission after the first successful delivery because `continue_on_success=False`. Transport that emitted the event: <HttpTransport(name=astro_primary, kind=http, priority=2)> 2026-08-07T03:19:45.271156Z [i
*Where*  `augmentor_daily_gcs/merge_day_d0`
*How it failed*  the failure is augmentor_daily_gcs.augment_hour_d0
*Fix*  Fix augmentor_daily_gcs.augment_hour_d0. This task never ran, so nothing here needs changing.
```

### `fangorn_inference_pipeline_run` / `challenger_inference_pipeline` — cluster_create_stockout

**1 log(s)** on 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-19/010417__fangorn_inference_pipeline_run__challenger_inference_pipeline__try1__failed.log`

Similar incidents: INC-025, INC-008, INC-002

```
RCA [high]: fangorn_inference_pipeline_run/challenger_inference_pipeline - infra/zonal-stockout
GCE had no capacity in zone us-central1-b for the requested machine type.
1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-b on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-challenger-inference-pipeline-20260819010439?project=mntn-targeting-prj-prod
```

Slack block:

```
*What failed*  *fangorn_inference_pipeline_run/challenger_inference_pipeline* — infra/zonal-stockout
*Why*  (settled from evidence) GCE had no capacity in zone us-central1-b for the requested machine type.
*Where*  `fangorn_inference_pipeline_run/challenger_inference_pipeline` · <https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-challenger-inference-pipeline-20260819010439?project=mntn-targeting-prj-prod|vertex job>
*How it failed*  the refusal names zone us-central1-b
*Fix*  1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-b on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
```

### `fangorn_inference_pipeline_run` / `challenger_inference_pipeline` — path_not_found_late_data

**1 log(s)** on 2026-08-08 · confidence **high** · representative `on-call/airflow_logs/2026-08-08/173828__fangorn_inference_pipeline_run__challenger_inference_pipeline__try2__failed.log`

Similar incidents: INC-025, INC-015, INC-002

```
RCA [high]: fangorn_inference_pipeline_run/challenger_inference_pipeline - late-data/missing-partition
The job read gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07 before it existed.
1. Now: check whether gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-challenger-inference-pipeline-20260808173846?project=mntn-targeting-prj-prod
```

Slack block:

```
*What failed*  *fangorn_inference_pipeline_run/challenger_inference_pipeline* — late-data/missing-partition
*Why*  (settled from evidence) The job read gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07 before it existed.
*Where*  `fangorn_inference_pipeline_run/challenger_inference_pipeline` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/fangorn_inference_pipeline_run/runs/scheduled__2026-08-06T18:00:00+00:00|Airflow run> · <https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-challenger-inference-pipeline-20260808173846?project=mntn-targeting-prj-prod|vertex job>
*How it failed*  the read that failed names gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07
*Fix*  1. Now: check whether gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
```

### `fangorn_inference_pipeline_run` / `wait_for_challenger_features` — UNCLASSIFIED

**1 log(s)** on 2026-08-08 · confidence **low** · representative `on-call/airflow_logs/2026-08-08/172725__fangorn_inference_pipeline_run__wait_for_challenger_features__try2__failed.log`

Similar incidents: INC-002, INC-003, INC-008

```
RCA [low]: fangorn_inference_pipeline_run/wait_for_challenger_features - no error text in log
A reschedule-mode sensor polled gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07/_SUCCESS 60 time(s) and never saw it. This try holds no timeout line because the sensor gave up in a different try; the question is whether the object ever landed, not why this log looks healthy.
```

Slack block:

```
*What failed*  *fangorn_inference_pipeline_run/wait_for_challenger_features* — no-cause-in-log
*Why*  (no cause in this log) A reschedule-mode sensor polled gs://mntn-data-archive-prod/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=2026-08-07/_SUCCESS 60 time(s) and never saw it. This try holds no timeout line because the sensor gave up in a different try; the question is whether the object ever landed, not why this log looks healthy.
*Where*  `fangorn_inference_pipeline_run/wait_for_challenger_features`
*How it failed*  60 poke(s), 60 reschedule(s)
*Fix*  Check whether the awaited object landed, and for a control-plane event at the last timestamp.
```

### `hashed_email_ds_26_signals` / `populate_hem_data_ds_26` — UNCLASSIFIED

**1 log(s)** on 2026-08-16 · confidence **low** · representative `on-call/airflow_logs/2026-08-16/021630__hashed_email_ds_26_signals__populate_hem_data_ds_26__try1__upstream_failed.log`

Similar incidents: INC-011, INC-019

```
RCA [low]: hashed_email_ds_26_signals/populate_hem_data_ds_26 - no error text in log
The task never ran; diagnose the upstream task that failed.
```

Slack block:

```
*What failed*  *hashed_email_ds_26_signals/populate_hem_data_ds_26* — no-cause-in-log
*Why*  (no cause in this log) The task never ran; diagnose the upstream task that failed.
*Where*  `hashed_email_ds_26_signals/populate_hem_data_ds_26`
*How it failed*  could not follow the chain: could not identify the run this task ran in
*Fix*  Diagnose the upstream task named above; this one never started.
```

### `hashed_email_ds_26_signals` / `wait_fpa` — external_task_target_skipped

**1 log(s)** on 2026-08-05 · confidence **high** · representative `on-call/airflow_logs/2026-08-05/230648__hashed_email_ds_26_signals__wait_fpa__try2__failed.log`

Similar incidents: INC-011, INC-019, INC-008

```
RCA [high]: hashed_email_ds_26_signals/wait_fpa - sensor/target-skipped-by-design
fpa_site_visit_batch_serverless.dsid26_predactiv_processing is skipped. the target was SKIPPED by design; the sensor should allow it (sk…
Add the target's skip to the sensor's allowed states (skipped_states, or soft_fail) so a by-design skip stops paging. Do not backfill:…
https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/fpa_site_visit_batch_serverless/runs/scheduled__2026-08-05T22:00:00+00:00
```

Slack block:

```
*What failed*  *hashed_email_ds_26_signals/wait_fpa* — sensor/target-skipped-by-design
*Why*  (matched signature) fpa_site_visit_batch_serverless.dsid26_predactiv_processing is skipped. the target was SKIPPED by design; the sensor should allow it (skipped_states / soft_fail), not page
*Where*  `hashed_email_ds_26_signals/wait_fpa` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/hashed_email_ds_26_signals/runs/scheduled__2026-08-05T22:00:00+00:00|Airflow run> · <https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/fpa_site_visit_batch_serverless/runs/scheduled__2026-08-05T22:00:00+00:00|other job>
*How it failed*  matched on "Airflow API state of fpa_site_visit_batch_serverless.dsid26_predactiv_processing"
*Fix*  Add the target's skip to the sensor's allowed states (skipped_states, or soft_fail) so a by-design skip stops paging. Do not backfill: the awaited partition will not land. Verify against the linked lines before changing anything.
```

### `hashed_email_ds_26_signals` / `wait_fpa` — sensor_timeout

**1 log(s)** on 2026-08-16 · confidence **high** · representative `on-call/airflow_logs/2026-08-16/021618__hashed_email_ds_26_signals__wait_fpa__try1__failed.log`

Similar incidents: INC-011, INC-019, INC-009

```
RCA [high]: hashed_email_ds_26_signals/wait_fpa - sensor-timeout
Downstream other job SUCCEEDED, orchestration-only failure.
A sensor watched a partition/upstream that was not ready by its deadline. Often benign (optional 3P partner ski…
Check whether the awaited object exists. Present, clear the sensor. Absent by design (partner skipped…
https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/fpa_site_visit_batch_serverless/runs/scheduled__2026-08-16T01:00:00+00:00
```

Slack block:

```
*What failed*  *hashed_email_ds_26_signals/wait_fpa* — sensor-timeout
*Why*  (matched signature) A sensor watched a partition/upstream that was not ready by its deadline. Often benign (optional 3P partner skipped that day) or the upstream is still running; verify source presence before treating it as a real failure.
*Where*  `hashed_email_ds_26_signals/wait_fpa` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/hashed_email_ds_26_signals/runs/scheduled__2026-08-16T01:00:00+00:00|Airflow run> · <https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/fpa_site_visit_batch_serverless/runs/scheduled__2026-08-16T01:00:00+00:00|other job>
*How it failed*  matched on "AirflowSensorTimeout"; the downstream job SUCCEEDED, so this is orchestration-only
*Fix*  Check whether the awaited object exists. Present, clear the sensor. Absent by design (partner skipped the day), no-op it. Absent unexpectedly, chase the producer.
```

### `hashed_email_guid_log_signals` / `populate_hem_data_ds_23` — UNCLASSIFIED

**1 log(s)** on 2026-08-16 · confidence **low** · representative `on-call/airflow_logs/2026-08-16/021630__hashed_email_guid_log_signals__populate_hem_data_ds_23__try1__upstream_failed.log`

Similar incidents: INC-019

```
RCA [low]: hashed_email_guid_log_signals/populate_hem_data_ds_23 - no error text in log
The task never ran; diagnose the upstream task that failed.
```

Slack block:

```
*What failed*  *hashed_email_guid_log_signals/populate_hem_data_ds_23* — no-cause-in-log
*Why*  (no cause in this log) The task never ran; diagnose the upstream task that failed.
*Where*  `hashed_email_guid_log_signals/populate_hem_data_ds_23`
*How it failed*  could not follow the chain: could not identify the run this task ran in
*Fix*  Diagnose the upstream task named above; this one never started.
```

### `hashed_email_guid_log_signals` / `wait_fpa` — sensor_timeout

**1 log(s)** on 2026-08-16 · confidence **high** · representative `on-call/airflow_logs/2026-08-16/021621__hashed_email_guid_log_signals__wait_fpa__try1__failed.log`

Similar incidents: INC-019, INC-011, INC-009

```
RCA [high]: hashed_email_guid_log_signals/wait_fpa - sensor-timeout
Downstream other job SUCCEEDED, orchestration-only failure.
A sensor watched a partition/upstream that was not ready by its deadline. Often benign (optional 3P partner ski…
Check whether the awaited object exists. Present, clear the sensor. Absent by design (partner skipped…
https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/fpa_site_visit_batch_serverless/runs/scheduled__2026-08-16T01:00:00+00:00
```

Slack block:

```
*What failed*  *hashed_email_guid_log_signals/wait_fpa* — sensor-timeout
*Why*  (matched signature) A sensor watched a partition/upstream that was not ready by its deadline. Often benign (optional 3P partner skipped that day) or the upstream is still running; verify source presence before treating it as a real failure.
*Where*  `hashed_email_guid_log_signals/wait_fpa` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/hashed_email_guid_log_signals/runs/scheduled__2026-08-16T01:00:00+00:00|Airflow run> · <https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/fpa_site_visit_batch_serverless/runs/scheduled__2026-08-16T01:00:00+00:00|other job>
*How it failed*  matched on "AirflowSensorTimeout"; the downstream job SUCCEEDED, so this is orchestration-only
*Fix*  Check whether the awaited object exists. Present, clear the sensor. Absent by design (partner skipped the day), no-op it. Absent unexpectedly, chase the producer.
```

### `keyword_ddp_reporting` / `wait_for_product_categorization` — external_task_failed

**1 log(s)** on 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-19/150513__keyword_ddp_reporting__wait_for_product_categorization__try2__failed.log`

Similar incidents: INC-006, INC-007, INC-008

```
RCA [high]: keyword_ddp_reporting/wait_for_product_categorization - upstream-failure
Downstream other job SUCCEEDED, orchestration-only failure.
The sensor's external task is in a failed state - this task is a symptom, not the cause. ExternalTaskFailedEr…
Resolve the external task's real state first. Skipped means no-op and do not backfill; failed m…
https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/mntn_match_incrementals_fetch/runs/scheduled__2026-08-18T09:00:00+00:00
```

Slack block:

```
*What failed*  *keyword_ddp_reporting/wait_for_product_categorization* — upstream-failure
*Why*  (matched signature) The sensor's external task is in a failed state - this task is a symptom, not the cause. ExternalTaskFailedError uses the SAME message for a SKIPPED external task (producer short-circuited on missing source data = benign partner-data gap, INC-011) as for a truly failed/upstream_failed one (real break, INC-006/007), so resolve the external task's ACTUAL state first: skipped -> check the producer's source_available_<ds> log for 'No source data', no-op the hour, do not backfill; failed/upstream_failed -> audit the upstream chain.
*Where*  `keyword_ddp_reporting/wait_for_product_categorization` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/keyword_ddp_reporting/runs/scheduled__2026-08-18T15:00:00+00:00|Airflow run> · <https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/mntn_match_incrementals_fetch/runs/scheduled__2026-08-18T09:00:00+00:00|other job>
*How it failed*  matched on "ExternalTaskFailedError"; the downstream job SUCCEEDED, so this is orchestration-only
*Fix*  Resolve the external task's real state first. Skipped means no-op and do not backfill; failed means fix that task, because this one is only the symptom.
```

### `keyword_ddp_reporting` / `wait_for_product_categorization` — external_task_target_unfinished

**1 log(s)** on 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-19/150003__keyword_ddp_reporting__wait_for_product_categorization__try1__failed.log`

Similar incidents: INC-006, INC-007, INC-009

```
RCA [high]: keyword_ddp_reporting/wait_for_product_categorization - sensor/target-unfinished-at-poke
mntn_match_incrementals_fetch.batch_post.product_categorization is success. state success is from AFTER the sensor ga…
Give the sensor a window that covers the target's real runtime, or move it later. The target did finish, after the sensor gave up.
https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/mntn_match_incrementals_fetch/runs/scheduled__2026-08-18T09:00:00+00:00
```

Slack block:

```
*What failed*  *keyword_ddp_reporting/wait_for_product_categorization* — sensor/target-unfinished-at-poke
*Why*  (matched signature) mntn_match_incrementals_fetch.batch_post.product_categorization is success. state success is from AFTER the sensor gave up (target ended 2026-08-19T22:11:22.851593Z, sensor failed 2026-08-19T15:00:12.045826Z). At poke time the target had not succeeded, so this was a real wait, not a sensor bug. The target finished on its own, just after the sensor stopped waiting.
*Where*  `keyword_ddp_reporting/wait_for_product_categorization` · <https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags/mntn_match_incrementals_fetch/runs/scheduled__2026-08-18T09:00:00+00:00|other job>
*How it failed*  matched on "Airflow API state of mntn_match_incrementals_fetch.batch_post.product_categorization"
*Fix*  Give the sensor a window that covers the target's real runtime, or move it later. The target did finish, after the sensor gave up.
```

### `materialize_mntn_select` / `materialize` — downstream_job_no_local_cause

**1 log(s)** on 2026-08-06 · confidence **high** · representative `on-call/airflow_logs/2026-08-06/210400__materialize_mntn_select__materialize__try1__failed.log`

Similar incidents: INC-012, INC-018, INC-017

```
RCA [high]: materialize_mntn_select/materialize - boilerplate/cause-one-layer-down
The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
https://console.cloud.google.com/dataproc/batches/us-central1/mntn-select-2026-08-06-1786049114?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *materialize_mntn_select/materialize* — boilerplate/cause-one-layer-down
*Why*  (matched signature) The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
*Where*  `materialize_mntn_select/materialize` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/materialize_mntn_select/runs/scheduled__2026-08-06T19:45:00+00:00|Airflow run> · <https://console.cloud.google.com/dataproc/batches/us-central1/mntn-select-2026-08-06-1786049114?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "Dataproc Agent reports job failure"
*Fix*  Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

### `mntn_match_audience_sizes` / `taxonomy_vector_index` — cluster_create_stockout

**1 log(s)** on 2026-08-19 · confidence **high** · representative `on-call/airflow_logs/2026-08-19/101542__mntn_match_audience_sizes__taxonomy_vector_index__try4__failed.log`

Similar incidents: INC-022, INC-008

```
RCA [high]: mntn_match_audience_sizes/taxonomy_vector_index - infra/zonal-stockout
GCE had no capacity in zone us-central1-b for the requested machine type.
1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-b on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
https://1262887251702944.4.gcp.databricks.com/jobs/189659289138084/runs/1017063373453062
```

Slack block:

```
*What failed*  *mntn_match_audience_sizes/taxonomy_vector_index* — infra/zonal-stockout
*Why*  (settled from evidence) GCE had no capacity in zone us-central1-b for the requested machine type.
*Where*  `mntn_match_audience_sizes/taxonomy_vector_index` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/mntn_match_audience_sizes/runs/scheduled__2026-08-18T09:00:00+00:00|Airflow run> · <https://1262887251702944.4.gcp.databricks.com/jobs/189659289138084/runs/1017063373453062|databricks job>
*How it failed*  the refusal names zone us-central1-b
*Fix*  1. Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on quota rather than capacity and the real cause gets hidden. 2. Then re-run in 1-2 hours. Autozone usually lands outside us-central1-b on the next attempt and the job goes green with no change. 3. If it keeps hitting the same zone, stop retrying: pin a different zone, or widen the machine family so more instance types qualify.
```

### `mntn_match_verticals_precache_v1_1` / `auto_assign_verticals` — spot_preemption

**1 log(s)** on 2026-08-20 · confidence **high** · representative `on-call/airflow_logs/2026-08-20/233001__mntn_match_verticals_precache_v1_1__auto_assign_verticals__try1__failed.log`

Similar incidents: INC-005, INC-012

```
RCA [high]: mntn_match_verticals_precache_v1_1/auto_assign_verticals - infra/spot-preemption
Spot/preemptible instance reclaimed mid-run.
Re-run. If it recurs on the same DAG, move that job's workers off spot or give it a non-preemptible primary pool.
```

Slack block:

```
*What failed*  *mntn_match_verticals_precache_v1_1/auto_assign_verticals* — infra/spot-preemption
*Why*  (matched signature) Spot/preemptible instance reclaimed mid-run.
*Where*  `mntn_match_verticals_precache_v1_1/auto_assign_verticals`
*How it failed*  matched on "PREEMPTIBLE_WITH_FALLBACK"
*Fix*  Re-run. If it recurs on the same DAG, move that job's workers off spot or give it a non-preemptible primary pool.
```

### `mntn_match_verticals_precache_v1_1` / `pre_cache_verticals` — dbt_model_runtime_error

**1 log(s)** on 2026-08-24 · confidence **high** · representative `on-call/airflow_logs/2026-08-24/213002__mntn_match_verticals_precache_v1_1__pre_cache_verticals__try1__failed.log`

Similar incidents: INC-022, INC-009, INC-012

```
RCA [high]: mntn_match_verticals_precache_v1_1/pre_cache_verticals - dbt/model-runtime-error
The model raised JSONDecodeError in model verticals_pre_cache: Expecting value: line 1 column 1 (char 0).
1. Now: open the model's source and fix the JSONDecodeError. dbt's line numbers are templated, so they point at the wrong line; search for the call in the message. 2. Then re-run this model alone before the full selector, so a second failure is not confused with the first. 3. If the message names a value rather than a bug, the input data changed: check the upstream table for the same period.
https://1262887251702944.4.gcp.databricks.com/jobs/678572222363638/runs/344951411123815
```

Slack block:

```
*What failed*  *mntn_match_verticals_precache_v1_1/pre_cache_verticals* — dbt/model-runtime-error
*Why*  (settled from evidence) The model raised JSONDecodeError in model verticals_pre_cache: Expecting value: line 1 column 1 (char 0)
*Where*  `mntn_match_verticals_precache_v1_1/pre_cache_verticals` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/mntn_match_verticals_precache_v1_1/runs/scheduled__2026-08-24T2100000000-d573f96da|Airflow run> · <https://1262887251702944.4.gcp.databricks.com/jobs/678572222363638/runs/344951411123815|databricks job>
*How it failed*  deepest exception in the traceback under dbt's Runtime Error line
*Fix*  1. Now: open the model's source and fix the JSONDecodeError. dbt's line numbers are templated, so they point at the wrong line; search for the call in the message. 2. Then re-run this model alone before the full selector, so a second failure is not confused with the first. 3. If the message names a value rather than a bug, the input data changed: check the upstream table for the same period.
```

### `mntn_match_verticals_precache_v1_1` / `pre_cache_verticals` — downstream_job_no_local_cause

**1 log(s)** on 2026-08-10 · confidence **high** · representative `on-call/airflow_logs/2026-08-10/220716__mntn_match_verticals_precache_v1_1__pre_cache_verticals__try2__failed.log`

Similar incidents: INC-012, INC-007, INC-017

```
RCA [high]: mntn_match_verticals_precache_v1_1/pre_cache_verticals - boilerplate/cause-one-layer-down
The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

Slack block:

```
*What failed*  *mntn_match_verticals_precache_v1_1/pre_cache_verticals* — boilerplate/cause-one-layer-down
*Why*  (matched signature) The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
*Where*  `mntn_match_verticals_precache_v1_1/pre_cache_verticals` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/mntn_match_verticals_precache_v1_1/runs/scheduled__2026-08-10T21:30:00+00:00|Airflow run>
*How it failed*  matched on "returned a failure.\nremote_pod"
*Fix*  Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

### `site_network_hourly` / `site_network_hourly` — UNCLASSIFIED

**1 log(s)** on 2026-08-19 · confidence **low** · representative `on-call/airflow_logs/2026-08-19/055001__site_network_hourly__site_network_hourly__try1__failed.log`

Similar incidents: INC-020, INC-021

```
RCA [low]: site_network_hourly/site_network_hourly - no error text in log
Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything.
```

Slack block:

```
*What failed*  *site_network_hourly/site_network_hourly* — no-cause-in-log
*Why*  (no cause in this log) Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything.
*Where*  `site_network_hourly/site_network_hourly`
*How it failed*  no cause in this log
*Fix*  Check whether it already retried; the worker died before the task could raise.
```

### `site_network_hourly` / `site_network_hourly` — impersonation_unavailable

**1 log(s)** on 2026-08-17 · confidence **high** · representative `on-call/airflow_logs/2026-08-17/075001__site_network_hourly__site_network_hourly__try1__failed.log`

Similar incidents: INC-020, INC-005, INC-012

```
RCA [high]: site_network_hourly/site_network_hourly - transient-infra/iam-503
GCP's credential-minting service returned 503 while impersonating the job service account, so the task died BEFORE submitting anything. N…
Re-run once; the credential service returned 503 and nothing was submitted. If it repeats, raise it with the IAM owners rather than the DAG owner.
https://console.cloud.google.com/dataproc/batches/us-central1/sit-net-hou-fv2-20260817-065000-1?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *site_network_hourly/site_network_hourly* — transient-infra/iam-503
*Why*  (matched signature) GCP's credential-minting service returned 503 while impersonating the job service account, so the task died BEFORE submitting anything. No batch exists, nothing to clean up.
*Where*  `site_network_hourly/site_network_hourly` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/site_network_hourly/runs/scheduled__2026-08-17T06:50:00+00:00|Airflow run> · <https://console.cloud.google.com/dataproc/batches/us-central1/sit-net-hou-fv2-20260817-065000-1?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "Unable to acquire impersonated credentials"
*Fix*  Re-run once; the credential service returned 503 and nothing was submitted. If it repeats, raise it with the IAM owners rather than the DAG owner.
```

### `tpa_ipdsc_export` / `get_dt` — dag_not_found_at_startup

**1 log(s)** on 2026-08-17 · confidence **high** · representative `on-call/airflow_logs/2026-08-17/194550__tpa_ipdsc_export__get_dt__try1__failed.log`

Similar incidents: INC-007, INC-010, INC-011

```
RCA [high]: tpa_ipdsc_export/get_dt - orchestration/dag-not-loaded
The worker could not load the DAG when the task started, so the task died before running any of its own code. Usually a deploy or DAG-bundle race: the scheduler queued the task against a bundle version the worker no longer has.
Re-run once the bundle version settles. Repeating across a deploy means the bundle did not propagate; check the deploy rather than editing the DAG.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/get_dt* — orchestration/dag-not-loaded
*Why*  (matched signature) The worker could not load the DAG when the task started, so the task died before running any of its own code. Usually a deploy or DAG-bundle race: the scheduler queued the task against a bundle version the worker no longer has.
*Where*  `tpa_ipdsc_export/get_dt`
*How it failed*  matched on "Dag not found during start up"
*Fix*  Re-run once the bundle version settles. Repeating across a deploy means the bundle did not propagate; check the deploy rather than editing the DAG.
```

### `tpa_ipdsc_export` / `ipdsc_ds_17` — UNCLASSIFIED

**1 log(s)** on 2026-08-05 · confidence **low** · representative `on-call/airflow_logs/2026-08-05/045256__tpa_ipdsc_export__ipdsc_ds_17__try2__failed.log`

Similar incidents: INC-014, INC-010, INC-016

```
RCA [low]: tpa_ipdsc_export/ipdsc_ds_17 - no error text in log
Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/ipdsc_ds_17* — no-cause-in-log
*Why*  (no cause in this log) Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything.
*Where*  `tpa_ipdsc_export/ipdsc_ds_17`
*How it failed*  no cause in this log
*Fix*  Check whether it already retried; the worker died before the task could raise.
```

### `tpa_ipdsc_export` / `ipdsc_ds_35` — UNCLASSIFIED

**1 log(s)** on 2026-08-19 · confidence **low** · representative `on-call/airflow_logs/2026-08-19/040840__tpa_ipdsc_export__ipdsc_ds_35__try1__failed.log`

Similar incidents: INC-021, INC-010, INC-014

```
RCA [low]: tpa_ipdsc_export/ipdsc_ds_35 - no error text in log
Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/ipdsc_ds_35* — no-cause-in-log
*Why*  (no cause in this log) Empty log on a failed task: the worker died before the task could raise. Check whether it already retried before touching anything.
*Where*  `tpa_ipdsc_export/ipdsc_ds_35`
*How it failed*  no cause in this log
*Fix*  Check whether it already retried; the worker died before the task could raise.
```

### `tpa_ipdsc_export` / `ipdsc_ds_67` — invalid_output_path_config

**1 log(s)** on 2026-08-04 · confidence **high** · representative `on-call/airflow_logs/2026-08-04/051722__tpa_ipdsc_export__ipdsc_ds_67__try3__failed.log`

Similar incidents: INC-010, INC-016, INC-014

```
RCA [high]: tpa_ipdsc_export/ipdsc_ds_67 - code/config-error
A model produced an invalid output path/bucket - often a Python bug where a method reference (e.g. write_location) is passed instead of its call result write_loc…
Fix the path expression in the model. A method reference passed without its call parentheses is the usual cause; a re-run cannot help.
https://console.cloud.google.com/dataproc/batches/us-central1/ipd-ds-67-lu1-20260803-023500-3?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/ipdsc_ds_67* — code/config-error
*Why*  (matched signature) A model produced an invalid output path/bucket - often a Python bug where a method reference (e.g. write_location) is passed instead of its call result write_location(), so the bucket becomes the method's repr. A real code fix in the model, not a re-run.
*Where*  `tpa_ipdsc_export/ipdsc_ds_67` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/tpa_ipdsc_export/runs/scheduled__2026-08-03T02:35:00+00:00|Airflow run> · <https://console.cloud.google.com/dataproc/batches/us-central1/ipd-ds-67-lu1-20260803-023500-3?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "IllegalArgumentException: Invalid GCS bucket name '<bound method BaseModel.write_location"
*Fix*  Fix the path expression in the model. A method reference passed without its call parentheses is the usual cause; a re-run cannot help. Verify against the linked lines before changing anything.
```

### `tpa_ipdsc_export` / `remove_icloud_ip` — dag_not_found_at_startup

**1 log(s)** on 2026-08-17 · confidence **high** · representative `on-call/airflow_logs/2026-08-17/194554__tpa_ipdsc_export__remove_icloud_ip__try1__failed.log`

Similar incidents: INC-007, INC-010, INC-011

```
RCA [high]: tpa_ipdsc_export/remove_icloud_ip - orchestration/dag-not-loaded
The worker could not load the DAG when the task started, so the task died before running any of its own code. Usually a deploy or DAG-bundle race: the scheduler queued the task against a bundle version the worker no longer has.
Re-run once the bundle version settles. Repeating across a deploy means the bundle did not propagate; check the deploy rather than editing the DAG.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/remove_icloud_ip* — orchestration/dag-not-loaded
*Why*  (matched signature) The worker could not load the DAG when the task started, so the task died before running any of its own code. Usually a deploy or DAG-bundle race: the scheduler queued the task against a bundle version the worker no longer has.
*Where*  `tpa_ipdsc_export/remove_icloud_ip`
*How it failed*  matched on "Dag not found during start up"
*Fix*  Re-run once the bundle version settles. Repeating across a deploy means the bundle did not propagate; check the deploy rather than editing the DAG.
```

### `tpa_ipdsc_export` / `tpa_export` — batch_id_attach_trap

**1 log(s)** on 2026-08-17 · confidence **high** · representative `on-call/airflow_logs/2026-08-17/054330__tpa_ipdsc_export__tpa_export__try2__failed.log`

Similar incidents: INC-016, INC-005, INC-017

```
RCA [high]: tpa_ipdsc_export/tpa_export - dag_bug/batch-id-reattach
The batch id is minted once by an upstream task and cached in XCom, so this retry reattached t…
Clear the id-minting task WITH downstream so a new batch id is minted. Clearing this task alone reattach…
https://console.cloud.google.com/dataproc/batches/us-central1/tpa-export-2026-08-16-1786939830?project=mntn-prj-prod-00
This is not the cause: it hides the earlier attempt's failure, which this retry inherited rat…
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/tpa_export* — dag_bug/batch-id-reattach
*Why*  (matched signature) The batch id is minted once by an upstream task and cached in XCom, so this retry reattached to the ALREADY-FAILED batch and inherited its error. The error text here is not a fresh fault.
*Where*  `tpa_ipdsc_export/tpa_export` · <https://console.cloud.google.com/dataproc/batches/us-central1/tpa-export-2026-08-16-1786939830?project=mntn-prj-prod-00|dataproc job>
*How it failed*  matched on "Batch with given id already exists"
*Fix*  Clear the id-minting task WITH downstream so a new batch id is minted. Clearing this task alone reattaches to the same failed batch. Verify against the linked lines before changing anything.
```

### `tpa_ipdsc_export` / `tpa_export_enrich` — UNCLASSIFIED

**1 log(s)** on 2026-08-08 · confidence **low** · representative `on-call/airflow_logs/2026-08-08/025312__tpa_ipdsc_export__tpa_export_enrich__try1__upstream_failed.log`

Similar incidents: INC-010, INC-014, INC-016

```
RCA [low]: tpa_ipdsc_export/tpa_export_enrich - no error text in log
The task never ran; diagnose the upstream task that failed.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/tpa_export_enrich* — no-cause-in-log
*Why*  (no cause in this log) The task never ran; diagnose the upstream task that failed.
*Where*  `tpa_ipdsc_export/tpa_export_enrich`
*How it failed*  could not follow the chain: could not identify the run this task ran in
*Fix*  Diagnose the upstream task named above; this one never started.
```

### `tpa_ipdsc_export` / `wait_ds17_src` — sensor_timeout

**1 log(s)** on 2026-08-05 · confidence **high** · representative `on-call/airflow_logs/2026-08-05/033816__tpa_ipdsc_export__wait_ds17_src__try1__failed.log`

Similar incidents: INC-010, INC-009, INC-011

```
RCA [high]: tpa_ipdsc_export/wait_ds17_src - sensor-timeout
A sensor watched a partition/upstream that was not ready by its deadline. Often benign (optional 3P partner skipped that day) or the upstream is still running; verify source presence before treating it as a real failure.
Check whether the awaited object exists. Present, clear the sensor. Absent by design (partner skipped the day), no-op it. Absent unexpectedly, chase the producer.
```

Slack block:

```
*What failed*  *tpa_ipdsc_export/wait_ds17_src* — sensor-timeout
*Why*  (matched signature) A sensor watched a partition/upstream that was not ready by its deadline. Often benign (optional 3P partner skipped that day) or the upstream is still running; verify source presence before treating it as a real failure.
*Where*  `tpa_ipdsc_export/wait_ds17_src` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/tpa_ipdsc_export/runs/scheduled__2026-08-04T02:35:00+00:00|Airflow run>
*How it failed*  matched on "AirflowSensorTimeout"
*Fix*  Check whether the awaited object exists. Present, clear the sensor. Absent by design (partner skipped the day), no-op it. Absent unexpectedly, chase the producer.
```

### `tpa_mntn_id_export` / `tpa_mntn_id_export` — path_not_found_late_data

**1 log(s)** on 2026-08-04 · confidence **high** · representative `on-call/airflow_logs/2026-08-04/052823__tpa_mntn_id_export__tpa_mntn_id_export__try3__failed.log`

Similar incidents: INC-005, INC-012

```
RCA [high]: tpa_mntn_id_export/tpa_mntn_id_export - late-data/missing-partition
The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
https://console.cloud.google.com/dataproc/batches/us-central1/tpa-mntn-id-20260804-3?project=mntn-prj-prod-00
```

Slack block:

```
*What failed*  *tpa_mntn_id_export/tpa_mntn_id_export* — late-data/missing-partition
*Why*  (settled from evidence) The job read gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 before it existed.
*Where*  `tpa_mntn_id_export/tpa_mntn_id_export` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/tpa_mntn_id_export/runs/manual__2026-08-04T05:04:02.125692+00:00|Airflow run> · <https://console.cloud.google.com/dataproc/batches/us-central1/tpa-mntn-id-20260804-3?project=mntn-prj-prod-00|dataproc job>
*How it failed*  the read that failed names gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67
*Fix*  1. Now: check whether gs://mntn-data-archive-prod/ipdsc/dt=2026-08-03/data_source_id=67 exists. If it does, the producer was simply late and re-running this task is the whole fix. 2. If they do not, the producer failed or was skipped. Diagnose that task; this one is correct to have stopped. 3. Do not widen the sensor window to make this pass. That hides a late producer until it is late enough to matter.
```

### `vertical_classification_api` / `response_tests` — downstream_job_no_local_cause

**1 log(s)** on 2026-08-11 · confidence **high** · representative `on-call/airflow_logs/2026-08-11/120727__vertical_classification_api__response_tests__try1__failed.log`

Similar incidents: INC-012, INC-007, INC-017

```
RCA [high]: vertical_classification_api/response_tests - boilerplate/cause-one-layer-down
The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

Slack block:

```
*What failed*  *vertical_classification_api/response_tests* — boilerplate/cause-one-layer-down
*Why*  (matched signature) The downstream job was submitted and failed, but this Airflow log carries only the wrapper, no cause.
*Where*  `vertical_classification_api/response_tests` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/vertical_classification_api/runs/scheduled__2026-08-11T10:30:00+00:00|Airflow run>
*How it failed*  matched on "returned a failure.\nremote_pod"
*Fix*  Pull the downstream job's own log (Dataproc driver output, or the pod log) and diagnose there. Nothing in the Airflow log is the cause.
```

### `vertical_classification_api` / `response_tests` — pod_evicted_404

**1 log(s)** on 2026-08-16 · confidence **high** · representative `on-call/airflow_logs/2026-08-16/173152__vertical_classification_api__response_tests__try1__failed.log`

Similar incidents: INC-009

```
RCA [high]: vertical_classification_api/response_tests - orchestration/pod-evicted
K8s pod evicted or lost mid-run (orchestration-only; the Spark/Databricks job may have succeeded and written data).
Check whether the underlying job succeeded and wrote its output before re-running. This is orchestration loss, not job failure.
```

Slack block:

```
*What failed*  *vertical_classification_api/response_tests* — orchestration/pod-evicted
*Why*  (matched signature) K8s pod evicted or lost mid-run (orchestration-only; the Spark/Databricks job may have succeeded and written data).
*Where*  `vertical_classification_api/response_tests` · <https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/dags/vertical_classification_api/runs/scheduled__2026-08-16T15:30:00+00:00|Airflow run>
*How it failed*  matched on "istio check"
*Fix*  Check whether the underlying job succeeded and wrote its output before re-running. This is orchestration loss, not job failure.
```

