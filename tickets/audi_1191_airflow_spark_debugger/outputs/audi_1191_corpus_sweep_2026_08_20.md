# AUDI-1191 corpus sweep — 991 logs

Identity oracle is the filename convention written by the acquisition layer.
`Classified` counts logs with a signature; a log with no error text has nothing
to classify and is counted separately, not as a taxonomy gap.

| Outcome | Logs | Identity resolved | Body alone | Body disagrees | Run id | Classified | No error text |
|---|---:|---:|---:|---:|---:|---:|---:|
| success | 831 | 831 | 0 | 0 | 0 | 2 | 826 |
| failed | 84 | 84 | 72 | 0 | 71 | 71 | 2 |
| upstream_failed | 59 | 59 | 0 | 0 | 0 | 0 | 58 |
| skipped | 14 | 14 | 0 | 0 | 0 | 0 | 9 |
| running | 2 | 2 | 0 | 0 | 0 | 0 | 2 |
| scheduled | 1 | 1 | 0 | 0 | 0 | 0 | 1 |

## Headline

- Identity: 991/991 resolved by `parse_log_file`; 72 from the log body alone, 0 contradicting the filename.
- Diagnosable failures (failed + upstream_failed, with error text): 83 of 143.
- Classified: 71/83 (85%) of diagnosable failures.
- Routable without a signature (job id present): 8. These carry no cause in the Airflow log and are resolved by the engine RCA, so they are not taxonomy gaps.
- Fires on a green run: 2 (a signature firing on a success log is a false positive unless the mechanism genuinely occurred).

## Signatures fired

- 26 — `cluster_create_stockout`
- 8 — `task_execution_timeout`
- 6 — `downstream_job_no_local_cause`
- 5 — `slack_notify_failed`
- 4 — `dbt_model_runtime_error`
- 4 — `path_not_found_late_data`
- 4 — `auth_error`
- 4 — `vertex_pipeline_task_failed`
- 3 — `sensor_timeout`
- 2 — `dbt_test_failure`
- 1 — `invalid_output_path_config`
- 1 — `external_task_failed`
- 1 — `pod_evicted_404`
- 1 — `batch_id_attach_trap`
- 1 — `impersonation_unavailable`

## UNCLASSIFIED clusters (with error text)

- **13** — no local cause, routes to engine RCA (job id present)
  - e.g. `on-call/airflow_logs/2026-08-05/033824__tpa_ipdsc_export__ipdsc_ds_17__try1__upstream_failed.log`
- **2** — slack notifier channel_not_found
  - e.g. `on-call/airflow_logs/2026-08-05/110652__url_pattern_identification__run_spark_pattern_identification__map0__try2__failed.log`
- **2** — other: [error] task Server indicated the task shouldn't be running anymore. Terminating process
  - e.g. `on-call/airflow_logs/2026-08-17/193813__tpa_ipdsc_export__bombora_src_prefix__try1__success.log`
- **2** — other: [error] task Dag not found during start up
  - e.g. `on-call/airflow_logs/2026-08-17/194550__tpa_ipdsc_export__get_dt__try1__failed.log`
- **2** — other: [error] task Task failed with exception
  - e.g. `on-call/airflow_logs/2026-08-17/203357__tpa_ipdsc_export__ipdsc__try1__failed.log`
