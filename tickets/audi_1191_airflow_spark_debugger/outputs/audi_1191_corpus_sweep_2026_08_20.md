# AUDI-1191 corpus sweep — 1031 logs

Identity oracle is the filename convention written by the acquisition layer.
`Classified` counts logs with a signature; a log with no error text has nothing
to classify and is counted separately, not as a taxonomy gap.

| Outcome | Logs | Identity resolved | Body alone | Body disagrees | Run id | Classified | No error text |
|---|---:|---:|---:|---:|---:|---:|---:|
| success | 842 | 842 | 0 | 0 | 4 | 5 | 837 |
| failed | 112 | 112 | 77 | 0 | 93 | 93 | 6 |
| upstream_failed | 60 | 60 | 0 | 0 | 1 | 1 | 58 |
| skipped | 14 | 14 | 0 | 0 | 0 | 5 | 9 |
| running | 2 | 2 | 0 | 0 | 0 | 0 | 2 |
| scheduled | 1 | 1 | 0 | 0 | 0 | 0 | 1 |

## Headline

- Identity: 1031/1031 resolved by `parse_log_file`; 77 from the log body alone, 0 contradicting the filename.
- Diagnosable failures (failed + upstream_failed, with error text): 108 of 172.
- Classified: 94/108 (87%) of diagnosable failures.
- Routable without a signature (job id present): 8. These carry no cause in the Airflow log and are resolved by the engine RCA, so they are not taxonomy gaps.
- Fires on a green run: 5 (a signature firing on a success log is a false positive unless the mechanism genuinely occurred).

## Signatures fired

- 40 — `cluster_create_stockout`
- 8 — `dbt_model_runtime_error`
- 8 — `task_execution_timeout`
- 6 — `downstream_job_no_local_cause`
- 5 — `slack_notify_failed`
- 4 — `path_not_found_late_data`
- 4 — `auth_error`
- 4 — `vertex_pipeline_task_failed`
- 3 — `sensor_timeout`
- 2 — `dbt_test_failure`
- 2 — `external_task_failed`
- 2 — `analysis_exception`
- 2 — `task_externally_terminated`
- 1 — `invalid_output_path_config`
- 1 — `pod_evicted_404`
- 1 — `batch_id_attach_trap`
- 1 — `impersonation_unavailable`

## UNCLASSIFIED clusters (with error text)

- **7** — no local cause, routes to engine RCA (job id present)
  - e.g. `on-call/airflow_logs/2026-08-05/033824__tpa_ipdsc_export__ipdsc_ds_17__try1__upstream_failed.log`
- **4** — other: [error] task Task failed with exception
  - e.g. `on-call/airflow_logs/2026-08-17/203357__tpa_ipdsc_export__ipdsc__try1__failed.log`
- **2** — slack notifier channel_not_found
  - e.g. `on-call/airflow_logs/2026-08-05/110652__url_pattern_identification__run_spark_pattern_identification__map0__try2__failed.log`
- **2** — other: [error] task Dag not found during start up
  - e.g. `on-call/airflow_logs/2026-08-17/194550__tpa_ipdsc_export__get_dt__try1__failed.log`
