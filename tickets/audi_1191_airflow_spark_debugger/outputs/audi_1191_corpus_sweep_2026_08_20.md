# AUDI-1191 corpus sweep — 1037 logs

Identity oracle is the filename convention written by the acquisition layer.
`Classified` counts logs with a signature; a log with no error text has nothing
to classify and is counted separately, not as a taxonomy gap.

| Outcome | Logs | Identity resolved | Body alone | Body disagrees | Run id | Classified | No error text |
|---|---:|---:|---:|---:|---:|---:|---:|
| success | 845 | 845 | 0 | 0 | 4 | 5 | 840 |
| failed | 114 | 114 | 78 | 0 | 94 | 100 | 6 |
| upstream_failed | 60 | 60 | 0 | 0 | 1 | 1 | 58 |
| skipped | 14 | 14 | 0 | 0 | 0 | 5 | 9 |
| running | 3 | 3 | 0 | 0 | 0 | 0 | 3 |
| scheduled | 1 | 1 | 0 | 0 | 0 | 0 | 1 |

## Headline

- Identity: 1037/1037 resolved by `parse_log_file`; 78 from the log body alone, 0 contradicting the filename.
- Diagnosable failures (failed + upstream_failed, with error text): 110 of 174.
- Classified: 101/110 (91%) of diagnosable failures.
- Routable without a signature (downstream handle present): 9. These carry no cause in the Airflow log and are resolved by fetching it from the system that owns it (Dataproc, Databricks, Vertex, the Airflow API), so they are not taxonomy gaps.
- Neither classified nor routable: 0. This is the real taxonomy gap.
- Fires on a green run: 5 (a signature firing on a success log is a false positive unless the mechanism genuinely occurred).

## Signatures fired

- 40 — `cluster_create_stockout`
- 8 — `dbt_model_runtime_error`
- 8 — `task_execution_timeout`
- 6 — `downstream_job_no_local_cause`
- 5 — `slack_notify_failed`
- 5 — `vertex_pipeline_task_failed`
- 4 — `path_not_found_late_data`
- 4 — `auth_error`
- 3 — `sensor_timeout`
- 2 — `dbt_test_failure`
- 2 — `batch_cancelled`
- 2 — `external_task_failed`
- 2 — `dag_not_found_at_startup`
- 2 — `batch_id_missing`
- 2 — `analysis_exception`
- 2 — `task_externally_terminated`
- 1 — `invalid_output_path_config`
- 1 — `pod_evicted_404`
- 1 — `batch_id_attach_trap`
- 1 — `impersonation_unavailable`

## UNCLASSIFIED clusters (with error text)

- **9** — no local cause, routes to the owning system (downstream handle present)
  - e.g. `on-call/airflow_logs/2026-08-05/033824__tpa_ipdsc_export__ipdsc_ds_17__try1__upstream_failed.log`
