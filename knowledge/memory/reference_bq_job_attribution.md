---
name: reference_bq_job_attribution
description: Attributing BigQuery jobs to Airflow dags/tasks via INFORMATION_SCHEMA - which jobs carry airflow-dag/airflow-task labels, which carry none, where airflow-ti jobs bill, and which JOBS_BY_* views are readable.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [bq job attribution, INFORMATION_SCHEMA, JOBS_BY_USER, JOBS_BY_ORGANIZATION, JOBS_BY_PROJECT, BigQueryInsertJobOperator, airflow-dag label, airflow-task label, airflow job_id prefix, python client no labels, dw-main-bronze billing, service account own jobs, bq_profile.py, AUDI-1241]
domain: [data, infra]
lifecycle: active
last_verified: 2026-08-28
---
**Verified empirically 2026-08-28 (AUDI-1241, feeds `airflow_optimizer/bq_profile.py`):**

- **`BigQueryInsertJobOperator` stamps labels `airflow-dag` / `airflow-task` on every job it
  submits**; direct inserts also carry a `job_id` of the form `airflow_<dag>_<task>_<ts>`, so the
  dag/task is recoverable from either field.
- **Jobs run by a python client INSIDE a task carry NO labels** — label-based attribution misses
  them entirely.
- **airflow-ti / camperbid jobs bill in `dw-main-bronze`** (that is the job project to query).
- **`JOBS_BY_USER` needs no extra grant when a service account reads its own jobs** — this is why
  the fleet SA can profile its own history with zero IAM change.
- **Access denied to `malachi@mountain.com`:** `JOBS_BY_ORGANIZATION` and
  `mntn-prj-prod-00:INFORMATION_SCHEMA.JOBS_BY_PROJECT`.

Related: [[project_airflow_optimizer]], [[reference_bq_location_reservation]].
