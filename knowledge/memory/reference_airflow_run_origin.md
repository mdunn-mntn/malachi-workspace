---
name: reference_airflow_run_origin
description: Airflow tells you WHO started a run via dag_run.triggered_by (ui/cli/rest_api/operator), and the manual__ run-id prefix does NOT mean a person did it.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [triggered_by, manual__, run_type, TriggerDagRunOperator, dag_run_id prefix, tpa_mntn_id_export, schedule=None, airflow REST v2, dagRuns, rest_api, AUDI-1191]
domain: [infra, repos]
lifecycle: active
last_verified: 2026-08-26
---
**A `manual__` run id does NOT mean a person started the run.** `TriggerDagRunOperator` produces the identical prefix, so any DAG triggered by another DAG has `manual__<iso>` run ids for every run it ever has. In airflow-ti, `tpa_mntn_id_export` has `schedule=None` and is fired only from `tpa_ipdsc_export`'s `TriggerDagRunOperator` (no `trigger_run_id`), so **filtering out `manual__` drops that entire paging DAG's failures.** Verified 2026-08-26 while building the AUDI-1191 sweep filter; the adversarial reviewer caught it before it shipped.

**The field that actually answers "who started this" is `dag_run.triggered_by`,** on `GET /dags/{dag_id}/dagRuns/{run_id}` (REST v2). Observed values: `operator` (a TriggerDagRunOperator), `ui`, `cli`, `rest_api`, plus `timetable`/`asset`/`backfill`. **The REST value is `rest_api`, not `rest`** — an easy and silent miss, since an unknown string simply never matches.

Live example, `tpa_mntn_id_export`: `{'dag_run_id': 'manual__2026-08-26T06:09:41.779147+00:00', 'run_type': 'manual', 'triggered_by': 'operator', 'state': 'success'}`. Note `run_type` is `manual` here too, so **`run_type` is no better a discriminator than the prefix.**

**Cost:** one extra `GET` per run. Do the lookup AFTER the failure filter and cache by `(dag_id, run_id)` — then only a run that actually failed costs a call. Keep a run whose lookup fails, because dropping a failure on a failed lookup loses the incident.

Related: [[project_airflow_debugger]], [[reference_airflow_ti]], [[reference_airflow_log_puller]].
