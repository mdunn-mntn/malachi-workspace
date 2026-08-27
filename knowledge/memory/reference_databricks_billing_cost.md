---
name: reference_databricks_billing_cost
description: How to price a Databricks job or dbt node from system.billing - the job_run_timeline dedupe that stops a 12x inflation, and why warehouse dollars are apportioned rather than measured.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [system.billing.usage, list_prices, job_run_timeline, query.history, usage_metadata, job_run_id, warehouse_id, DBU, databricks cost, AUDI-1194]
domain: [infra, data-catalog]
lifecycle: active
last_verified: 2026-08-26
---

**Pricing a Databricks job.** `system.billing.usage.usage_metadata` carries `job_id`,
`job_run_id`, `run_name`, `job_name`, `warehouse_id`, `cluster_id`. Join `job_run_id` to
`system.lakeflow.job_run_timeline.run_id` for dbt submissions, `job_id` to
`system.lakeflow.jobs` (use `max_by(name, change_time)`) for named jobs, and `sku_name` +
`usage_start_time` to `system.billing.list_prices` for dollars. Implemented as
`airflow_optimizer.databricks.job_costs` / `query_costs`.

**The dedupe is mandatory.** `job_run_timeline` holds ONE ROW PER HOURLY PERIOD of a run, so a
raw join multiplies every usage record. Over 2026-08-19..08-26 the naive join summed
**205,239 DBU** of `PREMIUM_JOBS_COMPUTE` against **16,460** deduped, a 12.5x error. Dedupe with
`SELECT run_id, min(run_name) ... GROUP BY run_id` first. The ratio is not a constant (10.4x to
12.5x across rolling windows) and is driven by a few long runs: 89% of usage `run_id`s have
exactly one timeline row. Row count inflates only 1.6x while quantity inflates 12.5x, so a
row-count fanout check badly understates the error. The deduped total reconciles exactly with the
SKU total from `usage` alone, but only because every usage row had a non-NULL `job_run_id` and
every `run_id` matched; an INNER join would silently undercount otherwise.

**Warehouse dollars are apportioned, never measured.** `system.billing` carries no per-query DBU.
A SQL warehouse bills by the hour, so a statement's share is allocated by its duration share of
that day's query time. Concurrent statements make summed durations exceed wall time: four dbt
tests launching within 30 ms of each other summed 297 query-hours against a **union of 83
wall-clock hours** and 101 warehouse running hours implied by billing. Report DBUs as primary and
any dollar figure as order-of-magnitude, and never call summed query duration "warehouse-hours".

**Dataproc Serverless is NOT in `system.billing`.** The Spark half stays on
`runtimeInfo.approximateUsage.milliDcuSeconds` from `gcloud dataproc batches describe`.

Grants: [[reference_databricks_system_schema_grants]]. Project: [[project_airflow_optimizer]].
