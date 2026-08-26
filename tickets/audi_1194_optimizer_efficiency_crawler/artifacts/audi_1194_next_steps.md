# AUDI-1194 — what is left, in the order it should happen (2026-08-26)

## Shipped today
- **airflow-ti #1222 merged and live in prod.** Verified on a real sweep: `profiled this sweep`
  went **2 → 12**, and only **7 of 217** scanned jobs cannot be tied to a DAG.
- **airflow-ti #1223 open** (manual trigger without a date). Gauntlet PASS, 2 rounds.
- **Databricks grants complete.** `system.{lakeflow,query,billing,compute,access,storage}` for
  both `malachi@mountain.com` and the `spark_optimizer` SP, plus `SELECT` + `USE CATALOG` on
  `CATALOG prod` for the SP. All verified by reading rows, never by the grants table.

## 1. Attribute real cost, now that billing is readable
`system.billing.usage` carries `usage_quantity` + `usage_unit` per record and a
`usage_metadata` struct with `job_id`, `job_run_id`, `run_name`, `job_name`, `warehouse_id`,
`cluster_id`. Two joins turn the optimizer's rankings into money:

- **dbt / job cost:** `usage_metadata.job_run_id` → `system.lakeflow.job_run_timeline.run_id`.
  Gives DBUs per ephemeral dbt submission, which is the unit `databricks.by_model` already groups.
- **warehouse / query cost:** `usage_metadata.warehouse_id` → the warehouse a
  `system.query.history` row ran on. Gives DBUs per statement, so the four dbt tests that burned
  **131 warehouse-hours over 2 days** get a dollar figure instead of an hours figure.
- **Dollars:** join `sku_name` + `usage_start_time` to `system.billing.list_prices`.

**Carry the discount caveat.** A committed-use or contract rate means cut DBUs may not cut the
bill proportionally. State DBUs as the primary unit and any dollar figure as order-of-magnitude
(same rule the Dataproc side already follows).

**This does not replace executor-hours for the Spark half** — Dataproc Serverless billing is not
in `system.billing`; that stays `milliDcuSeconds` from `gcloud dataproc batches describe`.

## 2. Fix the ranking that sent a wrong recommendation (IMP-084)
`shuffle_fetch_wait` divides fetch-wait by TASK time, so a stage doing little compute reports a
huge share of a tiny denominator. On `site_network_hourly` it reported 57-90% while that stall was
a median **0.28%** of the run's executor-hours; idle executors were **86%** and ranked below it.
Carry absolute hours on every finding and gate the impact tier on them. Until this lands, no
fetch-wait finding goes to an owner without dividing it by the run's executor-hours first.

## 3. Settle peak concurrency before touching any allocation
`site_network_hourly` holds a median **241 executor-hours** to do **27.5 hours of task work**
(2.5% slot utilization). Sizing `maxExecutors` needs a peak figure and the event log resists two
naive measures: unmatched `TaskStart` events (killed at stage end, speculative) inflate a running
count past 200% of slots held, and `ExecutorRemoved` for executors added before the log window
drives the executor count negative. Mean is solid (~34 concurrent tasks against 2,160 slots);
mean does not size a ceiling. Tooling started in `audi_1194_stage_read_parallelism.py`.

## 4. The EXPLAIN COST bridge works, and has a structural limit
`databricks.heavy_queries` reads `system.query.history` and `analyze_queries` plans each statement.
**But 0 of 20 heaviest statements are plannable**, because they all reference
`prod.ml.ddp_vertical_classification_api`, which is **dropped and recreated ~21x/day** (149 DROPs
in 7 days). EXPLAIN COST replays historical SQL, so a query touching a transient table can never
be planned after the fact.

Two consequences:
- The bridge should be run on RECENT queries against durable tables, or the plan captured at run
  time rather than replayed. Worth deciding before wiring it into the daily sweep.
- `explain_cost` now rejects a planner error. It used to return `TABLE_OR_VIEW_NOT_FOUND` AS the
  plan text, and an unresolved plan carries no statistics, so `missing_statistics` fired on a table
  that does not exist. Two findings were reported off that before the guard landed.

## 5. Chase the digest/coverage disagreement
The 2026-08-26 prod digest renders `fangorn_score_monitor` and `ipdsc_ds_35` unlinked, yet neither
appears in the coverage report's own unresolved list. One of those two surfaces is wrong. Both
read the same `Coverage`, so the likely suspects are the `_owner_index` cache and the difference
between what `unresolved()` is passed and what `resolve()` is passed.

## 6. Wire the Databricks half into the sweep
Blocked on the SP's OAuth secret reaching the worker (Vault, same path as the Slack token). The
grants are done, so this is credential plumbing, not access.

## Still on Malachi
- **Slack app** — blocks digest delivery entirely. `chat:write` only; Robin Fox reviews scopes.
- **`/frame` the follow-up ticket** and file it in Jira.
- **Review #1223.**
- Paste the Databricks + identity handoff into the AUDI-1191 debugger chat.
