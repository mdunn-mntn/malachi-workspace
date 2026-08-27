---
name: reference_spark_eventlog_cost_units
description: Costing a Spark run from its event log - ExecutorAdded is not a census, Launch-to-Finish overshoots slots by a sub-100ms handoff, and a task-time ratio cannot rank against executor-hours.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [SparkListenerExecutorAdded, ExecutorRemoved, TaskEnd, Launch Time, Finish Time, executor-hours, peak concurrency, shuffle_fetch_wait, spark.executor.cores, Total Cores, AUDI-1194]
domain: [infra]
lifecycle: active
last_verified: 2026-08-26
---

**`SparkListenerExecutorAdded` is not a census.** In one prod Dataproc Serverless log
(`app-20260825065124173-0803`) it appears for **359** executors while **497** ran tasks. 96
`ExecutorRemoved` events cover only 48 distinct ids, each logged **twice**, so a running
Added-minus-Removed counter reaches **-46**. The unadded ids cluster high (333-544) and the file
starts with `SparkListenerLogStart`, so it is not a truncated head. Any cost measure keyed on
`Added` alone under-counts: this one was **29% short (276.1 vs 356.6 executor-hours)**. Seed an
executor's start from its first task's `Launch Time` when no `Added` event exists.

**Slots come from the log, and unknown means unknown.** `spark.executor.cores` is absent from many
logs (both committed fixtures) while `ExecutorAdded.Executor Info.Total Cores` carries it. A
cores=1 fallback inflates any per-task cost by the real core count and can publish a figure larger
than the whole run: "100 of the run's 80 executor-hours". Return 0 when neither source reports one,
and let the finding say the cost is unknown rather than guess.

**Launch-to-Finish overshoots the slot ceiling by a handoff, not by a bias.** Taking occupancy as
`(Launch Time, Finish Time)` from `TaskEnd` puts EVERY executor above its 4-slot ceiling at some
instant (peaks 5 on 137, 6 on 309, 7 on 43, 8 on 8). The interval itself is sound: summed
Launch-to-Finish is 1.7% above summed Executor Run + Deserialize + Result Serialization.
Time-weighted, only **0.12%** of busy executor-time sits above 4. **Shrink each interval's end by
100ms and all 497 collapse to exactly 4**, the fleet to exactly `executors x cores`. Peak
concurrency IS measurable with that tolerance; the raw instantaneous maximum is not.

**A task-time ratio cannot rank against executor-hours.** `shuffle_fetch_wait` and `gc_pressure`
divide by summed TASK time, so a stage doing little compute reports 90% on a denominator worth
minutes. On `site_network_hourly` a true 68% fetch wait was **0.6 of the run's 356.6
executor-hours** while idle executors were 346. Gate `high` on absolute hours OR a share of the
run, never on the ratio alone, and never demote a finding whose cost could not be derived.

Project: [[project_airflow_optimizer]].
