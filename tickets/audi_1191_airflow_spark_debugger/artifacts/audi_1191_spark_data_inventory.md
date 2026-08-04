# AUDI-1191 — Complete Spark data inventory (are we getting everything?)

## BLUF

No — the analyzer currently consumes **1 of 7** Spark data surfaces (the SQL plan text). The full
surface is 7 REST endpoints, and **the Spark event log is a single artifact that contains ALL of them**.
So enabling the event log (IMP-023) is not "a" path — it is the **complete** capture. The richest
optimization signal (per-stage spill/shuffle, per-stage task **skew percentiles**, per-executor failed
tasks + GC + peak memory, the full config) is NOT in the plan text — it's in the other 6 surfaces.
Databricks and Dataproc are identical here (both are Apache Spark; same REST schema, same event log).

**Finding:** the saved `Spark UI - Databricks.html` is only the SPA shell (JS bundle + feature flags),
no data — the UI fetches everything from the Spark REST proxy at render time. So **scraping the UI HTML
is a dead end**; the REST API / event log is the only real source.

## The 7 surfaces (each Spark UI tab = a REST endpoint = event-log events)

| # | UI tab | REST `/api/v1/applications/<app>/…` | Event-log event | Key fields we're missing | Optimization it powers |
|---|---|---|---|---|---|
| 1 | Jobs | `/jobs`, `/jobs/{id}` | JobStart/End | job→stage graph, durations, failed/skipped counts | which job dominates; skipped-stage waste |
| 2 | **Stages** | `/stages`, `/stages/{id}/{att}` | StageCompleted, TaskEnd | **input/output bytes+rows, shuffleRead/Write bytes+rows, memory+disk SPILL, failureReason** | wide-shuffle sizing, spill, the FetchFailed (`MetadataFetchFailedException`) |
| 2b | Stage → task summary | `/stages/{id}/{att}` (taskSummary) + `/taskList` | TaskEnd (per task) | **percentiles min/25/median/75/max** of duration, GC, shuffle, spill; per-task locality | **SKEW** (max≫median), stragglers — the highest-value signal, invisible in the plan |
| 3 | **Executors** | `/executors`, `/allexecutors` | ExecutorAdded/Removed, TaskEnd | **failedTasks, totalGCTime, peakMemoryMetrics (heap/offheap/process-tree), input/shuffle bytes** | spot-preemption cost, GC-bound, memory pressure, executor skew |
| 4 | **Environment** | `/environment` | EnvironmentUpdate | **all Spark properties**: `shuffle.partitions`, memory, AQE, `clusterAvailability=PREEMPTIBLE_WITH_FALLBACK_GCP`, `clusterLogDeliveryEnabled=false` | config tuning; confirms spot + that log delivery is OFF |
| 5 | Storage | `/storage/rdd`, `/storage/rdd/{id}` | **BlockUpdated** (needs `logBlockUpdates.enabled=true`) | cached RDD bytes, evictions, storage level | cache effectiveness — is the cache used or evicted (INC-005 uncached recompute) |
| 6 | **SQL/DataFrame** | `/sql`, `/sql/{execId}` | SQLExecutionStart/End, AdaptiveExecutionUpdate | `physicalPlanDescription` (plan text + `== Optimizer Statistics ==`) **+ per-node metrics** (rows in/out, time, spill, peak mem, scan time, shuffle) | missing-stats, join strategy, per-operator cost, filter pushdown — **sub-execution IDs = nested plan detail** |
| 7 | (implicit) | `/stages/{id}/{att}/taskList` | TaskEnd | per-task attempt, GC, spill, locality, speculative | straggler / skew root-cause at task grain |

## What the screenshots concretely proved is available (and we weren't grabbing)

- **Stages:** stage 10 = 700.8 GiB input / 768.2 GiB shuffle write; stage 12 retry = 116.5 GiB in / 127.9 GiB shuffle write; **failed stage 14 = `MetadataFetchFailedException: Missing an output location for shuffle 3 partition 847`** (168 failed tasks). Shuffle volumes per stage = the spill/skew map.
- **Executors:** Dead(7), **168 failed tasks** (executor 2 alone = 113), 89.3h task time / **2.2h GC**, 1.1 TiB input, 1.3 TiB shuffle. `clusterAvailability = PREEMPTIBLE_WITH_FALLBACK_GCP` → spot kills = the 168 failed tasks (this is INC-009's root cause, visible in one number).
- **SQL plan:** WholeStageCodegen(2) = **9.98 h**, SortMergeJoin 554M rows, RunningWindow over a 182 GiB shuffle, InMemoryTableScan 435M — the per-node timings the plan text alone doesn't rank.
- **Environment:** `clusterLogDeliveryEnabled = false` — confirms the acquisition blocker at the source.

## Consequence for the build

1. **The event log captures all 7 surfaces in one file** → IMP-023 "enable the event log" = 100% capture, not partial. Prefer it over per-endpoint REST scraping. (`eventlog_profiler.py` already parses stages/tasks/executors/FetchFailed from it — extend it to emit the SQL per-node metrics + environment.)
2. **Expand the analyzer target model** from plan-text (`optimizations.py`, 5 detectors) to the full schema. New detectors the other surfaces unlock:
   - `skew` (stage taskSummary max≫median) · `disk_spill` (stage memory/diskBytesSpilled) ·
     `spot_preemption_cost` (executor failedTasks + availability) · `gc_pressure` (GC/taskTime ratio) ·
     `cache_ineffective` (storage cached fraction low / evicted) · `shuffle_fetch_instability`
     (FetchFailed count) · `default_shuffle_partitions` (environment vs data size).
3. **Dataproc parity:** identical Apache Spark REST + event log. A **persistent Spark History Server**
   (`spark_history_server_config`, already in `ipdsc_emr_cluster.py`) serves the same
   `/api/v1/applications/…` for terminated batches. Same 7 surfaces, same detectors.

## Bottom line

We are 100% sure now: the complete valuable surface is the 7 endpoints above, and the **event log is the
single artifact that holds all of them**. The plan text (what we built first) is one slice. The
acquisition ask (IMP-023) is therefore correct and, with the event log specifically, complete — and the
analyzer should grow to consume the full schema, with the highest-value additions being **stage-level
spill + task-level skew percentiles + executor failed-tasks/GC**, none of which live in the plan.
