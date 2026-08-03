---
name: databricks
description: Databricks: push >4h-risk queries there (memory-optimized); more small-core nodes for shuffle-heavy Spark
metadata:
  type: reference
doc_type: memory
keywords: [databricks, spark shuffle, executor cores, node sizing, augmentor_log, prospecting_intent, gcs parquet archive, bq 6-hour wall, memory-optimized cluster, victor benchmark, databricks cli, u2m oauth, jobs get-run, get-run-output, oncall databricks access, system.lakeflow, sql_warehouse]
domain: [bigquery, infra]
lifecycle: active
last_verified: 2026-08-03
---

## On-call RCA CLI access (verified 2026-08-03)
The on-call box CAN now read Databricks programmatically, key-free — this supersedes the INC-009-era "no programmatic Databricks access / CLI hangs on OAuth" note. Use the **U2M OAuth CLI profile `malachi@mountain.com`** (the `DEFAULT` profile is invalid — always pass `-p malachi@mountain.com`). Workspace `https://1262887251702944.4.gcp.databricks.com`.
- `databricks jobs get-run <run_id> -o json` → `state.result_state` (SUCCESS/FAILED) + `tasks[].run_id`.
- `databricks jobs get-run-output <TASK run_id> -o json` → `error` (root cause, e.g. `TABLE_OR_VIEW_ALREADY_EXISTS` SQLSTATE 42P07) + `error_trace`. **Must use the TASK run_id, not the parent job run_id.**
- SQL warehouse `sql_warehouse_2xs` is RUNNING → the `system.lakeflow` structured path (job_run_timeline, retries, duration) is available.
- This is the read path the [[project_airflow_debugger]] `databricks_rca.py` drives. See also [[reference_oncall_runbook]] INC-009.
## from reference_databricks_for_heavy_queries.md
For heavy lift / incrementality queries that scan augmentor_log or prospecting_intent over multi-day windows, propose Databricks BEFORE committing to a long BQ run. We have access to large clusters there.

**When to suggest Databricks:**
- Estimated BQ wall time > 4 hours
- Augmentor_log scans over 7+ days
- Need windows beyond augmentor's 10-day TTL (Databricks can read the GCS parquet archive at `gs://mntn-data-archive-prod/augmentor_log/` which has ~30d of history)
- Pooled per-advertiser × per-IP aggregations at scale

**Why:**
- BQ has a hard 6-hour interactive query wall (cannot raise — TI-933 lesson, 2026-05-06)
- Databricks Spark jobs can run for hours-to-days with no fixed wall
- prospecting_intent and augmentor parquet archive are both GCS-native — Spark reads them natively (no Datastream lag, no federated-table partition-pruning bugs)
- BQ-side data still accessible via BQ Spark connector

**Cluster sizing rough rule:**
- ATT-style lift work is **shuffle-heavy** (joins on advertiser_id + ip across many large CTEs). Memory-optimized clusters win — bigger executors hold more shuffle data in RAM, fewer disk spills.
- Compute-optimized helps the scan/decode stages (parquet read, hash bucket assignment) but the bottleneck is usually the join shuffle.
- **Default: memory-optimized large cluster.** Tune up if shuffle spill dominates the Spark UI.

**Source code reference:** Steelhouse has Spark job templates in `airflow-ti` repo (see memory `reference_airflow_ti.md`). Borrow patterns from there rather than write from scratch.

## from reference_databricks_node_sizing.md
For shuffle-heavy Spark workloads on Databricks, **smaller nodes outperform bigger nodes** at the same total core count. Confirmed by Victor's empirical benchmarks.

**Source:** https://mntn.atlassian.net/wiki/spaces/ML/pages/2771451908/DBX+node+size+impact+on+pipeline+performance

## The benchmark

Two clusters with **identical total cores (256) and total RAM**, on the `prospecting_max_reach.py` prod pipeline (steelhouse/dbt):
- **Cluster A:** 16 × m5d.4xlarge (16 cores each)
- **Cluster B:** 4 × m5d.16xlarge (64 cores each)

## Result — default Databricks config (1 executor per node)

| Stage | 4xlarge (small) | 16xlarge (big) | Big-node penalty |
|---|---|---|---|
| Read stage 75th-percentile task | 10s | 12s | +20% |
| Read stage total | 2.1 min | 2.8 min | +33% |
| Processing stage 75th-percentile task | 19s | **1.2 min** | **+278%** (3.5×) |
| Processing tasks completed in 3.6 min | 2,760 | 760 | -72% |

Processing stage hits **3.5× slower** on big nodes with default config. This is the shuffle-heavy stage (broadcast join + groupBy after array expand). The default 1-executor-per-node setting on a 16xlarge gives that single executor 64 cores and ~150GB heap — too big for the JVM to manage well, so per-core throughput collapses.

## The real lever: executors-per-node

Increasing executor count by reducing cores/memory per executor shrinks the gap (full table at the URL):

| Config | Processing 75th | Tasks in 3.6 min |
|---|---|---|
| 4xlarge × 64 executors (4 cores, 12.2g each) | **13s** | **3,715** ← best |
| 4xlarge × 32 executors (8 cores, 24.4g each) | 14s | 3,562 |
| 4xlarge × 16 executors (default) | 16s | 3,052 |
| 16xlarge × 64 executors (4 cores, 12.2g each) | 16s | 2,925 |
| 16xlarge × 32 executors (8 cores, 24.4g each) | 16s | 2,963 |
| 16xlarge × 16 executors (16 cores, 1.5g + 34.5g offHeap) | 29s | 2,057 |
| 16xlarge × 16 executors (16 cores, 48.7g on-heap) | 25s | 2,147 |

**Two findings:**
1. Tuning executor count narrows the gap dramatically — but small nodes still win (~25% in best-tuned config).
2. on-heap vs off-heap memory: minimal impact for this workload. on-heap slightly preferred when JVM GC overhead is negligible.

## Operational rules

1. **Default to smaller nodes** when scaling Databricks compute for shuffle-heavy work. More nodes of medium size beats fewer big nodes even with tuning.
2. **Tune `spark.executor.cores`** to ~4-8, not the node-default. The "1 executor per node" default is the primary cause of big-node slowness.
3. **Sample executor config (on-heap, no off-heap):**
   ```
   spark.executor.cores: 16
   spark.executor.memory: 49896m
   spark.memory.offHeap.enabled: false
   ```
4. **Sample executor config (off-heap split):**
   ```
   spark.executor.cores: 16
   spark.executor.memory: 11754m
   spark.memory.offHeap.enabled: true
   spark.memory.offHeap.size: 35262m
   ```

## Caveats / scope

- Test was on AWS m5d nodes; the pattern likely holds on GCP `c3d-highmem-*-lssd` family but isn't directly benchmarked there.
- The processing stage was a **broadcast join + array expand + groupBy** — typical shuffle-heavy pattern, matches our ATT lift workload. Other workload types (CPU-bound, memory-bound but no shuffle) may show different patterns.
- Test used job-compute with on-demand instances. Spot instances add interruption risk but don't change the per-core perf finding.
