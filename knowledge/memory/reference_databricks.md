---
name: databricks
description: Databricks: push >4h-risk queries there (memory-optimized); more small-core nodes for shuffle-heavy Spark
metadata:
  type: reference
doc_type: memory
keywords: [databricks, EXPLAIN COST, statement execution api, sql statements api, jobs list-runs, get-run-output empty, notebook_output, system.lakeflow denied, databricks admins, databricks PAT removed, photon plan, spark shuffle, executor cores, node sizing, augmentor_log, prospecting_intent, gcs parquet archive, bq 6-hour wall, memory-optimized cluster, victor benchmark, databricks cli, u2m oauth, jobs get-run, get-run-output, oncall databricks access, system.lakeflow, sql_warehouse]
domain: [bigquery, infra]
lifecycle: active
last_verified: 2026-08-21
---

## On-call RCA CLI access (verified 2026-08-03)
The on-call box CAN now read Databricks programmatically, key-free — this supersedes the INC-009-era "no programmatic Databricks access / CLI hangs on OAuth" note. Use the **U2M OAuth CLI profile `malachi@mountain.com`** (the `DEFAULT` profile is invalid — always pass `-p malachi@mountain.com`). Workspace `https://1262887251702944.4.gcp.databricks.com`.
- `databricks jobs get-run <run_id> -o json` → `state.result_state` (SUCCESS/FAILED) + `tasks[].run_id`.
- `databricks jobs get-run-output <TASK run_id> -o json` → `error` (root cause, e.g. `TABLE_OR_VIEW_ALREADY_EXISTS` SQLSTATE 42P07) + `error_trace`. **Must use the TASK run_id, not the parent job run_id.**
- ~~SQL warehouse `sql_warehouse_2xs` is RUNNING → the `system.lakeflow` structured path is available.~~ **Superseded 2026-08-20:** `sql_warehouse_2xs` is STOPPED and `system.lakeflow` returns `INSUFFICIENT_PERMISSIONS: no USE SCHEMA` — see the plan section below for what to use instead.
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
**§ Reading a Spark PLAN out of Databricks — the specced route is dead, use the SQL Statement Execution API (validated live 2026-08-20, AUDI-1194).**

`jobs get-run-output` does **NOT** carry a plan, on failure OR on success. On a SUCCEEDED prod run today (`prod-mntn_matched_reporting-targeted_signal_domain`, task run `502229322982640`) it returns exactly `{"metadata": ..., "notebook_output": {}}` — no plan, no stats, no logs — and `new_cluster.cluster_log_conf` is `None`, so no event log is persisted either. The pipe exists and is empty. Every AUDI-1191/1194 doc that presented `EXPLAIN COST via get-run-output` as the acquisition path was wrong about the transport.

**What works, with no dbt change and no cluster change:** run `EXPLAIN COST <query>` against a SQL warehouse through `/api/2.0/sql/statements` (poll `/api/2.0/sql/statements/<id>` while state is PENDING/RUNNING; rows come back in `result.data_array`, one plan line per row). Validated on real prod tables: a 5,412-char plan carrying `== Physical Plan ==`, `Statistics(sizeInBytes=)`, `Scan parquet <catalog.schema.table>`, and `== Optimizer Statistics (table names per statistics state) ==` with `missing = product_categorization, product_uniques` — fed straight into `airflow_optimizer.optimizations.analyze_plan` for 2 high-impact `missing_statistics` findings. Runner: `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_databricks_explain_cost.py`.

- **Warehouse:** `sql_warehouse_2xs` (`fa27430dfc609e6d`) is **STOPPED**; use `Serverless Starter Warehouse` `14b311ac86ee2ca2` (RUNNING). Check with `databricks warehouses list -p malachi@mountain.com`.
- **`system.lakeflow` access is blocked by a missing METASTORE ADMIN, and the enable error is a red herring.** Settled 2026-08-24 by David Qiu (Databricks) on the support thread. Two claims recorded here before that are wrong and are corrected, not deleted:
  - *(2026-08-20, wrong)* "a Unity Catalog **account** admin can grant it" — account admin does not confer MANAGE on system schemas.
  - *(2026-08-21, wrong)* "only Databricks can turn it on, so there is nobody at MNTN to ask" — that read the wrong error. `PUT /api/2.0/unity-catalog/metastores/<id>/systemschemas/lakeflow` returning `lakeflow system schema can only be enabled by Databricks.` is **expected and ignorable**: `system.lakeflow` is Databricks-managed and enabled automatically, so that call always rejects. Evidence class: vendor engineer on the metastore itself, which beats both prior readings (inference from an error string).
  - **The real blocker** is the second error, `User does not have MANAGE on Schema 'system.lakeflow'`. Granting on system schemas requires **metastore admin**. Accounts created after **Nov 2023** ship with **no metastore admin assigned at all** — MNTN's is one, which is why nobody has MANAGE.
- **Assigning a metastore admin is console-only and must be a GROUP.** Account console > Catalog > metastore `c5dc6763-eaae-4d6c-9ae2-7af6147595bb` > Metastore Admin > Edit. No API, no Terraform path as of 2026-08-24. An account admin sets it; then a member of that group runs the grants. This gate sits in front of **every** `system.*` schema, not just lakeflow — the identical `does not have USE SCHEMA` error came back for `system.query`, `system.access`, `system.billing`, `system.compute` on 2026-08-21, so missing-grant and not-enabled are indistinguishable from a query and IMP-062's "is billing different" question is moot until a metastore admin exists.
- **Three grants, not two** — the catalog level is easy to omit and blocks on its own:
  ```
  GRANT USE CATALOG ON CATALOG system TO `<principal>`;
  GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `<principal>`;
  GRANT SELECT ON SCHEMA system.lakeflow TO `<principal>`;
  ```
- **Enumerating ephemeral dbt submissions: one table, no joins.** `system.lakeflow.job_run_timeline` where `run_type = 'SUBMIT_RUN'` (cols `run_id, job_id, run_name, result_state, period_start_time, period_end_time, run_duration_seconds, compute_ids`). `run_name` carries whatever `runs/submit` passed, i.e. the dbt identifier. **Never join `system.lakeflow.jobs` for a job name — ephemeral runs have no row there and the join silently drops 100% of them.** Join `job_task_run_timeline` on `job_run_id` only for per-task detail. System tables are **batch ingested, ~10 min lag** before a run appears, so a poller must not treat absence as failure.
- **Only `missing_statistics` fires on real Databricks plans.** The shuffle-size regex wants `(ShuffleQueryStage|Exchange) ... sizeInBytes=` on ONE line, which is the Spark UI SQL-tab rendering; `EXPLAIN COST` attaches `Statistics(sizeInBytes=)` to LOGICAL operators and Photon renames the physical ones `PhotonShuffleExchangeSink/Source`. So `broadcast_candidate`, `shuffle_partition_sizing`, `window_full_sort` are dead on **both** engines (IMP-033 — the fix targets the plan RENDERING, not the engine).
- Nonsense estimates like `sizeInBytes=2.27E+22 B` are the CBO's default when stats are missing — corroboration, not a parse bug.

**§ Credentials: no PAT on disk (2026-08-20).** `~/.databrickscfg` `[DEFAULT]` held a long-lived token reported `Valid: NO` by `databricks auth profiles`; `databricks tokens list` returns **empty**, so it was already revoked server-side. The stanza is deleted and `.claude/databricks_setup.md` no longer instructs recreating it. Use the U2M OAuth profile and pass `-p malachi@mountain.com` on EVERY call. The keychain entry `databricks-ti837` still holds the dead token and `.claude/scripts/databricks_smoke.py` still reads it, so that script cannot authenticate (IMP-049). See [[project_deidentify_personal_credentials]].

**§ Ownership (Dustin Niehoff, #devops, 2026-08-20):** **Victor set up all the Databricks estate for TI. DPLAT explicitly wanted nothing to do with Databricks.** So Databricks questions route to Victor / TI, not to data-platform and not to devops. Practical consequence: creating a read-only service principal for a TI workload needs no devops ticket, and `malachi@mountain.com` is already in workspace `admins`, so it is self-serve. Existing principals `prod_runner` / `dev_runner` sit in `producers_prod` / `producers_dev`; a read-only workload belongs in neither.

**§ Access level:** `malachi@mountain.com` is in **`admins`** as of 2026-08-20 (groups: `producers_dev`, `users`, `admins`, a users-clone). The on-call runbook's `producers/dev/users` line is stale. Workspace `admin` still does not grant Unity Catalog `system` schema access.
