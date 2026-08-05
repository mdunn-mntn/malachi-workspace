---
name: project_airflow_optimizer
description: AUDI-1194 airflow_optimizer/ — scheduled key-free efficiency crawler over SUCCEEDED Spark jobs (Dataproc event logs + Databricks plans); split from the AUDI-1191 debugger 2026-08-05; optimizer half mostly built, needs productionizing
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [airflow optimizer, AUDI-1194, spark optimization crawler, efficiency sweep, eventlog parser, 7-surface spark, optimization detectors, skew spill shuffle, fleet crawl backlog, weekly optimizer cron, phs event logs, dataproc-debug pam, 242x skew, dataproc databricks optimization]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-08-05
---
**AUDI-1194 = the OPTIMIZER** (success-triggered efficiency sweep), **split from the AUDI-1191 debugger 2026-08-05** (both AUDI, type Task, Backlog). The two are separate workflows with distinct triggers/schedules/deliverables: the debugger fires only on a **failure**, the optimizer sweeps every DAG that **succeeds**. They can chain but are distinct. AUDI-1194: 5 story points, PMO rep Bryce Wagg, label q3_2026, folder `tickets/audi_1194_optimizer_efficiency_crawler/`, framing **LOCKED** (§0 in its summary). See [[project_airflow_debugger]] (the RCA half), [[reference_airflow_ti]], [[reference_oncall_runbook]].

**Question (framed):** Can a scheduled, key-free crawler read every succeeded Spark job across both engines (Dataproc event logs + Databricks EXPLAIN COST plans/metrics) and emit a ranked, actionable optimization backlog with no manual step? **Goal:** cut Spark compute cost + wall-clock fleet-wide, replacing the departed framework author's tribal knowledge (cost-reduction lever, Medium tier + bus-factor win). **Done-when:** a scheduled crawler scans every succeeded Spark job **including the ipdsc/tpa PHS logs** and emits a ranked cross-job backlog (worst-first, per-finding fix grouped CODE/INFRA/FAILURE) with no manual step.

**Package `airflow_optimizer/` (split from `airflow_debugger/`, commits a8ebad2d + b153266d).** Modules: `eventlog` (the 7-surface Spark event-log parser), `optimizations` (detectors), `optimize` (single-job BLUF), `crawl` (fleet backlog) + `tests`, `fixtures`, own `README`. **Fully decoupled from `airflow_debugger/`; the two packages share ONLY `eventlog.py`, which lives in `airflow_optimizer/`** (only the optimizer imports it — the debugger's RCA path doesn't). Entrypoints: `python3 -m airflow_optimizer.{optimize,crawl}` (debugger side = `python3 -m airflow_debugger.{orchestrate,report}`). All 6 tests pass, ruff-clean, git-tracked as renames.

**The engine (all built + validated on real event logs):**
- `eventlog.py::parse_eventlog()` → structured `SparkRun` across all **7 Spark surfaces** (jobs/stages/tasks/executors/environment/storage/SQL per-node), recovering per-operator metrics by joining `sparkPlanInfo` accumulatorId↔Accumulables/DriverAccumUpdates. Handles `.zstd` (dir/file). Surface 7 (storage/cache, `SparkListenerBlockUpdated`) needs `spark.eventLog.logBlockUpdates.enabled` — **UNCAPTURABLE on Dataproc Serverless** (rejects that prop), valid on managed clusters only.
- `optimizations.py::analyze_run()` → 3 rec types — **code** (skew/spill/shuffle-partitions), **infra** (gc_pressure, spot_preemption_cost, cache_ineffective), **failure** (fetch instability) — each with real numbers + fix, impact-ranked.
- `optimize.py` = one event log in → engineer-ready single-job BLUF backlog (plan-text `analyze_plan` + metric `analyze_run`). `crawl.py` = optimize every job in a dir/glob, rank a cross-job backlog worst-first.

**Acquisition state per engine:**
- **Batch-operator Dataproc fleet (88 models, no PHS):** event logs land in `gs://mntn-data-archive-{env}/spark-events` (PR #1169 turned this on fleet-wide, merged prod 2026-08-04) — accessible, download with `gsutil -o "GSUtil:check_hashes=never" cp` (`gcloud storage cp` corrupts `.zstd`).
- **ipdsc/tpa (PHS-attached):** logs are per-batch at `gs://{temp_bucket}/<dataproc-batch-uuid>/spark-job-history/app-<id>.zstd` — SPARSE + scattered across thousands of unsorted per-uuid temp dirs, most empty. A flat prefix scan is infeasible → the crawler must **ENUMERATE ipdsc/tpa batches via `gcloud dataproc batches list/describe` (→ uuid), then read that uuid's `spark-job-history`**. Validated end-to-end 2026-08-05 (parsed `Populate ipdsc_ds_67.DS67`, shuffle.partitions=1000). This reshapes the earlier "point the crawler at the PHS prefix = 1-line change" note, which was WRONG.
- **Databricks:** `EXPLAIN COST` plan + Spark job metrics via `jobs get-run-output` (live acquisition path still to validate).

**PHS event-log access (2026-08-05):** `malachi@mountain.com` has **NO standing `storage.objects.list`** on `gs://dataproc-temp-us-central1-995798185124-svhwvc6j`. Interim read = the **`dataproc-debug` PAM bundle** (Compute Viewer + Dataproc Viewer + Storage Object Viewer; self-service ~1h, 18h max with L1 devops-squad approval; access propagates ~30s after the grant activates). The 1h PAM grant can't run the weekly cron → **standing grant needed (Slack/mountain-devops → Christina): `roles/dataproc.viewer` on `mntn-prj-prod-00` (enumerate batches) + `roles/storage.objectViewer` on `dataproc-temp-us-central1-995798185124-svhwvc6j` (read logs)**.

**Weekly cron LIVE:** `.claude/scripts/oncall_weekly_optimizer.sh` + launchd `com.mntn.weekly-spark-optimizer` (Mon 11:00 PT). Pulls newest ≤40 logs from `spark-events`, runs `airflow_optimizer.crawl`, writes `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_<date>.md`. Idles with no git noise until enablement lands.

**Proof it works — real fleet finding (IMP-024):** crawl of 13 real prod jobs → **`Update Vertical Categorization` chronic Stage-0 skew up to 242x** (every run 10-242x) = #1 fleet target; `Prepare HTML Content` 18.4x; 6 jobs clean. Labeled by `spark.app.name` (event log self-identifies).

**Cadence decision (open):** measure the cost of one full sweep, then **daily if cheap, weekly if expensive**. Assumptions to resolve first: (1) standing GCS read on the temp bucket (blocker for the PHS subset), (2) efficient batch-enumeration for the scattered per-uuid PHS logs, (3) validate the live Databricks `EXPLAIN COST` path.
