---
doc_type: ticket
title: "AUDI-1194: Airflow/Spark optimization crawler"
status: in_progress
date: 2026-08-05
summary: "Scheduled efficiency sweep over succeeded Airflow DAGs (both engines); split from AUDI-1191 debugger"
result: "in progress — first external validation run 2026-08-07 on Ryan's aud-int-int-map batch (intent_score_map); IMP-029 rolling-dir fix shipped en route"
question: "Can a scheduled key-free crawler read every succeeded Spark job (Dataproc event logs + Databricks plans/metrics) and emit a ranked, actionable optimization backlog with no manual step?"
framing_state: locked
---

# AUDI-1194: Airflow/Spark optimization crawler

**Jira:** https://mntn.atlassian.net/browse/AUDI-1194
**Status:** in_progress
**Date Started:** 2026-08-05
**Assignee:** Malachi

---
## 0. Framing
Split from AUDI-1191 (which keeps the failure debugger). This ticket is the OPTIMIZER: the success-only, scheduled efficiency workflow. Distinct trigger (a job succeeds, not fails), distinct schedule, distinct deliverable. Shares only the event-log parser with the debugger.
- **Question (the unknown):** Can a scheduled, key-free crawler read every succeeded Spark job across both engines (Dataproc event logs + Databricks EXPLAIN COST plans/metrics) and emit a ranked, actionable optimization backlog with no manual step?
- **Goal (why / the decision):** Cut Spark compute cost and wall-clock across the whole airflow-ti fleet by surfacing inefficiencies automatically, replacing the departed framework author's tribal knowledge. The ranked backlog tells owners (DDP/ML/TPA) which jobs to fix first. North-star tie: cost-reduction lever (Medium) + velocity/bus-factor win — not the top incrementality bet (honest tier). Proof it works: the crawl already found a 242x prod skew (IMP-024).
- **Objective (done-when):** A scheduled crawler that, with no manual step, scans every succeeded Spark job across both engines — **full fleet including the ipdsc/tpa PHS logs** — and emits a ranked cross-job optimization backlog (worst-first, per-finding fix grouped CODE/INFRA/FAILURE). Done when it runs on a schedule and produces that full-fleet backlog automatically. Owner adoption is a separate outcome, not the close bar.
- **Approach (how):** Reuse the built modules (`eventlog` 7-surface parser, `optimizations` detectors, `optimize`, `crawl`). Acquisition per engine: batch-operator Dataproc fleet → event logs in `gs://mntn-data-archive-{env}/spark-events` (accessible); ipdsc/tpa → PHS temp-bucket, per-batch-uuid, via Dataproc batch-enumeration → uuid → `spark-job-history` (needs a standing GCS read grant, currently blocked); Databricks → `EXPLAIN COST` plan + Spark job metrics via `jobs get-run-output`. Measure the cost of one full sweep, then set cadence: **daily if cheap, weekly if expensive**. Deliver the ranked backlog as a file in `outputs/` (auto-post to owners deferred). Assumptions to resolve first: (1) standing GCS read on `dataproc-temp-us-central1-995798185124-svhwvc6j` (blocker for the PHS subset — no standing `storage.objects.list` today; interim read = the `dataproc-debug` PAM bundle, self-service ~1h, but the cron needs a standing `roles/storage.objectViewer` + `roles/dataproc.viewer` grant via mountain-devops → Christina); (2) efficient Dataproc batch-enumeration for the scattered per-uuid PHS logs; (3) validate the live Databricks `EXPLAIN COST` acquisition path.
- **What would change the answer:** If the findings are mostly false positives / not actionable (owners don't act), or the event-log/plan data can't be reached key-free at fleet scale, the "check every DAG automatically" premise fails and it degrades to a manual/on-request tool. Also: if a full daily sweep is too expensive AND weekly misses too much, the cadence model needs rethinking.

## 1. Introduction
The success-sweep optimizer, split from AUDI-1191 on 2026-08-05 (package `airflow_optimizer/`). Inherited optimizer artifacts moved here 2026-08-06: `artifacts/audi_1194_optimization_analyzer_scope.md` (scope + acquisition plan), `artifacts/audi_1194_spark_data_inventory.md` (the 7-surface data inventory), `artifacts/targeted_signal_demo.py` (Databricks demo), `artifacts/finding_vertical_categorization_skew.md` (the 242x finding), `outputs/optimizer_backlog_2026-08-04.md` (first prod crawl backlog). Deliverable: `My Drive/Tickets/AUDI-1194/AUDI-1194 Optimizer How It Works.xlsx` (generator in `artifacts/`).

## 2. The Problem
Spark compute waste across the airflow-ti fleet is invisible: succeeded jobs are never reviewed, and the one person who understood the framework's tuning left. Job owners (e.g. Ryan, 2026-08-07: 240 reserved executors idle ~1h on `intent_score_map`) can see symptoms in the console but have no tool that reads the event log and names the mechanism + fix. Impact: premium-tier DCU spend (the `audience_intent` DAG alone ≈ $39k/mo) with unquantified idle/spill waste.

## 3. Plan of Action
1. Build the engine: 7-surface event-log parser + plan/metric detectors + crawl ranking (done, split from AUDI-1191).
2. Acquisition: fleet event logs in `gs://mntn-data-archive-{env}/spark-events` (live since airflow-ti #1169); ipdsc/tpa PHS subset blocked on a standing GCS grant; Databricks EXPLAIN COST path unvalidated.
3. Validate on real asks — run 1: `aud-int-int-map` (2026-08-07, §4). Grow detectors from what each run exposes.
4. Productionize the weekly cron (currently exits silently when it can't list logs) and set cadence from measured sweep cost.

## 4. Investigation & Findings

### Validation run 1 (2026-08-07): `aud-int-int-map` — Ryan Kleck's live ask

First external request for the efficiency workflow. Ryan (Slack, 2026-08-07): "curious what your AI bot would think of this job and how to tune it... it got stuck on 1 executor but it still had 240 reserved nodes for like 1 hour." Batch `aud-int-int-map-20260806-20260807-053308-1` = `intent_score_map`, the terminal Avro fan-out of the `audience_intent` DAG (premium tier, dynamicAllocation 38→240 x 8 cores/19G, SUCCEEDED 05:33→07:20 UTC, 105.7 min).

**Ryan's observation confirmed and quantified** (`outputs/audi_1194_intent_score_map_2026_08_07_report.md` + `_timeline.csv`):
- 06:17:40→07:19:40 (62 min): ≤2 executors busy, 240 registered. 0 of 240 executors released during the entire run.
- 396.2 executor-hours held, 128.6 busy → **32.5% utilization, 267.5 idle exec-h**.

**Root cause is a STRAGGLER, not data skew.** Stage 6 (final stage, 4915 tasks) task idx=4844: 4,039s wall vs 301s median (13.4x) on IDENTICAL data (2.2 GiB / 70.1M records vs median 2.14 GiB / 69.9M). CPU time 213s (5% of wall), GC 1.7s, fetch wait 0 → a slow-node/IO stall. Its executor was not systematically slow (mean 84s vs fleet 62s across 245 tasks) → one-off stall, not a bad node. `spark.speculation=false`, so nothing re-ran it while 239 executors sat idle. All other 4,914 stage-6 tasks finished by 06:17:37.

**Why the fleet stayed reserved (mechanism CORRECTED by the adversarial verify pass):** shuffleTracking on serverless pins any executor whose shuffle blocks are referenced by a LIVE job — Spark 4.0.0 `ExecutorMonitor.timedOutExecutors()` excludes `hasActiveShuffle` executors BEFORE any deadline check. The final AQE job (job 4 = stages 4/5/6, 05:58→07:18) registered the stage-2/3 shuffles as active for its whole duration, and all 240 executors wrote stage-2/3 shuffle data → none release-eligible until 61s before app end. **`shuffleTracking.timeout` is NOT a fix here** (it only governs shuffles whose referencing jobs have all ended — it would have released ZERO executors this run). My initial draft recommended it; verifier `idle_hold` refuted it against Spark source. The only mid-query release lever is disabling shuffleTracking + decommission block migration, which has real risk (11.3 TB shuffle onto 38 min-executors' ~14.25 TB disk, resubmission risk) — not recommended. The correct lever is killing the tail: speculation.

**Secondary findings (verified numbers):** 88.8 TiB spill per run (78.6 mem + 10.2 disk; stage 2: 16.2, stage 3: 21.7, stage 6: 51.0 TiB). Stage 6 reads 10.29 TiB shuffle (2.14 GiB/task vs ~1.4 GiB unified execution memory per slot → 100% of its tasks spill); writes 2.07 TiB Avro.

**Cost (list-price, CUD caveat per `feedback_dataproc_cost_awareness`; all figures verified):** 2,918 DCU-h/run x $0.089/DCU-h premium ≈ $260/run (+~$20 shuffle storage) ≈ $7.8k/mo daily. Idle hold ≈ 1,960 DCU-h ≈ $175/run; the 62-min tail ≈ $160/run accounting, ~$135/run recoverable (minExecutors=38 floor).

**Recommendations for Ryan (config-only, he owns the change; both survived adversarial verify):**
1. `spark.speculation=true` (+ quantile ~0.9) — THE fix. A speculative copy finishes in ~5-7 min on the idle fleet; job 4 ends ~06:25, which also unpins/tears down the fleet (~210 of 396 exec-h saved). Committer chain (ManifestCommitterFactory + PathOutputCommitProtocol) verified speculation-safe.
2. Raise `spark.sql.shuffle.partitions` 4915 → ~30000 (top of range: even 15000 still spills at ~3.2 GiB in-mem vs 1.4 available) to cut the 51 TiB stage-6 spill. **Set in TWO places** — decorator runtime_properties AND hardcoded in the SparkSession builder (line ~89); the builder wins, changing only the decorator is a no-op. Output files go 4915→~30000 at ~72 MiB each — fine for GCS/Avro. Note this fix does not touch stages 2/3's 37.9 TiB map-side spill (input-split-driven).

**Engine improvements shipped from this run:**
- IMP-029 fixed: v2 rolling dirs (`events_1_..N_`) now parsed fully in numeric order (was: only part 1, which would have MISSED the entire tail — parts 1-3 cover the first 28 min, part 4 the final 76).
- New detector `straggler` — skew detector now cross-checks per-task data volume (`StageMetrics.data_skew_ratio`); duration skew on uniform data → speculation fix, not salting. The old detector misdiagnosed this exact case as data skew.
- New detector `idle_reserved_executors` — exec-hours held vs slot-busy, with the shuffleTracking live-job pinning callout (fix text corrected post-verify: tail fixes, not shuffleTracking.timeout).
- Deep-dive script `artifacts/audi_1194_executor_timeline.py` (registered-vs-busy timeline, low-parallelism windows, removal reasons) — candidate to fold into the engine later.
- Adversarial verify (4-agent workflow, ~300k tokens) earned its keep: 3 claims confirmed high-conf with numeric corrections; 1 refuted (shuffleTracking.timeout no-op) BEFORE the wrong rec reached the job owner. Verify-before-send stays mandatory for owner-facing tuning recs.

### Hardening pass (2026-08-07 PM): 41 corpus-confirmed defects found and fixed

Mirrored the AUDI-1191 pattern: 5 per-module finders against a 48-log / 611MB REAL corpus (both runtimes, 29KB-98MB, 3 rolling dirs) → skeptic verifiers reproducing by execution (41/42 confirmed, 1 rejected) → fix wave with a regression test per defect. Two workflow runs died to session restarts; resumed from journal cache, final 3 file-groups fixed inline. Commit `629660a1`.

**Worst confirmed defects (all fixed):**
- **Multi-frame zstd**: with the `zstandard` package installed, only frame 1 of a log decompressed → every real log parsed EMPTY and reported "clean." Worked here only because the package is absent (CLI fallback). Now streamed with `read_across_frames`; corrupt logs raise instead of passing as clean.
- **Memory**: the 98MB log peaked 18-19GB RSS (materialized 4x); now streamed end-to-end → 49MB peak, whole 48-log corpus in 58s.
- **Plan detectors were dead code on the real feed**: head-cap `[:8000]` stored the logical-plan prefix (physical plan lost in 67/159 real plans) + scan regex only matches Databricks-format text. Tail-cap fixed; OSS-format detector rework logged as follow-up (IMP-033 candidate).
- **Phantom skew**: zero-median fallback (`or 1`) manufactured 345,427x "data skew" (17 of 30 real skew findings); no duration floor let 13x-on-0.5s-tasks bury real tails (33/48 logs polluted). Zero-median → 1.0; 60s slowest-task floor (`SKEW_MIN_TASK_MS`).
- **Chimera merge**: any dir containing `events_*` parsed as ONE rolling log — the cron's flat download dir merged parts of DIFFERENT batches into one pseudo-job. Guarded (rolling = `eventlog_v2_*` name or `appstatus_*` marker); cron now reconstructs rolling dirs (`dest_for()` + `--selftest`).
- Also: stage-retry double-count, failed-task skew pollution, num_tasks=0 under dropped lifecycle events (silently disabled detectors on a 5,055-task stage), spill mem+disk conflation (6.5x overstatement), spot-preemption matching normal serverless "decommission" scale-downs, `App ID` field name, .inprogress logs invisible, ranking by phantom-high counts.

**New detectors from the pass:** `shuffle_fetch_wait` (fired immediately: 6 jobs at 53-72% fetch-wait) and zero-task idle fleets. Post-fix corpus crawl: 48/48 scanned (0 silent drops), 99 findings vs 168 pre-fix noise. **Systemic fleet finding: the hourly `aug_log_ip*`/`site_network_hourly` family runs at 2-8% executor utilization, 20-61 idle exec-h per run.**

**Sweep cost → cadence: DAILY.** 48 logs = 58s local CPU + 49MB RSS + ~600MB GCS download. The weekly launchd can move to daily once the cron's next green live run confirms.

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
- **mntn-devops#4724 (draft):** standing bucket-scoped `storage.objectViewer` on the PHS temp bucket for `audience-intelligence@` — Malachi marks ready + pings Christina. `dataproc.viewer` already standing via DEV-8182; `phs.py` enumeration works today (22 batches), fetch lights up on merge.
- **IMP-024 handoff:** owner is Ryan/targeting (not DDP); DAG is manual-only. Message drafted; profile the next manual run with the new discriminator before anyone codes a fix.
- **OSS plan-format detectors** (follow-up from hardening): scan/stats regexes are Databricks-format only; Dataproc physicalPlanDescription needs its own patterns. Backlog row pending.
- **Cadence switch weekly → daily** after the next green live cron run.
- **Databricks EXPLAIN COST acquisition** still unvalidated (pre-existing).
- **Fleet finding to route:** hourly aug_log_ip*/site_network_hourly at 2-8% executor utilization (20-61 idle exec-h/run) — candidates for min/initialExecutors cuts.
