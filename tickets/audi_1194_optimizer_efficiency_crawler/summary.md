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

**Why the fleet stayed reserved:** `spark.dynamicAllocation.shuffleTracking.enabled=true` with NO `shuffleTracking.timeout` (default infinite) → executors holding shuffle blocks for the live stage are exempt from `executorIdleTimeout=144s`. All 240 ran stage-2/3 map tasks, all held shuffle files, none ever became reap-eligible.

**Secondary findings (optimizer report, `outputs/audi_1194_intent_score_map_optimizer_report_2026_08_07.md`):** ~91 TiB spill per run (stage 2: 16.6 TiB, stage 3: 22.2 TiB, stage 6: 52.2 TiB at ~1.5 GiB shuffle-read per task under shuffle.partitions=4915); stage-3 shuffle write 7.4 TiB.

**Cost (list-price, CUD caveat per `feedback_dataproc_cost_awareness`):** 2,918 DCU-h/run (milliDcuSeconds=10,504,601,634) x $0.089/DCU-h premium ≈ $260/run ≈ $7.8k/mo daily. Idle hold ≈ 1,960 DCU-h ≈ $175/run ≈ $5.2k/mo; the 62-min tail alone ≈ $160/run.

**Recommendations for Ryan (config-only, he owns the change):**
1. `spark.speculation=true` (+ quantile ~0.9) — a straggler re-runs on the idle fleet; safe with the ManifestCommitter output path.
2. `spark.dynamicAllocation.shuffleTracking.timeout=300s` — idle executors get released even while a tail task runs.
3. Raise `spark.sql.shuffle.partitions` 4915 → ~15-30k to cut the 52 TiB stage-6 spill (AQE coalesce is on, overshoot is safe).

**Engine improvements shipped from this run:**
- IMP-029 fixed: v2 rolling dirs (`events_1_..N_`) now parsed fully in numeric order (was: only part 1, which would have MISSED the entire tail — parts 1-3 cover the first 28 min, part 4 the final 76).
- New detector `straggler` — skew detector now cross-checks per-task data volume (`StageMetrics.data_skew_ratio`); duration skew on uniform data → speculation fix, not salting. The old detector misdiagnosed this exact case as data skew.
- New detector `idle_reserved_executors` — exec-hours held vs slot-busy, with the shuffleTracking-hold callout.
- Deep-dive script `artifacts/audi_1194_executor_timeline.py` (registered-vs-busy timeline, low-parallelism windows, removal reasons) — candidate to fold into the engine later.

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
Anything not resolved, handed off, or deferred.
