---
doc_type: ticket
title: "AUDI-1194: Airflow/Spark optimization crawler"
status: in_progress
date: 2026-08-05
summary: "Scheduled efficiency sweep over succeeded Airflow DAGs (both engines); split from AUDI-1191 debugger"
result: "in progress — sweep now daily and full-fleet (214 jobs/278 findings vs 37/59 weekly), PHS half proven end-to-end, site_network_hourly Stage 9 verified and drafted for Ryan, Databricks EXPLAIN COST validated live"
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

### 2026-08-20: daily cadence, PHS half live, first owner-facing recommendation

**Cadence decided: DAILY, full-day coverage.** The measurement that settled it: the fleet emits **~160 event logs/day** (157-164/day, 2026-08-14..08-19), so the weekly `cap=40` sweep read ~6 hours of one day out of 168 -- **~4% of the fleet**, a coverage hole rather than a freshness preference. Cost at the measured 7.85 MB average object: ~1.3 GB/day download, ~3 min CPU, ~50 MB RSS. Shipped: `oncall_weekly_optimizer.sh` -> `.claude/scripts/oncall_daily_optimizer.sh`, `CAP` 40 -> 200, launchd `com.mntn.weekly-spark-optimizer` -> `com.mntn.daily-spark-optimizer` (11:00 PT daily, plist copy in `artifacts/`). First live run: **214 jobs scanned, 278 findings, 197 high-impact** vs 37/59/42 on the last weekly run.

**PHS half unblocked and proven end-to-end.** mntn-devops#4724 had sat as a DRAFT since 2026-08-07 with **zero reviewers requested** -- all CI green, `mergeable: true`, 219 commits behind main with no conflict. Nothing was blocking it but the ask. Branch updated, marked ready, review requested from `@SteelHouse/devops` (CODEOWNERS `* @SteelHouse/devops`) plus **`csz-mntn` = Cristina Szumilo** (note the spelling: memory had "Christina"). Slack draft in `artifacts/audi_1194_slack_cristina_phs_grant.md`.

Rather than wait for the merge, the path was proven under a 1h `audi-storage-object-view` PAM grant (auto-approved; both `audi-storage-object-view` and `dataproc-debug` are 64800s max and need 1 approval from devops-squad / gcp-audi-admins / pam-slack-bot): **22/22 PHS-attached SUCCEEDED batches enumerated, fetched (568 MB), and parsed**, yielding 21 findings on jobs the archive sweep has never seen (`materialize_mntn_select_*` Stage 6 at 40-78% fetch wait, `segment-updates-to-parquet-*` Stage 2 at 36-67%). The PHS stage is now wired into the daily script and fetches into the same download root, so both sources rank in one backlog; it 403s and skips quietly until the PR merges.

**Two `phs.fetch_logs` defects found and fixed en route** (regression tests that fail against the old code):
- `gsutil cp` had **no `-r`**, so any batch that wrote an `eventlog_v2_*` rolling dir downloaded empty and was silently dropped.
- No `dest_for()` equivalent: an `appstatus_*` marker landing beside the logs in a uuid dir makes `crawl._event_logs` read the whole dir as ONE rolling log. Top-level markers are now stripped; the marker *inside* a rolling dir is load-bearing and is kept.

**Highest-value finding actioned: `site_network_hourly` Stage 9.** Picked over the `aug_log_ip*` family on measured DCU (`runtimeInfo.approximateUsage.milliDcuSeconds`, metered not estimated): 17 SUCCEEDED runs on 2026-08-20 totalled **8,663 DCU-h, mean 510/run, range 164-1,547**, against 99-208 DCU-h/run for `aug_log_ip_hourly`.

The verify pass **refuted two hypotheses before anything reached the owner**, which is the third time that step has earned its keep:
1. *The detector's own stock fix was wrong.* `shuffle_fetch_wait` said "raise `spark.sql.shuffle.partitions`". In the same app, stages 29/35 fetch **23.4M blocks at 1,607 B with 1s of fetch wait** while stage 9 stalls on 4.2M blocks of the same size. Block count and size are not the cause, and raising partitions would multiply block count. Fix text corrected in `optimizations.py`.
2. *The source-read hypothesis was wrong.* `site_network_hourly.py` sets `shuffle.partitions=5000` in the builder and coalesces `current_partitions // 33` at ~line 203, which predicts ~151 reducers; the event log shows 74-622. The `// 33` coalesce is not what produces stage 9.

What the evidence does support: **map-side output spread.** Shuffle blocks are served by the executor that wrote them, so the reduce stage is rate-limited by how many map-side executors hold the output. Across all four logs profiled, the map stage feeding stage 9 starts with **exactly 50 executors** live (`initialExecutors=50`), runs 48-257s, and lands 90% of its output on 48-105 executors with one holding up to **24.6%**; the job's later map stages start with 306-500 executors, spread across ~480 with a **0.3%** max, and their reducers wait ~0%.

| log | stage 9 fetch wait | blocks | B/block | map-side spread | hottest |
|---|---|---|---|---|---|
| app-20260817065122856-0420 | 73% (17,379s of 23,716s) | 4,222,144 | 1,753 | 159 execs, 90% on 77 | 24.6% |
| app-20260817085115734-0691 | 64% | 1,356,749 | 1,544 | 206 execs, 90% on 105 | 19.9% |
| app-20260817125114709-0168 | 44% | 5,117,397 | 5,950 | 146 execs, 90% on 48 | 2.2% |
| app-20260820185132316-0176 | 58% | 709,722 | 1,333 | 191 execs, 90% on 85 | 18.1% |
| (clean) stage 29/35, same apps | 0-1% | 20.1-23.4M | 1,607-1,620 | 481-500 execs, 90% on ~430 | 0.3% |

**The one fact that does not fit, stated rather than buried:** stage 15 reads the *same* map output as stage 9, at comparable block count and size, and waits ~0%. The likely reason is that stage 9 is the first (cold) reader off the map-side executors' local disks and stage 15 the second (warm), but the event log cannot settle it. That is why the ask to Ryan is a **one-hour experiment** (`initialExecutors` 50 -> ~300, then re-profile) rather than a config prescription. Draft: `artifacts/audi_1194_slack_ryan_site_network_hourly.md`. Profiler: `artifacts/audi_1194_shuffle_concentration.py`. Owner routing is unambiguous: `JobTeamConfig.TPA_EXPORT` -> `Team.TARGETING`, `#alerts-tpa-pipeline`, and Ryan's last commit on the file is "Tune site_network_hourly executors and partition size" (2026-06-26). **DCU attributable to the stall is NOT established** -- that is what the experiment measures, and any dollar figure stays unproven while a CUD floor is in play.

**Databricks acquisition validated -- and the spec was wrong about the route.** On a SUCCEEDED prod run today (`prod-mntn_matched_reporting-targeted_signal_domain`, task run 502229322982640), `jobs get-run-output` returns `{"metadata": ..., "notebook_output": {}}`: no plan, no logs, no stats, and `new_cluster.cluster_log_conf` is `None`. The pipe exists and is empty. What **does** work, with no dbt change and no cluster change, is running `EXPLAIN COST` against a SQL warehouse through the **Statement Execution API** (`/api/2.0/sql/statements`). Validated live against real prod tables: a 5,412-char plan with `== Physical Plan ==`, `Statistics(sizeInBytes=)`, `Scan parquet`, and `== Optimizer Statistics (table names per statistics state) == / missing = product_categorization, product_uniques`, fed straight into `analyze_plan` -> 2 high-impact `missing_statistics` findings. First time the Databricks half has run on anything but a hand-transcribed 2026-07-31 screenshot. Module: `artifacts/audi_1194_databricks_explain_cost.py`.

Corrections that fell out of it:
- **Enumeration is solved without `system.lakeflow`.** `jobs list-runs --completed-only` surfaces the ephemeral `SUBMIT_RUN` submissions (including `targeted_signal_domain`) that `jobs list` misses. `system.lakeflow` is in fact *blocked*: `USE SCHEMA` denied, workspace `admins` is not a Unity Catalog metastore admin.
- **IMP-033 widened.** Only `missing_statistics` fires even on real Databricks output. The shuffle-size regex wants `(ShuffleQueryStage|Exchange) ... sizeInBytes=` on one line, which is the Spark UI SQL-tab rendering; EXPLAIN COST attaches `Statistics(sizeInBytes=)` to LOGICAL operators and Photon renames the physical ones `PhotonShuffleExchangeSink/Source`. So `broadcast_candidate`, `shuffle_partition_sizing`, `window_full_sort` are dead on **both** engines, not just OSS. The fix targets the plan rendering, not the engine.
- **PAT removed.** `~/.databrickscfg` `[DEFAULT]` held a long-lived token reported `Valid: NO`; `databricks tokens list` returns empty, so it was already revoked server-side. Stanza deleted, no backup kept, OAuth profile still works. `.claude/databricks_setup.md` no longer instructs recreating it. The keychain entry `databricks-ti837` still holds the dead token (IMP-049).
- Databricks group membership is now `admins` (the runbook records `producers/dev/users`).

### 2026-08-20 (later): ledger, coverage, digest, and the walkthrough deliverable

**Coverage is now stated, not implied.** `coverage.py` enumerates every unpaused DAG from the Airflow API and classifies each task by whether it can produce a Spark event log. Live result: **62 active DAGs, 24 with a Spark task, 38 structurally invisible** to this tool (BigQuery operators, sensors, plain Python), plus 4 `create_ip_verticals` tasks on Databricks job clusters whose plans are unreachable. All are listed by name in `outputs/optimizer_coverage_<date>.md`, so a backlog is never mistaken for the fleet. Auth stays in `.claude/scripts/airflow_api.py` (astro bearer); `coverage.py` shells out to it rather than holding a second copy.

**The ledger answers "did anything change".** `ledger.py` appends every finding to `outputs/optimization_ledger.jsonl` keyed on **job + detector + stage**, and replays the file to derive a state: `new` → `recurring` → `chronic` (3+ consecutive sweeps) → `resolved` (stopped firing for 3 sweeps), with `owner_notified` and `wont_fix` set by hand and **sticky across replays** because they record a human decision. Dedup is essential: the 2026-08-20 verification sweep turned **26 findings across 25 job-logs into 4 distinct keys** (an hourly job contributes ~24 logs a day and must count once).

**Two identity traps found and fixed while wiring it** — both would have made the ledger useless without failing anything:
- The finding key first took *any* digit in the title, so a title carrying task counts and byte totals minted a new key every sweep. Now it takes the stage number only (`shuffle_fetch_wait:9`), and detectors with no stage use the bare detector name rather than a dangling colon.
- `spark.app.name` carries per-run stamps. `materialize_mntn_select_16`, `_17`, `_18` are one job; `segment-updates-to-parquet-2026-08-20-[19]` is one job. But **`ipdsc_ds_67` and `ipdsc_ds_13` are different jobs** — a blind `_\d+` strip merges the whole ipdsc family into one key. Unambiguous stamps (dates, timestamps, `[n]`) are always stripped; a trailing `_<n>` is only removed when the stripped form is a DAG **the coverage pass actually saw**. That is why `sweep.py` runs coverage BEFORE the ledger.

**The digest is what a person reads.** `digest.py` leads with the delta (new / chronic / with the owner / stopped firing), links each job to its Airflow page, and prints "No change since the last sweep" when that is the truth. It links a DAG **only when the name matches one coverage saw** — Spark app names are not always dag_ids, and a dead link costs the reader trust. Written to `outputs/optimizer_digest_<date>.md`. Delivery is deliberately unimplemented (see below).

**`sweep.py` is the new post-download entry point**, so ordering and failure behaviour are testable in Python instead of spread across bash: crawl → coverage → ledger → coverage report → digest. Coverage and the ledger are best-effort; neither may sink a sweep that produced findings. The cron's `--selftest` now asserts through `sweep.run()` (4 jobs scanned + a digest written), so the wiring itself is regression-covered. 12 new tests, 40 total, ruff clean.

**Deliverables.** The workbook (`My Drive/Tickets/AUDI-1194 .../AUDI-1194 Optimizer How It Works.xlsx`) is rebuilt to 9 tabs: the step map gains O5 (ledger) and O6 (coverage + digest), the detector catalog is corrected to 14 with the fixed fetch-wait advice, "Real findings" is rebuilt on 2026-08-20 data, and there are new "Ex — site_network_hourly" and "Ledger + digest" tabs. The exhaustive line-by-line walkthrough is a companion page at **https://claude.ai/code/artifact/878ac222-4ed6-4376-aea5-cd1772308cca** — real DAG source, the gsutil corruption gotcha, one raw `SparkListenerTaskEnd` annotated field by field, the 7 surfaces, the detector verbatim, the ranked backlog, both refutations with their discriminating tables, the owner ask and fleet digest, the ledger schema, and four copy-paste demo commands.

**Runner and identities scoped, not built** (`artifacts/audi_1194_runner_and_identities.md`). The problem is not identity, it is expiry: the astro bearer dies in ~1h, the Databricks OAuth refresh needs an interactive renewal, and gcloud is personal SSO — a stale token yields a green cron run and an empty backlog. Recommendation: **GitHub Actions on a schedule with GCP Workload Identity Federation**, so no service-account key exists at all; Astro deployment token and Databricks service-principal secret in Secret Manager (those two have no keyless path); GitHub App scoped **`contents: read` + `metadata: read`**, which makes "never opens a PR" structural rather than a policy note. Sweep artifacts go to GCS rather than a repo commit, so the GitHub identity never needs write. Two of the four GCP grants already exist because DEV-8182 and mntn-devops#4724 were written against `group:audience-intelligence@`, not a person. **This is a distinct unit of work and should get its own ticket before any of it starts.**

### 2026-08-20 (part 3): the runner, reviewed twice and corrected twice

**Both corrections came from outside my own reasoning, and both mattered.**

**Correction 1 — GitHub Actions was the wrong shape.** I recommended GH Actions + Workload Identity Federation. Reading the actual pool config killed it: `mntn-prj-prod-gh-oidc` (project number `995798185124`, provider `github`) has an `attribute_condition` allow-listing **23 `SteelHouse/*` repositories**. This crawler lives in `mdunn-mntn/malachi-workspace`, a **personal** repo, which is not on it and must not be added — a personal repo on a prod OIDC allow-list means anyone with push access can mint prod GCP tokens. `airflow-ti` is not on the list either, so relocating the code does not obviously fix it. **Shape is now a Cloud Run Job + Cloud Scheduler with an attached SA**: no repository identity is involved at all.

**Correction 2 — I stated an inference as a fact, and Compass caught it.** I read the Terragrunt header on `daily-jedi-media-spend` saying *"Crossplane owns the rest of jedi-media-spend-job **IAM**"* and wrote it up as *"Crossplane owns the **V2Job manifest**"*. Compass grepped `kind: V2Job` across `argocd-v2/mgmt/platform/crossplane` (**no matches**) and `mntn-argocd` (**no `jedi-media-spend` at all**) and returned the manifest's home as **unresolved**. Only the IAM split is confirmed. Recorded as unresolved, and it is still open.

**Correction 3 — the secret store.** I assumed Secret Manager. SOP 052 makes **Vault/ESO the default** with Secret Manager a narrow exception requiring **all four** of: Google-only workload natively consuming Secret Manager, API enabled and not blocked by `restrictServiceUsage`, Workload Identity cannot eliminate the secret, exception recorded with an owner. A Databricks OAuth secret and an Astro token are third-party credentials and fail condition one outright. Its checklist leads with *"Identity possible? ALWAYS prefer Workload Identity. If identity works, no secret is allowed."*

**And the SOPS-vs-Vault contradiction I logged turned out not to be one.** They coexist **by repo** (SOP 055): `mntn-argocd` `apps-v3/secrets/**.enc.yaml` uses *Rotate a SOPS Secret*; `mntn-team-credentials` `secrets/**.enc.yaml` uses *Update Team Secret*, which also runs `sync-manifests`. **Using `rotate-secret` on `mntn-team-credentials` breaks Vault delivery.** SOPS-in-git is the transport INTO Vault, not a competing store.

**The team already owns this credential class — verified directly, not taken on trust.** `SteelHouse/mntn-team-credentials` → `secrets/team-engineering-targeting/databricks/teamsecret.yaml`: `kind: TeamSecret`, `owner: group:team-engineering-targeting`, `vault.path: teams/team-engineering-targeting/databricks`, `keys: [host, client_id, client_secret]`, described "Databricks Secrets for ShopperGraph". Siblings: `coredb`, `kafka-config`, `openai`, `reportingdb`, `sendgrid`, `targeting-secrets`, `vector-search`. The optimizer gets a **new sibling entry**, not an extension of the ShopperGraph one, so revoking one workload does not hit the other.

**IAM shape: direct bindings, not group membership.** DEV-8182 and mntn-devops#4724 both granted to `group:audience-intelligence@mountain.com`. The IAM audit **cannot expand Workspace group membership** (needs a standing Workspace admin role), so a grant routed through a group is invisible to the org's own audit — and the group also contains humans, which blurs attribution. Its `iacAttribution` is partial anyway: **499 bindings attributed to Crossplane, 0 to Terragrunt, 2,040 unmatched**, because Terraform state is not read. Whether the org nonetheless prefers the group pattern is a human ruling, not a query — the dataset that would settle it is the one that cannot see groups.

**GitHub: none.** SOP 060's Octo STS is scoped by its own first sentence to a token brokered *inside a GitHub Actions workflow* by the runner's OIDC token. A Cloud Run job has no Actions runner, so the paved road does not reach this workload. Publishing artifacts to GCS instead of committing them removes the only reason to want repo access, so the answer is no GitHub identity at all. Never a PAT — the SOP 052 FAQ prohibits it.

**Built this session:** `airflow_optimizer/Dockerfile` (on `google/cloud-sdk:slim` because gsutil is not optional; non-root; a build-time crawl of the fixtures fails the build if the parser breaks) and `sweep.publish()`, which copies backlog, digest, coverage report and ledger to `OPTIMIZER_GCS_PREFIX`. Verified against `gs://mntn-data-archive-prod/optimizer/` with a real sweep: three artifacts up, then removed.

**Ownership answers (Dustin Niehoff, #devops):** **Victor set up all the Databricks estate for TI; DPLAT wanted nothing to do with it** — so that routes to Victor/TI, and it is self-serve since I hold workspace `admins`. **"Astro owns them"** — deployment service accounts and API tokens are managed inside the Astro platform, not by MNTN devops. Neither needs a devops ticket. The Astro path for a cron is `astro deployment token create ... --clean-output` (`--expiration` 1-3650 days; omitting it means no expiry), and there is still **no built-in read-only role** — only `DEPLOYMENT_ADMIN` or a custom role.

**Two Compass notes worth keeping.** Both specialist advisors (`iam-advisor`, `secrets-advisor`) **aborted mid-run** (`cancelled: Request aborted`, blocked at 10% confidence) on an open-ended round, after burning budget on orientation calls; bound a question to a named view plus a filter and demand the run ID and collection age. And a bare numbered Q&A prompt is rejected by Compass's own report contract ("0 words; minimum 600; no root-cause gap or healthy-control block") — frame the ask as a design review and the answers come back as gap blocks.

**Still open, and no PRs until they land:** where the `V2Job` manifest lives; group-vs-direct bindings as an org preference; whether conditional IAM is used in this project (the audit run in scope was 58h stale with `denyPolicies` returning 0 rows and marked partial, so absence is not evidence of absence); whether Octo STS accepts a non-Actions OIDC issuer; SOP 052's effective date; and the Astro token's store, for which no Astro precedent exists anywhere in `mntn-team-credentials`.

## 5. Solution
- **Cadence:** daily, full-day. `.claude/scripts/oncall_daily_optimizer.sh` (renamed from `oncall_weekly_optimizer.sh`), `CAP` 40 -> 200, launchd `com.mntn.daily-spark-optimizer` at 11:00 PT.
- **Both log sources in one sweep:** the script now runs `phs.fetch_logs` into the same download root as the archive pull, so the archive fleet and the PHS-attached ipdsc/tpa batches rank in one backlog.
- **`phs.fetch_logs` fixed:** `gsutil cp -r` (rolling dirs were silently skipped) and top-level `appstatus_*` stripping (chimera guard), each with a regression test.
- **`shuffle_fetch_wait` fix text corrected** -- it recommended the one change that would make the `site_network_hourly` case worse.
- **Docs made honest:** `airflow_optimizer/__init__.py`, `README.md`, and the xlsx generator no longer present `jobs get-run-output` as a working Databricks input.
- **Recommendation delivered:** `artifacts/audi_1194_slack_ryan_site_network_hourly.md` (Ryan Kleck / Targeting), a one-hour `initialExecutors` experiment plus the full evidence table.
- **Grant unstuck:** mntn-devops#4724 out of draft with reviewers requested; `artifacts/audi_1194_slack_cristina_phs_grant.md`.
- **Credential removed:** the dead PAT stanza is gone from `~/.databrickscfg`.
- **Sweep pipeline:** `sweep.py` (crawl → coverage → ledger → digest), `ledger.py` (append-only, job+detector+stage, 5 states), `coverage.py` (62 active DAGs, 38 named as invisible), `digest.py` (delta-first, links only real DAGs). Wired into the daily cron; `--selftest` asserts through it.
- **Deliverables:** workbook rebuilt to 9 tabs; exhaustive walkthrough page at https://claude.ai/code/artifact/878ac222-4ed6-4376-aea5-cd1772308cca; runner/identity scoping in `artifacts/audi_1194_runner_and_identities.md`.
- New tooling: `artifacts/audi_1194_shuffle_concentration.py`, `artifacts/audi_1194_databricks_explain_cost.py`. Backlog rows IMP-046..IMP-049; IMP-033 widened.

## 6. Questions Answered
- **Q:** Daily or weekly?
  **A:** Daily. The fleet emits ~160 logs/day, so the weekly cap-40 sweep covered ~4% of it. A full-day sweep costs ~1.3 GB and ~3 min.
- **Q:** What was actually blocking mntn-devops#4724?
  **A:** Nothing technical. It was a draft with zero reviewers requested. CI green, mergeable, no conflict against 219 commits of drift.
- **Q:** Can the crawler read the PHS half on a schedule?
  **A:** The code path works today -- 22/22 batches enumerated, fetched and parsed under a PAM grant. Only the standing grant is missing, and it is one review away.
- **Q:** Is `site_network_hourly` Stage 9 a partition-sizing problem?
  **A:** No. Stages 29/35 in the same app fetch 5.5x more blocks of the same size with 1s of wait. Raising `shuffle.partitions` is the wrong lever. The evidenced difference is map-side spread: the feeding stage always starts with exactly 50 executors and concentrates its output; the later ones start with 400+ and do not stall.
- **Q:** Does the Databricks `EXPLAIN COST` acquisition path work?
  **A:** Yes, but not by the specced route. `jobs get-run-output` returns an empty `notebook_output` even on success. `EXPLAIN COST` via the SQL Statement Execution API works today with no dbt or cluster change, and the detectors fire on the real plan.

## 7. Data Documentation Updates

Nothing landed in `data_catalog.md` or `data_knowledge.md`: this ticket produced no schema, join
key or business-logic fact. Everything durable went to memory instead, because it is about how
the platform behaves rather than what the data means.

- `project_airflow_optimizer` — rewritten for the prod ship; the laptop-cron description is now
  history and the known post-launch defects are listed.
- `reference_airflow_ti` — four platform gotchas: the Airflow 3 ORM ban inside tasks, the 0.25
  CPU / 0.5 Gi default task pod, a failing unrelated deploy job blocking the DAG bundle, and the
  `cache: 'pip'` + `uv` defect.
- `reference_gcs_iam_creator_vs_user` — `objectCreator` cannot overwrite; impersonation from a
  pod's ADC is `serviceAccountTokenCreator`, not `workloadIdentityUser`.
- `feedback_review_own_pr_before_asking`, `feedback_branch_from_origin_not_local_main`,
  `feedback_sparse_code_comments` — working rules this ticket produced.

## 8. Open Items / Follow-ups

**Status 2026-08-21: SHIPPED.** `spark_optimizer_daily` runs in prod airflow-ti. First run:
215 jobs, 290 findings, 196 high, four artifacts in `gs://mntn-data-archive-prod/optimizer/`.
Closed since the last revision: mntn-devops#4724 (merged), mntn-devops#4971 (merged, the GSA and
its grants), airflow-ti#1212 (merged, the DAG), airflow-ti#1213 (merged, unblocked prod deploys),
and the whole runner design question.

### Broken, found by the first prod run

- **The coverage pass is dead on Airflow 3.** `collect_local` reads paused state from the
  metadata DB and a task gets `airflow session use is forbidden in this context`. The sweep
  degrades honestly (`DAG coverage unknown`, ledger declines to write rather than rekey), so it
  is a missing feature, not corruption. Fix: back to the REST API with a deployment token, or a
  Task-SDK call that exposes paused state. **This makes Ryan's token useful again** after the
  reshape had made it unnecessary.
- **The download is 200 serial `gsutil` invocations**, each paying interpreter startup. On the
  Astro default pod (0.25 CPU / 0.5 Gi, because the DAG sets no `executor_config`) run 1 took
  ~19 minutes and process spawn dominated the parse. Fix: one `gsutil -m cp -I`. Raising CPU is
  the smaller lever and should follow the batching, not precede it.
- **The digest cites the container path** (`/tmp/spark_events_*/out/...`) for the full backlog
  instead of the GCS URL.

### Next, and the ticket they belong to

Everything below is post-launch work on a shipped system, so it belongs in a **new ticket**, not
this one:

- **Slack delivery.** `digest.render()` already emits Slack markup; `compass-slack` in
  mntn-devops is the transport. This is the difference between artifacts in a bucket and a
  product someone reads.
- **Presentation.** The digest currently lists eight `fangorn_score_monitor` findings in a row
  because ranking is per-finding, not per-DAG (IMP-046). One line per DAG with its worst finding
  would read far better.
- **Databricks.** `USE SCHEMA` on `system.lakeflow` needs a Databricks **account** admin;
  workspace admin is not enough (`grants update` → `User is not an account admin`), which
  corrects Ryan's assumption. Then the `EXPLAIN COST` bridge from
  `artifacts/audi_1194_databricks_explain_cost.py` into the sweep.
- **Hand findings to the AUDI-1191 debugger.** The optimizer produces structured findings with
  file-level fixes and shares the event-log parser; the debugger already has the LLM path.

### Carried forward, unchanged

- **Send the `site_network_hourly` draft to Ryan** (`artifacts/audi_1194_slack_ryan_site_network_hourly.md`)
  and re-profile the experiment run. Deliberately deferred: get the optimizer working first.
- **IMP-024 handoff:** owner is Ryan/targeting (not DDP); DAG is manual-only.
- **Plan-shuffle detectors (IMP-033, widened):** three of five plan detectors read a Spark-UI
  shuffle rendering neither Dataproc nor Photon `EXPLAIN COST` emits.
- **`aug_log_ip*` family still unrouted:** 4-11% executor utilization, 21-52 idle exec-h/run,
  `shuffle_fetch_wait` 31-45% on 11 of 11 runs. Buried by the per-finding ranking (IMP-046).
- **PHS-only jobs newly visible and unreviewed:** `materialize_mntn_select_*` and
  `segment-updates-to-parquet-*`.
- **IMP-048:** `spark-events` has no lifecycle rule; the approved age-30 TTL was never applied.
- **IMP-049:** clear the dead `databricks-ti837` keychain entry.
- **New, from run 1:** `fangorn_score_monitor` spilled **1.7 TiB** on one stage and held ~422
  idle executor-hours at 7% utilization. Biggest single number the tool has produced; unrouted.

### The comment cleanup this ticket owes

The vendored `include/spark_optimizer/` package ships multi-line rationale comments throughout,
which violates the rule this session made explicit and enforced (`lint_comments.py` in the commit
gate). Its own linter would fail it today. Own PR, before anything else lands on the package.
