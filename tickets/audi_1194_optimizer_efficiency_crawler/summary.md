---
doc_type: ticket
title: "AUDI-1194: Airflow/Spark optimization crawler"
status: in_progress
date: 2026-08-05
summary: "Scheduled efficiency sweep over succeeded Airflow DAGs (both engines); split from AUDI-1191 debugger"
result: "in progress — prod DAG live, 4 surfaces (spark|bq|dbx|pod): pod surface LIVE 2026-09-01 (worker-default at 11% of its cpu limit = downsize candidate), BQ attribution verified complete 2026-09-02 (ledger unattributed bucket EMPTY, no labeling campaign needed), dbx 0 rows pending prod_runner grants (paste incl. warehouse Can-use sent to Alyson 2026-09-02); downloader freeze root-caused (gsutil -m forked workers), fix + debugger parse-rate canary on PR #1260, pod point-order fix PR #1259 — review queue; full-corpus sweep 67 pairs / 30,163+ exec-h; hackathon 13 Tasks AUDI-1269..1281 in sprint 8649 under epic AUDI-1290; digest user-verified from screenshots, rank-row alignment reformat queued"
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


### Full-corpus validation, 2026-08-26 — every archived log, not a sample

Malachi's ask: prove the tool works and name every gap before the digest goes to anyone.
Method: download every `.zstd` in `gs://mntn-data-archive-prod/spark-events` written inside the
last 30 days, parse each with `analyze_eventlog`, run all 14 detectors, and reconcile the result
against the live Airflow DAG/task inventory pulled over the REST API. Scripts are in
`artifacts/audi_1194_validation_*.py`; per-job table in `outputs/audi_1194_validation_jobs.csv`;
full result in `outputs/audi_1194_validation_analysis.json`; deliverable at
`My Drive/Tickets/AUDI-1194 Airflow Spark Optimization Crawler/AUDI-1194 Spark Optimizer Validation.xlsx`.

**Corpus.** 3,022 objects / 25 GB, collapsing to **2,954 event logs** (92 objects belong to 24
`eventlog_v2_*` rolling dirs). **2,954 of 2,954 parsed** after the no-op fix below. Span is
**2026-08-04 to 2026-08-26, 23 distinct days**, median 133 logs/day — not 30 days, because
archiving to that prefix only began 2026-08-04 (the bucket has a second, unrelated block from
2025-10-07..2025-11-12). `gsutil lifecycle get gs://mntn-data-archive-prod` confirms **no rule
matches `spark-events/`**, so nothing is expiring it and the window grows a day per day.

**Result.** 80 distinct Spark jobs, **3,620 findings**, **85,655 executor-hours**. Worst job by
far: `site_network_hourly`, 302 runs, **21,200 executor-hours**, 413 high-impact findings — the
top-10 jobs hold 79% of all executor-hours.

**Four defects found, all shipped-and-green, none visible from a passing run.**

1. **The digest linked 3 of 80 jobs to a DAG.** Two independent causes. `normalise_job` took the
   segment AFTER the dot in `Populate <table>.<Class>` (the class name) when the Airflow task is
   the table BEFORE it; and the digest never called it anyway, matching the ledger's job name
   against `dag_id`s exactly. Measured on the real fleet: old rule links 3/80, new
   `Coverage.resolve` links **77/80**. The coverage report's own `profiled this sweep` count went
   **2 -> 13**, which is the symptom that was visible in prod on 2026-08-25 and read as "the fleet
   barely ran". Fix: `job_keys` offers first segment, last segment, full name, and a digit-infix
   form (`ipdsc_14_monitor` -> `ipdsc_monitor`); `resolve` takes the first candidate naming exactly
   one DAG, and drops a name two DAGs share rather than guessing. Zero ambiguous hits on the fleet.
   **Correction to the first write-up of this finding:** the "0 of 62" figure quoted in commit
   `b80d3047` describes the coverage INDEX, not the digest. The digest's real before/after is 3/80.
   A gauntlet refuter caught the overstatement; the evidenced number is the one above.

2. **The crawl discarded no-op runs as unreadable.** `crawl()` skipped any log with no jobs and no
   stages as a truncated download. An app that allocated executors and never started a task parses
   identically. That silently dropped **15 high-impact findings over 546 executor-hours**; the worst
   was `aug_log_ip_hourly` holding **100 executors for 64.4 executor-hours with zero tasks run** —
   a finding `idle_reserved_executors` already knew how to raise and never got to. All 39 such logs
   carried a real app name and a real `ApplicationEnd`. **ApplicationEnd is the discriminator:** a
   torn download has none. Distribution of the 39: median 7s wall clock, max 4,586s (76 min).

3. **`phs_succeeded` selected 10 of 200 batches.** It filtered on `sparkHistoryServerConfig`, which
   Dataproc returns as an EMPTY dict for all but 10 batches. Of 200 recent prod batches, 13 write to
   the archive, 10 set an explicit temp path, and **175 set no `spark.eventLog.dir` at all** — and
   Dataproc still writes their log to `gs://<temp-bucket>/<uuid>/spark-job-history/`. Sampled 12 of
   the 175 at random: **12 of 12 had a readable log there.** New rule keeps any SUCCEEDED batch not
   writing to the archive: **185 of 200**. `MAX_BATCHES` 60 -> 150 (~585 MiB at the measured 3.9 MiB
   per batch, inside the 4 GiB budget that is the real guard).

4. **16 of 30 Spark DAGs ran successfully in the window and produced no readable log.** Not "they
   did not run": task-instance states over the window show e.g.
   `materialize_mntn_select.materialize` succeeded 24x, `fpa_site_visit_batch_serverless.dsid23_guid_log_processing`
   24x, `hashed_email_guid_log_signals.populate_hem_data_ds_23` 24x. Cause confirmed by
   `gcloud dataproc batches describe`: a visible batch sets
   `spark.eventLog.dir = gs://mntn-data-archive-prod/spark-events`; a dark one sets nothing.
   The temp bucket **is now readable** (403 as of 2026-08-20, open as of 2026-08-26 — mntn-devops
   #4724 is out of draft with DevOps requested). Proven end-to-end: fetched 14 batches, crawled
   14/14 clean, and `materialize_mntn_select_16` — a DAG the sweep had never seen — resolved to
   `materialize_mntn_select` with a finding at 31.7 executor-hours. Defect 3 is what closes this;
   `spark_optimizer_daily.py:100` already calls `phs.fetch_logs`, so no new wiring was needed.

**Detector coverage — 10 of 14 working, and the other 4 for a structural reason.**
Verdicts come from instrumenting a random 300-log sample of the same corpus, not from absence.

| Detector | Fired | Verdict | Evidence |
|---|---|---|---|
| `shuffle_fetch_wait` | 1,496 | working | |
| `disk_spill` | 884 | working | |
| `idle_reserved_executors` | 549 | working | |
| `shuffle_partition_sizing` | 324 | working | fires from `analyze_run`, never from the plan |
| `straggler` | 242 | working | |
| `skew` | 125 | working | |
| `gc_pressure` | 0 | working, nothing to report | `gc_time_ms` populated on 295/300; max GC share **4.3%** vs a 10% threshold |
| `spot_preemption_cost` | 0 | working, nothing to report | removal reasons populated; **no** `preempt`/`spot` string in 300 runs (serverless, no spot) |
| `shuffle_fetch_instability` | 0 | working, nothing to report | **0** FetchFailed tasks in 300 runs |
| `cache_ineffective` | 0 | **never exercised** | `cached_rdd_bytes == 0` fleet-wide; nothing caches, so the check is unproven either way |
| `missing_statistics` | 0 | **cannot run on this input** | see below |
| `broadcast_candidate` | 0 | **cannot run on this input** | see below |
| `window_full_sort` | 0 | **cannot run on this input** | see below |
| `repeated_scan` | 0 | **cannot run on this input** | see below |

**The four plan detectors are structurally dead on Spark event logs, and now measured.** 295 of 300
sampled runs DO carry SQL plan text — **4,734,637 chars of it** — but `parse_plan_text` extracts
**0 leaf scan nodes** from all of it. OSS Spark writes `Relation [cols...] parquet`; the detectors
need Databricks's `Scan parquet <table> ... Statistics(sizeInBytes=...)` annotation. This widens
IMP-033 from "the scan/stats regexes are Databricks-only" to "all five plan detectors are dead on
OSS text", and it is exactly what the `EXPLAIN COST` bridge would fix. Databricks read access was
granted 2026-08-25, so this is now buildable.

**Two jobs genuinely cannot be tied to a DAG, and stay gaps.**
`guid_log_ip_advertiser_id` — the Airflow task is `feature_group_1_source.guid_log_ip_advertiser_id_rollup`,
so the app name drops a `_rollup` the task carries; matching it would mean guessing at naming.
`ipdsc_third_party_audience_builder` — no task anywhere in the bundle defines the name.
One further name (`aug_log_ip_hourly` style collision) is claimed by two DAGs and is deliberately
dropped rather than sent to the wrong owner. Their findings still publish, without a DAG link.



### The site_network_hourly recommendation was wrong, and measuring every run is what caught it

Malachi is able to PR this himself, so the ask stopped being "get Ryan to run an experiment" and
became "write the change". Settling the mechanism first is what saved it from being the wrong
change. Tooling: `artifacts/audi_1194_stage_read_parallelism.py`.

**The detector's headline finding is real as a ratio and negligible as a cost.**
`shuffle_fetch_wait` divides `fetch_wait_ms` by `run_time_ms`, both sums of TASK time. Stage 9 of
`site_network_hourly` does almost no compute, so its denominator is tiny and the ratio is huge.
Measured on the 25 heaviest runs: stage 9's fetch wait is a median **0.28%** of the run's
executor-hours (max 18.6%, and that outlier is one run). The reported "57-90% of task time" is
true and misleading in the same breath.

**The fact that "did not fit" was never cold-vs-warm.** In one run, stage 9 fetched 721,452 remote
blocks / 1.3 GiB at 57% wait, while stages 29 and 35 fetched **18.6M blocks / 33.2 GiB at 0%
wait** — same 444 live executors, same ~1.9 KiB per block. Twenty-six times the blocks and
twenty-five times the bytes with none of the stall. Stages 29/35 simply do real work (9.2h and
8.1h of task time vs 0.9h), so fetch time is a small share of a large denominator. Map-side output
spread is NOT the mechanism, and neither is a first-cold/second-warm read.

**Where the money actually is.** Across the 30 heaviest runs the job holds a median **241
executor-hours** to perform a median **27.5 hours of task work**: a 9x over-allocation at **2.5%
slot utilization** (max 40.7%). One profiled run held **540 executors for 371 executor-hours to
run 28.9 hours of task time — 1.9%**. Fleet-wide, `idle_reserved_executors` fires on **236 of 302
runs**, median 11% utilization, and accounts for **18,334 of the job's 21,200 executor-hours
(86%)**. The recommendation was aimed at 0.3% while 86% sat in plain sight.

**What is still unproven, and why no PR ships yet.** Sizing `maxExecutors` needs a trustworthy
PEAK concurrency figure and the event log resists two naive measures. A running count over
`TaskStart`/`TaskEnd` deltas reports 3,300 concurrent tasks against 1,290 slots (>200%) because a
task whose end never lands — killed at stage end, or speculative — stays "running" forever;
counting only tasks with both a launch and a finish time still overshoots because
`SparkListenerExecutorRemoved` fires for executors added before the log window and drives the
executor count negative. Mean concurrency is solid (task-hours / wall span ≈ **34 tasks** against
2,160 slots held) but the mean does not size a ceiling. Settle peak before changing allocation.

**SETTLED 2026-08-26, and this paragraph's conclusion was wrong.** Peak concurrency IS measurable.
The overshoot is a sub-100ms slot handoff: at a peak instant four tasks finish and four launch
inside a 1-3ms window. Shrinking each interval's end by 100ms collapses every one of the 497
executors to a peak of exactly 4 and the fleet to exactly **1,988 = 497 x 4**. Time-weighted,
only 0.12% of busy executor-time sits above concurrency 4. So the job SATURATES its ceiling at
peak while averaging 43.9 concurrent tasks (2.2% of slots): lowering `maxExecutors` would
lengthen the peak, and the lever is the tail that holds the fleet, not the ceiling. Full numbers
and the corrections a verification pass forced: `artifacts/audi_1194_peak_concurrency.md`.

**Two things this changes beyond one job.**
- The Ryan draft is marked WITHDRAWN at the top of
  `artifacts/audi_1194_slack_ryan_site_network_hourly.md` rather than deleted, so the reasoning
  that produced a wrong ask stays readable.
- `shuffle_fetch_wait` needs an absolute-cost gate. A ratio on task time cannot rank against
  `idle_reserved_executors`, which is denominated in executor-hours held. Ranking the fleet
  backlog put a 0.3% finding above an 86% one on the same job. Logged as IMP-084.


### 2026-08-26 — cost got a unit, Databricks got a price, and the digest got delivered

**The cost figure was 29% short.** `SparkListenerExecutorAdded` is not a census. In
`app-20260825065124173-0803` it appears for **359** executors while **497** ran tasks; 96
`ExecutorRemoved` events cover only 48 distinct ids (each logged twice), so a running
Added-minus-Removed counter bottoms out at **-46**. Executors with no `Added` event scored zero
executor-hours. Seeding `added_ts` from the first task's launch moves the run **276.1 to 356.6
executor-hours**. Independently reproduced.

**IMP-084 closed.** `OptFinding.cost_h` carries the executor-hours at stake and `_gated` denies
`high` to a ratio finding under **10 executor-hours AND 10% of the run**. Both floors are needed:
share-only demoted a genuine 300-executor-hour stall on a 3,750-hour job. Two adversarial rounds
also forced `_cores` to return **0, not 1**, when neither `spark.executor.cores` nor
`ExecutorAdded.Total Cores` reports one — a cores=1 guess published "100 of the run's 80
executor-hours" on both committed fixtures. An underivable cost never demotes.

**Databricks now costs money, not hours.** `databricks.job_costs` and `query_costs` join
`system.billing.usage` to `lakeflow.job_run_timeline` and `query.history`, priced from
`list_prices`. Two traps: the timeline holds **one row per hourly period**, so the join must
dedupe to one row per `run_id` first (naive join inflated 7 days of `PREMIUM_JOBS_COMPUTE` from
16,460 to 205,239 DBU); and a warehouse bills by the hour, never per statement, so per-node
dollars are apportioned by query-time share, not measured. Query-hours also double-count under
concurrency.

**The four `ddp_vertical_classification_api` dbt tests are the whole warehouse.** Each reads
**5.13 TB / 2.15M files / 997 partitions with zero pruned** to produce **one row**, ~20x/day,
1,902 s of execution and 0 s of queue. They are 98.6% of warehouse `14b311ac86ee2ca2`, whose full
7-day list cost is **$850**. Detail in `artifacts/audi_1194_ddp_api_test_cost.md`.

**`heavy_queries` was returning the same four statements 15 times.** Deduped to one statement per
`node_id`; 4 of 8 distinct nodes then produce a readable `EXPLAIN COST`. The 4 that do not all
reference a table dropped ~20x/day, which replay can never resolve.

**Delivery shipped.** `notify.py` posts the digest to `#spark-optimizer` (`C0BSTH6E84T`, private)
reusing the existing `airflow-debugger` app; the gate is the credential, not a flag.
`digest.blocks()` renders Block Kit: the parent is the ranked list with each DAG's cost, owner,
finding count and worst finding, and each DAG's fix is a threaded reply. Verified live.

**Two keying bugs.** Coverage and the digest were keyed on different normalisations, so the digest
could render a job the coverage report never evaluated; `_rendered_dags` now feeds coverage every
name the digest can print. And a local sweep dropped every DAG link because nothing set
`AIRFLOW__API__BASE_URL` outside Airflow — the daily script now derives the UI base from the API
base.

**Still open.** The prod sweep's `collect_local` left `fangorn_score_monitor` and `ipdsc_ds_35`
unlinked while its own coverage report listed neither as unresolved. Against the live REST API
both resolve correctly (`audience_intent`, `tpa_ipdsc_export`), so the resolver is sound and the
disagreement is in the bundle path or in ledger names recorded on an earlier sweep. The
`_rendered_dags` fix makes the next sweep name any such job in the coverage report.


### 2026-08-26 late — one PR, and two defects in the renderer

All three open branches were consolidated into **airflow-ti #1229** so reviewers get one ask:
the two AUDI-1194 branches plus AUDI-1191's `audi-1191/two-channels`. Zero file overlap between
them, so the merge needed no rebase and no conflict resolution; 336 tests pass on the union.

The delivery gauntlet found two real defects in `digest.blocks()` before it shipped. The parent
collected only `new` + `chronic`, so a DAG in the `fix_not_working` state never appeared in the
Slack post at all even though the text digest has a section for it. And the partial-sweep and
no-change-tracking caveats were appended to the TEXT digest only, so a Slack reader saw a
confident ranked list with no indication the sweep was incomplete. Both now flow through a
`notes` tuple that the Block Kit parent renders.

One fix was **rejected**: the fixer deleted `notify._post` and imported the debugger's, inverting
the existing one-way dependency (`airflow_debugger/perf_profile.py` already imports
`spark_optimizer.optimize`) and dragging the debugger's module graph into the optimizer. Its own
report listed that finding as rejected while the diff applied it, which is why a fixer's diff is
read rather than trusted. The duplication is real and is the right thing to solve when the two
projects merge.

### 2026-08-26 close — #1229 MERGED (squash `03706e8`)

Rode in the squash: the three debugger comment blocks tightened to one line (`f48fea9`, flagged
by Malachi as failing the workspace comment lint). The spark delivery slice merged with only ONE
completed adversarial pass (run-1 round-1: 4 confirmed, 4 fixed, the notify import rejected by
hand). Two later attempts to certify a second round died without adjudicating: the first on the
session usage limit, the second on the resume defect below; a third run was cancelled at merge.

Two workspace/branch sync gaps found and closed by diffing, not by memory: the impact-hours
branch's final gauntlet commit (`244cac6`, the no-op-guard refinement) had never been ported back
to `airflow_optimizer/` (workspace commit `9c384d58`, tests 113 -> 115), and the workspace-side
`deliver_thread` port was sitting uncommitted (`f40aad70`). Diff the two trees before starting
anything; the handoff rule held.

**IMP-086:** `pr_gauntlet.js` resume replays a cached fixer's report without re-applying its
edits, while the run start resets the review files — so a resumed run reviews the pre-fix tree
and self-declares THRASH against its own ghost. The 19:0x THRASH verdict on the delivery branch
was this artifact, not a real oscillation.

### 2026-08-26 night — both deliveries verified LIVE, and the tool grew its reporting spine

**The debugger replied in a real thread.** With the Astro env vars live, a manual run on the
post-merge bundle posted NOTHING: `notify.deliver` was never called from `daily.run` (the token
gate had masked the missing wire across two PRs). The wire is airflow-ti **#1230** (CI green,
fast gauntlet, fixer added a GCS-existence re-post guard; awaiting review). The delivery itself
was then verified WITHOUT the merge by running the bundle's own chain locally with the keychain
token: `fangorn_inference_pipeline_run scheduled__2026-08-24T18:00` diagnosed
`cluster_create_stockout` high, posted `sent:true threaded:true` into the alert's live thread in
`#alerts-tpa-pipeline`, where the engineers' own fix PR (targeting-infra-ml#95) matches the
bot's remedy.

**The optimizer's threaded digest is live too.** Manual `spark_optimizer_daily` run
(`manual__2026-08-27T02:49`, bundle 02:33): **346 jobs / 274 findings / 132 high, complete**,
digest parent + 8 threaded fixes in `#spark-optimizer`. The widened PHS selector roughly
doubled coverage (217 -> 346). **The collect_local disagreement is CLOSED by observation**:
the digest now links `fangorn_score_monitor` -> `audience_intent` and `ipdsc_ds_35` ->
`tpa_ipdsc_export`; 21 unresolved names each carry a reason in the coverage report.

**New spine, all committed and ported to `audi-1194-sweep-followups`:**
- `ledger.savings()` + `render_savings()`: cumulative saved-since-X in measured units only
  (before-rate minus after-rate x days observed, resolved fixes only), published every sweep as
  `optimizer_savings.md`. 120 tests.
- `fetch.download` batched: one `gsutil -m cp -I` per destination dir (was 200 serial spawns).
- Daily gap-check loop: `.claude/scripts/daily_gap_check.sh` + launchd
  `com.mntn.daily-gap-check` (12:00 PT) writes `on-call/gap_checks/gaps_<date>.md` from the day's
  prod artifacts; a session works its checklist and folds real gaps into the open branch.

**Debugger corpus re-swept at 2,924 logs**: 123/150 classified + 19 routable = 95%;
gaps IMP-089 (watchdog logs quote other DAGs' failures; green-fire check needs an exclusion)
and IMP-090 (`databricks_guid_geos` 8-log unclassified cluster). Explainer artifacts published:
debugger `2ad4a4b8-0486-494b-903e-76f1c30683fc`, optimizer `28326201-9358-491e-83b4-9cfc1fe2e705`.
Implementation queue for the optimization PRs: `artifacts/audi_1194_implementation_queue.md`
(7 items, 2 blocked on staged owner asks). OpenAI-in-prod ask parked as IMP-088 (security
policy conflict; needs a sanctioned credential path).

### 2026-08-26 night — the DCU-to-executor-hour bridge, measured both ways

The AUDI-1191 handoff's open item "the Dataproc analyzer's DCU claims are unvalidated against
INC-005" closes with a number. INC-005's try-3 batch (`tpa-mntn-id-20260729-3`, CANCELLED at the
3h TTL) is still describable: **8,651,421,650 milliDcuSeconds = 2,403.2 DCU-h**. Its event-log
profile (`on-call/incidents/INC-005/try3_eventlog_profile.txt`) shows 150 executors held for the
full 2.944h span with 0 removed: **441.6 executor-hours**, so **5.44 DCU-h per executor-hour**
(1.36 per core-hour at 4 cores/executor). `site_network_hourly` on 2026-08-20 gives a second
point: mean 510 DCU-h/run against 51-70 executor-hours/run is a ratio of **7.3-9.9**.

The ratio is shape-dependent (driver DCUs, memory tier, allocation churn), not a constant, which
confirms the standing rule: `dcu_h` stays a separate, measured field and is never derived from
executor-hours. Any digest line pairing the two units states both as measured or omits the DCU.

### 2026-08-27 — #1230 and #1231 merged; the savings log ships; the gauntlet reverts my own change

**Both PRs merged 2026-08-27** (airflow-ti squash-and-merge). **#1230** carries the debugger's
Slack delivery wire plus this ticket's **cumulative optimizer savings log**; **#1231** is the
`fangorn_score_monitor` fix: `spark.sql.shuffle.partitions` 512/256 → **2048 in the decorator AND
the builder** (builder wins at `getOrCreate`, so both had to move).

**Savings log design, as merged:**
- Per-DAG once-only counting: saving = (before-rate minus after-rate) × days observed, resolved
  fixes only.
- Run rate spreads the saving over CALENDAR days since `applied_date`, not per observed sweep-day,
  so a weekly job does not project 7x.
- YTD year comes from the sweep date; est annual = rate × 365.
- Dollars only when `OPTIMIZER_USD_PER_EXEC_H` is set, labeled estimates.
- The headline posts into the Slack digest each sweep only when a measured saving exists.

**The gauntlet correctly reverted my evidence-backed speculation change to
`models/ipdsc/ipdsc_ds_35.py`.** airflow-ti pins `spark.speculation=false` on every GCS-writing
model: `models/audience_intent/advertiser_join.py` comments "Disabled to prevent race conditions
with ManifestCommitter", and `intent_score_map.py:54` also pins false. Speculation duplicates
in-flight write-task attempts, which the ManifestCommitter GCS commit path does not tolerate, so
speculation is not a safe straggler fix for those writers. Implementation-queue item 4
(ipdsc_ds_35 straggler) is back to **owner-gated**. Contradiction recorded in memory
`reference_dataproc_eventlog_profiling` (the 2026-08-07 "fix is speculation=true" rec stands as
the general Spark mechanism, not as an airflow-ti prescription).

**Found en route — airflow-ti CI job `model-unit-test` (`pr_model.yaml`) is broken repo-wide since
PR #1209 merged 2026-08-26.** `tests/models/test_model_read_write_in_dev.py` rewrites the
git-ignored generated `utils_model/model_core/model_config.json` with only its five pytest fixture
models during collection, so #1209's own `tests/models/test_tpa_mntn_id_export_model.py`, which
reads the real config at import, can never pass in the same run. Verified by local repro (compile
then pytest → 41 `ValueError` model_id not found). Owner rkleck-mntn; diagnosis posted as a
comment on #1231. The check is NOT required: `mergeStateStatus` UNSTABLE (red check, merge
allowed), not BLOCKED. Also confirmed: any change to a model decorator's `runtime_properties`
requires regenerating `dags/model_task_config.json` (`MNTN_SDLC_ENV=dev python model_upload.py
--dryrun`, uv dependency group `models`, commit the JSON) or the `model-upload-dryrun` check fails.

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

- **DONE 2026-08-27: fangorn applied marker written to the prod GCS optimizer ledger.** After
  the user reauthed gcloud: downloaded `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`,
  marked `fangorn_score_monitor` applied for `shuffle_partition_sizing:17`, `:19`, `disk_spill:17`,
  `:19` (fix_pr #1231, applied_date 2026-08-27, note "shuffle partitions 256 to 2048 in decorator
  and builder"), uploaded, verified 4 applied rows land. Stage-17/19 keys only: the fix's evidence
  is per-partition MiB in those two stages; stragglers and idle executors are NOT claimed, and if
  stages 23/26 spill also clears, resolution attributes via the same dag-level exec_h series.
  Next sweep (09:00 UTC) fetches this ledger; savings attribution to #1231 starts when the keys
  go quiet.

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
- **Databricks.** ~~`USE SCHEMA` on `system.lakeflow` needs a Databricks **account** admin~~ — first corrected 2026-08-21 to "Databricks-side only, nobody internal to escalate to", then **corrected again 2026-08-24 and that reading was also wrong.** David Qiu (Databricks) settled it on the support thread: lakeflow is Databricks-managed and enabled automatically, so `lakeflow system schema can only be enabled by Databricks` is expected and ignorable. The real blocker is `User does not have MANAGE`, i.e. a **metastore admin**, which MNTN has never assigned (post-Nov-2023 default, `owner: System user`). Assignment is console-only and must be a group. Original 2026-08-20 note kept below for the record:
  workspace admin is not enough (`grants update` → `User is not an account admin`), which
  corrects Ryan's assumption. Then the `EXPLAIN COST` bridge from
  `artifacts/audi_1194_databricks_explain_cost.py` into the sweep.
- **Hand findings to the AUDI-1191 debugger.** The optimizer produces structured findings with
  file-level fixes and shares the event-log parser; the debugger already has the LLM path.

### Carried forward, unchanged

- ~~**Send the `site_network_hourly` draft to Ryan**~~ — SUPERSEDED 2026-08-27: `site_network_hourly`
  is now the user's team's to fix directly (ownership shift, see the 2026-08-27 section below); the
  fix merged as airflow-ti #1232, no owner hand-off needed.
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

## 2026-08-27 — hackathon optimization list, AUDI-1241, and the ownership shift

**Full-corpus sweep: 3,085 logs** distilled to
`outputs/audi_1194_hackathon_optimizations_2026_08_27.md` — **67 distinct (job, mechanism) pairs,
30,163+ executor-hours at stake (a floor)**. Triage: **8 PR-READY, 53 VERIFY-FIRST, 6 already
queued**. **AUDI-1241** created under the Q3 tech-debt epic **AUDI-1054** to run the optimization
burn-down.

**Ownership shift 2026-08-27:** `site_network_hourly` (previously Ryan Kleck's) and the DDP dbt
tests (Sean Yang's team, which the user is on) are now the user's team's to fix directly. Queue
items 1, 2, 7 flipped from OWNER to OURS; fixes **merged (airflow-ti #1232)** or **in review
(dbt#174)**.

Confluence "TPA Pipeline On-Call Reference" (space TAR, page id `3769991216`) is remote-linked from
this ticket and AUDI-1191.

**SUPERSEDED 2026-08-28:** the Confluence content moved to the team's "TI On Call Playbook" page
(`2908061697`); `3769991216` is a redirect stub.

## 2026-08-28 — digest semantics, IMP-094, gcloud reauth

- **Digest wording pinned:** "N sweeps running" means the finding has persisted N CONSECUTIVE
  sweeps, not N total occurrences. "39 DAGs unprofiled" means those DAGs have NO Spark task at all,
  so the event-log optimizer cannot see them by design.
- **IMP-094 logged** (`improvements_backlog.md`): profile the 39 non-Spark DAGs via per-operator
  cost surfaces (BQ INFORMATION_SCHEMA slots, Databricks system tables, K8s requested-vs-used).
- **gcloud/gsutil hit `ReauthUnattendedError` 2026-08-28** on this Mac, blocking verification of
  the cumulative savings log until the user runs `gcloud auth login`.

## 2026-08-28 — #1241 merged (last Spark gap), BQ ledger table, Mode dashboard, fixlog sync

**airflow-ti #1241 MERGED** — the `adv_score` monitor writes Spark event logs via decorator
`runtime_properties`, closing the last readable-Spark coverage gap.

**Correction to the 2026-08-26/28 coverage note:** the coverage report ALREADY names the 39
non-Spark DAGs — a "No Spark task" section listing each DAG's operators. The earlier claim that it
only counted them came from reading a truncated report. IMP-094 (profile them via per-operator cost
surfaces) stands; the non-Spark profiling checklist is a comment on AUDI-1241 and the full phase
plan is `artifacts/audi_1194_nonspark_phase_plan.md`.

**BQ savings source built:**
- Dataset `mntn-prj-prod-00:optimizer` created via PAM breakglass-editor (auto-approved,
  roles/writer; `gcloud pam entitlements search` lists entitlements).
- External table `optimizer.optimization_ledger` over
  `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`, schema PINNED — autodetect
  typed `applied_date` DATE and `""` rows then failed at scan; pinned STRING.
- Mode SA `mode-analytics@dw-main-bronze` granted dataset READER via `bq update` ACL.
- `bq_run.sh` gotcha: backticked identifiers need `--use_legacy_sql=false` (bq CLI defaults legacy).

**Mode dashboard shipped:** report `e81786de8403` "Spark Optimizer Savings" in the Audience
Intelligence space, custom HTML layout (KPI cards, SVG line chart, DAG bar list, fixes table).
Report creation is `POST /api/mntn/reports` with `space_token`; layout PATCH-editable HTML; chart
specs via PATCH `view_vegas`; schedules set in the UI (API rejects documented payloads). Full API
mechanics: memory `reference_mode_api`.

**Savings semantics confirmed:** per-DAG per-CALENDAR-DAY exec-h (all runs of the day summed, run
frequency inherent). Before window = all ledger days pre-merge (fixed); after window grows daily,
recomputed every sweep indefinitely. **First real saving: fangorn #1231 = 575.6 exec-h/day =
$160.02/day, est $58.4k/yr at $0.278/exec-h.** Ledger: 446 rows with `exec_h`, 266 findings-only.

**`airflow_optimizer/fixlog.py` NEW** — syncs the "Optimizer fix log" section (markers
`optimizer-fixlog-start`/`optimizer-fixlog-end`) from the ledger into playbook `2908061697`; runs
in `daily_gap_check.sh`'s noon job.

## 2026-08-28 (later) — #1245 merged: the optimizer covers spark, bq, and dbx surfaces

**airflow-ti #1245 MERGED 2026-08-28** — all three surfaces shipped in ONE PR at the user's
request (142 tests, gauntleted):
- `bq_profile.py` profiles BigQuery per dag/task from `INFORMATION_SCHEMA.JOBS_BY_USER` via REST
  with a gcloud token; the fleet SA reads its own job history, so no new grant.
- Ledger `Entry` gained `surface` (`spark` | `bq` | `dbx`); resolution and savings are scoped per
  surface so slot-hours, DBUs, and executor-hours never mix.
- `billing.surface_rates()` prices bq slot-h from the billing export (service
  `'BigQuery Reservation API'`, sku `LIKE '%Slot%'`), env fallback `OPTIMIZER_USD_PER_SLOT_H`.
- `databricks.findings_reports()` detectors: `dbx_heavy_job` (>$50/day list),
  `dbx_failing_model` (3+ fails/7d).
- `sweep` writes `optimizer_bq_<date>.md` and records Databricks findings under `surface="dbx"`.

**BQ attribution, verified empirically** (memory `reference_bq_job_attribution`):
`BigQueryInsertJobOperator` stamps `airflow-dag`/`airflow-task` labels on every job (direct
inserts also carry `job_id` `airflow_<dag>_<task>_<ts>`); python-client jobs inside tasks carry NO
labels; airflow-ti/camperbid jobs bill in `dw-main-bronze`; `JOBS_BY_USER` needs no extra grant
for a SA reading its own jobs; `JOBS_BY_ORGANIZATION` and `mntn-prj-prod-00` `JOBS_BY_PROJECT`
are access-denied to `malachi@mountain.com`.

**Mode dashboard per-surface:** external table `optimizer.optimization_ledger` schema re-pinned
with `surface STRING` (`ignoreUnknownValues`); headline dollars query is now spark-only; new
"Savings by surface" query (token `513a4a7a4a71`); layout gained a Savings-by-surface table.

**Pod profiler BLOCKED** on the Astro Universal Metrics Exporter → GCP Managed Prometheus; setup
steps committed at `artifacts/audi_1194_astro_metrics_exporter_setup.md`. (**Superseded
2026-08-31:** the DEV-8821 relay is LIVE — see the 2026-08-31 evening section.)

**Open:** verify tomorrow's sweep writes the optimizer_bq/dbx sections and ledger rows carrying
`surface` fields.

## 2026-08-28 (evening) — live multi-surface validation: identity bug found and fixed same night

**Triggered the sweep post-#1246; the digest still said 39 unprofiled.** Pulled the task logs via
the astro CLI token (Bearer against the Airflow REST API) and found the bigquery surface skipped
(`jobs.create` denied) and billing unreadable. Root cause: **the sweep runs as
`spark-optimizer@mntn-prj-prod-00`** (the DAG's `SERVICE_ACCOUNT` impersonation), but the #5121
billing grant went to `airflow-ti-prod@` and `JOBS_BY_USER` under `spark-optimizer@` sees none of
the fleet's jobs — so the live billing rate has always fallen back to
`OPTIMIZER_USD_PER_EXEC_H=0.278` and the BQ profiler returned an empty day.

**Two PRs opened:** airflow-ti**#1247** (`JOBS_BY_PROJECT` filtered to `OPTIMIZER_BQ_SAS`, default
`airflow-ti-prod` + `airflow-camperbid-prod`; also made the sweep tests hermetic — they were
silently querying real BigQuery from a credentialed laptop via the unstubbed `bq` pass) and
mntn-devops**#5160** (`bigquery.jobUser` + `resourceViewer` on `dw-main-bronze` for
`spark-optimizer@`; the `iam/spark-optimizer/` dir already existed there). BQ surface blocked
until both merge. **UPDATE 2026-08-29: both merged, BQ surface live-verified — see the next
section.**

**DEV-8821 filed** (DEV board, DevOps Request / Infrastructure Improvement, linked Relates To
AUDI-1241) for the pod-metrics relay: Astro's Universal Metrics Exporter only does remote-write
with static auth, GMP requires OAuth, so an OTel collector on Cloud Run
(`monitoring.metricWriter`) relays. `artifacts/audi_1194_astro_metrics_exporter_setup.md`
rewritten accordingly.

**monitor-tpa added to `SLACK_ALERT_CHANNEL`** (`"C08CURMGNMQ,C067ZM2EC5S"`) via
`astro deployment variable update`.

**Pending debugger live test:** `fangorn_household` driver death (workers connection-refused to
the driver; cleared + retried by Malachi). It exposed the rapid debugger's lookback gap — a
failure whose end_date falls outside the 45-min window during a cycle pause gets no thread reply
(IMP-095).

## 2026-08-28/29 (night) — live BQ surface verified, billing rate live

**Both fix PRs MERGED (mntn-devops#5160, airflow-ti#1247).** After the Astro deploy went HEALTHY,
triggered `spark_optimizer_daily` (`manual__2026-08-29T01:23:07`, success). Live verification:
- **BQ surface live:** `optimizer_bq_2026-08-29.md` published; top entry
  `bos__spend campaign_summary_hourly-create` at 69 slot-h. Ledger now carries `surface=bq` rows
  (`bos__spend` 123.7 slot-h, `intent_score_threshold_v4` 54.1).
- **Billing rate LIVE, no more env fallback:** sweep log reads
  `[sweep] usd/exec-h 0.278 (blended from 30d of actual spend: $0.0511/DCU-h x 5.44 DCU-h per
  executor-hour)`. The live blended rate happens to equal the old `OPTIMIZER_USD_PER_EXEC_H=0.278`
  fallback value; prior savings dollars stand.
- **Coverage "0 cost-profiled of 39" is NOT a bug:** today's labeled BQ jobs all came from Spark
  DAGs; no-Spark `BigQueryInsertJobOperator` DAGs (category_taxonomy etc.) tag only on days they
  run; most of the 39 are pod/sensor DAGs blocked on DEV-8821 (pod-metrics relay).
- **No dbx ledger rows 2026-08-29:** nothing crossed the $50/day or 3-failure thresholds — clean
  day, not a defect.
- `fangorn_household_14day_lookback` succeeded on the second manual retry.

**Operational facts learned:**
- Mode report `e81786de8403` refreshes via API: `POST /api/mntn/reports/e81786de8403/runs` with
  `MODE_API_TOKEN`/`MODE_API_SECRET` from `~/.zshrc` (run `48140b50e8dc` succeeded).
- Airflow REST log pulls: `Accept: text/plain` is rejected ("Only application/json or
  application/x-ndjson"); logs paginate via `continuation_token`.
- Jira service-account request is **ITS-6496** (Pending External, Robin Fox).

## 2026-08-31 — hackathon refinement: 13 tickets filed into sprint 8649

**Bryce's hackathon structure (refinement 2026-08-31):** a fall tech-debt hackathon sprint with
three tracks — alerting audit, pipeline testing framework, pipeline optimization audit.
Refinement format: 30 min writing tickets + 30 min group review. Epic **AUDI-1290 "Pipeline
Optimization Hackathon"** was created 2026-08-31 and parents all 13; labels `hackathon` +
`q3_2026` on epic and children; descriptions rewritten to the laymen-BLUF+links standard.

**13 AUDI Tasks filed into hackathon sprint 8649 (09/07-09/21, board 1814): AUDI-1269..1281**,
drafted in `outputs/audi_1194_hackathon_ticket_drafts.md` (fix values pre-verified in the 08-27
corpus sweep `outputs/audi_1194_hackathon_optimizations_2026_08_27.md`).
**Grouping rule (user's): tickets group by CHANGE TYPE, not by DAG** — the same change across many
DAGs is ONE ticket; a different change on the same DAG is a DIFFERENT ticket; 1-2 SP each.
- AUDI-1269 shuffle.partitions pre-verified (10 DAGs, 2SP) · 1270 shuffle.partitions verify-first
  (15, 2SP) · 1271 initialExecutors pre-verified (2, 1SP) · 1272 initialExecutors verify-first
  (10, 2SP) · 1273 maxPartitionBytes (3, 1SP) · 1274 AQE advisory 16m (2, 1SP) · 1275 straggler
  decision (13, 2SP) · 1276 skew (4, 1SP) · 1277 BQ heavy queries (2SP) · 1278 BQ labels (1SP) ·
  1279 OpenAI observability (2SP) · 1280 tag-coverage CI (1SP) · 1281 perf-regression POC (2SP).
- **SP split:** 16 SP assigned to Malachi (1270, 1271, 1272, 1275, 1276, 1277, 1278, 1279, 1280,
  1281); 4 SP left deliberately simple for others (1269, 1273, 1274).

**No optimizer rescan was needed:** the 08-27 corpus sweep is still authoritative (fleet configs
unchanged since; the two merged fixes #1231/#1232 already measure in the ledger). New since
08-27 and folded into the drafts: live BQ-surface findings — `bos__spend` 1,275 + 977 slot-h/day,
`intent_score_threshold_v4` 1,075 slot-h, unattributed (unlabeled) jobs 1,185 slot-h/day →
AUDI-1277/1278.

**Cost-savings provenance during the hackathon:** the ledger auto-measures savings regardless of
who authors the fix (a finding that stops firing resolves and its savings accrue). Provenance is
stamped per merged fix with `python -m airflow_optimizer.ledger applied <dag> <key> <pr> <date>`;
plan = daily reconcile of merged airflow-ti PRs vs ledger findings during the sprint. Recorded in
memory `project_airflow_optimizer`.

**Jira mechanics verified in the filing** (routed to memory `reference_jira_conventions`): sprint
move = `POST /rest/agile/1.0/sprint/{id}/issue {"issues":[keys]}`; AUDI scrum board 1814; sprints
as of 2026-08-31: 8303 (active, ends 09/07), 8649 (hackathon, 09/07-09/21), 8650 (next); story
points = `customfield_10012`; assignee = `PUT /rest/api/2/issue/{key}/assignee {"accountId"}`.

**Handoff pointer:** cross-ticket next actions live in
`tickets/audi_1191_airflow_spark_debugger/outputs/audi_1191_next_actions_2026_08_31.md`.

## 2026-08-31 (evening) — dbx surface PR #1250, PR #1252, DEV-8821 relay LIVE

**PR #1250 OPEN — Databricks surface via SP OAuth REST.** `databricks._api` routes through curl +
a cached OIDC client-credentials token when `DATABRICKS_HOST` / `DATABRICKS_GCP_CLIENT_ID` /
`DATABRICKS_GCP_CLIENT_SECRET` env are set; the CLI fallback stays for laptops; the sweep prints
a skip line when no warehouse is configured.

**Root cause of dbx dormancy:** `databricks.report()` returns `""` SILENTLY without
`DATABRICKS_WAREHOUSE`; prod's image has no databricks CLI and only the `CLIENT_SECRET` var is
set — the surface never errored, it just produced nothing.

**The "ml_squad warehouse" is the MAIN workspace** `1262887251702944.4.gcp` (dbt
`ml_squad/profiles.yml`): warehouses `Serverless Starter` `14b311ac86ee2ca2` +
`sql_warehouse_2xs` `fa27430dfc609e6d`. Workspace SPs: `dev_runner` `81b867bc`,
`spark_optimizer` `07f36af7`, `prod_runner` `397d710b` (candidate client id for the prod vars;
whether it pairs with the EXISTING secret is verifiable only via the sweep log).

**dbt PR 174 (SteelHouse/dbt) baseline captured:** `prod-ml-ddp_vertical_classification_api` is
the top warehouse consumer — 306,352 query-s / 244 runs over 7 days. After #1250 merges: set
`DATABRICKS_HOST` + `DATABRICKS_GCP_CLIENT_ID` + `DATABRICKS_WAREHOUSE` on prod, verify dbx
ledger rows, stamp PR-174 provenance against this baseline.

**PR #1252 OPEN:** sweep-note `gs://` refs render as console URLs via `digest._gcs_link`;
`coverage.resolve` consults `OPTIMIZER_NAME_OVERRIDES` env JSON (app name → dag id) for names the
bundle crawl cannot tie (`ETL Audience Intent - *`, `segment-updates-to-parquet`) — populate the
values with the owning team before setting the var.

**DEV-8821 relay LIVE:** Cloud Run `astro-metrics-relay` in `mntn-prj-prod-00`, remote-write
`https://astro-metrics-relay-r64eabgqfq-uc.a.run.app/api/v1/write` (basic user `astro-metrics`,
password in Keychain service `astro_metrics_relay`); Astro prod Metrics Exports configured
~19:45 UTC 2026-08-31. GMP PromQL endpoint
`https://monitoring.googleapis.com/v1/projects/mntn-prj-prod-00/location/global/prometheus/api/v1/*`
— gotchas: `__name__` regex matcher unsupported; `label/__name__/values` returns ~18k built-in
Google names (filter `:` and `/` out to see prometheus-ingested); Malachi lacks `serviceusage` on
`mntn-prj-prod-00` (no relay log reads; the monitoring query API works). **Verification pending:**
no `container_*` series yet; Cristina checking relay logs. Then `pod_profile.py` (ledger surface
`"pod"`). Memory: `reference_astro_metrics_relay`.

**AUDI-1302 filed then closed Won't Do the same day per the user** — PR-only work he is driving
needed no ticket. Jira DELETE returns 403 without admin; sprint removal =
`POST /rest/agile/1.0/backlog/issue`. Lesson routed to memory
`feedback_auto_capture_and_ticket_flag` (§14: flag first, even for underway follow-on work).

## 2026-09-01 — #1250/#1252/#1253 live in prod; dbx engaged with a grants blocker; relay end to end

Full detail: `tickets/audi_1191_airflow_spark_debugger/outputs/audi_1191_next_actions_2026_08_31.md`
(kept current).

- **All four airflow-ti PRs merged and LIVE** on image `deploy-2026-09-01T19-06-22` via
  retrigger PR #1254 — Astro cancels superseded builds on back-to-back merges and never built
  the final SHA (memory `reference_astro_deploy_mechanics`). Env vars `DATABRICKS_HOST` /
  `DATABRICKS_GCP_CLIENT_ID` / `DATABRICKS_WAREHOUSE` staged on Astro prod.
- **dbx REST ENGAGED:** oauth works; the secret pairs `prod_runner` `397d710b`
  (`spark_optimizer` `07f36af7` does not — 401, swap-and-revert test). **Blocker:** `prod_runner`
  needs `SELECT` on `system.lakeflow` + `system.query` and `CAN USE` on warehouse
  `fa27430dfc609e6d`; grants ask drafted to ml_squad/Brian. The sweep's `[sweep] databricks
  skipped: no warehouse configured (DATABRICKS_WAREHOUSE)` prints despite the var being set —
  check `sweep.py` message routing.
- **`OPTIMIZER_NAME_OVERRIDES` draft** in `outputs/audi_1194_hackathon_ticket_drafts.md`:
  `segment-updates-to-parquet` → `materialize_mntn_first_party` confirmed; the `ETL Audience
  Intent - *` prod launcher is unconfirmed (apps in `spark/audience_intent/*.py`; owning team
  confirms before the var is set). RESOLVED later the same day: the var was SET with the ETL
  entries excluded — see the next section. IMP-097 filed: per-DAG owner mapping idea.
- **DEV-8821 relay FULLY LIVE** (zero drops; `kube_pod_status_phase` 162 series; `container_*`
  filling) after mntn-devops PRs 5193/5210/5218/5220 — memory `reference_astro_metrics_relay`.
  The 08-31 "Malachi lacks serviceusage / no relay log reads" note above is stale: the denial is
  gone. Next: `pod_profile.py`, ledger surface `"pod"`.

## 2026-09-01 (later) — "39 unprofiled" decomposed (PR #1255), overrides live, pod surface PR #1257, Mode BQ table

**The digest complaint "39 DAGs unprofiled" decomposed into three causes, none a profiler bug:**
1. **7 paused DAGs counted as active** — the ORM paused-set read is FORBIDDEN inside Astro tasks
   (`airflow session use is forbidden`); **PR #1255 OPEN** adds a REST fallback via
   `AIRFLOW_BEARER`.
2. **Only 1 DAG genuinely cost-covered** — the dbx surface is still blocked on the `prod_runner`
   grants.
3. **Blunt chip wording** — reworded to "N DAGs without cost data", computed from
   `Coverage.invisible`.

**`OPTIMIZER_NAME_OVERRIDES` SET on Astro prod: 14 source-verified entries** — all 12 unmatched
app names plus the hashed-email apps (`ds=22`/`ds=29`). The `ETL Audience Intent - *` entries
stay EXCLUDED pending owner confirmation.

**Pod surface PR #1257 OPEN** — `pod_profile.py`, ledger `surface="pod"`, unit core-hours/day,
findings `cpu-overprovisioned` + `memory-pressure`, reading requested-vs-used from the DEV-8821
relay metrics. Relayed counters land under the GMP `/unknown` descriptor variant and are
invisible to PromQL — read via the Cloud Monitoring v3 API (memory
`reference_astro_metrics_relay`). **Blocked on mntn-devops PR #5224**
(`roles/monitoring.viewer`) **+ the `OPTIMIZER_POD_PROJECT` env var.** #5224's gauntlet fixer
swapped in `roles/monitoring.metricReader`, which does not exist in GCP (IAM API 404), and the
refuter confirmed it anyway — caught pre-ship; rule routed to memory
`feedback_gauntlet_findings_not_fixes`.

**Mode dashboard: BQ cost table added end-to-end via the API** —
`POST /api/mntn/reports/e81786de8403/queries` created "BigQuery cost by task" (query token
`3ead7301daa8`), a layout PATCH added section `opt-bq`, and run `d2d0b89e9cef` succeeded.
Pattern routed to memory `reference_mode_api`.

**Review queue at close:** airflow-ti #1255/#1256/#1257 + mntn-devops #5224. *(Superseded the
same night: all three combined into PR #1258 and merged — next section.)*

## 2026-09-01 (evening) — combined PR #1258 LIVE; pod first light exposes two prod bugs (#1259/#1260); 12-day diagnosis

**PRs #1255/#1256/#1257 combined into PR #1258** (originals closed as superseded, branches kept;
octopus merge, 430 tests green) so one airflow-ti merge = one Astro deploy — no superseded-build
exposure. **#1258 MERGED + LIVE on image `deploy-2026-09-01T22-22-40` (HEALTHY).** mntn-devops
**#5224 MERGED** (`roles/monitoring.viewer` synced to IAM); `OPTIMIZER_POD_PROJECT=mntn-prj-prod-00`
set post-deploy. The failure-trigger plugin `airflow_debugger_trigger` is REGISTERED in prod
(`GET /plugins` lists it with its listener).

**Pod surface first light:** verification sweep `manual__22:36` SUCCESS —
`optimizer_pod_2026-09-01.md` published, pod ledger rows landed, honest warehouse message
confirmed. **But the numbers were wrong: Cloud Monitoring v3 `timeSeries.list` returns points
NEWEST FIRST**, so the cpu rate (computed oldest-minus-newest) went negative, was filtered to 0
cores everywhere, and `exec_h` came out NULL. **Fix PR #1259 OPEN** (branch
`audi-1194-pod-point-order`: rate + limits use the newest point; rate divisor = the span between
point TIMESTAMPS, so sparse points no longer inflate; fixture reversed to newest-first),
**verified live: worker-default 0.875 cores = 11% of its 8-core limit (a real downsize
candidate); dag-processor 55% of its cpu limit.** Two gauntlet runs on #1259 died on API server
errors mid-fixer, each leaving half-applied edits in the tree (an unused helper; a dangling call
to a never-written function) — diff before building on any post-gauntlet tree; an ERROR verdict
means findings stand unapplied; a resume replays cached agents free (memory
`feedback_gauntlet_findings_not_fixes`).

**Downloader freeze root-caused (the diagnosis's #1 item):** every sweep since 2026-08-28 exited
"Done" with ~2/192 event logs landed (194/200 counted failed), freezing finding resolution for 6
consecutive sweeps. Cause: gsutil `-m` FORKS worker processes that die quietly on the 0.25-CPU
pod while the parent exits cleanly. Proven by isolation: forked `-m` hangs/loses files on the
Mac AND the pod; plain `cp` and `-m` with `GSUtil:parallel_process_count=1` (threads-only) copy
everything on both. **Fix PR #1260 OPEN** — threads-only `-m` via `GSUTIL_OPTS` in `fetch.py`
(gauntlet clean pass). Also: spark-events objects are GHFS-synced composites with NO stored
hashes — gsutil's "Found no hashes to validate" warning under `check_hashes=never` is benign.
Memory `reference_gcloud_storage_over_gsutil`.

**Full-production-history diagnosis written and verified:**
`outputs/audi_1194_diagnosis_2026_09_01.md` — the "30 days" ask exceeds the system's life; the
report covers ALL 12 days (2026-08-21..09-01, 936 ledger rows, BQ table vs GCS mirror exact
match). Headlines: (1) the downloader freeze is the root of nearly everything downstream
(dags/day 65→20, 30 DAGs never seen again after 08-26, "nothing reported as resolved" 6 sweeps
running) — now fixed via #1260; (2) **dbx surface 0 rows ever** (blocked on the `prod_runner`
grants, ask to ml_squad/Brian outstanding); (3) **the debugger and optimizer see near-disjoint
fleets** — the debugger's top-3 offenders (72% of its diagnosis rows) have zero ledger rows
ever (Databricks-API/dbt/pod/OpenAI jobs, exactly this system's blind spots).

**Review queue at close: airflow-ti #1259 (pod rate) + #1260 (downloader; retitled 2026-09-02: + parse-rate canary).** After both merge +
deploy: manual sweep, expect `complete=True` and resolutions flowing again.

## 2026-09-02 (morning) — BQ attribution complete; grants paste to Alyson; PR #1260 gains the canary; digest verified

- **The BQ cost surface's unattributed bucket is EMPTY in the ledger (verified 2026-09-02):**
  every BigQuery job the surface measures carries `airflow-dag`/`airflow-task` labels — **no
  team-labeling campaign needed.** The 08-31 raw-profiling figure (unlabeled jobs 1,185
  slot-h/day) is a different population; reconciling hypothesis = jobs outside the
  airflow-launched measured set (ad-hoc/service jobs); settle in AUDI-1278 by joining the
  profiled unlabeled jobs against the ledger population. Routed to `knowledge/data_knowledge.md`.
- **The 35 "without cost data" DAGs close only three ways:** (1) the `prod_runner` dbx grants,
  (2) hackathon per-DAG event logging, (3) genuinely-no-compute. **Grants paste sent to Alyson
  2026-09-02** for `prod_runner` (`397d710b`): `system.lakeflow` + `system.query` SELECT
  ladders + warehouse `fa27430dfc609e6d` Can-use. Warehouse access is NOT SQL-grantable — UI
  SQL Warehouses -> Permissions -> "Can use", or the Permissions API (memory
  `reference_databricks_system_schema_grants`).
- **User verification pass (screenshots): the digest works as designed** — override links
  (incl. ETL Audience Intent), hour dots, deltas, cost chip, pod/BQ report links, threaded
  What/Fix, honest partial-sweep note. **New ask: ranked rows read ragged in Slack (emoji +
  number prefixes misalign) — reformat queued for the post-merge digest pass** (memory
  `feedback_slack_digest_not_per_event`).
- **PR #1260 retitled: "AUDI-1191/1194: downloader loses the batch; canary for silent parse
  breaks"** — the debugger's parse-rate canary rides it (275 tests; detail in AUDI-1191
  summary §7p). **Review queue: #1259 + #1260.**

## 2026-09-02 (overnight) — digest numbered list shipped; every unlinked job source-verified to its DAG; overrides live

- **Ranked digest rows are now one Slack `rich_text` ordered list** (commit dd53939, PR #1260). Slack cannot align hand-numbered mrkdwn rows: the emoji prefix shifts each number and wrapped lines lose their indent. With `rich_text_list style=ordered` the client renders the numbers in the gutter and hanging-indents every wrapped line. Meta (owner, findings, streak, delta) moved from the small-grey context block into the item as an italic third line; small-grey is not expressible inside rich_text, accepted trade. Format preview posted to #spark-optimizer 2026-09-02 ~02:55 with real 09-01 rows; screenshot confirms true numbered-set rendering. Tests updated: rank rows live in one rich_text block, so per-row assertions now index `ranked["elements"][0]["elements"]`; emoji assertions dropped the colons.
- **Why rows 1/3/8 had no Airflow link — answered at source.** All three ARE airflow-ti DAGs; the Spark appName is a free-form string the resolver cannot tie to a dag_id:
  - "ETL Audience Intent - {Prospecting Keywords, Prospecting Mid, Prospecting High, Vertical Mid, Vertical High}" = `audience_intent` DAG, which submits all five scripts (dags/audience_intent/audience_intent.py script_name= lines 415, 428, 453, 469, 525; appName strings confirmed in spark/audience_intent/*.py).
  - "Run Single-Day TPA Export for <date>" = `tpa_ipdsc_export` -> tpa_export_spark_batch (include/spark/data_source/ipdsc_emr_cluster.py:145) -> spark/exporter/export_tpa.py:94 (dated f-string appName).
  - "Populate targeted_signal for CRM source" = `targeted_signal_crm` DAG (dags/tpa_export/targeted_signal_crm.py TiPysparkBatchOperator -> spark/data_source/populate_targeted_signal_crm.py:23).
- **OPTIMIZER_NAME_OVERRIDES on prod: 14 -> 22 entries** (verified live on cmd6bd10c0gl901rfuokgryiq). Five ETL Audience Intent names -> audience_intent; "Populate targeted_signal for CRM source" AND "targeted_signal for CRM source" -> targeted_signal_crm (both spellings because the digest displays the Populate-stripped form); "Run Single-Day TPA Export for *" -> tpa_ipdsc_export.
- **coverage.resolve gained trailing-wildcard prefix overrides** (commit 3d87c6f, PR #1260): a key ending `*` matches by startswith, longest key wins, exact entry beats prefix. Needed because the TPA export names itself by date, so no exact override can ever own it. Exact entries work on current prod code at next pod restart; the wildcard needs the #1260 deploy.
- **Open question (did not block): flagged apps' event logs vanish from the archive.** Backlog app ids from both 09-01 and 09-02 (e.g. app-20260901043257483-0613, app-20260902053239432-0486) 404 in gs://mntn-data-archive-prod/spark-events while same-hour neighbors persist. The sweep read them fine at 07:00 UTC. Something removes or renames them within hours. Launchers were verified from source instead of log fingerprints.
- **Local Slack posting gotcha:** ~/.zshrc SLACK_BOT_TOKEN is the decommissioned bot (account_inactive). The live token is keychain `security find-generic-password -s slack_bot_token -w`; preview posts as the airflow-debugger app with OPTIMIZER_SLACK_CHANNEL=C0BSTH6E84T.

## 2026-09-02 (post-merge verification) — threads-only gsutil FALSIFIED in prod; system.billing was the missing grant

- **Deploy chain:** #1259+#1260 merged 16:56/16:58 UTC; Astro built neither (the #1259 build canceled as superseded, the #1260 push never registered a build). Recovery: UI "Retry Git Deploy" (main@d417679, live 17:34) then README PR #1262 merge forced a fresh full-HEAD build (deploy-2026-09-02T17-43-14). Lesson: the GitHub "Deploy to Prod" action only syncs GCS artifacts; a dead superseded build recovers only via a new push or the Astro UI retry.
- **Verification sweep (manual, 17:54 UTC, new image): mixed.** WORKING: override links live in the digest (audience_intent · intent_score_map, materialize_mntn_first_party linked), pod report real (verified locally too: worker-default 12% cpu of limit, dag-processor 65%), BQ report live, blended rate 0.277 usd/exec-h. BROKEN: downloads 194 of 200 failed; Databricks all reads INSUFFICIENT_PERMISSIONS.
- **CORRECTION (falsifies 2026-09-01 note): `-m` + `parallel_process_count=1` (threads-only) is NOT clean on the prod pod.** It lost 192 of 194 objects ("Operation completed over 2 objects" per batch): gsutil consumed ~2 stdin entries per invocation then reported Done. The 09-01 threads-only validation ran on the Mac; prod falsified it. Source deletion ruled out: immediate re-stat of 30 freshly listed objects found 0 missing. Fix: drop `-m` entirely, plain `gsutil cp -I` per destination dir (the mode the isolation matrix always showed clean). Branch audi-1194-fetch-sequential commit 4f76dfa, gauntlet PASS (2 found, 0 confirmed).
- **Databricks: the exact missing privilege was `USE SCHEMA` on `system.billing`** (SQLSTATE 42501) — the cost queries join billing.usage/list_prices and system.billing was never in the grants ladder (only system.lakeflow + system.query). Warehouse access confirmed working (query reached the SQL engine). Alyson ran USE SCHEMA + SELECT on system.billing same hour. Next sweep verifies rows.
- **Vanishing-logs mystery narrowed:** fresh listings re-stat clean, so whatever deletes flagged apps' logs operates on a longer horizon than the sweep window; it did NOT cause today's download loss.

## 2026-09-02 (evening) — downloader root cause final: gsutil itself is broken on Astro pods; fetch rewritten to the GCS JSON API

- **Sequential gsutil ALSO lost the batch in prod** (19:35 sweep pending as this is written; 18:38 sweep on #1263: 194 of 200 failed, "Operation completed over 2 objects" per invocation), while the identical sequential command moved all 194 objects (1.8 GiB) from the Mac. Conclusion: not concurrency, not source deletion (fresh listings re-stat clean), but gsutil-on-Astro-pod, the same environment failure the debugger's markers hit on 2026-08-28 (PR #1243), which is also the exact day optimizer resolutions froze.
- **fetch.py rewritten to gcloud-token + GCS JSON API** (PR #1264, merged): objects.list with pagination replaces `gsutil ls`; per-object `alt=media` reads replace `gsutil cp -I`; stored bytes arrive untranscoded (zstd -t verified on live smoke, 6/6). Gauntlet FIXED_UNVERIFIED round confirmed 2 hardenings (listing JSON validated; non-404 metadata error raises instead of reading as absent), mechanical gate + 38 tests re-run green.
- **Deploy history lesson repeated 3x today:** superseded builds die quietly (#1259 canceled, 59c81cb never built); recovery = UI Retry Git Deploy or a new push. README documents it (#1262).
- Interim PR #1263 (drop -m, keep gsutil) merged but insufficient; superseded by #1264 same evening.

**19:35 UTC sweep on #1264 (JSON API fetch): COMPLETE.** 346 jobs scanned (full corpus, vs 154 partial), complete=True, zero fetch failures, digest carries no partial note. Resolutions flow again for the first time since 2026-08-28: 41+ entries "stopped firing", including wildcard-override rows (Run Single-Day TPA Export for 2026-08-26/27/28 all linked to tpa_ipdsc_export). The six-day resolution freeze is closed.
