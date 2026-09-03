---
name: project_airflow_optimizer
description: AUDI-1194 airflow_optimizer/ — key-free Spark efficiency crawler, live as the spark_optimizer_daily DAG in airflow-ti; 2026-08-26 gained an executor-hour cost unit, Databricks dollar costing from system.billing, and Block Kit Slack delivery to #spark-optimizer; 2026-08-27 PR #1230 (Slack wire + cumulative savings log) and PR #1231 (fangorn shuffle partitions 2048) both merged, fangorn applied marker written to the prod ledger; full-corpus hackathon sweep (3,085 logs -> 67 pairs, 30,163+ exec-h) filed as AUDI-1241 under epic AUDI-1054; site_network_hourly + DDP dbt tests now ours (merged #1232, dbt#174 in review); 2026-08-28 PRs #1241-#1243 merged (#1244 open), BQ external table optimizer.optimization_ledger + Mode dashboard e81786de8403 live, first measured saving fangorn #1231 575.6 exec-h/day (~$58.4k/yr est); 2026-08-28 (later) #1245 merged — BQ profiler (bq_profile.py via JOBS_BY_USER), per-surface ledger (surface spark|bq|dbx), Databricks findings, billing surface_rates; pod profiler blocked on Astro metrics exporter; 2026-08-28 evening first live multi-surface sweep found the identity bug (sweep runs as spark-optimizer@ but billing+BQ grants target airflow-ti-prod@), fix PRs airflow-ti#1247 + mntn-devops#5160 open, pod-metrics relay filed as DEV-8821; 2026-08-29 both fix PRs MERGED, live BQ surface verified (optimizer_bq report + surface=bq ledger rows) and billing rate live-blended from 30d actual spend ($0.278/exec-h, no env fallback), Jira SA request ITS-6496 pending; 2026-08-31 hackathon refinement — 13 sprint tickets AUDI-1269..1281 filed into sprint 8649 grouped by change type (16 SP Malachi, 4 SP others), savings provenance = ledger applied stamps + daily PR-vs-ledger reconcile; 2026-08-31 evening — epic AUDI-1290 parents the 13, PR 1250 open (Databricks surface via SP OAuth REST; dormancy root cause = report() silently empty without DATABRICKS_WAREHOUSE on prod), PR 1252 open (gcs console links + OPTIMIZER_NAME_OVERRIDES), DEV-8821 relay LIVE; 2026-09-01 — PRs #1250/#1252/#1253 MERGED + LIVE on prod image deploy-2026-09-01T19-06-22 via retrigger PR #1254 (Astro superseded-build gap), dbx REST surface engaged (prod secret pairs prod_runner; blocked on system.lakeflow/system.query SELECT + warehouse CAN USE grants), env vars staged, DEV-8821 relay FULLY live end to end; 2026-09-01 (later) — '39 unprofiled' digest complaint decomposed (7 paused DAGs miscounted: ORM paused read forbidden on Astro tasks, REST fallback PR #1255 open; only 1 DAG cost-covered; chip reworded to 'N DAGs without cost data'), OPTIMIZER_NAME_OVERRIDES live on prod (14 source-verified entries, ETL Audience Intent excluded), pod surface PR #1257 open (pod_profile.py, core-hours/day, blocked on mntn-devops #5224 + OPTIMIZER_POD_PROJECT), Mode BQ cost table added via API; 2026-09-01 (evening) — #1255+#1256+#1257 COMBINED into PR #1258, MERGED + LIVE on deploy-2026-09-01T22-22-40, devops #5224 merged (monitoring.viewer synced), OPTIMIZER_POD_PROJECT=mntn-prj-prod-00 set; first optimizer_pod report published (sweep manual__22:36) but numbers wrong — Cloud Monitoring v3 timeSeries.list returns points NEWEST FIRST, rate went negative->0 — fix PR #1259 verified live (dag-processor 55% of cpu limit, worker-default 0.875 cores = 11% of 8, downsize candidate); downloader freeze root-caused (gsutil -m forked workers die quietly on the 0.25-CPU pod; ~2/192 logs landed every sweep since 08-28, 'Done' exit, resolution frozen 6 sweeps) — fix PR #1260 threads-only -m via GSUTIL_OPTS in fetch.py; 12-day full-history diagnosis written (outputs/audi_1194_diagnosis_2026_09_01.md: downloader freeze root of most regressions, dbx surface 0 rows ever, debugger/optimizer fleets near-disjoint); review queue #1259+#1260; 2026-09-02 — ledger unattributed BQ bucket verified EMPTY (every measured job labeled airflow-dag/airflow-task, no team-labeling campaign; 35 no-cost-data DAGs close via dbx grants / per-DAG event logging / genuinely-no-compute), PR 1260 retitled downloader + parse-rate canary, Alyson has the prod_runner grants paste (incl. warehouse Can-use), digest user-verified from screenshots (rank-row alignment reformat queued); 2026-09-02 (overnight) — digest rank rows fixed via rich_text ordered list (PR 1260 commit dd53939, user-confirmed preview), OPTIMIZER_NAME_OVERRIDES 22 entries live (was 14) with trailing-wildcard prefix keys in coverage.resolve (commit 3d87c6f), unlinked-apps question CLOSED (audience_intent / tpa_ipdsc_export / targeted_signal_crm launchers source-verified), PR 1260 = 3 commits retitled 'downloader loses the batch; parse canary; digest numbered list', new open question: flagged apps' spark-events logs vanish from GCS within hours; 2026-09-02 (afternoon/evening) — gsutil itself BANNED on Astro pods (every mode lands ~2/194, falsifies the threads-only fix), downloader rewritten on the GCS JSON API (PR 1263 insufficient, PR 1264 merged, deploy-2026-09-02T19-27-09), 19:35 UTC sweep complete=True 346 jobs, 41+ resolutions flowed, six-day freeze CLOSED; system.billing granted and dbx cost report live (top row Generate Graph & Metrics - PRODUCTION, 10,528 DBU, $1,579 list/7d); dbt 174 provenance stamp closed unmeasurable (job never entered the ledger); PRs merged today 1259 1260 1262 1263 1264; Jira comments 614410 + 614725; canary first live run = 09-03 17:00 UTC daily; 2026-09-02 (late night) — fangorn fix #1231 did NOT hold: first complete sweep shows 659.2 exec-h with stage-17/19 disk-spill CHRONIC again, the ~$900 cumulative savings was a blind-window artifact, Mode headline honestly $0 (after-rate 806.8 avg > before 687.7, GREATEST clamps to 0; per-finding wide-shuffle credit 28.5h stands); spill re-fix = top hackathon candidate; 2026-09-02 (hackathon execute wave) — AUDI-1274 landed on branch audi-1274-aqe-advisory-pivot (advisoryPartitionSizeInBytes=16m in both guid pivot builders, dry run diff-clean, Jira comment posted, PR pending gauntlet): the spec's spill mechanism was wrong, the 800-task floor is registered cores at plan time (parallelismFirst), not the 64 MiB advisory size; remedy unchanged; 2026-09-03 — AUDI-1273 landed (workspace 0d7a3d02, branch audi-1273-max-partition-bytes, PR pending gauntlet): maxPartitionBytes 64 MiB on ipdsc_ds_49 and 256 to 128 MiB on conv_log_derived_ip, ipdsc_ds_67 dropped because its DS4 input is 160 x 60 MiB single-row-group parquet the knob cannot split (keys disk_spill:3/:5 to wont_fix, broadcast fix = IMP-102), ledger hand-edit ops recipe recorded, conversion_log archive volume drop 08-20 flagged (IMP-103); 2026-09-03 AUDI-1275 landed, speculation safe by source for 11 of 13 straggler DAGs (FileOutputCommitter v2 under the commit coordinator, Iceberg), canary PR #1271 on site_network_hourly open with Ryan as reviewer, manifest pair owner-gated, the 08-27 ipdsc_ds_35 refutation contradicted on the record; 2026-09-03 AUDI-1278 landed: the unattributed BQ bucket (612 jobs, 1,110 slot-h per day) is fleet-SA jobs the ledger skips by design, 96.9% camperbid bos__spend Spark-BigQuery-connector reads (the 09-02 'outside the airflow-launched set' hypothesis refuted), airflow-ti PR #1278 open (labels at 8 python-client sites, 147 jobs/day), camperbid two-Spark-property hand-off drafted for pacing/performance-ml, measurement surface = the daily optimizer_bq report (exists from 08-28; baseline 606 jobs / 1,104.7 slot-h per day), decision 0007.
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [airflow optimizer, AUDI-1194, spark_optimizer_daily, airflow-ti 1212, spark-optimizer service account, serviceAccountTokenCreator, CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT, airflow session use is forbidden, spark optimization crawler, efficiency sweep, eventlog parser, 7-surface spark, optimization detectors, skew spill shuffle, fleet crawl backlog, daily optimizer cron, oncall_daily_optimizer, com.mntn.daily-spark-optimizer, phs event logs, phs.fetch_logs, dataproc-debug pam, audi-storage-object-view, 242x skew, dataproc databricks optimization, straggler detector, idle_reserved_executors, shuffle_fetch_wait, map-side concentration, site_network_hourly stage 9, optimization ledger, optimizer coverage gap, optimizer digest, sweep.py, ledger.py, coverage.py, digest.py, workload identity runner, EXPLAIN COST statement execution api, jobs get-run-output empty, IMP-029 rolling dirs, savings log, optimizer_savings, OPTIMIZER_USD_PER_EXEC_H, PR 1230, PR 1231, fangorn_score_monitor shuffle partitions 2048, speculation revert ipdsc_ds_35, ledger applied marker, hackathon optimizations, AUDI-1241, AUDI-1054 tech debt epic, full-corpus sweep 3085, PR 1232, dbt 174, ddp dbt tests ownership, site_network_hourly ours, PR 1241, PR 1242, PR 1243, PR 1244, adv_score event logs, SLACK_FALLBACK_CHANNEL, gsutil unauthenticated astro pods, gcs json api markers, rapid dag pause race, AIRFLOW_BEARER PATCH is_paused, mntn-prj-prod-00 optimizer dataset, optimization_ledger external table, pam breakglass-editor, pinned schema autodetect applied_date, use_legacy_sql false backticks, mode dashboard savings, e81786de8403, fixlog.py, optimizer fix log playbook, savings semantics calendar day, fangorn savings 575.6, AUDI-1249, priority rubric playbook, nonspark phase plan, PR 1245, bq_profile.py, JOBS_BY_USER, ledger surface field, surface spark bq dbx, surface_rates, OPTIMIZER_USD_PER_SLOT_H, dbx_heavy_job, dbx_failing_model, optimizer_bq report, astro universal metrics exporter, gcp managed prometheus, pod profiler blocked, savings by surface, ignoreUnknownValues, PR 1247, mntn-devops 5160, billing grant wrong identity, OPTIMIZER_BQ_SAS, hermetic sweep tests, DEV-8821 pod metrics relay, otel collector cloud run, ITS-6496 jira service account, blended billing rate, DCU-h, optimizer_bq_2026-08-29, airflow rest logs continuation_token, mode report runs refresh, hackathon sprint 8649, AUDI-1269 1281 sprint tickets, change type grouping one ticket per change, ledger applied provenance, daily pr ledger reconcile, bos__spend slot hours, intent_score_threshold_v4 slot hours, unattributed bq jobs, PR 1250, PR 1252, DATABRICKS_WAREHOUSE, DATABRICKS_GCP_CLIENT_ID, dbx dormancy silent empty report, ml_squad warehouse main workspace, prod_runner 397d710b, OPTIMIZER_NAME_OVERRIDES, astro-metrics-relay live, pod surface, dbt 174 baseline 306352 query-s, AUDI-1290 hackathon epic, AUDI-1302 wont do, PR 1253 merged, PR 1254 retrigger, deploy-2026-09-01T19-06-22, prod_runner grants blocker, dbx insufficient permissions, IMP-097 owner mapping, PR 1255, PR 1257, pod_profile.py, OPTIMIZER_POD_PROJECT, mntn-devops 5224, paused rest fallback, coverage invisible chip, 39 unprofiled decomposed, name overrides 14 entries live, hashed-email ds 22 29, core-hours per day, cpu-overprovisioned, memory-pressure, opt-bq mode section, 3ead7301daa8, rich_text_list ordered digest, name overrides 22 entries, wildcard prefix override keys, audience_intent prod launcher, tpa_export_spark_batch export_tpa, targeted_signal_crm, spark-events logs vanish hours, jira comment 614410, dd53939, 3d87c6f, PR 1263, PR 1264, gsutil banned astro pods, json api downloader rewrite, deploy-2026-09-02T19-27-09, freeze closed 41 resolutions, 346 jobs full corpus, system.billing granted, dbx cost report live, ddp vertical classification 10528 dbu, jira comment 614725, dbt 174 stamp unmeasurable, retry git deploy, fangorn fix not held, savings withdrawn zero, blind window resolved artifact, 659.2 exec-h, GREATEST clamp savings, resolved during visibility gap, AUDI-1274, guid pivot AQE floor, advisoryPartitionSizeInBytes 16m, disk_spill:33, disk_spill:34, spec mechanism corrected, audi-1274-aqe-advisory-pivot, AUDI-1273, audi-1273-max-partition-bytes, maxPartitionBytes, ipdsc_ds_49, ipdsc_ds_67, conv_log_derived_ip, disk_spill:1, disk_spill:3, disk_spill:5, ledger set wont_fix, STICKY states, owner_notified, ledger applied stamp, ledger hand edit window, sweep 09:00 UTC daily, ledger restored from gcs each sweep, single row group parquet, split probe, IMP-102 broadcast ds67, IMP-103 conversion_log volume drop, RESOLVE_SWEEPS 3, AUDI-1275, spark.speculation true, straggler gcs writers, canary site_network_hourly, PR 1271, OutputCommitCoordinator, FileOutputCommitter v2, manifest committer owner-gated, speculation contradiction, straggler detector fetch-wait blind spot, AUDI-1278, PR 1278, audi-1278-bq-job-labels, bq job labels, unattributed bucket settled, unattributed 612 jobs, bos__spend connector reads, spark-bigquery connector no labels, bigQueryJobLabel, spark.datasource.bigquery prefix, airflow-camperbid dag_utils google.py, run_dataproc_serverless properties, DataprocConfig asJson, camperbid handoff pacing performance-ml, airflow_job_labels, bq_job_labels.py, get_df configuration labels, optimizer_bq report from 2026-08-28, baseline 606 jobs 1104.7 slot-h, measurement option C daily report, mode resourceViewer option A, google_cloud_default get_df, IMP-105, decision 0007]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-09-03
---
**AUDI-1194 = the OPTIMIZER** (success-triggered efficiency sweep), **split from the AUDI-1191 debugger 2026-08-05** (both AUDI, type Task, Backlog). The two are separate workflows with distinct triggers/schedules/deliverables: the debugger fires only on a **failure**, the optimizer sweeps every DAG that **succeeds**. They can chain but are distinct. AUDI-1194: 5 story points, PMO rep Bryce Wagg, label q3_2026, folder `tickets/audi_1194_optimizer_efficiency_crawler/`, framing **LOCKED** (§0 in its summary). See [[project_airflow_debugger]] (the RCA half), [[reference_airflow_ti]], [[reference_oncall_runbook]].

**Question (framed):** Can a scheduled, key-free crawler read every succeeded Spark job across both engines (Dataproc event logs + Databricks EXPLAIN COST plans/metrics) and emit a ranked, actionable optimization backlog with no manual step? **Goal:** cut Spark compute cost + wall-clock fleet-wide, replacing the departed framework author's tribal knowledge (cost-reduction lever, Medium tier + bus-factor win). **Done-when:** a scheduled crawler scans every succeeded Spark job **including the ipdsc/tpa PHS logs** and emits a ranked cross-job backlog (worst-first, per-finding fix grouped CODE/INFRA/FAILURE) with no manual step.

**Package `airflow_optimizer/` (split from `airflow_debugger/`, commits a8ebad2d + b153266d).** Modules: `eventlog` (the 7-surface Spark event-log parser), `optimizations` (detectors), `optimize` (single-job BLUF), `crawl` (fleet backlog) + `tests`, `fixtures`, own `README`. **Coupling (updated 2026-08-07):** `eventlog.py` lives in `airflow_optimizer/`; since IMP-032 the debugger ALSO lazy-imports `airflow_optimizer.optimize` from `airflow_debugger/perf_profile.py` — escalation-only (ttl_exceeded/driver_oom/fetch_failed + event log), degrades to a note if the optimizer is absent. Entrypoints: `python3 -m airflow_optimizer.{optimize,crawl}` (debugger side = `python3 -m airflow_debugger.{orchestrate,report}`). All 6 tests pass, ruff-clean, git-tracked as renames.

**The engine (all built + validated on real event logs):**
- `eventlog.py::parse_eventlog()` → structured `SparkRun` across all **7 Spark surfaces** (jobs/stages/tasks/executors/environment/storage/SQL per-node), recovering per-operator metrics by joining `sparkPlanInfo` accumulatorId↔Accumulables/DriverAccumUpdates. Handles `.zstd` (dir/file). Surface 7 (storage/cache, `SparkListenerBlockUpdated`) needs `spark.eventLog.logBlockUpdates.enabled` — **UNCAPTURABLE on Dataproc Serverless** (rejects that prop), valid on managed clusters only.
- `optimizations.py::analyze_run()` → 3 rec types — **code** (skew/spill/shuffle-partitions), **infra** (gc_pressure, spot_preemption_cost, cache_ineffective), **failure** (fetch instability) — each with real numbers + fix, impact-ranked.
- `optimize.py` = one event log in → engineer-ready single-job BLUF backlog (plan-text `analyze_plan` + metric `analyze_run`). `crawl.py` = optimize every job in a dir/glob, rank a cross-job backlog worst-first.

**Acquisition state per engine:**
- **Batch-operator Dataproc fleet (88 models, no PHS):** event logs land in `gs://mntn-data-archive-{env}/spark-events` (PR #1169 turned this on fleet-wide, merged prod 2026-08-04) — accessible, download with `gsutil -o "GSUtil:check_hashes=never" cp` (`gcloud storage cp` corrupts `.zstd`).
- **ipdsc/tpa (PHS-attached):** logs are per-batch at `gs://{temp_bucket}/<dataproc-batch-uuid>/spark-job-history/app-<id>.zstd` — SPARSE + scattered across thousands of unsorted per-uuid temp dirs, most empty. A flat prefix scan is infeasible → the crawler must **ENUMERATE ipdsc/tpa batches via `gcloud dataproc batches list/describe` (→ uuid), then read that uuid's `spark-job-history`**. Validated end-to-end 2026-08-05 (parsed `Populate ipdsc_ds_67.DS67`, shuffle.partitions=1000). This reshapes the earlier "point the crawler at the PHS prefix = 1-line change" note, which was WRONG.
- **Databricks:** `EXPLAIN COST` plan + Spark job metrics via `jobs get-run-output` (live acquisition path still to validate).

**PHS event-log access (2026-08-05):** `malachi@mountain.com` has **NO standing `storage.objects.list`** on `gs://dataproc-temp-us-central1-995798185124-svhwvc6j`. Interim read = the **`dataproc-debug` PAM bundle** (Compute Viewer + Dataproc Viewer + Storage Object Viewer; self-service ~1h, 18h max with L1 devops-squad approval; access propagates ~30s after the grant activates). The 1h PAM grant can't run the weekly cron → **standing grant needed (Slack/mountain-devops → Cristina): `roles/dataproc.viewer` on `mntn-prj-prod-00` (enumerate batches) + `roles/storage.objectViewer` on `dataproc-temp-us-central1-995798185124-svhwvc6j` (read logs)**.

**DAILY cron LIVE (superseded the weekly one 2026-08-20):** `.claude/scripts/oncall_daily_optimizer.sh` + launchd `com.mntn.daily-spark-optimizer` (11:00 PT **daily**), `CAP` default **200** (`OPTIMIZER_LOG_CAP`), `OPTIMIZER_PHS=0` skips the PHS half. Writes `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_<date>.md`; idles with no git noise when a source is unreachable.

**Why daily, the number that settled it:** the fleet emits **~160 event logs/day** (157-164/day over 2026-08-14..08-19; 7.85 MB average object). The old weekly `cap=40` sweep therefore read ~6 hours of ONE day out of 168 — **~4% of the fleet**, a coverage hole, not a freshness preference. A full-day sweep costs ~1.3 GB download, ~3 min CPU, ~50 MB RSS. First daily run (2026-08-20): **214 jobs / 278 findings / 197 high-impact** vs 37/59/42 weekly.

**Proof it works — real fleet finding (IMP-024):** crawl of 13 real prod jobs → **`Update Vertical Categorization` chronic Stage-0 skew up to 242x** (every run 10-242x) = #1 fleet target; `Prepare HTML Content` 18.4x; 6 jobs clean. Labeled by `spark.app.name` (event log self-identifies).

**Hardening pass (2026-08-07 PM, commit 629660a1):** 41 corpus-confirmed defects fixed (5 finders on a 48-log/611MB real corpus → execution-required skeptics → fix wave, AUDI-1191 pattern). Worst: **multi-frame zstd — with the `zstandard` package installed only frame 1 decompressed, every real log parsed EMPTY and reported "clean"** (masked here by the CLI fallback); parser now streams (98MB log: 18GB→49MB RSS, corrupt→error). Phantom skew killed (zero-median guard + 60s floor `SKEW_MIN_TASK_MS`); stage retries attempt-keyed; failed tasks excluded from skew; chimera-dir guard; cron rebuilds rolling dirs (`--selftest`). **New detectors: `straggler`, `shuffle_fetch_wait` (6 prod jobs at 53-72% fetch-wait), zero-task idle fleets.** Plan detectors remain Databricks-format-only (dead on Dataproc OSS text) → IMP-033. **Systemic fleet finding: hourly `aug_log_ip*`/`site_network_hourly` run at 2-8% executor utilization, 20-61 idle exec-h/run.** Sweep cost measured: 48 logs = 58s + 49MB RSS + ~600MB download → **daily cadence is cheap** (switch after next green live cron). PHS: `phs.py` enumerates PHS-attached SUCCEEDED batches key-free (22 live); standing bucket read = draft PR mntn-devops#4724 (bucket-scoped objectViewer to `audience-intelligence@`, mirrors the mntn-marketo pattern; `dataproc.viewer` already standing via DEV-8182).

**Validation run 1 (2026-08-07) — first external ask (Ryan Kleck, `aud-int-int-map` = `intent_score_map`):** ticket now **in_progress**. Found + adversarially verified: a 67-min IO-stalled straggler (13.4x duration on 1.0x data, 5% CPU, speculation off) pinned 240 executors at 32% utilization (~$175 of the ~$260 list run idle); 88.8 TiB spill/run. En route: **IMP-029 FIXED** (v2 rolling dirs `eventlog_v2_batch-<uuid>/events_N_*` parsed in numeric order — the old cand[0] bug would have missed the entire tail), **2 new detectors** (`straggler` = duration-skew × uniform-data cross-check; `idle_reserved_executors` = exec-hours held vs slot-busy). **Verify-pass lesson: 4-agent adversarial workflow refuted my shuffleTracking.timeout rec against Spark source BEFORE it reached the job owner — keep verify-before-send mandatory for owner-facing tuning recs** (mechanism detail in [[reference_dataproc_eventlog_profiling]]). Recs delivered to Ryan: speculation=true + shuffle.partitions 4915→~30k (set in BOTH decorator and builder line ~89 — builder wins).

**2026-08-20 session — cadence CLOSED, PHS proven, Databricks route CORRECTED.**

**PHS half works end-to-end; only the standing grant is missing.** Proven under a 1h `audi-storage-object-view` PAM grant: **22/22** PHS-attached SUCCEEDED batches enumerated, fetched (568 MB) and parsed, producing 21 findings on jobs the archive sweep had never seen (`materialize_mntn_select_*` Stage 6 at 40-78% fetch wait, `segment-updates-to-parquet-*` Stage 2 at 36-67%). PAM detail: on `mntn-prj-prod-00` both `audi-storage-object-view` (objectViewer only, the least-privilege choice) and `dataproc-debug` (compute.viewer + dataproc.viewer + storage.objectViewer) are **64800s max** and need 1 approval from `devops-squad@` / `gcp-audi-admins@` / `pam-slack-bot` — it auto-approved in minutes. `gcloud pam entitlements search --caller-access-type=grant-requester` (NOT `--caller-access`) lists what you can request.

**Two `phs.fetch_logs` defects fixed (tests fail against the old code):** (1) `gsutil cp` had **no `-r`**, so any batch that wrote an `eventlog_v2_*` rolling dir downloaded empty and was silently dropped; (2) no `dest_for()` equivalent — a top-level `appstatus_*` marker in a uuid dir makes `crawl._event_logs` read the WHOLE dir as one merged log. Top-level markers are now stripped; the marker *inside* a rolling dir is load-bearing and kept.

**`site_network_hourly` Stage 9 — the verify pass refuted TWO hypotheses before anything reached the owner.** (a) The detector's OWN stock fix ("raise `spark.sql.shuffle.partitions`") is wrong here: in the same app, stages 29/35 fetch **23.4M blocks at 1,607 B with 1s of fetch wait** while stage 9 stalls on 4.2M blocks of the same size — block count/size is not the cause, and raising partitions multiplies it. Fix text corrected in `optimizations.py`. (b) The source-read guess was wrong: the builder's `shuffle.partitions=5000` with a `// 33` coalesce predicts ~151 reducers; the log shows 74-622. **What the evidence supports is map-side output spread** — shuffle blocks are served by the executor that WROTE them, so the reduce stage is rate-limited by how many map-side executors hold the output. On all four logs profiled the feeding map stage starts with **exactly 50 executors** (`initialExecutors=50`) and lands 90% of its output on 48-105 of them (hottest up to 24.6%); the later map stages start with 306-500, spread across ~480 (hottest 0.3%), and their reducers wait ~0%. **The one fact that does not fit:** stage 15 reads the SAME map output at comparable block count/size and waits ~0% (likely cold-vs-warm first read, unprovable from the event log) — so the ask to Ryan is a one-hour `initialExecutors` experiment, not a config prescription. Tooling: `artifacts/audi_1194_shuffle_concentration.py`. Backlog IMP-047.

**Cost is self-serve and metered:** `gcloud dataproc batches describe --format='value(runtimeInfo.approximateUsage.milliDcuSeconds)'` on `mntn-prj-prod-00`. `site_network_hourly` = **8,663 DCU-h over 17 runs on 2026-08-20** (mean 510, range 164-1,547) vs 99-208/run for `aug_log_ip_hourly` — which is why it outranked the aug family. DCU attributable to the stall is NOT established; the CUD caveat still applies ([[feedback_dataproc_cost_awareness]]).

**Ranking blind spot (IMP-046):** `JobReport.score` is per-LOG `(high, medium, total)`, and `shuffle_fetch_wait` is `high` only at ratio ≥0.50 — so `aug_log_ip_vertical_id_hourly` firing at 31-45% on 11 of 11 runs never reaches the top of the backlog. Chronic-across-every-run is invisible to per-log severity.

**`spark-events` has NO lifecycle rule (IMP-048).** `gs://mntn-data-archive-prod` has Delete rules for 14 prefixes, none for `spark-events/`; the age-30 TTL recorded as approved 2026-08-04 was never applied. 17.1 GiB / 2,237 `.zstd` today, growing ~1.3 GiB/day. Needs `storage.buckets.update`.

See [[reference_databricks]] for the corrected Databricks acquisition route, [[reference_mntn_devops_permissions]] for why #4724 sat unreviewed, [[feedback_verify_before_volunteering]] for the verify-before-send rule this session exercised again.

**2026-08-20 (later) — the sweep became a reporting pipeline, not just a crawl.** `sweep.py` is the post-download entry point (crawl → coverage → ledger → digest); the cron's `--selftest` asserts through it. 40 tests, ruff clean.
- **`ledger.py`** appends every finding to `outputs/optimization_ledger.jsonl` keyed **job + detector + stage**, replayed to derive `new` → `recurring` → `chronic` (3+ sweeps) → `resolved` (3 quiet sweeps); `owner_notified` / `wont_fix` are set by hand and **sticky**. Dedup matters: 26 findings across 25 job-logs collapse to **4 keys**.
- **Two identity traps, both silent.** (1) The key first swallowed any digit in the title, so task counts and byte totals minted a new key every sweep — now the stage number only. (2) `spark.app.name` carries per-run stamps, but **`_<n>` is ambiguous**: a run index in `materialize_mntn_select_16`, a data-source id in `ipdsc_ds_67`. Blind stripping merges the whole ipdsc family. Dates/timestamps/`[n]` always strip; `_<n>` only when the stripped form is a DAG **coverage actually saw** — which is why coverage runs BEFORE the ledger.
- **`coverage.py`** enumerates unpaused DAGs via `.claude/scripts/airflow_api.py` (astro bearer, shelled out so auth lives in one place). Live 2026-08-20: **62 active DAGs, 24 with a Spark task, 38 structurally invisible** (BQ operators, sensors, Python), plus 4 `create_ip_verticals` Databricks tasks. All named in `outputs/optimizer_coverage_<date>.md`.
- **`digest.py`** leads with the delta, says "No change since the last sweep" when true, and links a DAG **only when coverage saw it** (Spark app names are not always dag_ids). Delivery deliberately unbuilt — route is the `compass-slack` automation in mntn-devops, never a local credential.
- **Deliverables:** workbook rebuilt to 9 tabs (adds Ex — site_network_hourly, Ledger + digest, O5/O6 steps, 14 detectors); exhaustive walkthrough page `https://claude.ai/code/artifact/878ac222-4ed6-4376-aea5-cd1772308cca` (real DAG, raw `SparkListenerTaskEnd` annotated, detector verbatim, both refutations, demo commands).
- **Runner scoped, not built** (`artifacts/audi_1194_runner_and_identities.md`, its own ticket when approved): the blocker is **expiry, not identity** — astro bearer ~1h, Databricks OAuth refresh needs an interactive renewal, gcloud is personal SSO. Recommendation: **GH Actions + GCP Workload Identity Federation** (no SA key exists at all), Astro deployment token + Databricks service-principal secret in Secret Manager (no keyless path for those two), GitHub App scoped **`contents:read` + `metadata:read`** so "never opens a PR" is structural. Artifacts to GCS, so the GitHub identity never needs write. Two of four GCP grants already exist because DEV-8182 and #4724 were written against `group:audience-intelligence@`, not a person.

**Shipped-optimization register (IMP-054, 2026-08-20).** The finding ledger gained an **`applied`** state: `python3 -m airflow_optimizer.ledger applied <dag> <key> <pr> <date>` records the PR and applied date and carries both onto every later sweep, so an outcome stays attributable. **`applied` is deliberately NOT sticky** — a merged fix is not a verified fix, so the detectors decide what happened next: the finding going quiet becomes `resolved` (noted "cleared by <PR>"), the finding still firing after the grace window becomes **`fix_not_working`**. `ledger shipped` renders the register (applied date, DAG, finding, PR, outcome, DCU/h before vs after). **Cost is read per-DAG, not per-finding** — a fix that works stops the finding firing, so a per-finding "after" would always be empty. Validated both directions: a working fix reads 100.0 -> 40.0 DCU/h `resolved`, a non-working one 80.0 -> 79.0 `fix_not_working`. Answers "what has this tool actually saved", which the backlog alone never could.

---

## SHIPPED TO PROD 2026-08-21 — it is an airflow-ti DAG now, not a laptop cron

**`spark_optimizer_daily`** in `SteelHouse/airflow-ti` (`dags/spark_optimizer_daily.py`, package
vendored at `include/spark_optimizer/`), 09:00 UTC daily, `retries=1`, `execution_timeout=1h`,
`dagrun_timeout=2h`. **First prod run 2026-08-21: 215 jobs, 290 findings, 196 high**, all four
artifacts published to `gs://mntn-data-archive-prod/optimizer/`. The laptop launchd job and
`oncall_daily_optimizer.sh` still work and are still the local entrypoint.

**Why it moved off the runner design entirely:** Cristina Szumilo asked why a job owned by AUDI
was living in `mntn-devops` and being attributed to the platform team. She was right, and the
move was cheaper than the design it replaced: it deleted the container image, the GAR push, the
ArgoCD manifests, the `mntn-helm` chart change, **and the Astro API token** (a DAG can enumerate
DAGs locally). **Before designing a store for a credential, check whether moving the workload
removes the need for it.** Prior design iterations, all superseded, are in
`tickets/audi_1194_.../artifacts/audi_1194_runner_and_identities.md` §3-§13.

**Identity, verified in prod:** GSA `spark-optimizer@mntn-prj-prod-00` (mntn-devops#4971,
merged), impersonated from the deployment's own ADC `airflow-ti-prod@` via
`roles/iam.serviceAccountTokenCreator` and `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT`. Grants:
`dataproc.viewer` on the project, `storage.objectViewer` on `mntn-data-archive-prod` and the PHS
temp bucket, `storage.objectUser` conditioned to the `optimizer/` prefix. Mechanism detail:
[[reference_gcs_iam_creator_vs_user]].

### Known broken after run 1

**`coverage.collect_local` is dead on Airflow 3.** It reads the paused set from the metadata DB,
and a task gets `airflow session use is forbidden in this context`. The DAG-bundle parse is fine;
the ORM is not reachable from task code. The sweep degrades correctly (`DAG coverage unknown`,
and the ledger declines to write rather than rekey), so this is a missing feature, not a
corruption. **Fix: go back through the REST API** with a deployment token, or find a Task-SDK
call that exposes paused state.

**The download is 200 serial `gsutil` invocations.** `fetch.download()` shells out once per
object, each paying Python interpreter startup. On the Astro default pod (**0.25 CPU, 0.5 Gi** —
the DAG sets no `executor_config`) run 1 took **~19 minutes**, and process spawn dominated, not
the parse. **Fix: one `gsutil -m cp -I` fed the whole object list.** Raising CPU is the smaller
lever.

**The digest cites the container path.** `Full backlog: /tmp/spark_events_*/out/...` instead of
the GCS URL, because `run()` passes the local path through. Cosmetic but useless to a reader.

### Not yet built
Slack delivery (the digest already renders Slack markup; `compass-slack` in mntn-devops is the
transport), the Databricks `EXPLAIN COST` bridge, and `USE SCHEMA` on `system.lakeflow` — **reopened as an internal ask 2026-08-24.** The 2026-08-21 note here ("unobtainable, Databricks-side only") misread the enable error; David Qiu (Databricks) confirmed lakeflow is enabled automatically and that error is expected. The real gate is **metastore admin**, which MNTN has never assigned, and it blocks every `system.*` schema. Workspace admin is not enough and neither is account admin on its own (`grants update` → `User is not an account admin`); Ryan Kleck's assumption that workspace admin would do it is wrong. See [[reference_databricks]].

## 2026-08-26 — the sweep produces a ranked, linked, readable digest

Four merges since the prod ship (airflow-ti #1216, #1218, #1221, plus #1213's CI fix), all
gauntleted. What each one settled:

**Coverage no longer blinds the sweep (#1216).** Airflow 3 forbids ORM access from task code, so the
paused-DAG query raises `airflow session use is forbidden in this context`. That raise used to take the
whole DAG enumeration with it, `known` came back empty, and `sweep` correctly declined to write ledger
rows it could not key — so **change tracking sat frozen at 130 rows / 2026-08-21 for four days**. Only
the paused exclusion needs the DB; the bundle parses off disk. Split, and a lost paused set now costs
only the paused set. Verified: the 08-25 manual run wrote 136 new rows and enumerated 72 DAGs.

**A Spark app name is an Airflow TASK id, never a dag_id (#1218).** `aug_log_ip_hourly` is the task
`feature_group_1_source.aug_log_ip_hourly` inside DAG `feature_store_hourly`; `fangorn_score_monitor`
is a task in `audience_intent`. Matching job names against dag_ids resolved **0 of 57** on the 08-25
sweep, which is why every digest line since launch carried a bare name instead of a link. Coverage now
builds a task-name → dag_id index (DAG ids indexed too, ambiguous names dropped rather than guessed).
The coverage page also lists every unresolved job with its reason, so the gap is stated, not silent.
Three genuinely-unfixable classes remain: Spark set no app name (`app-2026...`, `segment-updates-...-[11]`),
the name is shared by two DAGs, or no DAG defines a task by that name.

**Findings rank by cost, not count (#1221).** The parser had been computing each run's executor-hours
and discarding it. `crawl` carries it as `exec_h`, the ledger records it, and the digest is now four
fixed blocks per DAG — What / Where / Why / How — capped at three DAGs, ranked on impact then hours.
`dcu_h` stays a separate, still-unpopulated field: measured DCU and executor-hours are different units
and conflating them would be a lie in a published artifact. Two costing bugs found adversarially:
a killed or still-rolling app writes no `ApplicationEnd` so the fleet's biggest runaway costed **0.0
and sorted last** (now measured to the log's last event), and an executor whose `added_ts` was 0 was
dropped by a falsy check.

**Databricks enumeration is live.** `airflow_optimizer/databricks.py` reads
`system.lakeflow.job_run_timeline` (1,531 SUBMIT_RUNs / 7 days). Ranked by hours:
`prod-ml-ddp_vertical_classification_api` 251 runs / 85.0h / 24 failures,
`prod-ml-verticals_pre_cache` 381 / 65.2h, `prod-tpa-guid_geos_raw` 188 / 24.5h.
`prod-mntn_matched-mntn_matched_taxonomy_vector` fails 16 of 25 runs. Gotchas in
[[reference_databricks]]. **The remaining gap is model → SQL**: enumeration says a model ran, not what
it ran, so `EXPLAIN COST` still needs the model's source.

**Still not built:** Slack delivery (blocked on the app — Robin Fox reviews scopes, see
[[reference_pi5_server]]), GitHub line permalinks in the *Where* block, batching the 200-invocation
`gsutil` download, and the `EXPLAIN COST` bridge.

**Two prod defects found and NOT yet fixed:** the DAG cannot be manually triggered without an explicit
`logical_date` (Airflow 3 gives such a run no data interval, so the task raises `KeyError('ds')` in
seconds — the 9am schedule is unaffected), and the failure callback's Slack post returns
`channel_not_found`, so a failed sweep notifies nobody.

## 2026-08-26 (later) — the full-corpus validation, and four defects only a full crawl could show

Malachi's ask was "prove there are no gaps before we send this to anyone". Every `.zstd` in
`gs://mntn-data-archive-prod/spark-events` written inside the last 30 days was downloaded and
parsed: **3,022 objects / 25 GB → 2,954 event logs** (92 objects belong to 24 `eventlog_v2_*`
rolling dirs), **all 2,954 parsed**, **80 distinct Spark jobs**, **3,620 findings**,
**85,655 executor-hours**. Scripts: `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_validation_*.py`.
Deliverable: `My Drive/Tickets/AUDI-1194 Airflow Spark Optimization Crawler/AUDI-1194 Spark Optimizer Validation.xlsx`.

**The archive holds 23 days, not 30.** Span is 2026-08-04..2026-08-26, median 133 logs/day.
Archiving to that prefix began 2026-08-04; the bucket also holds an unrelated 2025-10-07..11-12
block. `gsutil lifecycle get` still shows **no rule matching `spark-events/`** (IMP-048 open), so
nothing expires it and the window grows a day per day. Do not promise a 30-day lookback.

**A Spark app name puts the job BEFORE the dot; an Airflow task id puts it AFTER.** This corrects
the #1218 note above. `Populate site_network_hourly.SiteNetworkHourly` → the task is
`site_network_hourly`; `feature_group_1_source.aug_log_ip_hourly` → the task is
`aug_log_ip_hourly`. `normalise_job` took the segment after the dot for both, so it returned the
CLASS name. One segment cannot serve both sides. `job_keys` now offers first segment, last
segment, full name, and a digit-infix form (`ipdsc_14_monitor` → `ipdsc_monitor`, one datasource's
run of the `ipdsc_monitor` DAG); `Coverage.resolve` takes the first candidate naming exactly one
DAG. **Measured on the real fleet: the shipped digest linked 3 of 80 jobs, the fix links 77.**
The coverage report's own `profiled this sweep` count went **2 → 13** — that was the visible
symptom, and it read as "the fleet barely ran". `normalise_job`, `Coverage.task_owner` and
`digest._resolver` all lost their last caller and were deleted.
**Correction on the record:** the "0 of 62" figure in commit `b80d3047` describes the coverage
INDEX, not the digest; a gauntlet refuter caught the overstatement. 3 → 77 of 80 is the evidenced
number. Two names stay unresolvable: `guid_log_ip_advertiser_id` (its task is
`feature_group_1_source.guid_log_ip_advertiser_id_rollup`, so the app drops a `_rollup` the task
carries) and `ipdsc_third_party_audience_builder` (no task in the bundle defines it).

**175 of 200 recent Dataproc batches write their event log where the sweep never looked.** Of 200
listed batches: 13 set `spark.eventLog.dir = gs://mntn-data-archive-prod/spark-events`, 10 set an
explicit temp path, and **175 set nothing at all** — and Dataproc still writes their log to
`gs://<temp-bucket>/<uuid>/spark-job-history/`, history server attached or not. `phs_succeeded`
filtered on `sparkHistoryServerConfig`, which Dataproc returns as an **empty dict** (falsy) for all
but 10, so it kept 10 of 200. Sampled 12 of the 175 at random: **12 of 12 had a readable log
there.** New rule keeps any SUCCEEDED batch not writing to the archive → **185 of 200**;
`MAX_BATCHES` 60 → 150 (~585 MiB at the measured 3.9 MiB/batch). The temp bucket
`dataproc-temp-us-central1-995798185124-svhwvc6j` **is readable as of 2026-08-26** (403 on
2026-08-20; mntn-devops#4724 is out of draft with DevOps requested). `spark_optimizer_daily.py`
already calls `phs.fetch_logs`, so no new wiring was needed — the selector was the whole gap.

**16 of 30 Spark DAGs ran successfully in the window and produced no readable log.** Not "they did
not run": task-instance states show `materialize_mntn_select.materialize` 24 successes,
`fpa_site_visit_batch_serverless.dsid23_guid_log_processing` 24, `hashed_email_guid_log_signals.populate_hem_data_ds_23`
24. Cause confirmed by `batches describe`. Proven end-to-end: fetched 14 temp-bucket batches,
crawled 14/14 clean, and `materialize_mntn_select_16` — a DAG the sweep had never seen — resolved
with a finding at 31.7 executor-hours.

**A no-op run is a finding, and `ApplicationEnd` is the WRONG discriminator.** `crawl` skipped any
log with no jobs and no stages as a truncated download. 39 such logs in the corpus were all real
named apps that started and ended without submitting a job, costing **546 executor-hours** across
15 discarded high-impact findings; the worst held **100 executors for 64.4 executor-hours with zero
tasks run** — a finding `idle_reserved_executors` already knew how to raise and never got to.
The first fix keyed on `ApplicationEnd`; a gauntlet refuter proved that throws away the killed apps
the guard exists to catch (a TTL kill, cancel, or driver OOM writes no `ApplicationEnd` either, so
a 64-hour run read as a failed download at 0.0 cost, contradicting `executor_hours`, which costs
that exact case from `last_event_ts`). **Holding an executor is the discriminator**: skip only when
there are no jobs, no stages AND no executors.

**Detector status, measured on a random 300-log sample — 10 of 14 work.** Firing: `shuffle_fetch_wait`
1,496 · `disk_spill` 884 · `idle_reserved_executors` 549 · `shuffle_partition_sizing` 324 ·
`straggler` 242 · `skew` 125. Correctly quiet, with the input confirmed present: `gc_pressure`
(GC time recorded on 295/300, max share **4.3%** vs a 10% threshold), `spot_preemption_cost`
(removal reasons recorded, **no** `preempt`/`spot` string — serverless, no spot),
`shuffle_fetch_instability` (**0** FetchFailed tasks). Unproven either way: `cache_ineffective`
(`cached_rdd_bytes == 0` fleet-wide, nothing caches, so it has never had a case to judge).

**The four plan detectors are dead on Spark event logs, now measured, and the unblock is named.**
295 of 300 sampled runs DO carry SQL plan text — **4,734,637 chars** — and `parse_plan_text`
extracts **0 leaf scan nodes** from all of it. OSS Spark writes `Relation [cols...] parquet`; the
detectors need Databricks's `Scan parquet <table> ... Statistics(sizeInBytes=...)`. This widens
IMP-033 from "the scan/stats regexes are Databricks-only" to "all five plan detectors are dead on
OSS text". **`system.query.history` is the missing input** — it carries the statement text that
`system.lakeflow.job_run_timeline` does not, which is the model→SQL gap noted above. It exists and
is listed by `system.information_schema.schemata`, and returns
`INSUFFICIENT_PERMISSIONS: User does not have USE SCHEMA on Schema 'system.query'`. Same account-admin
ladder Alyson Lefkowitz ran for `system.lakeflow`; draft at
`artifacts/audi_1194_slack_alyson_query_schema.md`. See [[reference_databricks]].

**`site_network_hourly` is the fleet's #1 target, and now it is not a 4-log claim.** 302 runs in the
window, **21,200 executor-hours** (top-10 jobs hold 79% of the fleet total), per-run median 51.3h /
max 371.2h. A fetch-wait finding fires on **254 of 302 runs**, **252 of them on stage 9**, min 30% /
median 56% / max 90% of task time; `idle_reserved_executors` fires on 236. The Ryan Kleck draft now
rests on this rather than on the four profiled logs.


## 2026-08-26 (evening) — merged, verified in prod, and the Databricks half fully granted

**#1222 is live and the fix is real.** The 2026-08-26 prod sweep reports `profiled this sweep: 12`
(was 2) and names only **7 of 217** scanned jobs as untied to a DAG. The digest's *Where* block now
carries a working Astro link. **#1223 open** for the manual-trigger `KeyError('ds')`; gauntlet PASS
in 2 rounds, the first clean verdict of the day.

**Every Databricks grant is done**, verified by reading rows: `system.lakeflow`, `system.query`,
`system.billing` (168,853 rows), `system.compute` (114,899), `system.access` (612,075),
`system.storage`, for both `malachi@mountain.com` and the `spark_optimizer` SP; plus `SELECT` +
`USE CATALOG` on `CATALOG prod` for the SP, which `EXPLAIN COST` needs because it plans against
the real tables. Ladder, tiers and the stale-CLI trap: [[reference_databricks_system_schema_grants]].

**Billing can now be joined to the optimizer's own rankings.** `system.billing.usage` carries
`usage_quantity`/`usage_unit` and a `usage_metadata` struct with `job_id`, `job_run_id`,
`run_name`, `job_name`, `warehouse_id`, `cluster_id`. `job_run_id` joins
`system.lakeflow.job_run_timeline.run_id` (per-dbt-submission DBUs); `warehouse_id` joins the
warehouse a `system.query.history` row ran on (per-statement DBUs); `sku_name` +
`usage_start_time` join `system.billing.list_prices` for dollars. **Dataproc is NOT in
`system.billing`** - the Spark half stays on `milliDcuSeconds`.

**The EXPLAIN COST bridge works and has a structural limit worth knowing before relying on it.**
`databricks.heavy_queries` reads `system.query.history` (statement text is populated on every row;
2,820 SELECTs / 132.6 query-hours in 2 days) and `analyze_queries` plans each. But **0 of 20
heaviest statements are plannable**: all reference `prod.ml.ddp_vertical_classification_api`, which
is **dropped and recreated ~21x/day** (149 DROPs in 7 days). EXPLAIN COST replays historical SQL,
so any query touching a transient table can never be planned after the fact. Run it on recent
queries against durable tables, or capture the plan at run time.

**A false positive I shipped and had to retract.** `EXPLAIN COST` SUCCEEDS as a statement and
returns the planner's error as its RESULT TEXT, so catching an exception is not enough. That text
parses: an unresolved plan carries no statistics, so `missing_statistics` fired, and
`repeated_scan` fired on the two unresolved references. I reported both as real findings on
`ddp_vertical_classification_api` before checking whether the table existed. `explain_cost` now
rejects any plan containing a planner-error marker or `unresolvedalias`. **The lesson is the
order:** confirm the object exists before believing a finding about it.

**What survived that retraction, because it came from query history rather than a plan:** four dbt
TESTS on `ddp_vertical_classification_api` consumed **131 of the top-12 nodes' 135 warehouse-hours
over 2 days**, 57 runs each, **1,178 TiB read**; the models themselves are ~1 hour. Every test
filters `WHERE load_ts = (SELECT max(load_ts) FROM <same table>)`.

**Open disagreement to chase:** the prod digest renders `fangorn_score_monitor` and `ipdsc_ds_35`
unlinked while the coverage report does NOT list them as unresolved. Same `Coverage` object feeds
both, so suspect the `_owner_index` cache or the difference between what `unresolved()` is passed
and what `resolve()` is passed. Ordered plan: `artifacts/audi_1194_next_steps.md`.

## 2026-08-26 — cost has a unit, Databricks has a price, Slack has the digest

- **#1222 and #1223 merged.** Job-to-DAG resolution live in prod (`profiled this sweep` 2 -> 12,
  7 of 217 jobs untied); manual trigger no longer needs an explicit `logical_date`.
- **Cost was 29% short.** `ExecutorAdded` covered 359 of the 497 executors that ran tasks in one
  prod log; the rest were billed at zero. Seeding `added_ts` from the first task's launch moves
  that run 276.1 -> 356.6 executor-hours. [[reference_spark_eventlog_cost_units]]
- **IMP-084 closed.** `cost_h` on every finding; `high` needs 10 executor-hours OR 10% of the run.
  `_cores` returns 0 when unknown rather than guessing 1, and an underivable cost never demotes.
- **Peak concurrency settled** and an earlier conclusion reversed: it IS measurable, with a 100ms
  handoff tolerance. `site_network_hourly` saturates 1,988 slots at peak and averages 2.2%, so
  `maxExecutors` is not the lever; the tail is.
- **Databricks costed.** `job_costs` / `query_costs` price jobs and dbt nodes from
  `system.billing`. [[reference_databricks_billing_cost]]
- **Biggest single finding:** four `ddp_vertical_classification_api` dbt tests each scan 5.13 TB
  / 2.15M files ~20x/day to return one row, and are 98.6% of a warehouse costing $850/week.
- **Slack delivery live.** `notify.py` + `digest.blocks()` post Block Kit to `#spark-optimizer`
  (`C0BSTH6E84T`), reusing the `airflow-debugger` app. [[reference_slack_debugger_app]]
- **Open:** prod's `collect_local` leaves `fangorn_score_monitor` and `ipdsc_ds_35` unlinked while
  the REST path resolves both (`audience_intent`, `tpa_ipdsc_export`). Astro needs
  `OPTIMIZER_SLACK_CHANNEL` set before the DAG posts.
- **All work consolidated into airflow-ti #1229** on 2026-08-26 at the user's ask: the two
  AUDI-1194 branches plus AUDI-1191's `audi-1191/two-channels` (another session's), merged into
  `audi-1194-1191-combined`. Zero file overlap, no rebase, 336 tests. #1225/#1227/#1228 closed
  pointing at it. The debugger files were left untouched, including three 2-line comment blocks
  our `lint_comments.py` would fail but which airflow-ti does not lint.
- **#1229 MERGED same evening** (squash `03706e8`); the three comment blocks were tightened to
  one line before the squash (`f48fea9`). The delivery slice shipped with one completed
  adversarial pass (4 confirmed/fixed); a second-round certification was attempted twice and
  died both times (usage limit, then IMP-086: gauntlet resume replays a cached fixer's report
  but not its edits and self-declares THRASH), and a third run was cancelled at merge.
- **Two real defects the delivery gauntlet caught in the Block Kit renderer:** the parent
  collected only `new` + `chronic`, so a `fix_not_working` DAG never reached Slack; and the
  partial-sweep and no-change-tracking caveats reached only the text digest, so a Slack reader
  saw a confident post with the warning missing. Both fixed.

## 2026-08-27 — #1230 and #1231 MERGED; the savings log is the leadership number

**Both PRs merged 2026-08-27** (airflow-ti squash-and-merge): **#1230** = the Slack delivery wire
(debugger side, see [[project_airflow_debugger]]) + the **optimizer savings log**; **#1231** =
`fangorn_score_monitor` `spark.sql.shuffle.partitions` 512/256 → **2048 in the decorator AND the
builder** (builder wins at `getOrCreate`, so both must move).

**Savings log design (`ledger.savings()` / `render_savings()`), the parts that are easy to get wrong:**
- **Per-DAG once-only counting.** Saving = (before-rate minus after-rate) × days observed, resolved
  fixes only.
- **Run rate spreads the saving over CALENDAR days since `applied_date`**, not per observed
  sweep-day — otherwise a weekly job projects 7x.
- **YTD year comes from the sweep date**; est annual = rate × 365.
- **Dollars appear ONLY when `OPTIMIZER_USD_PER_EXEC_H` is set**, and are labeled estimates.
- The savings **headline posts into the Slack digest each sweep only when a measured saving exists**.

**The gauntlet reverted my own evidence-backed speculation change** to `models/ipdsc/ipdsc_ds_35.py`:
airflow-ti pins `spark.speculation=false` on every GCS-writing model (ManifestCommitter race —
`advertiser_join.py` comment, `intent_score_map.py:54`). Queue item 4 (ipdsc_ds_35 straggler) is
**back to owner-gated**. Contradiction record: [[reference_dataproc_eventlog_profiling]];
repo facts: [[reference_airflow_ti]]. *(Contradicted 2026-09-03 on AUDI-1275: the pin is on 2 of the 13 straggler DAGs only, the manifest-committer pair `advertiser_join` / `prospecting_join`; the other 11 run FileOutputCommitter v2 or Iceberg, where Spark's `OutputCommitCoordinator` or the task abort discards the losing attempt, and the `audience_intent` scoring batches have run speculation on v2 to GCS since 2025-08-15 with hundreds of duplicates killed per run and 0 failures. Both claims, the reconciling hypothesis and the settling check, the first post-merge `site_network_hourly` log after airflow-ti #1271, live in [[reference_dataproc_eventlog_profiling]]. `ipdsc_ds_35` was not among the 13 read, so its writer class is unverified and it stays owner-gated until Ryan answers.)*

**Found en route: airflow-ti CI job `model-unit-test` is broken repo-wide since #1209 merged
2026-08-26** (a fixture test rewrites the generated `model_config.json` during collection). Not a
required check — `mergeStateStatus` UNSTABLE, not BLOCKED. Owner rkleck-mntn, diagnosis posted on
#1231. Detail: [[reference_airflow_ti]]. Changing decorator `runtime_properties` also requires
regenerating `dags/model_task_config.json` (`MNTN_SDLC_ENV=dev python model_upload.py --dryrun`,
uv group `models`) or `model-upload-dryrun` fails. *(Contradicted 2026-09-02 on AUDI-1273: with Java 17 on
PATH and `PYSPARK_PYTHON` pinned to the venv, `tests/models` passes 145/145 at main `825b07e`; both claims
and the settling check live in [[reference_airflow_ti]] § CI Pipeline.)*

~~PENDING~~ **DONE 2026-08-27:** the fangorn applied marker is written to the prod GCS optimizer
ledger (4 rows: `shuffle_partition_sizing:17`/`:19` + `disk_spill:17`/`:19`, fix_pr #1231,
applied_date 2026-08-27) after the gcloud reauth. The next sweep starts savings attribution to
#1231 when those keys go quiet.

## 2026-08-27 — hackathon list, AUDI-1241, ownership shift

**Full-corpus sweep (3,085 logs)** distilled to
`tickets/audi_1194_.../outputs/audi_1194_hackathon_optimizations_2026_08_27.md`: **67 distinct
(job, mechanism) pairs, 30,163+ exec-h at stake (floor)** — 8 PR-READY, 53 VERIFY-FIRST, 6 already
queued. **AUDI-1241** filed under the Q3 tech-debt epic AUDI-1054 for the burn-down.

**Ownership shift:** `site_network_hourly` (was Ryan Kleck's) and the DDP dbt tests (Sean Yang's
team, which the user is on) are now the user's team's to fix directly. Queue items 1, 2, 7 flipped
OWNER → OURS; fixes merged (airflow-ti **#1232**) or in review (**dbt#174**). Confluence "TPA
Pipeline On-Call Reference" (`3769991216`) remote-linked from AUDI-1194 — content merged into the
"TI On Call Playbook" page `2908061697` on 2026-08-28; `3769991216` is now a redirect stub.

**Tuning reference installed 2026-08-27 (commit e7973a9b):** the compiled DBR 17.3 / Dataproc 2.3
reference is the skill `.claude/skills/pyspark-optimization-databricks-dataproc/SKILL.md`;
[[reference_pyspark_optimization_skill]] is the MNTN annex that wins on conflict (serverless
Spark 4.0 fleet, speculation/fetch-wait/EXPLAIN COST overrides). Read both before acting on any
optimizer or debugger finding. Fix text stays canonical in `optimizations.py` (drift: IMP-093).

## 2026-08-28 — #1241/#1242/#1243 merged, #1244 open; BQ ledger table; Mode dashboard; fixlog sync

**Merged today (airflow-ti):**
- **#1241** — the `adv_score` monitor writes Spark event logs via decorator `runtime_properties`;
  closes the LAST readable-Spark coverage gap.
- **#1242** — unmatched diagnoses post to `SLACK_FALLBACK_CHANNEL=C0BT9TKRMKM` (#airflow-debugger);
  alert channels are threaded-only.
- **#1243** — exactly-once markers now use a gcloud token + the GCS JSON API. **`gsutil` is
  UNAUTHENTICATED inside Astro task pods**, which made the rapid sweep's marker writes silently
  fail and spam duplicates; two purges of `C08CURMGNMQ` were run.
**#1244 OPEN** (priority rationale line in each filed ticket + IMP-087 alert-search cursor
pagination, 3 pages); Ryan to merge.

**Rapid DAG live and verified.** Unpause/pause via `PATCH is_paused` with `AIRFLOW_BEARER`.
**Deploy-rollout race repeats:** unpaused too early twice — wait ~10 min after merge before
unpausing.

**BQ ledger table (queryable savings source):**
- Dataset `mntn-prj-prod-00:optimizer` created via **PAM breakglass-editor** (auto-approved,
  roles/writer; list entitlements with `gcloud pam entitlements search`).
- External table `optimizer.optimization_ledger` over
  `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` with a **PINNED schema** —
  autodetect typed `applied_date` DATE, then `""` rows fail at scan time; pin it STRING.
- Mode SA (`mode-analytics@dw-main-bronze`) granted dataset READER via `bq update` ACL.
- **`bq_run.sh` gotcha: backticked identifiers need `--use_legacy_sql=false`** — the bq CLI
  defaults to legacy SQL.

**Mode dashboard shipped:** report `e81786de8403` "Spark Optimizer Savings", Audience Intelligence
space, custom HTML layout. API mechanics: [[reference_mode_api]].

**Savings semantics CONFIRMED:** the unit is per-DAG per-CALENDAR-DAY exec-h (all runs of the day
summed, so run frequency is inherent). Before window = all ledger days pre-merge (fixed); after
window grows daily and is recomputed every sweep indefinitely. **First real saving: fangorn #1231 =
575.6 exec-h/day = $160.02/day, est $58.4k/yr at $0.278/exec-h.** *(WITHDRAWN 2026-09-02: the measurement window was the downloader blind window; with full visibility the honest headline is $0 — see the 2026-09-02 late-night section. The old claim stays as the evidence trail.)* Ledger: 446 rows with `exec_h`,
266 findings-only.

**Coverage-report correction:** the coverage report ALREADY names the 39 non-Spark DAGs (a
"No Spark task" section listing operators) — the earlier claim that it only counted them was wrong
(report truncation, not a report gap).

**`airflow_optimizer/fixlog.py` NEW:** syncs an "Optimizer fix log" section (markers
`optimizer-fixlog-start`/`optimizer-fixlog-end`) from the ledger into Confluence playbook
`2908061697`; runs in `daily_gap_check.sh`'s noon job. AUDI-1241 carries the non-Spark profiling
checklist comment; full phase plan:
`tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_nonspark_phase_plan.md`.

## 2026-08-28 (later) — #1245 MERGED: the optimizer is multi-surface (spark | bq | dbx)

**airflow-ti #1245 merged 2026-08-28** — all three non-Spark surfaces in one PR at the user's ask:
- **`bq_profile.py`** profiles BigQuery per dag/task from `INFORMATION_SCHEMA.JOBS_BY_USER` via
  REST + a gcloud token. The fleet SA reads its OWN job history, so **no new grant** was needed.
  Attribution facts: [[reference_bq_job_attribution]].
- **Ledger `Entry` gained a `surface` field** (`spark` | `bq` | `dbx`); resolution and savings are
  scoped PER SURFACE so slot-hours, DBUs, and executor-hours never mix in one number.
- **`billing.surface_rates()`** prices bq slot-h from the billing export (service
  `'BigQuery Reservation API'`, sku `LIKE '%Slot%'`), with env fallback `OPTIMIZER_USD_PER_SLOT_H`.
- **`databricks.findings_reports()`** adds detectors `dbx_heavy_job` (>$50/day list) and
  `dbx_failing_model` (3+ fails in 7 days).
- **`sweep`** writes `optimizer_bq_<date>.md` and records Databricks findings under `surface="dbx"`.

**Pod profiler BLOCKED** on the Astro Universal Metrics Exporter → GCP Managed Prometheus; setup
steps at `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_astro_metrics_exporter_setup.md`.
(**Superseded 2026-08-31:** the DEV-8821 relay is LIVE — [[reference_astro_metrics_relay]] and the
2026-08-31 evening section.)

**Mode dashboard follows the surfaces:** external table `optimizer.optimization_ledger` schema
re-pinned with `surface STRING` (`ignoreUnknownValues`); the headline dollars query is now
spark-only; new query "Savings by surface" (token `513a4a7a4a71`); layout gained a
Savings-by-surface table section.

**Open:** verify the next sweep writes the optimizer_bq/dbx sections and ledger rows carrying
`surface` in prod.

## 2026-08-28 (evening) — first live multi-surface sweep: end-to-end validated, identity bug found

**The first live multi-surface sweep validated end-to-end and exposed the identity bug.** The
#5121 billing grant went to `airflow-ti-prod@`, but the sweep runs as `spark-optimizer@`, so the
live billing rate has ALWAYS fallen back to `OPTIMIZER_USD_PER_EXEC_H=0.278`. Same identity broke
the BQ profiler: `JOBS_BY_USER` under `spark-optimizer@` sees none of the fleet's jobs (see
[[reference_bq_job_attribution]] correction).

**Fix PRs open:**
- **airflow-ti#1247** — `JOBS_BY_PROJECT` + SA filter (`OPTIMIZER_BQ_SAS`); also made the sweep
  tests hermetic — they were silently querying real BigQuery from a credentialed laptop via the
  unstubbed `bq` pass.
- **mntn-devops#5160** — grants for `spark-optimizer@` (`bigquery.jobUser` + `resourceViewer` on
  `dw-main-bronze`). Note: the `iam/spark-optimizer/` dir already existed there — check before
  `mkdir`.

**BQ surface was blocked until both merged (they did, 2026-08-29 — next section).** Separately,
`fangorn_score_monitor` #1231 was re-marked applied 2026-08-27, so measured savings restarted
from 0 and rebuild daily.

## 2026-08-29 — fix PRs merged, BQ surface + live billing rate verified in prod

Both **mntn-devops#5160 and airflow-ti#1247 MERGED**; after the Astro deploy went HEALTHY a
manual `spark_optimizer_daily` run (`manual__2026-08-29T01:23:07`) succeeded and verified:
- **BQ surface live:** `optimizer_bq_2026-08-29.md` published (top:
  `bos__spend campaign_summary_hourly-create`, 69 slot-h); ledger has `surface=bq` rows
  (`bos__spend` 123.7 slot-h, `intent_score_threshold_v4` 54.1).
- **Billing rate LIVE, env fallback retired:** `[sweep] usd/exec-h 0.278 (blended from 30d of
  actual spend: $0.0511/DCU-h x 5.44 DCU-h per executor-hour)` — the live blended rate equals the
  old `OPTIMIZER_USD_PER_EXEC_H=0.278` fallback, so prior savings dollars stand.
- **Coverage "0 cost-profiled of 39" is expected, not a bug:** today's labeled BQ jobs all came
  from Spark DAGs; no-Spark `BigQueryInsertJobOperator` DAGs (category_taxonomy etc.) tag only on
  days they run; most of the 39 are pod/sensor DAGs blocked on DEV-8821.
- **No dbx ledger rows** — nothing crossed the $50/day or 3-failure detector thresholds.
- `fangorn_household_14day_lookback` succeeded on the second manual retry.
- **Airflow REST log pulls:** `Accept: text/plain` rejected ("Only application/json or
  application/x-ndjson"); logs paginate via `continuation_token`.
- **Jira SA request = ITS-6496** (Pending External, assignee Robin Fox).

## 2026-08-31 — hackathon refinement: the backlog became 13 sprint tickets (AUDI-1269..1281)

**Bryce's fall tech-debt hackathon** — sprint **8649** (09/07-09/21, board 1814); three tracks:
alerting audit / pipeline testing framework / pipeline optimization audit; refinement format =
30 min ticket writing + 30 min group review; epic **AUDI-1290 "Pipeline Optimization
Hackathon"** created 2026-08-31 (parents all 13; labels `hackathon`+`q3_2026` on epic and children).
**13 AUDI Tasks filed, grouped by CHANGE TYPE not by DAG** (user's rule: the same change across
many DAGs = ONE ticket; a different change on the same DAG = a DIFFERENT ticket; 1-2 SP each):
AUDI-1269..1281. **16 SP to Malachi** (1270, 1271, 1272, 1275-1281), **4 SP left deliberately
simple for others** (1269, 1273, 1274). Specs:
`tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

**No rescan needed:** the 08-27 full-corpus sweep is still authoritative (fleet configs
unchanged; merged fixes already measure in the ledger). New BQ-surface findings folded into the
drafts: `bos__spend` 1,275 + 977 slot-h/day, `intent_score_threshold_v4` 1,075 slot-h,
unattributed (unlabeled) jobs 1,185 slot-h/day → AUDI-1277/1278. *(2026-09-02: the LEDGER's unattributed bucket is EMPTY — every job the cost surface measures carries `airflow-dag`/`airflow-task` labels, so no team-labeling campaign is needed for the measured surface. The 1,185 figure is raw-profiling scope; reconciling hypothesis = jobs outside the airflow-launched set; settle in AUDI-1278 by joining the profiled unlabeled jobs against the ledger population.)* *(Settled 2026-09-03, AUDI-1278: hypothesis REFUTED. The bucket is fleet-SA jobs the ledger skips by design; 96.9% of its slot-hours are camperbid `bos__spend` Spark-BigQuery-connector reads. See the 2026-09-03 AUDI-1278 section.)*

**Savings provenance during the hackathon (others' PRs count too):** the ledger auto-measures
savings regardless of fix author — a finding that stops firing resolves and its savings accrue.
Stamp provenance per merged fix:
`python -m airflow_optimizer.ledger applied <dag> <key> <pr> <date>`.
Plan: daily reconcile of merged airflow-ti PRs vs ledger findings for the sprint duration.

## 2026-08-31 (evening) — PR #1250 dbx surface, PR #1252, DEV-8821 relay LIVE

**PR #1250 OPEN — Databricks surface via SP OAuth REST.** `databricks._api` routes through curl +
a cached OIDC client-credentials token when `DATABRICKS_HOST` / `DATABRICKS_GCP_CLIENT_ID` /
`DATABRICKS_GCP_CLIENT_SECRET` env are set; CLI fallback stays for laptops; the sweep prints a
skip line when no warehouse is configured.

**Root cause of dbx dormancy:** `databricks.report()` returns `""` SILENTLY without
`DATABRICKS_WAREHOUSE`; prod's image has no databricks CLI and only the `CLIENT_SECRET` var is
set — so the surface never errored, it just produced nothing.

**The "ml_squad warehouse" is the MAIN workspace** `1262887251702944.4.gcp` (dbt
`ml_squad/profiles.yml` targets it): warehouses `Serverless Starter` `14b311ac86ee2ca2` +
`sql_warehouse_2xs` `fa27430dfc609e6d`. Workspace SPs: `dev_runner` `81b867bc`,
`spark_optimizer` `07f36af7`, `prod_runner` `397d710b` (candidate client id for the prod vars;
PROVEN PAIRED 2026-09-01 — the prod secret pairs with `prod_runner`, `spark_optimizer` gets
oauth 401). Detail: [[reference_databricks]].

**dbt PR 174 (SteelHouse/dbt) baseline captured:** `prod-ml-ddp_vertical_classification_api` is
the top warehouse consumer — **306,352 query-s / 244 runs / 7 days**. After #1250 merges: set
`DATABRICKS_HOST` + `DATABRICKS_GCP_CLIENT_ID` + `DATABRICKS_WAREHOUSE` on prod, verify dbx
ledger rows, stamp PR-174 provenance against this baseline. *(2026-09-02: stamp CLOSED as
unmeasurable — `ledger.mark_applied` needs a ledger history row and this job never entered the
ledger; the fix shipped before the dbx surface existed and post-fix cost sits below the detector
threshold. Baseline preserved in the AUDI-1194 summary. No stamp will happen.)*

**PR #1252 OPEN:** sweep-note `gs://` refs render as console URLs via `digest._gcs_link`;
`coverage.resolve` consults `OPTIMIZER_NAME_OVERRIDES` env JSON (app name → dag id) for names the
bundle crawl cannot tie (`ETL Audience Intent - *`, `segment-updates-to-parquet`) — populate the
values with the owning team before setting the var.

**DEV-8821 relay LIVE** (endpoint/auth/PromQL gotchas: [[reference_astro_metrics_relay]]): Astro
prod Metrics Exports configured ~19:45 UTC 2026-08-31; verification pending (no `container_*`
series yet, Cristina checking relay logs). Then `pod_profile.py` — ledger surface `"pod"`.

## 2026-09-01 — all four PRs MERGED + LIVE (image deploy-2026-09-01T19-06-22); dbx engaged, grants blocker

- **PRs #1250 (dbx REST) / #1251 (digest threading) / #1252 (console links +
  `OPTIMIZER_NAME_OVERRIDES` hook) / #1253 (hour-scaled dots ≥100 red / ≥25 orange, chronic rows
  show hour deltas vs last sweep via `Entry.prev_exec_h`, 6 fix texts reworded action-first) all
  MERGED 2026-09-01 and LIVE via retrigger PR #1254** — the fast merge train hit the Astro
  superseded-build gap: [[reference_astro_deploy_mechanics]].
- **dbx REST surface ENGAGED in prod:** oauth mints, the prod secret pairs `prod_runner`
  `397d710b` (`spark_optimizer` `07f36af7` gets 401 — swap-and-revert test). **Blocker:**
  `INSUFFICIENT_PERMISSIONS` on job_costs/query_costs/plans; `prod_runner` needs `SELECT` on
  `system.lakeflow` + `system.query` and `CAN USE` on warehouse `fa27430dfc609e6d`; grants ask
  drafted to ml_squad/Brian. Detail: [[reference_databricks]].
- **Env vars `DATABRICKS_HOST` / `DATABRICKS_GCP_CLIENT_ID` / `DATABRICKS_WAREHOUSE` staged on
  Astro prod.** Oddity: the sweep prints `[sweep] databricks skipped: no warehouse configured
  (DATABRICKS_WAREHOUSE)` though the var is set — likely a mislabeled empty-report message in
  `sweep.py`; check the message routing.
- **`OPTIMIZER_NAME_OVERRIDES` draft mapping** in
  `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`:
  `segment-updates-to-parquet` → `materialize_mntn_first_party` CONFIRMED; the `ETL Audience
  Intent - *` apps live in `spark/audience_intent/*.py` but the prod launcher is unconfirmed —
  owning team confirms before setting the var (RESOLVED later the same day: var SET with the ETL
  entries excluded — next section). IMP-097 filed: per-DAG owner mapping idea.
- **DEV-8821 relay FULLY LIVE end to end** (zero drops; `kube_pod_status_phase` 162 series;
  `container_*` filling): [[reference_astro_metrics_relay]]. `pod_profile.py` built later the
  same day — next section.

## 2026-09-01 (later) — "39 unprofiled" decomposed (PR #1255), overrides live, pod surface PR #1257

**The digest's "39 DAGs unprofiled" complaint decomposed into three real causes, none a profiler
bug:**
1. **7 paused DAGs counted as active.** The ORM paused-set read is FORBIDDEN inside Astro tasks
   (`airflow session use is forbidden`); fix = REST fallback via `AIRFLOW_BEARER` — **PR #1255
   OPEN**.
2. **Only 1 DAG is actually cost-covered** — dbx still blocked on the `prod_runner` grants.
3. **Blunt chip wording** — reworded to "N DAGs without cost data", computed from
   `Coverage.invisible`.

**`OPTIMIZER_NAME_OVERRIDES` SET on Astro prod 2026-09-01: 14 source-verified entries** — all 12
unmatched app names plus the hashed-email apps (`ds=22`/`ds=29`). The `ETL Audience Intent - *`
entries stay EXCLUDED pending owner confirmation.

**Pod surface PR #1257 OPEN** — `pod_profile.py`, ledger `surface="pod"`, unit core-hours/day;
findings `cpu-overprovisioned` + `memory-pressure` (requested-vs-used from the DEV-8821 relay
metrics; relayed counters are read via the Monitoring v3 API, never PromQL —
[[reference_astro_metrics_relay]]). **Blocked on mntn-devops PR #5224**
(`roles/monitoring.viewer`; its gauntlet fixer swapped in the NONEXISTENT
`roles/monitoring.metricReader` and the refuter confirmed it — caught via the IAM API,
[[feedback_gauntlet_findings_not_fixes]]) **+ the `OPTIMIZER_POD_PROJECT` env var.** *(Cleared the same night: #5224 merged, var set — next section.)*

**Mode dashboard gained a BQ cost table entirely via the API** (query "BigQuery cost by task",
token `3ead7301daa8`, layout section `opt-bq`, run `d2d0b89e9cef` succeeded):
[[reference_mode_api]].

Review queue at close: airflow-ti #1255/#1256/#1257 + mntn-devops #5224. *(Superseded the same
night: all three combined into PR #1258 and merged — next section.)*

## 2026-09-01 (evening) — combined PR #1258 LIVE; pod first light exposes two prod bugs (#1259/#1260); 12-day diagnosis

- **PRs #1255/#1256/#1257 CLOSED as superseded and COMBINED into PR #1258** (branches kept;
  octopus merge, 430 tests green) so one airflow-ti merge = one Astro deploy — no
  superseded-build exposure. **#1258 MERGED + LIVE on image `deploy-2026-09-01T22-22-40`
  (HEALTHY).** mntn-devops **#5224 MERGED** (`roles/monitoring.viewer` synced to IAM);
  `OPTIMIZER_POD_PROJECT=mntn-prj-prod-00` set post-deploy. The failure-trigger plugin
  registered in prod — [[project_airflow_debugger]].
- **Pod surface first light:** verification sweep `manual__22:36` SUCCESS —
  `optimizer_pod_2026-09-01.md` published, pod ledger rows landed, honest warehouse message
  confirmed. **But the numbers were wrong: Cloud Monitoring v3 `timeSeries.list` returns points
  NEWEST FIRST**, so the cpu rate (oldest-minus-newest) went negative, filtered to 0 cores
  everywhere, `exec_h` NULL. **Fix PR #1259** (rate + limits use the newest point; rate divisor
  = span between point TIMESTAMPS so sparse points no longer inflate) **verified LIVE:
  worker-default 0.875 cores = 11% of its 8-core limit (real downsize candidate); dag-processor
  55%.** [[reference_astro_metrics_relay]]
- **Downloader freeze root-caused:** every sweep since 08-28 exited "Done" with ~2/192 event
  logs landed (194/200 counted failed), freezing finding resolution for 6 consecutive sweeps —
  gsutil `-m`'s process-forked workers die quietly on the 0.25-CPU pod. Proven by isolation
  (forked `-m` fails on the Mac AND the pod; plain `cp` and `parallel_process_count=1` copy
  everything). **Fix PR #1260** — threads-only `-m` via `GSUTIL_OPTS` in `fetch.py`. Also:
  spark-events objects are GHFS-synced composites with NO hashes — "Found no hashes to
  validate" under `check_hashes=never` is benign. [[reference_gcloud_storage_over_gsutil]]
- **Full-history diagnosis** (`outputs/audi_1194_diagnosis_2026_09_01.md`; the "30-day" ask
  covers the system's ENTIRE 12-day life 08-21..09-01, 936 ledger rows, BQ vs GCS mirror exact
  match): (1) the downloader freeze is the root of nearly everything downstream (dags/day
  65→20, 30 DAGs never seen after 08-26, resolution frozen) — now fixed via #1260; (2) **dbx
  surface 0 rows ever** (`prod_runner` grants ask outstanding); (3) **near-disjoint fleets** —
  the debugger's top-3 offenders (72% of its diagnosis rows) have zero ledger rows ever
  (Databricks-API/dbt/pod/OpenAI jobs = exactly the optimizer's blind spots).
- Two gauntlet runs on #1259 died on API server errors mid-fixer, leaving half-applied edits —
  diff before building on a post-gauntlet tree: [[feedback_gauntlet_findings_not_fixes]].
- **Review queue at close: #1259 (pod rate) + #1260 (downloader; retitled 2026-09-02: + parse-rate canary).** After both merge + deploy:
  manual sweep, expect `complete=True` and resolutions flowing again; next natural task failure
  proves the instant trigger end to end.

## 2026-09-02 (morning) — unattributed BQ bucket verified EMPTY; digest user-verified; Alyson has the grants paste

- **The BQ cost surface's unattributed bucket is EMPTY in the ledger (verified 2026-09-02):**
  every BigQuery job the surface measures carries `airflow-dag`/`airflow-task` labels — **no
  team-labeling campaign needed.** The 08-31 raw-profiling figure (unlabeled 1,185 slot-h/day)
  is a different population; reconciling hypothesis = jobs outside the airflow-launched
  measured set; settle in AUDI-1278. *(Settled 2026-09-03, AUDI-1278: REFUTED. Both populations are
  fleet-SA jobs; the ledger skips the unlabeled ones by design and 96.9% of their slot-hours are
  camperbid `bos__spend` connector reads. Section at the end of this file.)*
- **The 35 "without cost data" DAGs close only three ways:** (1) the `prod_runner` dbx grants,
  (2) hackathon per-DAG event logging, (3) genuinely-no-compute. **Grants paste sent to Alyson
  2026-09-02** for `prod_runner` (`397d710b`): `system.lakeflow` + `system.query` SELECT
  ladders + warehouse `fa27430dfc609e6d` Can-use (warehouse access is NOT SQL-grantable —
  [[reference_databricks_system_schema_grants]]).
- **User verification pass (screenshots): the digest works as designed** — override links
  (incl. ETL Audience Intent), hour dots, deltas, cost chip, pod/BQ report links, threaded
  What/Fix, honest partial-sweep note. **New ask: ranked rows read ragged in Slack (emoji +
  number misalign) — reformat queued for the post-merge digest pass**
  ([[feedback_slack_digest_not_per_event]]). *(Shipped overnight — next section.)*
- **PR #1260 retitled: "downloader loses the batch; canary for silent parse breaks"** — the
  debugger's parse-rate canary rides it ([[project_airflow_debugger]]). Review queue
  #1259 + #1260. *(Retitled again overnight, 3 commits — next section.)*

## 2026-09-02 (overnight) — digest numbered list shipped; overrides 22 entries; unlinked apps CLOSED

- **Digest ranked rows now ALIGN: a `rich_text` block with `rich_text_list style=ordered`** —
  the client renders the numbers in the gutter and hanging-indents wrapped lines; hand-numbered
  mrkdwn `*1.*` + emoji prefixes can never align. Shipped in **PR #1260 commit `dd53939`**,
  preview confirmed by the user 2026-09-02. Trade: context-block small-grey styling is not
  expressible inside `rich_text`, so the meta line became italic text. Recipe + the local
  preview token gotcha (`~/.zshrc` `SLACK_BOT_TOKEN` is DEAD, `account_inactive`; keychain
  only): [[feedback_slack_digest_not_per_event]], [[reference_slack_debugger_app]].
- **The "unlinked digest rows" question is CLOSED — every formerly-unlinked Spark app is an
  airflow-ti DAG** whose free-form `appName` the resolver could not tie to a dag_id.
  Source-verified mappings: five `ETL Audience Intent - *` apps → `audience_intent` DAG
  (`dags/audience_intent/audience_intent.py` submits `vertical_mid.py`, `vertical_high.py`,
  `prospecting_high.py`, `prospecting_keywords.py`, `prospecting_mid.py`);
  `Run Single-Day TPA Export for <date>` → `tpa_ipdsc_export` via `tpa_export_spark_batch` →
  `spark/exporter/export_tpa.py`; `Populate targeted_signal for CRM source` →
  `targeted_signal_crm`.
- **CORRECTION (supersedes the 2026-09-01 exclusion):** "`ETL Audience Intent - *` prod
  launcher unconfirmed, ask owning team" (evidence then: only the staging DAG
  `audience_intent_scoring_staging` found) is superseded — the prod launcher IS
  `dags/audience_intent/audience_intent.py`, traced in source 2026-09-02. The old note stays
  above for the evidence trail.
- **`OPTIMIZER_NAME_OVERRIDES` prod var now 22 entries (was 14)** — the 8 new entries
  source-verified and set live with NO merge dependency (env var, not code).
  **`coverage.resolve` accepts trailing-wildcard prefix keys** (PR #1260 commit `3d87c6f`):
  exact match beats prefix, longest prefix wins.
- **NEW OPEN QUESTION: flagged apps' event logs VANISH from
  `gs://mntn-data-archive-prod/spark-events` within hours** — the 09-01 and 09-02 backlog app
  ids 404 while same-hour neighbors persist, yet the sweep read them fine at 07:00 UTC.
- **PR #1260 now carries 3 commits** (canary `99ba84f`, digest `dd53939`, overrides `3d87c6f`),
  retitled **"AUDI-1191/1194: downloader loses the batch; parse canary; digest numbered
  list"**, body updated, Jira AUDI-1194 comment posted (id 614410). Both overnight gauntlets
  PASS clean first round. Review queue: #1259 + #1260. *(Both merged 2026-09-02 — next
  section.)*

## 2026-09-02 (afternoon/evening) — gsutil BANNED on pods, JSON API rewrite live, six-day freeze CLOSED; billing granted; dbt#174 stamp closed

- **The 2026-09-01 "threads-only `-m` is clean" fix is FALSIFIED** (evidence then: threads-only
  and plain `cp` copied everything in the Mac/pod isolation matrix). In prod, EVERY gsutil mode
  lands ~2 of ~194 objects on the pod — forked `-m`, threaded `-m` + `parallel_process_count=1`
  (PR #1260's fix), and plain sequential `cp -I` — while the identical sequential command moves
  all 194 (1.8 GiB) from a Mac. Not source deletion (fresh listings re-stat clean). **gsutil
  itself is broken on Astro task pods**; same pod-only class as the debugger's 2026-08-28 marker
  failure (PR #1243) — exactly the day resolutions froze. The 09-01 note stays above as the
  evidence trail. [[reference_gcloud_storage_over_gsutil]]
- **Downloader saga CLOSED: PR #1263 (drop `-m`) insufficient; PR #1264 (GCS JSON API rewrite —
  gcloud-token + `objects.list` + `alt=media`, stored bytes untranscoded, `zstd -t` verified)
  MERGED, live on `deploy-2026-09-02T19-27-09`.** The 19:35 UTC sweep ran **complete=True on the
  full corpus (346 jobs)** and **41+ resolutions flowed — the six-day finding-resolution freeze
  is CLOSED.**
- **Deploy recovery recipe learned en route** (Astro builds die quietly when superseded and a
  merge SHA can register NO build; the GitHub "Deploy to Prod" action only syncs GCS; recovery =
  Astro UI "Retry Git Deploy" or any new push to main; README PR #1262 documents it in-repo):
  [[reference_astro_deploy_mechanics]].
- **`system.billing` GRANTED by Alyson 2026-09-02 (USE SCHEMA + SELECT)** — the original grants
  ladder was MISSING it (the cost queries join `billing.usage` + `billing.list_prices`; the
  INSUFFICIENT_PERMISSIONS securable detail sits on the SECOND log line). **dbx cost report LIVE
  the same day** — top row `Generate Graph & Metrics - PRODUCTION`, 10,528 DBU, $1,579 list/7d.
  [[reference_databricks_system_schema_grants]]
- **dbt #174 provenance stamp CLOSED as unmeasurable:** `ledger.mark_applied` needs a ledger
  history row, and `prod-ml-ddp_vertical_classification_api` never entered the ledger — the fix
  shipped before the dbx surface existed, and post-fix cost sits below the detector threshold.
  Baseline (306,352 query-s/7d) preserved in the AUDI-1194 summary; no stamp will happen.
- **Verification state for 2026-09-03:** the parse canary's first live run is the **09-03 17:00
  UTC daily** — today's 17:00 run predates the 17:43 deploy, and `rca_2026-09-02.json` not
  existing is EXPECTED (dailies publish the prior data day). Rapid runs green every 15 min. PRs
  merged today: airflow-ti #1259 #1260 #1262 #1263 #1264. Jira AUDI-1194 comments: 614410
  (progress), 614725 (completion).

## 2026-09-02 (late night) — fangorn #1231 did NOT hold; Mode savings honestly $0

- **The fangorn "resolution" was an artifact of the blind window, and the honest Mode savings
  number is $0.** `fangorn_score_monitor` ran **687.7 exec-h on 08-26** and **954.3 on 08-27**
  (the day #1231 merged), was INVISIBLE 08-28..09-01 (the downloader lost every batch), and
  returned on 09-02's first complete sweep at **659.2 exec-h with the stage-17/19 `disk_spill`
  findings CHRONIC again**, `fix_pr` #1231 still attached.
- **The earlier ~$900 cumulative savings figure is WITHDRAWN** — it accrued only while the scan
  could not see the job. The Mode headline query clamps `GREATEST(before - after, 0)` to 0
  because the after-rate (**806.8 exec-h/day avg, fix day included**) exceeds the before-rate
  (687.7). `ledger.py`'s per-finding view still credits **28.5h** for the wide-shuffle
  components that genuinely resolved.
- **Lesson: a `resolved` state reached during a visibility gap is not evidence a fix worked** —
  before crediting a resolution, check the sweep actually SAW the job during the quiet window.
- **Fangorn spill re-fix = top hackathon candidate** (AUDI-1290 scope).

## 2026-09-02 (hackathon execute wave) — AUDI-1274 landed; the guid pivot spill mechanism was the core-count floor, not the 64 MiB advisory size

- **AUDI-1274 (spec rows 10-11, `guid_log_pivot_ip_vertical_id` / `guid_conv_log_pivot_ip_vertical_id`
  `disk_spill:33` / `disk_spill:34`) is committed on branch `audi-1274-aqe-advisory-pivot`:** one builder
  line per model, `.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16m")`, decorators and
  `dags/` untouched, `model_upload.py --dryrun` diff-clean (a builder line is never serialized into
  `model_task_config.json`). Jira comment posted, status In Progress, PR pending the gauntlet. Done-when:
  PR merged, `ledger applied` stamped on both keys, then `resolved` after `RESOLVE_SWEEPS = 3` quiet
  sweeps. Ledger on 09-02: both keys chronic (streak 6 / 7, exec_h 8.4 / 8.5).
- **Contradiction, appended not overwritten: the spec's mechanism was wrong, its remedy right.** Spec row
  10 evidence said AQE coalesces the 8,000 map partitions "to its 64 MiB advisory size (48.9 GiB / 800 =
  61 MiB)". The 08-26 event logs say every coalesced shuffle in the job lands on exactly 800 partitions
  whatever its size (17.7 to 31.4 GiB, per-partition 22.7 to 40.2 MiB, all below 64 MiB), and one stage
  planned at 90 executors landed on 728 = 90 x 8 cores. With `spark.default.parallelism` unset and
  `coalescePartitions.parallelismFirst` default true, the target is min(64 MiB, total / registered
  cores), so the count pins at cores. The spec's 61 MiB was the two co-partitioned inputs summed per task.
  Evidence for the old claim: the 08-05/08-06 log read in the 08-27 sweep; for the new: the 08-26 logs
  with `artifacts/audi_1274_exec_timing_probe.py`. Settling check: post-merge, every coalesced shuffle
  should report about (GiB x 64) partitions and the pivot stage 3,100 to 4,000 tasks; if counts still
  track cores, the floor was not what the log suggests. Full mechanism + the event-log trick:
  [[reference_dataproc_eventlog_profiling]].
- **Lesson for the detector's fix text:** on any AQE-coalesced reduce stage "raise
  `spark.sql.shuffle.partitions`" cannot move the task count above registered cores; the spec already
  routed rows 10-11 to the advisory size, but the same floor applies fleet-wide wherever
  `advisoryPartitionSizeInBytes` is unset, so check the coalesced count against executors x cores
  before prescribing partitions on a spill finding.
- Post-merge verification is the next scheduled `feature_store_setup_model` run (`3 1 * * *` UTC), never a
  manual trigger; the flagged apps' logs can vanish from `spark-events` within hours, so pull them the same
  morning (procedure: ticket `summary.md` §3 step 9).

## 2026-09-03 — AUDI-1273 landed; ds67 is not a maxPartitionBytes job; ledger hand-edit ops

- **AUDI-1273 (spec items 4, 7, 8; `ipdsc_ds_49 disk_spill:1`, `conv_log_derived_ip disk_spill:1`,
  `ipdsc_ds_67 disk_spill:3` / `:5`) is committed (workspace 0d7a3d02; airflow-ti branch
  `audi-1273-max-partition-bytes` in its worktree, PR pending the gauntlet):** `ipdsc_ds_49` gains
  `.config("spark.sql.files.maxPartitionBytes", "67108864")` (64 MiB; was the 128 MiB platform default),
  `conv_log_derived_ip` goes 268435456 -> 134217728 (256 -> 128 MiB). Dry run diff-clean, 145/145 model
  tests, Jira comment posted, status In Progress. Done-when: PR merged, `ledger applied` on both keys,
  `resolved` after 3 quiet sweeps. Ledger 09-02: ds49 chronic streak 6 (20.9 GiB disk, 3.1 exec-h),
  conv_log_derived_ip chronic 6 (4.2 GiB, 0.3 exec-h, shrinking daily since 08-20), ds67 chronic 8 x2
  (72.8 / 69.1 GiB, 9.4 exec-h).
- **`ipdsc_ds_67` dropped from the PR (user decision D1, Option A), keys go `wont_fix`:** its stage 3/5
  input `gs://mntn-data-archive-prod/ipdsc/dt=<date>/data_source_id=4/` is ~160 files of 60 MiB with ONE
  parquet row group each, and Spark hands a row group to the split holding its midpoint, so the spec's
  32 MiB cap gives one full task and one empty task per file (confirmed locally on Spark 3.5.3). The
  spill is the 81.6 GiB sort-merge exchange stages 3 and 5 each write to join `ui.audience_uploads`
  over JDBC before AQE converts that join to a broadcast anyway; the fix is `F.broadcast(...)` at
  `ipdsc_ds_67.py:80` plus caching `upload_ips`, owner Alyson Lefkowitz, logged as IMP-102, not a
  ticket. Mechanism + the row-group pre-check: [[reference_dataproc_eventlog_profiling]]; decision
  record `knowledge/decisions/0003_ipdsc_ds_67_max_partition_bytes_dropped.md`.
- **Lesson for the detector's fix text:** "lower `spark.sql.files.maxPartitionBytes`" on a read-stage
  spill presumes splittable input. Check file size and row groups per file (pyarrow footer) before
  prescribing it; on single-row-group files the lever is the writer's `parquet.block.size` or the join.
- **Ledger hand-edit ops (verified from `include/spark_optimizer/ledger.py` + `dags/spark_optimizer_daily.py`,
  2026-09-02):** the sweep runs `0 9 * * *` UTC (`max_active_runs=1`) and RESTORES
  `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` from GCS at the start of every sweep, so
  a hand edit is: download, `OPTIMIZER_LEDGER=<local> python -m include.spark_optimizer.ledger set|applied ...`,
  re-upload, all inside one 09:00-to-09:00 UTC window. `set <dag_id> <key> <state> [note words...]` accepts
  ONLY `owner_notified` and `wont_fix` (the `STICKY` states, which survive replay), copies
  impact/title/owner/streak from the key's latest row, and takes the note as the remaining argv words (no
  date argument). `applied <dag_id> <key> <pr> <YYYY-MM-DD>`. Exact ds67 commands: AUDI-1273 `summary.md` §5.
- **Found en route, flagged only (D2), no ticket:** the `conversion_log` GCS archive fell ~6x on 2026-08-20
  (18-23 -> 3-4 GiB/day, no recovery through 08-31) and `conv_log_ip` with it; `conv_log_derived_ip`'s spill
  key has been shrinking every sweep since, so that key may resolve on its own by ~09-19 and be attributed
  to the PR. IMP-103 + `data_knowledge.md` § conversion_log GCS archive volume drop.

## 2026-09-03 — AUDI-1275 landed; speculation is safe where the writer discards the loser; canary PR #1271

**Decision (user, D1+D2 2026-09-02, canary only):** `spark.speculation=true` is safe for 11 of the 13 straggler
DAGs because every writer in those applications discards the losing duplicate attempt (10 on Dataproc's injected
FileOutputCommitter v2 under Spark's `OutputCommitCoordinator`, `identity_targeted_signal` on Iceberg 1.10.2);
`advertiser_join` and `prospecting_join` run the manifest committer under Ryan's pin and stay owner-gated. Applied
to ONE canary, `site_network_hourly` (ours since #1232, hourly, each hour written twice so a bad hour self-repairs):
[airflow-ti#1271](https://github.com/SteelHouse/airflow-ti/pull/1271) OPEN 2026-09-03, +1 line in the decorator
`runtime_properties` and +1 regenerated line in `dags/model_task_config.json`, reviewer rkleck-mntn, gauntlet fast
tier (1 finding refuted, 0 confirmed). Jira comment posted, status In Progress. Slack ask for Ryan (three numbered
asks: his Nov-2025 account, the manifest pair, the other 10) drafted at
`tickets/audi_1290_pipeline_optimization_hackathon/audi_1275_straggler_gcs_writers/artifacts/audi_1275_slack_ask_ryan.md`,
user sends. Memo with source quotes, per-DAG verdicts and the post-merge checklist: `.../outputs/audi_1275_decision_memo.md`;
decision record `knowledge/decisions/0004_speculation_safe_where_writer_discards_loser.md`.
- **Why the 08-27 refutation was too broad:** the pin exists on 2 of 13 models; the Nov 2025 incident was a
  `FileNotFoundException` in the manifest committer's job-commit rename phase that persisted after speculation was
  off (fixed by `manifest.committer.io.threads=1` + `validate.output=false`); the `audience_intent` scoring batches
  have run speculation on v2 to GCS since 2025-08-15 with 366 and 88 duplicates killed in the two PHS logs read, 0
  failures. Contradiction appended, not overwritten: [[reference_dataproc_eventlog_profiling]], [[reference_airflow_ti]].
- **What the canary can and cannot show:** `site_network_hourly`'s long runs (up to 2 h 25 min) are `FetchFailed`
  storms from dynamic-allocation scale-down shuffle loss (up to 116 stage re-submits, 8,244 FetchFailed task ends),
  and the model swallows failed hour writes so the batch SUCCEEDS with 0 bytes. The straggler detector
  (`optimizations.py` L240-265: >= 8 tasks, `skew_ratio` >= 5, max >= 60 s, `data_skew_ratio` < 2) fires on those
  fetch-wait tails, which speculation cannot fix, so the canary measures SAFETY first (`Task Info.Speculative=true`
  attempts ending `TaskKilled` / `Success`, `_SUCCESS` + 4-75 files + 0.2-0.7 GB per hour) and runtime second;
  keys on stages without FetchFailed (11, 13 in `app-20260902115131178-0350`; 39 in `app-20260902005143827-0954`)
  are the ones that can resolve. Detector blind spot = a fix-text lesson (check FetchFailed per stage before
  prescribing speculation); the storms themselves are IMP-104.
- **Ledger follow-through (human):** after the bundle redeploy (up to 12 h) confirm `'spark.speculation': 'true'`
  on the `Compute batch:` line, then stamp `applied site_network_hourly straggler:<n> 1271 <merge-date>` on the 11
  keys present at merge (`:9`, `:11`, `:13`, `:15`, `:18`, `:21`, `:22`, `:23`, `:24`, `:29`, `:39`) with the
  hand-edit recipe in the 2026-09-03 AUDI-1273 section; three clean sweeps plus Ryan's answer gate the second PR
  (10 models).

## 2026-09-03 — AUDI-1278 landed; the unattributed BQ bucket is camperbid's `bos__spend` connector reads (97%); airflow-ti labels shipped (PR #1278)

**The 2026-08-31 / 09-02 "unattributed 1,185 slot-h/day" is fleet-SA jobs with no `airflow-dag` label, not outsiders.**
`bq_profile.py` `PROFILE_SQL` already filters `JOBS_BY_PROJECT` to `user_email IN (airflow-ti-prod@, airflow-camperbid-prod@)`,
so the "jobs outside the airflow-launched set" hypothesis (2026-09-02 morning section) is refuted; the ledger's bucket is
empty because `bq_profile.reports()` attaches no finding to the unattributed report and `ledger.record()` skips finding-less
reports (pinned by `test_heavy_task_is_a_finding_and_unattributed_is_not`). Old lines annotated, not deleted.
- **Composition, 7 days 2026-08-26..09-01 (`JOBS_BY_PROJECT`, one 4.86 GB query per day): 612 jobs/day, 1,109.9 slot-h/day.**
  96.9% of slot-hours = the four `bos__spend` Spark scripts in `SteelHouse/airflow-camperbid`
  (`campaign_group_flight_end_cost.py` 3,710 slot-h/7d, `campaign_flight_end_cost.py` 3,642, `sum_by_private_marketplace_by_hour.py`
  161, `campaign_utc_yesterday_costs_impressions_by_hour.py` 11) reading via `spark_scripts/utils/util_spark.py::bigquery_load_query`
  (Spark-BigQuery connector, `viewsEnabled` + `query`) on Dataproc Serverless batches from `dag_utils/google.py::run_dataproc_serverless`;
  `win_rate_hourly` 29.3 slot-h/day; airflow-ti 147 jobs/day, 3.9 slot-h/day (the `dlv_pattern_identification` CTAS; the
  `url_pattern_identification` paged reads are 972 jobs/7d at ~0 slot-h). Table: ticket §4.1,
  `outputs/audi_1278_unlabeled_jobs_by_submitter.csv`, branded xlsx on Drive `My Drive/Tickets/AUDI-1278 BQ Job Labels/`.
- **Report-side baseline (D1 = option C, user):** the daily `gs://mntn-data-archive-prod/optimizer/optimizer_bq_<date>.md`
  exists only from 2026-08-28 (first prod sweep day); its unattributed row reconciles to `JOBS_BY_PROJECT` within rounding on
  every day, 08-28..09-01 mean **606 jobs/day, 1,104.7 slot-h/day**. After = the same row re-read 7 days after each merge.
  Expected: airflow-ti PR alone −147 jobs/day, −3.9 slot-h/day; camperbid change also landed → ≈24 jobs/day and <1 slot-h/day
  left (autotof kedro job, `bvp_data_refresh_v7.py::load_bq`, the hhst_v# MERGE, 7 LOAD/day) and `bos__spend` rises
  ~1,630 → ~2,700 slot-h/day on the report with four new `bq_heavy_task` findings (attribution moving, not new spend; feeds
  AUDI-1277). No `ledger applied` stamp: labels move attribution, they do not save slot-hours, so the Mode savings number will
  not move. Option B (record the bucket as a ledger row) reverses the design and flips the pinned test; option A (Mode query
  over `JOBS_BY_PROJECT`) needs `roles/bigquery.resourceViewer` for `mode-analytics@dw-main-bronze` via mntn-devops. Decision
  record `knowledge/decisions/0007_unattributed_bq_measured_on_daily_report.md`.
- **airflow-ti side (ours): [airflow-ti#1278](https://github.com/SteelHouse/airflow-ti/pull/1278) OPEN 2026-09-03**, branch
  `audi-1278-bq-job-labels` off `825b07e`: new `include/util/bq_job_labels.py::airflow_job_labels()` (mirror of
  `BigQueryInsertJobOperator._add_job_labels`, `{}` outside a task) at 8 python-client call sites (`url_pattern_pipeline.py`'s
  three `QueryJobConfig`s, both tmobile workflows' `fetch_advertiser_ids`, three `get_df(configuration={"labels": ...})` DAGs);
  14 new tests, gate 34 passed / 2 pre-existing `REPO_ROOT` failures. Gauntlet medium tier: 6 findings, 3 refuted, 1 confirmed
  docstring style item whose auto-fix was dropped with an unrelated 22-file reformat. Jira comment posted, status In Progress.
  Dev-deployment run not done (agent may not deploy); verification = the first post-merge daily report showing
  `url_pattern_identification` / `dlv_pattern_identification` / `tmobile_blocked_*_export_dataproc` rows.
- **camperbid side (D2, owner hand-off, no PR by us):** two Spark properties in `dag_utils/google.py` (`run_dataproc_serverless`
  `runtime_config.properties` and `DataprocConfig.asJson` `pyspark_job.properties`):
  `spark.datasource.bigquery.bigQueryJobLabel.airflow-dag = {{ dag.dag_id | lower }}` and
  `...airflow-task = {{ task.task_id | lower | replace('.', '-') }}`; both dicts are template fields already rendering
  `{{ dag.dag_id }}`, every current task id fits 63 chars (longest `tables-campaign_utc_yesterday_costs_impressions_by_hour-create`,
  62). Covers every connector read (`bigquery_load_query`, `_v2`, `bigquery_load_table`, win_rate_hourly's private
  `_read_bigquery`) with no script edits. Send-draft + diff + owner dev check:
  `tickets/audi_1290_pipeline_optimization_hackathon/audi_1278_bq_job_labels/artifacts/audi_1278_camperbid_handoff.md`,
  recipients `@SteelHouse/pacing` + `@SteelHouse/performance-ml` (CODEOWNERS `*`), user sends. Camperbid runs Astro runtime
  3.2-5, prod SA `airflow-camperbid-prod@mntn-prj-prod-00`, dev SA `airflow-camperbid-dev@mntn-prj-dev-00` (dev jobs also bill
  in dw-main-bronze); `OPTIMIZER_BQ_SAS` lists prod SAs only, so dev validation queries `JOBS_BY_PROJECT` directly.
- **Open after this session:** the hand-off is unsent; the three `google_cloud_default` `get_df` DAGs produce zero
  dw-main-bronze jobs (which project they bill to is unknown; the optimizer never sees them); the
  `tests/test_url_pattern_pipeline.py` `REPO_ROOT = parents[2]` fix = IMP-105. Detail: [[reference_bq_job_attribution]],
  [[reference_airflow_ti]], [[reference_mode_api]].
