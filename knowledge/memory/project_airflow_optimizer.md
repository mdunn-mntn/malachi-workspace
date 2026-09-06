---
name: project_airflow_optimizer
description: AUDI-1194 airflow_optimizer/ — key-free Spark efficiency crawler, live as the spark_optimizer_daily DAG in airflow-ti; sprint 8649 (AUDI-1269/70/71/72/73/74/75/76/77/78/79/80/81 + mid-sprint AUDI-1316/1317, 2026-09-02/09-03) parallel hackathon execution yielded 13 PRs across airflow-ti/camperbid/shopper_graph — MERGE TRAIN SHIPPED 2026-09-03: all 11 airflow-ti PRs merged and each individually deployed (wait for DEPLOYED between merges, ~4-8 min per Astro build), plus shopper_graph #305 and, earlier the same day at 18:22 UTC, PR #1282 (AUDI-1317, squash e9cb5b9); only airflow-camperbid #580 left, blocked on Tony Chen's team; PR numbers / AUDI numbers / branch names are OFFSET (PR #1273 = AUDI-1269), always resolve the worktree via gh pr view --json headRefName, never the number; the four JSON-regenerating PRs (#1273/#1275/#1281/#1271) needed merge-order sequencing — each regenerates dags/model_task_config.json from base 825b07e and requires rebase + fresh model_upload.py --dryrun after the prior merge; merge order + offset table in [[reference_airflow_ti]]), one ticket (AUDI-1271) closed no-PR by design (spec breached kill criterion), one shipped no-code (AUDI-1316, Mode sections), session-limit recovery ×2 + hung-agent recovery ×1; MEASURED SAVINGS TO DATE ARE ~$5, NOT ~$900 — 19.4 exec-h from fangorn #1231 on ONE observed day at $0.28/exec-h; Mode's $0 is a rounding artifact of a real ~$5, not a broken ledger; the two #1231 disk_spill findings are still `watching`, never `resolved`, and are chronic again; the ledger also only applies its stamp after a finding resolves (3+ quiet sweeps), so a merged fix credits nothing until its finding goes quiet; the BQ surface is scoped by SERVICE ACCOUNT not by team (OPTIMIZER_BQ_SAS = airflow-ti-prod + airflow-camperbid-prod; the Spark surface excludes other teams by phs.TEAM, the BQ surface never did) which is why the sweep flagged camperbid and opened #580 — by design, not a leak; 2026-08-26 gained an executor-hour cost unit, Databricks dollar costing from system.billing, and Block Kit Slack delivery to #spark-optimizer; 2026-08-27 PR #1230 (Slack wire + cumulative savings log) and PR #1231 (fangorn shuffle partitions 2048) both merged, fangorn applied marker written to the prod ledger; full-corpus hackathon sweep (3,085 logs -> 67 pairs, 30,163+ exec-h) filed as AUDI-1241 under epic AUDI-1054; site_network_hourly + DDP dbt tests now ours (merged #1232, dbt#174 in review); 2026-08-28 PRs #1241-#1243 merged (#1244 open), BQ external table optimizer.optimization_ledger + Mode dashboard e81786de8403 live, first measured saving fangorn #1231 575.6 exec-h/day (~$58.4k/yr est); 2026-08-28 (later) #1245 merged — BQ profiler (bq_profile.py via JOBS_BY_USER), per-surface ledger (surface spark|bq|dbx), Databricks findings, billing surface_rates; pod profiler blocked on Astro metrics exporter; 2026-08-28 evening first live multi-surface sweep found the identity bug (sweep runs as spark-optimizer@ but billing+BQ grants target airflow-ti-prod@), fix PRs airflow-ti#1247 + mntn-devops#5160 open, pod-metrics relay filed as DEV-8821; 2026-08-29 both fix PRs MERGED, live BQ surface verified (optimizer_bq report + surface=bq ledger rows) and billing rate live-blended from 30d actual spend ($0.278/exec-h, no env fallback), Jira SA request ITS-6496 pending; 2026-08-31 hackathon refinement — 13 sprint tickets AUDI-1269..1281 filed into sprint 8649 grouped by change type (16 SP Malachi, 4 SP others), savings provenance = ledger applied stamps + daily PR-vs-ledger reconcile; 2026-08-31 evening — epic AUDI-1290 parents the 13, PR 1250 open (Databricks surface via SP OAuth REST; dormancy root cause = report() silently empty without DATABRICKS_WAREHOUSE on prod), PR 1252 open (gcs console links + OPTIMIZER_NAME_OVERRIDES), DEV-8821 relay LIVE; 2026-09-01 — PRs #1250/#1252/#1253 MERGED + LIVE on prod image deploy-2026-09-01T19-06-22 via retrigger PR #1254 (Astro superseded-build gap), dbx REST surface engaged (prod secret pairs prod_runner; blocked on system.lakeflow/system.query SELECT + warehouse CAN USE grants), env vars staged, DEV-8821 relay FULLY live end to end; 2026-09-01 (later) — '39 unprofiled' digest complaint decomposed (7 paused DAGs miscounted: ORM paused read forbidden on Astro tasks, REST fallback PR #1255 open; only 1 DAG cost-covered; chip reworded to 'N DAGs without cost data'), OPTIMIZER_NAME_OVERRIDES live on prod (14 source-verified entries, ETL Audience Intent excluded), pod surface PR #1257 open (pod_profile.py, core-hours/day, blocked on mntn-devops #5224 + OPTIMIZER_POD_PROJECT), Mode BQ cost table added via API; 2026-09-01 (evening) — #1255+#1256+#1257 COMBINED into PR #1258, MERGED + LIVE on deploy-2026-09-01T22-22-40, devops #5224 merged (monitoring.viewer synced), OPTIMIZER_POD_PROJECT=mntn-prj-prod-00 set; first optimizer_pod report published (sweep manual__22:36) but numbers wrong — Cloud Monitoring v3 timeSeries.list returns points NEWEST FIRST, rate went negative->0 — fix PR #1259 verified live (dag-processor 55% of cpu limit, worker-default 0.875 cores = 11% of 8, downsize candidate); downloader freeze root-caused (gsutil -m forked workers die quietly on the 0.25-CPU pod; ~2/192 logs landed every sweep since 08-28, 'Done' exit, resolution frozen 6 sweeps) — fix PR #1260 threads-only -m via GSUTIL_OPTS in fetch.py; 12-day full-history diagnosis written (outputs/audi_1194_diagnosis_2026_09_01.md: downloader freeze root of most regressions, dbx surface 0 rows ever, debugger/optimizer fleets near-disjoint); review queue #1259+#1260; 2026-09-02 — ledger unattributed BQ bucket verified EMPTY (every measured job labeled airflow-dag/airflow-task, no team-labeling campaign; 35 no-cost-data DAGs close via dbx grants / per-DAG event logging / genuinely-no-compute), PR 1260 retitled downloader + parse-rate canary, Alyson has the prod_runner grants paste (incl. warehouse Can-use), digest user-verified from screenshots (rank-row alignment reformat queued); 2026-09-02 (overnight) — digest rank rows fixed via rich_text ordered list (PR 1260 commit dd53939, user-confirmed preview), OPTIMIZER_NAME_OVERRIDES 22 entries live (was 14) with trailing-wildcard prefix keys in coverage.resolve (commit 3d87c6f), unlinked-apps question CLOSED (audience_intent / tpa_ipdsc_export / targeted_signal_crm launchers source-verified), PR 1260 = 3 commits retitled 'downloader loses the batch; parse canary; digest numbered list', new open question: flagged apps' spark-events logs vanish from GCS within hours; 2026-09-02 (afternoon/evening) — gsutil itself BANNED on Astro pods (every mode lands ~2/194, falsifies the threads-only fix), downloader rewritten on the GCS JSON API (PR 1263 insufficient, PR 1264 merged, deploy-2026-09-02T19-27-09), 19:35 UTC sweep complete=True 346 jobs, 41+ resolutions flowed, six-day freeze CLOSED; system.billing granted and dbx cost report live (top row Generate Graph & Metrics - PRODUCTION, 10,528 DBU, $1,579 list/7d); dbt 174 provenance stamp closed unmeasurable (job never entered the ledger); PRs merged today 1259 1260 1262 1263 1264; Jira comments 614410 + 614725; canary first live run = 09-03 17:00 UTC daily; 2026-09-02 (late night) — fangorn fix #1231 did NOT hold: first complete sweep shows 659.2 exec-h with stage-17/19 disk-spill CHRONIC again, the ~$900 cumulative savings was a blind-window artifact, Mode headline honestly $0 (after-rate 806.8 avg > before 687.7, GREATEST clamps to 0; per-finding wide-shuffle credit 28.5h stands); spill re-fix = top hackathon candidate; 2026-09-02 (hackathon execute wave) — AUDI-1274 landed on branch audi-1274-aqe-advisory-pivot (advisoryPartitionSizeInBytes=16m in both guid pivot builders, dry run diff-clean, Jira comment posted; shipped as PR #1270, MERGED 2026-09-03 19:44 UTC squash ca3b9e4): the spec's spill mechanism was wrong, the 800-task floor is registered cores at plan time (parallelismFirst), not the 64 MiB advisory size; remedy unchanged; 2026-09-03 — AUDI-1273 landed (workspace 0d7a3d02, branch audi-1273-max-partition-bytes; shipped as PR #1272, MERGED 2026-09-03 19:47 UTC squash 370f2bd): maxPartitionBytes 64 MiB on ipdsc_ds_49 and 256 to 128 MiB on conv_log_derived_ip, ipdsc_ds_67 dropped because its DS4 input is 160 x 60 MiB single-row-group parquet the knob cannot split (keys disk_spill:3/:5 to wont_fix, broadcast fix = IMP-102), ledger hand-edit ops recipe recorded, conversion_log archive volume drop 08-20 flagged (IMP-103); 2026-09-03 AUDI-1275 landed, speculation safe by source for 11 of 13 straggler DAGs (FileOutputCommitter v2 under the commit coordinator, Iceberg), canary PR #1271 (= AUDI-1275, the offset) on site_network_hourly, Ryan reviewer, MERGED 2026-09-03 20:20 UTC squash b9428f4 with his skew caveat as the kill criterion, manifest pair owner-gated, the 08-27 ipdsc_ds_35 refutation contradicted on the record; 2026-09-03 AUDI-1278 landed: the unattributed BQ bucket (612 jobs, 1,110 slot-h per day) is fleet-SA jobs the ledger skips by design, 96.9% camperbid bos__spend Spark-BigQuery-connector reads (the 09-02 'outside the airflow-launched set' hypothesis refuted), airflow-ti PR #1278 MERGED 2026-09-03 (labels at 8 python-client sites, 147 jobs/day), camperbid two-Spark-property hand-off drafted for pacing/performance-ml (route to Tony Chen, who owns camperbid/pacing now that Forrest Bajbek has left), measurement surface = the daily optimizer_bq report (exists from 08-28; baseline 606 jobs / 1,104.7 slot-h per day), decision 0007. AUDI-1194 itself closed Jira Done/Done 2026-09-03; this memory stays `active` because the DAG runs in prod daily and the AUDI-1290 hackathon work continues against it.
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [airflow optimizer, AUDI-1194, spark_optimizer_daily, airflow-ti 1212, spark-optimizer service account, serviceAccountTokenCreator, CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT, airflow session use is forbidden, spark optimization crawler, efficiency sweep, eventlog parser, 7-surface spark, optimization detectors, skew spill shuffle, fleet crawl backlog, daily optimizer cron, oncall_daily_optimizer, com.mntn.daily-spark-optimizer, phs event logs, phs.fetch_logs, dataproc-debug pam, audi-storage-object-view, 242x skew, dataproc databricks optimization, straggler detector, idle_reserved_executors, shuffle_fetch_wait, map-side concentration, site_network_hourly stage 9, optimization ledger, optimizer coverage gap, optimizer digest, sweep.py, ledger.py, coverage.py, digest.py, workload identity runner, EXPLAIN COST statement execution api, jobs get-run-output empty, IMP-029 rolling dirs, savings log, optimizer_savings, OPTIMIZER_USD_PER_EXEC_H, PR 1230, PR 1231, fangorn_score_monitor shuffle partitions 2048, speculation revert ipdsc_ds_35, ledger applied marker, hackathon optimizations, AUDI-1241, AUDI-1054 tech debt epic, full-corpus sweep 3085, PR 1232, dbt 174, ddp dbt tests ownership, site_network_hourly ours, PR 1241, PR 1242, PR 1243, PR 1244, adv_score event logs, SLACK_FALLBACK_CHANNEL, gsutil unauthenticated astro pods, gcs json api markers, rapid dag pause race, AIRFLOW_BEARER PATCH is_paused, mntn-prj-prod-00 optimizer dataset, optimization_ledger external table, pam breakglass-editor, pinned schema autodetect applied_date, use_legacy_sql false backticks, mode dashboard savings, e81786de8403, fixlog.py, optimizer fix log playbook, savings semantics calendar day, fangorn savings 575.6, AUDI-1249, priority rubric playbook, nonspark phase plan, PR 1245, bq_profile.py, JOBS_BY_USER, ledger surface field, surface spark bq dbx, surface_rates, OPTIMIZER_USD_PER_SLOT_H, dbx_heavy_job, dbx_failing_model, optimizer_bq report, astro universal metrics exporter, gcp managed prometheus, pod profiler blocked, savings by surface, ignoreUnknownValues, PR 1247, mntn-devops 5160, billing grant wrong identity, OPTIMIZER_BQ_SAS, hermetic sweep tests, DEV-8821 pod metrics relay, otel collector cloud run, ITS-6496 jira service account, blended billing rate, DCU-h, optimizer_bq_2026-08-29, airflow rest logs continuation_token, mode report runs refresh, hackathon sprint 8649, AUDI-1269 1281 sprint tickets, change type grouping one ticket per change, ledger applied provenance, daily pr ledger reconcile, bos__spend slot hours, intent_score_threshold_v4 slot hours, unattributed bq jobs, PR 1250, PR 1252, DATABRICKS_WAREHOUSE, DATABRICKS_GCP_CLIENT_ID, dbx dormancy silent empty report, ml_squad warehouse main workspace, prod_runner 397d710b, OPTIMIZER_NAME_OVERRIDES, astro-metrics-relay live, pod surface, dbt 174 baseline 306352 query-s, AUDI-1290 hackathon epic, AUDI-1302 wont do, PR 1253 merged, PR 1254 retrigger, deploy-2026-09-01T19-06-22, prod_runner grants blocker, dbx insufficient permissions, IMP-097 owner mapping, PR 1255, PR 1257, pod_profile.py, OPTIMIZER_POD_PROJECT, mntn-devops 5224, paused rest fallback, coverage invisible chip, 39 unprofiled decomposed, name overrides 14 entries live, hashed-email ds 22 29, core-hours per day, cpu-overprovisioned, memory-pressure, opt-bq mode section, 3ead7301daa8, rich_text_list ordered digest, name overrides 22 entries, wildcard prefix override keys, audience_intent prod launcher, tpa_export_spark_batch export_tpa, targeted_signal_crm, spark-events logs vanish hours, jira comment 614410, dd53939, 3d87c6f, PR 1263, PR 1264, gsutil banned astro pods, json api downloader rewrite, deploy-2026-09-02T19-27-09, freeze closed 41 resolutions, 346 jobs full corpus, system.billing granted, dbx cost report live, ddp vertical classification 10528 dbu, jira comment 614725, dbt 174 stamp unmeasurable, retry git deploy, fangorn fix not held, savings withdrawn zero, blind window resolved artifact, 659.2 exec-h, GREATEST clamp savings, resolved during visibility gap, AUDI-1274, guid pivot AQE floor, advisoryPartitionSizeInBytes 16m, disk_spill:33, disk_spill:34, spec mechanism corrected, audi-1274-aqe-advisory-pivot, AUDI-1273, audi-1273-max-partition-bytes, maxPartitionBytes, ipdsc_ds_49, ipdsc_ds_67, conv_log_derived_ip, disk_spill:1, disk_spill:3, disk_spill:5, ledger set wont_fix, STICKY states, owner_notified, ledger applied stamp, ledger hand edit window, sweep 09:00 UTC daily, ledger restored from gcs each sweep, single row group parquet, split probe, IMP-102 broadcast ds67, IMP-103 conversion_log volume drop, RESOLVE_SWEEPS 3, AUDI-1275, spark.speculation true, straggler gcs writers, canary site_network_hourly, PR 1271, OutputCommitCoordinator, FileOutputCommitter v2, manifest committer owner-gated, speculation contradiction, straggler detector fetch-wait blind spot, AUDI-1278, PR 1278, audi-1278-bq-job-labels, bq job labels, unattributed bucket settled, unattributed 612 jobs, bos__spend connector reads, spark-bigquery connector no labels, bigQueryJobLabel, spark.datasource.bigquery prefix, airflow-camperbid dag_utils google.py, run_dataproc_serverless properties, DataprocConfig asJson, camperbid handoff pacing performance-ml, airflow_job_labels, bq_job_labels.py, get_df configuration labels, optimizer_bq report from 2026-08-28, baseline 606 jobs 1104.7 slot-h, measurement option C daily report, mode resourceViewer option A, google_cloud_default get_df, IMP-105, decision 0007, merge train shipped, 11 prs merged deployed, deploy between merges, astro build 4 to 8 minutes, pr number ticket offset, gh pr view headRefName, savings 5 dollars not 900, 19.4 exec-h one day, 0.28 per exec-h, mode zero is rounding, findings still watching not resolved, OPTIMIZER_BQ_SAS service account scope, phs.TEAM spark surface only, camperbid 580 blocked, tony chen camperbid pacing, forrest bajbek left, swapnil patil, ryan kleck speculation caveat, speculation adds executors under skew, canary kill criterion executor-hours vs wall-clock, AUDI-1194 done, AUDI-1326, PR 1286, grace window defect, 28 false resolved rows, seen_dates no date filter, apply_manifest slides the window, RESOLVE_SWEEPS counts dates not sweeps, retry phantom resolved rows, mark_applied fabricated exec_h, savings MIN_OBSERVATIONS, 90% Welch interval, delta drops recurring, 51 of 186 entries dropped, savings 115 hours 32 dollars unsound, daily_h last-write-wins, forward-only fix, audi-1326-ledger-savings-correctness, AUDI-1329, coverage measured, 28 percent fleet coverage, 763 spark task instances per day, fleet denominator, 194 of 200 downloads failed, phs.list_batches returns empty, silent degraded sweep, MAX_BATCHES coupled, MAX_BYTES 4GiB, 8 detectors never fired, analyze_plan zero firings, no Input Metrics detector, driver prologue idle 754 exec-h, small file amplification, sub-second task swarm, zstd.inprogress 453, GHFS_SYNC_TMP_FILE, sweep window 28-40 hours, hour-of-day bias, report date one day behind, unlabeled batch defaults to ti, databricks opacity 26 percent, cluster_log_conf, DATABRICKS_PROFILE, operator map wrong, retention is not the ceiling]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-09-04
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
  *(REFINED 2026-09-03, appended not overwritten — a closer read of the ledger: the measured
  saving to date is **~$5**, being **19.4 exec-h** credited to #1231 on **ONE observed day** at
  **$0.28/exec-h**, so **Mode's `$0` is a ROUNDING artifact of a real ~$5**, not purely the clamp.
  The two `disk_spill` findings from #1231 are still **`watching`** — they never reached
  `resolved` — and are chronic again. Both readings agree on the substance: there is no ~$900.
  Settling check: read the ledger's per-key state and applied-day count directly, not the Mode
  headline.)*
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

## 2026-09-03 — AUDI-1277 landed; the 2,300 slot-h/day figure was double-counted in the profiler; 31% histogram savings proven, skip gate halves the heaviest rebuild

**Profiler double-count (PR airflow-ti#1277 merged):** `include/spark_optimizer/bq_profile.py` PROFILE_SQL was summing `total_slot_ms` and `total_bytes_billed` over every `INFORMATION_SCHEMA.JOBS` row, including both a script parent (already summed) and its constituent statements. When profiling `campaign_summary_hourly-create` (DELETE+INSERT script): parent SCRIPT = 151.7 slot-h, DELETE child = 0, INSERT child = 151.7 → uncorrected sum = 303.4 slot-h (2× high). Filter `parent_job_id IS NULL` takes only top-level jobs. True 2026-09-02 load after correction: `flight_metrics_per2388-create` 945 slot-h / 1,351 TiB; `population_histogram` 528 slot-h / 51.5 TiB; `campaign_summary_hourly-create` 517 slot-h / 18.6 TiB. The ticket's original 2,300 slot-h was 1.6-2× high for two of the three tasks; the sum ≈ 1,990 slot-h. The Mode report (query token `3ead7301daa8`) reads `optimization_ledger` not `INFORMATION_SCHEMA`, so the fix flows through with no Mode change; when this deploy lands the `opt-bq` table drops ~40% for these two tasks with no real saving. Baseline correction labeled and recorded.

**flight_metrics_per2388 skip gate (PR airflow-camperbid#580 open):** The task re-computes a 14 TiB all_facts scan 96 times per day while all_facts changes ~13 times per day (writer cadence ~2h, 72h lookback = 80-90% of runs recompute an unchanged result). A `@task.short_circuit(ignore_downstream_trigger_rules=False)` gate checks two metadata-only reads (all_facts partition max-modified-time via INFORMATION_SCHEMA, `dso.campaign_group_flight` active-flight set via a full-table fingerprint) and skips drop+create on match. Flight churn (starts/ends every quarter-hour on weekdays, 170-400 events/day, 41k on month-start) caps the gate at approximately 50% skip rate on weekdays (48% on 08-31, 28% on rollover); an hourly-schedule move is the fallback (D2 alternative). Expected: 96→~50 runs/day, 945→~450-500 slot-h/day, 1,351→~650-750 TiB/day.

**population_histogram INT64 dedup key (PR airflow-camperbid#580 open, confirmed on pinned 1h A/B):** The histogram stage spends 46% of slot-time shuffling on a STRING `ip` dedup key; replacing `FARM_FINGERPRINT(ip)` (INT64) + `FARM_FINGERPRINT(household_id_value)` for the flag-on household variant saves 31% slot-time with identical output (checksum and row count verified; 64-bit collision risk <1e-9 over multi-billion keys). Measured pinned 2026-09-02 12:00-13:00 UTC: baseline 14.52 slot-h / 195.5s wall; candidate 10.01 slot-h / 214.1s wall (wall +10% due to lower peak concurrency on the shared `adhoc` reservation, not shape).

**campaign_summary_hourly attribution (hand-off to Data Platform):** 76% of the run is the `spend_pacing` VIEW's own logic (external.impression__v1 read 3×, bidder_win_notifications, 17 integrationprod dims), re-evaluated 96×/day for a 24h sliding window + 4-day CIL lookback; only materializing the view (SQLMesh INCREMENTAL_BY_TIME_RANGE, owner `ber`) removes it. The 24h live window is a billing-safety buffer (Lizz, 2026-06-08) and the 5d CIL lookback was raised for correctness (PER-6212); do not shrink either. Artifacts: `artifacts/audi_1277_sqlmesh_ask.md` for #data-platform, and the stage attribution from the saved plan in the ticket (outputs/audi_1277_plan_csh.json, 259 stages, 6.87 slot-h).

**Measurement & Mode reconciliation:** airflow-ti PR #1277 baseline correction (40% drop for two tasks with no real saving); camperbid PRs (skip gate + histogram + spend_pacing ask) to Pacing/Performance-ML; all three changes fire `bq_heavy_task:*` findings until resolved after the gate/fix settle (ledger `applied` state records the PR for post-merge validation). See [[reference_bq_job_attribution]]; detail in AUDI-1277 ticket.

## 2026-09-03 — AUDI-1269/1270 landed; six DAGs in-flight at the sizing rule, one more underway, nine routed to input-stage lever

**The two tickets split work cleanly on one finding (AUDI-1269 pre-verified and shipped early; AUDI-1270 verify then size).**

**AUDI-1269 "Raise shuffle.partitions on 10 pre-verified spill DAGs" (airflow-ti#1273 MERGED):** 6 of 9 DAGs edited, 3 pulled by event-log gate.
- **Shipped (6 DAGs): ipdsc_ds_2 2048→8192, advertiser_score_distribution_monitor 128→916 (decorator+builder), conversion_log/site_visit_signal/guid_log 1000→3508/3392/3400 (builder-only inserted after `parquet.block.size`)** and ipdsc_third_party_audience_builder 512→2240 (decorator). Before: shuffle stages reduced spilled 218-814 GiB per run. After: 3+ quiet sweeps to `resolved` per ledger keys (disk_spill on reducer, shuffle_partition_sizing, applied stamp per PR#1273). Config regenerated (3-hunk dags/model_task_config.json), dryrun clean, 145 model tests pass, CI green. No comments added anywhere. Released to prod with builder values on first run (`.py` syncs live), decorator values after Astro bundle redeploy (up to 12 h).
- **Pulled (3 DAGs): intent_score_map (gate c: blocks 4.9/5.8 KiB below 8 KiB floor; map-side spill 1,320/4,262 GiB on stages 2/3 = AUDI-1273 mechanism), prospecting_join (gate b: no reducer spill on 09-01, already 287-438 MiB per task, 438 MiB < 512 MiB trigger = finding resolved by construction 2026-09-02), household_score_distribution_monitor (decision 1: owner question to Ryan on the 8,896 target vs the prior 512 after driver-OOM incidents).**

**AUDI-1270 "Verify event logs then raise shuffle.partitions on 15 spill DAGs" (airflow-ti#1275 MERGED, with defects corrected by the dispatcher):** 1 change, 2 conflicts, 3 no-spill, 1 code, 9 to AUDI-1273 mechanism.
- **Change (1 DAG): vertical_size_monitor 128→600 (decorator+builder, config regen, 145 tests pass)**. The DAG's two reducers read 25.6 GiB of shuffle and spill 136 GiB in memory (in-memory formula: 544→600, 232 MiB per task, 44 MiB shuffle read, sticks under AQE 32 MiB floor, 2.6x under 614 MiB worst-case budget where the model caches a dataframe). Post-merge ledger: 2 `disk_spill:*` keys to `resolved` after 3 quiet sweeps.
- **Conflict (2 DAGs): guid_log_advertiser_id_dsc_id stages 13/24 compute to 4100/3700 by in-memory bytes vs AUDI-1269's 3400 (sizing stages 5/16). Decision D1 (user, 2026-09-02): AUDI-1269 lands first at 3400, AUDI-1270 re-sizes stage 13/24 to 4100 ONLY if spill persists in the post-merge ledger (303 MiB per task at 3400 is well under budget, expected to stop spill); if resolved, nothing changes.**
- **No-spill (3 DAGs): ipdsc_ds_14, guid_log_pivot_household_id_vertical_id, aug_log_ip** (last 3+ runs spilled 0 GiB above the 2 GiB detector floor). Nothing to do.
- **Code (1 DAG): ipdsc_ds_47** spills in map-side partial aggregation over BigQuery read streams (1952 tasks, 681.9 GiB in memory, the knobs do not reach that stage). Code ownership via AUDI-1275.
- **Input-stage (9 DAGs) → AUDI-1273 mechanism (maxPartitionBytes on the input read): fangorn_prospecting_scoring stages 13/14, ipdsc_ds_17 stage 4, ipdsc_46/14/49_monitor stages 10/11, ipdsc_ds_13 stage 1, fangorn_predictions_vertical/household stage 2/1, advertiser_join stage 3.** Per-DAG hand-off with input GiB, task count, MiB per task, expansion factor, and current default/set maxPartitionBytes; 13 stage rows mapped to 9 unique DAGs (two monitors and one DAG appear 2x).

**Sizing rule settled:** `target = max(shuffle_read_bytes, mem_spill_bytes) / 256 MiB` rounded to next 100. The in-memory bytes are load-bearing because rows can expand 5.3x on string-heavy stages (the shuffle-bytes-only formula misses half the needed sizing). Gate checks: (1) projected blocks >= 8 KiB; (2) per-task bytes >= 32 MiB (AQE coalescing will not merge); (3) driver map-status memory <= 1 GiB (INC-018 ceiling at 5000 partitions on 9600m driver). Record `knowledge/memory/reference_dataproc_eventlog_profiling.md` (new lines on the sizing rule, coalescing floor, code constant caveat, driver ceiling, BigQuery/Iceberg writer mechanics).

**Cross-ticket rule:** when two tickets touch the same file for the same knob, one owns it (AUDI-1269 on guid_log stages 5/16) and the other records the delta for post-merge re-size or re-decision if spill persists.

## 2026-09-03 — AUDI-1271 refuted initialExecutors as a lever on aug_log; AUDI-1272 changed 2 of 10 DAGs; AUDI-1281 POC guard + ledger facts

**AUDI-1271 refutation: raising initialExecutors buys only idle cost, not stage throughput (CLOSED 2026-09-03 with NO PR and no change — no `audi-1271` branch or PR exists. The "airflow-ti#1271 PR open" written here originally was the numbering offset: PR #1271 is AUDI-1275's speculation canary).** The spec raises `spark.dynamicAllocation.initialExecutors` 100→200 on aug_log_ip_vertical_id_hourly to eliminate stage 11 fetch wait (31-46% of stage run time, 0.03-0.13 executor-hours). Evidence on 20 profiled runs (2026-08-31 to 2026-09-03 UTC): (1) The initial fleet of 100 registers in 11-73 s, then is trimmed to minExecutors (50) at +60 s flat in every run that trimmed before map (60s = the `executorIdleTimeout` default). (2) Map stage 7 started on 50 executors in 12 of 20 runs (prologue 92-2,306 s); on the other 8 runs the intact 97-100 survived to map. (3) **With initialExecutors=200 the same 60s trim applies**, so the 200 would cost +1.67 executor-hours per run (200 x 60s / 3600) and stage 11 waits only 0.03-0.13 → **fails §0 kill criterion by 12-56x at the medians**. (4) The real cost is the driver prologue: runtime `pip install tldextract` idles 50-100 executors for 23s-38min per run (44% of the job's executor-hours on average, 90%+ on the three heaviest runs), about 58 DCU-hours per run. Decision rule (§4.3): `(b)` fetch wait << `(a)` idle cost in every run, so shipped initialExecutors=200 alone, no `executorIdleTimeout` line. Result: the PR diff is ready in worktree, user decides merge for the ledger record (expect `fix_not_working` after grace window) or close no-change. Records §7: the 60s idle trim, the DCU-h per executor-hour fit (10.24 marginal), the runtime pip install as the cost driver, the `aug_log_rollup` shared label with aug_log_ip_hourly (batch name filter needed), and JSON API fetch as the gsutil -m workaround. CONTRADICTION appended (§3.4): `aug_log_ip_hourly` also shows a flat trim pattern, but 21 of 22 AUDI-1272 logs had zero removals and were busy from task 1, so the trim is specific to jobs with task-free prologues. Follow-up: bundle tldextract in `utils_model.zip` or as a wheel on `ti_resources` for aug_log and site_network_hourly (IMP-106 added).

**AUDI-1272 verdicts: 2 of 10 confirmed, 8 not confirmed (PR airflow-ti **#1281**, branch `audi-1272-initial-executors-verify-first`, MERGED 2026-09-03 20:12 UTC, squash `cd353d7`, deployed. The "#1272" written here originally was the numbering offset — PR #1272 is AUDI-1273).** The spec checks whether each of 10 DAGs' fetch-wait stage has map output sitting on the few executors the job started with, then raises initialExecutors if confirmed. Executed on 22 logs (2 per DAG): (1) **advertiser_mid confirmed, stages 8/9/19 fed by map stages that ran on 25 initial executors while the run reaches 90; initialExecutors 90** (the peak the run registers, capped at maxExecutors 90). Cost: 0.23 executor-hours boot. (2) **ipdsc_42_monitor confirmed, stages 18/22/26 fed by a 20-task map on 2-3 executors while the run reaches 7; initialExecutors 7** (peak, constrained by `executorAllocationRatio 0.3` on small stages). Cost: 0.03 executor-hours boot. (3-10) **Not confirmed (8 DAGs):** six already spread the output over every executor the run registers (tpa_export_enrich, tpa_mntn_id_export, both fangorn models, site_visit_signal, guid_log), two fail the cost rule (ipdsc_ds_46 wait 0.14 executor-hours, extras cost 0.43; vertical_size_monitor wait 0.12, extras 0.33; both already trimmed or at their peak). Parallel finding on the 10-DAG set (§4.5, §4.6): **Serverless scale-up latency 100-160s after the first backlog, and every stage submitted inside that window runs on the starting count alone.** Facts: starting fleet registers 9-20s after app start; first dynamic-allocation wave lands 100-160s after backlog arrival. `executorAllocationRatio 0.3` on every log (Serverless default) explains why the two monitors never exceed 7 executors (0.3 x 20 tasks / 4 cores ≈ 1.5, so 7 is headroom). Skipped-stage twin resolution: Spark re-labels an already-computed shuffle stage per job, so the executed twin is the stage with the same top `RDD ID` in `SparkListenerJobStart Stage Infos`; the old time-heuristic picked a 0-byte stage as the feeder on ipdsc_42_monitor. Test change: the profiler artifacts will be committed but SVG logs in `outputs/eventlogs/` are git-ignored so 22 event logs stay on disk (440 MB, under repo). Records §7: the Serverless allocation latency, the executorAllocationRatio 0.3 default, twin-stage resolution by parent ID, the fangorn models' 600s/3600s idle timeouts, gsutil -m stall + JSON API fetch, batch createTime→app-id resolution by minute-prefix + first 256 KiB app name, CSV outputs being git-ignored (`gitignore` L2 and L111), and a CSV deliverable needing a .md or .txt twin.

**AUDI-1281 POC: a regression guard that fails CI on a 2x stage regression vs 30-day baseline (airflow-ti#1281 PR open).** Built a stage-metrics ledger (per-stage per-run, restored and re-published each sweep), a regression guard CLI with two inputs (`--from-logs` on raw event logs, `--metrics <jsonl>` on the sweep-persisted file), and a test suite (189 tests, all green, no comments). Design findings: (1) **Stage identity is not `stage_id`**: `intent_score_map` submits two big shuffles concurrently so their ids swap between runs (4 of 18 complete runs, ids 2 and 3 trade places), causing a fixed-id rule to fire falsely at every swap. Guard now matches by (operation name, task count) first, id second, which survives the swaps and reports the match kind per line. (2) **Ledger `exec_h` is per-sweep-day sum, not per-run**: the 283.7 exec-h for 09-02 is 138.3 (09-01 run) + 145.4 (09-02 run), making a per-run baseline impossible. Baseline must come from event-log rows, not the ledger. (3) **Run-to-run CV on intent_score_map**: disk spill 0.02/0.15/0.03 on the three gated stages (17 runs); site_network_hourly fetch-wait 0.32 on stage 9 (5 runs); all under 0.5 kill criterion, so fixed 2x is right. (4) **Seeded-regression verdicts**: 2.0x fails (exit 1), 1.5x passes, both real runs pass (0 regressions), confirmed on both pipelines. Window coverage: intent_score_map 29 days (08-05..09-02, 31 rolling dirs, one per day plus two retry days), site_network_hourly 5 runs under the 700MB download cap. Records §7: stage-id instability from concurrent submission, ledger `exec_h` semantics (per-sweep-day sum, not per-run), intent_score_map 40000/14000-task stage swaps, CV > 0.5 adaptive threshold (the adaptive branch is implemented but the real CVs all pass), `zstd` multi-frame trap and streaming parse, the two code defects fixed in the same branch (ruff.toml glob scope, test_phs mock stale after the JSON API rewrite). Follow-up (§8 D1): sweep-side auto-gate on every DAG with >= 5 runs (no new credentials), or PR-side gate via workload identity (requires DevOps allow-list). First prod metrics file lands after the next 09:00 UTC sweep post-merge.

## 2026-09-03 — AUDI-1316/1317 measurement agreement confirmed; regression publisher 278 fleet pairs gated, ships ungated

**AUDI-1316 Mode dashboard query validation:** unowned BigQuery bucket measured independently 2026-09-03 with the bq_profile definition (`dw-main-bronze` `region-us-central1` `INFORMATION_SCHEMA.JOBS_BY_PROJECT`, `user_email` filter, zero `airflow-dag`/`airflow-task` labels) on single-day windows (1d 0.178 GB billed, 10.6 slot-s wall) and three-day reconciliation (08-31 592 jobs / 1,009.8 slot-h; 09-01 620 / 977.6; 09-02 600 / 958.6) against AUDI-1278's daily-report baseline. Both overlapping days (08-31, 09-01) match AUDI-1278's figures exactly, so the two independent measurements agree; the three-day mean (604 jobs, 982 slot-h) differs 11% from the seven-day mean (612 / 1,109.9) only because the day sets differ, not a definition mismatch. Mode query drafted and ready for paste into report `e81786de8403` opt-bq section. No grant needed (`mode-analytics@dw-main-bronze` already holds `bigquery.jobs.listAll`); the grant YAML standby exists for a terraform-drift 403. See [[reference_bq_job_attribution]], [[reference_mode_api]], decision 0007.

**AUDI-1317 regression publisher mechanism:** the daily sweep runs `regression_guard.evaluate()` over every profiled DAG, mints findings with detector `regression_<metric>` and stage-scoped keys (e.g., `regression_disk_spill:3`), and folds them into the existing one `ledger.record()` call (not a second call; `classify` → `_mark_resolved` would resolve the first call's output), so replay (new→recurring→chronic→resolved at RESOLVE_SWEEPS=3) and dedup are untouched. Regression titles end `; the run used 145 executor-hours` for the Mode card to regex-extract the hours. Fleet noise floor measured on 278 gated (stage, metric) judgements over 100 real run-days: 0 regressions fired, so the publisher ships ungated (no chronic-only gate needed). Test suite 197 passed (gauntlet caught regressions double-rendered and digest double-counted, both fixed); digest gains a regression line; Slack parent gains an `N regressions` stat chip. The Mode card `queries/audi_1317_mode_regressions.sql` is validated but not yet pasted into the report (owner action after merge). PR airflow-ti#1282 on branch audi-1317-publish-regressions stacked on audi-1281-perf-regression-guard (base until #1279 merges). See §7 and [[reference_mode_api]].

## 2026-09-03 (evening) — the 12-PR merge train SHIPPED; savings are honestly ~$5; the BQ surface is SA-scoped by design

**All 11 airflow-ti hackathon PRs merged and each individually deployed, plus shopper_graph #305.** Merge order with squash
commit: #1277 `b836214` 19:10 UTC · #1278 `fc51c0c` 19:18 · #1274 `4091d33` 19:29 · #1279 `090a58f` 19:37 · #1270 `ca3b9e4`
19:44 · #1272 `370f2bd` 19:47 · #1276 `fac8e94` 19:50 · #1273 `96b020e` 19:56 · #1275 `f58f756` 20:04 · #1281 `cd353d7` 20:12 ·
#1271 `b9428f4` 20:20. **A DEPLOYED status was waited for between merges** to avoid the superseded-build gap; each Astro build
took roughly **4-8 minutes**, which paced the ~70-minute train. **airflow-camperbid #580 is the only one left**, blocked on that
team. Full table and the per-ticket outcomes: `tickets/audi_1290_pipeline_optimization_hackathon/summary.md` §4-§5.

**PR numbers, AUDI numbers and branch names are OFFSET in this batch** — PR #1273 is AUDI-1269 on branch
`audi-1269-shuffle-partitions-preverified`, #1271 is AUDI-1275, #1279 is AUDI-1281. **Resolve the worktree from
`gh pr view <N> --json headRefName`, never from the PR number**; the first rebase attempt merged main into an already-merged
branch because the number looked right. **This corrects the ticket-to-PR labels in the AUDI-1271 / AUDI-1272 sections above,
which read the PR number as the ticket number.** Verified mapping in [[reference_airflow_ti]].

**CORRECTION to the 2026-09-02 "Mode savings honestly $0" section (appended, both evidence trails stand).** That section said
Mode reads `$0` because `GREATEST(before - after, 0)` clamps a negative delta. Re-read of the ledger 2026-09-03 gives a more
precise account: **the measured saving to date is ~$5** — **19.4 executor-hours** credited to fangorn PR #1231 on **ONE
observed day**, at the blended **$0.28/exec-h** rate. **Mode's `$0` is a ROUNDING artifact of a real ~$5, not a broken ledger
and not a pure clamp.** Both readings agree on the substance: **there is no ~$900 of savings, and any doc implying it is
wrong.** **The two `disk_spill` findings from #1231 are still `watching` — they never reached `resolved` — and are chronic
again.** Settling check if it matters later: read the ledger's per-key state and applied-day count directly rather than the
Mode headline.

**The BQ surface is scoped by SERVICE ACCOUNT, not by team, and that is by design.** `include/spark_optimizer/bq_profile.py`
`SAS` defaults to `airflow-ti-prod@` **plus** `airflow-camperbid-prod@mntn-prj-prod-00.iam.gserviceaccount.com` (env
`OPTIMIZER_BQ_SAS`). The **Spark** surface excludes other teams by team label (`phs.TEAM`); the **BQ** surface never did. That
is why the sweep flagged a camperbid job (`bos__spend` / `flight_metrics_per2388`) and produced airflow-camperbid #580 — not a
leak. This closes a question Malachi had raised with two different teams. Detail: [[reference_bq_job_attribution]].

**Ryan Kleck's review caveat on `spark.speculation` (AUDI-1275 / PR #1271), recorded on the PR:** with skew, speculation often
just **adds executors**, because the duplicate attempts chase the same long tail rather than shortening it — a speculative copy
of a task that is slow because it holds more data is just as slow, and you pay for both. **That is now the canary's kill
criterion: kill it if executor-hours rise while wall-clock stays flat.** Measurable on the existing ledger, no new
instrumentation.

**Ownership: Tony Chen owns the camperbid / pacing pipelines now** (Forrest Bajbek has left the team). His stated position on
#580 is to **prioritize stability, since those pipelines may be migrated away from anyway**. Swapnil Patil also pulled in.
Route camperbid pipeline questions to Tony. See `knowledge/mntn_business.md`.

## 2026-09-04 — four ledger correctness defects, AUDI-1326 / PR #1286

Found by a verification pass on the 2026-09-04 sweep, every claim independently reproduced against
the live 1,692-row ledger. Fixed on `audi-1326-ledger-savings-correctness`, gauntlet PASS.

**The grace window counted the sweep's own rows, and it had already fired in prod.** `classify()`
built `seen_dates` from the ledger as it stood when `record()` was called, with no `< date` filter,
so any row already dated today slid `_mark_resolved`'s window from {D-2, D-1} to {D-1, D} and a key
quiet for two sweeps resolved instead of three. **No retry was needed to trigger it:**
`apply_manifest()` runs immediately before `record()` and writes `applied` rows dated the sweep day,
which is enough on its own. The 2026-09-04 run succeeded on try 1 and still stamped **28 keys
`resolved` on 2026-09-03, every one of them after only two quiet sweeps**, and the digest reported
them cleared. Replay reproduces exactly 28 against the 28 rows in the live file. `record()` is also
called once per surface, so bq, dbx and pod were each one sweep early even on a clean run. A retry
was worse: 82 phantom `resolved` rows on the second attempt, and separately `last = past[-1]` read
the sweep's own resolved row so the retried digest dropped its whole Resolved section (139 -> 0).

**`RESOLVE_SWEEPS` counts distinct ledger DATES, not sweep executions.** A quiet fleet, an empty
crawl, or a run of `complete=False` sweeps does not advance the window at all, so pending
resolutions stall indefinitely. The ledger already shows the pattern: 2026-08-22, 08-23 and 08-24
are missing entirely even though digests published on those days. "Quiet for three sweeps" can span
a week after an outage.

**`mark_applied()` fabricated a measurement.** It copied `exec_h`/`dcu_h` from the key's previous
row onto the applied row, and `savings()` could not tell that point from a real reading. 50 of the
60 merge-train keys never fired on the 09-03 crawl, so for six DAGs every 09-03 row was synthetic.
Inert only because `savings()` uses strict `d < applied_date` / `d > applied_date`.

**The published savings figure rested on one before-day.** "115 hours all-time, ~$32 all-time, est.
$1,676/yr" came entirely from `fangorn_score_monitor` with n=1 before and n=2 after, against a DAG
whose own daily range with nothing changed is 592-954 executor-hours. `daily_h` was also
last-write-wins over file order, and 44 of 409 dag-days carry more than one distinct `exec_h`
because a retried sweep leaves a smaller partial day-sum. `savings()` now needs **3 sweep-days each
side plus a 90% interval clear of zero**, publishes the reason instead of a number when the bar is
unmet, and takes the fuller reading per dag-day.

**`delta()` had no branch for `recurring`.** It is an if/elif over new/chronic/resolved/
fix_not_working/STICKY, and `classify()` can also set `recurring`, so those entries reached no
digest renderer at all: **51 of 186 spark entries on the 09-03 sweep, 12 DAGs, 3,231 executor-hours
invisible**. Now a dict with a `chronic` fallback, and a test AST-greps `ledger.py` for state
literals so a future state trips CI.

**Left open, deliberately.** The 28 false `resolved` rows stay in the ledger; the fix is
forward-only and un-stamping them is a separate data edit. `prior_sweep_dates` is still computed
across all surfaces, so a bq/dbx/pod key can be resolved from dates on which only spark recorded.
A manual `mark_applied` run AFTER a sweep on the same date now leaves that dag-day with no
measurement, because `append()` dedups on `(date, dag_id, key)` and the hours are no longer copied.

## 2026-09-04 — AUDI-1329: coverage measured for the first time

**The optimizer scans 28% of the fleet on an average day.** Mean 215.4 jobs of **763 Spark task
instances/day** (the fleet denominator, measured from the Airflow REST API across 2026-08-29..09-03;
do NOT use the 1,262 project-wide Dataproc batch count, 640 of which are camperbid's). Best sweep
40% after de-duplication, worst 20%.

**Retention is NOT the binding ceiling.** This reverses the assumption carried in from AUDI-1327.
Retention removes nothing the optimizer could otherwise read; the gap is our own code plus engine
opacity. (The 7-day Cloud Logging window still caps a retrospective driver-log CORPUS — see
[[project_airflow_debugger]] — but not the tool's operational coverage.)

**6 of 14 published sweeps ran at 154 jobs instead of ~344 and published a confident backlog
anyway.** Their headers read `(newest 6 of 200, 194 failed)` and nothing surfaced an error.
`phs.list_batches` returns `[]` on any `gcloud` error without raising (`phs.py:35-38`), and the DAG
catches the exception into `phs_n = 0` with a `logger.warning`. The empty-sweep guard tests the
total, not each half. Restoring those six moves the mean from 215 to ~297/day, **28% to 39%, with no
design change.** `fetch.py` got the gsutil->JSON-API fix; `phs.fetch_logs` still shells out to
`gsutil cp -r` at `phs.py:110`, and gsutil is BANNED on Astro pods.

**The caps are coupled; raising one alone is inert.** `MAX_BATCHES=150` re-binds at 195-201 eligible
inside the 500-slice, so it and `phs.list_batches(limit=500)` must move together. Then
`MAX_BYTES=4 GiB` (`phs.py:83`) binds: 460 PHS dirs at a 4.4 MB mean is ~2.0 GiB against an archive
half already charging 1.81 GiB to the same tmpdir. All three together reach ~600/763 = 79%.

**8 of 21 detectors have never fired** in 11 days and 1,692 ledger rows: `missing_statistics`,
`broadcast_candidate`, `window_full_sort`, `repeated_scan`, `cache_ineffective`,
`spot_preemption_cost`, `pod_memory_pressure`, `regression_disk_spill`. **`analyze_plan` — the entire
plan-text half of `optimizations.py` — has ZERO production firings**, because the sweep feeds event
logs and never plan text.

**Measured waste classes with no detector at all.** Driver prologue idle: 754 idle executor-hours in
one day from two jobs, 73% of combined wall clock, worst run held 63 registered executors for 7,035s
with zero tasks launched (IMP-106 generalised; `idle_reserved_executors` fires on these and
prescribes a tail remedy for jobs with no tail). Small-file read amplification: `site_network_hourly`
reads 92.1% of 35,609 tasks at under 8 MiB against `maxPartitionBytes=256 MiB`, and **no detector
reads Input or Output Metrics at all**. Sub-second task swarm: four jobs run 32,828-154,018 tasks
with 97.9-99.8% under 1s; `shuffle_partition_sizing` only fires when partitions are too big.

**18% of the archive can never be read:** 966 of 5,246 objects — 453 permanently
`.zstd.inprogress`, accreting 13-18/day since 2026-08-21 and disproportionately the crashed, killed
and longest-running apps (excluded at `fetch.py:56`, discarded at `crawl.py:93`), plus 385
`_GHFS_SYNC_TMP_FILE`.

**The sweep window is 28-40 hours, not 24** (median 31.1h, measured from the app-id timestamps of
what it scanned), against a 24-hour schedule — so a per-sweep numerator over a per-day denominator
double-counts. It is also hour-of-day biased: 255 of 340 scanned apps ran 00:00-08:59 UTC.
**The report date is a day behind its window:** `optimizer_backlog_2026-09-03.md` is the sweep that
ran 2026-09-04T09:19Z, and 256 of its 344 sources carry a 2026-09-04 app id.

**PHS admission defaults foreign work in.** `(labels or {}).get('team','ti') == 'ti'` (`phs.py:23`)
admits any unlabeled batch as ours: 89 of 175 admitted had no team label; 22,133 unlabeled batches
across a 100k pull.

**26% of the fleet is engine-opaque.** 195 Spark task instances/day write nothing readable:
`DbxDbtOperator` 144.7, `ModelPysparkDbxJobOperator` 48, `ModelPysparkWorkflowOperator` 2. No cap or
retention change touches it; it needs `cluster_log_conf` on the Databricks job clusters and
`DATABRICKS_PROFILE` on the prod deployment.

**The operator maps are wrong.** `ModelPysparkDbxJobOperator` (288 runs/6d) and
`TiVertexPipelineOperator` (41 runs/6d) match neither `SPARK_OPERATORS` nor `OPAQUE_OPERATORS` and
are filed as non-Spark; 5 of the 11 entries are dead names with zero runs.


## 2026-09-04 (late) — the Mode savings dashboard is a THIRD, unfixed savings method

**The AUDI-1326 fix does not reach the dashboard.** Mode report `e81786de8403` recomputes savings in
its OWN SQL — query `5a66e5fad18c` "Savings headline", duplicated byte-identically in `513a4a7a4a71`
"Savings by surface" — directly over the raw ledger columns. `savings()` in `ledger.py` writes only
`optimizer_savings.md` and the Slack digest note; the string `savings` appears **0 times** across all
7 of the report's queries, and the external table has no savings/dollar/delta column. So deploying
#1286 changes nothing on screen: the Python digest will say "No measured savings to report" while the
dashboard publishes $960-$1,435. Two MNTN surfaces disagreeing 30-45x with no reconciliation.

**Published 2026-09-04T23:10:58Z: 3,455.2 exec-h / $960.55 all-time, $173,289/yr, 15 DAGs fixed.
Correct answer on the same ledger: zero measured savings.** Six SQL defects, each verified:
- `SUM(exec_h) GROUP BY dag_id, surface, DATE(date)` — but **`exec_h` is a DAG-level daily total
  stamped identically on every finding row for that DAG that day** (`ledger.py:280`). SUM multiplies
  true executor-hours by the finding count. `site_network_hourly` 2026-09-02: MAX 3,653.1, SUM
  91,590.6. fangorn's "before" of 6,189.3 is literally 9 x 687.7. MAX instead of SUM: 5,163.0 -> 174.2.
- The all-time figure multiplies by `DATE_DIFF(CURRENT_DATE(), applied_date, DAY)` — **elapsed
  calendar days, not observed days. It grows +1,707.8 exec-h / +$474.76 EVERY DAY with no new data.**
  Same SQL, same frozen ledger: 1,747.4 (09-03) -> 3,455.2 (09-04) -> 5,163.0 (09-05) -> 56,396.5 (10-05).
- `GREATEST(before_rate - after_rate, 0)` floors every regression at zero inside all four aggregates,
  so the headline is a one-sided selected sum that **structurally cannot show the program is net
  negative**. 6 of 15 scored DAGs are regressions; `site_network_hourly` contributes 0 instead of
  -130,827.9. Unfloored the same sum is **-125,734.2**: the published number has the wrong sign.
- After-window is inclusive (`d.d >= a.ad`) where the Python uses strict `>`.
- **No evidence gate at all** — no `outcome='resolved'`, no minimum observations, no interval. 14 of
  the 15 DAGs were applied 2026-09-03 with ZERO observed after-days and are still `watching`. Exactly
  the unsoundness AUDI-1326 was opened to kill, reproduced in a surface AUDI-1326 never looked at (its
  summary.md contains zero occurrences of "mode").
- 58% of the headline (1,997.1 h) is ONE DAG, `fangorn_score_monitor`, whose true per-finding rate
  moved ~0.3% (687.7 -> ~685) but whose count-scaled delta is multiplied by 8 calendar days off a
  **single** before-day.

**Freshness: the report's only schedule fires 06:00 UTC; the sweep rewrites the ledger ~09:19 UTC.**
Every unattended render is ~21 hours stale. Move the schedule to 10:00 UTC (artifacts land 09:08-09:24
across 14 observed days). The 3,455.2 on screen exists only because someone refreshed manually at 23:10.

**The external table schema is frozen at 2026-08-28T22:14:35Z with `ignoreUnknownValues=true` despite
`autodetect=true`.** `prev_exec_h` already exists on 781 of 1,692 rows and is invisible to SQL
("Unrecognized name"); `partial`, which #1286 adds to every row from the 2026-09-05 sweep on, will be
dropped the same silent way — so the Mode SQL can never implement the partial-sweep exclusion. Recreate
the table with an explicit 18-field schema.

**Dollars rest on frozen SQL literals** (`0.278`/exec-h, `0.04`/slot-h) while the report's hero text
claims "the blended rate from actual Dataproc spend". The sweep already computes the live rate (0.277
on 2026-09-04); join it instead. The `$0.04` slot-hour constant is sourced NOWHERE in either repo.
Upstream `billing.py:23` pins `DCU_PER_EXEC_H = 5.44`, "the conservative end of the 5.4-9.9 range" —
at the top of that range every dollar figure is ~82% higher and nothing re-measures it.

**PR #1286 IS deployed but has never executed.** `astro deployment inspect cmd6bd10c0gl901rfuokgryiq`
-> `current_tag deploy-2026-09-04T23-19-30`, HEALTHY, description "Merge pull request #1286", DEPLOYED
23:19:30.328Z after a FAILED 23:17:20 attempt on the same PR. Every savings artifact in prod was
written by the PRE-fix code at 09:19 UTC that morning, ~14 hours before the merge. First post-fix sweep
is **2026-09-05 09:00 UTC**. So "115 hours all-time / ~$32" still on disk is expected, not a failed
deploy. Behavioural test after that sweep: `optimizer_savings.md` should carry an 11-column header with
a "90% CI" column and the Welch/`MIN_OBSERVATIONS` parenthetical, and new ledger rows should carry
`partial`. An 8-column table means the image is wrong.

**Three Python defects survive #1286** (found by adversarial probe, not by the original ticket):
- **The all-time total is a sum over only the jobs that individually passed the 90% gate, with no
  multiplicity correction.** A 200-rep null simulation driving the real `savings()` on data where every
  job truly saved zero reported a positive total with a CI excluding zero in **124-131 of 200 reps**,
  mean +136.5 h against a truth of 0. It is forced, not chance: `total > sum(half_i) >= sqrt(sum(half_i^2))`.
  Per-job false-positive rate is a correct 4.8-5.0%; the aggregate is 62-64%. Either Holm-adjust the
  per-job alpha or stop publishing a summed headline and publish the per-job table with its intervals.
- `shipped()` never resets `outcome` once set to `resolved`, so a job later marked
  `owner_notified`/`wont_fix` is frozen as a permanent win while running at full pre-fix hours
  (driven through real `record()`/`mark_applied()`/`set_state()`: 903.9 h counted as saved).
- A fix's before-window has no lower bound, so on a job with two successive fixes the later fix's
  baseline averages in the era before the first. Lower-bound `before_days` at the previous
  `applied_date` for that `(dag_id, surface)`.

Full audit with per-defect evidence:
`tickets/audi_1325_debugger_optimizer_adoption/outputs/audi_1325_mode_savings_audit_2026_09_04.md`.

## 2026-09-05 — the "6 regressions" were 5 artifacts and 1 real one (ipdsc_ds_49)

**5 of the 6 vanish under correct aggregation; `ipdsc_ds_49` survives every correction.** PR #1272
halved `spark.sql.files.maxPartitionBytes` 128 -> 64 MiB and read-stage **tasks per GiB doubled
(12.58 -> 26.74)**, executors scaled 48 -> 107 -> 116 under `dynamicAllocation(4,180)`. Dataproc
DCU-h/day: **10.1-15.7 across 16 pre-fix days -> 18.7 (09-04) -> 24.3 (09-05)**, both above every
pre-fix day and still rising. Input-normalized `exec_h/GiB` held in [0.0557, 0.0628] across the whole
pre-fix window despite input growing 36.8%, then 0.0770 / 0.0897 (+43% over the pre-fix max). Per-run
event logs: pre mean 2.578, sd 0.258, n=17; post 4.261 and 5.350, **z = +6.5 and +10.7**. The fix
bought a 94% spill reduction and a 28% wall-clock cut for roughly 2x the executor-hours. **Causation
is mechanistic, not proven** — no revert has been run and the DAG's 7-day `site_network_hourly` input
stepped +43% on 09-01 (normalizing on that still leaves 0.3013/0.3643 vs a 0.2138-0.2428 pre-fix band).
**Discriminating test, runnable now, verdict 2026-09-09:** set `maxPartitionBytes` back to 128 (or 96)
MiB on `ipdsc_ds_49` ONLY, compare `milliDcuSeconds` per GiB of 7-day input, 3 days each side. The
bands separate by >6 sigma.

**Speculation canary (PR #1271, `site_network_hourly`): kill criterion NOT met, do not kill it.**
Ryan Kleck's criterion was "executor-hours rise while wall-clock stays flat". Wall-clock did not stay
flat under either reading (pooled n=17 median -26.5%; hour-matched n=3 +1.49x, sign p=0.75, permutation
p=0.205), so the precondition fails. And speculation cannot be the cost channel: **killed duplicate
attempts consumed 11.27 h of 9,214.34 h of slot time across all 19 post-flip runs, 0.12%.** The
expensive runs are FetchFailed storms (7,295 / 13,362 / 10,080), a pattern that also occurs PRE-flip
with speculation off (09-03 12:51: 0 speculative attempts, 7,104 FetchFailed) and is already IMP-104.
The run with the HIGHEST speculation rate (3,141 attempts, 3.77%) was the cheapest and fastest measured.

**`site_network_hourly`'s 65,413-hour "regression" is 19.3x row fanout.** The published after-value
88,476.1 is the SUM of 22 finding rows on the applied date, 11 of them `state='applied'` clones
(38,112.6 h); the true DAG-day value is 4,578.5. Normalized against the independent meter it is
**+0.8%** (1,929.7 DCU-h/GB output vs a 1,091.6-2,659.0 pre-fix band).

**`mark_applied` stamps the clone with the PREVIOUS sweep's `exec_h`** (`ledger.py:383`,
`exec_h=last.get("exec_h")`), so for `ipdsc_42_monitor`, `guid_log_pivot_ip_vertical_id` and
`ipdsc_ds_49` **the published "after" was literally the "before" relabelled**. `ipdsc_42_monitor`'s
after-rate 9.0 is its own 1.5 re-stamped six times; the genuine post-fix run improved ~7x.

**CONTRADICTED — "every entry's `exec_h` is its dag's total for the sweep-day" is false in the data.**
35 of ~470 DAG-day cells carry two or three distinct values (`site_network_hourly` 2026-08-26 holds
9.8, 35.3 and 73.7) because `append()` replaces only rows whose `(date, dag_id, key)` the current sweep
re-emits, so **earlier narrower sweep generations survive in the same cell**. Every "one value per
DAG-day" correction is therefore a MAX pick and must say so; under MIN the 09-02 -> 09-03 ratio is
1.41x not 1.25x. The docstring at `ledger.py:280` states the invariant the writer does not enforce.

**Two DIFFERENT events, not one.** Downloads fell to 6 of 200 on 08-27 through 09-01 (repaired by
`601483d` on 09-02) which DEFLATES `exec_h` and masks regressions; separately the PHS component jumped
22 -> 150 on 08-26 on a day with a full 200-of-200 download, which INFLATES it. Citing "jobs scanned
214 -> 344" as evidence of acquisition collapse has the mechanism backwards. **Only 09-02, 09-03 and
09-04 are known-full sweeps**, and 09-03 is the applied date, so every non-ds_49 verdict rests on n=1
until the 2026-09-07 sweep gives three full days each side.

**`fangorn_score_monitor` is the only DAG in the entire ledger with a real multi-day after-window**
(applied 08-27, after n=3): 687.7 -> mean 621.2, -9.7%, and it does NOT regress. Welch is undefined at
n_before=1.

Verdict doc: `tickets/audi_1325_debugger_optimizer_adoption/outputs/audi_1325_regression_verdict_2026_09_05.md`.

## 2026-09-05 — ipdsc_ds_49 settled: the config change, not the volume, and revert it

**Advertisers were NOT the cause.** Delivering advertisers were flat through the step: 7,005-7,180/day
from 08-18 to 09-04, with 7,145 on 09-01 itself, the day of the LOWEST impressions (57.6M) and lowest
media spend in the window. 15 advertisers had a first-ever delivery on 09-01, 0.2% of the base. The raw
upstream bid stream FELL 8% then 12% across the step. **The growth is 3-4 supply publishers jumping
5-20x overnight** (Samsung TV+ News 0.55B -> 4.21B events, Paramount Comedy 1.98B -> 6.55B, LG
Entertainment 1.47B -> 2.21B) while every other publisher stayed flat and the publisher count never
moved (203-208). Likeliest cause is a **site-to-publisher mapping edit, not demand**: one operator
edited 53 rows in a 27-minute session at midnight UTC on 09-01, touching Pluto TV, Vizio WatchFree and
Xumo bundles. That is a LEAD, not a conclusion; settling it needs the Postgres change history.

**The data contains a natural control that separates volume from config.** The job reads a rolling
7-day window, so the 09-01 step entered gradually and **two runs (09-01, 09-02) carried +7% and +15%
input while still on the OLD 128 MiB setting.** Their unit cost was 0.272 and 0.257 DCU-h/GiB, dead
inside the pre-change band of 0.235-0.279 across 30 runs. Volume rose 15%, unit cost did not move.
Then the setting landed and unit cost went **0.338, 0.408, 0.465** — 30%, 58%, 80% above the pre-change
mean. **Attribution of the 17.6 DCU-h rise: ~4.7 volume, ~12.9 the config change.** One quarter volume,
three quarters the setting.

**The mechanical fingerprint volume cannot fake:** work chunks per unit of input sat at 12.3-12.8 across
all 30 pre-change runs while input itself varied 37%, then jumped to 26.7 the instant the setting landed
and stayed. Only halving `maxPartitionBytes` does that. Machines grabbed under autoscaling went 48 -> 116.

**Both things the change bought are worth nothing here.**
- **The spill it eliminated was never billed.** Dataproc's shuffle-storage meter is provisioned PER
  MACHINE, not per byte spilled — an exact fixed multiple of the machine meter on every run. So the 94%
  spill cut (24.1 GB disk / 457 GB memory -> 1.4 / 19) recovered nothing on any invoice line, and that
  meter rose 81% alongside the machine meter.
- **The speed win evaporated.** Wall-clock went 335s, 358s, 423s against a pre-change average of ~438s.
  We now pay 2.5x for a job finishing at roughly its original speed. Even at its best the 1.4 minutes
  bought nothing: the DAG then sits behind an 8-hour sensor waiting on an external file, idling 1-3
  hours every recent run, and the last sibling to finish was `ipdsc_ds_35` on 7 of the last 9 runs.

**RECOMMENDATION: revert `ipdsc_ds_49` to the 128 MiB default.** Delete the one added line, or revert
only that file's hunk of commit `9ae505a` — **do NOT revert the whole commit**, it also carries an
unrelated `conv_log_derived_ip` change that is fine. Cost of being wrong: spill returns to ~30 GB disk
/ ~450 GB memory and the job takes a minute or two longer, neither billed, neither moving the DAG. The
residual risk is that the old setting has never run at today's volumes (52.3 GiB max previously, ~71
GiB at saturation) and spill grows faster than linearly, so the revert run will spill more than it ever
has (~33 GB over ~63 machines, half a GB each, nowhere near a disk limit). That knee is unmeasured.
**Reject the alternatives:** capping machines cannot work because ~60% of the increase is real task
time, not idle machines; 96 MiB is a guess with no measured baseline at any volume.

**Correction to the framing everyone was using: `ipdsc_ds_49` is a TASK inside the DAG
`tpa_ipdsc_export`, not a DAG.** It is metered on its own, so its cost is not mixed with its siblings.

**What it does, in plain terms:** nightly, it reads 7 days of MNTN's CTV ad-auction records and reduces
them to one list — for each home IP, which streaming channels that household had an MNTN ad opportunity
on. Last night: **93.8M addresses across 207 channels**. It is one of 20 such lists merged into the
nightly file that tells the ad-buying system what is known about each address. **Nothing in live
campaign setup references it** (0 hits across all 425,054 targeting-segment definitions and all 68,265
audience definitions), but a hard failure blocks that nightly file for all 20 lists and pages Targeting.

**Unexplained and worth watching:** unit cost is STILL rising at a byte-identical config (0.338 ->
0.408 -> 0.465). Leading candidate is that smaller chunks weaken the pre-grouping step so more rows
cross the network, a penalty that grows with volume. Also, both post-change runs read more data than
the old setting was ever observed handling, so a cost nonlinearity above 52.3 GiB cannot be excluded
from observation alone.

Full write-up:
`tickets/audi_1325_debugger_optimizer_adoption/outputs/audi_1325_ipdsc_ds_49_explained_2026_09_05.md`.
