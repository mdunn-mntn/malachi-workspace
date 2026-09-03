---
doc_type: ticket
title: "AUDI-1272: Verify map-output spread then raise initialExecutors on 10 fetch-wait DAGs"
status: backlog
date: 2026-09-02
summary: "Per DAG confirm map output sits on few executors, then raise initialExecutors"
result: "not started"
question: "For each of the 10 DAGs, does the slow-fetch stage's map output sit on the few executors the job started with, and what initialExecutors value spreads it?"
framing_state: locked
---

# AUDI-1272: Verify map-output spread then raise initialExecutors on 10 fetch-wait DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1272
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** For each of the 10 DAGs, does the slow-fetch stage's map output sit on the few executors the job started with, and what initialExecutors value spreads it?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A per-DAG verdict table in outputs/ (stage, executors holding 90% of map output, hottest share, current and target initialExecutors) and one PR (branch AUDI-1272) raising initialExecutors on every DAG where concentration is confirmed.
- **Approach (how):** Event logs as in 1270; run tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_shuffle_concentration.py per log; confirm maxExecutors caps allow the target; regenerate model_task_config.json.
- **What would change the answer:** Map output already spread across most executors, meaning the fetch wait has another cause; that DAG gets no change and the cause goes in §8.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Ten more jobs show the same waiting-to-copy-data symptom as AUDI-1271; confirm the cause per job before raising the starting machine count.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the fix only helps when the wait comes from early output crowded onto the few machines the job started with. The event log shows where that output sits.

**Task:** per DAG check the map-output spread in the event log, then raise `spark.dynamicAllocation.initialExecutors`:
- [advertiser_mid](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/advertiser_mid.py)
- [ipdsc_42_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_42_monitor.py)
- [tpa_export_enrich](https://github.com/SteelHouse/airflow-ti/blob/main/models/tpa_export/tpa_export_enrich.py), [tpa_mntn_id_export](https://github.com/SteelHouse/airflow-ti/blob/main/models/tpa_export/tpa_mntn_id_export.py)
- [audience_intent_scoring_staging (ds46 task)](https://github.com/SteelHouse/airflow-ti/blob/main/spark/machine_learning/audience_intent_scoring_staging_spark.py)
- [ipdsc_ds_46](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_46.py)
- [aug_log_ip_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/aug_log_ip_hourly.py)
- [vertical_size_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/vertical_size_monitor.py)
- [guid_log_derived_household_id_vertical_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_2_derived/guid_log_derived_household_id_vertical_id.py)
- [site_visit_signal_derived_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_2_derived/site_visit_signal_derived_advertiser_id_dsc_id.py)

**Done-when:** PR merged; optimizer ledger shows the finding resolved (savings auto-measure).

## 3. Plan of Action
Planning wave 2026-09-02 (nothing executed; §4 holds the checks the plan rests on). Executed by an agent in a per-ticket git worktree of airflow-ti on branch `AUDI-1272` (dispatcher creates it, commits, runs the gauntlet, opens the PR, stamps the ledger). All workspace paths below are relative to `/Users/malachi/Developer/work/mntn/workspace/`; all airflow-ti paths are relative to the worktree root. Read-only reference checkout: `/Users/malachi/Developer/work/mntn/airflow-ti-main` (origin/main at `825b07e`, 2026-09-02).

### 3.1 Where each DAG's config lives today (source of truth, verified on main)

Every one of the nine model DAGs sets its Spark runtime properties in the `runtime_properties` dict of the `@compute.dataproc_batch(...)` decorator in its model file. `MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun` compiles that dict into `dags/model_task_config.json` `<model_id>.batch.runtime_config.properties` (verified: the `advertiser_mid` entry mirrors its decorator key for key). None of the ten files sets a `spark.dynamicAllocation.*` or `spark.executor.*` key in the `SparkSession.builder` block, so the decorator is the only surface to change. When `initialExecutors` is absent, Dataproc Serverless launches `minExecutors` executors (`spark.executor.instances` = min in the batch spec and in the event log); when no dynamic-allocation key is set at all, the log shows min 2 / instances 2 / max 1000.

| # | DAG (ledger `dag_id`) | File | Lines | Today | Effective start / cap | Batch label filter | Flagged stage(s) | Recent logs (`gs://mntn-data-archive-prod/spark-events/<app>.zstd`) |
|---|---|---|---|---|---|---|---|---|
| 1 | advertiser_mid | `models/audience_intent/advertiser_mid.py` | L15-19 (min L17, max L18) | min 25, max 90, no initial | 25 / 90 | `labels.job_type=advertiser_mid` | 8, 9, 19, 24 (chronic, streak 4) | app-20260826045238873-0661 (12 MB), app-20260901055634611-0510, app-20260902051541259-0571 |
| 2 | ipdsc_42_monitor | `models/monitoring/ipdsc_42_monitor.py` | L160-181 (`spark.executor.instances` L172) | instances 2, no dynamic-allocation keys | 2 / 1000 (Serverless default, verified in log 0927) | `labels.job_type=ipdsc_42_monitor` | 18, 22, 26 (chronic) | app-20260825050530606-0927 (0.7 MB), app-20260901035806681-0636, app-20260902042001249-0056 |
| 3 | tpa_export_enrich | `models/tpa_export/tpa_export_enrich.py` | L49-51 (enabled, min L50, max L51) | min 10, max 120 | 10 / 120 | `labels.job_type=tpa_export_enrich` | 6 (chronic) | app-20260826071015202-0099 (12 MB), app-20260901073519357-0662, app-20260902082207698-0515 |
| 4 | tpa_mntn_id_export | `models/tpa_export/tpa_mntn_id_export.py` | L114-116 (enabled, min L115, max L116) | min 10, max 150 | 10 / 150 | `labels.job_type=tpa_mntn_id_export` | stage id drifts per run (398, 403, 367, 9): match by fetch-wait share, not id | app-20260826061108811-0774 (55 MB), app-20260901083658620-0891, app-20260902091759702-0256 |
| 5 | audience_intent_scoring_staging_ds46 | evidence: `models/machine_learning/fangorn_14day_lookback.py` L16-17 and `models/machine_learning/fangorn_household_14day_lookback.py` L16-17. Ticket link: `spark/machine_learning/audience_intent_scoring_staging_spark.py`, launched by `dags/machine_learning/audience_intent_scoring_staging.py` L109-131 with `runtime_config = {"version": "2.3"}` (L28, no properties) | see §4.3 | fangorn models: min 30, max 180, no initial; staging DAG: nothing set | 30 / 180 (fangorn); 2 / 1000 (staging) | `labels.job_type=fangorn_14day_lookback`, `labels.job_type=fangorn_household_14day_lookback`, `labels.airflow-dag-id=audience-intent-scoring-staging` | 20, 23 (14day) and 8, 11 (household), chronic | app-20260826000057091-0058 (21 MB, 14day), app-20260826003036565-0444 (57 MB, household), app-20260901212248899-0756, app-20260901213302742-0181 |
| 6 | ipdsc_ds_46 | `models/ipdsc/ipdsc_ds_46.py` | L9-13 (min L10, max L11) | min 4, max 180 | 4 / 180 | `labels.data-source-id=ds46 AND labels.application=ipdsc` | 3 (chronic) | app-20260826040949342-0431 (1.4 MB), app-20260902041933916-0926 |
| 7 | aug_log_ip_hourly | `models/feature_store/feature_group_1_source/aug_log_ip_hourly.py` | L31-36 (initial L33) | min 50, initial 100, max 200 | 100 / 200 | `labels.job_type=aug_log_ip_features` (hourly) | 4 (corpus sweep only: 2 runs, 38%; the daily ledger shows idle executors and stragglers, no fetch wait) | app-20260901171645146-0764, app-20260902161713255-0505, plus any two hourly logs |
| 8 | vertical_size_monitor | `models/monitoring/vertical_size_monitor.py` | L197-218 (`spark.executor.instances` L209) | instances 2, no dynamic-allocation keys | 2 / 1000 | `labels.job_type=vertical_size_monitor` | 13 (corpus sweep only: 1 run, 73%; daily ledger shows disk spill only) | app-20260826010837114-0307, app-20260902011415369-0939 |
| 9 | guid_log_derived_household_id_vertical_id | `models/feature_store/feature_group_2_derived/guid_log_derived_household_id_vertical_id.py` | L25-29 (initial L27) | min 5, initial 10, max 20 | 10 / 20 | `labels.job_type=guid_log_household_id_rollup_windows` | 11 (corpus sweep only, 1 run, 74%) | none in the ledger or backlogs: resolve from batches `gui-log-der-hou-id-ver-id-*-20260901-010300-1` (created 2026-09-02T01:14Z) and `...-20260902-010300-1` (2026-09-03T01:20Z) per step 2c |
| 10 | site_visit_signal_derived_advertiser_id_dsc_id | `models/feature_store/feature_group_2_derived/site_visit_signal_derived_advertiser_id_dsc_id.py` | L40-44 (initial L42) | min 50, initial 100, max 200 | 100 / 200 | `labels.job_type=site_visit_signal_derived_adv_dsc` | 19, 20 (08-24, 46% / 42%) | app-20260824014400982-0069 (6 MB); batches `sit-vis-sig-der-adv-id-dsc-id-*-2026090{1,2}-010300-1` |

### 3.2 Steps

1. **Set up tooling (workspace, no repo writes).** Copy `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_shuffle_concentration.py` to `artifacts/audi_1272_shuffle_spread.py` and make two edits: (a) the upstream-stage filter `tasks[s] > 1000` becomes a `--min-map-tasks` option defaulting to 100 (the two monitors run 128 shuffle partitions, so the 1000 floor printed no map side for `ipdsc_42_monitor` in §4.2); (b) when the time heuristic in `_map_side` is ambiguous, use `Stage Info -> Parent IDs` from `SparkListenerStageSubmitted` to pick the feeding stage. Run it as `PYTHONPATH=/Users/malachi/Developer/work/mntn/workspace python3 artifacts/audi_1272_shuffle_spread.py <log>...` (it imports `airflow_optimizer.eventlog`, byte-identical to `include/spark_optimizer/eventlog.py`; the `zstandard` module is not installed, the parser falls back to `/opt/homebrew/bin/zstd`, verified). Do not edit the parser.
2. **Collect two event logs per DAG (three for #5) into `outputs/logs/`.** (a) Take app ids from the table above; the newest are in `tickets/audi_1290_pipeline_optimization_hackathon/audi_1281_perf_regression_guard/outputs/optimizer_backlog_2026-09-02.md` (read-only, another ticket's folder) and `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_2026-08-2*.md`, lines shaped `Populate <model>.<Class> (app-....zstd)`. (b) Download with `gsutil -o "GSUtil:check_hashes=never" cp gs://mntn-data-archive-prod/spark-events/<app>.zstd outputs/logs/` (`gcloud storage cp` corrupts `.zstd`; the "no hashes" warning is expected). (c) For a DAG with no listed app id (#9, and any 404): `gcloud dataproc batches list --project=mntn-prj-prod-00 --region=us-central1 --limit=6 --filter='<label filter>' --format='value(name.basename(),state,createTime)'`, then `gsutil ls gs://mntn-data-archive-prod/spark-events/app-<YYYYMMDDHHmm>*` for the minute 0-3 min after `createTime` (app ids are UTC `app-YYYYMMDDHHmmssSSS-nnnn`; verified on both #5 logs: batch 23:59:29Z -> app 00:00:57, batch 00:29:16Z -> app 00:30:36), and confirm `spark.app.name` in the log's `SparkListenerApplicationStart`. Do not use a `batchId:` filter (timed out at 5 min); label filters return in seconds. No PHS grant is needed: all ten DAGs write to the batch-fleet archive.
3. **Read the effective config out of each log before trusting the file.** From `SparkListenerEnvironmentUpdate -> Spark Properties` record `spark.dynamicAllocation.minExecutors`, `initialExecutors` (if present), `maxExecutors`, `spark.executor.instances`, `spark.app.name`. A mismatch with the table above means the deploy is behind the file or the log belongs to another launcher; resolve it before the verdict.
4. **Attribute every `audience_intent_scoring_staging_ds46` log to its launcher (#5).** Three jobs share that app name: the staging DAG's spark script and the two fangorn 14-day lookback models. Match each log's start time to `createTime` from the three label filters in the table; min 30 / max 180 in the log means a fangorn model, min 2 / max 1000 means the staging DAG. Both 08-26 logs are fangorn (§4.3). Also check whether the staging DAG's own batches write an event log at all: `gcloud dataproc batches list --filter='labels.airflow-dag-id=audience-intent-scoring-staging' --limit=3` then `describe --format='value(runtimeConfig.properties)'` and look for `spark.eventLog.dir`; if absent, the staging job is unverifiable and goes in §8.
5. **Run the spread check and fill the verdict table `outputs/audi_1272_verdicts.csv`** with one row per DAG x flagged stage x log: `dag_id, app_id, stage, fetch_wait_pct, feeding_stage, map_output_gib, executors_holding_output, executors_holding_90pct, hottest_share_pct, executors_live_at_map_start, executors_registered_peak, current_initial, max, verdict, target_initial`. Verdict rule (from the 1194 pre-verification of rows 4 and 5 and the `site_network_hourly` verify pass): 
   - **confirmed, concentration**: the feeding map stage started on no more executors than today's start count and its output is crowded (90% on about that many executors, or hottest share >= 10%; `site_network_hourly` showed 90% on 48-105 of 50 initial, hottest up to 24.6%);
   - **confirmed, server count**: output evenly spread but only over the executors alive at map start, which is below the cap the run later reached, and a later stage of the same shape on the full fleet waits far less (`aug_log_ip_vertical_id_hourly`: 34% wait on 100 executors vs 1% on 200);
   - **not confirmed**: the map output already sits on about as many executors as the run ever registers (hottest share around 1% or less, map started near peak). No config change; write the alternative cause (block size, spill, disk) in §8 and the ledger note.
   Both logs must agree; a split verdict is "not confirmed" and goes in §8 with both rows.
6. **Target value per confirmed DAG.** `target_initial` = the peak executor count the run registers (the `N executors registered` line), capped at `maxExecutors`; if the peak is within 20% of the cap, use the cap (the 1271 pattern: initial = max on both DAGs). Never raise `maxExecutors` in this ticket. For the two monitors (#2, #8) add `spark.dynamicAllocation.initialExecutors` beside the existing `spark.executor.instances` line rather than changing `instances` (same key as every other DAG and as the ledger fix text; Spark takes the larger of the two).
7. **Record the cost baseline before any change** in `outputs/audi_1272_dcu_baseline.csv`: for each DAG the last three SUCCEEDED batches from step 2c with `gcloud dataproc batches describe <batch> --format='value(runtimeInfo.approximateUsage.milliDcuSeconds,runtimeInfo.approximateUsage.shuffleStorageGbSeconds)'` (DCU-h = milliDcuSeconds / 3,600,000; `advertiser_mid` 09-01 run = 47.4 DCU-h). The ledger measures executor-hours after merge; this is the per-run cost guard for the 1271 kill criterion (cost per run up more than the wait saved means revert).
8. **Edit the decorators in the worktree**, one line per confirmed DAG, inserted directly under the `minExecutors` line (or under `spark.executor.instances` for the monitors): `"spark.dynamicAllocation.initialExecutors": "<target>",`. For #7, #9, #10 change the existing value in place (L33, L27, L42). Item #5 only per the decision in §8 / the plan comment: if approved, the same one-line insert at L17 of both fangorn model files; if not, no edit and the verdict stays in the table. No other file, no comment lines, no reformatting.
9. **Regenerate the task config in the worktree**: `MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun` (uv at `/opt/homebrew/bin/uv`; dependency group `models` in `pyproject.toml` L28-34; `--dryrun` compiles and writes `dags/model_task_config.json` without uploading). Then check with a short python read that, for every edited model, `dags/model_task_config.json[<model_id>]["batch"]["runtime_config"]["properties"]["spark.dynamicAllocation.initialExecutors"]` equals the decorator value, and that `git diff --stat` in the worktree lists only the edited model files plus `dags/model_task_config.json`; any other changed file means the compile environment differs from main, stop and report.
10. **Lint like CI**: run ruff from the worktree root with the pinned config (per memory `reference_airflow_ti`, the pin floats within `>=0.16,<0.17`; fix any violation in the touched files only). `model-unit-test` is broken repo-wide since PR #1209 and is not a required check; do not treat it as a blocker. Never run `model_run.py` against prod, never trigger a DAG: the next scheduled cron after the Astro deploy is the first prod run, and runtime properties only take effect after the "Deploy to Prod" action (the `.py` syncs to GCS at merge, `model_task_config.json` only at deploy).
11. **Write up**: §4 gets the verdict table and per-DAG evidence lines (both logs); §5 the exact diff; §8 the not-confirmed DAGs with their alternative cause and the ds46 scope note. Hand the dispatcher: the PR body (answer line, What = files and values, Why = verdict table path, Validation = step 9 check output + ruff), and the ledger rows to stamp (`dag_id` + `shuffle_fetch_wait:<stage>` for every confirmed DAG; `note` for the not-confirmed ones). Delete `outputs/logs/*.zstd` before hand-off: `.zstd` is not git-ignored in this repo, and 20 logs are about 0.5 GB.

### 3.3 Assumptions to resolve empirically first
- A1. Two logs per DAG agree on the flagged stage (step 5). `tpa_mntn_id_export` stage ids move between runs; identify the stage by fetch-wait share and block count, not by id.
- A2. The script's map-side lookup needs the lower task floor for the monitors (step 1a); confirm it then names a feeding stage for `ipdsc_42_monitor` stages 18/22/26 (§4.2 printed none at the 1000 floor).
- A3. Dataproc Serverless defaults (min 2 / instances 2 / max 1000) and "instances = min when initial is absent" hold on every log, read from the environment surface per log (step 3), not assumed.
- A4. Every ds46-named log attributes cleanly by start time to one of the three launchers (step 4); the staging DAG's batches may write no event log at all.
- A5. The listed app ids still exist when the execute wave starts: all seven ledger ids from 08-24..26 were present on 09-02, but memory records 09-01/09-02 flagged apps disappearing from the bucket within hours. Download first; fall back to step 2c on any 404.
- A6. `uv run --group models` resolves the compile dependencies in the worktree; if the import of `utils_model` / `include.models.code_storage` fails, follow memory `reference_airflow_ti` for the environment before retrying.

### 3.4 Risks
- R1. A higher start count raises cost on runs whose later stages need fewer executors (`aug_log_ip_hourly` is already flagged at 5-16% utilization, 30-50 idle executor-hours per run). Guard: step 7 baseline plus the 1271 kill criterion; revert and mark the ledger row `fix_not_working` if cost per run rises more than the wait saved.
- R2. The two monitors never scaled past 7 executors on a 30-minute TTL (`timeout=1800`) with 9 MB shuffle blocks; the wait there may be block size on few executors, not scale-up timing. Do not change them on the fetch-wait share alone; the step 5 rule must name a feeding stage that started on fewer executors than the run reached.
- R3. Scope: the ticket links the staging spark script for #5, but both flagged logs come from the fangorn 14-day lookback models (§4.3). Editing those two decorators is a scope extension and is a user decision (plan comment). Note also that `fangorn_14day_lookback` batches FAILED twice on 2026-09-03; check with the owner that the model is not mid-change before editing it.
- R4. `model_task_config.json` regeneration also runs `write_registry_artifact()`; a diff beyond the edited keys means the compile environment differs from main (step 9 stop rule).
- R5. Measurement window: the ledger cannot see the change until the first cron run after the Astro deploy, not at merge; a "no change yet" reading the day after merge is expected.
- R6. Binary logs in `outputs/` would be swept into the workspace repo (not git-ignored); step 11 deletes them.

## 4. Investigation & Findings

### 4.1 Planning-wave checks (2026-09-02, no verdicts computed)
- Jira AUDI-1272: Task, Backlog, parent AUDI-1290, labels `hackathon`, `q3_2026`, no story points, no comments; description matches §2.
- Source of truth per DAG: decorator `runtime_properties` in the model file (table in §3.1), compiled into `dags/model_task_config.json` by `model_upload.py --dryrun` (`ctx.compile_models()`); the `advertiser_mid`, `ipdsc_ds_46`, `aug_log_ip_hourly`, `ipdsc_42_monitor` entries mirror their decorators. No builder-level dynamic-allocation keys in any of the ten files. `audience_intent_scoring_staging` is not a model: its DAG submits a plain `DataprocCreateBatchOperator` with `runtime_config = {"version": "2.3"}` and no properties.
- Event logs: `gs://mntn-data-archive-prod/spark-events/` holds 148-157 logs per day (08-26: 153, 08-30: 149, 09-01: 148, 09-02: 157). All seven ledger app ids checked (08-24..26) are still present; sizes 0.7-57 MB. The PHS temp bucket `gs://dataproc-temp-us-central1-995798185124-svhwvc6j/` is listable under the current grant but is not needed: every one of the ten DAGs appears in the batch-fleet archive (backlog files name their app ids under `Populate <model>.<Class>`).
- `gcloud dataproc batches list` with a label filter returns recent SUCCEEDED batches for all ten DAGs (labels in §3.1); `batches describe` exposes `runtimeInfo.approximateUsage.milliDcuSeconds` (advertiser_mid `adv-mid-z32-20260901-000800-1`: 170,817,750 ms-DCU = 47.4 DCU-h) and the batch spec's effective Spark properties (`spark.executor.instances = 25` = minExecutors, `spark.eventLog.dir = gs://mntn-data-archive-prod/spark-events`).
- Tooling: `airflow_optimizer/eventlog.py` in the workspace is byte-identical to `include/spark_optimizer/eventlog.py` on airflow-ti main; `zstandard` is not installed, the CLI fallback (`/opt/homebrew/bin/zstd`) parses fine; the concentration script needs `PYTHONPATH=<workspace root>`.
- Recipe for the config regeneration: `MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun` (memory `reference_airflow_ti`, verified 2026-08-27 in AUDI-1194); `uv` present at `/opt/homebrew/bin/uv`.

### 4.2 Parse check on four logs (`outputs/audi_1272_parse_check_2026_09_02.txt`; one log each, so not a verdict)
- `ipdsc_ds_46` app-20260826040949342-0431: 81 executors registered; stage 3 (340 tasks) 51% fetch wait, 696,320 blocks at 14 KB; fed by stage 1 (2,048 tasks, 9.3 GiB) over 81 executors, 90% on 56, hottest 19.3%, 1 executor live when stage 1 started (start count 4).
- `ipdsc_42_monitor` app-20260825050530606-0927: 7 executors registered in total; stages 18/22/26 (116-117 tasks) at 75-84% fetch wait on 4,194-4,321 blocks of 9.1-9.3 MB; 3 executors live at stage 18 start, 7 at 22/26. No feeding stage printed: every upstream stage has fewer than the script's 1,000-task floor (shuffle partitions 128).
- `audience_intent_scoring_staging_ds46` app-20260826000057091-0058 (fangorn_14day_lookback, see 4.3): 180 registered; stage 20 (8,192 tasks) 59% wait on 85.3M blocks at 55 KB, fed by stage 15 (10,408 tasks, 4,434 GiB) over 180 executors, 90% on 159, hottest 0.8%, 30 live when stage 15 started; stage 23 40% wait, fed by stage 20 (332.5 GiB over 180, hottest 0.7%, 180 live at start).
- `audience_intent_scoring_staging_ds46` app-20260826003036565-0444 (fangorn_household_14day_lookback): 180 registered; stage 8 (92,246 tasks) 69% wait on 214M blocks at 5.8 KB, fed by stage 3 (2,324 tasks, 1,166 GiB) over 142 executors, 90% on 121, hottest 1.5%, 29 live when stage 3 started; stage 11 76% wait, fed by stage 8 (204 GiB over 180, hottest 0.6%, 142 live at start).

### 4.3 The ds46 app name belongs to three launchers
`SparkSession.builder.appName("audience_intent_scoring_staging_ds46")` is set in `spark/machine_learning/audience_intent_scoring_staging_spark.py` L34 (staging DAG, `schedule="11 0 * * *"`), `models/machine_learning/fangorn_14day_lookback.py` L59 (DAG `audience_intent_scoring_14day_lookback`, `21 21 * * *`, task_id `audience_intent_scoring_staging`) and `models/machine_learning/fangorn_household_14day_lookback.py` L59 (DAG `audience_intent_scoring_household_14day_lookback`, `21 21 * * *`). Both 08-26 logs carry min 30 / max 180 / instances 30, the fangorn decorator values, and their start times sit 60-80 s after the fangorn batches' `createTime` (`aud-int-sco-sta-6fv-20260824-212100-1` 2026-08-25T23:59:29Z -> app 00:00:57; `fan-hou-14d-loo-lxz-20260824-212100-1` 2026-08-26T00:29:16Z -> app 00:30:36). The optimizer's `coverage.job_keys` maps the name to `audience_intent_scoring_staging`, which is why the ledger and the ticket carry that DAG id. Ledger pattern: stage 20/23 (plus a 2.2 TiB disk spill) = `fangorn_14day_lookback`; stage 8/11 = `fangorn_household_14day_lookback`.

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
