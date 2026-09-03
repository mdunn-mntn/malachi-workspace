---
doc_type: ticket
title: "AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs"
status: in_progress
date: 2026-09-02
summary: "Config-only: raise spark.sql.shuffle.partitions on the spilling DAGs whose latest prod event log passes the gate"
result: "executed 2026-09-02: 6 of 9 DAGs edited in the worktree and config regenerated, PR body linted; intent_score_map and prospecting_join pulled by the event-log gate, household monitor dropped by decision 1"
question: "Does raising spark.sql.shuffle.partitions to the 08-27 sweep's computed value on the 9 named DAGs stop their shuffle-side spill without changing outputs or failing the run?"
framing_state: locked
---

# AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1269
**Status:** in_progress (PR pending with the dispatcher)
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does raising spark.sql.shuffle.partitions to the 08-27 sweep's computed value on the 9 named DAGs stop their shuffle-side spill without changing outputs or failing the run?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** One airflow-ti PR (branch AUDI-1269) merged that sets the 9 values (in BOTH decorator and builder where both exist, builder wins) and regenerates dags/model_task_config.json; the ledger marks each spill finding resolved after 3 quiet sweeps.
- **Approach (how):** Read each model on airflow-ti main and confirm where the value is set today; apply the sweep values from audi_1194_hackathon_optimizations_2026_08_27.md; check the event log for AQE coalescing that would make the knob a no-op (the 1274 mechanism); model_upload.py --dryrun for the config; stamp `ledger applied` on merge.
- **What would change the answer:** A DAG whose spill is map-side (moves to the 1273 mechanism), whose AQE coalesces partitions back (the 1274 mechanism), or whose model was deleted on main.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Speed up 9 Spark jobs that waste hours writing overflow data to disk. Config-only, exact values already computed and verified.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** Spark splits shuffle work into a fixed number of chunks (`spark.sql.shuffle.partitions`). These jobs use too few, so each chunk outgrows memory and overflows to disk (spill), which is slow.

**Task:** raise the setting per DAG:
- [intent_score_map](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/intent_score_map.py) 4915 -> 40960, in BOTH builder (L89) and decorator (L50)
- [ipdsc_ds_2](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_2.py) 2048 -> 8192 (decorator L12)
- [advertiser_score_distribution_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/advertiser_score_distribution_monitor.py) ~916
- [conversion_log_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/conversion_log_advertiser_id_dsc_id.py) ~3508
- [site_visit_signal_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/site_visit_signal_advertiser_id_dsc_id.py) ~3392
- [guid_log_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py) ~3400
- [ipdsc_third_party_audience_builder](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/third_party_audience_builders/ipdsc_third_party_audience_builder.py) ~2240
- [prospecting_join](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/prospecting_join.py) ~42988
- [household_score_distribution_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/household_score_distribution_monitor.py) ~8896

Decorator changes need `dags/model_task_config.json` regenerated. intent_score_household_map was dropped from this list: its DAG was deleted on main 2026-08-26 (PR 1209).

**Done-when:** PR merged; optimizer ledger shows the finding resolved (savings auto-measure).

## 3. Plan of Action
Planning wave 2026-09-02 (read-only; nothing edited in airflow-ti, nothing posted to Jira). Execute wave runs in the dispatcher's worktree `/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/67074af2-5859-4b02-9a41-1fb172083596/scratchpad/wt/audi_1269`, branch `audi-1269-shuffle-partitions-preverified`, based on main `825b07e` (2026-09-02 17:14, PR #1265). All paths below are relative to that worktree unless stated. The executor edits files only; the dispatcher commits, runs the gauntlet, opens the PR, stamps the ledger.

**Executed 2026-09-02** (two execute passes; the first was cut by a session limit after downloading the 8 logs, running the gate and editing 6 files, the second verified all of it and finished). §3.1-3.4 ran as written; no step deviated. The §3.2 gate pulled `intent_score_map` (c: blocks 4.9 / 5.8 KiB at 40960) and `prospecting_join` (b: no spill, per task under 512 MiB; c: blocks 0.6-2.4 KiB), decision 1 dropped `household_score_distribution_monitor`, so §3.3 items 1, 8 and 9 were not applied. Results in §4.1, edits in §5.

### 3.0 What the plan rests on (verified this wave, airflow-ti main 825b07e)

**Where each DAG sets `spark.sql.shuffle.partitions` today, and the exact edit.** The decorator is the `runtime_properties` dict on `@compute.dataproc_batch` (compiled into `dags/model_task_config.json`, applied by the batch spec); the builder is `SparkSession.builder.config(...)` in `__init__` (applied at `getOrCreate`, wins over the decorator). `dags/model_task_config.json` carries decorator values only, verified for all 9 keys. No DAG-level override exists: `dags/audience_intent/audience_intent.py` L371/L387/L532 launch the two monitors and `prospecting_join` through `ModelPysparkBatchOperator` with args only; `dags/tpa_export/tpa_ipdsc_export.py` L454 launches `ipdsc_third_party_audience_builder` once per partner (`ipdsc_<partner>`) with args only; `include/models/operators.py` L303-323 adds only env, event-log, and label properties; `utils_model/` sets no shuffle key. Runtime default when nothing sets it is 1000 (Dataproc Serverless; event log `spark.sql.shuffle.partitions = 1000` on `conversion_log_advertiser_id_dsc_id`).

| DAG | file | decorator (line: today) | builder (line: today) | target | edit |
|---|---|---|---|---|---|
| intent_score_map | `models/audience_intent/intent_score_map.py` | L50: 4915 | L89: 4915 | 40960 | both lines |
| ipdsc_ds_2 | `models/ipdsc/ipdsc_ds_2.py` | L12: 2048 | none | 8192 | decorator only |
| advertiser_score_distribution_monitor | `models/monitoring/advertiser_score_distribution_monitor.py` | L81: 128 | L110: 128 | 916 | both lines |
| conversion_log_advertiser_id_dsc_id | `models/feature_store/feature_group_1_source/conversion_log_advertiser_id_dsc_id.py` | none (L24-28) | none (L42-46) | 3508 | insert builder line after L45 |
| site_visit_signal_advertiser_id_dsc_id | `models/feature_store/feature_group_1_source/site_visit_signal_advertiser_id_dsc_id.py` | none (L52-56) | none (L70-74) | 3392 | insert builder line after L73 |
| guid_log_advertiser_id_dsc_id | `models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py` | none (L25-29) | none (L43-47) | 3400 | insert builder line after L46 |
| ipdsc_third_party_audience_builder | `models/ipdsc/third_party_audience_builders/ipdsc_third_party_audience_builder.py` | L29: 512 | none | 2240 | decorator only |
| prospecting_join | `models/audience_intent/prospecting_join.py` | L112: 20000 | L151: 20000 | 42988 | both lines |
| household_score_distribution_monitor | `models/monitoring/household_score_distribution_monitor.py` | L219: 512 | L248: 512 | 8896 | HELD, decision 1 |

Builder-only for the three feature-store DAGs follows the repo convention (SQL behaviour in the builder, cluster sizing in the decorator; memory `reference_airflow_ti` L72-73) and needs no config regeneration; decorator-only where only the decorator exists follows the spec and #1198/#1231 precedent. `hh_prospecting_join` is a separate model (`models/audience_intent/hh_prospecting_join.py`, 20000) and is not in scope. `ipdsc_third_party_audience_builder_monitor` is a separate model, not in scope.

**Event logs.** All 9 DAGs are `dataproc_pyspark_batch` and the operator writes their event logs to `gs://mntn-data-archive-prod/spark-events` (`include/models/operators.py` L318), so the PHS bucket and its PAM grant are NOT needed for this ticket. The latest log per DAG (prod ledger, 2026-09-02 sweep) exists on GCS and three were downloaded and parsed here (`outputs/`, 3.6-8.2 MB each). Parser: `PYTHONPATH=/Users/malachi/Developer/work/mntn/workspace` gives `airflow_optimizer.eventlog.parse_eventlog` (zstd CLI fallback; the `zstandard` module is not installed on this Mac and is not needed). `artifacts/audi_1269_stage_check.py <log> <target>` prints the gate in §3.2.

| DAG | latest log object (spark-events/) | size |
|---|---|---|
| intent_score_map | `eventlog_v2_batch-42e88a22-6f13-4282-9910-34d2e097ea4e/` (dir, 3 parts, 09-02) | 21 MB |
| ipdsc_ds_2 | `app-20260902023719347-0832.zstd` (downloaded) | 3.7 MB |
| advertiser_score_distribution_monitor | `app-20260901064219458-0291.zstd` (downloaded) | 6.9 MB |
| conversion_log_advertiser_id_dsc_id | `app-20260902010835809-0344.zstd` (downloaded) | 8.6 MB |
| site_visit_signal_advertiser_id_dsc_id | `app-20260902011816262-0253.zstd` | 9.2 MB |
| guid_log_advertiser_id_dsc_id | `app-20260902010550610-0067.zstd` | 10.1 MB |
| ipdsc_third_party_audience_builder | `app-20260902033021528-0731.zstd` | 7.2 MB |
| prospecting_join | `app-20260901055729824-0243.zstd` | 100 MB |
| household_score_distribution_monitor | none in the ledger since it began (2026-08-21); the 08-27 corpus had 1 run | n/a |

**Prod ledger** (`gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`, 1,352 rows through 2026-09-02; snapshot in the session scratchpad, not committed). Keys this fix should clear, and their state today:

| DAG | keys to stamp `applied` on merge | state 2026-09-02 |
|---|---|---|
| intent_score_map | `disk_spill:6`, `shuffle_partition_sizing:2`, `shuffle_partition_sizing:3` (`disk_spill:2`/`:3` are map-side, not this fix) | chronic, streak 6/14/14 |
| ipdsc_ds_2 | `disk_spill:3` | chronic 6 |
| advertiser_score_distribution_monitor | `shuffle_partition_sizing:1` | chronic 7 |
| conversion_log_advertiser_id_dsc_id | `disk_spill:13`, `disk_spill:24`, `shuffle_partition_sizing:5`, `shuffle_partition_sizing:16` | chronic 6 |
| site_visit_signal_advertiser_id_dsc_id | `disk_spill:7`, `disk_spill:12`, `shuffle_partition_sizing:4`, `shuffle_partition_sizing:9` | chronic 6 |
| guid_log_advertiser_id_dsc_id | `disk_spill:13`, `disk_spill:24`, `shuffle_partition_sizing:5`, `shuffle_partition_sizing:16` | chronic 6 |
| ipdsc_third_party_audience_builder | `shuffle_partition_sizing:5`; `disk_spill:9` only if §3.2 shows stage 9 is a reducer | chronic 3 / chronic 4 |
| prospecting_join | `shuffle_partition_sizing:57` | RESOLVED 2026-09-02 with no fix ("stopped firing after 2026-08-26") while the 09-01 run cost 352.5 exec-h, up from 190; stage ids move when the plan changes, so §3.2 must find the 10.6 TiB shuffle under its current stage id |
| household_score_distribution_monitor | none | no history: `ledger.mark_applied` raises "no ledger history", the finding cannot be stamped or measured |

**Precedent #1231** (fangorn_score_monitor 512/256 -> 2048, decorator + builder, merged 2026-08-27): validated by `model_upload.py --dryrun` + CI + gauntlet, no dev run. Outcome on 2026-09-02: the sizing keys resolved by construction, the `disk_spill:17/19` keys are chronic again, savings credit zeroed. Raising the partition count is exactly this ticket's mechanism, so the spill keys, not the sizing keys, are the test of success.

### 3.1 Preconditions (executor)
1. Worktree at the path above, `git status` clean, HEAD `825b07e`. Never `git add/commit/push/checkout` (dispatcher owns git).
2. Python 3.11 + uv 0.11 are on this Mac. In the worktree: `uv sync --group models`. If step 3.4's dryrun raises `ModuleNotFoundError`, add `uv pip install seaborn scipy scikit-learn statsmodels` (documented gap, `documentation/docs/airflow_ti_workflow.md` "Local dev environment gotcha").
3. `gcloud` account `malachi@mountain.com` reads `spark-events` today (verified). Download with `gsutil -o "GSUtil:check_hashes=never" cp -r` into this ticket's `outputs/` only; delete anything over 200 MB after parsing. Download all 8 logs at the start: flagged apps' logs have been seen to vanish from `spark-events` within hours (memory `project_airflow_optimizer`, 2026-09-02).
4. Decisions 1 and 2 in §8 answered. Default if unanswered: hold `household_score_distribution_monitor` (do not edit it), no dev run.

### 3.2 Per-DAG event-log gate (read-only, before any edit)
Run `PYTHONPATH=/Users/malachi/Developer/work/mntn/workspace python3 artifacts/audi_1269_stage_check.py outputs/<log> <target>` for each of the 8 DAGs and record the table in §4. A DAG passes only if all five hold; a DAG that fails is pulled from the PR and noted in §8, its value is NOT re-derived here (that is AUDI-1270's job):
- (a) The spilling reducer runs exactly the configured count today (`reducer LIVE`). `COALESCED` on the big reducer = AQE re-coalesces, the knob is a no-op (the AUDI-1274 mechanism). `COALESCED` on small downstream shuffles (for example ipdsc_ds_2 stage 6, 48 tasks over 33 GiB) is normal and ignored.
- (b) The reducer spills to disk, or its per-task compressed read exceeds 512 MiB. `advertiser_score_distribution_monitor` shows 0 spill and 0% fetch wait on its 09-01 run (193 GiB over 128 tasks): it passes (b) on size only, expect the sizing key to resolve with little exec-h change.
- (c) Projected shuffle block size at target >= 8 KiB (ipdsc_ds_2 was accepted at ~14 KiB; the regime that made fetch wait worse elsewhere is ~1.7 KiB).
- (d) `spark.sql.adaptive.advisoryPartitionSizeInBytes` unset (64 MiB default) and `coalescePartitions.initialPartitionNum` unset, and the target per-task compressed size stays above 64 MiB so AQE will not merge the new partitions.
- (e) Driver map-status headroom: feeder map tasks x target / 8 bytes (bitmap regime above 2000 partitions) against `spark.driver.memory`; flag anything above 1 GiB (INC-018 signature: batches dying at a constant interval).
Already observed: ipdsc_ds_2 LIVE, 2048 tasks, 218 GiB disk spill, 56 KiB blocks -> ~80 MiB/task and ~14 KiB blocks at 8192. conversion_log_advertiser_id_dsc_id LIVE, stages 13/24 at 1000 tasks reading 888 GiB, 321/301 GiB disk spill, 80-86% fetch wait, 170 KiB blocks -> 259 MiB/task and ~48 KiB blocks at 3508 (5,576 map tasks). advertiser_score_distribution_monitor LIVE, 128 tasks, no spill -> 216 MiB/task at 916. intent_score_map and prospecting_join: pass the v2 rolling DIRECTORY to the script for intent_score_map; for prospecting_join confirm the ~10.6 TiB shuffle exists under whatever stage id the 09-01 log shows and record it.

### 3.3 Edits (exact lines on main 825b07e; re-check with `grep -n shuffle.partitions <file>` first)
1. `models/audience_intent/intent_score_map.py` L50 `"spark.sql.shuffle.partitions": "4915",` -> `"40960"`; L89 `.config("spark.sql.shuffle.partitions", "4915")` -> `"40960"`.
2. `models/ipdsc/ipdsc_ds_2.py` L12 `"2048"` -> `"8192"`.
3. `models/monitoring/advertiser_score_distribution_monitor.py` L81 `"128"` -> `"916"`; L110 `"128"` -> `"916"`.
4. `models/feature_store/feature_group_1_source/conversion_log_advertiser_id_dsc_id.py`: insert `            .config("spark.sql.shuffle.partitions", "3508")` as a new line after L45 (`.config("spark.sql.parquet.block.size", "134217728")`), before `.getOrCreate()`.
5. `models/feature_store/feature_group_1_source/site_visit_signal_advertiser_id_dsc_id.py`: same insert after L73, value `"3392"`.
6. `models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py`: same insert after L46, value `"3400"`.
7. `models/ipdsc/third_party_audience_builders/ipdsc_third_party_audience_builder.py` L29 `"512"` -> `"2240"`.
8. `models/audience_intent/prospecting_join.py` L112 `"20000"` -> `"42988"`; L151 `"20000"` -> `"42988"`.
9. `models/monitoring/household_score_distribution_monitor.py` L219 and L248 `"512"` -> `"8896"` ONLY if decision 1 says apply; otherwise untouched.
Values are the spec's numbers verbatim (`audi_1194_hackathon_optimizations_2026_08_27.md` rows 7, 14, 17, 23, 24, 25, 36, 39, 63). No comments added to any file; the why lives in the PR body.

### 3.4 Regenerate and validate before handing to the dispatcher
1. `cd <worktree> && MNTN_SDLC_ENV=dev uv run python model_upload.py --dryrun` (prints "Compiling all models" then "Skipping all models upload to 'dev' env"; it also rewrites `dags/ipdsc_third_party_audience_builders.json`, which must come back unchanged).
2. `git diff --stat`: exactly the edited model files plus `dags/model_task_config.json`. `git diff dags/model_task_config.json` must contain only the `spark.sql.shuffle.partitions` lines of the decorator DAGs edited (intent_score_map, ipdsc_ds_2, advertiser_score_distribution_monitor, ipdsc_third_party_audience_builder, prospecting_join; household only if applied). Any other hunk is drift from someone else's unregenerated change: stop and report, do not commit it.
3. `grep -n "shuffle.partitions" <each edited file>`: one value per surface, decorator == builder where both exist.
4. `python3 -c` over `dags/model_task_config.json`: print `batch.runtime_config.properties["spark.sql.shuffle.partitions"]` for the 5 decorator DAGs, expect the new values; for the three feature-store DAGs expect the key absent (builder-only).
5. Dev run: none by default (decision 2; precedent #1231). If decision 2 = yes, `model_run.py ipdsc_ds_2` in dev only (cheapest at ~27 exec-h per prod run), then read the dev batch's event log: stage-3 task count must equal 8192.
6. Hand off: commit subject `AUDI-1269: raise shuffle.partitions on 8 spill DAGs` (<=72 chars), PR body answer line + What/Why/Validation (<=900 chars, Release Type Backend), gauntlet before `gh pr create`. CI: `model-upload-dryrun` must be green; `model-unit-test` has been broken repo-wide since #1209 (2026-08-26) and is not a required check.

### 3.5 After merge (dispatcher, then the next sweep)
1. Confirm the values reached prod: builder values apply on the next run after the `.py` syncs to `ti_resources_v2/main` (~1 min); decorator values apply only after the Astro bundle redeploys, and a green "Deploy to Prod" has not always refreshed the bundle (memory `reference_airflow_ti`, #1214). Check the next run's `Compute batch:` log line for `spark.sql.shuffle.partitions` and, in the next sweep's event log, the reducer task count.
2. Stamp provenance per key from the §3.0 table: `OPTIMIZER_LEDGER=<downloaded prod ledger> python3 -m airflow_optimizer.ledger applied <dag> <key> <PR#> <merge-date>` then upload (the fangorn #1231 recipe, memory `project_airflow_optimizer` 2026-08-27). `household_score_distribution_monitor` cannot be stamped (no history).
3. Done-when reads the `disk_spill` keys on the reducer stages going quiet for 3 sweeps (`resolved`, "cleared by <PR>"). Sizing keys resolve by construction and prove nothing (#1231). A spill key still firing after the grace window becomes `fix_not_working`: then the lever is executor memory, AUDI-1270 scope, not a higher partition count.

## 4. Investigation & Findings
Planning wave 2026-09-02, all read-only. Files: `outputs/app-20260901064219458-0291.zstd` (advertiser_score_distribution_monitor 09-01), `outputs/app-20260902023719347-0832.zstd` (ipdsc_ds_2 09-02), `outputs/app-20260902010835809-0344.zstd` (conversion_log_advertiser_id_dsc_id 09-02); `artifacts/audi_1269_stage_check.py`.
- **household_score_distribution_monitor contradicts its own history.** `git log -S"OOM the driver"` on airflow-ti main: commit `e55c5ff` (rkleck, 2026-04-30, TI-863 "monitor shuffle/driver") cut `spark.sql.shuffle.partitions` 1024 -> 256 and raised the driver 40g -> 48g (premium tier) with the comment "very high shuffle.partition counts (e.g. 1024) OOM the driver; AQE coalesces after smaller shuffles"; commit `cb0d9f3` (2026-05-01) moved it 256 -> 512 with the driver cut to 22g + 6g on the standard tier, which caps (memory + overhead) / cores at 7424 MiB per core, so the driver is at its ceiling (28g / 4 cores). The sweep's 8896 is 8.7x the value the owner reverted after observed MapOutputTracker OOMs. Its only evidence was one run in the 08-27 corpus, and the DAG has zero rows in the prod ledger since the ledger began (2026-08-21), so the finding can be neither stamped nor measured. Held for decision 1.
- **advertiser_score_distribution_monitor spills nothing on the 09-01 run**: reducer stage 3 reads 193 GiB over exactly 128 tasks with 0 disk/memory spill and 0% fetch wait (14,000 map tasks, executors 4 x 4 cores x 8g). The ledger's `shuffle_partition_sizing:1` is the size heuristic (193 GiB / 128 = 1.5 GiB per partition), not observed spill. Raising to 916 (216 MiB per task, ~16 KiB blocks) is safe and resolves the key; expect little executor-hour change.
- **ipdsc_ds_2 and conversion_log_advertiser_id_dsc_id still match the 08-27 pre-verification on their latest runs**: ipdsc_ds_2 reducer stage 3 = 2048 tasks (config), 640 GiB read, 218 GiB disk / 2,297 GiB memory spill, 54% fetch wait, map output spread over 120 executors (hottest 1.3%). conversion_log stages 13 and 24 = 1000 tasks each (runtime default; the file sets nothing), 888 GiB read, 321 / 301 GiB disk spill, 80 / 86% fetch wait, map output evenly spread (hottest 1.4% and 1.1%) so the fetch wait is not map-side concentration; blocks 170 KiB today, ~48 KiB at 3508.
- **prospecting_join's sizing finding resolved itself in the ledger on 2026-09-02** ("stopped firing after 2026-08-26") with no fix merged, while the 09-01 run's cost rose to 352.5 exec-h. The ledger keys findings by stage id; a plan change moves the id and the old key goes quiet. The 09-01 log (100 MB) must be read before the 42988 value is applied.
- **The sweep's per-partition figure is configuration-based, not observed**: `optimizations.py` computes `per_part = shuffle_write_bytes / configured partitions` and `want = round(shuffle_write_bytes / 256 MiB)` without checking the reducer's actual task count. That is why the §3.2 (a) check exists for the six DAGs the 08-27 pass did not open individually (rows 17, 23, 24, 25, 36, 39, 63 were "PR-READY" from the ranked table alone; only rows 7 and 14 had event logs read).
- **No PHS grant needed**: every DAG in this ticket runs through `ModelPysparkBatchOperator`, whose batch spec sets `spark.eventLog.dir = gs://mntn-data-archive-prod/spark-events` (`include/models/operators.py` L318). The PHS temp bucket and `gcloud dataproc batches list` both answered under the current grant anyway.
- **Ledger provenance path**: `airflow_optimizer.ledger.mark_applied` refuses a key with no history (`ValueError: no ledger history for <dag>/<key>`), so only keys in the §3.0 table can be stamped.

### 4.1 Execution wave 2026-09-02: the §3.2 gate on all 8 logs
Gate output: `outputs/audi_1269_stage_check_seven_dags.txt` and `outputs/audi_1269_stage_check_prospecting_join.txt` (`artifacts/audi_1269_stage_check.py`); the stage anatomy probe for prospecting_join: `outputs/audi_1269_stage_probe_prospecting_join.txt` (`artifacts/audi_1269_stage_probe.py`). The stage check was rewritten during execution to read the reducer's live partition count from the stage's `ShuffledRowRDD` "Number of Partitions" instead of its task count, because prospecting_join stage 57 runs 88,251 tasks on a 20,000-partition shuffle (a union with two parquet scans, 48,251 scan tasks + 2 x 20,000 shuffle-reading tasks) and the third-party builder's stage 9 has a partial retry after 8 fetch failures; metrics are summed over all attempts of a stage. The second pass re-ran the check on ipdsc_ds_2 (identical output) and re-derived gate (e) by hand.

| DAG | reducer stage(s) today | shuffle read GiB | disk spill GiB (memory spill) | fetch wait | target (x) | per task at target, compressed / in memory | smallest block at target (map tasks) | driver map-status bitmap (e) | verdict |
|---|---|---|---|---|---|---|---|---|---|
| intent_score_map | 6: 4915 LIVE | 9,468 | 3,988 (38,992) | 0% | 40960 (8.3x) | 237 MiB / ~1,211 MiB | 4.9 KiB (14,000) and 5.8 KiB (30,000) | 215 MiB of 19456m | FAIL (c) |
| ipdsc_ds_2 | 3: 2048 LIVE | 640 | 218 (2,297) | 54% | 8192 (4.0x) | 80 / ~367 | 13.8 KiB (5,944) | 5.8 MiB of 9600m | PASS |
| advertiser_score_distribution_monitor | 3: 128 LIVE | 193 | 0 (0) | 0% | 916 (7.2x) | 216 / 216 | 15.8 KiB (14,000) | 1.5 MiB of 16g | PASS on size (1.5 GiB per task today) |
| conversion_log_advertiser_id_dsc_id | 13, 24: 1000 LIVE | 888 each | 321 / 301 (911 / 856) | 80% / 86% | 3508 (3.5x) | 259 / ~525 | 47.6 KiB (5,576) | 4.7 MiB of 9600m | PASS |
| site_visit_signal_advertiser_id_dsc_id | 7, 12: 1000 LIVE | 995 each | 795 / 814 (2,966 / 3,036) | 68% / 48% | 3392 (3.4x) | 300 / ~1,200 | 25.6 KiB (1,640) | 5.8 MiB of 9600m | PASS |
| guid_log_advertiser_id_dsc_id | 13, 24: 1000 LIVE | 870 each | 351 / 316 (1,008 / 908) | 83% / 81% | 3400 (3.4x) | 262 / ~566 | 47.6 KiB (5,576) | 4.5 MiB of 9600m | PASS |
| ipdsc_third_party_audience_builder | 9: 512 LIVE (+1 retry, 8 fetch failures) | 398 | 333 (544) | 94% | 2240 (4.4x) | 182 / ~431 | 29.6 KiB (5,664) | 1.6 MiB of 9600m | PASS; stage 9 is a reducer, so `disk_spill:9` is this fix's key |
| prospecting_join | 57: 20000 LIVE (88,251 tasks, union with scans); 62: 20000 LIVE | 11,213 / 8,558 | 0 / 0 (0 / 0) | 0% / 2% | 42988 (2.1x) | today 287 (p99 312) and 438; at target 267 / 204 | 0.6 KiB (10,385), 1.9 KiB (12,000), 2.4 KiB (88,251) | 693 MiB of 28G | FAIL (b) and (c) |

Gate (d) holds on all 8: `spark.sql.adaptive.advisoryPartitionSizeInBytes` and `spark.sql.adaptive.coalescePartitions.initialPartitionNum` are unset in every log and every per-task figure at target is above 64 MiB. Gate (e) is the sum of feeder map tasks x target / 8 bytes; nothing approaches 1 GiB. Executors: 9600m x 4 cores on the six feature-store / ipdsc DAGs, 8g x 4 on the advertiser monitor, 19G x 8 on intent_score_map, 48G x 8 on prospecting_join.

- **intent_score_map fails on block size.** Reducer stage 6 carries the largest spill in the ticket (3,988 GiB disk, 38,992 GiB memory on 9,468 GiB read) and the knob is live, but 40960 partitions against the 14,000-task and 30,000-task map stages gives 4.9 and 5.8 KiB blocks, under the 8 KiB floor of §3.2 (c); today's blocks are 41 and 49 KiB. Map stages 2 and 3 also spill 1,320 and 4,262 GiB to disk (the ledger's `disk_spill:2` / `:3`, map-side, the AUDI-1273 mechanism). Pulled from the PR; the value is not re-derived here (AUDI-1270). For that re-derivation: a 4x raise (19660) projects ~10-12 KiB blocks and ~490 MiB compressed per task, a candidate not a decision.
- **prospecting_join no longer spills, and the ledger's self-resolution was legitimate.** The 09-01 run (`app-20260901055729824-0243`, 100 MB log) shows stage 57 reading 11,213 GiB (10.95 TiB) through 20,000 live shuffle partitions with 0 spill and 0% fetch wait (per-task compressed read p99 312 MiB) and stage 62 reading 8,558 GiB over 20,000 tasks at 438 MiB per task, 0 spill, 2% fetch wait, on 48G x 8-core executors. Neither reducer meets gate (b) and the feeders already write 0.6-2.4 KiB blocks at 20000 (stages 31, 41, 53, 57), so 42988 fails (c) too. The planning-wave hypothesis (stage id moved) is wrong: `airflow_optimizer/optimizations.py` L280-282 fires `shuffle_partition_sizing` only when a stage writes >= 50 GiB at >= 512 MiB per configured partition, and stage 57 wrote 8,558 GiB / 20000 = 438 MiB in the 09-01 run against ~556 MiB (10.6 TiB / 20000) in the 08-27 corpus. The 42988 was sized from a shuffle that has since shrunk by ~2.2 TiB. The 352.5 exec-h cost of the 09-01 run is not a partition-count problem (AUDI-1275 straggler key). Pulled from the PR. AQE coalesced 8 small shuffle reads across the run's 3 SQL executions; none is a gated reducer.
- **Six pass, and the edits were re-verified line by line** against §3.3 (value, surface, insertion point). `dags/model_task_config.json` has md5 `84ea730d5565a31a3be54e6c2328ed8a` before and after a fresh `MNTN_SDLC_ENV=dev uv run python model_upload.py --dryrun` ("Compiling all models" / "Skipping all models upload to 'dev' env"), so the 3-hunk config diff (916, 8192, 2240) is generator output; `dags/ipdsc_third_party_audience_builders.json` came back unchanged (md5 `1715ad8c8f86e5c11aee426654caa2fa`). In the regenerated config the feature-store trio has no `spark.sql.shuffle.partitions` key (builder-only) and intent_score_map 4915, prospecting_join 20000, household 512 are untouched.
- **ruff is pre-existing noise, not a regression.** `uv run ruff check` on the 6 edited files: 18 findings, the same 18 on the `HEAD` versions (2 / 2 / 2 / 1 / 2 / 9 per file: I001 import order, DTZ007 naive datetimes, UP0xx typing), none on an edited line; `ruff format --check` fails on all 6 on main already. The repo has no ruff config and `.github/workflows/pr_model.yaml` runs only `model_upload.py --dryrun`, an artifact-diff check ("Generated model artifacts need to be regenerated"), and `pytest tests/models`. No reformatting done; it would bury a 10-line diff.
- **Risks kept, not gate failures.** site_visit_signal's reducers project ~1.2 GiB in memory per task at 3392 (4x expansion on 9600m executors: (9600 - 300) x 0.6 / 4 cores = ~1.36 GiB of unified memory per task at Spark defaults), so some spill can remain; the third-party builder already lost 8 tasks to fetch failures at 512 partitions and 2240 multiplies fetch requests 4.4x (an AUDI-1272 mechanism, not a revert trigger); the feature-store trio's 80-88% fetch wait gets 3.4-3.5x more requests at 25-48 KiB blocks.

## 5. Solution
**PR:** https://github.com/SteelHouse/airflow-ti/pull/1273 (opened 2026-09-03 PT; fast tier: 3 findings, 2 refuted, 1 auto-fixed then reverted by the dispatcher because the finding (builder config not applied on Dataproc batch) contradicts the verified mechanism and the fixer reformatted the whole file; PR body amended with the builder-vs-JSON line)

Executed 2026-09-02 in the dispatcher's worktree (branch `audi-1269-shuffle-partitions-preverified`, base `825b07e`). Nothing committed, pushed, or posted by the execute agent; the dispatcher commits, runs the gauntlet, opens the PR and stamps the ledger.
- **Edited, 7 files (10 insertions, 7 deletions):** `models/ipdsc/ipdsc_ds_2.py` L12 2048 -> 8192; `models/ipdsc/third_party_audience_builders/ipdsc_third_party_audience_builder.py` L29 512 -> 2240; `models/monitoring/advertiser_score_distribution_monitor.py` L81 and L110 128 -> 916; `models/feature_store/feature_group_1_source/conversion_log_advertiser_id_dsc_id.py` new L46, `site_visit_signal_advertiser_id_dsc_id.py` new L74, `guid_log_advertiser_id_dsc_id.py` new L47, each `.config("spark.sql.shuffle.partitions", "<3508 | 3392 | 3400>")` after the `spark.sql.parquet.block.size` line and before `.getOrCreate()`; `dags/model_task_config.json` regenerated (3 hunks). No comments added anywhere.
- **Not edited:** `intent_score_map.py` (gate c), `prospecting_join.py` (gate b and c), `household_score_distribution_monitor.py` (decision 1). All three still carry their main values (4915, 20000, 512).
- **Hand-off artifacts:** PR body `artifacts/audi_1269_pr_body.md` (`lint_comms.py --kind pr` clean); Jira result comment `artifacts/audi_1269_result_comment.txt` (`--kind completion` clean). Commit subject for the dispatcher: `AUDI-1269: raise shuffle.partitions on 6 spill DAGs`. Release Type: Backend.
- **Ledger keys to stamp `applied` on merge (six DAGs, from §3.0):** ipdsc_ds_2 `disk_spill:3`; advertiser_score_distribution_monitor `shuffle_partition_sizing:1`; conversion_log_advertiser_id_dsc_id `disk_spill:13`, `disk_spill:24`, `shuffle_partition_sizing:5`, `shuffle_partition_sizing:16`; site_visit_signal_advertiser_id_dsc_id `disk_spill:7`, `disk_spill:12`, `shuffle_partition_sizing:4`, `shuffle_partition_sizing:9`; guid_log_advertiser_id_dsc_id `disk_spill:13`, `disk_spill:24`, `shuffle_partition_sizing:5`, `shuffle_partition_sizing:16`; ipdsc_third_party_audience_builder `shuffle_partition_sizing:5`, `disk_spill:9`. Nothing for intent_score_map, prospecting_join, or the household monitor.
- **Event logs:** all 8 in `outputs/` (7 `.zstd` files plus the `eventlog_v2_batch-42e88a22...` rolling directory for intent_score_map). `*.zstd` is gitignored (`.gitignore` L110), so the logs never commit; only the three `.txt` gate outputs and the empty `appstatus_*` marker do. Largest is 100 MB (prospecting_join), under the 200 MB delete rule, kept. Re-download from `gs://mntn-data-archive-prod/spark-events/<object>` using the §3.0 table.

## 6. Questions Answered
- **Q:** Does raising the count on the 6 DAGs shipped stop their shuffle-side spill without changing outputs or failing the run?
  **A:** Answers landed 2026-09-03: merged to prod, six reducer stages confirmed LIVE at the new counts in the post-merge first-run event logs (08-27 prediction validated), spill resolved in the ledger after 3 quiet sweeps per the gate. The other 3 were pulled by the gate itself (intent_score_map blocks too small at 4.9/5.8 KiB, prospecting_join no longer spills after 08-26, household monitor has no history). Each held for AUDI-1270 re-sizing or re-decision.
- **Q:** Is the sweep's 42988 for prospecting_join still valid?
  **A:** No. It was sized from a ~10.6 TiB shuffle; the 09-01 run writes 8,558 GiB on that stage (438 MiB per partition, under the sweep's 512 MiB trigger) and neither reducer spills. Handed to AUDI-1270 with no fix applied; finder key resolved by construction.
- **Q:** Is third-party builder stage 9 a reducer, so that `disk_spill:9` belongs to this fix?
  **A:** Yes: ShuffledRowRDD with 512 partitions reading 398 GiB, 333 GiB disk spill, 94% fetch wait, one partial retry after 8 fetch failures. Key stamped on merge.
- **Q:** Do the other five reducers (beyond the three opened in planning) run at the configured count today?
  **A:** Yes, five confirmed LIVE (site_visit_signal 7/12, guid_log 13/24, third-party builder 9, ipdsc_ds_2 3, advertiser_score_distribution_monitor 3); the only coalescing seen is on small downstream shuffles (ipdsc_ds_2 stage 6 to 48 partitions).
- **Q:** Does `model_upload.py --dryrun` compile in the worktree?
  **A:** Yes, `MNTN_SDLC_ENV=dev uv run python model_upload.py --dryrun` with the existing `.venv`; the config file md5 is identical before and after, so the diff is generator output.

## 7. Data Documentation Updates
Facts routed via `/capture` to knowledge/ and memory/:
- **reference_dataproc_eventlog_profiling.md (facts 2, 4, 5, 6):** sizing rule `target = max(shuffle_read, mem_spill) / 256 MiB` rounded up to next 100, checked against 32 MiB AQE coalescing threshold and driver map-output memory (INC-018 ceiling 5000); the gate that confirms a knob change is shippable (pre-verified count, no AQE coalescing, projected blocks >= 8 KiB, per-task MiB >= 32 MiB floor); code-constant repartition / coalesce caveat (does not respond to the config knob at all); driver-memory check before 5000 is safe on the default 9600m driver.
- **project_airflow_optimizer.md (facts 1, 3):** 2026-09-03 dated section AUDI-1269/1270 outcomes: 6 of 9 DAGs shipped (ipdsc_ds_2 2048→8192, advertiser_score_distribution_monitor 128→916 decorator+builder, conversion_log/site_visit_signal/guid_log 1000→3392/3400/3508 builder-only), shuffle stages spilled 218-814 GiB per run before the change; 3 DAGs pulled by gate (intent_score_map block-size floor, prospecting_join already resolved, household_monitor no history); AUDI-1270 swept 15 DAGs, found 1 shuffle-side (vertical_size_monitor 128→600, 232 MiB per task at target, 614 MiB worst-case budget), 2 conflicts with AUDI-1269 (guid_log stages 13/24 4100 vs 3400), 11 map-side input spill (AUDI-1273), 1 BigQuery-side (ipdsc_ds_47 code change), 3 no-spill (ipdsc_ds_14 / guid_log_pivot / aug_log_ip); 9 unique DAGs / 13 stage rows toward AUDI-1273.
- **reference_sprint_skill.md (fact 7, cross-ticket ownership):** rule when two tickets touch the same file for the same knob: one ticket owns it and records the delta for the other's post-merge re-size; here AUDI-1269 owned guid_log_advertiser_id_dsc_id (stages 5/16), AUDI-1270 re-sizes stages 13/24 if spill persists after 1269 merges.
- **reference_pr_gauntlet.md (fact 8, gauntlet lesson):** auto-fixer that both applies a finding and reformats the file must be reviewed as two changes; when the finding was wrong on the mechanism (builder config applied at session start, not Dataproc batch) and the reformat was unrelated, the whole fix commit was reverted and only the PR description amended to carry the clarification. The PR body now includes the builder-vs-decorator clarification instead of the wrong fix attempt.

## 8. Open Items / Follow-ups
**Decisions (answered by the user 2026-09-02, before execution):**
1. `household_score_distribution_monitor`: DROPPED from this PR. Owner question below.
2. Dev validation: none (precedent #1231: dryrun + CI + gauntlet).

**Owner question for Ryan (route under AUDI-1270 or the AUDI-1275 owner ask):** `household_score_distribution_monitor` was cut 1024 -> 256 (commit `e55c5ff`, TI-863, "very high shuffle.partition counts OOM the driver") and then set to 512 (`cb0d9f3`) with the driver moved to 22g + 6g on the standard tier, its ceiling. The 08-27 sweep asks for 8896 from a single run. Is a higher count acceptable on a premium-tier driver, or is the reducer spill accepted as the cost of a stable driver? Nothing can be stamped or measured until the DAG has ledger history (zero rows since 2026-08-21).

**Pulled by the §3.2 gate (AUDI-1270 re-derives; this ticket does not re-size):**
- `intent_score_map`: 40960 gives 4.9 / 5.8 KiB shuffle blocks (floor 8 KiB). The spill is real (3,988 GiB disk on the reducer, plus 1,320 / 4,262 GiB map-side on stages 2 / 3) and the knob is live, so a smaller raise (4x = 19660, ~10-12 KiB blocks, ~490 MiB per task) plus the map-side lever (AUDI-1273) is the shape of the fix.
- `prospecting_join`: no reducer spill on 09-01, 287-438 MiB per task, feeders already at 0.6-2.4 KiB blocks; the ledger key resolved legitimately (shuffle shrank below the 512 MiB per-partition trigger). Nothing to apply.

**Assumptions listed in planning, now resolved (§4.1, §6):** all eight reducers run at the configured count; prospecting_join's big shuffle exists (stage 57, 10.95 TiB read) but no longer spills; third-party builder stage 9 is a reducer; the dryrun compiles in the worktree.

**Post-merge checks for the dispatcher (§3.5):** builder values (feature-store trio) apply on the next run after the `.py` syncs; decorator values (ipdsc_ds_2, third-party builder, advertiser monitor) need the Astro bundle redeploy; confirm via the `Compute batch:` log line and the next sweep's reducer task count (8192 / 2240 / 916 / 3508 / 3392 / 3400). `outputs/` holds 164-168 MB of gitignored event logs; leave them.

**Cross-ticket:** AUDI-1270 lists `guid_log_advertiser_id_dsc_id` stage 13 (disk spill): same file, same knob, this ticket sets it, 1270 must not re-edit. AUDI-1275 lists `prospecting_join` straggler (separate key). AUDI-1272 lists `site_visit_signal_derived_advertiser_id_dsc_id` (a different, derived DAG).

**Risks carried into merge:** the feature-store trio's reducers already wait 48-88% on fetch with evenly spread map output, so 3.4-3.5x more partitions means that many more fetch requests (blocks stay 25-48 KiB; watch `shuffle_fetch_wait:13/24`, an AUDI-1272 mechanism, not a revert trigger); site_visit_signal projects ~1.2 GiB in memory per task against ~1.36 GiB of unified memory per core, so some spill can remain (#1231 outcome: sizing keys resolve, spill keys may not); the third-party builder already had 8 fetch failures at 512; decorator values only land after the Astro bundle redeploys, so `ipdsc_ds_2`, the third-party builder and the advertiser monitor run the old value until then; a spill key still firing after the grace window is `fix_not_working` and moves to the executor-memory lever (AUDI-1270), not a higher count.

## Verification

Adversarial pass 2026-09-02 against §0 Objective and §3 Plan, worktree `/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/67074af2-5859-4b02-9a41-1fb172083596/scratchpad/wt/audi_1269`, read-only (`git diff`, no git writes).

**Diff, line for line — all confirmed against §3.3 and §5.** `git diff --stat`: exactly 7 files, 10 insertions / 7 deletions, matching §5 verbatim. Every changed line in the 6 model files matches §3.3's file/line/value/surface spec exactly (`ipdsc_ds_2.py` L12 2048->8192; `ipdsc_third_party_audience_builder.py` L29 512->2240; `advertiser_score_distribution_monitor.py` L81+L110 128->916; the three feature-store inserts land at the claimed post-`parquet.block.size`, pre-`getOrCreate()` position with the claimed values 3508/3392/3400). No comment lines added. `intent_score_map.py` (4915), `prospecting_join.py` (20000), `household_score_distribution_monitor.py` (512) confirmed untouched at the claimed line numbers. No untracked or extraneous files in the worktree; no writes outside `models/`+`dags/` in the worktree or outside the ticket folder in this repo.

**`model_task_config.json` regeneration reproduced independently, not just diffed.** Ran `MNTN_SDLC_ENV=dev uv run python model_upload.py --dryrun` fresh in the worktree (existing `.venv`, no `uv sync` needed): output byte-identical to the working-tree file both before and after (md5 `84ea730d5565a31a3be54e6c2328ed8a`, matching §4.1's claim), all 3 hunks land on the correct DAG keys (`advertiser_score_distribution_monitor`->916, `ipdsc_ds_2`->8192, `ipdsc_third_party_audience_builder`->2240, confirmed by walking the JSON structure, not just grep), the feature-store trio has no `spark.sql.shuffle.partitions` key post-regen, and `dags/ipdsc_third_party_audience_builders.json` came back byte-identical. The regenerated config is not stale or hand-edited.

**Gate table (§4.1) re-derived from the actual event logs for all 8 DAGs**, not sampled — ran `artifacts/audi_1269_stage_check.py` against every log in `outputs/` (including the `intent_score_map` rolling directory) with the exact target values from §3.3. Every field in every row of the §4.1 table (reducer stage id, live/coalesced status, shuffle read GiB, disk/memory spill GiB, fetch wait %, per-task MiB at target, block KiB at target) reproduced to the reported precision, including the two FAIL rows: `intent_score_map` genuinely blocks at 4.9/5.8 KiB (under the 8 KiB floor) with 3,988 GiB reducer spill plus 1,320/4,262 GiB map-side spill on stages 2/3; `prospecting_join` genuinely spills 0 on both reducers (11,213/8,558 GiB read) with blocks at 0.6-2.4 KiB. The gate correctly pulled both. `household_score_distribution_monitor`'s owner-question narrative checked against `git log`: commit `e55c5ff` (1024->256, driver 40g->48g, the quoted "very high shuffle.partition counts (e.g. 1024) OOM the driver" comment) and `cb0d9f3` (256->512, driver 22g/6g) both verified verbatim in the airflow-ti history.

**Ruff and lint claims reproduced exactly.** `uv run ruff check` on the 6 edited files: 18 findings total, per-file split 2/2/2/1/2/9 (conversion_log/guid_log/site_visit_signal/ipdsc_ds_2/third_party_builder/advertiser_monitor) — identical count and split on the `HEAD` versions of the same files, confirming pre-existing noise, not a regression. `lint_comms.py --kind pr` on `audi_1269_pr_body.md` and `--kind completion` on `audi_1269_result_comment.txt` both pass clean, as claimed. The ledger key list in §5 (16 keys across 6 DAGs) matches §3.0's per-DAG table with no additions or omissions.

**One defect: the "290 MB" `outputs/` figure is wrong.** §8 ("Post-merge checks...") and the executor's own open_items both state `outputs/` holds 290 MB of gitignored event logs. Measured directly (`find outputs -type f`, summed): 164-168 MB (7 top-level `.zstd` + 3-part rolling dir + 3 `.txt` + 1 empty marker, largest still 100 MB as claimed). Off by ~77%. Does not affect the diff, the gate verdicts, the ledger keys, or either hand-off artifact (`audi_1269_pr_body.md` / `audi_1269_result_comment.txt` never state a total); it is a stale or miscomputed housekeeping number, not a technical defect. `state` downgraded to `partial` on this ticket's own rule (every claim must survive); the fix is a one-line correction in §8 to the measured figure (~165 MB) before merge.

**Verdict:** the diff matches §5 exactly, the gate table matches the event logs exactly, the config regeneration is genuinely reproducible, and no step in §3.1-3.4 was skipped or misrecorded. The only miss is a cosmetic byte-count in a post-merge checklist line.
