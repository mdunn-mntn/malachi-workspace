---
doc_type: ticket
title: "AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs"
status: backlog
date: 2026-09-02
summary: "Config-only: raise spark.sql.shuffle.partitions on 9 spilling DAGs, values pre-verified 08-27"
result: "plan written 2026-09-02: 8 DAGs ready for one config-only PR, household monitor held for a decision"
question: "Does raising spark.sql.shuffle.partitions to the 08-27 sweep's computed value on the 9 named DAGs stop their shuffle-side spill without changing outputs or failing the run?"
framing_state: locked
---

# AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1269
**Status:** backlog
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
**Decisions for the user (planning wave 2026-09-02):**
1. `household_score_distribution_monitor`: drop from AUDI-1269 (recommended) or apply 8896. The owner reverted 1024 -> 256 -> 512 after driver out-of-memory failures (§4), the driver sits at the standard-tier ceiling, and the DAG has no ledger history so the outcome cannot be measured. If dropped, log it as an owner question (Ryan) under AUDI-1270 or the AUDI-1275 owner ask; if applied, the PR needs the §3.2 (e) driver check written into its body and the owner tagged as reviewer.
2. Dev validation: none (recommended; precedent #1231 was config-only with dryrun + CI + gauntlet, and the first prod execution is the next scheduled run per the prod-safety rule) or one `model_run.py` dev run of ipdsc_ds_2 (~27 exec-h per run at prod scale, ~$8 at $0.278/exec-h; prospecting_join would be ~350 exec-h, ~$100).

**Assumptions the execute wave resolves first (§3.2):** the other five reducers run at the configured count today (verified for 3 of 8); prospecting_join's 10.6 TiB shuffle still exists in the 09-01 log; third-party builder stage 9 is a reducer; the dryrun compiles in the worktree with the `models` group.

**Cross-ticket:** AUDI-1270 lists `guid_log_advertiser_id_dsc_id` stage 13 (disk spill): same file, same knob, this ticket sets it, 1270 must not re-edit. AUDI-1275 lists `prospecting_join` straggler (separate key). AUDI-1272 lists `site_visit_signal_derived_advertiser_id_dsc_id` (a different, derived DAG).

**Risks carried into execution:** the feature-store trio's reducers already wait 80-88% on fetch with evenly spread map output, so 3.5x more partitions means 3.5x more fetch requests (blocks stay ~48 KiB; watch `shuffle_fetch_wait:13/24`, an AUDI-1272 mechanism, not a revert trigger); spill may not clear even at 256 MiB compressed per task because in-memory expansion is 3-18x on 9600m / 4-core executors (#1231 outcome); decorator values only land after the Astro bundle redeploys, so `ipdsc_ds_2` and the third-party builder run the old value until then; flagged apps' event logs have vanished from `spark-events` within hours, so download all 8 first.
