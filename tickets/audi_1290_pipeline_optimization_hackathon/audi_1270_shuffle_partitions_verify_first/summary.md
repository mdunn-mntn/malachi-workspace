---
doc_type: ticket
title: "AUDI-1270: Verify event logs then raise shuffle.partitions on 15 spill DAGs"
status: backlog
date: 2026-09-02
summary: "Per DAG confirm shuffle-side spill in the event log, then size partitions to ~256 MiB per task"
result: "not started"
question: "For each of the 15 DAGs, is the spilling stage's spill shuffle-side, and what partition count puts about 256 MiB per task in memory?"
framing_state: locked
---

# AUDI-1270: Verify event logs then raise shuffle.partitions on 15 spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1270
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** For each of the 15 DAGs, is the spilling stage's spill shuffle-side, and what partition count puts about 256 MiB per task in memory?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A per-DAG verdict table in outputs/ (stage, spill side, current partitions, shuffle bytes, target partitions) and one PR (branch AUDI-1270) applying the targets to every DAG confirmed shuffle-side; DAGs that are not shuffle-side are handed to the right mechanism in §8.
- **Approach (how):** Event logs from gs://mntn-data-archive-prod/spark-events for the batch fleet and from the PHS temp bucket under the PAM grant for ipdsc jobs; parse with include/spark_optimizer/eventlog.py; target = shuffle write bytes / 256 MiB, rounded up; verify current values on main.
- **What would change the answer:** Spill is input-side (1273 mechanism) or AQE re-coalesces (1274 mechanism); a DAG with no recent event log cannot be verified and is left out, not guessed.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Same disk-overflow fix as AUDI-1269 for 15 more jobs, but each needs a short check of its Spark event log first to confirm the right knob.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** overflow (spill) can happen while shuffling data or while reading input; each has a different fix. The event log shows which one a job has.

**Task:** per DAG open the spilling stage in the event log, confirm the spill is shuffle-side, then set the partition count so each task holds about 256 MiB in memory:
- [fangorn_prospecting_scoring](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/fangorn_prospecting_scoring.py)
- [ipdsc_ds_17](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_17.py)
- [ipdsc_46_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_46_monitor.py), [ipdsc_14_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_14_monitor.py), [ipdsc_49_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_49_monitor.py)
- [ipdsc_ds_13](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_13.py), [ipdsc_ds_14](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_14.py), [ipdsc_ds_47](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_47.py)
- [fangorn_predictions_vertical](https://github.com/SteelHouse/airflow-ti/blob/main/models/machine_learning/fangorn_predictions_vertical.py), [fangorn_household_predictions_vertical](https://github.com/SteelHouse/airflow-ti/blob/main/models/machine_learning/fangorn_household_predictions_vertical.py)
- [vertical_size_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/vertical_size_monitor.py)
- [aug_log_ip](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/aug_log_ip.py)
- [guid_log_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py) (stage 13; its stage-1 fix is in AUDI-1269)
- [guid_log_pivot_household_id_vertical_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_3_pivoted/guid_log_pivot_household_id_vertical_id.py)
- [advertiser_join](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/advertiser_join.py)

**Done-when:** PR merged; optimizer ledger shows the finding resolved (savings auto-measure).

## 3. Plan of Action
Planning wave 2026-09-02 (read-only; nothing computed, nothing edited in airflow-ti). Everything below was verified against airflow-ti `origin/main` at `825b07e` (read-only checkout `/Users/malachi/Developer/work/mntn/airflow-ti-main`), the prod optimizer ledger, and the spark-events archive on 2026-09-02 evening PT.

### 3.1 What is verified and settles the plan

**Source of truth for each DAG's `spark.sql.shuffle.partitions` today.** Three places, precedence highest first:
1. `SparkSession.builder.config("spark.sql.shuffle.partitions", ...)` inside the model's `spark_session` builder. Wins at `getOrCreate` over the decorator (duplicate-setting trap, PR 1231 precedent changed both).
2. `@compute.dataproc_batch(runtime_properties={...})` in the same model file. Compiled by `model_upload.py --dryrun` into `dags/model_task_config.json` -> `<model>.batch.runtime_config.properties`. The JSON is generated, never hand-edited.
3. Neither: the Dataproc Serverless runtime supplies a default. The `ipdsc_ds_17` log shows `spark.sql.shuffle.partitions=1000` with nothing set in the model, so the default is NOT Spark's 200. Always read the effective value from the log's environment (`SparkRun.spark_props`), never assume.
Plus a fourth lever that is not the knob: an explicit `repartition(N, ...)` / `coalesce(N)` in the model fixes that stage's task count regardless of the setting.

All 15 models are `type: dataproc_pyspark_batch` (`@compute.dataproc_batch`), so PR 1169's operator injection writes their event logs to `gs://mntn-data-archive-prod/spark-events/app-<ts>-<n>.zstd`. **The PHS temp bucket and the PAM grant are not needed for this ticket** (the raw ipdsc/tpa batches that use the PHS are a different code path, `include/spark/data_source/`).

| # | DAG (model id) | Model file (airflow-ti main) | Decorator `runtime_properties` | Builder `.config` | Effective today | Code repartition / coalesce | DAG file, schedule UTC | Newest log (prod ledger 2026-09-02) | Ledger spill keys |
|---|---|---|---|---|---|---|---|---|---|
| 1 | fangorn_prospecting_scoring | `models/audience_intent/fangorn_prospecting_scoring.py` | L33-44, no partitions key | L54 = 2000 | 2000 (log) | L168 `repartition(WRITE_PARTITIONS)` | `dags/audience_intent/audience_intent.py` 00:08 daily | `app-20260901044942312-0337.zstd` 2.5 MiB (also `app-20260902052706684-0585`) | disk_spill:13 (5,043 GiB disk / 11,792 GiB mem), disk_spill:14, shuffle_partition_sizing:13 |
| 2 | ipdsc_ds_17 | `models/ipdsc/ipdsc_ds_17.py` | L11-21, executors only | none | 1000 (log, runtime default) | L92 `repartition(79, "ip")` | `dags/tpa_export/tpa_ipdsc_export.py` 02:35 daily | `app-20260902023710023-0850.zstd` 0.3 MiB | disk_spill:4 (33 GiB / 504 GiB) |
| 3 | ipdsc_46_monitor | `models/monitoring/ipdsc_46_monitor.py` | L191 = 128 (AQE + coalesce on, L189-190) | L222 = 128 | 128 | L350 `coalesce(1)` on the write only | `dags/monitoring/ipdsc_monitor.py` 00:05 daily | `app-20260901035249765-0966.zstd` 0.6 MiB | disk_spill:10 and :11 (11 GiB / 92 GiB each) |
| 4 | ipdsc_14_monitor | `models/monitoring/ipdsc_14_monitor.py` | L159 = 128 | L190 = 128 | 128 | L319 `coalesce(1)` write only | `ipdsc_monitor.py` | `app-20260901040209977-0949.zstd` 0.5 MiB | disk_spill:10 and :11 (7 GiB / 65 GiB) |
| 5 | ipdsc_49_monitor | `models/monitoring/ipdsc_49_monitor.py` | L174 = 128 | L204 = 128 | 128 | L319 `coalesce(1)` write only | `ipdsc_monitor.py` | `app-20260902030147786-0337.zstd` 0.6 MiB | disk_spill:10 and :11 (6 GiB / 51 GiB) |
| 6 | ipdsc_ds_13 | `models/ipdsc/ipdsc_ds_13.py` | L10-20, executors only | none | read from log | L64 `repartition(35, "ip")` | `tpa_ipdsc_export.py` | `app-20260902042740802-0974.zstd` 0.5 MiB | disk_spill:1 (32 GiB / 219 GiB) |
| 7 | ipdsc_ds_14 | `models/ipdsc/ipdsc_ds_14.py` | L10-19, executors only | none | read from log | L60 `repartition(12, "ip")` | `tpa_ipdsc_export.py` | none in the ledger since 08-21 (2 spilling runs in the 08-27 corpus, 3 GiB) | none live |
| 8 | ipdsc_ds_47 | `models/ipdsc/ipdsc_ds_47.py` | L11 = 5000 | none | 5000 | none | `tpa_ipdsc_export.py` | `app-20260902030204550-0576.zstd` 4.1 MiB | disk_spill:5 (44 GiB / 682 GiB); straggler:5 belongs to AUDI-1275 |
| 9 | fangorn_predictions_vertical | `models/machine_learning/fangorn_predictions_vertical.py` | L11-20, maxExecutors only | L34 = 32768, L35 `files.maxPartitionBytes` 512 MiB | 32768 | check | `dags/machine_learning/fangorn_14day_lookback_dag.py` 21:21 daily | `app-20260901212244494-0601.zstd` 10.6 MiB | disk_spill:2 (269 GiB / 736 GiB) |
| 10 | fangorn_household_predictions_vertical | `models/machine_learning/fangorn_household_predictions_vertical.py` | same shape as 9 | L34 = 32768, L35 512 MiB | 32768 | check | `fangorn_household_14day_lookback_dag.py` 21:21 daily | `app-20260901213237664-0623.zstd` 5.2 MiB | disk_spill:1 (4 GiB / 8 GiB) |
| 11 | vertical_size_monitor | `models/monitoring/vertical_size_monitor.py` | L202 = 128 (AQE + coalesce on) | L234 = 128 | 128 | L375 `coalesce(1)` write only | `dags/create_ip_vertical_assocations.py` 00:05 daily | `app-20260902011415369-0939.zstd` 1.4 MiB | disk_spill:11 new, :17 chronic, :13 resolved (the spilling stage's id moves between runs); also in AUDI-1272 |
| 12 | aug_log_ip | `models/feature_store/feature_group_1_source/aug_log_ip.py` | L22-33, executors only | L43 = 2000 | 2000 | L93 `repartition(8, "ip")` | `dags/models/feature_store_setup_model.py` 01:03 daily | last firing `app-20260831014555966-0069.zstd`; key resolved 09-02 | disk_spill:1 resolved (1.3 GiB disk, under the 2 GiB detector floor) |
| 13 | guid_log_advertiser_id_dsc_id | `models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py` | L24-34, executors only | none (L44-46 set `files.*` only) | read from log | L134 `coalesce(min(current, target))` | `feature_store_setup_model.py` 01:03 daily | `app-20260902010550610-0067.zstd` 9.7 MiB | disk_spill:13 (351 GiB / 1,008 GiB), disk_spill:24 (316 GiB / 908 GiB); sizing:5/:16 owned by AUDI-1269 (~3400) |
| 14 | guid_log_pivot_household_id_vertical_id | `models/feature_store/feature_group_3_pivoted/guid_log_pivot_household_id_vertical_id.py` | L26 = 8000 | none (L51-53 `files.*` only) | 8000 | L137 `repartition("household_id","snapshot_date")`, L272 `repartition(target_partitions, "household_id")` | `feature_store_setup_model.py` daily; also `feature_store_snapshot.py`, fangorn hhid pipelines | none in the ledger since 08-21 (1 spilling run in the corpus, stage 33, 10 GiB) | none live |
| 15 | advertiser_join | `models/audience_intent/advertiser_join.py` | L14-58, no partitions key (speculation pinned false L36) | L69 = 28000 | 28000 | L142 `coalesce(14000)` | `audience_intent.py` 00:08 daily | `app-20260901060920436-0979.zstd` 7.7 MiB | disk_spill:3 (2,493 GiB / 13,216 GiB); straggler:3 belongs to AUDI-1275 |

All 13 ledger app ids above were confirmed present in the archive on 2026-09-02 (sizes as listed, ~50 MiB total). The archive holds 147-157 logs per day for 08-25..09-02.

**Parser proven on two of these logs** (downloaded to `outputs/eventlogs/`, 2.9 MB total): `include/spark_optimizer/eventlog.py::parse_eventlog` runs from the airflow-ti root on system python 3.11 with no `zstandard` module (it falls back to `/opt/homebrew/bin/zstd`). Per stage it yields `num_tasks, input_bytes, shuffle_read_bytes, shuffle_write_bytes, mem_spill, disk_spill, peak_exec_mem`; `run.spark_props` carries the effective `spark.sql.shuffle.partitions`, `spark.sql.adaptive.enabled`, `coalescePartitions.enabled`, `advisoryPartitionSizeInBytes`, `executor.memory/cores`, `driver.memory`, `files.maxPartitionBytes`. Result of the two parses (verification sample, NOT the answer):

| log | stage | tasks | input GiB | shuffle read GiB | shuffle write GiB | mem spill GiB | disk spill GiB | effective partitions |
|---|---|---|---|---|---|---|---|---|
| fangorn_prospecting_scoring `app-20260826050509373-0553` | 13 | 2048 | 65.2 | 0.0 | 2246.8 | 10242.9 | 4165.3 | 2000, AQE on (model sets nothing for AQE) |
| ipdsc_ds_17 `app-20260826030005732-0166` | 4 | 100 | 16.5 | 0.0 | 16.0 | 506.3 | 33.1 | 1000 (runtime default), executor.memory 9600m |

Both spilling stages read files (`input_bytes` > 0) and read no shuffle (`shuffle_read_bytes` = 0): the spill happens while the map tasks sort their shuffle output, which is the input-side case §0 routes to the AUDI-1273 mechanism. Expect the same shape on several more of the 15.

**Prior art:** PR 1231 (`ddf55a9`) is the exact template: `fangorn_score_monitor` 512/256 -> 2048 in decorator AND builder, `dags/model_task_config.json` regenerated, 2 files. CI for a models PR (`.github/workflows/pr_model.yaml`) = `uv export --only-group models` + `MNTN_SDLC_ENV=dev python model_upload.py --dryrun` + fail if `dags/model_task_config.json` or `dags/ipdsc_third_party_audience_builders.json` differ + `pytest tests/models`. Both jobs green on main at run 33698428551 (2026-09-03).

### 3.2 Steps (execute wave; worktree = airflow-ti branch AUDI-1270 created by the dispatcher)

1. **Snapshot inputs.** `gsutil -o "GSUtil:check_hashes=never" cp gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl outputs/audi_1270_prod_ledger_snapshot_<date>.jsonl`; for each of the 15 DAGs take the newest `app_id` of every `disk_spill:*` key (2026-09-02 values in the table above). Download each to `outputs/eventlogs/` with the same `gsutil -o "GSUtil:check_hashes=never" cp` (the "Found no hashes to validate" warning is benign). Download in the same session as the listing: flagged apps' logs have vanished from the archive within hours before (memory 2026-09-02). Delete any file over 200 MB after parsing (largest known is 10.6 MiB).
2. **Locate logs for the three DAGs without a live ledger row** (ipdsc_ds_14, guid_log_pivot_household_id_vertical_id, and aug_log_ip whose key resolved). Verified fallback: list the DAG's schedule window, `gsutil ls "gs://mntn-data-archive-prod/spark-events/app-<YYYYMMDD><HH>*"` (~12 objects per hour), then read each object's app name from its first 128 KiB: `gsutil cat -r 0-131071 <obj> | zstd -d -c 2>/dev/null | grep -o '"App Name":"[^"]*"' | head -1` (names are `Populate <model_id>.<Class>`). Windows: ipdsc_ds_14 02:00-05:00 UTC (tpa_ipdsc_export 02:35); guid_log_pivot_household_id_vertical_id and aug_log_ip 01:00-06:00 UTC (feature_store_setup_model 01:03). Parse the three newest runs of each. If no stage spills 2 GiB or more to disk (the detector floor), the verdict is "no recent spill, left out" per §0.
3. **Parse.** From the worktree root, `PYTHONDONTWRITEBYTECODE=1 python3 -c "from include.spark_optimizer.eventlog import parse_eventlog; ..."` (or a small `artifacts/audi_1270_stage_verdict.py` wrapping it) printing, per log, `spark_props` for the keys listed in 3.1 and every stage with `disk_spill`, `mem_spill`, or shuffle bytes over 1 GiB. Parse at least two logs per DAG (newest plus the previous ledger app id) because the spilling stage's id moves between runs (vertical_size_monitor 13 -> 11/17): match stages by position and byte profile, not id alone.
4. **Verdict per DAG** (one row per spilling stage, written to `outputs/audi_1270_verdict_table.csv` and mirrored in §4):
   - R1 spill side: `shuffle_read_bytes > 0` = shuffle-side (the stage's tasks are reducers); `shuffle_read_bytes == 0` and `input_bytes > 0` = input-side -> §8 for the AUDI-1273 mechanism (`spark.sql.files.maxPartitionBytes`), record `input_bytes`, `num_tasks`, and `shuffle_write_bytes / input_bytes` (map expansion) for that ticket. No config change here.
   - R2 the knob owns the stage: `num_tasks == effective spark.sql.shuffle.partitions` -> yes. `num_tasks` smaller with AQE coalescing on (AQE is on by default on this runtime; fangorn's log shows `adaptive.enabled=true` with nothing set in the model) -> AQE re-coalesced, §8 for the AUDI-1274 mechanism (`advisoryPartitionSizeInBytes`). `num_tasks` equal to a `repartition(N)` / `coalesce(N)` constant in the model -> the code constant is the lever, see decision D2.
   - R3 target (only rows passing R1 and R2): `ceil(shuffle_read_bytes / 256 MiB)`, rounded up to the next 100; several spilling stages in one app -> the max; never lower an existing value; the monitors' stages 10 and 11 carry identical bytes (same shuffle recomputed) and get one value.
   - R4 memory check: per-task in-memory today = `mem_spill / num_tasks`; after = that x `num_tasks / target`; execution memory per task ~ `executor.memory x 0.6 / executor.cores` (both from `spark_props`; ipdsc_ds_* run the 9600m default). Record both; if "after" still exceeds execution memory the ticket's own rule applies (executor memory only if spill persists) and it goes in §8, not in this PR.
   - R5 driver check (INC-018, memory `reference_airflow_ti`): map-status memory scales with map tasks x reduce partitions and tipped a 9600m driver at 5000 partitions. Any DAG whose target exceeds 5000 while `driver.memory` is unset or 9600m goes to decision D3 before it is written.
   - Columns: dag, model_file, log_app_id, stage, stage_tasks, input_gib, shuffle_read_gib, shuffle_write_gib, mem_spill_gib, disk_spill_gib, effective_partitions, config_site (builder / decorator / both / none), aqe_coalesce, repartition_constant, spill_side, knob_owns_stage, target_partitions, per_task_after_mib, exec_mem_per_task_mib, driver_mem, verdict (change / 1273 / 1274 / code / no_spill / conflict_1269).
5. **Confirm "current" against the file before editing.** For every DAG marked `change`, the log's effective value must equal the value at the file line in the table (or the runtime default where the file sets none). A mismatch means the model changed after the log; re-pull the newest log and re-run step 4 for that DAG.
6. **Edit** (only DAGs with verdict `change`; exact sites from the table, all verified on main at `825b07e`):
   - Value in both decorator and builder (ipdsc_46_monitor L191+L222, ipdsc_14_monitor L159+L190, ipdsc_49_monitor L174+L204, vertical_size_monitor L202+L234): change BOTH lines to the same value, as PR 1231 did.
   - Builder only (fangorn_prospecting_scoring L54, aug_log_ip L43, advertiser_join L69, fangorn_predictions_vertical L34, fangorn_household_predictions_vertical L34): change the builder line; do not add a decorator key.
   - Decorator only (ipdsc_ds_47 L11, guid_log_pivot_household_id_vertical_id L26): change the decorator line.
   - Neither (ipdsc_ds_13, ipdsc_ds_14, ipdsc_ds_17, guid_log_advertiser_id_dsc_id): `models/ipdsc/*` add `"spark.sql.shuffle.partitions": "<N>"` to the decorator `runtime_properties` (ipdsc_ds_2 / ipdsc_ds_47 precedent); the feature_store model adds `.config("spark.sql.shuffle.partitions", "<N>")` to the builder chain after L46 (repo convention: Spark SQL behaviour lives in the builder). guid_log_advertiser_id_dsc_id follows decision D1 first.
   - No comments in the diff (the why goes in the PR body). No DAG file edits, no `spark.speculation`, no executor or driver memory changes without decision D3.
7. **Regenerate and validate** (in the worktree, every time the branch changes and again after any rebase):
   1. `uv sync --group models --group test` (fresh worktree has no `.venv`; `.venv` is gitignored; `dags/current_branch.json` is absent so the code root defaults to dev).
   2. `MNTN_SDLC_ENV=dev uv run python model_upload.py --dryrun` -> prints `Compiling all models` then `Skipping all models upload to 'dev' env`.
   3. `git status --short` shows only the edited model files plus `dags/model_task_config.json` (only when a decorator changed); `dags/ipdsc_third_party_audience_builders.json` must be unchanged.
   4. `python3 -c "import json; d=json.load(open('dags/model_task_config.json')); print({k: d[k]['batch']['runtime_config']['properties'].get('spark.sql.shuffle.partitions') for k in [<changed dags>]})"` equals the targets; `grep -n "shuffle.partitions" <each edited model>` shows every site at the new value.
   5. `uv run ruff check <edited files>` clean (ruff is pinned 0.16 in the repo; lint like CI).
   6. `uv run python -m pytest tests/models -q` green (green on main 2026-09-03; if it fails on something this branch did not touch, record it, do not fix it here).
   7. `git diff --stat` matches the verdict table's `change` rows exactly. Hand off to the dispatcher (commit, gauntlet, PR). PR body: one row per DAG (stage, spill side, current, shuffle GiB, target, log app id) plus the `1273` / `1274` / `code` / `no_spill` rows so the reviewer sees why a listed DAG is absent.
8. **After merge (dispatcher / next wave, not this PR):** builder changes apply on the next scheduled run (the `.py` is read live from GCS); decorator changes apply only after the Astro bundle redeploys (automatic on merge via "Deploy to Prod"). Verify on the next run's event log `spark_props` (schedules in the table), not on the merge. Stamp provenance per key: `python -m include.spark_optimizer.ledger applied <dag> disk_spill:<stage> <PR#> <merge-date>` against the prod ledger (`OPTIMIZER_LEDGER` env, fangorn precedent 2026-08-27); the ledger marks a key resolved after 3 quiet sweeps (`RESOLVE_SWEEPS=3`, sweeps run 17:00 UTC daily). Update §4-§8 here and run `/capture`.

### 3.3 Assumptions to resolve empirically first (execute wave)
- A1 Each ledger `app_id` still exists when downloaded (13/13 existed 2026-09-02 evening PT; logs have vanished within hours before).
- A2 The effective `spark.sql.shuffle.partitions` in each log equals the model's current value on main (step 5 checks it); for the four models that set nothing, the log supplies the runtime default (1000 seen on ipdsc_ds_17).
- A3 `uv sync --group models` succeeds in a fresh worktree (network install; `/Users/malachi/Developer/work/mntn/airflow-ti/.venv` shows it has worked on this Mac).
- A4 The newest log's spilling stage is the one the ledger key names (vertical_size_monitor already violates this by id; match by profile).
- A5 The monitors' stage 10 and stage 11 spill are the same shuffle computed twice (identical bytes in every ledger row); confirm from the parse before assigning one value.
- A6 ipdsc_ds_14 and guid_log_pivot_household_id_vertical_id still run daily and simply stopped spilling above the detector floor (0 ledger rows since 08-21 vs 2 and 1 spilling runs in the 08-27 corpus).

### 3.4 Decisions for the user
- D1 `guid_log_advertiser_id_dsc_id`: AUDI-1269 sets ~3400 (stage 5/16 sizing) in the same file and the same generated JSON; this ticket's stage 13/24 spill rides the same single setting. Proposed: 1269 lands first, 1270 rebases and raises only if its computed target exceeds 3400 (one line, same site). Alternative: 1270 takes the DAG and 1269 drops it.
- D2 Stages whose task count comes from a code constant (`repartition(N)` / `coalesce(N)` sites in the table): change the constant in this PR, or hand it to §8 with the computed N? Proposed: §8, because a repartition constant also fixes the output file count that downstream readers see; this ticket stays config-only like its siblings.
- D3 A target above 5000 on a DAG running the 9600m default driver (ipdsc_ds_13/14/17 candidates): add `spark.driver.memory` 16g + `memoryOverhead` 4g as PR 1198 did on ipdsc_geo, or cap the target? Proposed: flag per DAG in the verdict table and ask before writing either.
- D4 (rule, not a fork) Every epic PR (1269, 1272, 1273, 1274, 1275) regenerates `dags/model_task_config.json`; after any rebase re-run step 7 and never hand-merge that file. vertical_size_monitor (1272), advertiser_join and ipdsc_ds_47 (1275) share model files with this ticket.

### 3.5 Risks
- The planning sample (2 of 15 logs) shows input-side spill on both; the PR may carry far fewer than 15 DAGs. That is the ticket's designed outcome; say so in the PR body rather than forcing the knob.
- AQE coalescing is on for every DAG here by runtime default; a raised count can be coalesced back on stages whose shuffle bytes per partition sit under the 64 MiB advisory default (the 1274 mechanism). R2 catches it; do not skip R2.
- INC-018 driver OOM: `9600m` driver x 5000 partitions x many map tasks died at a constant interval. R5 is the gate.
- Decorator-only changes look merged but do nothing until the bundle redeploys; verify on the run's event log.
- Spilling stage ids move between runs; a verdict keyed on stage id alone can name the wrong stage.
- The archive has no lifecycle rule (IMP-048) but flagged logs have still disappeared within hours; download before analysing, never rely on a listing from an earlier session.
- Never lower an existing value, never touch `spark.speculation` (pinned false on GCS writers, gauntlet reverted it once), never edit a DAG file, never trigger prod.

### 3.6 Sources
- Spec: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` item 18; ranked rows in `outputs/audi_1194_hackathon_optimizations_2026_08_27.md` (rows 15, 18-22, 28, 31, 33, 41, 45, 50, 53, 57, 65).
- Live findings: `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` (snapshot `outputs/audi_1270_prod_ledger_snapshot_2026_09_02.jsonl`, 1,352 rows, 08-21..09-02); the workspace copy in the 1194 folder stops at 08-26.
- Event logs: `gs://mntn-data-archive-prod/spark-events/` (two samples in `outputs/eventlogs/`).
- Parser and detectors: `include/spark_optimizer/eventlog.py` (`StageMetrics`, `SparkRun`), `optimizations.py` L266-292 (disk_spill floor 2 GiB disk or 32 GiB memory; sizing rule 256 MiB).
- Config pipeline: `model_upload.py`, `dags/model_task_config.json` (112 models), `.github/workflows/pr_model.yaml`, airflow-ti `CLAUDE.md` L45-46, L106, L520.
- Precedent: airflow-ti PR 1231 (`ddf55a9`), PR 1198 (driver memory), memory `reference_airflow_ti` (duplicate-setting trap, bundle redeploy, INC-018, PR 1169 event-log injection), `project_airflow_optimizer.md` (2026-08-26 corpus validation, 2026-09-02 vanishing logs), `feedback_airflow_prod_safety`.
- Sibling framings: `../audi_1269_shuffle_partitions_preverified/summary.md`, `../audi_1272_initial_executors_verify_first/summary.md`, `../audi_1275_straggler_gcs_writers/summary.md`.

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

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
