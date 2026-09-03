---
doc_type: ticket
title: "AUDI-1270: Verify event logs then raise shuffle.partitions on 15 spill DAGs"
status: in_progress
date: 2026-09-02
summary: "Per DAG confirm shuffle-side spill in the event log, then size partitions to ~256 MiB per task"
result: "executed 2026-09-02: 1 of 15 DAGs is shuffle-side and edited (vertical_size_monitor 128 -> 600, decorator + builder, config regenerated, dryrun clean, 145 model tests pass); guid_log_advertiser_id_dsc_id is shuffle-side but owned by AUDI-1269 (3400; in-memory sizing says 4100, §8); 11 DAGs spill on the map side while reading input (AUDI-1273 mechanism), ipdsc_ds_47 spills on its BigQuery read stage (code), ipdsc_ds_14 / guid_log_pivot_household_id_vertical_id / aug_log_ip no longer spill above the 2 GiB floor"
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
   - R3 target (only rows passing R1 and R2), **as executed (rewritten 2026-09-02, see §3.7)**: `max(ceil(shuffle_read_bytes / 256 MiB), ceil(mem_spill / 256 MiB))`, each rounded up to the next 100, never below the current value; several spilling stages in one app -> the max. The planning-wave formula used shuffle bytes alone; execution showed vertical_size_monitor already sits at 200 MiB of shuffle bytes per partition and still spills because the rows expand 5.3x in memory, so the in-memory figure (the ledger's "in-memory at spill time", `mem_spill`) is the one the ticket question actually names. Then check the value against adaptive coalescing: partitions merge only while two neighbours fit in the 64 MiB advisory, so a raised count sticks as long as `shuffle_read_bytes / target >= 32 MiB` (empirical in §4.3).
   - R4 memory check: per-task in-memory today = `mem_spill / num_tasks`; after = `mem_spill / target`; execution memory per task ~ `executor.memory x 0.6 / executor.cores` (both from `spark_props`; ipdsc_ds_* run the 9600m default). Record both. Execution showed the nominal budget is not discriminating on its own (vertical_size_monitor spills at 1.06 GiB per task against a nominal 1.2 GiB because `vertical_compare_df.cache()` can hold half the unified pool), so "after" must clear the worst-case half-pool budget (`executor.memory x 0.3 / cores`) too; if it does not, the ticket's own rule applies (executor memory only if spill persists) and it goes in §8, not in this PR.
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

### 3.7 Execution wave 2026-09-02: what changed against the plan
A first execute agent was cut off by a session limit after downloading 17 logs (the `.gstmp` files in `outputs/eventlogs/` were complete: `zstd -t` passed on all 17, sizes matched the ledger listing; renamed in place, not re-downloaded). This wave ran steps 1-7 as written with these deviations:
- **R3 rewritten** (text in §3.2 step 4 updated): target uses `max(shuffle_read, mem_spill) / 256 MiB`, not shuffle bytes alone. Reason in §4.4: the only `change` DAG sits at 200 MiB of shuffle bytes per partition and spills anyway; the bytes-only formula would have produced 200 (a rounding artefact of "next 100" over 103) and left the stage at 0.68 GiB in memory per task, inside the range that spills today.
- **R4 extended** with the half-pool worst case for models that cache (the vertical_size_monitor model caches `vertical_compare_df` at L282).
- **Step 2's fallback was needed for all three ledger-less DAGs**; `artifacts/audi_1270_archive_scan.sh` did the listing + app-name read (241 objects over 15 hour-prefixes, 0 unreadable). Their 3 newest runs each were downloaded and parsed (8 more logs; 31 total, 112 MB, largest 15 MB, nothing near the 200 MB delete threshold).
- **Step 7.6 needed the CI-equivalent environment** exactly as the AUDI-1273 wave found (`JAVA_HOME=/opt/homebrew/opt/openjdk@17`, `PYSPARK_PYTHON=PYSPARK_DRIVER_PYTHON=.venv/bin/python`): without it 83 passed / 62 errors (`JAVA_GATEWAY_EXITED`), with it 145 passed in 22.8 s.
- Decision D2 produced no §8 rows: none of the 21 spilling stages is governed by a code constant (the constants in the table all sit on non-spilling write stages).
- macOS has no `timeout` binary; long commands ran unwrapped.

## 4. Investigation & Findings

### 4.1 Inputs (steps 1-2)
- Ledger snapshot re-pulled 2026-09-02 23:45 PT: `outputs/audi_1270_prod_ledger_snapshot_2026_09_02.jsonl`, 1,352 rows, dates 08-21..09-02 (same row count as the planning snapshot; the 09-02 sweep was already in it).
- Event logs: 31 files in `outputs/eventlogs/` (gitignored), all 23 ledger app ids for the 12 DAGs with live rows plus the 8 located by the archive scan. Every ledger app id listed in §3.1 still existed (A1 holds). App names verified from each file's `SparkListenerApplicationStart`, all 31 match the expected model (`outputs/audi_1270_spark_props.csv`, column `app_name`).
- Archive scan (`outputs/audi_1270_archive_scan_2026_09_02.tsv`, 241 objects, hours 01-05 UTC on 08-31, 09-01, 09-02): ipdsc_ds_14 runs at 03:23-03:55 UTC (`app-20260902032301606-0535`, `app-20260901035336630-0765`, `app-20260831035522861-0782`, 133-137 KB each); guid_log_pivot_household_id_vertical_id at 01:38-01:45 UTC (`app-20260902013834083-0011`, `app-20260901014548961-0929`, `app-20260831013827557-0910`, ~7 MB each); aug_log_ip at 01:41-01:45 UTC (`app-20260902014437530-0436`, `app-20260901014112861-0343`, plus the ledger's `app-20260831014555966-0069`, ~410 KB each). All three DAGs still run daily (A6 holds).

### 4.2 Parse (step 3)
`artifacts/audi_1270_stage_verdict.py` wraps `include.spark_optimizer.eventlog.parse_eventlog` (run from the worktree root with `PYTHONPATH=<worktree>`; the parser shells out to `/opt/homebrew/bin/zstd`). Outputs: `outputs/audi_1270_stage_metrics.csv` (every stage with spill or shuffle bytes over 1 GiB, 31 logs, 150 rows) and `outputs/audi_1270_spark_props.csv` (effective config per run). Effective `spark.sql.shuffle.partitions` per DAG matched the file in every case (step 5): the four models that set nothing (ipdsc_ds_13, ipdsc_ds_14, ipdsc_ds_17, guid_log_advertiser_id_dsc_id) all show 1000, confirming the Dataproc Serverless runtime default (A2 holds). `spark.sql.adaptive.enabled=true` on all 31 runs; `coalescePartitions.enabled` is explicitly true on the four monitors and unset (runtime default true) elsewhere; no run sets `advisoryPartitionSizeInBytes` (64 MiB default).

### 4.3 Verdict table (step 4), newest log per DAG; mirrored from `outputs/audi_1270_verdict_table.csv`
Exec MiB = `executor.memory x 0.6 / cores`, the nominal per-task execution budget. In-memory per task today = `mem_spill / tasks`.

| DAG | log | stage | tasks | input GiB | shuffle read GiB | shuffle write GiB | mem spill GiB | disk spill GiB | partitions (site) | spill side | knob owns stage | target | in-mem/task today -> after | exec MiB | driver | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fangorn_prospecting_scoring | `app-20260902052706684-0585` | 13 | 2048 | 64.7 | 0 | 2526.5 | 11684.5 | 4969.9 | 2000 (builder) | input | n/a | | 5.7 GiB | 3072 | 20G | 1273 |
| fangorn_prospecting_scoring | same | 14 | 21 | 0.7 | 0 | 25.9 | 15.2 | 3.1 | 2000 | input | n/a | | 0.7 GiB | 3072 | 20G | 1273 |
| ipdsc_ds_17 | `app-20260902023710023-0850` | 4 | 100 | 17.9 | 0 | 16.3 | 504.4 | 32.9 | 1000 (runtime default) | input | n/a | | 5.0 GiB | 1440 | 9600m | 1273 |
| ipdsc_46_monitor | `app-20260901035249765-0966` | 10, 11 | 20 | 2.2 | 0 | 12.7 / 12.8 | 91.3 / 92.1 | 11.0 / 11.0 | 128 (both) | input | n/a | | 4.6 GiB | 1229 | 16g | 1273 |
| ipdsc_14_monitor | `app-20260901040209977-0949` | 10, 11 | 14 | 1.6 / 1.7 | 0 | 7.9 / 8.0 | 65.2 / 65.6 | 6.8 / 6.8 | 128 (both) | input | n/a | | 4.7 GiB | 1229 | 16g | 1273 |
| ipdsc_49_monitor | `app-20260902030147786-0337` | 10, 11 | 13 | 1.3 | 0 | 7.4 / 6.8 | 51.2 / 45.6 | 6.4 / 5.8 | 128 (both) | input | n/a | | 3.9 GiB | 1229 | 16g | 1273 |
| ipdsc_ds_13 | `app-20260902042740802-0974` | 1 | 248 | 25.0 | 0 | 42.2 | 219.0 | 32.4 | 1000 (runtime default) | input | n/a | | 0.9 GiB | 1440 | 9600m | 1273 |
| ipdsc_ds_14 | `app-20260902032301606-0535` | 1 (largest) | 100 | 7.0 | 0 | 4.1 | 0 | 0 | 1000 (runtime default) | input | n/a | | 0 | 1440 | 9600m | no_spill |
| ipdsc_ds_47 | `app-20260902030204550-0576` | 5 | 1952 | 0 | 0 | 248.4 | 681.9 | 44.3 | 5000 (decorator) | source (BigQuery read) | n/a | | 0.35 GiB | 1440 | 9600m | code |
| fangorn_predictions_vertical | `app-20260901212244494-0601` | 2 | 717 | 127.7 | 0 | 278.6 | 736.2 | 268.7 | 32768 (builder) | input | n/a | | 1.0 GiB | 1440 | 9600m | 1273 |
| fangorn_household_predictions_vertical | `app-20260901213237664-0623` | 1 | 9 | 3.3 | 0 | 3.8 | 8.0 | 3.6 | 32768 (builder) | input | n/a | | 0.9 GiB | 1440 | 9600m | 1273 |
| **vertical_size_monitor** | `app-20260902011415369-0939` | 11, 17 | 128 | 0 | 25.6 | 0 | 136.0 / 136.0 | 16.3 / 16.3 | 128 (both) | **shuffle** | **yes** (128 = setting) | **600** | 1.06 GiB -> 232 MiB | 1229 (614 half-pool) | 16g | **change** |
| aug_log_ip | `app-20260902014437530-0436` | 1 | 205 | 22.1 | 0 | 35.7 | 30.1 | 0.7 | 2000 (builder) | input | n/a | | 0.15 GiB | 1440 | 9600m | no_spill (under the 2 GiB floor) |
| guid_log_advertiser_id_dsc_id | `app-20260902010550610-0067` | 13 | 1000 | 0 | 870.3 | 0.5 | 1007.9 | 351.4 | 1000 (runtime default) | shuffle | yes (1000 = setting) | 4100 | 1.0 GiB -> 252 MiB | 1440 | 9600m | conflict_1269 |
| guid_log_advertiser_id_dsc_id | same | 24 | 1000 | 0 | 870.3 | 0.5 | 907.6 | 316.2 | 1000 | shuffle | yes | 3700 | 0.9 GiB -> 251 MiB | 1440 | 9600m | conflict_1269 |
| guid_log_pivot_household_id_vertical_id | `app-20260902013834083-0011` | 1 (largest) | 750 | 13.7 | 0 | 17.7 | 0 | 0 | 8000 (decorator) | input | n/a | | 0 | 1440 | 9600m | no_spill |
| advertiser_join | `app-20260901060920436-0979` | 3 | 4798 | 390.2 | 0 | 2413.8 | 13215.8 | 2493.0 | 28000 (builder) | input | n/a | | 2.75 GiB | 3686 | 28G | 1273 |

Cross-run stability (second log per DAG, `outputs/audi_1270_stage_metrics.csv`): every spilling stage above has the same task count, the same side and bytes within 10% in the previous run (fangorn_prospecting_scoring 08-26/09-01/09-02, ipdsc_ds_17 08-26/09-02, ipdsc_ds_13 08-31/09-02, ipdsc_ds_47 08-27/09-02, fangorn_*_vertical 08-26/09-01, advertiser_join 08-27/09-01, guid_log_advertiser_id_dsc_id 08-27/09-02, vertical_size_monitor 08-27/09-02). A4 (stage ids move): vertical_size_monitor's first spilling stage is id 13 in the 08-27 log and id 11 in the 09-02 log with an identical profile (128 tasks, 25.6 GiB read, 136.0 GiB memory, 16.3 GiB disk); stage 17 is stable. A5 (monitor stages 10/11 are the same shuffle twice): confirmed by bytes on all three monitors, moot for the knob because both are map-side.

**Adaptive coalescing, measured on this runtime (governs whether a raised count sticks):**
- ipdsc_ds_17 stage 6: setting 1000, 16.3 GiB read -> 334 tasks. 16.7 MiB per original partition, merged in threes (50 MiB <= 64 MiB advisory; four would be 67 MiB). 1000 / 3 = 334 exactly.
- ipdsc_ds_47 stage 7: setting 5000, 248.4 GiB -> 5000 tasks (51 MiB each; a pair is 102 MiB > 64 MiB, so nothing merges).
- fangorn_prospecting_scoring stage 15: setting 2000, 2545.6 GiB -> 2000 tasks (1.3 GiB each).
- ipdsc_ds_13 stage 3: setting 1000, 42.2 GiB -> 1000 tasks (43 MiB each, pairs exceed 64 MiB).
- Rule that fits all four: partitions merge only while two neighbours fit in 64 MiB, so a count sticks while `shuffle bytes / count >= 32 MiB`. For vertical_size_monitor (25.6 GiB) that is any count up to ~819; 600 gives 44 MiB per task.
- fangorn_predictions_vertical stage 4 is the exception: setting 32768, 278.6 GiB -> 997 tasks (286 MiB each), i.e. neither the setting nor the 64 MiB advisory. The stage is an Iceberg `overwritePartitions` write; the Iceberg write path sizes that shuffle itself (its own advisory partition size, default the 512 MiB target file size). Unverified mechanism, recorded because it means `spark.sql.shuffle.partitions` does not reach the Iceberg writers' reducer stage at all.

### 4.4 Why vertical_size_monitor gets 600, not the bytes-formula 200
- Stage 11 (and the identical stage 17): 128 tasks read 25.6 GiB of shuffle (200 MiB per task, already under the 256 MiB rule of thumb) yet spill 136.0 GiB of in-memory data (1.06 GiB per task) and 16.3 GiB to disk. The in-memory representation is 5.3x the compressed shuffle bytes and the spill files compress 8.3x (136.0 / 16.3), consistent with wide string rows.
- Peak execution memory per task (`Peak Execution Memory`, max over tasks) is 0.8 GiB, so a task got about 0.8 GiB before its first spill; the nominal budget is 1,229 MiB (8g x 0.6 / 4 cores) but the model caches `vertical_compare_df` (L282) and storage can hold half the unified pool, leaving 614 MiB per task in the worst case.
- Bytes formula: ceil(25.6 GiB / 256 MiB) = 103 -> next 100 = 200 -> 0.68 GiB in memory per task: above the 614 MiB worst-case budget, inside the range that spills today.
- In-memory formula (the ticket question): ceil(136.0 GiB / 256 MiB) = 544 -> 600 -> 232 MiB in memory per task, 44 MiB of shuffle per task (sticks under adaptive coalescing, §4.3), 2.6x under the worst-case budget. 600 tasks over 49-89 executors x 4 cores = 2-3 waves.
- Side effects checked: stages 15, 19, 21 (128 tasks, 17.7-19.4 GiB read, no spill) drop to 30-33 MiB per partition and will coalesce in pairs to roughly 300; the write stays `coalesce(1)` (L375); driver is 16g + 4g overhead with 600 x 248 map tasks of map status, far under the INC-018 shape.

### 4.5 The 12 non-shuffle spills (why they leave this ticket)
Every one has `shuffle_read_bytes = 0` on the spilling stage: the tasks are the map side of an exchange, buffering and sorting their own shuffle output (`shuffle_write_bytes` is the stage's output). The reducer stage that consumes each of these shuffles was checked and does not spill: fangorn_prospecting_scoring stage 15 (2000 tasks, 2545.6 GiB read, 0 spill), ipdsc_ds_17 stage 6 (334, 16.3 GiB), the monitors' stages 20/25 (128 tasks, 7-13 GiB, 0 spill), ipdsc_ds_13 stage 3 (1000, 42.2 GiB), ipdsc_ds_47 stage 7 (5000, 248.4 GiB), fangorn_predictions_vertical stage 4 (997, 278.6 GiB), fangorn_household stage 3 (11, 3.8 GiB), advertiser_join stage 5 (14000, 2413.8 GiB). Raising `spark.sql.shuffle.partitions` changes only those reducer stages. Per-stage hand-off figures are in §8.
- ipdsc_ds_47 differs from the other 11: its spilling stage has `input_bytes = 0` too. The model reads `dw-main-silver.identity.crm_audience` through the BigQuery connector (`read_model("bigquery_data.BQ")`, `models/ipdsc/ipdsc_ds_47.py`), whose read streams report no Hadoop input metrics; 1952 tasks = read streams, and the spill happens in the map-side partial aggregation of `groupBy("ip").agg(collect_set(...))`. Neither `spark.sql.shuffle.partitions` nor `spark.sql.files.maxPartitionBytes` reaches that stage.

### 4.6 Validation of the edit (steps 6-7)
- Edit: `models/monitoring/vertical_size_monitor.py` L202 (decorator) and L234 (builder) `"128"` -> `"600"`; `grep -n shuffle.partitions` shows exactly those two lines. Log value (128) matched both sites before the edit (step 5).
- `MNTN_SDLC_ENV=dev uv run python model_upload.py --dryrun` on the clean worktree first: `Compiling all models` / `Skipping all models upload to 'dev' env`, `git status` empty (baseline: the JSON on main is current). After the edit: the same two lines plus `dags/model_task_config.json` with one hunk, `vertical_size_monitor.batch.runtime_config.properties["spark.sql.shuffle.partitions"]` 128 -> 600; `dags/ipdsc_third_party_audience_builders.json` unchanged.
- `uv run ruff check models/monitoring/vertical_size_monitor.py` (ruff 0.16.1): 6 findings (UP035, I001, UP006, UP045, DTZ007, DTZ005), identical count on `git show HEAD:` content, none on lines 202/234; the repo has no ruff config and CI does not lint models. Left alone.
- `JAVA_HOME=/opt/homebrew/opt/openjdk@17 PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python uv run python -m pytest tests/models -q`: 145 passed in 22.84 s.
- `git diff --stat`: `dags/model_task_config.json` (1 line) and `models/monitoring/vertical_size_monitor.py` (2 lines). Matches the verdict table's single `change` row.

## 5. Solution
**PR:** https://github.com/SteelHouse/airflow-ti/pull/1275 (opened 2026-09-03 PT; fast tier, 1 finding refuted, 0 confirmed)

- Branch `audi-1270-shuffle-partitions-verify-first` (worktree `scratchpad/wt/audi_1270`, base main `825b07e`): `models/monitoring/vertical_size_monitor.py` decorator L202 and builder L234 `spark.sql.shuffle.partitions` 128 -> 600; `dags/model_task_config.json` regenerated (one line). Uncommitted; the dispatcher commits, runs the gauntlet and opens the PR.
- PR body: `artifacts/audi_1270_pr_body.md` (lint `--kind pr` OK, 128 words / 835 chars). Result comment: `artifacts/audi_1270_result_comment.txt` (lint `--kind completion` OK).
- Verdict table: `outputs/audi_1270_verdict_table.csv` (21 rows, one per spilling stage, every DAG represented). Builder: `artifacts/audi_1270_build_verdict.py`; parser wrapper: `artifacts/audi_1270_stage_verdict.py`; archive name scan: `artifacts/audi_1270_archive_scan.sh`.

## 6. Questions Answered
- **Q:** For each of the 15 DAGs, is the spilling stage's spill shuffle-side?
  **A:** Two are: vertical_size_monitor (stages 11/17, 128 tasks read 25.6 GiB) and guid_log_advertiser_id_dsc_id (stages 13/24, 1000 tasks read 870.3 GiB). Eleven spill on the map side while reading files (fangorn_prospecting_scoring, ipdsc_ds_17, ipdsc_46/14/49_monitor, ipdsc_ds_13, fangorn_predictions_vertical, fangorn_household_predictions_vertical, advertiser_join, plus aug_log_ip under the floor), ipdsc_ds_47 spills on its BigQuery read stage, and ipdsc_ds_14 / guid_log_pivot_household_id_vertical_id do not spill in their last three runs.
- **Q:** What partition count puts about 256 MiB per task in memory?
  **A:** vertical_size_monitor 600 (136.0 GiB in memory / 256 MiB = 544 -> 600, 232 MiB per task); guid_log_advertiser_id_dsc_id 4100 (1007.9 GiB / 256 MiB = 4032 -> 4100, 252 MiB per task) versus AUDI-1269's 3400 (303 MiB per task).
- **Q:** Does the Dataproc Serverless runtime default `spark.sql.shuffle.partitions` to 200?
  **A:** No, 1000: seen on all four models that set nothing (ipdsc_ds_13, ipdsc_ds_14, ipdsc_ds_17, guid_log_advertiser_id_dsc_id), 9 runs.
- **Q:** When does adaptive coalescing undo a raised count?
  **A:** Only while two neighbouring partitions fit in the 64 MiB advisory: 1000 -> 334 at 16.7 MiB each (ipdsc_ds_17), unchanged at 43 MiB (ipdsc_ds_13) and 51 MiB (ipdsc_ds_47). A count sticks while shuffle bytes / count >= 32 MiB.

## 7. Data Documentation Updates
Nothing written to `knowledge/` by this wave (off-limits to the execute agent). Facts handed to the dispatcher for routing: the 1000 runtime default (4 more DAGs), the 32 MiB coalescing threshold, `memoryBytesSpilled` as the in-memory sizing input and its 5x gap from shuffle bytes on string-heavy stages, the Iceberg writer reducer stage ignoring `spark.sql.shuffle.partitions`, the BigQuery connector read stage reporting zero input bytes, the `.gstmp` gsutil temp files being complete, and the `tests/models` environment recipe (JAVA_HOME + PYSPARK_PYTHON).

## 8. Open Items / Follow-ups
1. **guid_log_advertiser_id_dsc_id (D1).** AUDI-1269 inserts `.config("spark.sql.shuffle.partitions", "3400")` in the builder. This ticket's stages 13 and 24 (1000 tasks, 870.3 GiB read, 1007.9 / 907.6 GiB in memory, 351.4 / 316.2 GiB to disk) size to 4100 by the in-memory rule (3500 by shuffle bytes). At 3400 each task holds 303 MiB in memory and 262 MiB of shuffle, well under the 1,440 MiB nominal budget, so 3400 is expected to stop the spill. After 1269 merges: if `disk_spill:13` / `disk_spill:24` persist in the ledger, change that one builder line to 4100 (sticks under coalescing: 217 MiB per partition; driver 9600m with 5576 map tasks stays under the 5000 cap of D3).
2. **AUDI-1273 hand-off (11 map-side stages).** Columns: input GiB / tasks = MiB per task today; shuffle write / input = expansion; in-memory per task at spill; current `files.maxPartitionBytes`.
   - fangorn_prospecting_scoring st13: 64.7 GiB / 2048 = 32 MiB per task; 39x; 5.7 GiB; default 128 MiB. Tasks already read one 32 MiB file each, so a lower `maxPartitionBytes` only helps if the parquet row groups are smaller than the files. st14: 0.7 GiB / 21 tasks, 37x, 0.7 GiB.
   - ipdsc_ds_17 st4: 17.9 GiB / 100 = 183 MiB per task (above the 128 MiB default, so the input is not being split: check whether the source is splittable); 0.9x; 5.0 GiB.
   - ipdsc_46_monitor st10/11: 2.2 GiB / 20 = 113 MiB; 5.8x; 4.6 GiB. ipdsc_14_monitor st10/11: 1.6 GiB / 14 = 117 MiB; 4.9x; 4.7 GiB. ipdsc_49_monitor st10/11: 1.3 GiB / 13 = 102 MiB; 5.7x; 3.9 GiB. All three: executor 8g, default 128 MiB.
   - ipdsc_ds_13 st1: 25.0 GiB / 248 = 103 MiB; 1.7x; 0.9 GiB; default 128 MiB.
   - fangorn_predictions_vertical st2: 127.7 GiB / 717 = 182 MiB; 2.2x; 1.0 GiB; **512 MiB set at L35** (the direct 1273 case: 128 MiB would give ~2,900 tasks). fangorn_household_predictions_vertical st1: 3.3 GiB / 9 = 375 MiB; 1.2x; 0.9 GiB; 512 MiB set at L35.
   - advertiser_join st3: 390.2 GiB / 4798 = 83 MiB; 6.2x; 2.75 GiB against 3,686 MiB nominal; default 128 MiB; task skew 11.0x (AUDI-1275).
   - aug_log_ip st1: 22.1 GiB / 205 = 110 MiB; 1.6x; 0.15 GiB; 0.7-1.3 GiB disk in the last three runs, under the 2 GiB floor, no action.
3. **ipdsc_ds_47 (code).** Spill is in the map-side partial `collect_set` aggregation over the BigQuery read (1952 read streams, 681.9 GiB in memory, 44.3 GiB disk, skew 5.9x). Levers are in the model (read parallelism or the aggregation), not in either config knob; straggler side belongs to AUDI-1275.
4. **Post-merge verification for vertical_size_monitor** (dispatcher / next wave): the builder value applies on the first run after the `.py` syncs to `ti_resources_v2/main`, the decorator value after the bundle is adopted (tens of minutes to 12 h, memory `reference_airflow_ti`). Check the next event log (`Populate vertical_size_monitor.VerticalSizeMonitor`, 00:05 UTC schedule, `dags/create_ip_vertical_assocations.py`): `spark_props` 600, the two 25.6 GiB reducer stages at 600 tasks, `mem_spill = disk_spill = 0`. Then `python -m include.spark_optimizer.ledger applied vertical_size_monitor disk_spill:11 <PR#> <date>` and the same for `disk_spill:17`. AUDI-1272 edits the same model file (initialExecutors); whichever lands second rebases and re-runs the dryrun.
5. **No-spill DAGs:** ipdsc_ds_14 and guid_log_pivot_household_id_vertical_id spilled nothing in their last three runs; nothing to change, nothing to stamp (no ledger keys).
6. **D2:** no entries; no spilling stage is governed by a code constant.

## Verification (adversarial, 2026-09-03)

Checked `git diff` in the worktree (`scratchpad/wt/audi_1270`) against §3.2/§5/§8, re-derived the target math independently, and re-ran (not just re-read) the tests and lint.

**Diff matches the claim exactly.** `git diff --stat` on the worktree touches only `models/monitoring/vertical_size_monitor.py` (L202 decorator + L234 builder, both `128`→`600`, no comment added) and `dags/model_task_config.json` (one property, `128`→`600`, no stray hunks). `grep -n shuffle.partitions` on the model file shows exactly those two lines. No writes outside the airflow-ti worktree and this ticket folder; `outputs/*.csv`/`*.tsv` are gitignored as claimed, the ledger `.jsonl` is unchanged from the planning-wave commit (byte-identical re-pull, consistent with "same row count" in §4.1).

**Target math re-derived from the verdict table, independent of the agent's arithmetic (all confirmed):**
- vertical_size_monitor: ceil(25.6 GiB shuffle_read / 256 MiB)=103, ceil(136.0 GiB mem_spill / 256 MiB)=544 → max → next 100 = **600**.
- guid_log_advertiser_id_dsc_id st13: ceil(1007.9 GiB / 256 MiB)=4032 → **4100**; st24: ceil(907.6 GiB / 256 MiB)=3631 → **3700**. At 3400 (AUDI-1269's value): 870.3 GiB / 3400 = 262 MiB shuffle/task, 1007.9 GiB / 3400 = 303 MiB in-memory/task — matches §8 item 1.
- Half-pool worst case 8g×0.3/4 cores = 614 MiB; nominal 8g×0.6/4 = 1229 MiB — both match §4.3/§4.4.

**Re-ran, not just re-read, in the worktree:**
- `tests/models` with the CI-equivalent env (`JAVA_HOME=/opt/homebrew/opt/openjdk@17`, `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON=.venv/bin/python`): **145 passed in 22.58s**, and `--collect-only` independently counts 145 — matches the claimed 145/22.84s.
- `ruff check models/monitoring/vertical_size_monitor.py` on the worktree and on `git show HEAD:` content: **identical 6 findings** (UP035, I001, UP006, UP045, DTZ007, DTZ005), none on lines 202 or 234 — matches.

**Defect 1 — the DAG/stage count in every prose surface is wrong, and contradicts this ticket's own verdict table.** `outputs/audi_1270_verdict_table.csv` (21 rows, source of truth per §5) actually has **9 unique DAGs / 13 stage-rows** with verdict `1273`, confirmed programmatically:
```
Counter({'1273': 13, 'no_spill': 3, 'change': 2, 'conflict_1269': 2, 'code': 1})   # 21 rows total
9 unique DAGs carry verdict=1273: advertiser_join, fangorn_household_predictions_vertical,
fangorn_predictions_vertical, fangorn_prospecting_scoring, ipdsc_14_monitor, ipdsc_46_monitor,
ipdsc_49_monitor, ipdsc_ds_13, ipdsc_ds_17
```
15 DAGs = 1 change + 1 conflict_1269 + 3 no_spill + 1 code + **9** 1273 (not 11). But the frontmatter `result:` field, §6 Q&A ("Eleven spill on the map side..."), §8 item 2 header ("AUDI-1273 hand-off (11 map-side stages)"), `artifacts/audi_1270_result_comment.txt`, and `artifacts/audi_1270_pr_body.md` all assert **11** (or 12 counting ipdsc_ds_47). The draft result comment's own count doesn't even close: 12 (input/BigQuery) + 1 (guid_log_advertiser_id_dsc_id) + 1 (vertical_size_monitor) + 3 (no_spill) = 17 ≠ 15 DAGs — an internal arithmetic error independent of how "11" was meant to be read.

**Defect 2 — the draft jira comment self-contradicts in three consecutive sentences.** Its opening line, "1 of 15 DAGs gets the raise... The other 14 do not spill on the shuffle-read side," is immediately contradicted by its own next bullet: "guid_log_advertiser_id_dsc_id is shuffle-side (870 GiB read, 1 TiB in memory)." One DAG is claimed both shuffle-side and not-shuffle-side two lines apart.

Both defects are reporting-only — the verdict table, the config change, the target math, and the §8 hand-off data rows are all correct and independently re-derived above; no re-analysis is needed, only the prose counts and the draft comment/PR body wording. `artifacts/audi_1270_pr_body.md` carries the same wrong "12" ("Of the other 14 DAGs, 12 spill while reading input or BigQuery") and needs the same fix before the dispatcher opens the PR. Corrected `jira_comment` (lints `--kind completion` OK, 116 words / 775 chars / 6 bullets) is in the dispatcher handoff.

**Verdict: `partial`.** The code change is correct, tested, and safe to ship; the ticket's own count and the draft PR/Jira text are not and must be fixed first.
