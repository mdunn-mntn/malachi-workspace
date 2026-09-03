---
doc_type: ticket
title: "AUDI-1273: Lower files.maxPartitionBytes on 3 map-side-spill DAGs"
status: backlog
date: 2026-09-02
summary: "Read input in smaller pieces on 3 DAGs that spill while reading, not shuffling"
result: "plan written 2026-09-02: ipdsc_ds_49 and conv_log_derived_ip edits executable as specified; ipdsc_ds_67 blocked, its 60 MiB single-row-group input files cannot be split by the knob, decision needed"
question: "Does lowering spark.sql.files.maxPartitionBytes on ipdsc_ds_49, conv_log_derived_ip and ipdsc_ds_67 remove their map-side spill?"
framing_state: locked
---

# AUDI-1273: Lower files.maxPartitionBytes on 3 map-side-spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1273
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does lowering spark.sql.files.maxPartitionBytes on ipdsc_ds_49, conv_log_derived_ip and ipdsc_ds_67 remove their map-side spill?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** One PR (branch AUDI-1273) merged setting 64 MiB, 128 MiB and 32 MiB respectively in each builder; the ledger marks the three spill findings resolved.
- **Approach (how):** Confirm current builder config on main (conv_log_derived_ip already overrides to 256 MiB), apply the three values, run model_upload.py --dryrun; confirm from the 08-27 sweep that the spill is in the input-read stage.
- **What would change the answer:** The spilling stage is a shuffle stage after all (1269/1270 mechanism), or the input is not splittable so the knob does nothing.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Three jobs overflow to disk while reading their input, not while shuffling, so the fix is reading in smaller pieces. Raising shuffle partitions does nothing here.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** `spark.sql.files.maxPartitionBytes` sets how much input one task reads at once. Too big and the task cannot hold it in memory, so it spills to disk.

**Task:** set in the builder:
- [ipdsc_ds_49](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_49.py) add 67108864 (64 MiB)
- [conv_log_derived_ip](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_2_derived/conv_log_derived_ip.py#L58) 268435456 -> 134217728 (256 -> 128 MiB)
- [ipdsc_ds_67](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_67.py) add 33554432 (32 MiB)

**Done-when:** PR merged; optimizer ledger shows the finding resolved (savings auto-measure).

## 3. Plan of Action
Planning wave 2026-09-02 (read-only; nothing executed, no file in airflow-ti touched). Verdict: **two of the three edits are executable as specified; the third (ipdsc_ds_67) hits the §0 kill criterion "input is not splittable so the knob does nothing" and needs a decision before the PR.**

### 3.0 What the planning wave established (facts every step below relies on)

**Source of truth for the knob.** `spark.sql.files.maxPartitionBytes` is a SQL conf; the only place any of the three DAGs sets Spark SQL confs is the `SparkSession.builder` chain inside the model class `__init__`. No base class creates a session (`grep getOrCreate utils_model/base_model/*.py utils_model/ipdsc/model.py` is empty), the `@compute.dataproc_batch(runtime_properties=...)` decorator only carries `spark.dynamicAllocation.*`, and `dags/model_task_config.json` is generated from the decorator, not the builder. So a builder-only edit does not change `model_task_config.json` (the execute step still runs the regeneration and proves the diff is empty). On airflow-ti main `825b07e` (2026-09-02, "Merge pull request #1265"):
- `models/ipdsc/ipdsc_ds_49.py` lines 40-44: builder sets only `spark.sql.shuffle.partitions=1700` (line 42). Last commit 2026-04-13 (rkleck-mntn).
- `models/feature_store/feature_group_2_derived/conv_log_derived_ip.py` lines 56-62: builder sets `spark.sql.files.maxPartitionBytes=268435456` (line 58), `openCostInBytes=8388608` (59), `parquet.block.size=134217728` (60). Last commit 2026-06-16.
- `models/ipdsc/ipdsc_ds_67.py` lines 29-32: bare builder (appName only). Last commit 2026-08-04 (Alyson Lefkowitz; model added 2026-07-31).
- Fleet precedent for the knob in a builder: 30+ models under `models/feature_store/` and `models/machine_learning/` set it the same way (e.g. `bae_ip.py:48`, `fangorn_predictions_vertical.py:35`), so the edit shape is established.

**Event logs exist, parse, and carry the builder conf.** All six app logs the 08-27 sweep cites are in the batch-fleet archive `gs://mntn-data-archive-prod/spark-events/` (0.4-1.8 MB each; no PHS/PAM grant needed for these three DAGs, the PHS bucket also listed fine under today's grant). Parsed with `include/spark_optimizer/eventlog.py` via the `zstd` CLI fallback (`/opt/homebrew/bin/zstd`; the `zstandard` Python module is not installed locally). `artifacts/audi_1273_eventlog_probe.py` prints the per-stage table, the input-split confs, and the scan/join plan lines; output for the three 08-05 logs is in `outputs/audi_1273_eventlog_probe_2026_08_05.txt`:
- `ipdsc_ds_49` app-20260805041408174-0229: stage 1 = 583 tasks, 47.2 GiB input, 0 shuffle read, 71.2 GiB shuffle write, 406.7 GiB memory spilled, 17.9 GiB disk spilled. Executors 9600m / 4 cores. `maxPartitionBytes` unset (platform default 128 MiB).
- `conv_log_derived_ip` app-20260805012514130-0370: stage 1 = 91 tasks, 14.5 GiB input, 20.6 GiB shuffle write, 58.4 GiB memory / 5.0 GiB disk spilled. **The environment surface shows `spark.sql.files.maxPartitionBytes = 268435456`**, so a builder conf lands in the event log and the post-merge check can read the new value directly.
- `ipdsc_ds_67` app-20260805053727558-0431: stages 3 and 5 = 160 tasks each, 10.6 GiB input, 81.6 GiB shuffle write, 129 GiB memory / ~80 GiB disk spilled each. Plan lines: `Generate explode(data_source_category_ids)` over `FileScan parquet [ip,data_source_category_ids]`, feeding `SortMergeJoin [data_source_category_id]` against `Scan JDBCRelation` (the `ui.audience_uploads` query); AQE later rewrites the join to `BroadcastHashJoin ... BuildRight` with a `ReusedExchange`, i.e. the small Postgres side is broadcast at runtime, but only after the 81.6 GiB map-side shuffle write has already happened. Stage 5 is the second pass over the same scan (`upload_ips` is used twice, never cached).

**Input layout per DAG (the splittability check, `outputs/audi_1273_input_parquet_probe_2026_09_02.txt`).** Spark's parquet reader hands a row group to the file split that contains the row group's midpoint, so a split cap smaller than a file's single row group produces one task that reads the whole file and one that reads nothing.
- `ipdsc_ds_49` reads `gs://mntn-data-archive-prod/ipdsc_site_network/site_network_hourly/dt=<7 days>/hh=*/`: 716 files/day, median 13 MiB, max 26 MiB, one row group each. Files are already smaller than 64 MiB; the cap only changes how many whole files pack into one task (today ~82 MiB per task with the 128 MiB cap and a 4 MiB open cost per file; ~45 MiB per task at 64 MiB). **Knob works.**
- `conv_log_derived_ip` reads `feature_store/feature_group_1_source/conv_log_ip/dt=<30 days>/`: 8 files/day, one row group each; 62-93 MiB per file through 2026-08-19, **9-11 MiB per file from 2026-08-20** (see the volume drop under Risks). Files are below 128 MiB either way, whole-file packing. **Knob works**, though the per-run input is now ~2.6 GiB, not 14.5 GiB.
- `ipdsc_ds_67` stage 3/5 read `gs://mntn-data-archive-prod/ipdsc/dt=<date>/data_source_id=4/`: dt=2026-08-05 = 160 files, median 67 MiB, 10.6 GiB total (matches the log exactly); dt=2026-08-31 = 162 files, all 60 MiB; two sampled files each hold **one row group** (1.32 M rows, 69 MiB uncompressed). **A 32 MiB cap cannot divide them**: the split [0,32 MiB) reads the whole file, the split [32,60) reads zero rows. The knob is a no-op on this DAG. DS4 is written by the targeted-signal pipeline (`spark/data_source/populate_data_source.py`, `include/spark/data_source/targeted_signal_cluster.py`), not by an `models/` file.

**Ledger.** The local copy `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimization_ledger.jsonl` stops at 2026-08-26. The live ledger is `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` (1,352 rows, 2026-08-21..2026-09-02, the `spark_optimizer_daily` DAG restores it from GCS each sweep, `dags/spark_optimizer_daily.py:28-33`). Latest state of the four keys this ticket must resolve: `ipdsc_ds_49 disk_spill:1` chronic streak 6 (09-02: 20.9 GiB disk, 3.1 exec-h); `conv_log_derived_ip disk_spill:1` chronic streak 6 (09-02: 4.2 GiB disk, 0.3 exec-h, shrinking daily since 08-20); `ipdsc_ds_67 disk_spill:3` and `disk_spill:5` chronic streak 8 (09-02: 72.8 / 69.1 GiB disk, 9.4 exec-h). `fix_pr` empty on all. Stamp command: `python -m include.spark_optimizer.ledger applied <dag_id> <key> <pr> <YYYY-MM-DD>` with `OPTIMIZER_LEDGER` pointing at a fresh download of the GCS file, then re-upload (the recipe used for PR #1231 on 2026-08-27, memory `project_airflow_optimizer`). A key resolves after `RESOLVE_SWEEPS = 3` quiet sweeps (`include/spark_optimizer/ledger.py:36`).

**Validation tooling in the repo.** `.github/workflows/pr_model.yaml` runs on any PR touching `models/**`: job 1 = `python model_upload.py --dryrun` (env `MNTN_SDLC_ENV=dev`, deps `uv export --only-group models`) then fails if `dags/model_task_config.json` or `dags/ipdsc_third_party_audience_builders.json` differ; job 2 = `python -m pytest tests/models -v` (not a required check; known red on fresh checkouts since PR #1209 because a model imports the git-ignored generated `utils_model/model_core/model_config.json`, noted 2026-08-26 in `audi_1194_handoff.md`). `--dryrun` compiles every model (imports the module, runs the decorators, does not instantiate the class, so the builder never runs), rewrites `dags/model_task_config.json`, `dags/current_branch.json` (git-ignored) and `utils_model/model_core/model_config.json`, then returns before any upload. Local toolchain present: `uv 0.11.3`, `python3.11`, `pyarrow 14.0.2`, `zstd`.

**Schedules (for the post-merge check).** `ipdsc_ds_49` and `ipdsc_ds_67` are tasks of `dags/tpa_export/tpa_ipdsc_export.py` (schedule `35 2 * * *` UTC; the 08-05 apps started 04:14 and 05:37 UTC). `conv_log_derived_ip` is a task of `dags/models/feature_store_setup_model.py` (schedule `3 1 * * *` UTC; app started 01:25 UTC) plus the monthly snapshot in `dags/models/feature_store_snapshot.py`. First prod execution after merge is the next scheduled run; never trigger one by hand (memory `feedback_airflow_prod_safety`).

### 3.1 Steps for the execute wave

Work only inside the AUDI-1273 worktree the dispatcher creates; agents never run git write commands, never touch Jira, never trigger a DAG. Paths below are relative to the worktree root unless stated.

1. **Confirm the worktree matches the plan's baseline.** `git log -1 --format=%H` must be `825b07e30d1ac10dd4f8f387c8b14e916c3f3114` or a descendant. Verify the anchors by content, not line number: `grep -n '"spark.sql.shuffle.partitions", "1700"' models/ipdsc/ipdsc_ds_49.py` (expect line 42); `grep -n '"spark.sql.files.maxPartitionBytes", "268435456"' models/feature_store/feature_group_2_derived/conv_log_derived_ip.py` (expect line 58); `grep -n 'SparkSession.builder.appName' models/ipdsc/ipdsc_ds_67.py` (expect line 30) and confirm no `.config(` follows it. If any anchor moved, re-read the file and adjust; if any builder already sets `maxPartitionBytes`, stop and report.
2. **Edit `models/ipdsc/ipdsc_ds_49.py`.** Insert one line after line 42 (`.config("spark.sql.shuffle.partitions", "1700")`), same indentation (12 spaces): `.config("spark.sql.files.maxPartitionBytes", "67108864")`. No comment. Resulting builder = appName, shuffle.partitions, maxPartitionBytes, getOrCreate.
3. **Edit `models/feature_store/feature_group_2_derived/conv_log_derived_ip.py` line 58.** Change `"268435456"` to `"134217728"` in `.config("spark.sql.files.maxPartitionBytes", ...)`. Lines 59-60 (openCostInBytes, parquet.block.size) stay.
4. **`ipdsc_ds_67`: follow the user's decision (see Decisions, §3.3 D1).** Do not apply the 32 MiB value from the ticket description; the planning wave showed it does nothing on 60 MiB single-row-group inputs.
   - **Option A (drop from this PR):** no edit. Record in §4 and hand the dispatcher a ledger note for `ipdsc_ds_67 disk_spill:3` and `disk_spill:5`: `python -m include.spark_optimizer.ledger set ipdsc_ds_67 disk_spill:3 wont_fix "input is 160 x 60 MiB single-row-group parquet; maxPartitionBytes cannot split it; needs writer or join change"` (and the same for `disk_spill:5`). The Jira description's third bullet is then corrected by the dispatcher.
   - **Option B (broadcast the small side instead, one-line code change, same file):** in `models/ipdsc/ipdsc_ds_67.py` line 80, change `ips.join(audience_upload_ids, on="data_source_category_id", how="inner")` to `ips.join(F.broadcast(audience_upload_ids), on="data_source_category_id", how="inner")`. `F` is already imported (line 7). This removes the sort-merge exchange that stages 3 and 5 write (81.6 GiB each, the whole spill), because the join then happens inside the scan task and drops non-ego rows before any shuffle. Size guard: the 08-05 log already shows AQE converting this join to `BroadcastHashJoin ... BuildRight` over the JDBC scan at runtime, which Spark only does when that side is under the 10 MB adaptive broadcast threshold; the static hint broadcasts the same data before the exchange instead of after it. Re-check the guard in the post-merge log (step 10): if `ui.audience_uploads` type-8 rows ever grow past the threshold the hint still forces the broadcast, so record the broadcast size from the plan metrics.
   - **Option C (fix the writer):** out of this ticket; the DS4 writer belongs to the targeted-signal pipeline and would need `parquet.block.size` lowered there. Record as an `improvements_backlog.md` row via the dispatcher, no edit here.
5. **Validate the diff shape.** `git diff --stat` shows exactly the edited files (2, or 3 under Option B), one or two changed lines each. `python3 -m py_compile` on each edited file. `git diff` must contain no comment lines and no whitespace-only hunks.
6. **Regenerate and prove nothing else moved.** In the worktree: `uv sync --group models --group test` (first time only), then `MNTN_SDLC_ENV=dev python model_upload.py --dryrun`. Expect the console line `Skipping all models upload to 'dev' env`. Then `git diff --quiet dags/model_task_config.json dags/ipdsc_third_party_audience_builders.json && echo CONFIG_UNCHANGED`. Expected result: CONFIG_UNCHANGED (builder-only edits do not reach the generated task config). If the diff is NOT empty: inspect `git diff dags/model_task_config.json`; if the hunks are confined to the three model ids and reflect these edits (they should not be), include the file; if the hunks touch other models, main was already stale and the regeneration is someone else's change: stop, do not stage it, report the model ids in §8. `git status --porcelain` should list only the edited model files plus `dags/model_task_config.json` if regenerated; `dags/current_branch.json` and `utils_model/model_core/model_config.json` are generated scratch and must not be staged.
7. **Run the model tests once**: `python -m pytest tests/models -q` after step 6 (the generated `model_config.json` from the dryrun is what the collection-time import needs). Record pass/fail counts in §4. A failure in `test_ipdsc_third_party_audience_builder.py` or another test that also fails on an untouched checkout of main is the pre-existing #1209 defect, not this PR; note it, do not fix it here.
8. **Write the PR description** to `artifacts/audi_1273_pr_description.md` and lint it: `python3 .claude/scripts/lint_comms.py --kind pr --file <path>`. Shape: answer line ("Lower the input read size on two spill-at-read DAGs" plus the third DAG's disposition), What (the exact confs and files), Why (per DAG: input GiB / tasks / spill from the 08-05 logs above), Validation (dryrun clean, config unchanged, tests result, post-merge check in step 10). No `Co-Authored-By`; no comments in code; the dispatcher opens the PR on branch `AUDI-1273` after `/pr_gauntlet`.
9. **Ledger stamp (dispatcher, after merge, same day).** For each shipped key run `python -m include.spark_optimizer.ledger applied <dag_id> <key> <PR#> <merge date>` against a fresh download of `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` (`OPTIMIZER_LEDGER=<local path>`), then re-upload between sweeps. Keys: `ipdsc_ds_49 disk_spill:1`, `conv_log_derived_ip disk_spill:1`, and under Option B also `ipdsc_ds_67 disk_spill:3`, `ipdsc_ds_67 disk_spill:5`. Under Option A set those two to `wont_fix` with the note from step 4 instead.
10. **Post-merge check (first scheduled prod run after deploy, T+1 to T+3 days; no manual trigger).** For each DAG: `gsutil ls -l gs://mntn-data-archive-prod/spark-events/ | grep <YYYY-MM-DD>T0[1-6]` to find that morning's logs, download with `gsutil -o "GSUtil:check_hashes=never" cp` into `outputs/eventlogs/` (delete after parsing), run `python3 artifacts/audi_1273_eventlog_probe.py --repo <airflow-ti checkout> <log>` and match `app_name` (`Populate ipdsc_ds_49.DS49`, `Populate conv_log_derived_ip.ConvLogDerivedIp`, `Populate ipdsc_ds_67.DS67`). Pass criteria: the `conf spark.sql.files.maxPartitionBytes` line shows the new value; `ipdsc_ds_49` stage 1 task count roughly doubles (583 to ~1,000-1,200 on similar input) with memory-spilled and disk-spilled at or near 0; `conv_log_derived_ip` stage 1 per-task input roughly halves and spill drops; under Option B `ipdsc_ds_67` stages 3/5 show shuffle write well under 81.6 GiB and no spill. Write the before/after table into §4 and §5, then let the ledger resolve the keys after three quiet sweeps (check with `gsutil cat <ledger> | grep <dag_id>`).
11. **Close-out**: update §4-§8 of this file, hand `knowledge/` facts back through the dispatcher (the row-group rule, the conversion_log volume drop, the ds67 mechanism), self-review entry.

### 3.2 Sources
- Spec and pre-verified values: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` (items 4, 7, 8) and `audi_1194_hackathon_optimizations_2026_08_27.md` (rows 8, 12, 13 with the per-app evidence).
- Ledger: local `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimization_ledger.jsonl` (to 08-26); live `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` (to 09-02).
- airflow-ti read-only checkout `/Users/malachi/Developer/work/mntn/airflow-ti-main` at `825b07e`: the three model files, `utils_model/base_model/compute.py`, `utils_model/model_core/context.py:247` (`compile_models`), `model_upload.py`, `.github/workflows/pr_model.yaml`, `include/spark_optimizer/{eventlog,ledger}.py`, `dags/spark_optimizer_daily.py`, `dags/tpa_export/tpa_ipdsc_export.py`, `dags/models/feature_store_setup_model.py`.
- Event logs: `gs://mntn-data-archive-prod/spark-events/app-20260805041408174-0229.zstd`, `app-20260806041813727-0821.zstd` (ds49); `app-20260805012514130-0370.zstd`, `app-20260806012306304-0847.zstd` (conv_log_derived_ip); `app-20260805053727558-0431.zstd`, `app-20260806045914626-0006.zstd` (ds67). Parsed output: `outputs/audi_1273_eventlog_probe_2026_08_05.txt`.
- Input parquet probe: `outputs/audi_1273_input_parquet_probe_2026_09_02.txt` (GCS listings plus pyarrow footers of one or two files per input; binaries deleted).
- Memory: `knowledge/memory/project_airflow_optimizer.md` (ledger stamp recipe, regeneration rule, PR #1231 precedent), `knowledge/memory/feedback_airflow_prod_safety.md` (dev-only `model_run.py`, no manual prod triggers, `--dryrun` before push).
- Jira AUDI-1273 (read 2026-09-02): Task, Backlog, parent AUDI-1290, labels hackathon + q3_2026, no story points set, no comments.

### 3.3 Assumptions the execute wave resolves empirically first, and decisions
- **A1** The three anchors (step 1) are unchanged on the worktree's base commit. Check by grep before editing.
- **A2** A builder-only edit leaves `dags/model_task_config.json` byte-identical after `--dryrun`. Check in step 6; a non-empty diff is a stop condition, not something to commit through.
- **A3** Spark 3.5.3 (Dataproc runtime 2.3, per `model_task_config.json` `version: "2.3"`) assigns each parquet row group to the split holding its midpoint, which is why the 32 MiB cap is a no-op on 60 MiB single-row-group files. Cheap local confirmation if wanted: `pip install pyspark==3.5.3` in a scratch venv, read one downloaded DS4 file with `spark.sql.files.maxPartitionBytes=33554432`, and print `df.rdd.glom().map(len).collect()`; expect one partition with all 1.32 M rows and one with 0.
- **A4** `ipdsc_ds_49` per-task in-memory footprint after the change (~350 MiB at ~45 MiB input, using the sweep's 8.6x expansion ratio) fits the ~1.4 GiB execution share per task slot on 9600m / 4-core executors. Only the post-merge log settles it (step 10); if spill persists, the next lever is executor memory, not a further cap reduction.
- **A5** `conv_log_derived_ip` savings are now small: the 30-day input is ~2.6 GiB since the upstream volume drop (Risks), and the 09-02 sweep already shows the spill shrinking (6.3 to 4.2 GiB disk). The edit stays in scope (harmless, matches the ticket), but the ledger may resolve this key on its own by ~2026-09-19 and attribute it to the PR.
- **D1 (user decision, blocks the third edit):** `ipdsc_ds_67`: Option A drop from the PR and mark `wont_fix` with the reason; Option B replace the config change with the one-line `F.broadcast(audience_upload_ids)` join hint (largest expected win: removes two 81.6 GiB shuffle writes and ~150 GiB of disk spill per run, ~9.4 exec-h/day on the 09-02 sweep); Option C ask the targeted-signal owners to write DS4 with smaller row groups (out of scope here).
- **D2 (new-work flag, §14):** `gs://mntn-data-archive-prod/conversion_log/dt=<date>` fell from 18-23 GiB/day (08-18, 08-19) to 3-4 GiB/day from 2026-08-20 with no change to `conv_log_ip.py`. Either an intended upstream change or a data loss feeding the feature store and Fangorn. Proposed: a Spike under AUDI, "conversion_log volume drop 2026-08-20", not part of this ticket.

### 3.4 Risks
- **Wrong lever on ds67 if executed as written:** the 32 MiB cap would add ~160 empty tasks per pass and change no spill; the ledger would then mark the PR `fix_not_working` after three sweeps.
- **Savings attribution confound on conv_log_derived_ip** (A5): the key can resolve from the upstream volume drop rather than this change.
- **Task-count growth on ds49** (~583 to ~1,100 tasks) raises scheduler and shuffle block counts (1,100 maps x 1,700 reducers = 1.9 M blocks, ~38 KiB each, well above the ~1.7 KiB tiny-block regime that hurt fetch wait elsewhere in the fleet). Acceptable; checked in step 10 via stage 3 fetch-wait.
- **Generated-file drift:** if main's `model_task_config.json` is stale for unrelated models, the CI regeneration check fails on this PR through no fault of these edits (step 6 stop condition).
- **CI `model-unit-test` red for the pre-existing #1209 reason** (not a required check); do not chase it inside this PR.
- **First prod run is the next cron** (no manual trigger), so verification lands 1-3 days after deploy; a regression shows up as spill still present or a slower stage, and the revert is the same one-line change.

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
