---
doc_type: ticket
title: "AUDI-1274: Set AQE advisoryPartitionSizeInBytes=16m on the 2 pivot DAGs"
status: in_progress
date: 2026-09-02
summary: "Cap AQE coalesce target at 16m on the two guid pivot DAGs where shuffle.partitions is a no-op"
result: "executed 2026-09-02: one builder line added to both models on branch audi-1274-aqe-advisory-pivot, dry run exit 0 with generated config unchanged; pending commit, gauntlet, PR, merge, ledger stamp and the post-merge run check"
question: "Does spark.sql.adaptive.advisoryPartitionSizeInBytes=16m stop guid_log_pivot_ip_vertical_id and guid_conv_log_pivot_ip_vertical_id from spilling after AQE coalesces their shuffle back to about 800 partitions?"
framing_state: locked
---

# AUDI-1274: Set AQE advisoryPartitionSizeInBytes=16m on the 2 pivot DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1274
**Status:** in_progress
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does spark.sql.adaptive.advisoryPartitionSizeInBytes=16m stop guid_log_pivot_ip_vertical_id and guid_conv_log_pivot_ip_vertical_id from spilling after AQE coalesces their shuffle back to about 800 partitions?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** One PR (branch AUDI-1274) merged adding the builder config to both models; the ledger marks both spill findings resolved.
- **Approach (how):** Confirm in the event log that AQE coalesces about 8000 to about 800 partitions (SQL plan AQEShuffleRead node); add the config; model_upload.py --dryrun.
- **What would change the answer:** No coalescing in the plan, in which case shuffle.partitions is the lever and the ticket becomes a 1270 item.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

The two pivot jobs spill to disk because Spark's auto-tuner merges their work chunks back into oversized ones; cap the merged size at 16 MB.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** AQE (adaptive query execution) coalesces about 8000 partitions back to about 800 after the shuffle, so raising `spark.sql.shuffle.partitions` is a no-op. The knob that sticks is the target size AQE merges to.

**Task:** add builder config `spark.sql.adaptive.advisoryPartitionSizeInBytes=16m`:
- [guid_log_pivot_ip_vertical_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id.py)
- [guid_conv_log_pivot_ip_vertical_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_3_pivoted/guid_conv_log_pivot_ip_vertical_id.py)

**Done-when:** PR merged; optimizer ledger shows the finding resolved (savings auto-measure).

## 3. Plan of Action
Written 2026-09-02 (planning wave). Every path below is absolute or relative to the airflow-ti worktree; another agent can execute without this session's context.

**Execution record (execute wave, 2026-09-02, same day).** Steps 1 to 6 and 8 executed as written; evidence per step in §4 "Execute wave". Step 7 skipped (no Java on this Mac) and replaced by a string check of the pyspark 3.5.3 jars in the dry-run venv (§4, A3). Steps 9 and 10 wait on the merge. One number in the plan was refined, not reversed: the expected pivot-stage task count is 3,100 to 4,000, not "about 3,100"; the 3,100 was total bytes / 16 MiB and is the floor, the greedy merge over 6.3 MiB granules lands at 4,000 (§4, post-change arithmetic). Steps 8 and 9 below carry the refined band. No `dags/` file changed; the ruff findings on the two files are pre-existing at `825b07e` and were left alone.

**Worktree and branch.** The dispatcher's worktree is `/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/67074af2-5859-4b02-9a41-1fb172083596/scratchpad/wt/audi_1274` on branch `audi-1274-aqe-advisory-pivot`, HEAD `825b07e30d1ac10dd4f8f387c8b14e916c3f3114` (= origin/main, merge of #1265, 2026-09-02 17:14 PT), tree clean, no `.venv`. The read-only reference checkout `/Users/malachi/Developer/work/mntn/airflow-ti-main` sits on the same commit.

### Steps
1. **Pre-flight (read-only git).** In the worktree: `git status --short` must be empty and `git rev-parse HEAD` must equal `git rev-parse origin/main` after a `git fetch` by the dispatcher. If main moved, re-read both model files' lines 49-55 before editing; the insert point below assumes the file content at `825b07e`.
2. **Edit file 1:** `models/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id.py`. The Spark builder is lines 49-55:
   ```
   49        self.__spark = (
   50            SparkSession.builder.appName(f"Populate {self.model_id()}")
   51            .config("spark.sql.files.maxPartitionBytes", "268435456")
   52            .config("spark.sql.files.openCostInBytes", "8388608")
   53            .config("spark.sql.parquet.block.size", "134217728")
   54            .getOrCreate()
   55        )
   ```
   Insert ONE line after line 53, same 12-space indent, so `.getOrCreate()` becomes line 55:
   `            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16m")`
   No comment, no other change. Leave the decorator `runtime_properties` (lines 21-31, `spark.sql.shuffle.partitions: "8000"` at line 26) untouched.
3. **Edit file 2:** `models/feature_store/feature_group_3_pivoted/guid_conv_log_pivot_ip_vertical_id.py`. Lines 49-55 are byte-identical to file 1; apply the identical insert after line 53.
4. **Do not touch:** `models/feature_store/feature_group_3_pivoted/guid_log_pivot_household_id_vertical_id.py` (same builder shape, but it is on AUDI-1270's verify-first disk-spill list, not this ticket); `dags/model_task_config.json` by hand (the builder line is not serialized into it, see step 6); any `dags/*.py`.
5. **Local validation, part 1:** `git diff --stat` shows exactly the 2 files, +1 line each. `python3 -m py_compile <file1> <file2>` passes. `ruff check <file1> <file2>` from the worktree root passes (models/ is outside CI's ruff scope, which covers only `include/airflow_debugger` and `include/spark_optimizer`; keep it clean anyway).
6. **Local validation, part 2, the CI gate replicated** (`.github/workflows/pr_model.yaml` job `model-upload-dryryn`, triggered by any change under `models/**`). In the worktree:
   ```
   uv venv --python 3.11 .venv
   uv export --no-hashes --only-group models -o requirements-model-upload.txt
   VIRTUAL_ENV=$PWD/.venv uv pip install -r requirements-model-upload.txt
   MNTN_SDLC_ENV=dev .venv/bin/python model_upload.py --dryrun
   git diff --quiet dags/model_task_config.json dags/ipdsc_third_party_audience_builders.json && echo CLEAN
   ```
   Expected: `Compiling all models` ... `Skipping all models upload to 'dev' env`, then `CLEAN`. The generated JSON must NOT change: `compute.dataproc_batch` (`utils_model/base_model/compute.py:59`) serializes only the decorator arguments into `DataprocBatchCfgSerializable`, so a builder `.config()` never reaches `runtime_config.properties`. If the diff is non-empty: a diff inside the two pivot entries means the key landed in the decorator by mistake (revert, redo step 2-3); a diff anywhere else is pre-existing drift on main, stop and hand it to the dispatcher as a decision (CI would require committing it). If a `ModuleNotFoundError` surfaces during compile, `uv pip install` the missing package into `.venv` and rerun (known gotcha, `documentation/docs/airflow_ti_workflow.md` § Local dev environment gotcha). Delete `requirements-model-upload.txt` (untracked) before handing back; `.venv` is gitignored (`.gitignore:6`).
7. **Optional value-parse check (skip if no local Java; none on this Mac):** `.venv/bin/python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').config('spark.sql.adaptive.advisoryPartitionSizeInBytes','16m').getOrCreate(); print(s.conf.get('spark.sql.adaptive.advisoryPartitionSizeInBytes')); s.stop()"` prints `16m`. Spark byte-string syntax accepts `16m` (the default is documented as `64MB`), so this is a belt-and-braces check, not a gate.
8. **Hand-back to the dispatcher** (agents never commit): write `artifacts/audi_1274_pr_body.md` in this ticket folder with the PR text, lint it with `python3 .claude/scripts/lint_comms.py --kind pr --file ...`. Suggested commit subject: `AUDI-1274: cap AQE coalesce target at 16m on the 2 guid pivot models`. PR body facts to carry: the two 08-26 numbers from §4 (800 tasks, 49.1 GiB shuffle read, 862.5 GiB memory spilled, 19.0 GiB disk spilled per run), the expected after-state (3,100 to 4,000 tasks at 12 to 16 MiB compressed, about 215 to 280 MiB in memory each), the CI-gate result from step 6, and that no `dags/` file or DAG schedule changes. The dispatcher runs `/pr_gauntlet`, opens the PR against `main` (Ryan merges; merge to main auto-deploys the prod bundle and syncs the model files to prod GCS, memory `reference_airflow_ti`), and stamps the ledger (step 10). `model-unit-test` is red repo-wide since #1209 and is NOT a required check (UNSTABLE, not BLOCKED); do not treat it as a blocker.
9. **Post-merge verification, never a manual trigger** (memory `feedback_airflow_prod_safety`). First prod execution is the next scheduled `feature_store_setup_model` run (`dags/models/feature_store_setup_model.py:19`, `3 1 * * *` UTC, tasks at lines 246-255) after the deploy; the monthly snapshot variants (`dags/models/feature_store_snapshot.py`, `0 2 * * *`, day 2 = guid_conv, day 15 = guid pivot, lines 199-210) run the same model files and inherit the cap. The same morning (flagged apps' logs have vanished from `spark-events` within hours before, memory `project_airflow_optimizer` 2026-09-02): find the two app ids (the daily sweep's ledger row `app_id`, or `gsutil ls gs://mntn-data-archive-prod/spark-events/app-<YYYYMMDD>013*` and read `spark.app.name`), download with `gsutil -o "GSUtil:check_hashes=never" cp` into `outputs/`, and run `zstd -dc <log> | python3 artifacts/audi_1274_aqe_probe.py --stage 33 --stage 34`. Pass criteria: environment shows `spark.sql.adaptive.advisoryPartitionSizeInBytes = 16m`; every `AQEShuffleRead coalesced` node in the `save` execution reports about (its total GiB x 1024 / 16) coalesced partitions (1,100 to 2,000 per shuffle instead of 800); the pivot stage (was 33 / 34, the one with about 49 GiB shuffle read) runs 3,100 to 4,000 tasks with `disk_spill` 0.0 GiB and `mem_spill` far below 862 GiB; `exec_h` on the ledger row drops from the 8.4 / 8.5 baseline. Record the before/after table in §4 and §5.
10. **Ledger stamp (dispatcher, after merge):** on a local copy of `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` (1,352 rows as of 2026-09-02), with `PYTHONPATH=include OPTIMIZER_LEDGER=<local copy>`: `python -m spark_optimizer.ledger applied guid_log_pivot_ip_vertical_id disk_spill:33 <PR#> <merge YYYY-MM-DD>` and `python -m spark_optimizer.ledger applied guid_conv_log_pivot_ip_vertical_id disk_spill:34 <PR#> <merge YYYY-MM-DD>`, then upload the copy back (the 2026-08-27 procedure used for #1231). `applied` is not sticky: the finding flips to `resolved` after `RESOLVE_SWEEPS = 3` consecutive quiet sweeps (`include/spark_optimizer/ledger.py:36`) or to `fix_not_working` if it keeps firing. Done-when is met when both keys read `resolved`.

### Sources
- Spec rows 10-11: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_optimizations_2026_08_27.md` (table lines 35-36, evidence lines 137-146); items 5-6 in `.../audi_1194_hackathon_ticket_drafts.md` (lines 20-21).
- Jira AUDI-1274 (Task, Backlog, parent AUDI-1290, labels hackathon + q3_2026, no story points set, no comments) read 2026-09-02; description reproduced in §2.
- airflow-ti at `825b07e`: the two model files; `utils_model/base_model/compute.py:59-110`; `dags/model_task_config.json` entries for both models (decorator properties only); `dags/models/feature_store_setup_model.py`; `dags/models/feature_store_snapshot.py`; `.github/workflows/pr_model.yaml`; `include/spark_optimizer/eventlog.py` and `ledger.py`.
- Event logs: `outputs/app-20260826014549943-0459.zstd` (guid_log_pivot, 6.8 MiB) and `outputs/app-20260826013253579-0911.zstd` (guid_conv, 7.0 MiB); probe `artifacts/audi_1274_aqe_probe.py`. The 09-02 logs `app-20260902013718570-0386.zstd` / `app-20260902013749517-0909.zstd` still exist in `spark-events` (checked 2026-09-02 20:10 PT) if a fresher baseline is wanted.
- Prod ledger `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`, rows for both DAGs (§4).
- Memory: `feedback_airflow_prod_safety`, `reference_airflow_ti` (dryrun invocation, CI check status), `project_airflow_optimizer` (ledger stamp, vanishing logs).

### Assumptions to resolve empirically first (execute wave)
- A1. Worktree HEAD still equals origin/main and lines 49-55 of both files are unchanged (step 1).
- A2. The dry run leaves `dags/model_task_config.json` unchanged (step 6). Reading `compute.py` says it will; the run is the proof.
- A3. Spark accepts `16m` for this key on Dataproc Serverless runtime 2.3 / Spark 3.5.3 (step 7 if Java is available, otherwise the post-merge environment line in step 9 settles it).
- A4. The 800-partition floor is `spark.default.parallelism` (registered cores at planning time: 100 initial executors x 8 cores), not the 64 MiB advisory size (§4, contradiction record). The fix binds either way because 16 MiB is below both bounds; step 9's counts settle which bound was active.

### Risks
- R1. Session-wide effect: the cap applies to every coalesced shuffle in the job, not only the spilling stage. The six `save`-execution shuffles (17.7 to 31.4 GiB each) go from 800 to roughly 1,100 to 2,000 tasks each; the 0.06 GiB `collect` shuffle stays at about 49. Shuffle block sizes do not change (blocks are per map task x reduce partition; coalescing only merges contiguous reads), so the fetch-wait regime should not move. Watch scheduler overhead in the post-merge duration (4.8 / 5.6 min today).
- R2. Spec mechanism partly wrong (§4): if someone later raises `initialExecutors` on these DAGs the floor rises, but the 16 MiB cap keeps binding, so the fix survives that change.
- R3. If the spill persists after the change, the next lever is executor memory or `spark.memory.fraction`, not partitions; that is a new ticket, not an extension of this one.
- R4. Prod exposure: the first prod run is the scheduled one; if it fails, the DAG's own alerting fires and the revert is the same one-line diff.
- R5. `model-unit-test` red on every PR since #1209; not required, but a reviewer may ask. Cite `reference_airflow_ti`.

## 4. Investigation & Findings
Planning-wave verification, 2026-09-02 (read-only; no model file touched).

**Source of truth for each DAG's Spark config today (airflow-ti main `825b07e`).**
- Decorator `@compute.dataproc_batch(runtime_properties=...)` in each model file (lines 20-36) -> serialized by `model_upload.py --dryrun` into `dags/model_task_config.json` `<model_id>.batch.runtime_config.properties` (both entries carry exactly the 9 decorator keys, `spark.sql.shuffle.partitions: "8000"` among them, runtime version 2.3). `include/models/operators.py:296-320` adds only the env, event-log and label properties on top.
- Builder `SparkSession.builder...config(...)` in `__init__` (lines 49-55) -> session-level, applied at runtime, never in the JSON. Today: `maxPartitionBytes=268435456`, `openCostInBytes=8388608`, `parquet.block.size=134217728`. No `spark.sql.adaptive.*` key anywhere in either model or in their JSON entries. Fleet-wide, `spark.sql.adaptive.advisoryPartitionSizeInBytes` appears in no model file: this PR is the first use.
- The builder is the right home (spec says builder; memory `project_airflow_optimizer` "builder wins" over the decorator for the same key on intent_score_map).

**Event logs exist at the needed grain and parse.** Downloaded the ledger's 08-26 logs (7.2 MB / 7.3 MB) from `gs://mntn-data-archive-prod/spark-events`; both parse with `include/spark_optimizer/eventlog.py::parse_eventlog` (14 / 15 stages, 2 SQL executions, 100 executors, 4.8 / 5.6 min). The optimizer parser does NOT expose AQE nodes (it keeps only `SQLExecutionStart` plan info; the final adaptive plan arrives in `SparkListenerSQLAdaptiveExecutionUpdate`, which it ignores, so `node_metrics` shows 0 AQEShuffleRead nodes). `artifacts/audi_1274_aqe_probe.py` reads the raw log and joins the last adaptive plan's `AQEShuffleRead` metrics to their accumulators.

Probe output, `app-20260826014549943-0459` (guid_log_pivot) and `app-20260826013253579-0911` (guid_conv), Spark 3.5.3, `spark.sql.adaptive.enabled=true`, every `spark.sql.adaptive.coalescePartitions.*` and `advisoryPartitionSizeInBytes` unset (defaults), `spark.sql.shuffle.partitions=8000`, `executor.cores=8`, `executor.memory=19200m`, `initialExecutors=100`:

| execution | AQEShuffleRead node | coalesced partitions | per-partition size (avg = max) | total |
|---|---|---|---|---|
| 0 `collect` (distinct vertical_ids) | 1 | 49 | 1.3 MiB (max 2.2) | 0.06 GiB |
| 0 `collect` | 2 | 728 (guid_log) / 800 (guid_conv) | 28.0 / 25.5 MiB | 19.9 GiB |
| 1 `save` | 6 nodes | 800 each | 22.7, 24.3, 25.5, 27.3, 35.6, 40.2 MiB | 17.7, 19.0, 19.9, 21.3, 27.8, 31.4 GiB |

Spilling stage (stage 33 in guid_log_pivot, stage 34 in guid_conv, identical numbers): 800 tasks, shuffle read 49.1 GiB (= the 27.8 + 21.3 GiB pair, the pivot aggregate joined to the totals), shuffle write 23.5 GiB, memory bytes spilled 862.5 GiB, disk bytes spilled 19.0 GiB. Per task: 63 MiB compressed in, about 1.08 GiB in memory at spill time. No other stage spills. The final `repartition(500, "ip")` stage (41 / 42) is fixed-count and unaffected.

**Contradiction record (appended, not overwritten).** The spec (row 10) says "AQE coalesces to its 64 MiB advisory size (48.9 GiB / 800 = 61 MiB compressed)". The log says every coalesced shuffle in the job lands on exactly 800 partitions whatever its size (17.7 to 31.4 GiB, partitions of 22.7 to 40.2 MiB, all uniform, all below 64 MiB), and 800 = 100 initial executors x 8 cores. Reconciling hypothesis: with `spark.sql.adaptive.coalescePartitions.parallelismFirst` at its default `true`, AQE's target size is min(advisory 64 MiB, total bytes / `spark.default.parallelism`); at planning time default parallelism was 800 registered cores, so the second term (22 to 40 MiB) was the binding one and the count pinned at 800. The spec's 61 MiB figure is the two co-partitioned inputs summed per task, not one shuffle's partition size. Consequence for the fix: unchanged. With `advisoryPartitionSizeInBytes=16m` the target becomes min(16 MiB, 22 to 40 MiB) = 16 MiB, so the pivot stage's combined 49.1 GiB splits into about 3,100 partitions at about 280 MiB in memory each, against roughly 1.4 GB of unified memory per core on a 19200m / 8-core executor. The check that settles the hypothesis: post-merge, each shuffle's coalesced count should equal its GiB x 64 (16 MiB target), and the stage count about 3,100; if instead counts track registered cores, the floor was not what the log suggests.

**Ledger state (prod `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`, 1,352 rows, read 2026-09-02).**

| dag_id | key | latest date | state | streak | exec_h | app_id |
|---|---|---|---|---|---|---|
| guid_log_pivot_ip_vertical_id | disk_spill:33 | 2026-09-02 | chronic | 6 | 8.4 | app-20260902013718570-0386 |
| guid_conv_log_pivot_ip_vertical_id | disk_spill:34 | 2026-09-02 | chronic | 7 | 8.5 | app-20260902013749517-0909 |

Earlier rows: 08-25 recurring, 08-26/08-27 chronic (exec_h 6.4 to 9.0), 08-30 (guid_conv only); gaps match the downloader freeze closed 2026-09-02. No `fix_pr` / `applied_date` on either key. Both keys are live inputs for the done-when.

**DAG wiring.** Daily `feature_store_setup_model` (`3 1 * * *` UTC; the app ids' 01:3x timestamps match) runs both models as tasks `guid_log_pivot_ip_vertical_id` and `guid_conv_log_pivot_ip_vertical_id`; `feature_store_snapshot` (`0 2 * * *`) runs the `--snapshot monthly` variants on day 2 (guid_conv) and day 15 (guid pivot) with 8 features per vertical instead of 4, so a heavier pivot on the same builder. Downstream consumers gate on the pivot's `_SUCCESS` (fangorn inference at 18:00 UTC, fangorn training on the 16th monthly).

**Toolchain on this Mac.** `python3` 3.11.12 (no `zstandard` module; `zstd` CLI at `/opt/homebrew/bin/zstd`, which the parser falls back to), `uv` at `/opt/homebrew/bin/uv`, `ruff` 0.16.1, no Java runtime (so no local pyspark session). No PAM grant needed: both DAGs are batch-fleet, their logs live in `spark-events`, not the PHS temp bucket.

**Execute wave, 2026-09-02 (this session, worktree `.../scratchpad/wt/audi_1274`, branch `audi-1274-aqe-advisory-pivot`).**

- A1 held. Worktree HEAD `825b07e` = local `origin/main` = remote `refs/heads/main` (`git ls-remote`, no fetch); `git status --short` empty; lines 49-55 of both files byte-identical to each other and to the plan (diff of the two ranges empty). No `spark.sql.adaptive.advisoryPartitionSizeInBytes` anywhere under `models/`, `utils_model/`, `include/models/` (only `adaptive.enabled` / `coalescePartitions.enabled` / `skewJoin.enabled` in four `models/machine_learning/` decorators).
- Edit applied with `sed '53a'` in both files: one line `.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16m")` at line 54, 12-space indent, `.getOrCreate()` now line 55. `git diff --stat` = 2 files, +1 line each, nothing else. `python3 -m py_compile` passes on both.
- ruff 0.16.1 on both files: 2 findings per file, both present on the HEAD version too (checked with `git show HEAD:<file> | ruff check --stdin-filename <file> -`): `I001` import block unsorted (lines 1-10), `DTZ007` naive `datetime.strptime` (line 63). Left alone: a one-line PR should not reformat imports, and `models/` is outside CI's ruff scope (`pr_airflow_debugger.yaml` and `pr_spark_optimizer.yaml` lint only their own packages).
- CI gate replicated (step 6). `uv venv --python 3.11 .venv` (CPython 3.11.12) → `uv export --no-hashes --only-group models -o requirements-model-upload.txt` (137 lines) → `VIRTUAL_ENV=$PWD/.venv uv pip install -r ...` → `MNTN_SDLC_ENV=dev .venv/bin/python model_upload.py --dryrun` printed exactly two lines, `Compiling all models` and `Skipping all models upload to 'dev' env`, exit 0, no error or warning text, no `ModuleNotFoundError` (the TI-956 gotcha in `airflow_ti_workflow.md` did not fire: the `models` group in `pyproject.toml` now pins `pretty_html_table`, `matplotlib`, `sendgrid`). `git diff --quiet dags/model_task_config.json dags/ipdsc_third_party_audience_builders.json` → CLEAN (run twice). A2 confirmed: both models' `batch.runtime_config.properties` in the regenerated JSON carry exactly the 9 decorator keys (`dynamicAllocation.{min,initial,max}Executors` 50/100/300, `executor.cores` 8, `network.timeout` 600s, `rpc.askTimeout` 300s, `shuffle.io.maxRetries` 20, `shuffle.io.retryWait` 30s, `sql.shuffle.partitions` 8000), no adaptive key. `requirements-model-upload.txt` deleted and `__pycache__` dirs removed; `.venv` left in place (gitignored, `.gitignore:6`) for the dispatcher's gauntlet. `git status --short` after cleanup shows only the two modified model files.
- A3 (value syntax) settled from the pyspark 3.5.3 wheel in `.venv` instead of a live session (no Java runtime on this Mac). `spark-common-utils_2.12-3.5.3.jar` → `org/apache/spark/network/util/JavaUtils.class` (not in `spark-network-common` on 3.5) carries the suffix table `b k kb m mb g gb t tb p pb` and the message "Size must be specified as bytes (b), kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). E.g. 50b, 100k, or 250m."; `spark-catalyst_2.12-3.5.3.jar` → `org/apache/spark/sql/internal/SQLConf$.class` carries the key `spark.sql.adaptive.advisoryPartitionSizeInBytes`, the check string "advisoryPartitionSizeInBytes must be positive", the `64MB` default literal, and the `spark.sql.adaptive.coalescePartitions.parallelismFirst` doc fragment "When true, Spark does not respect the target size specified by". `16m` = 16 MiB parses. macOS `strings` chokes on `.class` files (it tries to parse them as Mach-O); a Python regex over the bytes works.
- A4 (the 800 floor is registered cores) settled from the 08-26 logs with `artifacts/audi_1274_exec_timing_probe.py` (output `outputs/audi_1274_exec_timing_2026_08_26.txt`). `spark.default.parallelism` is unset in both environments, so it equals registered cores at the moment AQE plans the stage. guid_log stage 3 (the 728-partition coalesced read) was submitted at t = 34.2 s with 90 executors = 720 cores registered; every 800-partition stage in both apps was submitted at 100 executors = 800 cores; guid_conv stage 4 (800 partitions) at 95 executors = 760 cores. Coalesce rule (Spark 3.5.3 `CoalesceShufflePartitions` + `ShufflePartitionsUtil.coalescePartitions`, general Spark knowledge, not from a workspace doc): target = max(minPartitionSize 1 MiB, min(advisory 64 MiB, ceil(total bytes / defaultParallelism))) when `parallelismFirst` is at its default true; contiguous map partitions merge greedily while the running sum stays at or under target. With 19.9 GiB over 8,000 map partitions the granule is 2.55 MiB: at 720 cores target = 28.3 MiB → 11 granules per output → ceil(8000/11) = 728; at 800 cores target = 25.5 MiB → 10 per output → 800; at 760 cores target = 26.8 MiB → still 10 per output → 800. A fixed 64 MiB advisory size cannot produce 728 and 800 for equal-size shuffles; the registered-core floor produces both exactly. The planning-wave contradiction resolves in favour of the log; the spec's "AQE coalesces to its 64 MiB advisory size" is wrong in mechanism, right in remedy.
- Post-change arithmetic (same rule, target = min(16 MiB, total / 800) = 16 MiB for every shuffle above 12.5 GiB): 17.7 GiB (2.27 MiB granules) → 7 per output → 1,143 partitions; 19.0 and 19.9 GiB → 6 → 1,334; 21.3 GiB (2.73) → 5 → 1,600; 27.8 GiB (3.56) → 4 → 2,000; 31.4 GiB (4.02) → 3 → 2,667. The pivot stage reads the 27.8 and 21.3 GiB shuffles co-partitioned in one stage (49.1 GiB), so AQE coalesces them on the summed per-index size: 6.29 MiB granules → 2 per output → 4,000 tasks at 12.6 MiB compressed each (63 MiB today). The plan's 3,100 (49.1 GiB / 16 MiB) is the floor the greedy packing can only exceed, so the step-9 pass band is 3,100 to 4,000. In-memory per task at today's 17x expansion (862.5 GiB spilled / 800 tasks = 1.08 GiB from 63 MiB compressed): about 215 MiB, against about 1.4 GiB of unified memory per core (19200m executor, 300 MiB reserved, `spark.memory.fraction` 0.6, 8 cores). The 0.06 GiB shuffle: total / 800 = 0.08 MiB, floored to 1 MiB by `minPartitionSize`, stays at about 49 to 61 partitions. The final `repartition(500, "ip")` is a fixed-count shuffle AQE never coalesces; unchanged. Task count in the save execution rises from about 5,600 to about 13,000; at Spark's per-task scheduling cost that is seconds against a 4.8 / 5.6 min job.
- Not run: `pytest tests/models` (`model-unit-test`, red repo-wide since #1209; `grep -rl` for either model id over `tests/` is empty, so no test exercises them); `model_run.py` (prod path, forbidden); any DAG trigger.

## 5. Solution
- Code: one builder line per model, `.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16m")`, in `models/feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id.py` and `guid_conv_log_pivot_ip_vertical_id.py` (line 54 in each). Uncommitted on branch `audi-1274-aqe-advisory-pivot` in the dispatcher's worktree; suggested commit subject `AUDI-1274: cap AQE coalesce target at 16m on the 2 guid pivot models`. Decorators, `dags/`, schedules untouched; `dags/model_task_config.json` regenerated by the dry run with zero diff.
- Hand-back artifacts: `artifacts/audi_1274_pr_body.md` (lint kind pr, pass), `artifacts/audi_1274_result_comment.txt` (lint kind completion, pass), `artifacts/audi_1274_exec_timing_probe.py` + `outputs/audi_1274_exec_timing_2026_08_26.txt` (the A4 evidence), `artifacts/audi_1274_aqe_probe.py` (planning wave, reused post-merge).
- Pending, dispatcher: commit, `/pr_gauntlet`, PR against `main`, merge (Ryan), ledger `applied` stamp for `disk_spill:33` / `disk_spill:34`, then the post-merge log check (§3 steps 8 to 10). Done-when lands when both ledger keys read `resolved` after three quiet sweeps.

## 6. Questions Answered
- **Q:** Does the builder `.config()` reach `dags/model_task_config.json`?
  **A:** No. The dry run regenerated the file with zero diff; only decorator `runtime_properties` are serialized (`utils_model/base_model/compute.py:99-107`).
- **Q:** Why did `spark.sql.shuffle.partitions=8000` not relieve the spill?
  **A:** With `coalescePartitions.parallelismFirst` at its default true, AQE's merge target is min(64 MiB advisory, total bytes / registered cores). At 100 executors x 8 cores that second term is 22 to 40 MiB for these shuffles, so every coalesced shuffle lands at about 800 partitions whatever the map-side count; 8000 only sets the granule AQE merges (2.3 to 4.0 MiB).
- **Q:** Is the 800 floor the advisory size or the core count?
  **A:** Core count. 728 partitions when 90 executors (720 cores) were registered, 800 at 100 executors, both reproduced exactly by the greedy merge arithmetic; a 64 MiB target cannot produce two counts for equal-size shuffles.
- **Q:** Does `16m` bind under that floor?
  **A:** Yes. min(16 MiB, total / 800) = 16 MiB for every shuffle above 12.5 GiB; all six save-side shuffles are 17.7 to 31.4 GiB.
- **Q:** Is `16m` valid syntax on Spark 3.5.3?
  **A:** Yes. The 3.5.3 byte parser accepts `b k kb m mb g gb t tb p pb`; the key's own default literal is `64MB`.
- **Q:** Why 3,100 to 4,000 pivot tasks and not one number?
  **A:** 49.1 GiB / 16 MiB = 3,100 is the floor; the merge packs whole 6.29 MiB granules (two co-partitioned shuffles summed per index), two per output, giving 4,000 at 12.6 MiB. The post-merge log settles it.

## 7. Data Documentation Updates
Nothing written to `knowledge/` by this agent (masters are off-limits in the execute wave); facts handed to the dispatcher in the structured return, for routing to `documentation/docs/airflow_ti_workflow.md` / memory `project_airflow_optimizer` / `reference_dataproc_eventlog_profiling`:
- AQE coalesce floor on the batch fleet is registered cores at plan time (`spark.default.parallelism` unset, `parallelismFirst` default true), not the 64 MiB advisory size; raising `spark.sql.shuffle.partitions` above it is a no-op for the coalesced count.
- A builder `SparkSession.builder.config()` never reaches `dags/model_task_config.json`; the dry run is diff-clean for builder-only changes, so no JSON commit is needed for them.
- `model_upload.py --dryrun` under `uv --only-group models` now completes with no extra installs (pyproject pins `pretty_html_table`, `matplotlib`, `sendgrid` in the group); the TI-956 gotcha is stale for the current lockfile.
- `JavaUtils` (byte-string parser) lives in `spark-common-utils_2.12-3.5.3.jar` on Spark 3.5, not `spark-network-common`; macOS `strings` cannot read `.class` files, use a Python regex over the bytes.
- Event-log trick: `SparkListenerExecutorAdded` count at each `SparkListenerStageSubmitted` gives registered cores at plan time; it explains any coalesced count that is a multiple of executor cores.

## 8. Open Items / Follow-ups
- Dispatcher: commit the two-file diff, run `/pr_gauntlet`, open the PR with `artifacts/audi_1274_pr_body.md`, post `artifacts/audi_1274_result_comment.txt` to Jira, stamp the ledger after merge, then the post-merge log check (§3 steps 8 to 10; pass band 3,100 to 4,000 pivot tasks, disk spill 0.0 GiB, every coalesced shuffle at about 16 MiB per partition).
- If the post-merge pivot stage still spills at 12 to 16 MiB compressed per task, the lever is executor memory / `spark.memory.fraction`, a new ticket (§3 R3).
- Pre-existing ruff findings (`I001`, `DTZ007`) in both model files left as found; fix in a fleet-wide lint pass if `models/` ever enters CI's ruff scope, not here.
- `.venv` (gitignored) remains in the worktree for the gauntlet's own dry run; delete with the worktree.

## Verification
Adversarial pass, 2026-09-02, worktree `.../scratchpad/wt/audi_1274` (read-only `git diff`/`git status`, no commits). Every claim checked below survived.

- **Diff matches §5 exactly.** `git diff` = 2 files, +1 line each, identical text `.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16m")` at line 54 in both, inserted in the builder (`self.__spark = SparkSession.builder...`), not the decorator. Decorator `runtime_properties["spark.sql.shuffle.partitions"] = "8000"` untouched at line 26 in both files. No other file changed; `git status` clean beyond the two models.
- **Config precedence correct.** New key lives in the builder `.config()` chain, never in `@compute.dataproc_batch(runtime_properties=...)` — the placement the plan required (builder wins / never reaches the decorator-serialized JSON).
- **`dags/model_task_config.json` unchanged.** Tracked, present, `git status`/`git diff` show zero delta — consistent with "regenerated by dry run, zero diff."
- **Numbers re-derived from source, not taken on faith.** Ran `artifacts/audi_1274_aqe_probe.py` directly against `outputs/app-20260826014549943-0459.zstd`: stage 33 = 800 tasks, shuffle_read 49.1 GiB, mem_spill 862.5 GiB, disk_spill 19.0 GiB — exact match to §4. Ran `artifacts/audi_1274_exec_timing_probe.py`'s underlying logic independently: confirmed 0 `SparkListenerExecutorRemoved` events in either log (100 added, 0 removed), so the cumulative-add executor count the A4 argument rests on is valid, not an overcount from ignoring removals.
- **ruff claim verified both ways.** `ruff check` on the current files and on `git show HEAD:<file>` both report the identical 2 findings (`I001`, `DTZ007`) — genuinely pre-existing, not newly introduced and not newly fixed.
- **CI-scope claim verified from the workflow files.** `.github/workflows/pr_model.yaml` (`model-upload-dryryn` + `model-unit-test`, triggered on `models/**`) runs no ruff step at all; ruff only runs in `pr_spark_optimizer.yaml` and `pr_airflow_debugger.yaml`, each scoped to its own `include/` package. `models/` is genuinely outside ruff CI scope.
- **`model-unit-test` claim verified.** `grep -rl` for either model id over `tests/` returns nothing — no test exercises these models, consistent with "not run, nothing to run."
- **Lint claims verified by direct execution**, not re-trusted from the agent: `lint_comms.py --kind completion` on `audi_1274_result_comment.txt` → 120w/681c/7 bullets, OK. `--kind pr` on `audi_1274_pr_body.md` → 127w/874c/3 bullets, OK.
- **No writes outside the two allowed surfaces.** Worktree touches only the two model files; ticket-folder writes are confined to `artifacts/`, `outputs/`, and `summary.md`.
- **Minor, non-fatal note:** §3 R1 (planning wave) says "six save-execution shuffles... 1,100 to 2,000"; §4's execute-wave post-change arithmetic (and the result comment's "five other shuffles... 1,100 to 2,700") supersedes it, correctly accounting for the two shuffles that co-partition into the single 4,000-task pivot stage rather than coalescing independently. Verified by hand: the 4 non-combined save shuffles (1,143/1,334/1,334/2,667) plus the collect execution's 728-partition shuffle recomputed under the cap (19.93 GiB / 720 cores at plan time → 16 MiB target, 2.55 MiB granules → 1,334) give exactly 5 values spanning 1,143 to 2,667 — the result comment's figure is the accurate one; R1 is stale but harmless (superseded in text, never acted on).

**Verdict: state stays `done`.** No claimed change is missing from the diff, no config landed in the decorator, no number in §4/§5/the artifacts failed independent re-derivation, no step was silently skipped without appearing in open_items, no write escaped the two allowed directories. `jira_comment` (`artifacts/audi_1274_result_comment.txt`) passes as authored, unchanged.
