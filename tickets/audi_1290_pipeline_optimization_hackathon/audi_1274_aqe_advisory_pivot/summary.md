---
doc_type: ticket
title: "AUDI-1274: Set AQE advisoryPartitionSizeInBytes=16m on the 2 pivot DAGs"
status: backlog
date: 2026-09-02
summary: "Cap AQE coalesce target at 16m on the two guid pivot DAGs where shuffle.partitions is a no-op"
result: "plan written 2026-09-02; execution pending (PLANNING wave only)"
question: "Does spark.sql.adaptive.advisoryPartitionSizeInBytes=16m stop guid_log_pivot_ip_vertical_id and guid_conv_log_pivot_ip_vertical_id from spilling after AQE coalesces their shuffle back to about 800 partitions?"
framing_state: locked
---

# AUDI-1274: Set AQE advisoryPartitionSizeInBytes=16m on the 2 pivot DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1274
**Status:** backlog
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
Written 2026-09-02 (planning wave). Every path below is absolute or relative to the airflow-ti worktree; another agent can execute without this session's context. Nothing here has been executed except the read-only verification recorded in §4.

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
8. **Hand-back to the dispatcher** (agents never commit): write `artifacts/audi_1274_pr_body.md` in this ticket folder with the PR text, lint it with `python3 .claude/scripts/lint_comms.py --kind pr --file ...`. Suggested commit subject: `AUDI-1274: cap AQE coalesce target at 16m on the 2 guid pivot models`. PR body facts to carry: the two 08-26 numbers from §4 (800 tasks, 49.1 GiB shuffle read, 862.5 GiB memory spilled, 19.0 GiB disk spilled per run), the expected after-state (about 3,100 tasks at about 280 MiB in memory each), the CI-gate result from step 6, and that no `dags/` file or DAG schedule changes. The dispatcher runs `/pr_gauntlet`, opens the PR against `main` (Ryan merges; merge to main auto-deploys the prod bundle and syncs the model files to prod GCS, memory `reference_airflow_ti`), and stamps the ledger (step 10). `model-unit-test` is red repo-wide since #1209 and is NOT a required check (UNSTABLE, not BLOCKED); do not treat it as a blocker.
9. **Post-merge verification, never a manual trigger** (memory `feedback_airflow_prod_safety`). First prod execution is the next scheduled `feature_store_setup_model` run (`dags/models/feature_store_setup_model.py:19`, `3 1 * * *` UTC, tasks at lines 246-255) after the deploy; the monthly snapshot variants (`dags/models/feature_store_snapshot.py`, `0 2 * * *`, day 2 = guid_conv, day 15 = guid pivot, lines 199-210) run the same model files and inherit the cap. The same morning (flagged apps' logs have vanished from `spark-events` within hours before, memory `project_airflow_optimizer` 2026-09-02): find the two app ids (the daily sweep's ledger row `app_id`, or `gsutil ls gs://mntn-data-archive-prod/spark-events/app-<YYYYMMDD>013*` and read `spark.app.name`), download with `gsutil -o "GSUtil:check_hashes=never" cp` into `outputs/`, and run `zstd -dc <log> | python3 artifacts/audi_1274_aqe_probe.py --stage 33 --stage 34`. Pass criteria: environment shows `spark.sql.adaptive.advisoryPartitionSizeInBytes = 16m`; every `AQEShuffleRead coalesced` node in the `save` execution reports about (its total GiB x 1024 / 16) coalesced partitions (1,100 to 2,000 per shuffle instead of 800); the pivot stage (was 33 / 34, the one with about 49 GiB shuffle read) runs about 3,100 tasks with `disk_spill` 0.0 GiB and `mem_spill` far below 862 GiB; `exec_h` on the ledger row drops from the 8.4 / 8.5 baseline. Record the before/after table in §4 and §5.
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
