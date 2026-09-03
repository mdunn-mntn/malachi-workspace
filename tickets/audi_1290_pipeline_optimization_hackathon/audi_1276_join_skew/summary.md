---
doc_type: ticket
title: "AUDI-1276: Confirm joins and fix skew on 4 DAGs"
status: in_progress
date: 2026-09-02
summary: "Join-caused skew on all 4 DAGs: a plan-time broadcast hint removes the join-key shuffle; the DS42 monitor also computes its comparison once instead of three times"
result: "Skew confirmed as join-caused on 4/4 DAGs (every join is broadcast at runtime, so AQE skewJoin cannot fire); F.broadcast / BROADCAST hints applied in 4 files plus a one-pass comparison in ipdsc_42_monitor; branch audi-1276-join-skew ready for the gauntlet and PR; no dev run (user decision)"
question: "For each of the 4 DAGs, is the skewed stage a join, and does AQE skewJoin or salting the hot key spread it?"
framing_state: locked
---

# AUDI-1276: Confirm joins and fix skew on 4 DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1276
**Status:** in_progress
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** For each of the 4 DAGs, is the skewed stage a join, and does AQE skewJoin or salting the hot key spread it?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A per-DAG verdict table in outputs/ (stage, SQL node, hot key share, current skewJoin setting, chosen fix) and one PR (branch AUDI-1276) applying the fix to every DAG where the skewed stage is a join.
- **Approach (how):** Event log stage to SQL node via include/spark_optimizer/eventlog.py; check whether spark.sql.adaptive.skewJoin.enabled is already on (Spark 3 default) and why it did not fire (broadcast join, non-sort-merge join, thresholds); salt in the model code where AQE cannot help.
- **What would change the answer:** The skewed stage is an aggregation or a window, not a join; that DAG gets the matching fix or none, recorded in §8.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

In 4 jobs one machine gets nearly all the work because a single join key dominates the data (skew); spread that key so the work parallelizes.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** a join groups rows by key. When one key holds most rows, its task runs for hours while the rest idle.

**Task:** per DAG confirm from the event log that the slow stage is a join, then enable AQE skewJoin or salt the hot key:
- [conv_log_ip_advertiser_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/conv_log_ip_advertiser_id.py)
- [guid_log_ip_guid_advertiser_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_ip_guid_advertiser_id.py)
- [guid_log_ip_advertiser_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_ip_advertiser_id.py)
- [ipdsc_42_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_42_monitor.py)

**Done-when:** PR merged; optimizer ledger shows the finding resolved (savings auto-measure).

## 3. Plan of Action
Planning wave, 2026-09-02 (Malachi's planning agent). Plan only; nothing below was executed beyond the verification reads in §3.1. Execution happens in a per-ticket git worktree of `airflow-ti` on branch `AUDI-1276`; the dispatcher commits, runs the gauntlet, opens the PR.

### 3.1 What planning verified (evidence, not the final answer)
Method check: one live prod event log per DAG was downloaded to `outputs/` and parsed with `include/spark_optimizer/eventlog.py` (read-only checkout `/Users/malachi/Developer/work/mntn/airflow-ti-main`, main @ 825b07e). Stage-to-operator mapping works two ways and both were exercised: (a) `SparkListenerStageCompleted.Stage Info.RDD Info[].Scope` names the `WholeStageCodegen (N)` block(s) a stage runs; (b) the last `SparkListenerSQLAdaptiveExecutionUpdate.sparkPlanInfo` tree for that execution lists the operators inside each codegen block (`nodeName` + `simpleString` carry join type, build side and keys). `SparkListenerJobStart.Properties['spark.sql.execution.id']` + `Stage IDs` ties stages to SQL executions. `eventlog.py` does not retain the stage-to-scope link, so the execute step needs a small ticket-local script (see step 2), not a repo change.

| DAG | Log parsed (outputs/) | Run wall | Flagged stage | Operators in the stage | Only shuffle in the job | Runtime join strategy | max task | skew x / data-skew x | largest partition share |
|---|---|---|---|---|---|---|---|---|---|
| conv_log_ip_advertiser_id | app-20260903010456361-0990 (2026-09-03 01:04Z) | 0.9 min | 13 | Sort, Window row_number over (advertiser_id, guid, order_id, order_amt, ...), WindowGroupLimit, Union, partial HashAggregate(ip, advertiser_id, dt) | `Exchange hashpartitioning(advertiser_id, 1000)` | BroadcastHashJoin (valid_advertisers inner, advertiser_verticals left), BuildRight | 16 s | 3.6 / 12.8 | 0.31 |
| guid_log_ip_advertiser_id | app-20260903010515014-0514 (01:05Z) | 1.6 min | 9 | HashAggregate partial+final (ip, advertiser_id, dt), BroadcastHashJoin x2 | `Exchange hashpartitioning(advertiser_id, 1000)` | BroadcastHashJoin x2, BuildRight | 25 s | 3.5 / 4.4 | 0.05 |
| guid_log_ip_guid_advertiser_id | app-20260903014043029-0037 (01:40Z) | 2.4 min | 9 | HashAggregate partial+final (ip, guid, advertiser_id, dt), BroadcastHashJoin x2 | `Exchange hashpartitioning(advertiser_id, 1000)` | BroadcastHashJoin x2, BuildRight | 33 s | 8.2 / 9.5 | 0.05 |
| ipdsc_42_monitor | app-20260902042001249-0056 (2026-09-02 04:20Z, the ledger's own app id) | 22.2 min | 18 (22 and 26 are the same shape) | HashAggregate(category_id, ip) distinct, HashAggregate count(distinct ip) by category_id, BroadcastHashJoin LeftSemi on deal_df, Sort, SortMergeJoin FullOuter today/yesterday | `Exchange hashpartitioning(category_id, 128)` (plan ids 555/593) | BroadcastHashJoin LeftSemi (deal_df) after AQE; SortMergeJoin FullOuter is on the tiny per-category rows | 261 s | 8.0 / 6.2 | 0.04 |

Mechanism common to all four (read off the plans above, to be confirmed across 3 or more runs each in step 2): the small side of every join is a JDBC read (`fpa.advertiser_verticals`, `public.advertisers`, `core.private_marketplace_deals`) with no size statistics, so the planner starts from a SortMergeJoin and inserts a shuffle keyed on the join column (`advertiser_id` or `category_id`) on the big side. AQE turns the join into a broadcast join at runtime, but the shuffle has already run. The downstream aggregate or window needs a clustered distribution on a superset of that key, `hashpartitioning(advertiser_id)` satisfies it, so no second shuffle is planned and the aggregate/window runs on key-skewed partitions (no Exchange between the partial and final HashAggregate in any of the four plans). This is join-caused skew, but `spark.sql.adaptive.skewJoin` cannot fire: it only rewrites sort-merge and shuffled-hash joins, and every join here is a broadcast join at runtime.

Config facts (from `SparkListenerEnvironmentUpdate` in the same logs): Spark 3.5.3 (Dataproc Serverless runtime 2.3, `runtime_version="2.3"` in `utils_model/base_model/compute.py:59`). `spark.sql.adaptive.enabled=true` in all four; `spark.sql.adaptive.skewJoin.enabled` unset in all four, so it is at the Spark 3.5 default `true` (factor 5, 256 MB threshold). Nothing in the four model files or `dags/model_task_config.json` sets skewJoin for these jobs (it is set only for fangorn_* and segment_quality_scoring). `spark.sql.autoBroadcastJoinThreshold` is 192m on the feature-store batches and 163m on the monitor. `spark.sql.shuffle.partitions` 1000 (feature store) / 128 (monitor). Ticket premise "enable AQE skewJoin" is therefore moot; the lever is removing the planted shuffle.

Magnitude today (changes the leverage, not the framing): the three feature-store jobs run 0.9 to 2.4 min on 10 executors and their slowest task is 16 to 33 s, under the optimizer's 60 s skew floor (`SKEW_MIN_TASK_MS` in `include/spark_optimizer/optimizations.py:24`), which is why the live ledger (`outputs/prod_optimization_ledger_2026_09_02.jsonl`, 1,352 rows, pulled from `gs://mntn-data-archive-prod/optimizer/`) holds no row for any of them. The 08-27 corpus numbers (`audi_1194_validation_jobs.csv`: 19.4 / 9.2 / 6.5 exec-h over 22 runs) put them at well under 1 exec-h per run. `ipdsc_42_monitor` is the only material one: ledger keys `skew:18`, `skew:22`, `skew:26` chronic (streak 7 on 2026-09-02) plus `shuffle_fetch_wait:18/22/26` (73 to 82 percent of task time), 1.0 to 3.1 exec-h per run, and the three stages each read the same 40.1 GB shuffle inside the single `count()` execution (execution id 5, jobs 12/13/14): the SQL's `comparison` CTE is evaluated more than once (`all_rows` unions it with `total_row`, which aggregates it again) and Spark does not materialise CTEs.

Prior art that already settles things: AUDI-1194 08-27 sweep rows 32/34/51/52 (`audi_1194_hackathon_optimizations_2026_08_27.md`) are the source finding and are VERIFY-FIRST; `reference_dataproc_eventlog_profiling` memory: skew needs the data cross-check (done: data-skew 4.4x to 12.8x, so not a straggler); `feedback_airflow_prod_safety`: dev first, first prod run is the next cron; `reference_airflow_ti` line 151: the user cannot submit a dev batch from `model_run.py` (no `iam.serviceAccounts.actAs` on the dev SA), the dev Astro deployment can; `reference_airflow_ti_dev_testing`: dev bundle re-syncs from main hourly, assert `dag_versions[].bundle_version` on every run, neutralise fan-out by marking tasks success then clearing the ones wanted; `project_airflow_optimizer`: flagged apps' logs vanish from `spark-events` within hours (the 09-02 monitor log was still present at 20:04 PT, the three feature-store logs were pulled 8 h after they landed; all four are now preserved in `outputs/`).

### 3.2 Steps (execute agent; each names the file it touches and how it is validated)
1. Refresh evidence (no repo edits). For each DAG pull the newest 3 finished logs: list `gs://mntn-data-archive-prod/spark-events/` for `app-<YYYYMMDD>01*` (feature_store_setup_model fires 01:03Z) and `app-<YYYYMMDD>04*` (ipdsc_monitor 00:05Z with 18 h sensors, lands ~04:20Z), identify by app name with `gsutil cat -r 0-65535 <obj> | zstd -d -c | grep -o '"App Name":"[^"]*"'` (streaming decode of a truncated frame prints the header before erroring), then `gsutil -o "GSUtil:check_hashes=never" cp` into `outputs/` (never `gcloud storage cp`, it zeroes .zstd). Do this first thing: the logs vanish within hours. Delete anything over 200 MB after parsing (today's four are 0.3 to 0.7 MB).
2. Write `outputs/audi_1276_stage_map.py` (ticket-local, imports `spark_optimizer.eventlog._read_events` with `sys.path.insert(0, '<airflow-ti-main>/include')`). Per log: stage id -> codegen scopes (RDD Info Scope), execution id -> final `sparkPlanInfo` (last `SQLAdaptiveExecutionUpdate`), operators + `simpleString` per codegen block, per-stage `task_read_bytes` max share, max task seconds, skew and data-skew ratios, and `spark_props` for `spark.sql.adaptive.*`, `autoBroadcastJoinThreshold`, `shuffle.partitions`. Emit one row per (DAG, run, flagged stage) to `outputs/audi_1276_verdict_table.csv` with columns: dag, app_id, run_date, stage, sql_operators, shuffle_key, runtime_join_strategy, max_task_s, skew_x, data_skew_x, hot_partition_share, skewjoin_setting (unset = default true), why_skewjoin_did_not_fire, chosen_fix, ledger_key. Ranked descending by hot_partition_share. This CSV is the Objective's verdict table.
3. Optional hot-key confirmation for conv_log only (the 0.31 share is large enough to name the advertiser): `bq_run.sh --dry_run` then run `SELECT advertiser_id, COUNT(*) c FROM \`dw-main-bronze.raw.conversion_log\` WHERE DATE(time) = '<run_date>' GROUP BY 1 ORDER BY c DESC LIMIT 20` (one day, bronze retention ~9 months, abort if the dry run exceeds 5 GB). guid_log in BQ is 366 TB partitioned by day, so for the guid jobs use the event-log partition share only; ipdsc ds42 has no BQ table, event log only. Record the top-key share next to the partition share in the CSV.
4. Fix, feature-store models (three files, same two-line change each, no decorator change so `dags/model_task_config.json` stays untouched):
   - `models/feature_store/feature_group_1_source/conv_log_ip_advertiser_id.py`: `.join(valid_advertisers_df, "advertiser_id", "inner")` -> `.join(F.broadcast(valid_advertisers_df), "advertiser_id", "inner")`; `.join(advertiser_verticals_df, on="advertiser_id", how="left")` -> `.join(F.broadcast(advertiser_verticals_df), on="advertiser_id", how="left")`.
   - `models/feature_store/feature_group_1_source/guid_log_ip_guid_advertiser_id.py`: same two joins.
   - `models/feature_store/feature_group_1_source/guid_log_ip_advertiser_id.py`: same two joins.
   Expected effect: no `Exchange hashpartitioning(advertiser_id)`; the aggregate shuffles on its full key (ip, advertiser_id[, guid], dt) which is uniform; the conv_log windows shuffle on their full partition tuple. Both small sides are already broadcast at runtime today, so the hint only moves the decision to plan time. No comments in the diff; the why goes in the PR body.
5. Fix, monitor: `models/monitoring/ipdsc_42_monitor.py`.
   - In `IPDSC42_COMPARE_SQL`, `today_by_category` and `yesterday_by_category`: `SELECT /*+ BROADCAST(dd) */ t.category_id, COUNT(DISTINCT t.ip) ...` (same for `y`). Removes the `category_id` shuffle; the distinct then partitions on (category_id, ip).
   - Compute the per-category comparison once: split the SQL so `comparison` is produced by a first `spark.sql(...)`, `.persist()` + `.count()` it, register it as a temp view, and run `total_row`/`all_rows`/final projection in a second `spark.sql(...)` over that view. Leaves one 40 GB-class shuffle read per run instead of three (ledger keys skew:22 and skew:26 resolve by disappearing). Keep `out.cache()` semantics for the downstream `toPandas`/`save` (they already hit `InMemoryTableScan`, verified).
   - `spark.sql.maxPlanStringLength`, email, and write paths untouched.
6. Fallback only if step 8 shows the hint did not remove the shuffle: set `"spark.sql.requireAllClusterKeysForDistribution": "true"` in each model's `@compute.dataproc_batch(runtime_properties=...)` (forces the aggregate to shuffle on all its keys), then regenerate `dags/model_task_config.json` with `MNTN_SDLC_ENV=dev python model_upload.py --dryrun` under the uv `models` group and include the regenerated JSON in the branch (the `model-upload-dryrun` CI check fails otherwise). Prefer the hint; the config flag is job-wide.
7. Local checks before any dev run: `ruff` as CI runs it (ruff pinned 0.16, see `reference_airflow_ti`); `MNTN_SDLC_ENV=dev python model_upload.py --dryrun` must produce no diff to `dags/model_task_config.json` for steps 4-5; `python -c "import ast; ast.parse(open(f).read())"` on the four files; `lint_comments.py --staged` equivalent: no new comment lines.
8. Dev validation (dispatcher does the git part). Push the branch content onto `dev` (auto-deploys the dev bundle via Astro git integration and copies model files to `gs://mntn-data-archive-dev/ti_resources_v2/dev/`). Within the same hour: trigger `feature_store_setup_model` on the dev deployment (`cmcvcbd3j03vk01p91ksvm1vd`) with a fresh `logical_date`, mark every task success except `feature_group_1_source.guid_log_ip_advertiser_id_rollup`, `guid_log_ip_guid_advertiser_id`, `conv_log_ip_advertiser_id`, clear those three; trigger `ipdsc_monitor`, mark `precondition_ds42/49/65` success (dev ipdsc holds only ds 4 and 43; the model reads the prod archive regardless of ENV), let `monitor_ipdsc_42` run. Assert `dag_versions[].bundle_version` on each run. Models read prod inputs and write dev buckets (`location_root_dev`), cost is 10 executors x 1-3 min each plus one 22 min monitor run.
9. Read the dev event logs from `gs://mntn-data-archive-dev/spark-events/` (the dev SA writes there, verified 2026-08-04), re-run step 2's script on them, and accept when: no `Exchange hashpartitioning(advertiser_id|category_id)` in the final plan, flagged-stage data-skew under 2x and largest partition share under 0.05, output row counts equal the prod run for the same date (compare `gs://mntn-data-archive-dev/feature_store/feature_group_1_source/<model>/dt=<D>/` vs prod, and the monitor's compare table row count), monitor run wall time and total shuffle read bytes down (target: one 40 GB-class read).
10. Hand-off: update this summary §4-§6 and the CSV with the dev-run rows, then the dispatcher runs the PR gauntlet and opens PR `AUDI-1276` on `SteelHouse/airflow-ti` (PR body: mechanism above, before/after numbers, dev run ids and bundle version). Reviewer/owner: Ryan Kleck (`rkleck-mntn`, owns `guid_log_ip_advertiser_id.py` and `ipdsc_42_monitor.py`, feature-store pipeline); FYI Alex Knorr (`conv_log_ip_advertiser_id.py`) and syang413 (`guid_log_ip_guid_advertiser_id.py`). First prod run is the next scheduled cron after merge; no manual prod trigger.
11. After merge (Malachi's daily ledger reconcile, not the execute agent): stamp provenance for the only DAG with ledger history, `python -m include.spark_optimizer.ledger applied ipdsc_42_monitor skew:18 <PR#> <merge-date>` and the same for `skew:22`, `skew:26` (the `shuffle_fetch_wait:18/22/26` keys resolve on their own when the reads disappear). `mark_applied` raises "no ledger history" for the three feature-store DAGs; record them in §8 as fixed-but-unmeasurable-by-ledger and cite the dev/prod event-log before/after instead.

### 3.3 Sources
- Jira AUDI-1276 (Task, Backlog, parent AUDI-1290, labels hackathon + q3_2026, no points, 0 comments as of 2026-09-02).
- Spec: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` item 21; `audi_1194_hackathon_optimizations_2026_08_27.md` rows 32, 34, 51, 52 (and row 29: ipdsc_42_monitor shuffle_fetch_wait, which AUDI-1272 also lists); `audi_1194_validation_jobs.csv`.
- Ledger: `outputs/prod_optimization_ledger_2026_09_02.jsonl` (copy of `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`); `optimizer_backlog_2026-09-02.md` and `optimizer_coverage_2026-09-02.md` in the same prefix (coverage lists `guid_log_ip_advertiser_id` as unresolvable: task id is `guid_log_ip_advertiser_id_rollup`).
- Code (read-only main): the four model files; `dags/models/feature_store_setup_model.py` (01:03Z daily, task ids above); `dags/monitoring/ipdsc_monitor.py` (00:05Z, `monitor_ipdsc_42`, sensors on ds42/49/65 `_SUCCESS`); `dags/model_task_config.json` entries for the four models (runtime 2.3, no AQE keys on the feature-store three); `include/spark_optimizer/{eventlog,optimizations,ledger,fetch}.py`; `dags/spark_optimizer_daily.py` (`LOG_CAP = 200`, 09:00Z).
- Event logs in `outputs/`: the four `.zstd` files named in the table.
- Memory: `feedback_airflow_prod_safety`, `reference_airflow_ti` (dev deploy mechanics, event-log bucket, `.zstd` download gotcha), `reference_airflow_ti_dev_testing`, `reference_dataproc_eventlog_profiling`, `project_airflow_optimizer`, `feedback_dataproc_cost_awareness`.
- Inputs: `gs://mntn-data-archive-prod/conversion_log/dt=2026-09-01/` 3.9 GB, `guid_log/dt=2026-09-01/` 349 GB, `ipdsc/dt=2026-09-01/data_source_id=42/` three ~181 MB parquet files (`gsutil du -s` reports 0 on this prefix; use `ls -l`).

### 3.4 Assumptions to resolve empirically first
- A1: the shuffle-on-join-key mechanism holds on 3 or more runs per DAG, not just the one log each parsed here (step 1-2).
- A2: `F.broadcast()` on a JDBC-sourced DataFrame and the SQL `BROADCAST` hint remove the planted Exchange on Spark 3.5.3 (step 9 checks the dev plan; fallback step 6).
- A3: the dev deployment can run the three feature-store tasks and the monitor against prod inputs within one bundle-sync hour; dev SA still writes `gs://mntn-data-archive-dev/spark-events/`.
- A4: the monitor's triple 40 GB read comes from the `comparison` CTE being re-evaluated (jobs 12/13/14 of execution 5), not from cache misses on `toPandas`/`save` (those already show `InMemoryTableScan`); confirm by counting Exchange reads per job in the dev log after step 5.
- A5: ipdsc/tpa PHS logs were not needed: all four jobs run through the batch operator and write to `spark-events`; no PAM grant was requested or used in planning.

### 3.5 Risks
- Leverage: three of the four DAGs are 1-2 min jobs whose skewed task is 16-33 s; the fix is correct but the saving is minutes per day. `ipdsc_42_monitor` carries the measurable win (2 of 3 40 GB reads removed plus the skew).
- Done-when as filed ("ledger shows the finding resolved") is only satisfiable for `ipdsc_42_monitor`; the other three never entered the live ledger.
- Dev run of `ipdsc_42_monitor` sends the DS42 monitor email (subject suffixed `DEV`) to `targeting-infrastructure@`, `machine-learning-squad@` and two Slack mail-in channels; nothing in the model gates it by env.
- The dev bundle re-syncs from `main` hourly and silently reverts the branch; a run created after the sync tests `main`. Assert the bundle version on every run.
- `SortMergeJoin FullOuter` on `category_id` between today/yesterday stays a sort-merge join (tiny inputs); AQE may broadcast it. Not a skew source.
- Overlap: AUDI-1272 (initialExecutors verify-first) also names `ipdsc_42_monitor` for the same stages' fetch-wait; if this ticket lands first, that item should be re-verified rather than applied.
- Broadcasting `advertiser_verticals` (type 1) and `advertisers` (pixel_isolation false) is what AQE already does today at 192m threshold; if either table ever grows past the driver's comfort the hint forces it anyway. Both are tens of thousands of rows now.

### 3.6 Decisions for the user (not taken by the plan)
- D1: ship all four as filed (1 SP), or narrow the PR to `ipdsc_42_monitor` and record the three feature-store DAGs in §8 as "join-caused skew confirmed, below the 60 s floor, hint applied or not". Plan recommends shipping all four: the feature-store change is two `F.broadcast` calls per file, dev-validated in the same run.
- D2: accept one `DEV`-subject monitor email/Slack post from the dev validation run, or skip the monitor's dev run and rely on the feature-store dev runs proving the mechanism (the monitor change is the same hint plus a CTE materialisation).
- D3: dev validation requires pushing the branch content onto `dev` (a shared branch, auto-deploys); confirm the dispatcher may do that for this ticket.

### 3.7 Executed as (execute agent, 2026-09-02, resumed after a session cut-off)
Steps 1-5 and 7 ran as planned; 6 was not needed; 8-9 were replaced by a local check on the user's instruction; 10-11 stay with the dispatcher and Malachi.
- Step 1: 15 prod logs pulled (4 per feature-store DAG for 2026-08-31 to 2026-09-03, 3 monitor runs 2026-08-30, 08-31, 09-02), all 0.29 to 0.71 MB, kept in `outputs/`.
- Step 2: `outputs/audi_1276_stage_map.py` written and run over all 15; per-log `outputs/<app>.stagemap.txt`, every stage in `outputs/audi_1276_stage_rows.csv`, the flagged stages in `outputs/audi_1276_verdict_table.csv` (21 rows). A `top_key` / `top_key_share` column pair was added after step 3.
- Step 3: one-day BigQuery count on `dw-main-bronze.raw.conversion_log` (0.115 GB), `outputs/audi_1276_conv_log_top_advertisers_2026_09_02.csv`.
- Step 4: the three `F.broadcast()` edits, exactly as written. Step 5: the monitor SQL split; `.cache()` was used instead of `.persist()` (same default storage level for DataFrames).
- Step 6 (fallback config flag): not applied. The local plan check (below) shows the hint alone removes the planted shuffle.
- Step 7: ruff 0.16.1, `ast.parse`, `MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun`, comment-line check, all clean (§4.6).
- Steps 8-9 (dev deploy + dev event logs): NOT run. User decisions D2 (skip the monitor's dev run) and D3 (no push to the shared dev branch, no `model_run.py`). Replaced by `outputs/audi_1276_local_plan_check.py`, a local Spark 3.5.3 run on synthetic data that reproduces the plan-time decision path and checks the rewritten SQL for row equality (§4.5). The first real validation is the first prod cron after merge (§8).
- Step 10: this summary plus `artifacts/audi_1276_pr_body.md` and `artifacts/audi_1276_result_comment.txt` (both linted). D1: all four DAGs ship in one PR.
- Savings per DAG estimated from the event logs with `outputs/audi_1276_savings_estimate.py` -> `outputs/audi_1276_savings_estimate.csv` (§5.2).

## 4. Investigation & Findings

### 4.1 Evidence refresh: the mechanism holds on every run (A1 confirmed)
15 logs, 21 flagged-stage rows (`outputs/audi_1276_verdict_table.csv`, ranked by `hot_partition_share`). In every run of every DAG: the flagged stage is the first stage downstream of `Exchange hashpartitioning(advertiser_id, 1000)` (feature store) or `Exchange hashpartitioning(category_id, 128)` (monitor, plan ids 555 and 593); every join in the final adaptive plan is a `BroadcastHashJoin` (BuildRight on the JDBC side); `spark.sql.adaptive.enabled=true`; `spark.sql.adaptive.skewJoin.enabled` unset (Spark 3.5.3 default true); `spark.sql.autoBroadcastJoinThreshold` 192m (feature store) / 163m (monitor); no Exchange between the partial and final aggregate or under the window.

| DAG | Runs | Flagged stage | max task s | median task s | skew x | data-skew x | hot partition share | stage read GB |
|---|---|---|---|---|---|---|---|---|
| conv_log_ip_advertiser_id | 4 (08-31, 09-01, 09-02, 09-03) | 13 (Sort, Window row_number over the dedup identity tuple, WindowGroupLimit, Union, partial HashAggregate(ip, advertiser_id, dt)) | 16.4 to 21.4 | 4.5 to 6.0 | 2.7 to 3.7 | 12.7 to 15.3 | 0.309 to 0.366 | 0.37 to 0.47 |
| guid_log_ip_advertiser_id | 4 | 9 (partial+final HashAggregate(ip, advertiser_id, dt), both BroadcastHashJoins) | 15.0 to 37.1 | 6.6 to 9.3 | 1.8 to 4.0 | 3.3 to 4.5 | 0.044 to 0.049 | 4.6 to 5.6 |
| guid_log_ip_guid_advertiser_id | 4 | 9 (partial+final HashAggregate(ip, guid, advertiser_id, dt), both BroadcastHashJoins) | 19.1 to 32.6 | 4.0 to 4.4 | 4.3 to 8.2 | 5.9 to 9.5 | 0.033 to 0.045 | 10.4 to 12.6 |
| ipdsc_42_monitor | 3 (08-30, 08-31, 09-02) | 18, 22, 26 (each: HashAggregate(category_id, ip) distinct, count(distinct ip) by category_id, BroadcastHashJoin LeftSemi on deal_df, Sort, SortMergeJoin FullOuter today/yesterday) | 146 to 269 | 16.4 to 48.3 | 5.6 to 8.9 | 6.2 | 0.041 | 39.7 to 40.1 each |

Monitor fetch-wait: 29.5 to 81.6 percent of task time on the three stages (the `shuffle_fetch_wait:18/22/26` ledger keys); 0.0 to 0.3 percent on the feature-store stages.

Executor picture (from executor ids seen in the flagged stage plus `spark.executor.cores`): feature-store batches 10 executors x 4 cores = 40 slots; monitor 4 to 7 executors x 4 cores (dynamic allocation from `spark.executor.instances=2`, `maxExecutors=1000`).

### 4.2 Hot key named (step 3)
`SELECT advertiser_id, COUNT(*) c, ROUND(COUNT(*)/SUM(COUNT(*)) OVER (),4) share FROM dw-main-bronze.raw.conversion_log WHERE DATE(time) = "2026-09-02" GROUP BY 1 ORDER BY c DESC LIMIT 20` (0.115 GB, 1.2 s, 7,724,509 rows read). Advertiser 36206 = 2,285,605 rows = 29.6 percent; 66701 = 2,109,880 = 27.3 percent; third place 42999 = 3.9 percent. Two advertisers hold 56.9 percent of the day's conversion log. The run that processed that day (`app-20260903010456361-0990`, `dt=2026-09-02`) has a 0.311 hot-partition share on stage 13: the partition holding 36206 plus whatever else hashes into the same bucket of 1000. Recorded in the verdict table as `top_key` / `top_key_share`. The guid jobs' 0.045 share is a 4 to 10x heavy bucket, not a single dominant advertiser; not queried (guid_log in BQ is 366 TB). ipdsc ds42 has no BQ table.

### 4.3 Mechanism (confirmed on 15 runs, reproduced locally)
1. The small side of every join is a JDBC read (`public.advertisers`, `fpa.advertiser_verticals`, `core.private_marketplace_deals`). A JDBC relation carries no size statistics, so the planner treats it as huge and plans a SortMergeJoin, inserting `Exchange hashpartitioning(<join key>)` on the big side.
2. AQE converts the join to a BroadcastHashJoin at runtime (the JDBC side is tens of thousands of rows, well under 192m/163m), but the shuffle has already been planned and executed.
3. The next operator (groupBy on (ip, advertiser_id[, guid], dt), the dedup window on (advertiser_id, guid, order_id, order_amt, ...), or the count-distinct on (category_id, ip)) requires a clustered distribution on a superset of the join key; `hashpartitioning(<join key>)` satisfies it, so no second Exchange is planned and the operator runs on key-skewed partitions.
4. `spark.sql.adaptive.skewJoin` is on by default but only rewrites SortMergeJoin and ShuffledHashJoin; every join here is a BroadcastHashJoin by the time AQE looks, so it never fires. The ticket's "enable AQE skewJoin" premise is moot; salting is unnecessary because the shuffle itself goes away.
5. A plan-time broadcast (`F.broadcast()` on the DataFrame, `/*+ BROADCAST(dd) */` in SQL) makes the planner choose BroadcastHashJoin up front, so no join-key Exchange exists; the aggregate or window then plants its own Exchange on its full key, which is uniform (for the monitor the distinct's key is (category_id, ip); the count(distinct) second phase re-shuffles on category_id but only carries per-partition partial counts).

### 4.4 Monitor: the comparison CTE is evaluated three times (A4 confirmed)
Final plan of execution 5 (`out.count()` in `analyze_ipdsc_changes`) in `outputs/app-20260902042001249-0056.stagemap.txt`: `Exchange hashpartitioning(category_id, 128) [plan_id=555]` and `[plan_id=593]` each appear three times, once per use of `comparison` (the `all_rows` union, the `total_row` sum, and the final projection's ORDER BY input). Spark reuses the shuffle write (stages 8 and 10 write 20.2 + 19.9 GB once) but every consumer re-reads all 40.1 GB and re-runs the count-distinct: stages 18 (job 12), 22 (job 13), 26 (job 14), sequential, windows 359 to 704 s, 705 to 1018 s, 1018 to 1323 s of a 1331 s run (72 percent of the run wall; the first 359 s are the two parquet scans and the JDBC read). `toPandas` (execution 6) and `save` (execution 7) already read `InMemoryTableScan` from `out.cache()`, so they add nothing.

### 4.5 Local Spark 3.5.3 check (stands in for the dev run, steps 8-9)
`outputs/audi_1276_local_plan_check.py` (worktree `.venv` PySpark 3.5.3, Homebrew openjdk 17.0.20), output `outputs/audi_1276_local_plan_check.txt`. The stats-less JDBC relation is emulated with `spark.sql.autoBroadcastJoinThreshold=-1` at plan time and `spark.sql.adaptive.autoBroadcastJoinThreshold=10 MB` at runtime, which is the same decision path the prod logs show (SortMergeJoin planned, broadcast at runtime). Synthetic inputs: 20,000 log rows with one advertiser holding 30 percent (mirroring 36206), 9 valid advertisers, verticals per advertiser; monitor inputs of 7 today rows / 5 yesterday rows / 5 deals covering "today only", "yesterday only", "both", "deal with no data", "category that is not a deal", empty array, null array, duplicate ips.
- guid_log shape, as on main: initial plan = SortMergeJoin with `Exchange hashpartitioning(advertiser_id)` on the log side and NO Exchange of its own under `HashAggregate(ip, advertiser_id, dt)`. With `F.broadcast()`: both joins are BroadcastHashJoin at plan time, the only `hashpartitioning(advertiser_id)` exchanges sit under the small sides' `dropDuplicates`, and the aggregate gets `Exchange hashpartitioning(ip, advertiser_id, dt)`. Same 2,246 output rows either way.
- conv_log shape (window over (advertiser_id, guid, order_id, order_amt) then the rollup), as on main: SortMergeJoin plus `Exchange hashpartitioning(advertiser_id)` on the log side, window with no Exchange of its own. With `F.broadcast()`: the window shuffles on its full partition tuple, the rollup on (ip, advertiser_id, dt), no advertiser_id shuffle on the log side. Same 2,246 rows. Caveat: in the local "as on main" final plan AQE broadcast the log side (BuildLeft) because the synthetic log is tiny, so the runtime picture for main comes from the prod logs, not from this emulation; the emulation is evidence for the plan-time decision only.
- Monitor, main SQL vs branch (three input cases): identical row multisets, TOTAL row first in both, e.g. category 1 today-only (2, 0, 2, 0.0), category 2 (3, 2, 1, 0.5), category 4 yesterday-only (0, 2, -2, -1.0), TOTAL (7, 6, 1, 0.1667); empty-today gives all-zero today values and pct_change -1.0 in both. Branch `IPDSC42_BY_CATEGORY_SQL` plan: BroadcastHashJoin at plan time, `Exchange hashpartitioning(category_id, ip)` under the distinct with a pre-shuffle `HashAggregate(keys=[category_id, ip])`, then `Exchange hashpartitioning(category_id)` carrying only `partial_count(distinct ip)` rows; the FullOuter SortMergeJoin on the per-category rows remains (tiny). Branch `IPDSC42_COMPARE_SQL` final plan: the `all_rows` union and the `total_row` sum both read `InMemoryTableScan` of the cached per-category frame; no `Generate explode` outside the cached relation.

### 4.6 Local checks (step 7)
- ruff 0.16.1 on the four files: 14 findings, the identical (file, code) set to `main` (`outputs/audi_1276_ruff_baseline_main.txt` vs `outputs/audi_1276_ruff_branch.txt`); none introduced. CI's ruff covers only `include/` packages, so `models/` findings are pre-existing and out of scope.
- `ast.parse` clean on all four; `git diff` adds no comment line (the two `/*+ BROADCAST(dd) */` are SQL hints).
- `MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun`: "Compiling all models ... Skipping all models upload to 'dev' env", exit 0, `dags/model_task_config.json` unchanged (no decorator change in the diff). First run synced 295 packages into the worktree `.venv`, 23 s total.
- Diff: 4 files, +30 / -25.

### 4.7 Dead ends and gotchas
- `/usr/libexec/java_home` reports no JDK on this Mac although Homebrew has `openjdk@17` (unlinked); `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` works.
- Local PySpark first failed with `PYTHON_VERSION_MISMATCH` (worker `/usr/bin/python3` 3.9 vs driver 3.11): set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the venv interpreter.
- In zsh `echo =====` errors ("==== not found"): a leading `=` is command-path expansion; quote it.
- The by-category cached plan prints its own `== Initial Plan ==` marker inside the outer plan text, so any awk that stops at the first marker truncates the outer plan; count `InMemoryTableScan` over the whole section.
- The optimizer ledger cannot show the feature-store fix landing: their slowest task is 16 to 37 s, under the 60 s floor (`SKEW_MIN_TASK_MS`), so they never entered the ledger and `mark_applied` will raise "no ledger history".

## 5. Solution
**PR:** https://github.com/SteelHouse/airflow-ti/pull/1276 (opened 2026-09-03 PT; fast tier re-run after an infrastructure error, 0 findings; reviewer Ryan Kleck)


### 5.1 Code changes (branch `audi-1276-join-skew`, worktree `scratchpad/wt/audi_1276`, uncommitted; the dispatcher commits, gauntlets and opens the PR)
- `models/feature_store/feature_group_1_source/conv_log_ip_advertiser_id.py`, `guid_log_ip_advertiser_id.py`, `guid_log_ip_guid_advertiser_id.py`: `.join(valid_advertisers_df, "advertiser_id", "inner")` -> `.join(F.broadcast(valid_advertisers_df), ...)` and `.join(advertiser_verticals_df, on="advertiser_id", how="left")` -> `.join(F.broadcast(advertiser_verticals_df), ...)`. Two lines per file, nothing else.
- `models/monitoring/ipdsc_42_monitor.py`: `IPDSC42_COMPARE_SQL` split into `IPDSC42_BY_CATEGORY_SQL` (today/yesterday explode, `/*+ BROADCAST(dd) */` on both deal joins, count-distinct by category, FULL OUTER JOIN into the per-category comparison rows) and `IPDSC42_COMPARE_SQL` (`total_row`, `all_rows`, deal-name projection and ORDER BY over a temp view named `comparison`). `analyze_ipdsc_changes` runs the first SQL, `.cache()`s it, logs `comparison.count()` ("DS42 categories compared"), registers the view, then runs the second SQL with the existing `out.cache()` and row-count log. Email, HTML, write path, `spark.sql.maxPlanStringLength` untouched. Decorator untouched, so `dags/model_task_config.json` is unchanged and no bundle redeploy is needed for the change to take effect (model `.py` files are read live from GCS on the next run after merge).
- PR body: `artifacts/audi_1276_pr_body.md` (lint `--kind pr` pass). It states that dev-deployment validation was not run and why.

### 5.2 Expected savings (from `outputs/audi_1276_savings_estimate.csv`; balanced wall = sum of task time / slots)
| DAG | Flagged stage wall today | Balanced wall | Saving per run | Per day (1 run/day) | Executor-hours per day |
|---|---|---|---|---|---|
| conv_log_ip_advertiser_id | 16.4 to 21.5 s | 2.8 to 4.0 s | 12.5 to 17.5 s (mean 14.3 s) | 0.24 min | 0.04 (10 executors) |
| guid_log_ip_advertiser_id | 23.3 to 37.1 s | 15.7 to 23.3 s | 7.6 to 13.8 s (mean 9.9 s) | 0.17 min | 0.03 |
| guid_log_ip_guid_advertiser_id | 31.0 to 39.7 s | 21.9 to 24.2 s | 9.1 to 15.5 s (mean 12.1 s) | 0.20 min | 0.03 |
| ipdsc_42_monitor | stages 22 + 26: 346 to 618 s per run (42 to 46 percent of a 13.7 to 22.2 min run); stage 18: 179 to 345 s | stages 22 + 26 removed; stage 18 balanced 155 to 217 s | 370 to 746 s (6.2 to 12.4 min) | 6 to 12 min | 0.5 to 1.0 (sum of task time in stages 22 + 26 / 4 cores: 0.46, 0.74, 1.03 on 08-31, 08-30, 09-02) plus the stage-18 balance |

The three feature-store DAGs together: about 0.6 min of wall and 0.1 executor-hours per day. That is the honest number; the fix is correct and cheap but the leverage is in the monitor. Per-run task-level rows for every flagged stage are in the CSV (`sum_task_s`, `max_task_s`, `median_task_s`, `balanced_wall_s`, `saving_wall_s`, `fetch_wait_s`, stage windows).

Not claimed: with the hint, the remaining monitor pass pre-aggregates (category_id, ip) before its shuffle, so the shuffle write may shrink below 40 GB; and the `shuffle_fetch_wait:18/22/26` ledger keys resolve on their own when the two reads disappear.

### 5.3 What was not done
- No dev deployment run, no `model_run.py`, no DAG trigger (D2, D3). No Jira write, no Slack, no git write by this agent.
- No `knowledge/` edit (off-limits to this agent): facts handed to the dispatcher (§7).

## 6. Questions Answered
- **Q:** For each DAG, is the skewed stage a join?
  **A:** No stage executes a skewed join: every join is a BroadcastHashJoin at runtime in all 15 runs. The skewed stage is the aggregate (guid jobs, monitor) or the dedup window (conv_log) running on the join's plan-time shuffle, which is keyed on `advertiser_id` / `category_id` because the JDBC side has no statistics. The skew is join-caused, not join-executed.
- **Q:** Does AQE skewJoin spread it?
  **A:** No. It is already enabled (default true, unset in all four jobs) and only rewrites sort-merge and shuffled-hash joins, so it can never act on these broadcast joins.
- **Q:** Salt the hot key?
  **A:** Not needed. A plan-time broadcast hint removes the keyed shuffle; the aggregate/window then shuffles on its full key, which is uniform (verified in the local plan check).
- **Q:** Why does `ipdsc_42_monitor` read 40 GB three times?
  **A:** Spark does not materialise CTEs; `comparison` is referenced by `all_rows`, `total_row` and the final projection, so its count-distinct runs three times (shuffle write reused, read and aggregate repeated). Cache + count + temp view makes it one pass.
- **Q:** Which advertiser is the hot key in conversion_log?
  **A:** 36206 (29.6 percent of 2026-09-02 rows), with 66701 close behind (27.3 percent).

## 7. Data Documentation Updates
None written here (`knowledge/` is off-limits to the execute agent). Handed to the dispatcher for routing:
- Spark gotcha: a JDBC-sourced small table is planned as a SortMergeJoin (no stats), AQE broadcasts it at runtime, but the planted `Exchange hashpartitioning(<join key>)` on the big side stays and any downstream aggregate/window on a superset key inherits the key skew; AQE skewJoin cannot fire on broadcast joins. Fix: `F.broadcast()` / `/*+ BROADCAST(alias) */` at plan time (verified Spark 3.5.3).
- Spark gotcha: CTEs are not materialised; N references = N evaluations (shuffle write reused, read + aggregate repeated). Cache + count + temp view.
- Event-log stage-to-operator mapping recipe and script (`outputs/audi_1276_stage_map.py`).
- conversion_log 2026-09-02: advertisers 36206 and 66701 = 57 percent of rows.
- Local PySpark on this Mac: `JAVA_HOME` Homebrew openjdk@17 path, `PYSPARK_PYTHON` to the venv.
- `model_upload.py --dryrun` under `uv run --group models`: 23 s, syncs 295 packages on first run.
- Feature-store batches run 10 executors x 4 cores; the monitor 4 to 7 x 4 under dynamic allocation.

## 8. Open Items / Follow-ups
- Dispatcher: commit the worktree diff (4 files) and this ticket folder, run `/pr_gauntlet`, open the PR on `SteelHouse/airflow-ti` with `artifacts/audi_1276_pr_body.md`; reviewer Ryan Kleck (`rkleck-mntn`), FYI Alex Knorr and syang413 per §3.2 step 10. Post `artifacts/audi_1276_result_comment.txt` to AUDI-1276.
- Dev validation was not run (D2, D3). First validation = first prod cron after merge: `feature_store_setup_model` 01:03Z, `ipdsc_monitor` lands ~04:20Z. Pull the new logs from `gs://mntn-data-archive-prod/spark-events/` within hours and run `outputs/audi_1276_stage_map.py`; accept when there is no `Exchange hashpartitioning(advertiser_id|category_id)` on the big side, flagged-stage data-skew under 2x and largest partition share under 0.05, output row counts equal the prior day's pattern, and the monitor shows one 40 GB-class read.
- If the prod plan still shows the join-key Exchange (A2 failing on the real JDBC relation), apply §3.2 step 6 (`spark.sql.requireAllClusterKeysForDistribution=true` in each decorator, regenerate `dags/model_task_config.json`).
- After merge (Malachi's ledger reconcile, §3.2 step 11): `ledger applied ipdsc_42_monitor skew:18|22|26 <PR#> <date>`; the three feature-store DAGs are fixed-but-unmeasurable-by-ledger (below the 60 s floor); cite before/after event logs instead.
- Self-review entry (`self_review/self_review_2.md`) to be added by the dispatcher; off-limits to this agent.
- AUDI-1272 overlap: re-verify its `ipdsc_42_monitor` fetch-wait item after this lands rather than applying it.
- §3 was not rewritten; §3.7 records the deviations (steps 6, 8, 9).

## Verification (adversarial re-check, 2026-09-03)
Verdict: **PARTIAL**. Every technical claim and quantitative value checked against source reproduced independently; the Jira `result_comment.txt` slated to post carries one wrong number.

**Independently reproduced, all matched:**
- Worktree `git diff`: 4 files, +30/-25, exactly the two-line `F.broadcast()` edit in each of the three feature-store files and the SQL split (`IPDSC42_BY_CATEGORY_SQL` + `IPDSC42_COMPARE_SQL`, `/*+ BROADCAST(dd) */` on both deal joins, `.cache()` + `comparison` temp view) in `ipdsc_42_monitor.py`. No decorator edit in the diff; re-ran `MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun` myself — identical "Compiling all models / Skipping all models upload to 'dev' env", exit 0, `dags/model_task_config.json` still untouched afterward.
- `ruff check` on the 4 branch files, run independently: 14 errors, same (file, code) set as `audi_1276_ruff_baseline_main.txt`/`_branch.txt` (line numbers shift by the monitor file's net +5 lines, as expected from +30/-25).
- Re-ran `outputs/audi_1276_local_plan_check.py` end-to-end (Spark 3.5.3, `JAVA_HOME` openjdk@17): guid_log/conv_log shapes both reproduce "same output rows with and without the hint: True"; the monitor's 3-case check reproduced the exact row values quoted in §4.5 (category 1 `(2,0,2,0.0)`, category 2 `(3,2,1,0.5)`, category 4 `(0,2,-2,-1.0)`, TOTAL `(7,6,1,0.1667)`) and `same row multiset=True` in all 3 cases.
- `audi_1276_verdict_table.csv`: 21 data rows as claimed; `data_skew_x` range 3.3-15.3x matches the PR body's "3-15x"; §4.1's per-DAG ranges all reproduce from the CSV.
- `audi_1276_savings_estimate.csv`: recomputed §5.2's monitor row from raw columns — stage 22+26 wall 346.2-618.3s ("346 to 618" ✓), stage 18 balanced 154.8-217.2s ("155 to 217" ✓), total saving 370.2-746.0s = **6.2-12.4 min** (§5.2's "6 to 12 min" ✓), exec-hours 0.4637/0.7408/1.0335 on 08-31/08-30/09-02 ("0.46, 0.74, 1.03" ✓). Feature-store per-DAG saving ranges and means (14.3s/9.9s/12.1s) all reproduce.
- `SKEW_MIN_TASK_MS = 60_000` confirmed at `include/spark_optimizer/optimizations.py:24`; `ledger.mark_applied` raises `"no ledger history for {dag_id}/{key}; nothing to mark applied"` verbatim.
- Reviewer/FYI list (rkleck-mntn / Alex Knorr / syang413) matches last-commit authorship on the 4 files.
- `*.csv` and `*.zstd` are both workspace-gitignored — the open_items force-add instruction is correct; `audi_1276_stage_rows.csv` (raw per-stage dump) is left out of that list, reasonably.
- No commits ahead of `origin/main` on `audi-1276-join-skew`; no Jira/Slack write; `self_review_2.md` untouched.
- §0's "what would change the answer" clause: no DAG's flagged stage is literally a join (§6 confirms). The plan anticipated exactly this and allowed "matching fix or none" — the agent's choice to still apply the join-side broadcast fix is reasoned through in §4.3/§4.5, not glossed over, so this does not contradict the Objective.

**Defect: `artifacts/audi_1276_result_comment.txt` understates the monitor saving.** It reads "one pass saves 6 to 10 min"; §5.2 and the savings CSV (above) both give 6.2-12.4 min, i.e. "6 to 12 min." The PR body does not repeat the error. Corrected text (same lint pass, `--kind completion`: 119 words / 698 chars / 7 bullets, cap 120/800/8):

> Join-caused skew confirmed on all 4 DAGs; fix is a plan-time broadcast hint, PR ready.
>
> *Findings*
> * Every join is broadcast at runtime, so AQE skew handling cannot fire; the skew is the join's plan-time shuffle on advertiser_id or category_id (one advertiser is 30% of conversion_log).
> * ipdsc_42_monitor reads its 40 GB shuffle three times (CTE re-evaluated); one pass saves 6 to 12 min and 0.5 to 1.0 executor-hours per run.
> * Each feature-store DAG saves 8 to 18 s per run, under the ledger's 60 s floor.
>
> *Next*
> * Gauntlet and open the PR; no dev run (shared dev branch), first prod cron validates.
> * After merge, mark the three ipdsc_42_monitor skew findings applied in the optimizer ledger.

Use this corrected version when posting to AUDI-1276; the file on disk still has the "6 to 10" error and should be fixed before posting.
