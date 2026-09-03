---
doc_type: ticket
title: "AUDI-1276: Confirm joins and fix skew on 4 DAGs"
status: backlog
date: 2026-09-02
summary: "Confirm the skewed stage is a join, then enable AQE skewJoin or salt the hot key on 4 DAGs"
result: "not started"
question: "For each of the 4 DAGs, is the skewed stage a join, and does AQE skewJoin or salting the hot key spread it?"
framing_state: locked
---

# AUDI-1276: Confirm joins and fix skew on 4 DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1276
**Status:** backlog
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
