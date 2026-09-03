---
doc_type: ticket
title: "AUDI-1277: Tune the 2 heaviest BigQuery query shapes"
status: backlog
date: 2026-09-02
summary: "bos__spend hourly creates and intent_score_threshold_v4 histogram, ~2,300 slot-h/day together"
result: "not started"
question: "What in the bos__spend hourly create queries and the intent_score_threshold_v4 population_histogram drives about 2,300 slot-hours a day, and what change to the query shape or its filters cuts it?"
framing_state: locked
---

# AUDI-1277: Tune the 2 heaviest BigQuery query shapes

**Jira:** https://mntn.atlassian.net/browse/AUDI-1277
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** What in the bos__spend hourly create queries and the intent_score_threshold_v4 population_histogram drives about 2,300 slot-hours a day, and what change to the query shape or its filters cuts it?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** Root cause per query shape in outputs/ from execution plans, PRs in the owning repo merged (or an owner hand-off with the exact change if the repo is not ours), and slot-hours per day down on the Mode optimizer BQ report.
- **Approach (how):** INFORMATION_SCHEMA.JOBS via bq_run.sh (us-central1) for job text, labels, bytes billed, slot-ms and plan stages, date-filtered and LIMITed; locate the SQL in source (bos service, dbt, SQLMesh or airflow-ti); test the reshaped query with --dry_run and compare bytes.
- **What would change the answer:** The plan is already partition-pruned and the cost is genuine volume, in which case the recommendation is cadence or materialization, not shape.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Tune the two heaviest BigQuery query patterns we found: together about 2,300 slot-hours per day, with one 1,347 TiB scan day.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the [cost dashboard](https://app.mode.com/mntn/reports/e81786de8403) shows bos__spend's campaign_summary_hourly-create (1,275 slot-h/day across 288 runs) plus flight_metrics_per2388-create (977 slot-h), and intent_score_threshold_v4's population_histogram (1,075 slot-h, 99 TiB in 4 jobs). Likely missing partition filters or repeated identical runs.

**Task:** read each query's execution plan, fix the query shape or add the missing filters.

**Done-when:** PRs merged; slot-hours drop on the [optimizer BQ report](https://app.mode.com/mntn/reports/e81786de8403).

## 3. Plan of Action
Planning wave 2026-09-02 (read-only; nothing executed, no Jira/git writes). Everything below was verified empirically; evidence files are in `outputs/`.

### 3.0 What planning established

**Source of truth.** Both DAGs live in GitHub `SteelHouse/airflow-camperbid` (no local clone; read via `gh api`, reference copies in `outputs/source_refs/`). Not `airflow-ti`, not SQLMesh, not the BOS service.
- `bos__spend`: `dags/bos/bos__spend.py`, schedule `0,15,30,45 * * * *` (96 runs/day). Task `campaign_summary_hourly.create` is a two-statement script (DELETE 5 days + INSERT: 24h of `dw-main-silver.logdata.spend_pacing` UNION 4 days of `cost_impression_log`, both joined to `dw-main-gold.public.campaigns`). Task `flight_metrics_per2388.create` is a single SELECT with `writeDisposition=WRITE_TRUNCATE` after a `drop`: all-time aggregate of `dw-main-silver.summarydata.all_facts` joined to `dw-main-gold.dso.campaign_group_flight`, `af.hour >= '2024-01-01' AND af.hour >= cgf.flight_start_time_local`. Copied from `db_repo coredw/lds/functions/populate_campaign_performance_v2.sql`.
- `intent_score_threshold_v4`: `dags/intent_score_threshold_v4/dag.py` (schedule `0 0 * * *`) + `sql/population_histogram.sql` (PACE-6989, PRs 537/542: one shared 24h scan of `raw.bid_price_log` + `raw.bidder_bid_events`, MAX score per (campaign, ip), histogram; ends with an ASSERT > 100k rows).
- Owners: `.github/CODEOWNERS` has no rule for `dags/bos*` or `dags/intent_score_threshold*`, so the global default applies: `@SteelHouse/pacing` + `@SteelHouse/performance-ml`. `dags/bos/README.md` names DAG owner Forrest Bajbek (`forrest-mntn`) and downstream owner Tony Chen (`tonychen-mountain`). Histogram authors: Varun Jain (`varunjainMNTN`, PRs 523/537/542) and Tony Chen (PR 572, 2026-09-01). `malachi` has `push` (not admin) on `airflow-camperbid` and on `SteelHouse/sqlmesh`. CI on PR = `lint.yaml` (pre-commit: black/isort/flake8/gitleaks). Dev and prod deploy together ON MERGE (`deploy_dev.yaml`, `deploy_prod.yaml`); there is no pre-merge dev deploy, so pre-PR validation is rendered-SQL dry runs, pinned comparison runs, and `astro dev` parse.

**Job history.** `dw-main-bronze`.`region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, `user_email = airflow-camperbid-prod@mntn-prj-prod-00.iam.gserviceaccount.com`. `mntn-prj-prod-00` JOBS_BY_PROJECT is denied to malachi (no `bigquery.jobs.create`). Dry-run cost of the JOBS view: 2 days 16.5 GB, 1 day 9.3 GB, 6-7 h 3.0-3.3 GB; stay under 5 GB with ≤12 h windows. `query`, `labels`, `referenced_tables`, `job_stages`, `parent_job_id` all present at job grain; `bq show --format=json -j <job_id>` (project `dw-main-bronze`) returns the full plan free of charge. Saved plans: `outputs/audi_1277_plan_{hist,fm_max,fm_median,csh}.json`; 6h job sample `outputs/audi_1277_jobs_sample_6h.json`; daily optimizer reports `outputs/optimizer_bq_2026-08-28..09-02.md`.

**The ticket's numbers double count two of the three tasks.** `include/spark_optimizer/bq_profile.py` `PROFILE_SQL` (airflow-ti, ours) sums `total_slot_ms` and `total_bytes_billed` over every JOBS row. A BigQuery script's parent job (`statement_type = 'SCRIPT'`, `parent_job_id IS NULL`) already carries the sum of its children, and the children are rows too. Verified over 7 h (`outputs/audi_1277_job_structure_7h.txt`): `campaign_summary_hourly-create` SCRIPT parent 151.7 slot-h = INSERT child 151.7 (DELETE child 0); `population_histogram` = SCRIPT parent + CTAS child 540.6 + tiny SELECT + ASSERT. The 288 "jobs" are 96 runs × 3 rows; the 4 histogram "jobs" are 1 run. `flight_metrics_per2388-create` is a plain SELECT job (96 rows = 96 runs), so its figure is real. True daily load (2026-09-02): `flight_metrics_per2388-create` ≈ 977 slot-h / 1,347 TiB (the heaviest), `campaign_summary_hourly-create` ≈ 640 slot-h / ~22 TiB, `population_histogram` ≈ 540 slot-h / ~51 TiB (matches PR 537's own measurement, 555 slot-h / 50.8 TB). Sum ≈ 2,150 slot-h, not 3,300.

**flight_metrics_per2388-create: repeated identical runs, not a missing filter.** 99% of slot time is one stage reading the physical `sqlmesh__summarydata.summarydata__all_facts__3194417682` (54 B rows, 976 partitions processed = every day since the 2024-01-01 floor; DAY partitioning on `hour` prunes correctly; 14.08 TiB and 8-22 slot-h per run). `dso.campaign_group_flight` is 21,871 rows, exactly one per `campaign_group_id` (no join fan-out), `MIN(flight_start_time_local)` 2022-06-01 so the 2024 floor binds. The source is a SQLMesh `INCREMENTAL_BY_TIME_RANGE` model (`lookback 72` h, `cron '@hourly'`) that is MERGE-written about every 2 h in practice (6 MERGEs in 12 h, `outputs/audi_1277_all_facts_writer_cadence_12h.txt`; partitions 08-30..09-02 rewritten together at 09-03 02:29). So roughly 85-90% of the 96 daily runs recompute an unchanged result. Sizing for a rollup fallback: all_facts has 199,720 distinct `(campaign_id, campaign_group_id, channel_id, hour)` per day vs 53.4 M rows (267×); `sum_by_campaign_by_day` exists with mergeable HLL `uniques` and every metric column but lacks `campaign_group_id`/`channel_id` and is day-grain, so it cannot reproduce `hour >= flight_start_time_local` exactly.

**population_histogram: genuine volume, partition-pruned.** `raw.bid_price_log` is HOUR-partitioned on `time` with `requirePartitionFilter=true`, clustered by `ip` (2.8 PiB / 10-day TTL); `raw.bidder_bid_events` is ingestion-time HOUR-partitioned (12 PiB / 30-day TTL). Dry-run of the 24h column-pruned reads: `bid_price_log` 15.0 TiB, `bidder_bid_events` 43.4 TiB. Campaign filter ≈ 1,954 campaigns. Plan (`audi_1277_plan_hist.json`, 539 slot-h): stage 9 scan+filter+union 242 slot-h (45%), stage 15 repartition of 55.4 B rows for the MAX-per-(campaign, ip) dedup 127 slot-h (23.5%), stage 17 aggregate 10.3 B rows → 286 M histogram rows 123 slot-h (23%). Nothing to prune; §0's "what would change the answer" applies to this shape.

**campaign_summary_hourly-create: the cost is the `spend_pacing` VIEW, re-evaluated 96×/day.** 0.227 TiB but 259 stages and ~6.5 slot-h per run. `dw-main-silver.logdata.spend_pacing` is a SQLMesh `kind VIEW` (`SteelHouse/sqlmesh` `models/dw-main-silver/logdata/spend_pacing.sql`, `owner 'ber'`, copy in `outputs/source_refs/sqlmesh_spend_pacing.sql`) that scopes itself to `dt >= CURRENT_DATE - 2` over three hive-partitioned external parquet tables (`external.impression__v1` read 3× = 1.5 B rows, 17.5% of slot; `bidder_win_notifications__v1`; `vastimpression__v1`), unions CIL, joins 17 margin/dim tables and dedups with ROW_NUMBER. The caller's 24h `sp.time` predicate cannot prune inside the view (`dt` is the only partition key). The 4-day CIL half is ~6% of slot. No change inside `airflow-camperbid` reduces this without duplicating the view's business logic (margins, unlinked, PSA exclusion); the 24h live window is a billing-safety buffer (Lizz, #data-platform 2026-06-08) and the 5-day CIL lookback was raised for correctness (PER-6212, PR 394). Do not shrink either.

### 3.1 Steps (each names the file, the change, and the pre-PR validation)

**Step 0. Fix the measurement first (airflow-ti, ours).**
- File: `include/spark_optimizer/bq_profile.py`, `PROFILE_SQL`: add `AND parent_job_id IS NULL` to the WHERE clause (top-level jobs only; a script parent already carries its children's slot-ms and bytes). Adjust the docstring's one line about jobs if it now reads wrong.
- Test: `include/spark_optimizer/tests/test_bq_profile.py` (2 existing tests) gains one asserting the rendered SQL contains `parent_job_id IS NULL`; run `uv run pytest include/spark_optimizer/tests/`.
- Validation: run the corrected SQL for 2026-09-02 via `bq_run.sh --project_id=dw-main-bronze` in two 12 h windows (1 day dry-runs at 9.3 GB, over the 5 GB cap) and confirm `campaign_summary_hourly-create` ≈ 96 jobs / ~640 slot-h and `population_histogram` = 1 job / ~540 slot-h.
- PR body and Jira must say: the day this deploys, the Mode `opt-bq` table drops ~40% for these two tasks with no real saving; `bq_heavy_task:*` findings stay above the 50 slot-h threshold, so nothing resolves falsely. Decision D3 (below): file under AUDI-1277 or AUDI-1278.

**Step 1. `flight_metrics_per2388`: stop recomputing an unchanged result (airflow-camperbid).**
- File: `dags/bos/bos__spend.py`, TaskGroup `flight_metrics_per2388`. Add a `@task.short_circuit(ignore_downstream_trigger_rules=False)` task `source_changed` ahead of `drop`. It computes a fingerprint from two metadata-only reads: `MAX(last_modified_time)` from `dw-main-silver.sqlmesh__summarydata.INFORMATION_SCHEMA.PARTITIONS WHERE table_name = 'summarydata__all_facts__3194417682'` and `FARM_FINGERPRINT(STRING_AGG(CONCAT(campaign_group_id, '|', flight_start_time_local) ORDER BY campaign_group_id))` over `dw-main-gold.dso.campaign_group_flight` (21,871 rows). Compare with Airflow Variable `bos__flight_metrics_per2388_fingerprint`; on match skip `drop` + `create`, else run and store the new value. The query text itself is untouched, so semantics are identical.
- Downstream: `tables.campaign_performance.create` must run when the group skips; give it `trigger_rule="none_failed"` (it also depends on `campaign_summary_hourly`). Keep `drop` (minimal diff) or delete it (create is WRITE_TRUNCATE); either is fine.
- Expected: 96 → ~6-12 runs/day, ~977 → ~60-120 slot-h/day, 1,347 → ~85-170 TiB/day.
- Validation before PR: (a) render the DAG in local `astro dev start` (README) and `airflow tasks render`; (b) run the fingerprint SQL through `bq_run.sh` twice, ~30 min apart, and show it changes only when an all_facts MERGE lands (cross-check with the 12 h writer list); (c) `uv run pre-commit run --all`; (d) `py_compile` of the DAG. No pre-merge dev deploy exists, so the PR body carries (a)-(c). After merge: confirm skipped runs in the Airflow UI and the drop on the Mode `opt-bq` table.
- Alternative if Pacing prefers no stored state (decision D2): move the TaskGroup into `dags/bos/bos__hourly.py` (schedule `0 * * * *`, `max_active_runs=1`), 24 runs/day = 75% saving, `campaign_performance` keeps reading the same table.
- Follow-up only if Pacing wants every-run freshness (or all_facts becomes truly hourly): hour-grain rollup `dw-main-bronze.external.camperbid_{env}__bos__all_facts_campaign_hourly` (`campaign_id, campaign_group_id, channel_id, hour, HLL_COUNT.MERGE_PARTIAL(uniques)`, SUMs of the 11 metric columns; ~200k rows/day), refreshed for the trailing 5 days by a task in `bos__hourly` (all_facts lookback is 72 h), and `flight_metrics_per2388.create` reads the rollup with the same `hour >= flight_start_time_local` join. Validate by diffing per-campaign output of old vs new query, both pinned with `FOR SYSTEM_TIME AS OF` the same timestamp: zero diffs required.

**Step 2. `population_histogram`: measure the one shape lever, ship only on evidence (airflow-camperbid).**
- File: `dags/intent_score_threshold_v4/sql/population_histogram.sql`, BOTH branches (flag-on and flag-off). Candidate: compute `hh_score` inside each source CTE so `time` and `conquest_score_ttl` never enter the union/shuffle, and group the dedup on `FARM_FINGERPRINT(ip)` (INT64) instead of the STRING `ip` (flag-on `'mntn'` keyspace likewise on `household_id_value`). Output is identical up to 64-bit hash collisions (~1e-9 expected over 10 B keys); the dedup shuffle (46% of slot) gets narrower rows.
- Measurement: run baseline and candidate on ONE hour (`time >= end - 1 HOUR`), both `FOR SYSTEM_TIME AS OF` the same timestamp, as `CREATE TABLE` into a scratch dataset the execute agent can write to (or as `SELECT ... ` with `COUNT`/`FARM_FINGERPRINT` checksum if no scratch dataset). Expected ~22 slot-h per side. Compare `totalSlotMs` from `bq show -j` and assert identical `(campaign_id, key_type, hh_score, population)` rows.
- Ship threshold: ≥15% slot saving. Below that, record the §0 verdict (genuine volume, partition-pruned, nightly cadence already minimal) with the plan evidence and close this shape without a PR. Name in the hand-off, not ours to fix: bidder event volume ~4× since 2026-08-05 (PR 523) roughly doubles this query; owner RTB.

**Step 3. `campaign_summary_hourly`: upstream hand-off, materialize the view (SteelHouse/sqlmesh, Data Platform).**
- Measurement first: run the `spend_pacing` CTE alone and the CIL CTE alone for one run's window (pinned) and attribute the ~6.5 slot-h; expected ≥90% in the view.
- Exact change to propose in `models/dw-main-silver/logdata/spend_pacing.sql` (owner `'ber'`): either change `kind VIEW` to an incremental table (`INCREMENTAL_BY_TIME_RANGE` on `time_hr`, `cron '*/15 * * * *'`, lookback covering the 2-day `buffer_dt`) or add a sibling `logdata.spend_pacing_materialized` with that kind and repoint consumers. BOS then needs no change (name kept) or a one-line FROM swap in `bos__spend.py`. Evidence for the ask: `outputs/audi_1277_plan_csh.json` (impression__v1 read 3×, 1.5 B rows, 259 stages, ~6.5 slot-h per 0.23 TiB run × 96/day ≈ 640 slot-h/day), plus the fact that any other consumer of the view pays the same each read.
- Owner: Data Platform (SQLMesh model owner `ber`), Pacing (Forrest Bajbek) cc'd as the consumer. We have push on `SteelHouse/sqlmesh`; the PR can be ours if Data Platform agrees (decision D1). Do not touch the 24h live window or the 5-day CIL lookback.

**Step 4. Provenance and close.** After each merge run `python -m airflow_optimizer.ledger applied <dag> <key> <PR#> <merge-date>` (keys `bq_heavy_task:flight_metrics_per2388-create`, `bq_heavy_task:population_histogram`, `bq_heavy_task:campaign_summary_hourly-create`), watch `gs://mntn-data-archive-prod/optimizer/optimizer_bq_<date>.md` and the Mode `opt-bq` section, fill §4-§6 here, `/capture` (facts for `knowledge/`: the profiler double count; camperbid jobs live in `dw-main-bronze` JOBS_BY_PROJECT under `airflow-camperbid-prod@`; all_facts MERGE cadence ~2 h with 72 h lookback; `spend_pacing` is a self-scoping 2-day view over three external parquet tables).

### 3.2 Sources
- Jira AUDI-1277 (read-only), parent AUDI-1290; spec `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` items 22-23.
- `SteelHouse/airflow-camperbid` main: `dags/bos/bos__spend.py`, `dags/bos/bos__hourly.py`, `dags/bos/README.md`, `dags/intent_score_threshold_v4/{dag.py,README.md,sql/population_histogram.sql}`, `.github/CODEOWNERS`, PRs 394, 523, 537, 542 (copies under `outputs/source_refs/`).
- `SteelHouse/sqlmesh` main: `models/dw-main-silver/logdata/spend_pacing.sql`, `models/dw-main-silver/summarydata/all_facts.sql` (copies under `outputs/source_refs/`).
- `airflow-ti` main (`/Users/malachi/Developer/work/mntn/airflow-ti-main`): `include/spark_optimizer/bq_profile.py`, `sweep.py`, `tests/test_bq_profile.py`.
- BigQuery (all via `bq_run.sh`, logged in `knowledge/bq_perf_log.jsonl` under AUDI-1277): `dw-main-bronze` JOBS_BY_PROJECT samples, `bq show` on the 11 source/destination tables, `bq show -j` plans, `INFORMATION_SCHEMA.PARTITIONS` for all_facts, `dw-main-silver` JOBS_BY_PROJECT writer list.
- Prod optimizer outputs `gs://mntn-data-archive-prod/optimizer/optimizer_bq_2026-08-28..09-02.md` and `optimization_ledger.jsonl` (surface=bq rows for both DAGs).
- Memory: `knowledge/memory/project_airflow_optimizer.md` (2026-08-28/29 BQ surface sections), `reference_bq_job_attribution.md`, `reference_mode_api.md`; `knowledge/bq/logdata/spend_pacing.md`, `knowledge/bq/summarydata/{all_facts,sum_by_campaign_by_day}.md`, `knowledge/bq/external/bidder_win_notifications__v1.md`; `knowledge/data_knowledge.md` line 4812 (24h pacing buffer).

### 3.3 Assumptions to resolve empirically before executing
1. all_facts write cadence over a full week matches the 12 h sample (~every 2 h, 72 h rewrite window): rerun the writer-list query in 12 h windows for 3-4 days; the skip rate estimate depends on it.
2. `campaign_group_flight.flight_start_time_local` changes rarely intraday: log the fingerprint every 15 min for a day; if it flips often the gate degenerates and D2 (hourly cadence) is the better change.
3. The execute agent has a writable scratch dataset in `dw-main-bronze` or `dw-main-dev` for Step 2's pinned A/B (`bq_run.sh` is read-only by rule; a `CREATE TABLE` into scratch needs an explicit OK, else use checksum SELECTs).
4. Airflow 3 on Astronomer supports `Variable` reads/writes from a `@task.short_circuit` in this repo (PR 475 shows `airflow.sdk.Variable` in use, so yes; confirm in `astro dev`).
5. The Mode `opt-bq` query (token `3ead7301daa8`) reads the ledger / optimizer_bq outputs rather than INFORMATION_SCHEMA directly; if it reads JOBS itself it needs the same `parent_job_id IS NULL` filter.
6. PAM/PHS access was not needed (no Spark event logs in scope); nothing to record.

### 3.4 Risks
- Baseline shift: Step 0 lowers the dashboard for two tasks by ~40% without saving anything; land it before Steps 1-3 and say so in the PR, Jira and the ledger note so the hackathon savings tally is honest.
- Repo we do not own: `airflow-camperbid` and `sqlmesh` PRs need Pacing / Data Platform review; a merge deploys dev and prod together, so the change must be right the first time (no dev-only rollout). Rollback = revert.
- Step 1 stored state: a stale or lost Airflow Variable only causes an extra recompute, never a skipped needed one, provided the comparison defaults to "changed" when the Variable is missing.
- Step 1 correctness edge: `campaign_performance` consumes `flight_metrics_per2388` via the Spark job args; a skipped create leaves last run's table in place (WRITE_TRUNCATE table persists), which is exactly the intended behaviour, but the `drop` must be inside the skipped branch or the table vanishes.
- Step 2 may show <15% saving; that is an acceptable outcome per §0 and must be recorded as such, not forced.
- Step 3 is outside our control; if Data Platform declines, the shape stays at ~640 slot-h/day and the ticket closes with the hand-off documented.
- The `adhoc` reservation (1,000 slots, `ignore_idle_slots`) runs the histogram; wall time, not slot-h, is what pages, so any shape change must be checked for runtime regression on the 1 h A/B as well.

### 3.5 Decisions for the user
- D1: open the `airflow-camperbid` (Step 1, 2) and `sqlmesh` (Step 3) PRs ourselves with owner review (push permission verified on both), or hand off the exact change and let the owners author it. Recommendation: PRs ours for Steps 1-2 (small, semantics-preserving), Step 3 as an ask first.
- D2: Step 1 mechanism: fingerprint gate (~90% saving, exact, stored state) vs hourly cadence (75%, stateless) vs both. Recommendation: gate, with the hourly move as the fallback if Pacing objects to state.
- D3: Step 0 (profiler double count) under AUDI-1277 or AUDI-1278 (BQ labels / attribution ticket).

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
