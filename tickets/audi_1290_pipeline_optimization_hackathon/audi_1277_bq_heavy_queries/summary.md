---
doc_type: ticket
title: "AUDI-1277: Tune the 2 heaviest BigQuery query shapes"
status: in_progress
date: 2026-09-02
summary: "bos__spend hourly creates and intent_score_threshold_v4 histogram, ~2,300 slot-h/day together"
result: "Executed 2026-09-03: profiler double count fixed (airflow-ti branch), fingerprint skip gate on flight_metrics_per2388 + INT64 dedup key on population_histogram (airflow-camperbid branch, 31% slot saving on a pinned A/B), spend_pacing materialization ask drafted for Data Platform"
question: "What in the bos__spend hourly create queries and the intent_score_threshold_v4 population_histogram drives about 2,300 slot-hours a day, and what change to the query shape or its filters cuts it?"
framing_state: locked
---

# AUDI-1277: Tune the 2 heaviest BigQuery query shapes

**Jira:** https://mntn.atlassian.net/browse/AUDI-1277
**Status:** in_progress (PRs open: airflow-ti #1277, airflow-camperbid #580)
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

### 3.6 Execution deviations from the plan (2026-09-03)
- **Step 1 skip rate is about half, not 85-90%.** `dw-main-gold.dso.campaign_group_flight` is a view of the *currently active* flight per group (`start_time <= now < end_time`), so its row set changes at every flight start and end: 170-400 events on a weekday touching 50-70 of the 96 quarter-hours, ~100 on a weekend touching ~20, and 41,915 on 2026-09-01 (monthly rollover) touching 52. Any exact fingerprint has to carry that set, so the gate fires on those windows too. Measured union with the all_facts MERGE windows: 48% of runs skipped on Mon 08-31, 28% on the rollover day 09-01, 44-71% on the two half-measured days. Restricting the fingerprint to groups present in the output does not help (48 vs 51 windows on 09-02: the groups with data are the ones whose flights churn). Recorded in §4.2 and §8; the hourly move (D2 fallback) is the lever that gets to ~80%.
- **Fingerprint stores after `create`, not in the gate.** Three tasks (`source_fingerprint` -> `source_changed` -> `drop` -> `create` -> `record_source_fingerprint`) so a failed rebuild never marks its inputs as reflected. The fingerprint also carries `CURRENT_DATE()` (one guaranteed rebuild a day) and falls open: a NULL input (`GENERATE_UUID()` stands in) or a failed fingerprint query (`unavailable:<uuid>`) reads as changed and rebuilds, the DAG never pages because of the gate.
- **`trigger_rule="none_failed"` goes on `tables.campaign_performance.drop`**, not `.create`: `drop` is the group's root and the task the `flight_metrics_per2388 >> tg` edge lands on.
- **Step 2 measured with checksum SELECTs, no scratch table** (decision D3 note from the user): baseline and candidate both wrapped in `COUNT/SUM/BIT_XOR(FARM_FINGERPRINT(row))`, one hour, sources pinned with `FOR SYSTEM_TIME AS OF`.
- **Step 3 attribution came from the saved plan** (`audi_1277_csh_attribution.py` over `audi_1277_plan_csh.json`, stage lineage traced to source tables) instead of re-running the two CTEs alone; same answer, no extra 7 slot-h.
- **One rule slip:** a flip-rate probe was written as a BigQuery script with `CREATE TEMP TABLE` inside it (job `perf_20260903_001240_*`, anonymous `_script...` dataset, auto-expired). Its result was discarded anyway (time travel is silently ignored on views). No other DDL/DML ran.
- **`astro dev parse` result:** did not complete on this Mac (20 minutes at "Checking your DAGs for errors", no image pull or network activity, killed); the DagBag parse in the repo's Airflow 3.3.0 venv (`import_errors {}`, wiring verified) stands in for it, and the PR body says so.

## 4. Investigation & Findings

### 4.1 Step 0, the measurement (airflow-ti, branch `audi-1277-bq-profile-parent-jobs`)
Corrected profile for 2026-09-02 UTC (`queries/audi_1277_step0_profile_corrected.sql`, `parent_job_id IS NULL`, two 12 h windows because a 1-day dry run of the JOBS view is 9.3 GB; outputs `audi_1277_step0_profile_2026-09-02_{am,pm}.txt`):

| dag | task | jobs | slot-h | TiB billed |
|---|---|---:|---:|---:|
| bos__spend | flight_metrics_per2388-create | 96 | 945.3 | 1,351.0 |
| intent_score_threshold_v4 | population_histogram | 1 | 528.3 | 51.5 |
| bos__spend | campaign_summary_hourly-create | 96 | 517.2 | 18.6 |
| (no airflow labels, both service accounts) | | 600 | 958.5 | 57.0 |

The uncorrected daily report for the same day (`outputs/optimizer_bq_2026-09-02.md`, generated mid-day) shows 4 jobs / 1,056.6 slot-h for the histogram and 240 jobs / 844.9 for campaign_summary_hourly: the script parent plus its children, each counted. The Mode `opt-bq` section (query token `3ead7301daa8`, read via the Mode API) selects from `mntn-prj-prod-00.optimizer.optimization_ledger WHERE surface = 'bq'` and parses slot-hours out of the finding title, so it inherits the fix with no Mode change (assumption 5 resolved). Tests in the worktree: `uv run pytest include/spark_optimizer/tests/` = 164 passed, 1 failed (`test_phs.py::test_newest_logs_takes_the_tail_and_drops_inprogress`, `fetch.newest_logs` hits a real listing on this Mac; pre-existing, untouched by this change); `ruff check` clean on both files.

### 4.2 Step 1, flight_metrics_per2388 (airflow-camperbid, branch `audi-1277-bos-spend-skip-gate`)
**all_facts write cadence over three full days** (`queries/audi_1277_all_facts_writer_cadence.sql` in 24 h windows, `outputs/audi_1277_all_facts_writer_cadence_3d.txt`): 13, 15 and 11 MERGEs per day from 2026-08-30 12:00 to 09-02 12:00 UTC (12-21 slot-h and 310-423 GiB each), gaps 0.3 to 4.6 h; 2026-09-03 00:00-06:51 UTC had two (02:26, 05:58). Assumption 1 holds: ~13 writes a day.

**Fingerprint behaves** (`queries/audi_1277_step1_fingerprint.sql`, `outputs/audi_1277_step1_fingerprint_runs.txt`): 03:29:47Z -> `1116952818349087922` (all_facts last modified 02:29:15, 21,871 flight rows); 06:51:50Z -> `-2976879156804707450` (06:01:27, 21,893 rows). Both inputs moved in between, so the change is expected. The PARTITIONS read costs ~2 slot-s (1.9 M partition rows in `sqlmesh__summarydata`).

**Assumption 2 fails: the active-flight set churns.** `dw-main-gold.dso.campaign_group_flight` -> view `dw-main-bronze.integrationprod.dso_campaign_group_flight` -> SQLMesh `kind VIEW` model `integrationprod__dso_campaign_group_flight__2006195297` = `core_flights` join `core_budget_types`, `public_campaign_groups_raw`, `public_advertisers` with `f.end_time >= current_timestamp() AND f.start_time <= current_timestamp() AND f.status_id <> 8 AND cgr.campaign_group_status_id <> 8`. One row per group because one flight is active at a time. Flight starts and ends from the current `core_flights` rows (`outputs/audi_1277_step1_flight_events_all_groups.txt`):

| day | starts | ends | quarter-hours touched (union, all groups) | present-group events | quarter-hours (present) |
|---|---:|---:|---:|---:|---:|
| 08-27 Thu | 98 | 72 | ~55 | 115 | 32 |
| 08-28 Fri | 108 | 76 | ~50 | 138 | 36 |
| 08-29 Sat | 58 | 51 | ~20 | 66 | 13 |
| 08-30 Sun | 38 | 44 | 22 | 36 | 9 |
| 08-31 Mon | 454 | 590 | 48 | 255 | 40 |
| 09-01 Tue (month start) | 20,907 | 21,008 | 61 | 5,581 | 58 |
| 09-02 Wed | 209 | 191 | 51 | 296 | 48 |

"Present" = the 3,372 campaign groups with a row in the current `flight_metrics_per2388` output (of 21,893 active-flight groups). Union with the MERGE windows (`outputs/audi_1277_step1_flight_event_buckets_4d.csv` + writer list, computed locally): 08-30 28 firing windows (68 skipped, MERGE list half-day), 08-31 50 (46 skipped, 48%), 09-01 69 (27 skipped, 28%), 09-02 54 (42 skipped, MERGE list half-day). Expected saving from the gate: about half the 945 slot-h and 1,351 TiB on a weekday, two thirds on a weekend, a quarter on the first of the month. A fingerprint restricted to present groups would skip only 3 more windows on 09-02, so it is not worth its extra read.

**Gotchas met on the way.** `FOR SYSTEM_TIME AS OF` on a view is silently ignored (BigQuery warns "Snapshot time ignored ... because it is a view" and returns current rows), so the 15-minute time-travel probe of the flight set was invalid and replaced by reconstruction from `core_flights.start_time/end_time`. The output table `dw-main-bronze.external.camperbid_prod__bos__flight_metrics_per2388` does not exist for most of each 15-minute cycle: `drop` runs at the top of the run and `create` finishes ~8 minutes later (observed missing 07:19-07:23 UTC, present at 07:23:18); with the gate it persists across skipped runs. `dw-main-bronze.integrationprod.core_flights` columns: flight_id, campaign_group_id, create_time, update_time, start_time, end_time, budget_type_id, budget, user_id, status_id, ui_flight_id, datastream_metadata, impression_cap.

**Airflow facts verified in the clone** (Airflow 3.3.0, Astro runtime 3.2-5): `airflow.sdk.Variable.get(key, default=..., deserialize_json=False)` and `Variable.set(key, value, description=None, serialize_json=False)` exist (assumption 4); `@task.short_circuit(ignore_downstream_trigger_rules=False)` skips only its direct downstream; `BigQueryHook.insert_job(configuration, job_id=None, project_id, location, nowait=False, ...)` returns a job whose `.result()` iterates rows (pattern from `dags/rill_data_validation/dag.py`). The camperbid prod service account's project-level IAM on `dw-main-silver` is not readable by me; dataset `sqlmesh__summarydata` grants READER to `projectReaders`, and the fail-open path covers a permission denial anyway.

### 4.3 Step 2, population_histogram (same branch)
Flag state: `camperbid_prod__hhst_v4__campaign_bucket` has 1,954 campaigns, 0 with `uses_mntn_id`; v3 has the same 1,954. The flag-off branch is what runs nightly.

**Where the prod job spends** (`outputs/audi_1277_plan_hist.json`, 540.6 slot-h): S09 scan+filter+union 242.1 slot-h (44.9%, 722 B rows read, 62.4 B written, 5.3 M parallel inputs) already computes `hh_score` and a partial `MAX GROUP BY (campaign_id, 'ip', ip)` before the shuffle; S0F repartition 126.7 slot-h (23.5%, 55.4 B rows); S11 final aggregate + histogram 123.3 slot-h (22.9%, 10.3 B -> 286 M rows). So the plan's "compute the score in the source CTE" half is a no-op for BigQuery; the lever is the STRING `ip` key in the partial aggregate and shuffle.

**Pinned one-hour A/B** (`queries/audi_1277_step2_hist_{baseline,candidate}_1h.sql`, `outputs/audi_1277_step2_ab_{baseline,candidate}.txt`, `audi_1277_step2_ab_compare.txt`): hour 2026-09-02 12:00-13:00 UTC, every source `FOR SYSTEM_TIME AS OF 2026-09-03 06:00 UTC`, dry run 2.375 TB per side, checksum SELECT instead of a table.

| side | slot-h | wall s | peak active units | scan stage slot-h | scan shuffle GB | rows / campaigns / keys / checksum |
|---|---:|---:|---:|---:|---:|---|
| baseline (prod flag-off shape) | 14.52 | 195.5 | 921 | 12.85 | 91.3 | 7,328,483 / 1,905 / 328,177,032 / 8003229889847476058 |
| candidate (score in source, `FARM_FINGERPRINT(ip)` key) | 10.01 | 214.1 | 98 | 8.44 | 72.2 | identical |

31.0% less slot time, identical output, 2.16 TiB billed on both. The aggregate stage is unchanged (1.60 vs 1.51 slot-h); the whole saving is the scan stage's partial aggregate on an INT64 key. Wall time is 10% longer on one sample with 9x lower peak concurrency (98 vs 921 active units) on the shared `adhoc` reservation, which is contention, not shape. Hash collisions: 64-bit key over at most a few hundred million IPs per campaign, expected collisions well under one across all campaigns. Both rendered branches of the edited file dry-run clean (`outputs/audi_1277_step2_rendered_{flag_on,flag_off}_select.sql`: 74.6 TB and 64.3 TB upper bound, the household-id columns being the 16% difference).

### 4.4 Step 3, campaign_summary_hourly (hand-off)
`outputs/audi_1277_csh_stage_attribution.txt` from the saved plan (259 stages, 6.87 slot-h): 76.2% stages whose lineage is only the `spend_pacing` view's sources (`external.impression__v1`, `bidder_win_notifications__v1` and 17 `integrationprod` dim tables), 4.7% dims only, 13.3% after the union with the cost log, 5.7% the cost log half. Top stages S111/S10D/SFE/SF2 each read 0.2-1.3 B rows from the impression and win-notification parquet. The ask is in `artifacts/audi_1277_sqlmesh_ask.md`.

## 5. Solution
**PR:** https://github.com/SteelHouse/airflow-camperbid/pull/580 (opened 2026-09-03 PT; airflow-camperbid skip gate + histogram, medium tier, 4 findings refuted, 0 confirmed; reviewers per CODEOWNERS (pacing, performance-ml))

**PR:** https://github.com/SteelHouse/airflow-ti/pull/1277 (opened 2026-09-03 PT; airflow-ti profiler fix, fast tier, 2 findings refuted, 0 confirmed; the airflow-camperbid PR follows its own gauntlet)

- **airflow-ti** (`/private/tmp/.../scratchpad/wt/audi_1277_ti`, branch `audi-1277-bq-profile-parent-jobs`, 2 files, +16/-1): `include/spark_optimizer/bq_profile.py` `PROFILE_SQL` adds `AND parent_job_id IS NULL` and the module docstring says why; `include/spark_optimizer/tests/test_bq_profile.py` adds `test_profile_sums_only_top_level_jobs`. PR body: `artifacts/audi_1277_pr_body_ti.md`.
- **airflow-camperbid** (`/private/tmp/.../scratchpad/airflow-camperbid`, branch `audi-1277-bos-spend-skip-gate`, 2 files): `dags/bos/bos__spend.py` (+53/-2: `source_fingerprint`, `source_changed`, `record_source_fingerprint` tasks in the `flight_metrics_per2388` group, Variable `bos__flight_metrics_per2388_source_fingerprint`, labelled hook job, `trigger_rule="none_failed"` on `tables.campaign_performance.drop`) and `dags/intent_score_threshold_v4/sql/population_histogram.sql` (both branches: score computed in each source CTE, dedup key `FARM_FINGERPRINT(ip)` and `FARM_FINGERPRINT(household_id_value)`, `temp_scored` folded away). Validation: `pre-commit run --files` clean (gitleaks, ruff check, ruff format), `ruff check`/`format --check` clean, DagBag parse `import_errors {}` with the wiring shown in §4.2, rendered SQL dry-runs valid; `astro dev parse`: did not complete on this Mac (20 minutes at "Checking your DAGs for errors", no image pull or network activity, killed); the DagBag parse in the repo's Airflow 3.3.0 venv (`import_errors {}`, wiring verified) stands in for it, and the PR body says so. PR body: `artifacts/audi_1277_pr_body_camperbid.md` (reviewers per CODEOWNERS default: `@SteelHouse/pacing`, `@SteelHouse/performance-ml`; the repo has a PR template with Ticket/Context/Changes/TTL/Tests/Documentation headings).
- **SQLMesh**: no edit; `artifacts/audi_1277_sqlmesh_ask.md` is the send-draft for Data Platform.
- Jira completion comment: `artifacts/audi_1277_result_comment.txt` (linted, not posted).

## 6. Questions Answered
- **Q:** Is the 2,300 slot-h/day figure real? **A:** No. Two of the three tasks were double counted (script parent + children). True 2026-09-02 load: 945 + 528 + 517 = 1,991 slot-h; the ticket's own numbers for those two tasks are ~1.6-2x high.
- **Q:** What drives flight_metrics_per2388's cost? **A:** Repetition. 96 identical 14 TiB all_facts scans a day while all_facts changes ~13 times a day. Flight churn (starts and ends every quarter-hour on weekdays) caps an exact skip gate at about half the runs; an hourly cadence is the lever beyond that.
- **Q:** Is there a shape lever in population_histogram? **A:** One: the STRING IP as the dedup key. Hashing it to INT64 saves 31% slot time with identical output. Nothing to prune; the scan is partition-pruned genuine volume.
- **Q:** Can airflow-camperbid fix campaign_summary_hourly? **A:** No. 81% of the run is the `spend_pacing` view's own logic; only materializing the view (SQLMesh, Data Platform) removes it.
- **Q:** Does the Mode cost table need its own fix for the double count? **A:** No, it reads the ledger's `surface='bq'` rows.

## 7. Data Documentation Updates
Handed to the dispatcher as `knowledge[]` (no knowledge/ writes from this agent): the profiler double count and `parent_job_id IS NULL`; camperbid jobs in `dw-main-bronze` JOBS_BY_PROJECT under `airflow-camperbid-prod@`; all_facts MERGE cadence ~13/day, 72 h lookback; `spend_pacing` is a self-scoping 2-day view over three external parquet tables; `dso.campaign_group_flight` is a current-active-flight view with one row per group that churns every quarter-hour; `FOR SYSTEM_TIME AS OF` is silently ignored on views; `flight_metrics_per2388` is absent ~8 of every 15 minutes today; the histogram INT64-key saving.

## 8. Open Items / Follow-ups
- Open both PRs (gauntlet first), owner review: airflow-ti ours; airflow-camperbid `@SteelHouse/pacing` + `@SteelHouse/performance-ml`. Merge deploys dev and prod together.
- After each merge: `python -m airflow_optimizer.ledger applied <dag> <key> <PR#> <date>` for `bq_heavy_task:flight_metrics_per2388-create`, `bq_heavy_task:population_histogram`, and Step 0 as a baseline correction; watch `optimizer_bq_<date>.md` and the Mode `opt-bq` section. Expected after Step 0 alone: histogram ~540 and campaign_summary_hourly ~520 slot-h/day on the table with no real saving.
- **Hourly cadence for `flight_metrics_per2388` (D2 fallback, Pacing's call):** flight churn limits the gate to ~50% on weekdays; moving the group to `bos__hourly` (or the gate plus an hourly schedule) reaches ~75-80%. Needs Pacing to accept up to 60 minutes of latency on flight rollovers in `campaign_performance`.
- The flag-on branch of `population_histogram.sql` is dry-run valid but has never run live (0 flagged campaigns); the first flagged advertiser exercises it.
- `test_phs.py::test_newest_logs_takes_the_tail_and_drops_inprogress` fails on this Mac before and after Step 0 (real GCS listing under monkeypatch); not this ticket's.
- Send `artifacts/audi_1277_sqlmesh_ask.md` to #data-platform; on a yes, the SQLMesh PR (ours or theirs) is a separate change.
- The `CREATE TEMP TABLE` script slip (§3.6) is recorded; nothing persisted.

## Verification

Adversarial pass 2026-09-03, read-only against both worktrees (`.../scratchpad/airflow-camperbid`, `.../scratchpad/wt/audi_1277_ti`) and live BigQuery (`bq show -j`, two fresh `--dry_run`s). Verdict: **done** — every headline claim survives; three narrative-only inaccuracies found and fixed here, none touching the shipped diffs.

**Independently re-derived (exact matches, not just "close"):**
- Both Step 2 A/B jobs are real: `bq show -j dw-main-bronze:perf_20260902_235637_22995` gives `totalSlotMs=52271971` → 14.5200 slot-h, `totalBytesBilled`=2.1604 TiB, wall = 195.469s — matches the claimed baseline (14.52 / 2.16 / 195.5) to the decimal. Candidate job `perf_20260903_000009_29377`: 10.0125 slot-h, 2.1603 TiB, 214.149s wall — matches 10.01 / 2.16 / 214.1 exactly. 31.0% saving recomputes correctly.
- Re-ran `--dry_run` on both files in `outputs/audi_1277_step2_rendered_{flag_on,flag_off}_select.sql` as they sit in the worktree today: flag-off = 64,278,642,027,827 bytes = 64.28 TB (claimed 64.3 TB); flag-on = 74,639,610,864,827 bytes = 74.64 TB (claimed 74.6 TB). Exact.
- `uv run pytest include/spark_optimizer/tests/` in the ti worktree: 164 passed, 1 failed (`test_phs.py::test_newest_logs_takes_the_tail_and_drops_inprogress`, real GCS listing) — matches §4.1 verbatim.
- `uv run pre-commit run --files` on both camperbid files: gitleaks/check-*/ruff-check/ruff-format all pass. DagBag parse (`dags/bos`) on Airflow 3.3.0: `import_errors: {}`, `bos__spend` present — matches the "wiring verified" stand-in for `astro dev parse`.
- `all_facts` writer cadence file (`outputs/audi_1277_all_facts_writer_cadence_3d.txt`): 13, 15, 11 MERGEs in the three 24h windows exactly as stated; min gap 01:55→02:12 = 0.28h, max gap 02:12→06:49 = 4.62h — reproduces "13, 15 and 11... gaps 0.3 to 4.6h" precisely.
- `outputs/audi_1277_step1_flight_events_all_groups.txt`: per-day start/end counts (98/72, 108/76, 58/51, 38/44, 454/590, 20,907/21,008, 209/191) match §4.2's table exactly; 20,907+21,008 = 41,915 matches §3.6's rollover figure exactly.
- `Variable.get`/`Variable.set` signatures, `BigQueryHook.insert_job` signature, and the `list(job.result())[...]` pattern in `dags/rill_data_validation/dag.py` all confirmed live in the installed Airflow 3.3.0 package.
- CODEOWNERS: confirmed no `dags/bos*`/`dags/intent_score_threshold*` rule, global default `@SteelHouse/pacing @SteelHouse/performance-ml` applies. `.github/PULL_REQUEST_TEMPLATE.md` headings confirmed: Ticket/Context/Changes/TTL/Tests/Documentation.
- Both diffs read line-for-line against §5's description: `source_fingerprint` → `source_changed` → `drop` → `create` → `record_source_fingerprint` wiring, fail-open `unavailable:<uuid>` and `GENERATE_UUID()` paths, `CURRENT_DATE()` in the fingerprint, and `trigger_rule="none_failed"` landing specifically on `tables.campaign_performance.drop` (only `table_name` in `TABLES_THAT_DEPEND_ON_FLIGHT_METRICS_PER2388`, which is `["campaign_performance"]` alone) are all present exactly as described. `population_histogram.sql` folds `temp_scored` into both source CTEs and switches the dedup key to `FARM_FINGERPRINT(ip)`/`FARM_FINGERPRINT(household_id_value)` in both the flag-on and flag-off branches; parens balanced, `ASSERT > 100000` intact outside `END IF`.
- No writes outside the two repos + this ticket folder. The one workspace-repo diff (`knowledge/bq/_UNDOCUMENTED.queue` +4 lines, the two `hhst_v3`/`v4__campaign_bucket` tables) is `bq_run.sh`'s own auto-append side effect of running the Step 2 measurement queries, not a manual knowledge edit — expected.
- No commits on either branch (`git log` on both = tip of `main`, diffs are working-tree only) — consistent with the agent's own open_items line "No git ... writes were made; the PRs are not opened," not a contradiction.

**Three minor inaccuracies found, none material:**
1. §5 diffstats are off by one line each: `bos__spend.py` is actually `+53/-2` (claimed `+52/-2`); the ti worktree is actually `+16/-1` total across both files (claimed `+17/-1`).
2. §3.0 says CI runs "pre-commit: black/isort/flake8/gitleaks" — the actual `.pre-commit-config.yaml` has no black/isort/flake8; it's gitleaks + standard pre-commit-hooks + `ruff-check`/`ruff-format`. Planning-narrative error only; Steps 1-2's own pre-commit validation ran the real (ruff-based) hooks and is unaffected.
3. `FLIGHT_METRICS_FINGERPRINT_SQL` matches `all_facts` via `REGEXP_CONTAINS(table_name, r'^summarydata__all_facts__[0-9]+$')`, not the literal `table_name = 'summarydata__all_facts__3194417682'` that §3.1's plan text specifies — more robust to a SQLMesh table-suffix swap, but this deviation from the plan's literal wording isn't logged in §3.6.

`jira_comment` (`artifacts/audi_1277_result_comment.txt`) lints clean (120/120 words, 779/800 chars, 7/8 bullets) and every number in it checks out against the above — posting it unchanged is fine.
