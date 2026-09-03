---
doc_type: decision
title: "0007 — Unattributed BigQuery spend is measured on the daily optimizer_bq report; camperbid labels ship as two Spark properties via owner hand-off"
summary: "The optimizer's unattributed BQ bucket stays out of the ledger and off Mode; before/after for AUDI-1278 is the unattributed row of gs://mntn-data-archive-prod/optimizer/optimizer_bq_<date>.md (baseline 08-28..09-01: 606 jobs/day, 1,104.7 slot-h/day). The 97% that is camperbid Spark-BigQuery-connector reads gets labels from spark.datasource.bigquery.bigQueryJobLabel.* in dag_utils/google.py, owned and merged by pacing/performance-ml, not by an airflow-ti PR"
status: accepted
date: 2026-09-02
last_verified: 2026-09-03
keywords: [AUDI-1278, AUDI-1290, unattributed bq jobs, optimizer_bq report, optimization_ledger surface bq, bq_profile unattributed, measurement surface, Mode e81786de8403, bigquery.resourceViewer, mode-analytics, bigQueryJobLabel, spark.datasource.bigquery, airflow-camperbid dag_utils google.py, bos__spend, camperbid handoff, airflow-ti 1278, airflow_job_labels, test_heavy_task_is_a_finding_and_unattributed_is_not]
supersedes: null
tags: [airflow-optimizer, bigquery, attribution, camperbid]
---

# 0007 — Unattributed BigQuery spend is measured on the daily optimizer_bq report; camperbid labels ship as two Spark properties via owner hand-off

## Context
AUDI-1278 (hackathon epic AUDI-1290) asked which submitters produce the ~600 unlabeled BigQuery jobs a day (1,185 slot-h)
and whether python-client labels attribute them. The Jira Done-when says "the unattributed share drops on the optimizer BQ
report" (Mode `e81786de8403`), but that report's "BigQuery cost by task" query reads
`mntn-prj-prod-00.optimizer.optimization_ledger WHERE surface = 'bq'`, and the ledger never holds the unattributed bucket:
`bq_profile.reports()` attaches no finding to it and `ledger.record()` skips finding-less reports, pinned by
`test_heavy_task_is_a_finding_and_unattributed_is_not`. The bucket exists only in the daily GCS report `optimizer_bq_<date>.md`
(from 2026-08-28), whose unattributed row reconciles to `INFORMATION_SCHEMA.JOBS_BY_PROJECT` within rounding on every day
checked. 96.9% of the bucket's slot-hours are four `bos__spend` Spark scripts in `SteelHouse/airflow-camperbid` reading through
the Spark-BigQuery connector, which submits its own query jobs with no labels; airflow-ti's python-client sites are 147 jobs/day
and 3.9 slot-h/day.

## Decision
D1 (user, 2026-09-02): measure before/after on the daily report's unattributed row (7-day means of jobs and slot-h; baseline
08-28..09-01 = 606 jobs/day, 1,104.7 slot-h/day), no ledger or Mode change. D2: the camperbid fix is two Jinja-rendered Spark
properties in `dag_utils/google.py` (`spark.datasource.bigquery.bigQueryJobLabel.airflow-dag = {{ dag.dag_id | lower }}`,
`...airflow-task = {{ task.task_id | lower | replace('.', '-') }}`, on both the Serverless batch path and the workflow-template
path), handed to `@SteelHouse/pacing` and `@SteelHouse/performance-ml` (CODEOWNERS) with a dev validation query; we do not
open a PR in their repo. D3: airflow-ti labels go on all 8 python-client sites in
[#1278](https://github.com/SteelHouse/airflow-ti/pull/1278), including the three `get_df` DAGs that do not bill in dw-main-bronze.

## Alternatives considered
- **A: a Mode query "BigQuery jobs by attribution" over `JOBS_BY_PROJECT`** — not taken: needs `bigquery.jobs.listAll`
  (`roles/bigquery.resourceViewer`) for `mode-analytics@dw-main-bronze` via a mntn-devops PR (`iam_bronze_extras.tf` line 158
  grants only the layer reader role). Recorded as an open option for the user.
- **B: record the unattributed bucket as a ledger row** — rejected: reverses a deliberate design ("a fix cannot be filed against
  a job no dag will admit to") and flips a pinned test.
- **Edit the camperbid Spark scripts, or pass labels per read** — rejected: the connector reads `bigQueryJobLabel.*` from Spark
  conf, so two properties cover every connector read (`bigquery_load_query`, `_v2`, `bigquery_load_table`, win_rate_hourly's
  private `_read_bigquery`) with no script edits.
- **Open the camperbid PR ourselves** — rejected: another team's repo and deployment; the owner's one dev batch is also the
  remaining runtime confirmation that the 0.42-line connector honours the conf-prefixed labels.

## Consequences
- After the airflow-ti merge the report's unattributed row drops by ~147 jobs/day and ~4 slot-h/day; after the camperbid merge
  it falls to ~24 jobs/day and <1 slot-h/day, and `bos__spend` rises from ~1,630 to ~2,700 slot-h/day with four new
  `bq_heavy_task` findings. That is attribution moving, not new spend: no `ledger applied` provenance stamp, and the Mode
  savings numbers should not move.
- The residual (autotof kedro job, `bvp_data_refresh_v7.py::load_bq`, the hhst_v# MERGE, 7 LOAD jobs/day) stays unattributed
  until the owners label those python-client sites.
- Re-run the attribution query for 7 post-merge days after each merge and record the after column in the ticket.
- **Affected knowledge docs:** [`../memory/reference_bq_job_attribution.md`](../memory/reference_bq_job_attribution.md),
  [`../memory/project_airflow_optimizer.md`](../memory/project_airflow_optimizer.md) § 2026-09-03 AUDI-1278,
  [`../memory/reference_mode_api.md`](../memory/reference_mode_api.md), [`../data_knowledge.md`](../data_knowledge.md)
  § BigQuery Behavioral Gotchas, ticket `tickets/audi_1290_pipeline_optimization_hackathon/audi_1278_bq_job_labels/summary.md`
  §5 / §8.

## Superseded Note (2026-09-03, AUDI-1316)

Alternative A ("a Mode query over JOBS_BY_PROJECT") is **now available with no new grant**. `mode-analytics@dw-main-bronze` already holds `bigquery.jobs.listAll` and `bigquery.jobs.create` via its `medallion_bronze_reader` role, confirmed by live project policy + role permission list. The query is written, validated (0.178 GB per day), and ready for paste into report `e81786de8403`. This does NOT affect the D1/D2/D3 decisions on the unattributed bucket's measurement surface or the camperbid hand-off — those stand.
