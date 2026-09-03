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
Numbered steps of the approach taken. Updated as the plan evolves.
1. Step one
2. Step two
3. ...

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
