---
doc_type: ticket
title: "AUDI-1278: Label python-client BigQuery jobs for cost attribution"
status: backlog
date: 2026-09-02
summary: "Add airflow-dag/airflow-task labels to python-client BQ submits so every job is attributed"
result: "not started"
question: "Which submitters produce the roughly 600 unlabeled BigQuery jobs a day (1,185 slot-hours), and does adding airflow-dag and airflow-task labels in the python client attribute them?"
framing_state: locked
---

# AUDI-1278: Label python-client BigQuery jobs for cost attribution

**Jira:** https://mntn.atlassian.net/browse/AUDI-1278
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Which submitters produce the roughly 600 unlabeled BigQuery jobs a day (1,185 slot-hours), and does adding airflow-dag and airflow-task labels in the python client attribute them?
- **Goal (why / the decision):** The 2026-09-02 finding that the ledger's unattributed bucket is empty means the 1,185 figure may lie outside the airflow-launched set; settle that first. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A table in outputs/ of unlabeled jobs by principal, client and query fingerprint joined against the ledger population, PRs adding labels where the submitter is ours, and the unattributed share down on the Mode optimizer BQ report.
- **Approach (how):** JOBS_BY_PROJECT via bq_run.sh where labels are missing, grouped by user_email and job pattern; join to the optimizer ledger population; add labels via the python client's job_config.labels where airflow-ti submits.
- **What would change the answer:** The unlabeled jobs are not submitted from airflow-ti at all (Mode, humans, another service); then the deliverable is the attribution table and an owner hand-off, no PR.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

About 600 BigQuery jobs a day (1,185 slot-hours) show up with no owner on the [cost dashboard](https://app.mode.com/mntn/reports/e81786de8403), so a third of BQ spend cannot be traced to a pipeline.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** jobs submitted through the python client carry no labels, while Airflow-submitted jobs are labeled with their DAG and task automatically.

**Task:** add airflow-dag and airflow-task labels where the python client submits BQ jobs, so every job is attributed.

**Done-when:** the unattributed share drops on the [optimizer BQ report](https://app.mode.com/mntn/reports/e81786de8403).

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
