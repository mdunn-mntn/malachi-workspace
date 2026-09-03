---
doc_type: ticket
title: "AUDI-1278: Label python-client BigQuery jobs for cost attribution"
status: backlog
date: 2026-09-02
summary: "Add airflow-dag/airflow-task labels to python-client BQ submits so every job is attributed"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1278: Label python-client BigQuery jobs for cost attribution

**Jira:** https://mntn.atlassian.net/browse/AUDI-1278
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** {the single, falsifiable question — a stranger could tell whether it's been answered}
- **Goal (why / the decision):** {the decision or outcome the answer serves + who's waiting on it + north-star tie}
- **Objective (done-when):** {the concrete deliverable + the bar that closes it — binary: it exists and clears the bar, or it doesn't}
- **Approach (how):** {data sources, method/protocol, and the key assumptions to resolve empirically first}
- **What would change the answer:** {the smallest result that flips the conclusion — the kill criteria that keep scope honest}

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
