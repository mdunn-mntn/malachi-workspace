---
doc_type: ticket
title: "AUDI-1316: Show unowned BigQuery spend on the cost dashboard"
status: backlog
date: 2026-09-03
summary: "Mode query over JOBS_BY_PROJECT plus the Mode service-account grant, so unowned slot-hours are visible"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1316: Show unowned BigQuery spend on the cost dashboard

**Jira:** https://mntn.atlassian.net/browse/AUDI-1316
**Status:** backlog
**Date Started:** 2026-09-03
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
Follow-on to AUDI-1278, which measured the unowned BigQuery jobs at 612 jobs and 1,110 slot-hours a day and shipped labels for the airflow-ti share. The Mode cost dashboard reads the optimizer's finding ledger only, so the unowned bucket never reaches it; AUDI-1278 chose the daily report as its measurement surface instead. This ticket is that report's dashboard half.

## 2. The Problem
What exactly is broken, unclear, or needed? Include:
- Symptoms observed
- Who reported it / who it affects
- Impact (data quality, revenue, user experience, etc.)

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
