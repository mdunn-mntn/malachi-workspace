---
doc_type: ticket
title: "AUDI-1316: Show unowned BigQuery spend on the cost dashboard"
status: backlog
date: 2026-09-03
summary: "Mode query over JOBS_BY_PROJECT plus the Mode service-account grant, so unowned slot-hours are visible"
result: "not started"
question: "Can the cost dashboard show unowned BigQuery slot-hours per day, and what grant does the Mode service account need to read them?"
framing_state: locked
---

# AUDI-1316: Show unowned BigQuery spend on the cost dashboard

**Jira:** https://mntn.atlassian.net/browse/AUDI-1316
**Status:** backlog
**Date Started:** 2026-09-03
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-03 by the dispatcher from the ticket description and the AUDI-1278/1281 records it follows.
- **Question (the unknown):** Can the cost dashboard show unowned BigQuery slot-hours per day, and what grant does the Mode service account need to read them?
- **Goal (why / the decision):** AUDI-1278 measured 612 unowned jobs and 1,110 slot-hours a day but its measurement surface is a daily file nobody opens; the dashboard is where cost is actually read. Cost-reduction lever under epic AUDI-1290.
- **Objective (done-when):** A Mode section on report e81786de8403 showing unowned slot-hours per day by submitter, backed by a merged mntn-devops grant PR; the number falls as the AUDI-1278 labels merge.
- **Approach (how):** Verify the Mode connection's actual service account and its current denial on dw-main-bronze INFORMATION_SCHEMA.JOBS_BY_PROJECT before writing anything; mirror the spark-optimizer grant pattern in mntn-devops (crossplane ProjectIAMMember, sync-wave 3); draft the Mode SQL in artifacts/ and validate it with bq_run.sh under a date filter and a LIMIT; the Mode report itself is edited in the UI, so this ticket ships a grant PR plus a drafted query, not a Mode PR.
- **What would change the answer:** If the Mode connection already holds the read, the grant PR is unnecessary and the ticket is just the query. If Mode's principal is a user rather than a service account, the grant target changes and the devops ask has to name it.

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
