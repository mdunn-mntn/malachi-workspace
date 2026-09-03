---
doc_type: ticket
title: "AUDI-1317: Publish pipeline regressions to the cost dashboard"
status: backlog
date: 2026-09-03
summary: "Write the AUDI-1281 guard's regressions to the finding ledger and render them on Mode"
result: "not started"
question: "Can the daily sweep write the regression guard's verdicts to the finding ledger so a doubling of spill or fetch wait renders on the dashboard and in the digest?"
framing_state: locked
---

# AUDI-1317: Publish pipeline regressions to the cost dashboard

**Jira:** https://mntn.atlassian.net/browse/AUDI-1317
**Status:** backlog
**Date Started:** 2026-09-03
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-03 by the dispatcher from the ticket description and the AUDI-1278/1281 records it follows.
- **Question (the unknown):** Can the daily sweep write the regression guard's verdicts to the finding ledger so a doubling of spill or fetch wait renders on the dashboard and in the digest?
- **Goal (why / the decision):** AUDI-1281 built the guard and shipped no publisher, so a real regression is still invisible the morning it happens. Reliability lever under epic AUDI-1290, and the sweep-side option needs no new credentials.
- **Objective (done-when):** Merged airflow-ti change: the sweep runs the guard over every profiled DAG, writes each regression as its own ledger key, and the digest carries a regression line; a seeded regression appears in both and clears when the job recovers.
- **Approach (how):** Branch stacks on audi-1281-perf-regression-guard (PR #1279, unmerged) and the PR targets that branch, not main, until it lands; reuse the guard's own evaluate() rather than reimplementing thresholds; the new key follows the ledger's existing new/recurring/chronic/resolved replay so a recovered job resolves itself; test with the repo's own suite and a seeded row.
- **What would change the answer:** If the guard's verdicts prove noisy across the fleet (many DAGs flagged on a normal day), the publisher gates to chronic-only or the ticket reports the noise floor instead of shipping a firehose.

## 1. Introduction
Follow-on to AUDI-1281, whose regression guard flags a doubling of disk spill or shuffle-fetch wait against a job's own 30-day median but publishes nowhere. Its plan recorded two options for a live gate and shipped neither; this ticket takes the sweep-side one, which needs no new credentials.

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
