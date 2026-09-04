---
doc_type: ticket
title: "AUDI-1329: Measure detector and fleet coverage"
status: in_progress
date: 2026-09-04
summary: "Detector taxonomy gaps plus what fraction of the fleet is ever scanned"
result: "not started"
question: "What share of our failures does the debugger catch, and what share of our fleet does the optimizer scan, with the uncovered classes and jobs named?"
framing_state: locked
---

# AUDI-1329: Measure detector and fleet coverage

**Jira:** https://mntn.atlassian.net/browse/AUDI-1329
**Status:** backlog
**Date Started:** 2026-09-04
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** What share of our failures does the debugger actually catch, and what
  share of our fleet does the optimizer actually scan, expressed as two numbers with the uncovered
  classes and the uncovered jobs named?
- **Goal (why / the decision):** "It covers everything" is currently an assertion. IMP-104 is the
  standing counterexample: `site_network_hourly` loses whole hours to FetchFailed storms, reports
  SUCCEEDED, and passes both tools clean. Until coverage is a number, nobody can say whether the
  next silent failure is an outlier or the norm, and AUDI-1325's adoption case cannot be made to
  another team.
- **Objective (done-when):** A coverage number exists for each axis, with the uncovered classes and
  the uncovered jobs listed by name, and the ceiling that retention imposes stated separately from
  the ceiling our own code imposes.
- **Approach (how):** Detector axis: build a taxonomy of failure and inefficiency classes actually
  observed, then map each to the detector that catches it; the residue is the gap. Fleet axis: count
  what fraction of runs are ever scanned and establish what the event-log retention caps it at.
  Sources: the 3,627 real task logs under `on-call/airflow_logs/`, the optimizer's published sweeps
  in `gs://mntn-data-archive-prod/optimizer/`, the ledger, and the Airflow REST API for the
  denominator. Assumptions to resolve empirically first: whether a denominator of all runs is even
  obtainable, and whether "scanned" means the crawl saw the job or the detectors ran on it.
- **What would change the answer:** If the uncovered residue is dominated by classes that cannot be
  detected from the artifacts we retain, the deliverable is a retention argument, not a detector
  backlog. If the fleet denominator turns out unobtainable, the fleet axis becomes a bound rather
  than a number and must say so.

## 1. Introduction
Brief context: what system/feature/data is involved, and why this ticket exists.

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
