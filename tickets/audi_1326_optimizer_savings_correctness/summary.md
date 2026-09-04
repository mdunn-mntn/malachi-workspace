---
doc_type: ticket
title: "AUDI-1326: Fix the optimizer savings figure and the retry that mass-resolves findings"
status: backlog
date: 2026-09-04
summary: "Gate the savings estimate, fix the retry grace window, fix digest buckets"
result: "not started"
question: "Can the optimizer's savings figure and resolution rule be made to say only what the data supports, so a retried sweep changes nothing and no dollar figure appears without evidence?"
framing_state: locked
---

# AUDI-1326: Fix the optimizer savings figure and the retry that mass-resolves findings

**Jira:** https://mntn.atlassian.net/browse/AUDI-1326
**Status:** backlog
**Date Started:** 2026-09-04
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Can the optimizer's published savings figure and its resolution rule be made to say only what the data supports, so that a retried sweep changes nothing and no dollar figure appears without evidence behind it?
- **Goal (why / the decision):** The digest is a leadership-facing surface. It published "115 hours all-time, ~$32 all-time" on 2026-09-04 while all 60 fixes shipped 09-03 were still `watching` with zero days observed. Until this is right, every downstream measurement (AUDI-1328) and every claim about the tools' value rests on a number nobody can defend. Malachi is waiting on it; it gates AUDI-1328.
- **Objective (done-when):** A simulated retry of a sweep resolves zero keys beyond the first try; the digest names the shipped PR against every finding carrying `fix_pr`; the savings surface renders an interval or states it lacks evidence; the `recurring` state is bucketed. Each of the four has a test that fails on origin/main and passes after.
- **Approach (how):** Design each fix against the live prod ledger copy (1692 rows) rather than from code reading alone, simulating before/after numbers for each. Four defects, four patches, one PR through /pr_gauntlet. Assumptions to resolve empirically first: the correct grace-window boundary given that a sweep writes rows dated the previous day; the minimum n per DAG that makes the savings estimate stable, derived from the ledger's own per-DAG daily variance rather than picked.
- **What would change the answer:** If the per-DAG variance in the ledger turns out low enough that n=1 is defensible, the savings gate is unnecessary and only the retry and digest defects remain. If the retry path cannot actually persist the ledger and then fail (verify the upload order in `sweep.run()`), the 209-key mass-resolve is not reachable in prod and drops in priority.

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
