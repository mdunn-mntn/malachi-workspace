---
doc_type: ticket
title: "AUDI-1281: Perf-regression guard POC from optimizer metrics"
status: backlog
date: 2026-09-02
summary: "CI check that fails when a model's spill or fetch-wait doubles vs its 30-day optimizer baseline"
result: "not started"
question: "Can a CI check compare a model's latest spill and shuffle-fetch-wait against its own 30-day baseline from optimizer metrics and fail on a 2x regression?"
framing_state: locked
---

# AUDI-1281: Perf-regression guard POC from optimizer metrics

**Jira:** https://mntn.atlassian.net/browse/AUDI-1281
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Can a CI check compare a model's latest spill and shuffle-fetch-wait against its own 30-day baseline from optimizer metrics and fail on a 2x regression?
- **Goal (why / the decision):** POC for Bryce's pipeline-testing-framework track. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A POC script plus test in airflow-ti (branch AUDI-1281) that reads the optimizer ledger or GCS outputs, computes a 30-day per-stage baseline for one critical pipeline, and flags a seeded 2x regression; run demonstrated against that pipeline.
- **Approach (how):** Metric source is include/spark_optimizer ledger rows and per-sweep outputs in gs://mntn-data-archive-prod/optimizer/; baseline = 30-day median per (dag, stage, metric); pipeline chosen from chronic ledger findings (intent_score_map or fangorn_score_monitor unless the user says otherwise); seeded regression = synthetic row at 2x.
- **What would change the answer:** Run-to-run noise above 50% CV on the chosen metric, in which case a fixed 2x threshold is wrong and the POC reports the adaptive threshold it needs instead.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Catch pipeline slowdowns in CI before they reach prod, using measurements the optimizer already collects. POC for the testing-framework track.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the [optimizer](https://github.com/SteelHouse/airflow-ti/blob/main/include/spark_optimizer/) records disk spill and network wait per stage for every run. A job that doubles against its own 30-day baseline is a real regression nobody sees today.

**Task:** build a check that fails CI when a model's spill or fetch-wait doubles vs its 30-day baseline; run it against one critical pipeline.

**Done-when:** POC runs against one critical pipeline and flags a seeded regression.

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
