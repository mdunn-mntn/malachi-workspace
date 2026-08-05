---
doc_type: ticket
title: "AUDI-1194: Airflow/Spark optimization crawler"
status: backlog
date: 2026-08-05
summary: "Scheduled efficiency sweep over succeeded Airflow DAGs (both engines); split from AUDI-1191 debugger"
result: "not started — split from AUDI-1191 (debugger); optimizer half mostly built (eventlog/optimizations/optimize/crawl + weekly cron), needs productionizing"
question: "Can a scheduled key-free crawler read every succeeded Spark job (Dataproc event logs + Databricks plans/metrics) and emit a ranked, actionable optimization backlog with no manual step?"
framing_state: locked
---

# AUDI-1194: Airflow/Spark optimization crawler

**Jira:** https://mntn.atlassian.net/browse/AUDI-1194
**Status:** backlog
**Date Started:** 2026-08-05
**Assignee:** Malachi

---
## 0. Framing
Split from AUDI-1191 (which keeps the failure debugger). This ticket is the OPTIMIZER: the success-only, scheduled efficiency workflow. Distinct trigger (a job succeeds, not fails), distinct schedule, distinct deliverable. Shares only the event-log parser with the debugger.
- **Question (the unknown):** Can a scheduled, key-free crawler read every succeeded Spark job across both engines (Dataproc event logs + Databricks EXPLAIN COST plans/metrics) and emit a ranked, actionable optimization backlog with no manual step?
- **Goal (why / the decision):** Cut Spark compute cost and wall-clock across the whole airflow-ti fleet by surfacing inefficiencies automatically, replacing the departed framework author's tribal knowledge. The ranked backlog tells owners (DDP/ML/TPA) which jobs to fix first. North-star tie: cost-reduction lever (Medium) + velocity/bus-factor win — not the top incrementality bet (honest tier). Proof it works: the crawl already found a 242x prod skew (IMP-024).
- **Objective (done-when):** A scheduled crawler that, with no manual step, scans every succeeded Spark job across both engines — **full fleet including the ipdsc/tpa PHS logs** — and emits a ranked cross-job optimization backlog (worst-first, per-finding fix grouped CODE/INFRA/FAILURE). Done when it runs on a schedule and produces that full-fleet backlog automatically. Owner adoption is a separate outcome, not the close bar.
- **Approach (how):** Reuse the built modules (`eventlog` 7-surface parser, `optimizations` detectors, `optimize`, `crawl`). Acquisition per engine: batch-operator Dataproc fleet → event logs in `gs://mntn-data-archive-{env}/spark-events` (accessible); ipdsc/tpa → PHS temp-bucket, per-batch-uuid, via Dataproc batch-enumeration → uuid → `spark-job-history` (needs a standing GCS read grant, currently blocked); Databricks → `EXPLAIN COST` plan + Spark job metrics via `jobs get-run-output`. Measure the cost of one full sweep, then set cadence: **daily if cheap, weekly if expensive**. Deliver the ranked backlog as a file in `outputs/` (auto-post to owners deferred). Assumptions to resolve first: (1) standing GCS read on `dataproc-temp-us-central1-995798185124-svhwvc6j` (blocker for the PHS subset — no standing `storage.objects.list` today; interim read = the `dataproc-debug` PAM bundle, self-service ~1h, but the cron needs a standing `roles/storage.objectViewer` + `roles/dataproc.viewer` grant via mountain-devops → Christina); (2) efficient Dataproc batch-enumeration for the scattered per-uuid PHS logs; (3) validate the live Databricks `EXPLAIN COST` acquisition path.
- **What would change the answer:** If the findings are mostly false positives / not actionable (owners don't act), or the event-log/plan data can't be reached key-free at fleet scale, the "check every DAG automatically" premise fails and it degrades to a manual/on-request tool. Also: if a full daily sweep is too expensive AND weekly misses too much, the cadence model needs rethinking.

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
