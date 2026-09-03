---
doc_type: epic
title: "AUDI-1290: Pipeline Optimization Hackathon"
status: backlog
date: 2026-09-02
summary: "Fall tech-debt hackathon: cut Spark and BQ waste found by the optimizer, close Aug alerting gaps"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1290: Pipeline Optimization Hackathon

**Jira:** https://mntn.atlassian.net/browse/AUDI-1290
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
Bryce's fall tech-debt hackathon, sprint 8649 (2026-09-07 to 2026-09-21), board 1814. Three tracks: alerting audit, pipeline testing framework, pipeline optimization audit. Findings come from the AUDI-1194 optimizer's 2026-08-27 full-corpus sweep (67 finding pairs, 30,163 exec-h at stake) plus the live BigQuery cost surface. Savings auto-measure on the optimizer ledger and the Mode cost dashboard https://app.mode.com/mntn/reports/e81786de8403.

Children:
- AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs (`audi_1269_shuffle_partitions_preverified/`)
- AUDI-1270: Verify event logs then raise shuffle.partitions on 15 spill DAGs (`audi_1270_shuffle_partitions_verify_first/`)
- AUDI-1271: Raise initialExecutors on 2 pre-verified fetch-wait DAGs (`audi_1271_initial_executors_preverified/`)
- AUDI-1272: Verify map-output spread then raise initialExecutors on 10 fetch-wait DAGs (`audi_1272_initial_executors_verify_first/`)
- AUDI-1273: Lower files.maxPartitionBytes on 3 map-side-spill DAGs (`audi_1273_max_partition_bytes/`)
- AUDI-1274: Set AQE advisoryPartitionSizeInBytes=16m on the 2 pivot DAGs (`audi_1274_aqe_advisory_pivot/`)
- AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs (`audi_1275_straggler_gcs_writers/`)
- AUDI-1276: Confirm joins and fix skew on 4 DAGs (`audi_1276_join_skew/`)
- AUDI-1277: Tune the 2 heaviest BigQuery query shapes (`audi_1277_bq_heavy_queries/`)
- AUDI-1278: Label python-client BigQuery jobs for cost attribution (`audi_1278_bq_job_labels/`)
- AUDI-1279: OpenAI batch pipeline observability: dead-cohort alarm and status logging (`audi_1279_openai_batch_observability/`)
- AUDI-1280: Debugger alerting tag coverage: fleet audit and CI check (`audi_1280_debugger_tag_coverage_ci/`)
- AUDI-1281: Perf-regression guard POC from optimizer metrics (`audi_1281_perf_regression_guard/`)

## 2. The Problem
Tickets grouped by change type, not by DAG: the same config change across many DAGs is one ticket. Optimization tickets (1269-1276) are airflow-ti model config PRs; 1277-1278 are BigQuery; 1279 is shopper_graph; 1280-1281 are debugger/optimizer tooling. Every optimization ticket closes on 'PR merged; optimizer ledger shows the finding resolved'.

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
