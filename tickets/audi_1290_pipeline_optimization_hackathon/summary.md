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
1. `/sprint --next` pulled the 13 issues, scaffolded the epic and child folders, and locked §0 Framing on all 13 in one batched gate (2026-09-02).
2. Plan wave: one agent per ticket wrote §3 Plan of Action and returned its open decisions. Plans stay in `summary.md`; nothing is posted to Jira at that stage (user's call 2026-09-02).
3. Execute wave: fresh agents per ticket, then an adversarial verifier per result. Two waves were cut off by session limits and one agent hung; each was re-dispatched from its partial worktree and ticket state.
4. Landing (dispatcher only, serial per ticket): commit the ticket folder, post the Jira comment, transition to In Progress, commit the code branch, run `/pr_gauntlet`, open the PR, record it, then `/capture` scoped to that ticket.

## 4. Investigation & Findings

Per-ticket outcome, 2026-09-03:

| Ticket | Result | PR |
|---|---|---|
| AUDI-1269 | 6 of 9 spill DAGs resized; 2 pulled by the per-DAG gate, 1 dropped for driver out-of-memory history | airflow-ti #1273 |
| AUDI-1270 | 1 of 15 is shuffle-side (vertical_size_monitor 128 to 600); 9 handed to the AUDI-1273 mechanism | airflow-ti #1275 |
| AUDI-1271 | Spec refuted on its own kill criterion: the change costs about 17 DCU-hours a run to save 0.1 executor-hours | none, closed with no change |
| AUDI-1272 | 2 of 10 confirmed (advertiser_mid 90, ipdsc_42_monitor 7); 8 unchanged | airflow-ti #1281 |
| AUDI-1273 | 2 of 3 shipped; ipdsc_ds_67 dropped, its input files cannot be split | airflow-ti #1272 |
| AUDI-1274 | Both pivot models cap the adaptive merge at 16 MiB | airflow-ti #1270 |
| AUDI-1275 | Speculation proven safe for 11 of 13 writers; canary on site_network_hourly, owner ask drafted | airflow-ti #1271 |
| AUDI-1276 | Skew is a plan-time shuffle from a stats-less JDBC join; broadcast hints plus one-pass monitor SQL | airflow-ti #1276 |
| AUDI-1277 | Profiler double-count fixed, skip gate halves the heaviest rebuild, histogram 31% cheaper | airflow-ti #1277, camperbid #580 |
| AUDI-1278 | The unattributed third of BigQuery spend is four camperbid Spark scripts; airflow-ti labels shipped | airflow-ti #1278 |
| AUDI-1279 | Per-batch OpenAI status lines and a dead-cohort alarm | shopper_graph #305 |
| AUDI-1280 | 32 of 67 alerting DAGs were unwatched; one tag fixes 25, CI blocks the next miss | airflow-ti #1274 |
| AUDI-1281 | Regression guard flags a seeded 2x spill and fetch-wait on two pipelines | airflow-ti #1279 |

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
