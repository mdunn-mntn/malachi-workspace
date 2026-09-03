---
doc_type: ticket
title: "AUDI-1272: Verify map-output spread then raise initialExecutors on 10 fetch-wait DAGs"
status: backlog
date: 2026-09-02
summary: "Per DAG confirm map output sits on few executors, then raise initialExecutors"
result: "not started"
question: "For each of the 10 DAGs, does the slow-fetch stage's map output sit on the few executors the job started with, and what initialExecutors value spreads it?"
framing_state: locked
---

# AUDI-1272: Verify map-output spread then raise initialExecutors on 10 fetch-wait DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1272
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** For each of the 10 DAGs, does the slow-fetch stage's map output sit on the few executors the job started with, and what initialExecutors value spreads it?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A per-DAG verdict table in outputs/ (stage, executors holding 90% of map output, hottest share, current and target initialExecutors) and one PR (branch AUDI-1272) raising initialExecutors on every DAG where concentration is confirmed.
- **Approach (how):** Event logs as in 1270; run tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_shuffle_concentration.py per log; confirm maxExecutors caps allow the target; regenerate model_task_config.json.
- **What would change the answer:** Map output already spread across most executors, meaning the fetch wait has another cause; that DAG gets no change and the cause goes in §8.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Ten more jobs show the same waiting-to-copy-data symptom as AUDI-1271; confirm the cause per job before raising the starting machine count.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the fix only helps when the wait comes from early output crowded onto the few machines the job started with. The event log shows where that output sits.

**Task:** per DAG check the map-output spread in the event log, then raise `spark.dynamicAllocation.initialExecutors`:
- [advertiser_mid](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/advertiser_mid.py)
- [ipdsc_42_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_42_monitor.py)
- [tpa_export_enrich](https://github.com/SteelHouse/airflow-ti/blob/main/models/tpa_export/tpa_export_enrich.py), [tpa_mntn_id_export](https://github.com/SteelHouse/airflow-ti/blob/main/models/tpa_export/tpa_mntn_id_export.py)
- [audience_intent_scoring_staging (ds46 task)](https://github.com/SteelHouse/airflow-ti/blob/main/spark/machine_learning/audience_intent_scoring_staging_spark.py)
- [ipdsc_ds_46](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_46.py)
- [aug_log_ip_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/aug_log_ip_hourly.py)
- [vertical_size_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/vertical_size_monitor.py)
- [guid_log_derived_household_id_vertical_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_2_derived/guid_log_derived_household_id_vertical_id.py)
- [site_visit_signal_derived_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_2_derived/site_visit_signal_derived_advertiser_id_dsc_id.py)

**Done-when:** PR merged; optimizer ledger shows the finding resolved (savings auto-measure).

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
