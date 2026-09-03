---
doc_type: ticket
title: "AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs"
status: backlog
date: 2026-09-02
summary: "Config-only: raise spark.sql.shuffle.partitions on 9 spilling DAGs, values pre-verified 08-27"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1269
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
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Speed up 9 Spark jobs that waste hours writing overflow data to disk. Config-only, exact values already computed and verified.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** Spark splits shuffle work into a fixed number of chunks (`spark.sql.shuffle.partitions`). These jobs use too few, so each chunk outgrows memory and overflows to disk (spill), which is slow.

**Task:** raise the setting per DAG:
- [intent_score_map](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/intent_score_map.py) 4915 -> 40960, in BOTH builder (L89) and decorator (L50)
- [ipdsc_ds_2](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_2.py) 2048 -> 8192 (decorator L12)
- [advertiser_score_distribution_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/advertiser_score_distribution_monitor.py) ~916
- [conversion_log_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/conversion_log_advertiser_id_dsc_id.py) ~3508
- [site_visit_signal_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/site_visit_signal_advertiser_id_dsc_id.py) ~3392
- [guid_log_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py) ~3400
- [ipdsc_third_party_audience_builder](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/third_party_audience_builders/ipdsc_third_party_audience_builder.py) ~2240
- [prospecting_join](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/prospecting_join.py) ~42988
- [household_score_distribution_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/household_score_distribution_monitor.py) ~8896

Decorator changes need `dags/model_task_config.json` regenerated. intent_score_household_map was dropped from this list: its DAG was deleted on main 2026-08-26 (PR 1209).

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
