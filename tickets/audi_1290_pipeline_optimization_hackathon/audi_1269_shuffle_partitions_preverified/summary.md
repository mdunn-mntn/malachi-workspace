---
doc_type: ticket
title: "AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs"
status: backlog
date: 2026-09-02
summary: "Config-only: raise spark.sql.shuffle.partitions on 9 spilling DAGs, values pre-verified 08-27"
result: "not started"
question: "Does raising spark.sql.shuffle.partitions to the 08-27 sweep's computed value on the 9 named DAGs stop their shuffle-side spill without changing outputs or failing the run?"
framing_state: locked
---

# AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1269
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does raising spark.sql.shuffle.partitions to the 08-27 sweep's computed value on the 9 named DAGs stop their shuffle-side spill without changing outputs or failing the run?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** One airflow-ti PR (branch AUDI-1269) merged that sets the 9 values (in BOTH decorator and builder where both exist, builder wins) and regenerates dags/model_task_config.json; the ledger marks each spill finding resolved after 3 quiet sweeps.
- **Approach (how):** Read each model on airflow-ti main and confirm where the value is set today; apply the sweep values from audi_1194_hackathon_optimizations_2026_08_27.md; check the event log for AQE coalescing that would make the knob a no-op (the 1274 mechanism); model_upload.py --dryrun for the config; stamp `ledger applied` on merge.
- **What would change the answer:** A DAG whose spill is map-side (moves to the 1273 mechanism), whose AQE coalesces partitions back (the 1274 mechanism), or whose model was deleted on main.

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
