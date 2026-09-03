---
doc_type: ticket
title: "AUDI-1270: Verify event logs then raise shuffle.partitions on 15 spill DAGs"
status: backlog
date: 2026-09-02
summary: "Per DAG confirm shuffle-side spill in the event log, then size partitions to ~256 MiB per task"
result: "not started"
question: "For each of the 15 DAGs, is the spilling stage's spill shuffle-side, and what partition count puts about 256 MiB per task in memory?"
framing_state: locked
---

# AUDI-1270: Verify event logs then raise shuffle.partitions on 15 spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1270
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** For each of the 15 DAGs, is the spilling stage's spill shuffle-side, and what partition count puts about 256 MiB per task in memory?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A per-DAG verdict table in outputs/ (stage, spill side, current partitions, shuffle bytes, target partitions) and one PR (branch AUDI-1270) applying the targets to every DAG confirmed shuffle-side; DAGs that are not shuffle-side are handed to the right mechanism in §8.
- **Approach (how):** Event logs from gs://mntn-data-archive-prod/spark-events for the batch fleet and from the PHS temp bucket under the PAM grant for ipdsc jobs; parse with include/spark_optimizer/eventlog.py; target = shuffle write bytes / 256 MiB, rounded up; verify current values on main.
- **What would change the answer:** Spill is input-side (1273 mechanism) or AQE re-coalesces (1274 mechanism); a DAG with no recent event log cannot be verified and is left out, not guessed.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Same disk-overflow fix as AUDI-1269 for 15 more jobs, but each needs a short check of its Spark event log first to confirm the right knob.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** overflow (spill) can happen while shuffling data or while reading input; each has a different fix. The event log shows which one a job has.

**Task:** per DAG open the spilling stage in the event log, confirm the spill is shuffle-side, then set the partition count so each task holds about 256 MiB in memory:
- [fangorn_prospecting_scoring](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/fangorn_prospecting_scoring.py)
- [ipdsc_ds_17](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_17.py)
- [ipdsc_46_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_46_monitor.py), [ipdsc_14_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_14_monitor.py), [ipdsc_49_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_49_monitor.py)
- [ipdsc_ds_13](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_13.py), [ipdsc_ds_14](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_14.py), [ipdsc_ds_47](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_47.py)
- [fangorn_predictions_vertical](https://github.com/SteelHouse/airflow-ti/blob/main/models/machine_learning/fangorn_predictions_vertical.py), [fangorn_household_predictions_vertical](https://github.com/SteelHouse/airflow-ti/blob/main/models/machine_learning/fangorn_household_predictions_vertical.py)
- [vertical_size_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/vertical_size_monitor.py)
- [aug_log_ip](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/aug_log_ip.py)
- [guid_log_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py) (stage 13; its stage-1 fix is in AUDI-1269)
- [guid_log_pivot_household_id_vertical_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_3_pivoted/guid_log_pivot_household_id_vertical_id.py)
- [advertiser_join](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/advertiser_join.py)

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
