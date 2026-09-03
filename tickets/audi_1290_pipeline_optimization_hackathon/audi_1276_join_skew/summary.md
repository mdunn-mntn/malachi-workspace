---
doc_type: ticket
title: "AUDI-1276: Confirm joins and fix skew on 4 DAGs"
status: backlog
date: 2026-09-02
summary: "Confirm the skewed stage is a join, then enable AQE skewJoin or salt the hot key on 4 DAGs"
result: "not started"
question: "For each of the 4 DAGs, is the skewed stage a join, and does AQE skewJoin or salting the hot key spread it?"
framing_state: locked
---

# AUDI-1276: Confirm joins and fix skew on 4 DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1276
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** For each of the 4 DAGs, is the skewed stage a join, and does AQE skewJoin or salting the hot key spread it?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A per-DAG verdict table in outputs/ (stage, SQL node, hot key share, current skewJoin setting, chosen fix) and one PR (branch AUDI-1276) applying the fix to every DAG where the skewed stage is a join.
- **Approach (how):** Event log stage to SQL node via include/spark_optimizer/eventlog.py; check whether spark.sql.adaptive.skewJoin.enabled is already on (Spark 3 default) and why it did not fire (broadcast join, non-sort-merge join, thresholds); salt in the model code where AQE cannot help.
- **What would change the answer:** The skewed stage is an aggregation or a window, not a join; that DAG gets the matching fix or none, recorded in §8.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

In 4 jobs one machine gets nearly all the work because a single join key dominates the data (skew); spread that key so the work parallelizes.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** a join groups rows by key. When one key holds most rows, its task runs for hours while the rest idle.

**Task:** per DAG confirm from the event log that the slow stage is a join, then enable AQE skewJoin or salt the hot key:
- [conv_log_ip_advertiser_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/conv_log_ip_advertiser_id.py)
- [guid_log_ip_guid_advertiser_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_ip_guid_advertiser_id.py)
- [guid_log_ip_advertiser_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/guid_log_ip_advertiser_id.py)
- [ipdsc_42_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/ipdsc_42_monitor.py)

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
