---
doc_type: ticket
title: "AUDI-1271: Raise initialExecutors on 2 pre-verified fetch-wait DAGs"
status: backlog
date: 2026-09-02
summary: "Raise dynamicAllocation.initialExecutors to 200 on two hourly DAGs stalled on shuffle-fetch wait"
result: "not started"
question: "Does raising spark.dynamicAllocation.initialExecutors to 200 on aug_log_ip_vertical_id_hourly and site_network_hourly remove the shuffle-fetch wait on stage 11 and stage 9 without raising DCU-hours per run?"
framing_state: locked
---

# AUDI-1271: Raise initialExecutors on 2 pre-verified fetch-wait DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1271
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does raising spark.dynamicAllocation.initialExecutors to 200 on aug_log_ip_vertical_id_hourly and site_network_hourly remove the shuffle-fetch wait on stage 11 and stage 9 without raising DCU-hours per run?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** One PR (branch AUDI-1271) merged with both values and a regenerated dags/model_task_config.json; the ledger marks both shuffle_fetch_wait findings resolved and per-run DCU-h does not rise.
- **Approach (how):** Edit the two decorators, confirm maxExecutors is at or above 200 on both, regenerate the config with model_upload.py --dryrun; after merge read DCU-h per run from gcloud dataproc batches describe and compare to the 2026-08-20 baseline (site_network_hourly mean 510 DCU-h/run).
- **What would change the answer:** Fetch wait unchanged after the change, or DCU-h per run up more than the wait saved, in which case revert and record it in the ledger as fix_not_working.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Two hourly jobs spend a third to half their runtime waiting to copy data between machines. Starting them with more machines removes the wait. Values verified.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** these jobs start small and scale up. The first phase's output lands on the few starting machines, then every later machine queues to fetch from those few (shuffle-fetch wait).

**Task:** raise `spark.dynamicAllocation.initialExecutors`:
- [aug_log_ip_vertical_id_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py#L72) 100 -> 200
- [site_network_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/bidstream_hourly/site_network_hourly.py#L31) 50 -> 200

Then regenerate `dags/model_task_config.json`.

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
