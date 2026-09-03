---
doc_type: ticket
title: "AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs"
status: backlog
date: 2026-09-02
summary: "Speculation is unsafe on GCS writers; settle a safe straggler remedy then apply to 13 DAGs"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1275
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

Thirteen jobs regularly sit waiting on one slow task (a straggler). Spark's built-in remedy is unsafe for jobs that write to GCS, so first pick a safe remedy, then apply it to all 13.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the built-in fix (speculative execution) runs a second copy of the slow task, and it is app-wide; two copies writing the same GCS output can corrupt it. It was proposed and refuted twice for that reason.

**Task:** settle the safe pattern with the owning team (per-stage alternatives, committer guarantees), then apply to:
- [advertiser_join](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/advertiser_join.py), [advertiser_high](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/advertiser_high.py), [prospecting_join](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/prospecting_join.py)
- [identity_targeted_signal](https://github.com/SteelHouse/airflow-ti/blob/main/models/signals/identity_targeted_signal.py)
- [fangorn_score_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/fangorn_score_monitor.py)
- [ipdsc_ds_42](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_42.py), [ipdsc_ds_47](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_47.py), [ipdsc_ds_63](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_63.py), [hhdsc_ds_19](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/hhdsc_ds_19.py)
- [aug_log_ip_vertical_id_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py)
- [site_network_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/bidstream_hourly/site_network_hourly.py)
- [site_visit_signal_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/site_visit_signal_advertiser_id_dsc_id.py)
- [tpa_mntn_id_export](https://github.com/SteelHouse/airflow-ti/blob/main/models/tpa_export/tpa_mntn_id_export.py)

**Done-when:** decision recorded and fix PRs merged.

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
