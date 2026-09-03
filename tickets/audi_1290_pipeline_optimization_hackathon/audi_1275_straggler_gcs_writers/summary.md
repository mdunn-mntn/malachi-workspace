---
doc_type: ticket
title: "AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs"
status: backlog
date: 2026-09-02
summary: "Speculation is unsafe on GCS writers; settle a safe straggler remedy then apply to 13 DAGs"
result: "not started"
question: "Which straggler remedy is safe for Spark jobs that overwrite GCS output, and which of the 13 DAGs can take it now?"
framing_state: locked
---

# AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1275
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Which straggler remedy is safe for Spark jobs that overwrite GCS output, and which of the 13 DAGs can take it now?
- **Goal (why / the decision):** Speculation was proposed and refuted twice as unsafe for GCS writers; the answer unblocks 13 straggler findings. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A decision memo in outputs/ (committer guarantees per writer path, safe remedy, per-DAG verdict), a Slack ask drafted for Ryan Kleck in artifacts/ for the user to send, and one PR (branch AUDI-1275) applying the remedy only to DAGs whose safety is proven from source; the rest wait on the owner's answer.
- **Approach (how):** For each DAG read the writer path (format, mode, committer, GCS connector version) on airflow-ti main; check Spark and GCS-connector committer semantics (FileOutputCommitter v1/v2, Dataproc GCS committer, task-attempt isolation); from the event log confirm whether the straggler is in a write stage; user's decision 2026-09-02: draft the ask, execute the safe subset.
- **What would change the answer:** A DAG whose writer is not idempotent under duplicate task attempts is never changed without the owner's word; if no remedy is provably safe the deliverable is the memo and the ask alone.

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
