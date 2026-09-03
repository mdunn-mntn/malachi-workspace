---
doc_type: ticket
title: "AUDI-1273: Lower files.maxPartitionBytes on 3 map-side-spill DAGs"
status: backlog
date: 2026-09-02
summary: "Read input in smaller pieces on 3 DAGs that spill while reading, not shuffling"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1273: Lower files.maxPartitionBytes on 3 map-side-spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1273
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

Three jobs overflow to disk while reading their input, not while shuffling, so the fix is reading in smaller pieces. Raising shuffle partitions does nothing here.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** `spark.sql.files.maxPartitionBytes` sets how much input one task reads at once. Too big and the task cannot hold it in memory, so it spills to disk.

**Task:** set in the builder:
- [ipdsc_ds_49](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_49.py) add 67108864 (64 MiB)
- [conv_log_derived_ip](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_2_derived/conv_log_derived_ip.py#L58) 268435456 -> 134217728 (256 -> 128 MiB)
- [ipdsc_ds_67](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_67.py) add 33554432 (32 MiB)

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
