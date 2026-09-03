---
doc_type: ticket
title: "AUDI-1273: Lower files.maxPartitionBytes on 3 map-side-spill DAGs"
status: backlog
date: 2026-09-02
summary: "Read input in smaller pieces on 3 DAGs that spill while reading, not shuffling"
result: "not started"
question: "Does lowering spark.sql.files.maxPartitionBytes on ipdsc_ds_49, conv_log_derived_ip and ipdsc_ds_67 remove their map-side spill?"
framing_state: locked
---

# AUDI-1273: Lower files.maxPartitionBytes on 3 map-side-spill DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1273
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does lowering spark.sql.files.maxPartitionBytes on ipdsc_ds_49, conv_log_derived_ip and ipdsc_ds_67 remove their map-side spill?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** One PR (branch AUDI-1273) merged setting 64 MiB, 128 MiB and 32 MiB respectively in each builder; the ledger marks the three spill findings resolved.
- **Approach (how):** Confirm current builder config on main (conv_log_derived_ip already overrides to 256 MiB), apply the three values, run model_upload.py --dryrun; confirm from the 08-27 sweep that the spill is in the input-read stage.
- **What would change the answer:** The spilling stage is a shuffle stage after all (1269/1270 mechanism), or the input is not splittable so the knob does nothing.

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
