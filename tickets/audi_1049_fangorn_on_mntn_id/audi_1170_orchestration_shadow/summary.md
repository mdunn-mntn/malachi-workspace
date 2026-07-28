---
doc_type: ticket
title: "AUDI-1170: Orchestration, backfill, and shadow validation for the household FS"
status: backlog
date: 2026-07-28
summary: "Wire L1→L2→L3 as an additive task group in feature_store_setup_model.py; backfill runner + shadow parity dashboards"
result: ""
question: "Can we run the household FS (L1→L2→L3) as an additive task group on the existing DAG, backfill it, and pass shadow parity vs the IP store?"
framing_state: draft
---

# AUDI-1170: Orchestration, backfill, and shadow validation

**Jira:** https://mntn.atlassian.net/browse/AUDI-1170
**Parent epic:** AUDI-1049 · **Build umbrella frame:** `../audi_1134_feature_store_build/summary.md`
**Status:** backlog · **Assignee:** Malachi (unassigned in Jira — claim at planning)

---
## 0. Framing  ← run `/frame` when you start; inherits the AUDI-1134 build-frame
- **Question:** Can we run the household FS (L1→L2→L3) as an **additive task group** on the existing
  `feature_store_setup_model.py` (no forked DAG), backfill it over historical graph days, and pass **shadow
  parity** vs the IP store?
- **Goal:** Makes the whole household FS operational + trustworthy before anything consumes it. Sept-4 MVP.
- **Objective (done-when):** additive task group with L1→L2→L3 dependency edges on the one schedule; a backfill
  runner over `as_of_date` (**60-day BQ + `household_graph_parquet` fallback**); shadow-parity dashboards
  (household vs IP audience sizes, resolution/coverage split, day-over-day household stability); naming-standards
  doc updated.
- **Approach:** extend `feature_store_setup_model.py` + `model_task_config.json`; backfill via `--run_date` on
  1166. Don't block on AUDI-1101 — proceed on 60d + parquet fallback, widen when retention is confirmed.
- **What would change the answer:** shadow parity fails (implausible sizes, poor coverage, high day-over-day
  churn = HHID instability, §6.4) → resolution/graph-join rework before 1103 trains.

## 1. Introduction
Component 5 of 5. Orchestration + the trust layer. The **shadow-run parity check is the gate** — nothing
downstream (train, HHDSC export) consumes the household output until parity holds.

## 2. The Problem
The four models must run in order on one schedule (no forked DAG), backfill deep enough to train, and prove —
via shadow parity — that household audience sizes/coverage/stability are plausible before consumption.

## 3. Plan of Action
1. Additive task group + dependency edges (L1→L2→L3) in `feature_store_setup_model.py`.
2. Backfill runner over `as_of_date` (60-day BQ + parquet fallback).
3. Shadow-parity dashboards: household-vs-IP audience sizes, resolution/coverage split, day-over-day household
   stability (the HHID-churn check, §6.4).
4. Update `docs/feature_store_naming_standards.md`.

## 4. Investigation & Findings
_(queries in `queries/`, results in `outputs/`)_

## 5. Solution
_(PRs, config, code)_

## 6. Questions Answered
- **Q:** — **A:** —

## 7. Data Documentation Updates
_(document the shadow-parity methodology + household-stability metric)_

## 8. Open Items / Follow-ups
- Backfill depth gated by 60-vs-90d retention (§6.1, AUDI-1101). Reconciliation band (§6.6) undefined — this
  ticket produces the parity numbers that band will be set against.
