---
doc_type: ticket
title: "AUDI-1169: Build Layer-3 household pivot models (~900-col mntn_id-grain, trainable)"
status: backlog
date: 2026-07-28
summary: "Wide ~900-col mntn_id-grain model-ready pivot + column-parity report vs the IP pivot; consumable by AUDI-1103"
result: ""
question: "Can we emit a wide ~900-col mntn_id-grain model-ready pivot that passes column parity vs the IP pivot and is trainable by AUDI-1103?"
framing_state: draft
---

# AUDI-1169: Build Layer-3 household pivot models

**Jira:** https://mntn.atlassian.net/browse/AUDI-1169
**Parent epic:** AUDI-1049 · **Build umbrella frame:** `../audi_1134_feature_store_build/summary.md`
**Status:** backlog · **Assignee:** Malachi (unassigned in Jira — claim at planning)
> **⚠ Update 2026-07-29 (epic §7i): now co-owned Malachi + Brian McAdams** (Sean moved to DS13/19). Core L2/L3
> build you two lead, `hh_`-prefixed models in the existing DAG.

---
## 0. Framing  ← run `/frame` when you start; inherits the AUDI-1134 build-frame
- **Question:** Can we emit a wide **~900-col mntn_id-grain** model-ready pivot that passes **column parity**
  vs the IP pivot and is directly trainable by AUDI-1103?
- **Goal:** The model-ready table — the direct input to the retrain (1103) and validation (1105). Sept-4 MVP.
- **Objective (done-when):** L3 pivot at `mntn_id` grain producing the full ~900-column model-ready wide table
  + a **column-parity report** vs the IP L3 pivot (same columns, comparable distributions), consumable by 1103.
- **Approach:** mirror the IP L3 pivot at household grain over the AUDI-1168 L2 outputs. **Reconcile daily vs
  monthly L3** (§6.2) — PDF says train on monthly L3, tickets encode daily; the training table may be monthly.
- **What would change the answer:** column parity fails (missing/renamed features) → 1103 can't train;
  household pivot has implausible sizes/nulls → resolution/L2 rework.

## 1. Introduction
Component 4 of 5. The wide model-ready pivot Fangorn trains on. Keep the schema **uplift-friendly** — the
incrementality model (RFD B, §8 of the epic) is slated to train on these same L3 tables.

## 2. The Problem
XGBoost-per-vertical (1103) needs the ~896-feature vector at MNTN-ID grain in the same shape as the IP pivot,
or the retrain and the offline validation (1105, rolled-up IP baseline) won't be comparable.

## 3. Plan of Action
1. L3 wide pivot at `mntn_id` grain over the 1168 L2 derived models.
2. Column-parity report vs the IP L3 pivot (column set + distribution comparison).
3. Resolve daily-vs-monthly training pivot (§6.2) with Matt/Sean before finalizing.
4. Keep schema uplift-model-friendly (RFD B / AUDI-1052).

## 4. Investigation & Findings
_(queries in `queries/`, results in `outputs/`)_

## 5. Solution
_(PRs, config, code)_

## 6. Questions Answered
- **Q:** — **A:** —

## 7. Data Documentation Updates
_(document the household L3 pivot schema + parity methodology)_

## 8. Open Items / Follow-ups
- Daily-vs-monthly L3 (§6.2) unresolved. Backfill depth gated by 60-vs-90d retention (§6.1, AUDI-1101).
