---
doc_type: epic
title: "AUDI-1049: Fangorn on MNTN ID — parallel feature store + model"
status: backlog
date: 2026-07-27
summary: "Household-grain (MNTN ID) Fangorn: parallel MNTN-ID-keyed feature store + retrained model. Sept-4 MVP."
result: "not started"
question: "Can a Fangorn-like model trained on a MNTN-ID-keyed household feature store match/beat the current IP model rolled up to household, and ship for the Sept-4 MVP?"
framing_state: "skip: epic — per-child framing (1134 build, 1100 tuning, 1103 train, 1105 validate)"
---

# AUDI-1049: Fangorn on MNTN ID — parallel feature store + model

**Jira:** https://mntn.atlassian.net/browse/AUDI-1049
**Status:** backlog
**Date Started:** 2026-07-27
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
Epic (from the 6/23 TI refinement). As Identity rolls out **MNTN ID** (household ID / ID-graph, initiative
**ID-327**), AUDI builds a Fangorn-like model on a **MNTN-ID-keyed feature store** (household grain) rather
than transforming IP-level scores at the end. Approach (Matt): key the store on MNTN ID at daily grain,
retrain, validate with a custom experiment. Milestones: **Aug 3** reporting baseline, **Sept 4** enhanced
targeting-categories MVP. Owner: Matt Brorby (epic). RFD decider: Alyson Lefkowitz.

**Governing docs:** RFD "MNTN-ID Feature Store Data Path & IPDSC Pipeline" (Sean, TAR/3704094762, covers
AUDI-1055/1056); "AUDI-1057: Fangorn-on-MNTN-ID" modeling scope (Matt, TAR/3695312930). Chosen path =
**Option 1 (hybrid translation)**: join the identity graph inside the existing pipeline, re-key IP→household,
emit a second household-keyed output (parallel run, not cutover). Identity-graph schema →
`knowledge/data_catalog.md` → "Identity Graph".

## 2. The Problem
The Fangorn feature store + intent pipeline (airflow-ti) is IP-keyed end to end. To score at household grain
(where MNTN ID lives) the store, labels, and thresholds must be re-derived at household grain, then the model
retrained and validated — without a like-for-like champion/challenger (grain mismatch: IP vs household).

## 3. Child map & sequencing (Malachi's lane in **bold**)
Critical path: **feature store build → train → validate → experiment**; tuning is a parallel improvement.

| Ticket | Owner | Role | Sprint |
|---|---|---|---|
| AUDI-1055 Spike: data path | Sean | ✅ done — Option 1 chosen | — |
| **AUDI-1134 Build household feature store** | **Malachi** | **v1 IP-parity store (L1 mirror→resolution→L2/L3→orchestration); blocks 1103** | A (07/27–08/10) |
| AUDI-1102 Visit label (`guid_hh_log`) | Matt | Household-grain training labels | A |
| AUDI-1101 History depth for backtest | Matt | 60d-vs-90d retention (gates backfill window) | A |
| AUDI-1103 Train MNTN ID Fangorn | Brian | Retrain XGBoost on household store | A |
| AUDI-1106 Daily household scoring job | Brian | Productionize predictions | A |
| AUDI-1104 Tune household intent thresholds | Alex | Re-calibrate hi/mid cutoffs | A |
| AUDI-1136 / 1138 audience_intent + staging jobs | Sean | Household scoring + staging | A |
| **AUDI-1100 Household feature engineering** | **Malachi** | **Tuning (sum/mean/recency) + enrichment on top of 1134; follow-up** | B (08/10–08/24) |
| AUDI-1105 Validate MNTN ID vs IP model | *unassigned* | Offline AUC/PR-AUC vs rolled-up IP baseline — *Malachi candidate* | B |
| AUDI-1107 Household scoring monitors | *unassigned* | Mirror IP Fangorn monitors | B |
| AUDI-1108 Design household experiment | *unassigned* | Online design w/ Exp team — *Malachi candidate* | B |

**Decomposition (planning today):** AUDI-1134's five components become **new Tasks under this epic** (Bryce:
new tasks, not subtasks). Sean creates them at planning after team review. See `audi_1134_feature_store_build/`.

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
