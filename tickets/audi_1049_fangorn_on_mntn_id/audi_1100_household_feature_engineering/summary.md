---
doc_type: ticket
title: "AUDI-1100: Household Feature Engineering (Fangorn on MNTN ID)"
status: backlog
date: 2026-07-27
summary: "Tune aggregation (sum/mean/recency) + cross-IP enrichment on top of 1134's v1 IP-parity household store"
result: "not started"
question: "On top of AUDI-1134's v1 IP-parity household store, which per-feature aggregation changes (sum/mean/recency/conf-weighted) and cross-IP/device enrichment measurably improve the household Fangorn model over the v1 parity baseline?"
framing_state: locked
---

# AUDI-1100: Household Feature Engineering (Fangorn on MNTN ID)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1100
**Status:** backlog
**Date Started:** 2026-07-27
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** On top of AUDI-1134's **v1 IP-parity** household store, which per-feature aggregation changes (sum / mean / recency / confidence-weighted) and which cross-IP/device enrichment features **measurably improve the household Fangorn model over the v1 parity baseline**?
- **Goal (why / the decision):** Improve the household feature store beyond mechanical IP-parity → a better-performing MNTN ID Fangorn. **Follow-up to AUDI-1134** (the build), sequenced after the v1 store + baseline model exist. Enrichment is the "bonus" per the 15-Jul decision — *prioritized only as long as it doesn't delay the Sept MVP*. North-star tie: Theme 3 MM AI / identity-graph integration (ID-327).
- **Objective (done-when):** A tuned feature spec (per-feature aggregation choices + any new cross-IP features) applied to 1134's store, with an evaluation on household-grain model metrics (AUC / PR-AUC, per-vertical audience-size deltas) showing **improvement vs the v1 parity baseline** — OR a documented finding that IP-parity is already sufficient for MVP (close as "parity sufficient"). Depends on **AUDI-1134** (store) + **AUDI-1103** (baseline model); shares the eval harness with **AUDI-1105**.
- **Approach (how):** Start from 1134's L3 column-parity pivot. Propose aggregation changes by feature type (rates→mean vs sum, recency windows, confidence-weighting for shared IPs); optional cross-device enrichment (cross-IP reach, household device count) gated on Sept-4. Evaluate deltas on the household model with the AUDI-1105 validation harness. **Resolve first:** 1134's v1 store + column-parity report must land; confirm which feature blocks actually lose signal under IP-parity (those are the tuning targets).
- **What would change the answer:** If tuning yields no material lift over v1 parity → close as "parity sufficient for MVP," defer enrichment. If a feature block is degenerate at household grain → that's a **1134 fix**, not a 1100 tuning target. If Sept-4 timeline tightens → enrichment drops, tuning narrows to the highest-signal features.

## 1. Introduction
Part of epic **AUDI-1049** (Fangorn on MNTN ID: parallel feature store + model). As Identity rolls out
MNTN ID (household ID / ID-graph, initiative ID-327), AUDI is building a Fangorn model keyed on MNTN ID
at the household grain rather than transforming IP scores at the end. Today's Fangorn is an IP-keyed
XGBoost per vertical (~896 features, `advertiser_vertical` dominant, label = visit in a ~14-day forward
window) served as a hardcoded Dec-2025 artifact.

**Scope (revised 2026-07-27):** this ticket is the **feature-engineering follow-up** to the build. The
mechanical re-grain of the ~896 features to household lives in **AUDI-1134** at **v1 IP-parity aggregation**
(Sean's scope). 1100 is the *tuning + enrichment* pass **on top of** 1134's store: revisit per-feature
aggregation choices (sum/mean/recency/confidence-weighting) and add cross-IP/device enrichment, only where
it beats the v1 parity baseline and does not delay Sept-4. Sequenced **after** 1134 + AUDI-1103 (baseline).
Source of truth for the IP→household mapping: `knowledge/data_catalog.md` → "Identity Graph".

## 2. The Problem
1134's v1 store re-grains features with straight IP-parity aggregation (sum/max as-is). Some feature blocks
will lose signal under that mechanical roll-up (e.g., a rate averaged as a sum, recency collapsed to max),
and cross-device signal that only exists at household grain is left on the table. The open question is which
aggregation changes / new features actually move the household model — vs. adding tuning cost for no lift.
Depends on: **AUDI-1134** (v1 store + column-parity report), **AUDI-1103** (baseline model), shares eval with
**AUDI-1105**.

## 3. Plan of Action
Sequenced after 1134's v1 store lands. Then:
1. **Read the column-parity report** (from 1134's L3 pivot) — identify feature blocks that lose signal under
   IP-parity aggregation (the tuning targets).
2. **Propose aggregation changes** by feature type (rates→mean vs sum, recency windows, confidence-weighting
   for shared IPs) on the highest-signal / most-degraded features first.
3. **Optional enrichment** — cross-IP/device features (cross-device reach, household device count), gated on
   not delaying Sept-4.
4. **Evaluate** deltas on the household model using the AUDI-1105 validation harness (AUC/PR-AUC, audience-size
   deltas) vs the v1 parity baseline.
5. **Decide:** ship the tuned spec if it beats parity, else document "parity sufficient for MVP" and close.

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
