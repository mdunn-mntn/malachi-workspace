---
doc_type: ticket
title: "AUDI-1105: Validate MNTN ID model vs IP model (offline, custom — not champion/challenger)"
status: backlog
date: 2026-07-28
summary: "Offline validation: household-grain AUC/PR-AUC + audience-size deltas + rank-corr vs the IP model rolled up to household"
result: ""
question: "Does the household-grain MID Fangorn model match or beat the IP model rolled up to household, offline?"
framing_state: draft
---

# AUDI-1105: Validate MNTN ID Model vs IP Model

**Jira:** https://mntn.atlassian.net/browse/AUDI-1105
**Parent epic:** AUDI-1049
**Status:** backlog · **Assignee:** Malachi (candidate — secondary lane; unassigned in Jira)

---
## 0. Framing  ← run `/frame` when you start
- **Question:** Does the household-grain MID Fangorn model (1103) **match or beat** the current IP model
  **rolled up to household**, offline — on discrimination, audience-size, and ranking?
- **Goal:** The **go/no-go gate for the Sept-4 MVP** — nothing launches without a valid comparison. Champion/
  challenger is **explicitly invalid** (IP-vs-household grain mismatch), so this needs a custom design.
  North star: proves whether household-keying actually improves the model (Kale's incrementality thesis).
- **Objective (done-when):** an offline evaluation reporting, per vertical: household-grain **AUC / PR-AUC** of
  the MID model vs the IP-model-rolled-up-to-household baseline; **per-vertical audience-size deltas**; and
  **rank correlation** (does the MID model reorder households vs the rolled-up IP scores?). A written verdict:
  better / at-parity / worse, with the label-sensitivity caveat.
- **Approach:** build the **rolled-up-IP baseline** (score IPs with the current model, aggregate to household);
  score households with the MID model (1103); compare on a common `guid_hh_log` label (1102). **This is exactly
  the causal/eval-methodology fit** — bring the label-sensitivity analysis (§6.5) and cluster-bootstrap CIs.
- **What would change the answer:** MID model underperforms the rolled-up IP baseline → the epic's premise
  fails (revisit aggregation/labels before serving); label choice (roll-up VV vs models 31/33) flips the verdict.

## 1. Introduction
The offline validation ticket (WS-C). Dovetails with the FS build (you'll have the household store in hand).
Its online counterpart is **AUDI-1108** (design the Nov-15 household experiment with the Experimentation team),
which you can extend into if you want the full validation arc.

## 2. The Problem
You can't A/B an IP model against a household model directly (different grain). The comparison must be
constructed: roll the IP model up to household and evaluate both on a shared household label — and the label
choice itself (§6.5) is a confound that must be tested, not assumed.

## 3. Plan of Action
1. Build the rolled-up-IP baseline (IP scores → household aggregate) — mind the collapse function (§6.3).
2. Score households with the MID model (AUDI-1103) on the same `guid_hh_log` label (AUDI-1102).
3. Per-vertical AUC / PR-AUC, audience-size deltas, rank correlation; cluster-bootstrap CIs.
4. Label-sensitivity: repeat under roll-up-VV vs models-31/33 labels (§6.5) — report if the verdict flips.
5. Written verdict + the reconciliation band (§6.6) recommendation.

## 4. Investigation & Findings
_(queries in `queries/`, results in `outputs/`)_

## 5. Solution
_(recommendation + verdict)_

## 6. Questions Answered
- **Q:** — **A:** —

## 7. Data Documentation Updates
_(document the offline validation protocol for MID-vs-IP grain-mismatch comparisons)_

## 8. Open Items / Follow-ups
- Gated by AUDI-1103 (train) + AUDI-1102 (label). Online experiment = AUDI-1108 (design w/ Exp team).
- Apply `knowledge/experimentation.md` Standard Analysis Protocol (DiD + CausalImpact, cluster-bootstrap).
- **First empirical step (Ryan: "code it and compare", epic §7f) — does re-keying even move the features?**
  Compare **household-aggregated features vs the IP version** BEFORE training. IPv4→HHID is multiple:multiple
  (not 1:1), so the household distribution diverges from IP only via (a) HHIDs inheriting **multiple IPv4s'**
  features (the real value) or (b) **orphaned IPv4s**, plus the **as-of historical** resolution. Quantify how
  much the L3 feature distribution + per-vertical audience sizes shift household-vs-IP; if ~nil (pick-one,
  ~1:1), re-keying won't move the model and that's the finding. This scopes whether the whole MID model is
  worth it — run it early.
