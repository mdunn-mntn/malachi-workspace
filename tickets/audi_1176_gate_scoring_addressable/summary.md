---
doc_type: ticket
title: "AUDI-1176: Gate audience_intent scoring input to DS14-addressable IP set"
status: backlog
date: 2026-07-28
summary: "Intersect 31d DS13/DS19 scoring input with DS14 (8d) before scoring; cut ~39-69% compute"
result: ""
question: "Can we gate the scoring input to DS14-addressable IPs and cut ~39-69% of daily scoring compute with no coverage loss?"
framing_state: locked
---

# AUDI-1176: Gate audience_intent scoring input to DS14-addressable IP set

**Jira:** https://mntn.atlassian.net/browse/AUDI-1176
**Status:** Backlog
**Date Started:** 2026-07-28
**Assignee:** Malachi
**Blocked by:** [AUDI-1175](https://mntn.atlassian.net/browse/AUDI-1175) (sizing + consumer audit + go/no-go)

---

## 0. Framing (locked 2026-07-28)

- **Question:** Can the `audience_intent` scoring input be gated to the DS14-addressable (8-day) IP set, cutting ~39% (verticals) / ~69% (MM Core) of daily scoring compute, without losing any biddable coverage?
- **Goal (why):** Realize the cost reduction AUDI-1175 sizes. Also shrinks IPDSC volume (MembershipDB resilience). Decision AUDI-1175 gates funding.
- **Objective (done-when):** Scoring input intersected with current DS14 set before `vertical_high/mid` + `populate_data_source`; a before/after run shows the compute drop and identical biddable delivery (no lost impressions/reach on a holdout of advertisers). Shipped behind a flag with rollback.
- **Approach:** Insert a DS14-recent filter at the input of `vertical_high.py` / `vertical_mid.py` (pre-filter the exploded 31-day DS13 IPDSC set against the current DS14 8d set), and mirror for the Fangorn 14-day path. Alternative insertion point: intersect `intent_score_map` output before serving-store load. **The largest $ lever is the DS19 path: gate `prospecting_keywords` input (34% of DAG cost, ~$9.6k/mo prize) — not just the vertical jobs (~$1.3k/mo).** Validate delivery parity on a shadow run before cutover.
- **What would change the answer:** if AUDI-1175's consumer audit finds a non-bidding consumer that needs the full universe, gate only the serving-bound output path (not the model inputs). If shadow-run delivery drops, revert and diagnose the coverage-loss mechanism.

## 1. Introduction

Implementation ticket for the optimization AUDI-1175 identified: MM/vertical scoring runs over the full 31-day IP universe; only the ~8-day DS14-addressable slice is ever biddable. See AUDI-1175 §4 for the full finding and sizing.

## 2. The Problem

Daily scoring recomputes the full 31-day universe. 39% (DS13) / 69% (DS19) of scored IPs cannot be bid on within the 8-day DS14 window and are thrown away and recomputed the next day.

## 3. Plan of Action

1. Gate on AUDI-1175 go/no-go (consumer audit + $ figure).
2. Add DS14-recent intersection at `vertical_high/mid` input (flagged).
3. Shadow run: compare scored-set size, compute cost, and biddable delivery vs current.
4. Verify zero coverage loss on an advertiser holdout.
5. Cutover; measure realized compute drop.

## 8. Open Items / Follow-ups

- Precise insertion point (input pre-filter vs output intersection) TBD in design.
- Fangorn 14-day path (`fangorn_14day_lookback.ipdsc_inclusion_flag`) may already be partially gated — confirm.
- **Hard constraint (AUDI-1175 consumer audit):** the LIVE DDM/Redshift automated HHST threshold recommender (`ETL-DCO-Automated-Threshold-Adjustment.py` → `hhst_generate_recommendation`, owner **Devon Rogers**) reads the FULL scored `prospecting_intent`/`advertiser_intent` as its population denominator to SET the production HHST gate. Do NOT let a naive input-gate starve it. Options: gate only the serving-bound output, OR preserve a cheap full-universe population COUNT for the recommender + sizing procs while scoring per-IP only on the addressable set. Watch the recommender's small-population guardrails (`<100→6666`) and its 30-day winnable vs 8-day DS14 mismatch.
- **Confirm before global gate:** AUD-5221 deciles (Alex/Zach — no code found) and any LiftLab full-scored export (#dev-incremental-lift — none found).
