---
doc_type: ticket
title: "AUDI-1215: ElevenLabs lift pre/post 6/30 audience change (CGID 122748)"
status: in_progress
date: 2026-08-21
summary: "Did ElevenLabs incrementality lift change after the 2026-06-30 audience change on CGID 122748?"
result: "in progress"
question: "Did incrementality lift for CGID 122748 change after the 2026-06-30 audience change?"
framing_state: locked
---

# AUDI-1215: ElevenLabs lift pre/post 6/30 audience change (CGID 122748)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1215
**Status:** In Progress
**Date Started:** 2026-08-21
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Did incremental lift (visits, conversions) for ElevenLabs CGID 122748 change after the 2026-06-30 audience change, comparing pre (≤2026-06-30) vs post (≥2026-07-11) with Matt Brorby's 2026-07-01..07-10 blackout excluded?
- **Goal (why / the decision):** Mike Dolt's urgent P0 Slack ask (2026-08-21); informs whether the audience change improved incrementality for a top account. BER-2250 incrementality is the Q2+ north-star priority.
- **Objective (done-when):** A pre/post lift table (point, CI, p on both periods, plus the delta) from the ghost-bid ITT with a corroborating instrument, posted back to Mike. Binary: the table exists with uncertainty quantified, or it doesn't.
- **Approach (how):** Ghost-bid randomized ITT from `silver.enriched.lift__ghost_bid_visits` (entry-cohort anchor, ghost_frac gate 0.09–0.11, partner 8 only) as primary; the newer fixed-membership holdout lineage (`lift__holdout_*`, `v_lift__results_by_month`) as corroboration; attributed panel (all_facts) as context only. Verify the audience change itself from CDC dims. Adversarial verify before reporting.
- **What would change the answer:** Post-period ghost_frac outside 0.09–0.11 (depletion bias) or instrument disagreement demotes the verdict from a lift-change claim to a caveated directional read; silver floor 2026-06-22 means the pre window is thin (~8 days) and may be underpowered — then the honest answer is the MDE, not a point claim.

## 1. Introduction
ElevenLabs (AID 51660) is a B2B advertiser on US CTV (Beeswax leg). TI-1044 (June 2026) established: clean ghost-bid ITT lift ≈ 0 (CVR −1.7% n.s., total visits ~0), large attributed/ATT numbers are attribution + win-selection bias, and CVR is unpowered at the 0.062% base rate. On ~2026-06-30 the audience on CGID 122748 ("growth_mntn_agents-priming-in-platform_english", objective 1 prospecting, PTV) was changed. Mike Dolt asks whether incrementality moved after the change.

## 2. The Problem
- Need pre/post incrementality lift for one campaign group with a mid-window treatment change (audience swap ~6/30).
- Blackout convention set by Matt Brorby: pre ends 6/30, 7/1–7/10 excluded, post starts 7/11.
- Known instrument issues: silver ghost-bid table floor 2026-06-22 (thin pre window); entry-cohort holdout depletion inflates post-period lift (gate ghost_frac 0.09–0.11); partner 79 rows are garbage (exclude; keep partner 8); 7-day outcome window right-censors the last 7 days.

## 3. Plan of Action
1. Log Jira spike (AUDI-1215, P0, sprint 8270) — done.
2. Verify CGID 122748 ownership + metadata — done (AID 51660, prospecting, PTV).
3. Workflow `wf_8b658238-b57`: scout (audience-change evidence, delivery panel, lift-table coverage) → measure (ghost-bid ITT pre/post, holdout lineage pre/post + monthly, gold strata composition) → adversarial verify (bias lens + repro lens).
4. Synthesize: pre/post lift table with CI/p + delta, caveats signed; reply to Mike; Jira completion comment.

## 4. Investigation & Findings
_Workflow in flight (wf_8b658238-b57); findings land here on completion._

## 5. Solution
_Pending._

## 6. Questions Answered
_Pending._

## 7. Data Documentation Updates
_Pending._

## 8. Open Items / Follow-ups
_Pending._
