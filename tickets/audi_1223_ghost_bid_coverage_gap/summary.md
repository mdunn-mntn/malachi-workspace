---
doc_type: ticket
title: "AUDI-1223: ghost-bid lift data missing 142 actively-prospecting advertisers"
status: backlog
date: 2026-08-25
summary: "Why does the ghost-bid measurement pipeline skip 142 eligible advertisers that run standard prospecting?"
result: "not started"
question: "What determines which advertisers the ghost-bid lift pipeline covers, and why are 142 actively-prospecting eligible advertisers absent?"
framing_state: draft
---

# AUDI-1223: ghost-bid lift data missing 142 actively-prospecting advertisers

**Jira:** https://mntn.atlassian.net/browse/AUDI-1223
**Status:** backlog (sprint 8303, 2026-08-24..09-07)
**Date Started:** 2026-08-25
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** {draft} What rule or defect determines which advertisers enter `enriched.lift__ghost_bid_visits`, and why are 142 eligible advertisers with active standard Beeswax prospecting absent?
- **Goal (why / the decision):** Absent advertisers cannot be measured for lift; ThirdLove's absence blocked Edgar von Trotha's LiftLab-to-Ghost-Bidding-beta pivot. Coverage certainty gates every beta enrollment decision.
- **Objective (done-when):** {draft} Cause named with evidence; fix or workaround agreed with Matt Brorby.
- **Approach (how):** {draft} Trace with Matt where advertisers drop between bidder ghost-bid logging (`bid_price_log`) and the silver lift table build.
- **What would change the answer:** {draft} If the absentees turn out to have zero ghostBid rows in `bid_price_log` too, the gap is bidder-side, not pipeline-side.

## 1. Introduction
Spawned from INCR-75 (see `tickets/incr_75_eligible_advertisers/summary.md`, 2026-08-25 sections). Edgar von Trotha asked why ThirdLove shows "no data yet" in the lift-test candidates workbook while weighing a Ghost Bidding beta pivot. Verification found ThirdLove is one of 142 eligible advertisers that run standard prospecting yet never appear in the ghost-bid measurement.

## 2. The Problem
- `enriched.lift__ghost_bid_visits` (and the gold rollup) has zero rows, at any date, for 142 of 1,215 eligible advertisers that served 10K+ Beeswax prospecting impressions during the clean measurement window 2026-06-23..07-07 (689 comparable advertisers ARE covered).
- Ruled out on 2026-08-25 (evidence in INCR-75 summary): inactivity, bidder leg, missing score thresholds, audience-targeting composition (polarity-parsed; absent and present profiles are near-identical), query error (positive control: Gruns returns 23M rows in the identical probe).
- Impact: incrementality is unmeasurable for these advertisers; ghost-bidding beta enrollment decisions are blind for them. Largest absentees: 7 For All Mankind (474K window imps), Sur La Table, Shea Homes, Seasons 52, Aceable, ThirdLove (226K).

## 3. Plan of Action
1. Matt Brorby is checking the pipeline side (Slack, 2026-08-25).
2. If needed: check `bid_price_log` (10-day TTL) for ghostBid rows for a few absentees — bidder-side vs pipeline-side split. Advertiser filter costs ~7.8 TB/day unfiltered; scope by ip-cluster or ask Matt for the cheap path.
3. Document the coverage rule in `knowledge/experimentation.md` ghost-bid section once named.

## 4. Investigation & Findings
See INCR-75 summary 2026-08-25 sections for the full verification trail. Advertiser list: INCR-75 `outputs/incr_75_ghost_absent_prospectors.csv` (gitignored; attached to AUDI-1223, also on Drive as "INCR-75 Ghost Bid Coverage Gap.xlsx").

## 5. Solution
Not started.

## 6. Questions Answered
None yet.

## 7. Data Documentation Updates
None yet.

## 8. Open Items / Follow-ups
- Matt Brorby's read on the pipeline (pending, Slack 2026-08-25).
