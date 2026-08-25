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
2. ~~Check `bid_price_log` for ghostBid rows for a few absentees~~ DONE 2026-08-25: present with ghost bids → pipeline-side.
3. Document the coverage rule in `knowledge/experimentation.md` ghost-bid section once named.

## 4. Investigation & Findings

### Raw-log probe settles it: the gap is in the SQLMesh silver model (2026-08-25)

Matt's discriminating test: if the absentees are in `dw-main-bronze.raw.bid_price_log` for these campaigns, "something is up with the sqlmesh model." They are. One-hour window (2026-08-24 18:00-19:00 UTC, 395 GB on-reservation, `queries/audi_1223_raw_bid_log_probe.sql`):

| Advertiser | ghostBid rows | submitted ('' reason) | ghost/submitted |
|---|---:|---:|---:|
| 7 For All Mankind (31602, ABSENT) | 1,820 | 40,857 | 4.5% |
| Sur La Table (32244, ABSENT) | 992 | 9,893 | 10.0% |
| ThirdLove (32127, ABSENT) | 28 | 1,901 | 1.5% |
| Gruns (42097, control, PRESENT) | 15,051 | 93,221 | 16.1% |

Absent advertisers have both ghost bids and submitted bids in the raw log, across multiple campaigns each, yet zero rows in `enriched.lift__ghost_bid_visits`. Bidder-side logging is fine; the silver SQLMesh build drops them. With Matt to trace the model. (Single-hour ghost/submitted ratios are noisy; presence, not ratio, is the finding.)

### ROOT CAUSE: per-campaign env label; silver model keeps env='prod' only (Matt Brorby + verified, 2026-08-25)

Matt: "same issue as the test-campaigns. in bid_price_log the env variable is burnin instead of the expected prod." Verified same hour window (`queries/audi_1223_env_by_campaign.sql`):

- **env is assigned per CAMPAIGN, not per advertiser.** ThirdLove: prospecting/MT trio 573186/573185/573188 (CG 115424) 100% burnin; its ONLY prod rows are the TV Retargeting campaigns (199776-79, 390309-10). Shea Homes (44720): no prod ghost/submitted rows at all. Gruns (control): old prospecting 578184/431456 in prod (7,400 + 6,555 ghost bids/hr) — which is why it IS covered.
- **Burnin dominates raw volume for everyone** (even Gruns: 11.9M burnin vs 1.9M prod rows/hr, all reasons) — the env='prod' filter is doing heavy lifting in the model.
- **Coverage nuance beyond the 142:** Gruns' newer campaigns 626274/626275/626276 (CG 126905, the AUDI-1148 group) read burnin in this hour, yet that CG had June-July rows in the silver table. So either env labels changed over time or campaigns migrate burnin→prod. Means COVERED advertisers can still be missing individual campaigns from measurement — the campaign-level coverage question is open even where advertiser-level presence looks fine.

Fix is on the bidder/model side (Matt). Once fixed, re-run the INCR-75 fold to re-gate the 437 "no data yet" advertisers.

See INCR-75 summary 2026-08-25 sections for the full verification trail. Advertiser list: INCR-75 `outputs/incr_75_ghost_absent_prospectors.csv` (gitignored; attached to AUDI-1223, also on Drive as "INCR-75 Ghost Bid Coverage Gap.xlsx").

## 5. Solution
Not started.

## 6. Questions Answered
None yet.

## 7. Data Documentation Updates
None yet.

## 8. Open Items / Follow-ups
- Matt Brorby: fix env labeling (bidder) or model filter; then re-run INCR-75 fold to re-gate "no data yet".
- Campaign-level coverage audit for PRESENT advertisers (burnin campaigns of covered advertisers are invisible too, e.g. Gruns 626276 as of 2026-08-24).
- Did env labels CHANGE over time? CG 126905 was measurable June-July but reads burnin now.
