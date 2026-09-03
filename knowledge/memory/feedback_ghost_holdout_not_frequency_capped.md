---
name: feedback_ghost_holdout_not_frequency_capped
description: "Ghost bids increment no counter, so the holdout is neither frequency capped nor paced — the holdout share drifts off 10% in the bidder, above 11% it goes negative, and no post-hoc analysis repairs it"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [ghost bid, ghost_frac, holdout, frequency cap, pacing, Beeswax, partner_id 8, Mountain bidder, selection effect, bid count, incrementality dashboard, TI-1313, Rogus, Ryan Kleck, negative lift]
domain: [incrementality, experimentation, bidding]
lifecycle: active
last_verified: 2026-09-03
---
**The bidder increments a counter on a real bid and that counter drives both the frequency cap and pacing.
A ghost bid increments nothing.** So a held-back household keeps being bid on after its treated twin is
capped, and pacing never slows for it. The holdout is exactly 10% in the audience and stops being 10% only
once it flows through the bidder.

**Above ~11% ghost fraction, measured lift goes negative** because the holdout has accumulated highly active
IPs the frequency cap would have removed, raising its baseline. Below ~10% those IPs get caught in the
first-day cohort and dropped by the first-day washout.

**Why:** this is not a data-cleaning problem, it is a bidding-mechanism problem, so **no reanalysis of
historical data repairs it** and a future fix will move every number. Bidding less on the holdout does not fix
it either — that lowers the percentage without selectively removing the active IPs. The only real fix is
tracking holdout bids on the bidder side. Rogus refused that in Beeswax (fear of leaking into spend and
pacing); it exists on the MNTN bidder, which runs essentially only Select, so it does not help `partner_id=8`.

**How to apply:** gate hard (block ghost fraction above 11% and below 7%; TI-1313 used a tighter 9-11% and
loosening to 7-11% moves pooled lift +7.9% → +10.7%, which is artifact). **Never stratify on bid count** —
that is the post-treatment split this defect creates. Put an explicit asterisk on any deliverable built from
ghost-bid lift. Full mechanism, numbers and quotes in `knowledge/data_knowledge.md` §"ROOT CAUSE: ghost bids
are never counted". Related: [[feedback_report_both_lift_scales]], [[feedback_no_naive_pre_post]].
