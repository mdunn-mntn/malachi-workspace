---
name: reference_select_vs_nonselect_incrementality
description: "AUDI-1172: Select prospecting ~5x more incremental than non-Select (PTV) — +22.0% vs +4.3% rel visit lift pooled, 27/35 advertisers favor Select (ghost-bid, 2026-07)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: ab62c6c6-93ab-41d7-aee9-578a3fb85ad7
doc_type: memory
keywords: [select vs nonselect, incrementality, audi-1172, ghost bid, visit lift, product_id, lift__ghost_bid_rollup, ptv, prospecting, kirsa]
domain: [incrementality, experimentation]
lifecycle: active
last_verified: 2026-07-28
---
**AUDI-1172 (2026-07-28, Kirsa's Select Performance Analysis):** for advertisers running both, **MNTN Select prospecting is ~5x more incremental than non-Select (PTV) prospecting.** Pooled across 35 advertisers running both (ghost-bid holdout, 2026-06-22→07-27, prospecting): **Select +22.0% rel visit lift** (treated 2.39% vs holdout 1.19%) vs **non-Select +4.3%** (1.38% vs 1.21%), both p<.05, near-identical holdout baselines. Robust per-advertiser: Select wins **27/35**, median edge +46pp; largest all confirm (Zazzle +95/+5, Hugo +84/+2, Planned Parenthood +142/+11, Upneeq +79/+25).

**This is the per-advertiser Select-vs-non-Select cut TI-933 said was "gated on ghost-bidder" — now unblocked** because `lift__ghost_bid_*` is queryable. Method: `dw-main-gold.reporting.lift__ghost_bid_rollup` (level=campaign_group) × `campaign_groups.product_id` (2=Select/1=PTV), clean-gate `se>0 AND NOT low_coverage`, IVW-pool CGs → advertiser×product (weight 1/se²; NOT count pool). Rel = IVW abs / holdout VR.

**Gotchas:** ghost-bid ITT is **bid-grain** (win-rate diluted ~10%) → compare RELATIVE lift, never the absolute pp. Data floor **2026-06-22** (no backfill), Beeswax leg only (MNTN Rust bidder not folded in) → Select coverage is a subset. Coverage of Kirsa's 93 AIDs: 74 have clean data, 35 both, 8 Select-only, 31 non-Select-only. All rows objective_id=1 (holdout is prospecting-only by construction). Deliverable: xlsx in `My Drive/Tickets/AUDI-1172/`. **The 7-day window is short vs the 30-45d advertiser-standard attribution → the +22% is a conservative FLOOR** (true effect over an advertiser's horizon is larger; don't compare head-to-head with advertiser-attributed numbers). Can't widen yet: only ~35d of ghost-bid data (breaks a matured 30d window) + variable/longer windows break cross-product comparability. Views accumulate (no TTL) → **re-measure Select vs non-Select on a matured 30d window ~Sept 2026** (expect a bigger gap). "user"=IP≈household; window is fixed 7d from each IP's first bid, not the full period. See [[reference_ghost_bid_lift_register]], [[reference_mntn_campaign_stages]], [[project_bidder_level_ghost_bidding_approved]].
