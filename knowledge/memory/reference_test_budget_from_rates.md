---
name: reference_test_budget_from_rates
description: "Rates can't predict advertiser spend (R2=0.10); they predict REQUIRED test spend. spend_required() + the 1/IVR rule, and how to screen a lapsed advertiser."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [required spend, spend_required, ti_884_mde_calculator, incrementality test budget, lapsed advertiser, churned advertiser, win-back, INCR-75, eligibility screen, IVR, CVR, MDE, cost_impression_log TTL, sum_by_advertiser_by_day, PSA 9090, Al Beretta]
domain: [incrementality, experimentation, bigquery]
lifecycle: active
last_verified: 2026-08-11
---
**When anyone asks to "estimate spend from visit/conversion rate," redirect to REQUIRED spend.** Visit rate and conversion rate are ratios, so they're scale-free and carry almost no signal about how much an advertiser spends: on the 1,566 INCR-75 advertisers (`spend_30d > $1k`, `IVR > 0`), OLS on `log(spend_30d)` gives **R² = 0.045 (log IVR) / 0.098 (log CVR) / 0.100 (both)**; within one IVR decile spend spans **15–66x** p10→p90. The answerable question is the inverse — **the spend a test needs to detect a lift** — via `spend_required(p, target_mde_rel, cpm, imps_per_ip)` in `tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts/ti_884_mde_calculator.py`. That function already produced INCR-75's `budget_for_mde_ivr_*` columns; never reimplement it.

**The shortcut, and its trap.** Required N ∝ (1−p)/p ≈ 1/p, so budget scales as **1/visit-rate**. At α=.05, power=.80, 10% holdout, 8 weeks, **$30 CPM and 15 imps-per-IP**: 8-wk budget ≈ **$14,100 ÷ IVR** for a 5% relative MDE. **That constant is conditional on those two defaults** — general form `$14,100 / IVR × (CPM/30) × (impsPerIP/15)`. BoggBag (46426) at $12.33 CPM / 3.65 imps-per-IP has a combined factor of 0.10, so the bare shortcut is **10x too high**. Always scale by the advertiser's own delivery shape. MDE is **relative** (5% on a 2% IVR = detect 2.1%, not 7%); CVR baselines run ~30x lower so a conversion-powered test costs 7–10x more and stays informational, never pass/fail.

**Screening a LAPSED advertiser is cheap; the assumed blocker is imaginary.** INCR-75 covers only advertisers delivering in the trailing 30d (universe CTE `HAVING SUM(impressions_ip) > 0`) — that, **not a spend threshold**, is why a churned advertiser is missing (spend is scored, never cut). All three metric inputs retain years, so any window back to 2023-10-01 is recomputable. Three gotchas that cost real time: (1) the `incr_75_advertiser_metrics.sql` comment claiming `cost_impression_log` has a **90-day TTL is WRONG** — no TTL, floor 2023-10-01, 1,047 partitions, 77.6B rows; (2) the 12-month spend CTE reads `agg__daily_sum_by_campaign`, **frozen at 2026-04-30** → swap to `summarydata.sum_by_advertiser_by_day` (2024-01-01+, fresh, `require_partition_filter=TRUE`); (3) **BQ can't prune partitions on a subquery-derived date** — resolving the last-active day and pulling metrics in ONE statement scanned 39.5 GB vs **5.5 GB** split into two steps with the window as a literal. Fork reproduces INCR-75 rates within **0.21%**.

**A lapsed advertiser structurally caps at Mid tier.** INCR-75 tiers on POWER × CONFIRMED-LIFT, and `confirmed +` needs ≥20 holdout visits at p<.05 from a **live** ghost-bid holdout. No delivery means no bids means no measurable lift, ever. Say so before anyone promises a customer a top-tier read. See [[reference_ghost_bid_lift_register]], [[reference_select_vs_nonselect_incrementality]], [[project_bidder_level_ghost_bidding_approved]]. Full method: `knowledge/experimentation.md` §"Screening a LAPSED advertiser"; ticket `tickets/audi_xxx_lapsed_advertiser_test_eligibility/`.
