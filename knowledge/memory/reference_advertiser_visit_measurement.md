---
name: reference_advertiser_visit_measurement
description: "Three different per-advertiser visit counts (their raw site traffic, our verified visits, matched IPs) and which ratio answers 'are we reaching this advertiser's audience' — plus the size and grain traps on both"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [share of site visits, share of voice, raw_visits, verified visits, clickpass_log, match rate, IVR zero, low visit rate, pixel not firing, conv_pixel_opt_out, sum_by_advertiser_by_day, AUDI-1210, Johnny Chen, site size quintile, pixel triage]
domain: [data-catalog, business, experimentation]
lifecycle: active
last_verified: 2026-08-19
---
**"Advertiser X has 0 or near-0 visit rate, is their pixel broken?" — check three numbers, not one.** Full detail: `knowledge/data_knowledge.md` §"Three different visit counts". Ticket: `tickets/audi_1210_zero_visit_rate_advertisers/`.

1. **Their site traffic** = `summarydata.sum_by_advertiser_by_day.raw_visits` (INT64) — their own pixel, independent of MNTN.
2. **Our verified visits** = `SUM(clicks + views + competing_views)` same table — the client-facing Reporting figure.
3. **Matched IPs** = distinct `cost_impression_log.ip` also in `clickpass_log` — a household count, not a visit count.

**`matched IPs / served IPs` is the WRONG lens.** It tracks campaign audience against site size: Maurices (66784) 3.15% match / 0.40% of site traffic; Re-Bath Cherry Hill (39510) 0.13% match / 1.27%. **Use `verified visits / raw_visits` = share of site visits**, and rank it WITHIN a site-size quintile — corr(log site visits, log share) = -0.24, medians 1.09% → 0.39% across the range, so an unadjusted cut just picks big sites. Needs ≥1,000 reported site visits.

**Grain trap:** visit rate is a 30-day CUMULATIVE rate ≈ 6x the daily one. Medians: advertiser×30d **2.0%** · advertiser×day 0.42% · campaign×day **0.334%**. "0.2% IVR" is ordinary daily, bottom-fifth cumulatively. A 0.5% cumulative cut = bottom 28% of the base, not an outlier line.

**Denominator sanity check:** `raw_visits` reflects wherever the pixel is placed. ElevenLabs (51660) reports 21.5M visits/day vs 1.2M for WGU on 350M impressions — that is app/API telemetry, not marketing traffic, and every ratio built on it is uncomparable for that advertiser. Check the daily rate before acting on a low share.

**Zeros:** verified ≤ raw by construction, so a zero on a quiet site is arithmetic. Separate never-installed from broken with 12 months of `raw_visits` — an opt-out NEVER reports; a defect reports then stops. **`advertisers.conv_pixel_opt_out` is NOT backfilled** (0.00% TRUE for every creation year 2010-2021, 0.80% by 2025) and covers the CONVERSION pixel only; `pixel_id` is NULL for every row and `tracking_pixel_status_id`=10 for essentially all live advertisers, so neither discriminates. Route pixel questions to Johnny Chen or [[reference_pixel_ops_routing]].
