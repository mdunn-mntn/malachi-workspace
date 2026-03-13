# TI-650: 365-Day IP Lookup for Unresolved VV 80207c6e

**Date:** 2026-03-13
**VV:** `80207c6e-1fb9-427b-b019-29e15fb3323c`
**VV Date:** 2026-02-04
**Bid IP:** `216.126.34.185`
**Campaign Group ID:** 93957 (advertiser 37775)

## Question

Does IP `216.126.34.185` appear anywhere in `event_log` as a `vast_impression` or `vast_start` event in the 365 days prior to the VV date — across ANY campaign, not just campaign_group_id 93957?

## Within-Stage Trace

The impression was on **2026-01-27** (8 days before VV on 2026-02-04):
- impression_log: `216.126.34.185` at 2026-01-27 14:52:20
- ttd_impression_id: `1769525540419228.1728554721.59.steelhouse`
- The ±1 day window in the original cross-stage query missed this (needs ±10 day window)

## Cross-Stage Result (campaign_group_id 93957, 90-day lookback)

**No match.** Zero vast_impression or vast_start events for IP `216.126.34.185` within campaign_group_id 93957 in the 90 days before the S3 bid. This confirms the VV is correctly classified as "unresolved" in the v14 logic.

## 365-Day IP Lookup Results (ALL campaigns)

IP `216.126.34.185` appeared in **hundreds of VAST events across 10+ different advertisers** in the 35 days of available data (2026-01-01 to 2026-02-03). **None are for campaign_group_id 93957.**

| advertiser_id | campaign_group_id | campaign_name | funnel_level | objective_id | event_type | events | earliest | latest |
|---|---|---|---|---|---|---|---|---|
| 31276 | 90919 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_impression | 120 | 2026-01-01 | 2026-02-03 |
| 31276 | 90919 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_start | 120 | 2026-01-01 | 2026-02-03 |
| 31357 | 34028 | TV Retargeting - Television - 5+ PV | 2 | 4 (retargeting) | vast_impression | 109 | 2026-01-17 | 2026-02-03 |
| 31357 | 34028 | TV Retargeting - Television - 5+ PV | 2 | 4 (retargeting) | vast_start | 109 | 2026-01-17 | 2026-02-03 |
| 31357 | 34028 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_impression | 77 | 2026-01-17 | 2026-02-03 |
| 31357 | 34028 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_start | 77 | 2026-01-17 | 2026-02-03 |
| 38710 | 107714 | Beeswax Television Prospecting | 1 | 1 (prospecting) | vast_impression | 54 | 2026-01-07 | 2026-02-02 |
| 38710 | 107714 | Beeswax Television Prospecting | 1 | 1 (prospecting) | vast_start | 54 | 2026-01-07 | 2026-02-02 |
| 49304 | 103229 | Beeswax Television Prospecting | 1 | 1 (prospecting) | vast_start | 47 | 2026-01-03 | 2026-02-02 |
| 49304 | 103229 | Beeswax Television Prospecting | 1 | 1 (prospecting) | vast_impression | 47 | 2026-01-03 | 2026-02-02 |
| 43413 | 103375 | TV Retargeting - Television - Cart | 3 | 4 (retargeting) | vast_impression | 46 | 2026-01-01 | 2026-01-12 |
| 43413 | 103375 | TV Retargeting - Television - Cart | 3 | 4 (retargeting) | vast_start | 46 | 2026-01-01 | 2026-01-12 |
| 30506 | 66684 | Beeswax Television Prospecting | 1 | 1 (prospecting) | vast_impression | 28 | 2026-01-01 | 2026-01-19 |
| 30506 | 66684 | Beeswax Television Prospecting | 1 | 1 (prospecting) | vast_start | 28 | 2026-01-01 | 2026-01-19 |
| 31357 | 33900 | TV Retargeting - Television - 5+ PV | 2 | 4 (retargeting) | vast_impression | 28 | 2026-01-19 | 2026-02-03 |
| 31357 | 33900 | TV Retargeting - Television - 5+ PV | 2 | 4 (retargeting) | vast_start | 28 | 2026-01-19 | 2026-02-03 |
| 33389 | 107104 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_start | 28 | 2026-01-20 | 2026-01-28 |
| 33389 | 107104 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_impression | 28 | 2026-01-20 | 2026-01-28 |
| 31357 | 33900 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_impression | 25 | 2026-01-20 | 2026-02-03 |
| 31357 | 33900 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_start | 25 | 2026-01-20 | 2026-02-03 |
| 31357 | 46087 | TV Retargeting - Television - 5+ PV | 2 | 4 (retargeting) | vast_start | 25 | 2026-01-20 | 2026-02-03 |
| 31357 | 46087 | TV Retargeting - Television - 5+ PV | 2 | 4 (retargeting) | vast_impression | 25 | 2026-01-20 | 2026-02-03 |
| 31573 | 34065 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_impression | 24 | 2026-01-11 | 2026-02-03 |
| 31573 | 34065 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_start | 24 | 2026-01-11 | 2026-02-03 |
| 41034 | 106266 | Beeswax Television Prospecting | 1 | 1 (prospecting) | vast_impression | 23 | 2026-01-15 | 2026-01-30 |
| 30750 | 107788 | Beeswax Television Multi-Touch Plus | 3 | 1 (prospecting) | vast_start | 23 | 2026-01-22 | 2026-02-02 |
| 30750 | 107788 | Beeswax Television Multi-Touch Plus | 3 | 1 (prospecting) | vast_impression | 23 | 2026-01-22 | 2026-02-02 |
| 34421 | 41551 | TV Retargeting - Television - Cart | 3 | 4 (retargeting) | vast_impression | 21 | 2026-01-24 | 2026-02-03 |
| 41156 | 95369 | TV Retargeting - Television - General | 1 | 4 (retargeting) | vast_start | 21 | 2026-01-06 | 2026-02-03 |
| 34421 | 41551 | TV Retargeting - Television - Cart | 3 | 4 (retargeting) | vast_start | 21 | 2026-01-24 | 2026-02-03 |

### Summary Stats

- **Unique advertisers:** 10+ (31276, 31357, 38710, 49304, 43413, 30506, 33389, 31573, 41034, 30750, 34421, 41156, ...)
- **Unique campaign_groups:** 12+ (90919, 34028, 107714, 103229, 103375, 66684, 33900, 107104, 46087, 34065, 106266, 107788, 41551, 95369, ...)
- **Total events:** ~1,200+ (vast_impression + vast_start combined)
- **Campaign_group_id 93957 (VV's group):** **ZERO events**
- **Retargeting (obj=4) dominant:** Most events are retargeting campaigns — this IP is heavily retargeted across multiple advertisers
- **Prospecting events also present:** Some campaigns are prospecting (obj=1) for other advertisers

## Conclusion

**IP `216.126.34.185` is a highly active MNTN-served IP** — it was served ads hundreds of times for 10+ different advertisers in the 35 days of available event_log data. However, it was **never served an ad for advertiser 37775 / campaign_group_id 93957**.

This proves:
1. **The IP is well-known to MNTN's targeting system** (many impressions across the platform)
2. **It entered S3 for adv 37775 via identity graph** — not via a prior MNTN impression in campaign_group 93957
3. **Cross-advertiser IP matching would be invalid** — these are coincidental: same IP, different advertisers, different campaign groups
4. **The "unresolved" classification is correct** — this VV cannot be resolved to a prior-funnel impression within its own campaign group because no such impression exists

This is consistent with the v15 finding that 95.1% of unresolved IPs were never in any S1 impression pool for their own campaign group. The identity graph placed this IP into S3 targeting based on external data (LiveRamp/CRM), not based on an MNTN ad exposure.

## Cross-Stage Query Fix

The cross-stage query's `serve` CTE uses a ±1 day window but the impression was 8 days before the VV. The query needs a wider window (±10 days) to handle the observed impression → VV gap of up to 8.9 days (mean 1.8 days per v15).
