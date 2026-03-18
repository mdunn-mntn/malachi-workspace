# TI-650: Bid IP Divergence Analysis Results

**Date:** 2026-03-16
**Hypothesis:** Do any of the 7 S3 impressions for IP `216.126.34.185` in campaign 450300 (cg 93957) have a different `bid_ip`? If so, does that different IP have S1/S2 history in cg 93957?

## Result: No IP Divergence — All 7 Identical

**Every ad_served_id has `216.126.34.185` at every pipeline stage.** There are no new IPs to investigate.

## Full Pipeline Trace

| ad_served_id | Impression Date | bid_logs | win_logs | impression_log | event_log | clickpass_log | IP at ALL stages |
|---|---|---|---|---|---|---|---|
| `d65c4799` | 2026-01-25 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | — | — | 216.126.34.185 |
| `80207c6e` | 2026-01-27 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 |
| `3887adcd` | 2026-02-09 | 216.126.34.185 | — | 216.126.34.185 | — | — | 216.126.34.185 |
| `bb657a8b` | 2026-02-11 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | — | 216.126.34.185 |
| `74c6a03d` | 2026-02-22 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | — | 216.126.34.185 |
| `f5df758c` | 2026-02-22 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | — | 216.126.34.185 |
| `c890f55a` | 2026-02-23 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 | 216.126.34.185 |

### Pipeline coverage detail

- **d65c4799 (Jan 25):** Bid → Win → Impression only. No VAST delivery (creative never played). No VV.
- **80207c6e (Jan 27):** Full chain. **Original VV** (clickpass 2026-02-04). Already fully traced in v19.
- **3887adcd (Feb 9):** Bid → Impression only. No win, no VAST, no VV. Bid was placed but auction may not have been won, or win record missing.
- **bb657a8b (Feb 11):** Bid → Win → Impression → VAST. Full creative delivery, no VV (user didn't visit).
- **74c6a03d (Feb 22):** Bid → Win → Impression → VAST. Full creative delivery, no VV.
- **f5df758c (Feb 22):** Bid → Win → Impression → VAST. Full creative delivery, no VV. (Duplicate bid_log entry.)
- **c890f55a (Feb 23):** Full chain. **Second VV** (clickpass 2026-02-26, 3 days after impression). Same IP.

### VV summary

2 of 7 impressions became verified visits:
1. `80207c6e` — VV on 2026-02-04 (8 days after impression)
2. `c890f55a` — VV on 2026-02-26 (3 days after impression)

Both had identical IP at clickpass: `216.126.34.185`.

## Step 3: Cross-Stage Search — Not Needed

Since ALL 7 ad_served_ids have the exact same bid_ip (`216.126.34.185`), there are no new IPs to search for S1/S2 impressions in cg 93957. **Step 3 is moot.**

## Conclusion

**The bid IP divergence hypothesis is disproven.** This household used the same IP (`216.126.34.185`) for every single auction, impression, VAST delivery, and visit across all 7 S3 impressions over a 30-day period (Jan 25 – Feb 23, 2026). The IP never varied at any pipeline stage.

Combined with v18's exhaustive 2-year search confirming zero S1/S2 impressions for this IP in cg 93957 across all 3 cross-stage tables (event_log, viewability_log, impression_log), this conclusively confirms:

**This household entered S3 targeting entirely via the identity graph (data_source_id=3/4 in tmul_daily), not via any prior MNTN impression within campaign group 93957.** No alternative IP pathway exists.

### What this means for the audit

The ~8% unresolved rate (v14) is structural and correct:
- The IP-based ceiling of ~92% reflects the fraction of S3 VVs whose households had prior MNTN S1/S2 impressions within the same campaign group
- The remaining ~8% were placed into S3 targeting by the identity graph based on external data sources (CRM, LiveRamp), not by IP matching to prior MNTN ad exposure
- GUID bridge recovers ~85% of these (bringing total to ~99.6%), confirming the identity resolution is valid — just not IP-traceable
- This specific IP is a textbook example: stable IP, 7 S3 impressions, zero S1/S2 history in its campaign group, present in ipdsc with CRM data_source_id=4

## Queries Used

1. **clickpass_log lookup** (6 other ad_served_ids): Found 1 additional VV (`c890f55a`)
2. **impression_log** (all 7): All `bid_ip = impression_ip = 216.126.34.185`, extracted `ttd_impression_id` for downstream joins
3. **event_log** (all 7): 5/7 have vast_start + vast_impression, all `ip = bid_ip = 216.126.34.185`
4. **bid_logs** (all 7 auction_ids): All 7 present, `bid_ip = 216.126.34.185`
5. **win_logs** (all 7 auction_ids): 6/7 present (missing `3887adcd`), all `win_ip = 216.126.34.185`
