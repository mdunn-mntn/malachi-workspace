# TI-650: S3 Lookback vs Resolution Analysis — Adv 31357 (WGU)

## Summary

Two queries ran against the same S3 VV population (589,630 VVs, Feb 4-11, adv 31357):

1. **Lookback analysis** (180d VV pool, VV-pool-only): 589,627 matched (99.999%)
2. **Resolution query** (90d, T1-T4 tiers): 568,839 resolved (96.47%)

**Gap: 20,788 VVs** — matched in lookback but unresolved in resolution.

## Decomposition of the 20,788 Gap

The lookback query uses a **180d VV pool** but only a **90d IP trace window**. It matches S3 VVs to S1/S2 clickpass_log IPs using simple (campaign_group_id + IP) join with temporal precedence (`earliest_time < vv_time`).

The resolution query uses a **90d lookback** for everything and has more complex tier joins (T1 chain requires S2 VV → S2 bid_ip → S1 pool, not just IP match).

### Bucket analysis from lookback query:

| Bucket | Count | % of matched |
|--------|-------|-------------|
| within_90d (gap_latest 0-90d) | 569,224 | 96.54% |
| beyond_90d (gap_latest >90d) | 1,858 | 0.32% |
| after_vv (gap_latest <0d) | 18,545 | 3.15% |
| **Total matched** | **589,627** | **100%** |

### Resolution query coverage:

| Metric | Count | % |
|--------|-------|---|
| Resolved (T1+T2+T3) | 568,839 | 96.47% |
| Unresolved | 20,791 | 3.53% |
| **Total** | **589,630** | **100%** |

### Gap decomposition:

The 569,224 "within 90d" from lookback vs 568,839 resolved from resolution → **385 VVs within 90d don't resolve through the tier structure**. This is the tier join overhead — VVs that match by simple IP+campaign_group but fail the more complex T1/T2/T3 joins.

The remaining 20,788 - 385 = **20,403** are due to the lookback window difference:
- **1,858 beyond_90d**: Most recent S1/S2 VV was >90d ago. Resolution query pools don't include these.
- **18,545 after_vv**: Most recent S1/S2 VV was AFTER the S3 VV, but these VVs also had an EARLIER match (per `earliest_time < vv_time` requirement). Their **earliest** match may have been >90d ago, falling outside the resolution query's 90d pool window.

## Key Insight

The 18,545 "after VV" VVs are the largest contributor to the gap. These are IPs with S1/S2 VV activity both before AND after the S3 VV — actively churning through the lower funnel. Their earliest S1/S2 VV (the one that temporally preceded the S3 VV) is likely older than 90d, putting it outside the resolution query's pool window.

## Recommendation

**Re-run resolution with 180d lookback** (submitted as `bqjob_r3eaa2fec2525504c_0000019d017dd2d0_1`, 18.2 TB).

Expected outcomes:
- Most of the 20,788 gap should close — particularly the 18,545 "after VV" VVs whose earliest match falls in the 91-180d range
- Resolution should approach ~99.5% (matching lookback's 99.999% minus tier join overhead)
- The 3 truly unmatched VVs (from lookback) will remain unresolved
- Some tier join overhead VVs (~385) may persist

## Production Lookback Recommendation

| Advertiser Type | Recommended Lookback |
|-----------------|---------------------|
| Normal (most advertisers) | 90d (confirmed sufficient via S2→S1 analysis, most S3 advertisers 98-99% at 90d) |
| WGU / extreme spend | 180d (P99=89d, max=152d for S3 VV pool gap) |
| Production default | 120d (covers P99+margin for WGU, adds ~33% scan cost vs 90d) |

Per Zach: WGU has "abnormally long S3 lookback window." The 180d lookback data confirms this — S3 lookback requirements are much longer than S2→S1 (max 69d).
