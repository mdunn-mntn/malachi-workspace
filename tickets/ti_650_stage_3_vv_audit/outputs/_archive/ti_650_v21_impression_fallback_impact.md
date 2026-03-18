# TI-650: v21 Impression Fallback Impact — v20 vs v21 Comparison

**Query:** `queries/ti_650_resolution_rate_v21.sql`
**Run:** 2026-03-16 | 356s wall time | 4,640 GB billed | 10 advertisers
**Date range:** VVs Feb 4–11, 90-day lookback (Nov 6–Feb 11)
**Scoping:** Prospecting only (obj IN 1,5,6), funnel_level IN (1,2,3), campaign_group_id scoped

## Changes from v20

1. **S1 pool expanded:** Added viewability_log + impression_log (display coverage) alongside event_log + CIL
2. **S2 impression chain fallback (T3):** Re-added v14-style chain — S3.bid_ip → S2 impression IP (event_log/viewability_log/impression_log) → S2 bid_ip (CIL) → S1 pool

## S2 Results (UNCHANGED)

Identical to v20. No impact from S1 pool expansion on S2→S1 resolution.

## S3 Results — v20 vs v21

| Advertiser | Total VVs | v20 % | v21 % | Δ pp | v20 Unresolved | v21 Unresolved | New Resolutions |
|---|---|---|---|---|---|---|---|
| 31276 | 15,477 | 98.97% | **99.10%** | **+0.13** | 87 | 67 | +20 |
| 31357 | 589,630 | 74.54% | **78.77%** | **+4.23** | 148,299 | 123,360 | +24,939 |
| 32766 | 14,149 | 99.40% | 99.40% | 0.00 | 17 | 17 | 0 |
| 34835 | 33,875 | 99.34% | **99.35%** | **+0.01** | 131 | 127 | +4 |
| 35237 | 17,345 | 98.66% | **98.70%** | **+0.04** | 34 | 27 | +7 |
| 36743 | 5,874 | 99.40% | 99.40% | 0.00 | 6 | 6 | 0 |
| 37775 | 23,844 | 99.05% | **99.08%** | **+0.03** | 75 | 69 | +6 |
| 38710 | 14,838 | 99.15% | **99.16%** | **+0.01** | 20 | 19 | +1 |
| 42097 | 16,463 | 98.48% | **98.65%** | **+0.17** | 191 | 164 | +27 |
| 46104 | 15,021 | 99.47% | 99.47% | 0.00 | 14 | 14 | 0 |

## S3 Tier Breakdown (v21 — counts overlap)

| Advertiser | Total S3 | T1: VV Bridge Chain | T2: S1 VV Direct | T3: Imp Chain | T4: S1 Imp Direct | Resolved | Unresolved |
|---|---|---|---|---|---|---|---|
| 31276 | 15,477 | 8,990 | 8,007 | 11,587 | 364 | 15,338 | 67 |
| 31357 | 589,630 | 187,817 | 177,254 | 345,331 | 43,764 | 464,473 | 123,360 |
| 32766 | 14,149 | 3,373 | 10,341 | 8,251 | 647 | 14,064 | 17 |
| 34835 | 33,875 | 17,371 | 23,440 | 23,832 | 184 | 33,656 | 127 |
| 35237 | 17,345 | 8,979 | 9,131 | 15,240 | 278 | 17,120 | 27 |
| 36743 | 5,874 | 1,521 | 4,945 | 2,285 | 257 | 5,839 | 6 |
| 37775 | 23,844 | 9,421 | 17,526 | 13,585 | 1,450 | 23,624 | 69 |
| 38710 | 14,838 | 4,957 | 10,537 | 9,104 | 641 | 14,714 | 19 |
| 42097 | 16,463 | 11,399 | 5,082 | 10,857 | 230 | 16,240 | 164 |
| 46104 | 15,021 | 5,199 | 9,824 | 8,103 | 525 | 14,941 | 14 |

Note: Tiers overlap (a VV can match multiple paths). Resolved = distinct VVs matching ANY tier.

## Impact Summary

### Impression fallback is marginal for most advertisers, material for 31357

| Metric | v20 (VV bridge only) | v21 (+ imp fallback) | Delta |
|---|---|---|---|
| **37775 S3 unresolved** | 75 | 69 | **-6 (8% reduction)** |
| **31357 S3 unresolved** | 148,299 | 123,360 | **-24,939 (16.8% reduction)** |
| **42097 S3 unresolved** | 191 | 164 | **-27 (14.1% reduction)** |
| **All 10 advs total new resolutions** | — | — | **+25,004** |

### T3 (impression chain) fires extensively

T3 counts are large for every advertiser — often larger than T1 VV bridge chain. This means many S3.bid_ip values match BOTH prior VV clickpass IPs AND prior impression IPs. The overlap is expected: same-device scenarios produce matching IPs across both tables.

The incremental value of T3 (cases where T3 matches but T1/T2 don't) is small for 9/10 advertisers. The exception is **31357** where the impression chain resolved 24,939 additional VVs — likely same-device cases where the VV clickpass IP was different but the impression IP matched.

### T4 (expanded S1 impression direct) adds modest value

T4 counts show viewability_log and impression_log IPs in the S1 pool are catching some S3 VVs that event_log VAST + CIL missed. This is the display coverage improvement.

### 37775 reference advertiser

- v20: 99.05% (75 unresolved)
- v21: 99.08% (69 unresolved)
- The 6 additional resolutions came from the impression chain fallback
- Remaining 69 are the true irreducible floor — no prior VV or impression IP match in any table

## Key Findings

1. **VV bridge (T1+T2) remains the dominant resolution mechanism.** The impression fallback is additive but not transformative for most advertisers.
2. **31357 benefits most** from impression fallback (+4.23pp, 24,939 new resolutions). Heavy identity-graph-driven population has same-device cases where impression IPs provide additional coverage.
3. **S1 pool expansion (viewability_log + impression_log) provides marginal lift** — display impression IPs were mostly already covered by CIL.
4. **The 5-tier architecture is comprehensive.** The remaining unresolved (69 for 37775, 123,360 for 31357) represent the true floor — IPs with no prior MNTN touchpoint in any log table within the same campaign group.
