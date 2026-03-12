# TI-650: Profile of 567 Irreducible Unresolved S3 VVs

**Query:** `queries/ti_650_unresolved_567_profile.sql`
**Run:** 2026-03-12 | 113s wall | 1,469 GB processed
**Pool:** ALL campaigns (incl retargeting obj=4) in S1 pool — widest possible
**Advertiser:** 37775 | **Trace:** Feb 4–11 | **Lookback:** 90 days

## Summary

| Dimension | Value | % |
|-----------|-------|---|
| **Total unresolved** | **567** | — |
| Cross-device = true | 310 | 54.7% |
| Cross-device = false | 257 | 45.3% |
| Primary attribution (models 1-3) | 183 | 32.3% |
| Competing attribution (models 9-11) | 384 | 67.7% |
| T-Mobile CGNAT (172.5x) | 396 | 69.8% |
| IP never in S1 pool (any time) | 539 | 95.1% |
| IP in S1 pool (wrong temporal order) | 28 | 4.9% |
| GUID in guid_identity_daily | 567 | **100.0%** |
| Has first_touch_ad_served_id | 4 | 0.7% |
| Distinct bid IPs | 467 | — |

## Attribution Model Detail

| Model | Count | Description |
|-------|-------|-------------|
| 1 (GUID) | 64 | Primary GUID-based |
| 2 (IP) | 98 | Primary IP-based |
| 3 (GA Client ID) | 21 | Primary GA-based |
| 9 (Competing GUID) | 193 | Competing GUID-based |
| 10 (Competing IP) | 165 | Competing IP-based |
| 11 (Competing GA) | 26 | Competing GA-based |

## Impression-to-VV Time Gap

| Metric | Days |
|--------|------|
| Average | 2.3 |
| Minimum | 0.0 |
| Maximum | 19.4 |

## Key Findings

1. **100% GUID bridge potential.** Every single one of the 567 unresolved VVs has a GUID present in `guid_identity_daily`. This is the highest possible GUID bridge coverage — Q2 will determine how many actually resolve.

2. **95.1% IP never appeared in S1 pool at any time** (539/567). Consistent with prior 752 cohort (96.7%). These are pure identity graph entries — the IP was never served any MNTN impression. The 28 that do appear in S1 are CGNAT IP recycling (temporal mismatch).

3. **69.8% T-Mobile CGNAT** (396/567). Up from ~66% in the 752 cohort. As easier cases are resolved, CGNAT concentration increases.

4. **67.7% competing attribution** (384/567). Similar to prior 68%. These are secondary attribution VVs — not the primary conversion path.

5. **Only 4 have first_touch_ad_served_id.** This alternate linking path is essentially unavailable for the unresolved cohort.

6. **Cross-device rate = 54.7%.** Consistent with prior 55%. Over half switched devices between impression and visit.

## Comparison with Prior 752 Cohort

| Dimension | 752 (v12 prosp-only) | 567 (all-campaigns) | Direction |
|-----------|---------------------|---------------------|-----------|
| IP never in S1 | 96.7% | 95.1% | ≈ same |
| Cross-device | 55% | 54.7% | ≈ same |
| Competing attribution | 68% | 67.7% | ≈ same |
| T-Mobile CGNAT | ~66% | 69.8% | ↑ harder cases |
| GUID in identity_daily | ~95% (est) | 100.0% | ↑ all available |

The 567 is a purer subset of the 752 — the 185 resolved by retargeting pool or chain were the easier cases. The remaining 567 are structurally identical but slightly more concentrated in CGNAT.
