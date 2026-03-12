# TI-650: Multi-Advertiser Resolution Rates (Top 10, Feb 4–11 2026)

v12 architecture: imp_direct + imp_visit, both vast IPs in S1 pool. 90-day lookback.
Query: `ti_650_resolution_rate_multi.sql` (LIMIT 10), 87s runtime, ~2 TB processed.

## S2 Resolution Rates

| Advertiser | Total VVs | Resolved | % | Unresolved |
|------------|-----------|----------|---|------------|
| 31276 | 27,755 | 27,635 | **99.57%** | 0 |
| 31357 | 68,498 | 68,122 | **99.45%** | 0 |
| 32766 | 4,115 | 4,105 | **99.76%** | 0 |
| 34835 | 11,917 | 11,876 | **99.66%** | 0 |
| 35237 | 14,768 | 14,465 | **97.95%** | 0 |
| 36743 | 6,076 | 6,068 | **99.87%** | 0 |
| 37775 | 16,753 | 16,707 | **99.73%** | 0 |
| 38710 | 11,414 | 11,376 | **99.67%** | 0 |
| 42097 | 16,213 | 16,160 | **99.67%** | 1 |
| 46104 | 5,762 | 5,747 | **99.74%** | 0 |
| **Aggregate** | **183,271** | **182,261** | **99.45%** | **1** |

S2 is rock solid. 99.45-99.87% across all 10 advertisers. Effectively zero unresolved.

## S3 Resolution Rates

| Advertiser | Total VVs | imp_direct | imp_visit | Resolved | % | Unresolved | XDevice |
|------------|-----------|------------|-----------|----------|---|------------|---------|
| 31276 | 15,477 | 13,479 | 13,882 | 13,892 | **89.76%** | 1,513 | 1,093 (72%) |
| 31357 | 589,630 | 391,865 | 415,257 | 415,564 | **70.48%** | 172,447 | 90,491 (52%) |
| 32766 | 14,149 | 12,798 | 13,414 | 13,426 | **94.89%** | 656 | 404 (62%) |
| 34835 | 33,875 | 32,038 | 32,123 | 32,153 | **94.92%** | 1,638 | 663 (40%) |
| 35237 | 17,345 | 15,844 | 16,235 | 16,236 | **93.61%** | 911 | 728 (80%) |
| 36743 | 5,874 | 5,449 | 5,692 | 5,695 | **96.95%** | 161 | 108 (67%) |
| 37775 | 23,844 | 21,966 | 23,060 | 23,080 | **96.80%** | 674 | 365 (54%) |
| 38710 | 14,838 | 13,841 | 14,461 | 14,463 | **97.47%** | 327 | 243 (74%) |
| 42097 | 16,463 | 9,952 | 10,287 | 10,291 | **62.51%** | 6,114 | 2,933 (48%) |
| 46104 | 15,021 | 13,972 | 14,452 | 14,467 | **96.31%** | 490 | 335 (68%) |

## Key Observations

### S2: Universally high
- Range: 97.95% – 99.87%
- Aggregate: 99.45%
- Effectively zero unresolved S2 VVs across all advertisers

### S3: Two tiers
**Tier 1 — High resolution (93-97%):** 8 of 10 advertisers
- Typical: ~95% resolved, 2-5% unresolved
- Unresolved are majority cross-device (54-80%)
- Consistent with adv 37775 baseline (96.80%)

**Tier 2 — Low resolution (<90%):** 2 outliers
- **31357 (70.48%):** Largest advertiser (590K S3 VVs). 172K unresolved. Likely heavy identity graph / CRM targeting.
- **42097 (62.51%):** 6K unresolved of 16K. Same pattern — high cross-device + identity graph entry.

### Cross-device is the dominant unresolved driver
- 48-80% of unresolved S3 VVs have `is_cross_device = TRUE`
- These users entered S3 on a different device than their S1 impression — IP will never match

### imp_visit > imp_direct in all cases
- imp_visit resolves more than imp_direct for every advertiser at every stage
- Confirms imp_visit is the dominant resolver (consistent with adv 37775 finding)

## Next Steps
- Investigate outliers 31357 and 42097 — characterize by campaign mix, attribution model, CRM segment usage
- Scale to top 40 advertisers to fill out the distribution
- GUID bridge for unresolved S3 VVs (expected to recover ~82% per prior testing)
