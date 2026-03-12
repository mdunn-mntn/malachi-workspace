# TI-650: v13 Resolution Rates — Full S3→S2→S1 Chain

**Query:** `queries/ti_650_resolution_rate_v13.sql`
**Run:** 2026-03-11 | 173s wall time | 2,862 GB billed | 10 advertisers
**Date range:** VVs Feb 4–11, 90-day lookback (Nov 6–Feb 11)
**Scoping:** Prospecting only (obj IN 1,5,6), funnel_level IN (1,2,3)

## S2 Results (single hop to S1)

| Advertiser | Total VVs | Resolved | % | Unresolved |
|---|---|---|---|---|
| 31276 | 27,755 | 27,635 | 99.57% | 0 |
| 31357 | 68,498 | 68,122 | 99.45% | 0 |
| 32766 | 4,115 | 4,105 | 99.76% | 0 |
| 34835 | 11,917 | 11,876 | 99.66% | 0 |
| 35237 | 14,768 | 14,465 | 97.95% | 0 |
| 36743 | 6,076 | 6,068 | 99.87% | 0 |
| 37775 | 16,753 | 16,707 | 99.73% | 0 |
| 38710 | 11,414 | 11,376 | 99.67% | 0 |
| 42097 | 16,213 | 16,160 | 99.67% | 1 |
| 46104 | 5,762 | 5,747 | 99.74% | 0 |

**S2 summary:** 97.95–99.87% resolution. Near-perfect. Unchanged from v12.

## S3 Results (chain + direct)

| Advertiser | Total VVs | via S2→S1 | Direct S1 | Resolved | % | Unresolved | Unres xDevice |
|---|---|---|---|---|---|---|---|
| 31276 | 15,477 | 3 | 13,889 | 13,892 | 89.76% | 1,513 | 1,093 |
| 31357 | 589,630 | 0 | 415,564 | 415,564 | 70.48% | 172,447 | 90,491 |
| 32766 | 14,149 | 8,239 | 5,367 | 13,606 | 96.16% | 476 | 288 |
| 34835 | 33,875 | 0 | 32,153 | 32,153 | 94.92% | 1,638 | 663 |
| 35237 | 17,345 | 303 | 15,933 | 16,236 | 93.61% | 911 | 728 |
| 36743 | 5,874 | 2,865 | 2,835 | 5,700 | 97.04% | 156 | 104 |
| 37775 | 23,844 | 14,182 | 9,032 | 23,214 | 97.36% | 540 | 298 |
| 38710 | 14,838 | 10,981 | 3,535 | 14,516 | 97.83% | 274 | 202 |
| 42097 | 16,463 | 0 | 10,291 | 10,291 | 62.51% | 6,114 | 2,933 |
| 46104 | 15,021 | 8,732 | 5,821 | 14,553 | 96.88% | 404 | 284 |

## Key Findings

### 1. The S2 chain is significant — for advertisers that have S2 campaigns

Six of ten advertisers show S3→S2→S1 chain resolution:
- **37775:** 14,182 via chain (59.5% of all S3), 9,032 direct (37.9%)
- **38710:** 10,981 via chain (74.0%), 3,535 direct (23.8%)
- **46104:** 8,732 via chain (58.1%), 5,821 direct (38.8%)
- **32766:** 8,239 via chain (58.2%), 5,367 direct (37.9%)
- **36743:** 2,865 via chain (48.8%), 2,835 direct (48.3%)
- **35237:** 303 via chain (1.7%), 15,933 direct (92.0%)

Four advertisers show **zero chain resolution** (31276, 31357, 34835, 42097) — these likely have minimal or no S2 campaigns in their prospecting funnel.

### 2. Chain adds net new resolutions (v13 vs v12 for adv 37775)

| Metric | v12 (direct only) | v13 (chain + direct) | Delta |
|---|---|---|---|
| S3 Resolved | 23,080 | 23,214 | +134 |
| S3 Unresolved | 674 | 540 | -134 |
| S3 % | 96.80% | 97.36% | +0.56pp |

The chain added **134 net new resolutions** for adv 37775 — S3 VVs whose bid_ip matched an S2 vast_ip (but not an S1 vast_ip directly), and the S2 impression's bid_ip DID match S1. This is the ~1.2% vast_ip ≠ bid_ip case amplified through the chain.

### 3. Outlier: adv 31357 (70.48%) and adv 42097 (62.51%)

These two advertisers have very low S3 resolution. Prior analysis (v12) showed unresolved S3 VVs trace overwhelmingly to identity-graph origin (LiveRamp/CRM) — IPs that entered targeting through the identity graph rather than through prior MNTN impressions. These are not "lost" — they are correctly unresolvable via IP-based lineage.

### 4. Cross-device accounts for ~52-80% of unresolved

Consistent with v12 findings. is_cross_device = TRUE strongly correlates with unresolvable — different device at S3 means different IP, no IP-based chain possible.

## Performance

- **Wall time:** 173s (target <300s) ✓
- **Bytes billed:** 2,862 GB (~2x the v12 query's ~1,500 GB, as expected)
- **Slot time:** 79,784s (131 stages)
- **Cache:** cold run (false)
