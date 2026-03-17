# TI-650: S2→S1 Lookback Gap Analysis — Earliest vs Latest Match

**Advertiser:** 31357 (WGU — Western Governors University, ~30% MNTN monthly spend)
**VV window:** Feb 4–11, 2026 | **S1 pool lookback:** 180 days | **Scoped to:** campaign_group_id
**Query:** `queries/ti_650_s2_lookback_analysis.sql`

## Results

| Metric | EARLIEST match (MIN — biased) | MOST RECENT match (MAX — correct) |
|--------|------|------|
| **Max** | 186 days | **69 days** |
| **Median** | 136 days | **6 days** |
| **P95** | 183 days | **29 days** |
| **P99** | 185 days | **35 days** |
| Within 90d | 25,402 (37%) | 58,265 (85%) |
| Beyond 90d | 43,090 (63%) | **0 (0%)** |

**Total matched:** 68,492

### Distribution of latest S1 match timing
| Bucket | Count | % |
|--------|-------|---|
| Latest S1 match BEFORE VV, within 90d | 58,265 | 85.1% |
| Latest S1 match AFTER VV (IP still active in S1) | 10,227 | 14.9% |
| Latest S1 match >90d before VV | 0 | 0% |

## Key Findings

1. **The 186-day max gap was an artifact of using MIN(impression_time).** When selecting the most recent S1 match instead of the earliest, the max drops to 69 days. The initial report to Zach was misleading.

2. **90-day lookback covers 100% of resolvable VVs.** Zero IPs have their latest S1 match more than 90 days before the S2 VV. Every resolvable IP has at least one S1 event within the past 90 days.

3. **15% of IPs are still actively getting S1 impressions** even after the S2 VV — meaning the S1→S2 pipeline is continuously feeding these IPs.

4. **Zach's 88-day estimate (14+30+14+30) was actually close.** The empirical max of 69 days is within his theoretical window.

5. **WGU (adv 31357) has "abnormally long" S3 lookback** per Zach — but even for them, 90d is sufficient when using the most recent match.

## What This Means for Production

- **90-day lookback is sufficient** for the S1 pool in production queries (with CIDR fix + all 4 source tables + clickback_log)
- **180-day lookback is safe but wasteful** — scans 2x the data for zero additional resolution
- The 442 previously unresolved VVs were likely fixed by **CIDR stripping + expanded S1 pool tables**, NOT by the extended lookback

## BQ Performance
- 12,371 GB processed, 1,823s wall time (~30 min)
- Heavy query due to 4 source tables × 180 days × MIN+MAX aggregation
