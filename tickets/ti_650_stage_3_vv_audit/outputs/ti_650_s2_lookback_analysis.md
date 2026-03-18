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

**NOTE (v21c):** The "0 beyond 90d" metric uses `MAX(impression_time)` across ALL time (including post-VV events). The 10,227 "latest_after_vv" IPs have negative gap (latest S1 impression is AFTER the VV). A theoretical concern was raised that some IPs might have their latest pre-VV impression >90d away while having post-VV events masking this. **Empirical verification disproved this** — running the full v8 query with 90d S1 pool returns 68,498/68,498 = 100% resolved, identical to 180d.

### Verification: 90d vs 180d actual resolution
| Lookback | Resolved | Unresolved | % |
|----------|----------|------------|---|
| 180 days | 68,498 | 0 | 100% |
| 90 days | 68,498 | **0** | **100%** |

## Key Findings

1. **The 186-day max gap was an artifact of using MIN(impression_time).** When selecting the most recent S1 match instead of the earliest, the max drops to 69 days. The initial report to Zach was misleading.

2. **90-day lookback covers 100% of resolvable VVs.** Verified empirically: full v8 query (5-source IP trace + 4-table S1 pool + CIDR fix) with 90d lookback = 68,498/68,498. Zero unresolved.

3. **15% of IPs are still actively getting S1 impressions** even after the S2 VV — meaning the S1→S2 pipeline is continuously feeding these IPs.

4. **Zach's 88-day estimate (14+30+14+30) was actually close.** The empirical max of 69 days is within his theoretical window.

5. **WGU (adv 31357) has "abnormally long" S3 lookback** per Zach — but even for them, 90d is sufficient with the full resolution pipeline.

## What This Means for Production

- **90-day lookback is sufficient** for the S1 pool in production queries (with CIDR fix + all 4 source tables + clickpass_log)
- **180-day lookback is safe but wasteful** — scans 2x the data for zero additional resolution
- The 442 previously unresolved VVs were fixed by **CIDR stripping + expanded S1 pool tables**, NOT by the extended lookback

## BQ Performance
- 12,371 GB processed, 1,823s wall time (~30 min)
- Heavy query due to 4 source tables × 180 days × MIN+MAX aggregation
