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

**CORRECTION (v21c):** The "0 beyond 90d" metric has a measurement flaw. `gap_latest_days` uses `MAX(impression_time)` across ALL time (including post-VV events), NOT `MAX(impression_time WHERE time < vv_time)`. The 10,227 "latest_after_vv" IPs have negative gap (latest S1 impression is AFTER the VV), so they pass the ≤90d check trivially. But 6 of those 10,227 IPs have their latest **PRE-VV** S1 impression >90d before the VV — while also having S1 impressions after the VV that masked this in the analysis.

### Verification: 90d vs 180d actual resolution
| Lookback | Resolved | Unresolved | % |
|----------|----------|------------|---|
| 180 days | 68,498 | 0 | 100% |
| 90 days | 68,492 | **6** | 99.99% |

The 6 edge cases: IPs whose only pre-VV S1 impressions are in the 90-180d window, but also have post-VV S1 events (still actively being served). The lookback analysis correctly identified that the MAX gap for IPs with only pre-VV events is 69d. But it failed to separately measure the pre-VV gap for IPs with post-VV events.

## Key Findings

1. **The 186-day max gap was an artifact of using MIN(impression_time).** When selecting the most recent S1 match instead of the earliest, the max drops to 69 days. The initial report to Zach was misleading.

2. **90-day lookback covers 99.99% of resolvable VVs (CORRECTED from 100%).** 68,492/68,498 VVs resolve with 90d. 6 IPs (0.009%) need >90d lookback — their only pre-VV S1 impressions are older than 90 days, masked in the gap analysis by having post-VV S1 events.

3. **15% of IPs are still actively getting S1 impressions** even after the S2 VV — meaning the S1→S2 pipeline is continuously feeding these IPs. 6 of these 10,227 have no pre-VV S1 impression within 90d.

4. **Zach's 88-day estimate (14+30+14+30) was actually close.** The empirical max of 69 days (for IPs with only pre-VV events) is within his theoretical window.

5. **WGU (adv 31357) has "abnormally long" S3 lookback** per Zach — but even for them, 90d is sufficient for 99.99% when using the most recent match.

## What This Means for Production

- **90-day lookback is recommended** for the S1 pool in production queries — 99.99% resolution, half the scan cost
- **180-day lookback achieves true 100%** — 6 additional VVs resolved, 2x the data scanned
- **The 6 edge cases are negligible** — 0.009% of VVs, not worth doubling scan cost
- The 442 previously unresolved VVs were fixed by **CIDR stripping + expanded S1 pool tables**, NOT the extended lookback (90d resolves 68,492 vs the prior 68,056 without CIDR fix)

## BQ Performance
- 12,371 GB processed, 1,823s wall time (~30 min)
- Heavy query due to 4 source tables × 180 days × MIN+MAX aggregation
