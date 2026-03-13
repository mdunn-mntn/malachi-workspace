# TI-650 v18: Exhaustive IP Trace — Unresolved VV `80207c6e`

## Summary

IP `216.126.34.185` was searched across all 3 cross-stage connecting tables (event_log, viewability_log, impression_log) within campaign_group_id 93957, going back **2+ years** (Jan 2024 – Feb 2026). **Zero S1/S2 impressions found.** The only records found are the VV's own S3 impressions.

The IP **was** served S1 prospecting ads for the same advertiser (37775) — but in a different campaign_group (78903). Campaign_group_id scoping (Zach's directive) correctly prevents this from being a valid cross-stage link.

## VV Details

| Field | Value |
|---|---|
| ad_served_id | `80207c6e-1fb9-427b-b019-29e15fb3323c` |
| VV date | 2026-01-27 |
| IP | `216.126.34.185` |
| Advertiser | 37775 |
| Campaign | 450300 — Beeswax Television Multi-Touch Plus |
| Campaign Group | 93957 |
| Funnel Level | 3 (S3) |
| Channel | CTV (8) |
| Objective | 1 (Prospecting — broken obj, should be 6 per Ray) |
| **Is retargeting?** | **No** (obj=1, not obj=4) |

## Campaign Group 93957 — All Campaigns

| campaign_id | funnel_level | channel | objective | name |
|---|---|---|---|---|
| 450305 | 1 (S1) | CTV (8) | 1 | Beeswax Television Prospecting |
| 450301 | 2 (S2) | CTV (8) | 1 | Beeswax Television Multi-Touch |
| 450303 | 2 (S2) | Display (1) | 5 | Multi-Touch |
| **450300** | **3 (S3)** | **CTV (8)** | **1** | **Beeswax Television Multi-Touch Plus** ← VV |
| 450304 | 3 (S3) | Display (1) | 6 | Multi-Touch - Plus |
| 450302 | 4 (Ego) | CTV (8) | 7 | Beeswax Television Prospecting - Ego |

No deleted campaigns. No test campaigns.

## Search Results — Within Campaign Group 93957

All 3 cross-stage connecting tables checked. CIDR-safe (SPLIT on `/`). Lookback: Jan 2024 – Feb 2026.

### Query 1: event_log (CTV path — vast_start / vast_impression)

**Cross-stage key:** `S3.bid_ip → event_log.ip (vast_start or vast_impression)`

| Result | Count |
|---|---|
| S1/S2 campaigns (450305, 450301, 450303) | **0 records** |
| S3 campaigns (450300 — VV's own) | 2 records (Jan 25 + Jan 27, 2026) |

### Query 2: viewability_log (viewable display path)

**Cross-stage key:** `S3.bid_ip → viewability_log.ip`

| Result | Count |
|---|---|
| All campaigns in cg 93957 | **0 records** |

### Query 3: impression_log (non-viewable display path)

**Cross-stage key:** `S3.bid_ip → impression_log.ip`

| Result | Count |
|---|---|
| S1/S2 campaigns (450305, 450301, 450303) | **0 records** |
| S3 campaigns (450300 — VV's own) | 2 records (Jan 25 + Jan 27, 2026) |

## Search Results — Across ALL Campaigns for Advertiser 37775

When searching event_log across **all campaigns** for advertiser 37775 (no campaign_group filter):

| Campaign | campaign_group_id | funnel_level | channel | Events Found | Date Range |
|---|---|---|---|---|---|
| 311968 | **78903** | **1 (S1)** | CTV | ~6 events | Feb 24, 2025 |
| 311966 | **78903** | 3 (S3) | CTV | ~40+ events | Mar 31 – Apr 3, 2025 |

**Key finding:** The IP received S1 prospecting CTV ads in campaign_group **78903** (Feb 2025) — 11 months before the VV in campaign_group **93957** (Jan 2026). The IP was a real MNTN viewer, just for a different campaign group.

## Conclusion

This VV is correctly unresolvable under campaign_group_id scoping:

1. **Within campaign_group 93957:** Zero S1/S2 impressions for this IP across all 3 connecting tables, 2+ years of data, CIDR-safe search.
2. **Across the advertiser:** IP had S1 exposure in a different campaign_group (78903). Campaign_group scoping correctly prevents this from being linked.
3. **Not retargeting:** VV campaign (450300) is obj=1/funnel_level=3 (S3 prospecting), not retargeting.
4. **Identity graph entry:** This IP entered S3 targeting for campaign_group 93957 via identity graph (data_source_id=3), not via any prior MNTN impression within the same campaign group.

## Queries

`queries/ti_650_v18_exhaustive_ip_trace.sql` — all 4 queries (3 within cg + 1 cross-advertiser)
