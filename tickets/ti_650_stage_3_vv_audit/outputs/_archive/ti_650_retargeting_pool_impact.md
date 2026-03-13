# TI-650: Impact of Adding Retargeting to S1 Pool

**Query:** `queries/ti_650_retargeting_pool_test.sql`
**Run:** 2026-03-12 | 204s wall time | adv 37775 S3 VVs only
**Test:** Compare prospecting-only S1 pool vs all-campaigns S1 pool (including retargeting obj=4)

## Results (adv 37775, S3 VVs, Feb 4–11, direct S3→S1 only — no chain)

| Scenario | Resolved | Unresolved | No Impression | Total |
|---|---|---|---|---|
| Prospecting-only S1 pool | 23,080 | 654 | 1,074 | 23,844 |
| All-campaigns S1 pool | 23,190 | 567 | 1,074 | 23,844 |
| **Retargeting net new** | **+110** | **-87** | — | — |

Note: "No Impression" = VVs where ad_served_id has no CIL record (can't resolve via IP regardless).

## Comparison with v13 Chain

| Approach | S3 Resolved | S3 Unresolved |
|---|---|---|
| v12: direct only, prosp pools | 23,080 | 674 |
| Direct only, all-campaigns pools | 23,190 | 567 |
| v13: chain, prosp pools | 23,214 | 540 |
| **Theoretical max: chain + all-campaigns pools** | **~23,280** | **~470** |

The S2 chain through prospecting campaigns (v13) resolves MORE than adding retargeting to direct —
23,214 vs 23,190. The chain captures S3→S2→S1 paths where vast_ip ≠ bid_ip at S2.

## Key Finding

**Adding retargeting to the S1 pool resolves 110 additional S3 VVs (14.4% of the 764 previously unresolved).**

These are IPs that:
1. Had a retargeting S1 impression (entered segment via retargeting/identity graph)
2. Did NOT have a prospecting S1 impression
3. Subsequently had a prospecting S3 VV

They are real MNTN impressions — just not prospecting ones. Whether to include them depends on
the audit scope:
- **Prospecting lineage only:** Exclude. These IPs didn't enter through the prospecting funnel.
- **Any MNTN impression lineage:** Include. The IP was served by MNTN at S1, regardless of objective.

## Still Unresolved (567 even with all campaigns)

567 S3 VVs remain unresolved even with all campaigns in the S1 pool. Plus 1,074 with no CIL record.
These are:
- Cross-device: different IP at S3 than any prior impression
- Identity graph only: entered targeting via CRM/LiveRamp with no prior MNTN ad at that IP
- IP rotation: CGNAT/VPN changed IP between impression and visit

**100% resolution is not achievable via IP matching.** The ~2.4% floor (567/23,844) represents the
true cross-device + identity-graph-only rate.
