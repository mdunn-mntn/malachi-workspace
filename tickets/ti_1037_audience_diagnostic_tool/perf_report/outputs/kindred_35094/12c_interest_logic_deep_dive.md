# Kindred Bravely (35094) — Interest-logic deep-dive (targeting DNA + funnel gate)

**No campaign narrows MM with a required 3P segment.** MM×3P join across campaigns: OR (OR = additive/broadening). The differentiator is a DS16 net-new funnel gate on the gated campaigns, not 3P.

## Per-campaign targeting DNA (ranked by % of prospecting spend)

| Campaign | % spend | Geo tier | MM kw | 3P seg | MM×3P | Funnel gate (DS16) | Reach | Net-new vs base | Read |
|---|--:|---|---:|---:|---|---|---:|---:|---|
| 69884 CTV Prospecting High Pop | 36% | High · 20 | 255 | 11 | OR | — | 6.37M | — | broad · ungated (base) |
| 109926 CTV Prospecting Mid Pop | 30% | Mid · 38 | 255 | 11 | OR | — | 1.06M | 65% | geo slice |
| 96108 CTV Prospecting LowPop | 10% | Low · 152 | 255 | 14 | OR | — | 920K | 79% | geo slice |
| 115945 CTV Prospecting HiPop Motherhood-Journey | 8% | High · 20 | 255 | 11 | OR | **net-new (AND'd)** | 432K | 57% | net-new residual gate |
| 115943 CTV Prospecting HiPop Q1-2026-Harter | 8% | High · 20 | 255 | 11 | OR | **net-new (AND'd)** | 437K | 64% | net-new residual gate |
| 115946 CTV Prospecting HiPop Mom-Focus | 8% | High · 20 | 255 | 11 | OR | **net-new (AND'd)** | 437K | 60% | net-new residual gate |

## The differentiator — DS16 net-new funnel gate
`AND ( NOT DS16[own Impressions/Wins]  OR  DS16[own campaign-group tag] )` — decoded via `tpa.categories` (data_source_id=16 = the advertiser's own funnel). Target a household **iff** it was NEVER impressed/won by this advertiser **OR** is already owned by this campaign = a **net-new-reach gate**.

## Empirical reach & net-new (BQ-native HLL on `sum_by_campaign_by_day`)

| Metric | Value |
|---|---|
| Base (69884 High Pop) reach | 6,366,174 distinct households |
| Gated-variant reach | 432K–437K (3 gated campaigns) |
| Base ∩ gated variant | **~60% net-new vs base** (avg across gated) |

**Read:** the gate narrows by WHO (net-new households), not by 3P — gated campaigns fish the residual net-new pool the ungated base already skipped.

