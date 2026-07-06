# The Bouqs eCommerce (32147) — Interest-logic deep-dive (targeting DNA + funnel gate)

**No campaign narrows MM with a required 3P segment.** MM×3P join across campaigns: ?/OR (OR = additive/broadening). The differentiator is a DS16 net-new funnel gate on the gated campaigns, not 3P.

## Per-campaign targeting DNA (ranked by % of prospecting spend)

| Campaign | % spend | Geo tier | MM kw | 3P seg | MM×3P | Funnel gate (DS16) | Reach | Net-new vs base | Read |
|---|--:|---|---:|---:|---|---|---:|---:|---|
| 119362 CTV eComm Prospecting 2026 | 36% | National · US | 187 | 4 | OR | **net-new (AND'd)** | 5.09M | 84% | net-new residual gate |
| 108055 CTV Prospecting MM VDay 2026 | 26% | Low · 84 | 120 | 0 | ? | — | 1.94M | 72% | geo slice |
| 85384 CTV eComm Prospecting 2026 -old | 19% | National · US | 187 | 4 | OR | — | 5.83M | — | broad · ungated (base) |
| 119363 CTV eComm High Frequency v2 Prospecting 2026 | 5% | National · US | 187 | 4 | OR | **net-new (AND'd)** | 431K | 76% | net-new residual gate |
| 119361 CTV eComm Low Frequency v2 Prospecting 2026 | 5% | National · US | 187 | 4 | OR | **net-new (AND'd)** | 490K | 79% | net-new residual gate |
| 116732 CTV Subscriptions Prospecting | 3% | National · US | 63 | 6 | OR | **net-new (AND'd)** | 449K | 38% | net-new residual gate |
| 117983 CTV eComm High Frequency Prospecting 2026 | 2% | National · US | 187 | 4 | OR | **net-new (AND'd)** | 278K | 76% | net-new residual gate |
| 117985 CTV eComm Low Frequency Prospecting 2026 | 2% | National · US | 187 | 4 | OR | **net-new (AND'd)** | 304K | 76% | net-new residual gate |
| 117987 CTV eComm Auto Frequency Prospecting 2026 | 1% | National · US | 187 | 4 | OR | **net-new (AND'd)** | 362K | 75% | net-new residual gate |

## The differentiator — DS16 net-new funnel gate
`AND ( NOT DS16[own Impressions/Wins]  OR  DS16[own campaign-group tag] )` — decoded via `tpa.categories` (data_source_id=16 = the advertiser's own funnel). Target a household **iff** it was NEVER impressed/won by this advertiser **OR** is already owned by this campaign = a **net-new-reach gate**.

## Empirical reach & net-new (BQ-native HLL on `sum_by_campaign_by_day`)

| Metric | Value |
|---|---|
| Base (85384 eComm   -old) reach | 5,828,969 distinct households |
| Gated-variant reach | 278K–5.09M (7 gated campaigns) |
| Base ∩ gated variant | **~72% net-new vs base** (avg across gated) |

**Read:** the gate narrows by WHO (net-new households), not by 3P — gated campaigns fish the residual net-new pool the ungated base already skipped.

