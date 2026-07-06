# The Bouqs Subscriptions (31906, dark 2026) — Interest-logic deep-dive (targeting DNA + funnel gate)

**No campaign narrows MM with a required 3P segment.** MM×3P join across campaigns: OR (OR = additive/broadening). No net-new funnel gate present.

## Per-campaign targeting DNA (ranked by % of prospecting spend)

| Campaign | % spend | Geo tier | MM kw | 3P seg | MM×3P | Funnel gate (DS16) | Reach | Net-new vs base | Read |
|---|--:|---|---:|---:|---|---|---:|---:|---|
| 76699 CTV Prospecting Subscriptions | 100% | National · US | 223 | 3 | OR | — | 1.55M | — | broad · ungated (base) |

## The differentiator — DS16 net-new funnel gate
`AND ( NOT DS16[own Impressions/Wins]  OR  DS16[own campaign-group tag] )` — decoded via `tpa.categories` (data_source_id=16 = the advertiser's own funnel). Target a household **iff** it was NEVER impressed/won by this advertiser **OR** is already owned by this campaign = a **net-new-reach gate**.

## Empirical reach & net-new (BQ-native HLL on `sum_by_campaign_by_day`)

| Metric | Value |
|---|---|
| Base (76699 Subscriptions) reach | 1,548,937 distinct households |

**Read:** the gate narrows by WHO (net-new households), not by 3P — gated campaigns fish the residual net-new pool the ungated base already skipped.

