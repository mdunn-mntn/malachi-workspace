# Kindred Bravely (35094) — Interest-logic deep-dive (targeting DNA + funnel gate)

**No campaign narrows MM with a required 3P segment.** All 6 prospecting campaigns share the same interest core `(MM DS19[255 kw] OR 3P DS35[11–14 maternity/baby segs])` — OR = additive/broadening. The suspected `MM AND 3P` pattern is absent everywhere.

## Per-campaign targeting DNA

| Campaign | Geo tier · #DMAs | MM kw | 3P seg | MM×3P | Funnel gate (DS16) | Read |
|---|---|---:|---:|---|---|---|
| 69884 High Pop (base) | High · 20 | 255 | 11 | OR (additive) | — | broad · ungated |
| 109926 Mid Pop | Mid · 38 | 255 | 11 | OR (additive) | — | clean geo slice |
| 96108 Low Pop | Low · 152 | 255 | 14 | OR (additive) | — | clean geo slice |
| 115943 HiPop Harter | High · 20 | 255 | 11 | OR (additive) | **net-new only (AND'd)** | net-new residual gate |
| 115945 HiPop Motherhood-J | High · 20 | 255 | 11 | OR (additive) | **net-new only (AND'd)** | net-new residual gate |
| 115946 HiPop Mom-Focus | High · 20 | 255 | 11 | OR (additive) | **net-new only (AND'd)** | net-new residual gate |

## The differentiator — the 3 Q1-2026 variants add a DS16 funnel gate
`AND ( NOT DS16[7291 Impressions, 787280 Wins]  OR  DS16[own campaign-group] )` — decoded via `tpa.categories` (data_source_id=16 = the advertiser's own funnel). Target a household **iff** it was NEVER impressed/won by Kindred **OR** is already owned by this variant = a **net-new-reach gate**.

## Empirical narrowing (BQ-native HLL reach on `sum_by_campaign_by_day`, Jan–May '26)

| Metric | Value |
|---|---|
| Base High Pop reach | 1,643,877 distinct households |
| Each variant reach | ~435K = **~26% of base** (¼ the pool) |
| Base ∩ variant | ~27% → **~72% net-new vs base** |
| Variant ∩ variant | ~9% → **~90% mutually disjoint** (3-way creative split) |

**Rotation:** base High Pop (ungated, ROAS 2.39x) wound down Jan→Mar and went dark by April; the 3 gated variants (ROAS 1.18–1.35x) ramped up to replace it — so by May, top-20 prospecting is run by the gated variants fishing the smaller, lower-quality residual net-new pool. **The gate narrows by WHO (net-new households), not by 3P.**

