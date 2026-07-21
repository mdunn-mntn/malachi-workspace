# MNTN Matched vs 3P Segments: Prospecting Performance by Vertical

Trailing 6 months. Stage-1 prospecting. AUDI-1141. 2026-07-20.

## What this compares

MNTN Matched (MM) vs 3P-segment prospecting performance across the 8 sales verticals. Every campaign
is classified from its live bidder audience expression into one of four groups:

- **MM (gated):** MNTN scoring (DS13/19/38/46) with an intent score threshold active. Any 3P segment is
  joined by OR (additive reach only), and the geo is broad.
- **MM (no gate):** MM with no intent score threshold set.
- **MM restricted:** MM whose audience is narrowed by an AND-required 3P clause, or by a sub-DMA geo
  (zip, city, or radius).
- **3P:** bought interest segments only (ShareThis, Dstillery, LiveRamp), no MM signal.

The OR vs AND distinction matters: about 85% of campaigns that carry a 3P segment join it with OR, which
only adds reach and leaves MM doing the scoring. Only an AND-required 3P actually narrows the audience.
Grouping all 3P-carrying campaigns as "3P" would misread additive 3P as restrictive.

## The one thing to remember

For the typical (median) advertiser, gated MNTN Matched drives about 6x the visit rate of 3P at roughly
one quarter the cost per visit, and it leads visit rate in every vertical. Remove the intent gate, or
narrow the audience, and most of that edge goes away.

![Overall](audi_1141_chart_overall.png)

| Group | Visit rate (visits/imps) | Cost per visit |
|---|---|---|
| **MM (gated)** | **0.46%** | **$9** |
| MM (no gate) | 0.13% | $22 |
| MM restricted | 0.18% | $19 |
| 3P | 0.07% | $37 |

Median advertiser. IVR = visits/impressions, CVR = conversions/impressions (both rates are over impressions).

## By vertical

MM (gated) beats 3P on visit rate and cost per visit in all 8 verticals:

| Sales vertical | MM (gated) IVR | 3P IVR | IVR advantage | MM (gated) CPV | 3P CPV |
|---|---|---|---|---|---|
| CPG & Health | 0.28% | 0.02% | **17.0x** | $12 | $93 |
| Gaming / Entertainment | 1.19% | 0.14% | **8.3x** | $3 | $17 |
| Auto, Travel & Hospitality | 0.78% | 0.10% | **8.1x** | $6 | $39 |
| Retail / Ecom | 0.46% | 0.06% | **7.7x** | $9 | $46 |
| Telco & Tech | 0.29% | 0.05% | **5.9x** | $18 | $48 |
| ProServ | 0.42% | 0.09% | **4.7x** | $9 | $22 |
| Restaurants / Dining | 0.21% | 0.05% | **4.1x** | $16 | $47 |
| Education | 0.67% | 0.21% | **3.2x** | $8 | $19 |

Median advertiser.

![IVR by vertical](audi_1141_chart_ivr_by_vertical.png)

![CPV by vertical](audi_1141_chart_cpv_by_vertical.png)

## Caveats (please read before quoting these)

1. **3P can look competitive in aggregate, but it is one account.** If all impressions are pooled, 3P
   visit rate rises, because roughly 39% of 3P impressions come from a single large, non-representative
   account. The per-advertiser (median) numbers above are the ones to use.
2. **These are prospecting numbers only** (top-funnel Stage 1). Revenue mostly lands in retargeting,
   which is excluded, so treat ROAS as directional, not absolute. Some verticals also have unreliable
   conversion pixels. Visit rate and cost per visit are the solid metrics.
3. **Vertical grouping is an interim mapping** of the 37 MNTN internal verticals into the 8 sales
   verticals. If RevOps has an official crosswalk, send it and it is a one-line swap.
4. **MM restricted is expected for local businesses.** Auto and ProServ legitimately use local (zip,
   radius) targeting, so their restricted share is high by design. In other verticals it flags narrowing.

## Bottom line for the pitch

- Lead with visit rate and cost per visit. MM wins both, in every vertical, and it is not close.
- Strongest verticals for the MM story: CPG & Health, Auto, Gaming, Retail.
- If a prospect says they use 3P segments, the honest counter is that MM gets about 6x the site visits at
  a fraction of the cost, as long as the intent gate stays on and the audience is not over-narrowed.
