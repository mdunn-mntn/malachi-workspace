# MNTN Matched vs 3P Segments: Prospecting Performance by Vertical

Trailing 6 months. Stage-1 prospecting. AUDI-1141. 2026-07-20.

## What this compares

MNTN Matched (MM) vs 3P-segment prospecting performance across the 8 sales verticals. Each campaign is
classified from its live bidder audience expression. There are two ways to read it:

- **MM (all) vs 3P:** every MM campaign vs 3P, including mis-configured ones. The realistic average.
- **MM (gated) vs 3P:** MM campaigns with the intent gate on and a broad audience. What MM does when
  configured correctly (the best case).

## The one thing to remember

For the typical (median) advertiser, MM beats 3P about 4x on visit rate, about 3x on cost per visit, and
about 2x on ROAS, in aggregate and in every vertical. Configure MM well (intent gate on, audience not
over-narrowed) and the visit-rate gap widens to about 6x.

![Overall](audi_1141_chart_overall.png)

| Group | Visit rate | Cost per visit | ROAS |
|---|---|---|---|
| MM (all) | 0.28% | $13 | 0.92 |
| **MM (gated)** | **0.46%** | **$9** | **0.92** |
| MM (no gate) | 0.13% | $22 | 0.56 |
| MM restricted | 0.18% | $19 | 0.94 |
| 3P | 0.07% | $37 | 0.40 |

Median advertiser. IVR = visits/impressions, CVR = conversions/impressions (rates are over impressions).
ROAS is directional (see caveats).

## By vertical (MM all vs 3P, the realistic average)

MM beats 3P on visit rate and cost per visit in all 8 verticals:

| Sales vertical | MM IVR | 3P IVR | IVR adv | MM CPV | 3P CPV | MM ROAS | 3P ROAS |
|---|---|---|---|---|---|---|---|
| CPG & Health | 0.13% | 0.02% | **7.8x** | $20 | $93 | 0.36 | 0.06 |
| Restaurants / Dining | 0.31% | 0.05% | **6.0x** | $11 | $47 | 0.62 | 0.67 |
| Gaming / Entertainment | 0.84% | 0.14% | **5.8x** | $4 | $17 | 1.69 | 1.28 |
| Retail / Ecom | 0.29% | 0.06% | **4.9x** | $11 | $46 | 0.99 | 0.30 |
| Auto, Travel & Hospitality | 0.42% | 0.10% | **4.4x** | $10 | $39 | 3.02 | 1.85 |
| Telco & Tech | 0.17% | 0.05% | **3.5x** | $25 | $48 | 0.29 | 0.35 |
| ProServ | 0.28% | 0.09% | **3.1x** | $15 | $22 | 1.42 | 0.50 |
| Education | 0.40% | 0.21% | **1.9x** | $11 | $19 | 1.81 | 0.00 |

Median advertiser. The gated-only view (best case) is in the workbook's "MM gated vs 3P" tab.

![IVR by vertical](audi_1141_chart_ivr_by_vertical.png)

![CPV by vertical](audi_1141_chart_cpv_by_vertical.png)

## What the intent gate is

An MM campaign performs best when the bidder only bids on IPs the model scored as high intent. That is
controlled by a score threshold. Some advertisers run the threshold at 0, which bypasses the model and
bids broadly, similar to a 3P segment. The threshold drops to 0 for a few reasons: Max Reach, many short
flights (lowered to keep delivery flowing), or an audience narrowed so far that the high-intent pool is
exhausted and the score is lowered for deliverability. The gate is the score threshold setting, not the
scoring model itself.

## Caveats (please read before quoting these)

1. **ROAS is directional.** Prospecting only, last-touch. Revenue mostly lands in retargeting, which is
   excluded, and some verticals have unreliable conversion pixels. Visit rate and cost per visit are the
   solid metrics; treat per-vertical ROAS as a rough signal.
2. **3P can look competitive if impressions are pooled**, because roughly 39% of 3P impressions come from
   a single large, non-representative account. The per-advertiser (median) numbers above are the ones to use.
3. **Vertical grouping is an interim mapping** of the 37 MNTN internal verticals into the 8 sales verticals.
   If RevOps has an official crosswalk, send it and it is a one-line swap.

## Bottom line for the pitch

- Lead with visit rate and cost per visit. MM wins both, in every vertical, whether you look at all MM
  campaigns or just the well-configured ones.
- Strongest verticals for the MM story: CPG & Health, Gaming, Retail, Auto.
- If a prospect uses 3P segments, the honest counter is that MM gets about 4x the site visits at about a
  third of the cost, and more when configured well.
