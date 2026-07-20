# MNTN Matched vs 3P Segments — Prospecting Performance by Vertical

*Prepared for Jon Zucker · trailing 6 months · S1 prospecting · AUDI-1141 · 2026-07-20*

---

## What you asked for

MM vs 3P-segment performance, by the 8 sales verticals, last 6 months. The wrinkle you and Malachi
flagged is real, so here's how it's handled up front:

- **"MM vs 3P" is not campaigns-that-use-each** — ~72% of campaigns carrying a 3P segment *also* carry
  an MM signal, and MM is doing the scoring underneath. So I split every campaign into three clean buckets:
  **MM** (MNTN scoring only), **3P** (bought interest only — ShareThis/Dstillery/LiveRamp), and **Mixed** (both).
- **Score-limited MM** is broken out separately: **MM (gated)** = an intent score threshold is on;
  **MM (no gate)** = MM is on but not gating. This is the "MM tuned down behaves like 3P" case.
- **Zip-code campaigns** are dropped everywhere **except Auto and ProServ** (your call).
- **Advertiser-weighted** so one huge account can't set the story (more on that below).

---

## The one thing to remember

> **For the typical advertiser, gated MNTN Matched drives ~4× the visit rate of 3P at ~⅓ the cost per
> visit — and wins in every vertical. Turn MM's score gate off and it collapses toward 3P.**

![Overall](audi_1141_chart_overall.png)

| Bucket | Visit rate (per 1k imps) | Cost per visit | 
|---|---|---|
| **MM (gated)** | **3.9** | **$9.69** |
| MM (no gate) | 1.3 | $18.38 |
| Mixed | 2.5 | $16.56 |
| 3P | 0.9 | $29.92 |

*Median advertiser. The gate matters: un-gated MM (1.3) is barely better than 3P (0.9) — the intent
score is what makes MM work.*

---

## By vertical

MM (gated) beats 3P on **both** visit rate and cost-per-visit in **all 8 verticals**:

| Sales vertical | MM (gated) VR | 3P VR | MM VR advantage | MM (gated) CPV | 3P CPV |
|---|---|---|---|---|---|
| Auto, Travel & Hospitality | 7.8 | 1.0 | **8.1x** | $6 | $39 |
| CPG & Health | 2.1 | 0.2 | **12.4x** | $16 | $126 |
| Education | 5.0 | 1.8 | **2.8x** | $12 | $20 |
| Gaming / Entertainment | 9.4 | 1.4 | **6.8x** | $3 | $20 |
| ProServ | 4.5 | 1.0 | **4.6x** | $9 | $24 |
| Restaurants / Dining | 3.4 | 1.2 | **2.8x** | $9 | $36 |
| Retail / Ecom | 4.5 | 1.5 | **3.1x** | $9 | $20 |
| Telco & Tech | 2.3 | 0.5 | **4.7x** | $24 | $51 |

*Median advertiser, visits per 1,000 impressions.*

![VR by vertical](audi_1141_chart_vr_by_vertical.png)

![CPV by vertical](audi_1141_chart_cpv_by_vertical.png)

---

## Honest caveats (please read before quoting these)

1. **3P *can* look good in aggregate — but it's one account.** If you pool all impressions together,
   3P's visit rate actually beats MM. That's almost entirely **WGU**: a single account is ~39% of all
   3P impressions with an unusually high visit rate. It's a known outlier (~30% of MNTN spend). Strip
   it out and look per-advertiser — which is what a sales conversation is about — and MM wins clearly.
   Use the per-advertiser numbers above, not pooled totals.
2. **These are prospecting numbers only** (top-funnel S1). Revenue/ROAS mostly lands in retargeting,
   which is excluded — so treat ROAS here as **directional**, not absolute (some verticals also have
   bad conversion pixels that throw ROAS off entirely). **Visit rate and cost-per-visit are the solid
   metrics.**
3. **Vertical grouping is my mapping**, rolling MNTN's 37 internal verticals into your 8. If RevOps has
   an official crosswalk, send it and I'll swap it in (it's a one-line change). B2B, Food & Beverage,
   and a few others are judgment calls.
4. **"MM (no gate)" is a small group** (74 advertisers) — directionally clear, but thinner than the rest.

---

## Bottom line for the pitch

- Lead with **visit rate and cost-per-visit** — MM wins both, in every vertical, and it's not close.
- The strongest verticals for the MM story: **CPG & Health, Auto, Gaming**.
- If a prospect says "we use 3P segments," the honest counter is: *MM gets you ~4× the site visits at a
  fraction of the cost — as long as you leave the intent gate on.*

*Questions or want a different cut (channel, specific advertiser, add a vertical) — ping Malachi.*
