---
doc_type: ticket
title: "AUDI-1141: MM vs 3P prospecting performance by sales vertical (6mo)"
status: in_progress
date: 2026-07-20
summary: "Sales request: compare MNTN Matched vs 3P-segment prospecting performance across the 8 sales verticals, trailing 6 months."
result: "MM (gated) beats 3P ~6x on visit rate and ~4x on cost-per-visit for the median advertiser, in all 8 verticals. Un-gated MM and audience-restricted MM both fall well below gated MM. 3P only looks competitive when impression-pooled, which is ~39% one account."
---

# AUDI-1141: MM vs 3P prospecting performance by sales vertical

Jira: https://mntn.atlassian.net/browse/AUDI-1141
Requested via Slack (Sales), 2026-07-20.
Status: analysis complete, shareable workbook drafted.

## 1. Introduction

Sales asked for a MNTN Matched (MM) vs 3P-segment prospecting comparison, filterable by the 8 sales
verticals, over the trailing 6 months. "MM vs 3P" is not straightforward: what counts as MM, how to
treat modified/geo-restricted campaigns, and how to treat score-limited MM all change the answer. This
ticket resolves those empirically and delivers a per-vertical scorecard.

8 sales verticals: ProServ, Education, Retail/Ecom, Gaming/Entertainment, Telco & Tech,
Restaurants/Dining, CPG & Health, Auto Travel & Hospitality.

## 2. The problem

A naive "campaigns using MM vs campaigns using 3P" split is wrong. About 85% of campaigns that carry a
3P segment join it with OR (additive reach), leaving MM to do the scoring underneath. Only an AND-required
3P (or a narrow geo) actually restricts the audience. The comparison has to separate additive 3P
(effectively MM) from restrictive 3P, and control for the intent score gate.

## 3. Method

### Cohort
- S1 prospecting campaigns (objective_id=1, funnel_level=1, deleted=FALSE) that delivered (impressions>0)
  in the trailing 180 days.
- Classified from the latest bidder-facing targeted segment (audience.audience_segments,
  expression_type_id=2, is_targeted=TRUE, most recent per campaign).
- 8,202 campaigns after dropping the "Neither" group; 7,138 in the scored MM/3P groups.

### Buckets (AND vs OR semantics, per TI-999 Pass 26 LCA tree-walk)
- MM signal = DS13/19/38/46 positive. 3P = DS17/18/35 positive.
- A JS UDF walks the categories op-tree and finds the lowest common ancestor of each MM and 3P positive
  clause: LCA = OR means additive, LCA = AND means intersecting/narrowing.
- Narrow geo = zip (location_type_id=7) or city (6) in the geos INCLUDE block (before the first
  "op":"not"), or a geo_radii clause. Broad = national or DMA-tier.
- Groups:
  - MM: has MM, 3P (if any) is OR-additive, geo broad. Split by intent gate into MM (gated) and MM (no gate).
  - MM restricted: MM narrowed by an AND-include 3P (or mixed), or by narrow geo.
  - 3P: 3P positive, no MM.
  - Neither: CRM/1P/geo-only (excluded from the comparison).
- Spend mix: MM 51%, MM restricted 24%, 3P 15%, Neither 10%. Among MM+3P campaigns, 85% OR-additive,
  15% AND/mixed (matches TI-999: intersective 3P is rare).

### Intent gate
- household_score_threshold_archives, threshold>0 = intent gate on. capped = gate on for the majority of
  in-window writes. The gate is the norm on MM prospecting (median gated-fraction 0.99).

### Vertical rollup
- advertiser -> fpa_advertiser_verticals type=0 parent (37 canonical parents) -> 8 sales buckets via a
  crosswalk in the SQL. 3 orphans (News & Politics, Non-Profits, Holidays & Events) -> Other/Unmapped.
  Crosswalk is interim and needs RevOps sign-off.

### Metrics (all rates over impressions, per TI-999 Pass 26)
- IVR = visits/impressions (visits = views+clicks). CVR = conversions/impressions. CTR = clicks/impressions.
- CPV = spend/visits. CPM = spend/impressions*1000. ROAS = revenue/spend.
- Advertiser-weighted median is the headline (each advertiser one vote, 20k-impression floor); pooled
  (impression/spend-weighted) reported as a cross-check.

## 4. Findings

Headline (advertiser-weighted median, all verticals):

| Group | n adv | n camp | IVR (median) | CPV (median) | ROAS (median)* |
|---|---|---|---|---|---|
| MM (all) | 2,560 | 6,004 | 0.28% | $12.52 | 0.92 |
| MM (gated) | 1,262 | 2,248 | 0.46% | $9.13 | 0.92 |
| MM (no gate) | 144 | 289 | 0.13% | $21.84 | 0.56 |
| MM restricted | 1,393 | 3,467 | 0.18% | $19.02 | 0.94 |
| 3P | 438 | 1,134 | 0.07% | $37.18 | 0.40 |

- Two lenses (Jon requested both): **MM (all)** blends every MM campaign = the realistic average;
  **MM (gated)** is the best-configured subset. Both beat 3P clearly, which answers the "is gated a
  perfect-scenario average?" concern: MM (all) still beats 3P ~4x IVR / ~3x CPV / ~2.3x ROAS.
- Gated MM beats 3P ~6.6x on visit rate and ~4x on CPV. Both restrictions hurt: MM (no gate) 0.13% and
  MM restricted 0.18% sit below MM (gated) 0.46%; the intent gate is the single biggest lever.
- MM leads IVR and CPV in all 8 verticals under both lenses.
- **Intent gate clarified (for the requester):** an MM campaign only bids on model-scored high-intent
  IPs when the score threshold is >0. Some advertisers run it at 0 (Max Reach; many short flights that
  lower it for deliverability; or an over-narrowed HI pool exhausted quickly), which bypasses the model
  and bids broadly like a 3P segment. The gate is the threshold setting, not the scoring model.

Pooled cross-check: impression-pooled 3P IVR (0.62%) exceeds MM-gated pooled, but ~39% of 3P impressions
come from one large account (advertiser 31357) with an unusually high visit rate. Per-advertiser (median)
is the correct lens and favors MM decisively.

ROAS is directional only: prospecting/last-touch, revenue concentrates in retargeting (excluded), and
some cells carry pixel artifacts (one ProServ advertiser shows >800x). Use median, never mean.

## 5. Deliverables
- outputs/audi_1141_mm_vs_3p_scorecard.xlsx: shareable workbook (upload to Google Sheets). Tabs: Read me,
  MM vs 3P by vertical (blended MM-all vs 3P, ROAS + counts), MM gated vs 3P by vertical (best case, ROAS),
  Full scorecard, Overall, Campaign detail (advertiser names, pivotable), Queries. Rebuild:
  artifacts/audi_1141_build_xlsx.py.
- outputs/audi_1141_campaign_grain.csv, audi_1141_scorecard_overall.csv, audi_1141_scorecard_by_vertical.csv,
  audi_1141_scorecard2_overall.csv, audi_1141_scorecard2_by_vertical.csv, audi_1141_advertiser_names.csv
- artifacts/audi_1141_chart_overall.png, audi_1141_chart_ivr_by_vertical.png, audi_1141_chart_cpv_by_vertical.png
- artifacts/audi_1141_findings.md
- queries/audi_1141_cohort_scorecard.sql, artifacts/audi_1141_aggregate.py, artifacts/generate_charts.py

### Drive
- Live workbook synced to `My Drive/Tickets/AUDI-1141 MM vs 3P by Vertical/AUDI-1141 MM vs 3P Scorecard.xlsx`
  (via the local Drive mount). Rebuild `build_xlsx.py` then copy over the same path to update in place.

## 6. Questions answered
- MM vs 3P by vertical, 6mo: MM (gated) wins IVR and CPV in every vertical; scorecard delivered.
- Does score-limited MM behave like 3P: un-gated MM (0.13%) falls toward 3P (0.07%); gated MM is 0.46%.
- Does narrowing MM hurt: yes, MM restricted (0.18%) is well below broad MM (0.46%).
- Is 3P ever better: only impression-pooled, which is ~39% one account.

## 7. Data documentation updates
- 3P bought-interest DS set = DS17/18/35.
- AND vs OR classification via TI-999 Pass 26 LCA tree-walk (additive 3P stays MM; only AND-include narrows).
- Geo detection: location_type_id 7=zip, 6=city, 5=state, 3/4=DMA, 2=country; INCLUDE block = geos before
  first "op":"not"; geo_radii = radius targeting.
- Rate conventions (TI-999 Pass 26): IVR/CVR/CTR are all over impressions.
- Intent gate is the norm on MM prospecting.

## 8. Open items
- Vertical crosswalk needs RevOps sign-off (B2B, Food & Beverage are judgment calls; 3 orphans).
- Confirm KPI/attribution lens (currently default non-competing, prospecting-only).
