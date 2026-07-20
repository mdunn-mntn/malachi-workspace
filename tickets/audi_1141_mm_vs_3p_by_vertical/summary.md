---
doc_type: ticket
title: "AUDI-1141: MM vs 3P prospecting performance by sales vertical (6mo)"
status: in_progress
date: 2026-07-20
summary: "Sales (Jon Zucker/Karen) request — compare MNTN Matched vs 3P-segment prospecting performance across the 8 sales verticals, trailing 6 months."
result: "MM (gated) beats 3P on visit rate (~4.3x median) and cost-per-visit (~3x cheaper) in all 8 verticals; un-gated MM collapses toward 3P — the intent gate creates the value. 3P only looks competitive when impression-pooled, and that is ~39% one account (WGU)."
---

# AUDI-1141: MM vs 3P prospecting performance by sales vertical

**Jira:** https://mntn.atlassian.net/browse/AUDI-1141
**Requested by:** Jon Zucker (via Karen), Slack, 2026-07-20
**Status:** In progress — analysis complete, deliverable drafted

---

## 1. Introduction

Sales asked for a MNTN Matched (MM) vs 3P-segment prospecting performance comparison, filterable by the 8 "new sales verticals," over the trailing 6 months. Malachi flagged in-thread that "MM vs 3P" is deceptively complex (what counts as MM; modified/geo-restricted campaigns; score-limited MM). This ticket resolves those ambiguities empirically and delivers a per-vertical scorecard.

**The 8 sales verticals:** ProServ, Education, Retail/Ecom, Gaming/Entertainment, Telco & Tech, Restaurants/Dining, CPG & Health, Auto Travel & Hospitality.

**Jon's scoping decisions (Slack):** omit zip-code-targeted campaigns for all verticals **except Auto and ProServ**; trailing 6 months.

**Malachi's methodology decisions (2026-07-20):** full scorecard (VR/CPV/CVR/ROAS); three buckets MM / 3P / Mixed; split MM into uncapped vs HHST-capped; advertiser-weighted; use the proposed 37→8 vertical rollup.

---

## 2. The Problem

A naive "campaigns using MM vs campaigns using 3P" split is wrong: **~72% of campaigns carrying a 3P segment also carry an MM signal** (MM does the scoring underneath — Victor, 2026-05-28). So a fair comparison must isolate **pure** buckets and control for the score gate.

---

## 3. Method

### Cohort
- **Unit:** S1 prospecting campaigns (`objective_id=1 AND funnel_level=1`, `deleted=FALSE`) that delivered (`impressions>0`) in the trailing 180 days.
- **Classification layer:** bidder-facing segment expressions — `dw-main-silver.audience.audience_segments`, `expression_type_id=2 AND is_targeted=TRUE`; `data_source_id` leaves regex-extracted.
- **Result:** 8,217 campaigns / 2,986 advertisers.

### Buckets (from the audience expression)
- **MM** = DS13 / DS19 / DS38 / DS46 present, no 3P
- **3P** = DS17 ShareThis / DS18 Dstillery / DS35 LiveRamp present, no MM
- **Mixed** = both ; **Neither** = CRM/1P/IP-list/geo-only (excluded — not part of the MM-vs-3P question)
- Bucket mix (spend): MM 39%, Mixed 36%, 3P 15%, Neither 10%. **Pure 3P is only 15% of spend / 446 advertisers.**

### Score-cap split (the confound Malachi flagged)
- HHST gate from `dw-main-silver.archives.household_score_threshold_archives` (`threshold>0` = intent gate ON).
- **capped ("gated")** = gate ON for the majority of in-window threshold writes; **uncapped ("no gate")** = otherwise.
- **The gate is the norm:** median in-window gated-fraction = 0.99; only 74 qualifying advertisers run un-gated MM.

### Zip filter
- Geo targeting is expressed only via `location_ids` (no zip/postal keyword). Zip = `geo.location_data.location_type_id=7`.
- Geos structure: `"geos":{"where":{"op":"and","value":[ <INCLUDE or-block>, {"op":"not", <EXCLUDE>} ]}}`.
- **Zip-narrowed** = a zip-level `location_id` in the INCLUDE block (before the first `"op":"not"`). 1,665 campaigns.
- Dropped zip-narrowed campaigns except in Auto & ProServ (per Jon) → 1,321 campaigns removed.
- (Also captured but not filtered on: `city_narrow` type-6, `radius_narrow` `geo_radii` — radius targeting is a bigger local signal, 2,284 campaigns.)

### Vertical rollup
- Advertiser → `fpa_advertiser_verticals` **type=0 parent** (37 canonical parents) → 8 sales buckets via crosswalk (in the SQL). 3 orphans (News & Politics, Non-Profits, Holidays & Events) → "Other / Unmapped." **Crosswalk is Malachi's proposal — needs Sales sign-off.**

### KPIs (`summarydata.sum_by_campaign_by_day`, default/non-competing attribution)
- Visits = views+clicks · Conv = click+view conversions · Revenue = click+view order value · Spend = media+data+platform.
- **VR** = visits/1k imps · **CPV** = spend/visits · **CVR** = conv/visits · **ROAS** = revenue/spend.
- **Advertiser-weighted** (each advertiser one vote, ≥20k imps floor): median = whale-robust headline, mean secondary. **Pooled** (impression/spend-weighted) reported as a cross-check.

---

## 4. Findings

### Headline (advertiser-weighted, median advertiser, all verticals)
| Bucket | n adv | Median VR (/1k) | Median CPV | Median ROAS* |
|---|---|---|---|---|
| **MM (gated)** | 1,307 | **3.9** | **$9.69** | 0.94 |
| MM (no gate) | 114 | 1.3 | $18.38 | 1.59 |
| Mixed | 1,073 | 2.5 | $16.56 | 0.89 |
| 3P | 388 | 0.9 | $29.92 | 0.40 |

- **Gated MM beats 3P ~4.3x on visit rate and ~3x on cost-per-visit for the typical advertiser.**
- **Un-gated MM collapses (VR 1.3, toward 3P's 0.9)** — the intent gate is what creates MM's value. This is the concrete answer to "score-limited MM behaves like 3P."
- MM (gated) wins **both VR and CPV in all 8 real verticals** (see charts). Biggest VR gaps: Gaming (9.4 vs 1.4), Auto (7.8 vs 1.0), CPG (2.1 vs 0.2). Closest: Education (5.0 vs 1.8).

### The pooled-vs-advertiser divergence (important caveat)
- Impression-**pooled** VR flips: 3P 10.0 vs MM-gated 7.7. **This is ~entirely WGU (advertiser 31357): 38.8% of all 3P impressions, VR 21.1.** Top-2 3P accounts = 59% of 3P imps; top-5 = 70%. WGU is a known non-representative outlier (~30% of MNTN spend; its revenue is also known-unreliable — see [[reference_wgu_pixel_case]]).
- **Conclusion:** 3P looks competitive only at the aggregate impression level, and only because one whale dominates. Per-advertiser (the lens Sales needs), gated MM wins decisively. This is why advertiser-weighting is the correct lens here.

### ROAS is directional only (*)
- Prospecting-only, last-touch → absolute ROAS is low at the median (<1 for most buckets); revenue concentrates in retargeting (excluded). MM-gated (0.94) still ~2.4x 3P (0.40).
- **Pixel artifacts inflate ROAS in some cells** — e.g. ProServ advertiser 61909 = 812x ROAS ($16.8k spend → $13.6M revenue). Median neutralizes it; do **not** quote mean ROAS.

---

## 5. Deliverables
- **`outputs/audi_1141_mm_vs_3p_scorecard.xlsx`** — the shareable workbook (upload to Google Sheets): Read-me · MM-vs-3P-by-vertical headline · Full scorecard · Overall · Campaign detail (5,965 rows w/ advertiser names, pivotable). Rebuild: `artifacts/audi_1141_build_xlsx.py`.
- `outputs/audi_1141_campaign_grain.csv` — 8,217-campaign cohort with all flags + KPIs
- `outputs/audi_1141_scorecard_overall.csv`, `outputs/audi_1141_scorecard_by_vertical.csv`, `outputs/audi_1141_advertiser_names.csv`
- `artifacts/audi_1141_chart_overall.png` (VR + CPV + gate story), `..._vr_by_vertical.png`, `..._cpv_by_vertical.png`
- `artifacts/audi_1141_findings.md` — Jon-facing findings doc (markdown)
- `queries/audi_1141_cohort_scorecard.sql`, `artifacts/audi_1141_aggregate.py`, `artifacts/generate_charts.py`, `artifacts/audi_1141_build_xlsx.py`

## 6. Questions Answered
- **Q: MM vs 3P performance by vertical, 6mo?** A: MM (gated) wins VR & CPV in every vertical; scorecard delivered.
- **Q: does score-limited MM behave like 3P?** A: Yes — un-gated MM (median VR 1.3) collapses toward 3P (0.9); gated MM is 3.9.
- **Q: is 3P ever better?** A: Only impression-pooled, and that is ~39% one account (WGU).

## 7. Data Documentation Updates
- 3P bought-interest DS set for classification = **DS17/18/35** (17 ShareThis, 18 Dstillery, 35 LiveRamp).
- **Zip/local geo detection:** `location_type_id` 7=zip, 6=city, 5=state, 3/4=DMA, 2=country; geo targeting only via `location_ids`; INCLUDE block = geos before first `"op":"not"`; `geo_radii` = radius targeting.
- **HHST gate is the norm** on MM prospecting (median gated-fraction 0.99).
- WGU (31357) = ~39% of pure-3P impressions — dominates any impression-pooled 3P metric.

## 8. Open Items
- **Vertical crosswalk needs Sales/RevOps sign-off** (esp. B2B→Telco vs ProServ; Food & Beverage→Restaurants vs CPG; 3 orphans).
- Confirm KPI/attribution lens with Jon (currently default non-competing, prospecting-only).
- Optional: add channel split (CTV vs display) and a Mixed-by-gate cut.
