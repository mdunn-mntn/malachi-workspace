# TI-849: Monitor Fangorn score lift and visit rate improvements

**Jira:** https://mntn.atlassian.net/browse/TI-849
**Status:** Done (closed as infrastructure-complete 2026-05-01)
**Date Started:** 2026-04-20
**Date Completed:** 2026-05-01
**Assignee:** Malachi
**Story Points:** 3 (reduced from 5 — actual scope was infrastructure-only)
**Priority:** P1 - Critical
**Parent:** TI-457
**Follow-up:** [TI-921](https://mntn.atlassian.net/browse/TI-921) — final evaluation + Mode dashboard (next sprint)

---

## Methodology (revised 2026-05-01 PM, per user direction)

Two methods only:

1. **Descriptive pre/post KPIs** ([queries/ti_849_pre_post_summary.sql](queries/ti_849_pre_post_summary.sql) + daily/CG variants). Volume context only — NOT a lift claim. Spend confound is real and the user explicitly rejected this as a headline.

2. **CausalImpact synthetic control** per (treated AID, metric) — TI-748 / TI-542 / TI-803 / TI-504 pattern. Predicts what the advertiser's daily IVR/CVR/ROAS/CPA/CPV would have been WITHOUT Fangorn, using non-Fangorn advertisers as platform covariates plus holiday/lag/spend covariates. VIF → BIC → CausalImpact validation. Daily granularity (not weekly — post-period is too short).
   - Query: [queries/ti_849_method3_covariate_pull.sql](queries/ti_849_method3_covariate_pull.sql)
   - Pipeline: [artifacts/ti_849_method3_causal_impact.py](artifacts/ti_849_method3_causal_impact.py)

**DEPRECATED — Method 2 within-AID DiD via TI-835 holdout + augmentor_log:** infeasible. Augmentor scan is TB-scale and can't run daily. CausalImpact's covariate matrix absorbs the same confounds (spend, seasonality, secular platform trends) without the data lift. File [queries/ti_849_method2_did_period_lift.sql](queries/ti_849_method2_did_period_lift.sql) kept as methodology trail; do not run.

---

## 1. Introduction

Fangorn — MNTN's first ML-based IP intent scoring model — launched 2026-04-30 (DAGs completed early 2026-05-01). Initial production rollout flipped 3 Tier-1 advertisers to `vertical_data_source = 46` in `audience.advertiser_configurations`, triggering DS13 → DS46 audience swaps in the Audience Service. This ticket monitors KPI movements vs the pre-launch baseline and delivers a leadership writeup for Richard's D+7 review on 2026-05-07.

## 2. The Problem

Without structured monitoring, a successful or failing Fangorn rollout is invisible to leadership until ad-hoc analysis. We need:
- A pre-rollout baseline so changes are a comparison, not a guess
- Per-advertiser visibility so any regressions are isolable
- Self-serve dashboards so stakeholders don't pull weekly from Malachi
- A defensible D+7 writeup for Richard on 2026-05-07

The user's framing (2026-05-01): "by lift we're more interested in just the traditional KPIs we've been doing, not a traditional lift metric. Just, did we see these rates increase. Similar to TI-221 / TI-270." → Pre/post KPI suite, NOT a formal causal-lift estimator.

## 3. Plan of Action

Pre/post KPI suite per the TI-221 / TI-270 (Jaguar) pattern, BQ-ported, sourced from `silver.summarydata.*_facts` tables. Three views:

1. **Per-advertiser summary** ([ti_849_pre_post_summary.sql](queries/ti_849_pre_post_summary.sql)) — pre vs post KPIs with %change.
2. **Daily trend** ([ti_849_daily_trend.sql](queries/ti_849_daily_trend.sql)) — daily KPI series for trend visualization.
3. **Campaign-group breakdown** ([ti_849_campaign_breakdown.sql](queries/ti_849_campaign_breakdown.sql)) — per-CG within each AID.
4. **State of rollout** ([ti_849_state_of_rollout.sql](queries/ti_849_state_of_rollout.sql)) — daily check on which AIDs are flipped.

### Tier 1 launch advertisers (verified 2026-05-01 08:00 UTC flip)

| AID | Advertiser | Vertical | Flip time UTC |
|-----|------------|----------|---------------|
| 32320 | Biz2Credit | 111004 — Lending & Brokerage | 08:00:50 |
| 38659 | Big Blue Bubble Inc. | 110001 — Games & Comics | 08:01:04 |
| 32233 | University of Northwestern Ohio | 107000 — Colleges & Universities | 08:01:16 |

All three confirmed `industry_standard` reporting style (NOT last_touch) — simplifies attribution math to the COALESCE-includes-competing branch of the TI-221 logic.

### Periods
- Pre: 2026-03-31 → 2026-04-29 (30 days)
- Post: 2026-05-01 → CURRENT_DATE - 1 (grows daily)
- 2026-04-30 (launch day) excluded from both periods per TI-221 convention

### KPI suite
Volume: impressions, uniques (HLL), vv, conversions, order_value, spend.
Rates: IVR (vv/imp), VVR (vv/uniques), CVR (conv/vv), ROAS (rev/spend), CPV, CPA, AOV.

### Filters
- `audience_advertiser_configurations.vertical_data_source = 46` (auto-detect of flipped AIDs)
- `campaigns.funnel_level = 1` (prospecting only — Fangorn is a prospecting-layer intervention)
- `deleted = FALSE AND is_test = FALSE` on all dim tables

## 4. Investigation & Findings

### Pre-period baseline (Mar 31 → Apr 29, 30 days, prospecting only)

| AID | Impressions | VV | Conv | Spend | IVR | VVR | CVR |
|-----|------------:|---:|-----:|------:|----:|----:|----:|
| 32320 Biz2Credit | 2,143,901 | 22,729 | 1,113 | $43,123 | 1.06% | 1.87% | 4.90% |
| 38659 Big Blue Bubble | 248,760 | 2,441 | 0 | $23,818 | 0.98% | 1.08% | 0.00%* |
| 32233 UNW Ohio | 341,199 | 1,397 | 19 | $21,871 | 0.41% | 0.70% | 1.36% |

\* Big Blue Bubble has no conversion pixel firing — CVR/ROAS/AOV are not meaningful for them. Same caveat for AOV at Biz2Credit and UNW Ohio (lead-gen, no $-value per conversion).

### Source-table pivot (D0 finding)
The TI-221 Greenplum query relied on `summarydata.sum_by_campaign_group_by_day` rollups. The BQ silver equivalents are stale at **2026-04-14** (17 days behind, including the entire post-launch window). Pivoted to the underlying fact tables in `silver.summarydata`, which ARE fresh through 2026-05-01:

| Table | Use | Freshness |
|-------|-----|-----------|
| `silver.summarydata.impression_facts` | impressions, uniques (HLL) | 2026-05-01 ✓ |
| `silver.summarydata.visit_facts` | VVs (clicks + views + competing_views) | 2026-05-01 ✓ |
| `silver.summarydata.conversion_facts` | conversions, order_value | 2026-05-01 ✓ |
| `silver.summarydata.spend_facts` | spend | 2026-05-01 ✓ |
| `silver.summarydata.sum_by_campaign_group_by_day` | DEPRECATED for now | stale 2026-04-14 ✗ |
| `silver.aggregates.agg__daily_sum_by_campaign` | DEAD | empty since 2026-03-31 ✗ |

This is a meaningful gotcha for any other ticket using these aggregates. Logged in `knowledge/data_catalog.md`.

## 5. Solution

_(Populated at completion — Mode dashboard URL, methodology doc, stakeholder contacts.)_

## 6. Questions Answered

- **Q:** How is "lift" determined?
  **A:** Pre/post KPI suite per AID (TI-221 / Jaguar pattern). Pre = Mar 31 – Apr 29, Post = May 1 onward (grows daily through May 7). Headline rates: IVR, VVR, CVR, ROAS. Volume: impressions, VVs, conversions, spend.
- **Q:** What aggregations?
  **A:** Per-AID (summary), per-day (trend), per-CG (breakdown). All filtered to `funnel_level = 1` (prospecting). Vertical is dimensional — for the May 1 launch, each of the 3 AIDs is in a different vertical so no cross-AID rollups needed yet.
- **Q:** Why aren't we doing CausalImpact / formal lift?
  **A:** User decision (2026-05-01) — "we're more interested in just the traditional KPIs we've been doing, not a traditional lift metric." Pre/post pattern follows TI-221 (RTC release) and TI-270 (Jaguar GA) which are the team's standard for scoring-rollout monitoring.

## 7. Data Documentation Updates

- **silver.summarydata fact tables fresh, sum_by_*_by_day rollups stale** — added gotcha to `knowledge/data_catalog.md` and pattern to `knowledge/data_knowledge.md`.
- **Fangorn rollout mechanics** — added `audience_advertiser_configurations.vertical_data_source = 46` switch, DS13 → DS46 swap behavior, May 1 launch AID list to `knowledge/data_knowledge.md`.
- **Industry-standard attribution simplification** — all 3 launch AIDs are `industry_standard`, simplifying TI-221's CASE/last_touch logic.

## 8. Open Items / Follow-ups

- D+1..D+7: daily refresh, monitor for anomalies, post Slack updates if anything moves >20%.
- Phase 2 (only if time): CausalImpact synthetic control on IVR/VVR/CVR per AID; within-advertiser TI-835 holdout comparison.
- May 7 deliverable: 1-page Confluence writeup ([artifacts/ti_849_d7_review.md](artifacts/ti_849_d7_review.md)) + Mode link to Richard, Kale, Mike, Alex Bohr, Matt Brorby.
- Resolve sum_by_*_by_day staleness with data platform (separate ticket — affects more than TI-849).
- Mode dashboard scaffolding (D+1 work).
- AOV/ROAS unreliable for the 3 launch AIDs — flag clearly in dashboard and writeup. Lead-gen and gaming verticals don't carry $-conversion values.
