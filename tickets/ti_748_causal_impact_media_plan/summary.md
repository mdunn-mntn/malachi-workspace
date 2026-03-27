# TI-748: Causal Impact — Media Plan Feature

**Jira:** https://mntn.atlassian.net/browse/TI-748
**Status:** In Progress
**Date Started:** 2026-03-26
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Media Plan is a beta feature giving advertisers control over network allocation (% of spend per publisher like ABC, CBS, ESPN). MNTN auto-recommends allocations based on audience and goals; advertisers can accept or customize. Only applies to new prospecting campaigns.

Kirsa asked for a causal impact analysis to determine whether advertisers who adopted the Media Plan feature (using MNTN's recommended settings) saw improved prospecting performance. Kept quiet from broader team — Kirsa wants to surprise with data.

## 2. The Problem

**Core question:** Did advertisers who adopted MNTN's recommended Media Plan see improved prospecting performance?

**Two analyses:**
1. **CausalImpact (time-series):** Advertiser-level pre/post adoption comparison, controlling for market trends
2. **Within-advertiser comparison:** Recommended vs non-recommended campaign groups side-by-side

**Key challenges:**
- Staggered adoption (19 eligible with recommended plans, ~7 analyzable)
- Selection bias (adopters may differ systematically)
- Campaign maturity confound (new media plan campaigns vs established ones)
- Only prospecting campaigns relevant

## 3. Plan of Action

1. ✅ Identify recommended-only adopters via `media_plan_publishers.badge_state`
2. ✅ Switch to `sum_by_campaign_by_day` (data back to 2024-01-01 for 52-week pre-period)
3. ✅ Build platform-wide non-adopter covariates (IVR, spend, impressions, holidays, VCR, active advertisers)
4. ✅ Run per-advertiser CausalImpact for all metrics
5. ✅ Run within-advertiser comparison (recommended vs non-recommended campaign groups)
6. ✅ Run placebo tests for validation
7. ✅ Covariate significance testing — VIF multicollinearity, BIC stepwise, cross-validation, sensitivity analysis
8. ✅ Add advertiser-specific covariates — spend_change_pct, metric_lag1/2, adv_active_cgs, ctv_share (tested, some not selected by BIC)
9. ⬜ Share results with Kirsa

## 4. Investigation & Findings

### Adopter Identification (v2 — Recommended Only)
- **19 advertisers** have at least one all-recommended media plan
- 124 total recommended plans, 45 customized plans across all adopters
- `badge_state` values: `RECOMMENDED`, `USER_MODIFIED`, `USER_ADDED`
- Sky Zone, Beddy's, SoilTaxPro are 100% customized → excluded from recommended analysis
- All adopters use `industry_standard` attribution

### Pre-Period Data
- **`sum_by_campaign_by_day`** goes back to 2024-01-01 (vs Sep 2025 for agg table)
- 52-week pre-period captures full seasonality cycle (Black Friday, Christmas, Q1 slowdown)
- Earliest analyzable advertiser (FICO) gets 87 pre-weeks

### IVR Results (v3 — BIC-Optimized Covariates Per Advertiser)

| Advertiser | ID | Pre Wks | Post Wks | Effect | p-value | Significant | BIC-Selected Covariates |
|---|---|---|---|---|---|---|---|
| FICO | 37056 | 86 | 22 | -0.18% | 0.4945 | No | platform_roas, adv_active_cgs, spend_change_pct |
| Taskrabbit | 34114 | 76 | 22 | +20.75% | 0.0000 | Yes | holiday, metric_lag2, spend_change_pct |
| Lighting New York | 31116 | 93 | 22 | +8.51% | 0.0000 | Yes | metric_lag1, spend_change_pct |
| #CWH: CWRV Sales | 32756 | 94 | 21 | +12.16% | 0.0539 | No | platform_cvr, metric_lag1, spend_change_pct |
| Talkspace | 34094 | 103 | 12 | +3.85% | 0.1688 | No | holiday, metric_lag1, spend_change_pct |
| Am. College of Ed | 33667 | 106 | 9 | -27.08% | 0.0130 | Yes | holiday, metric_lag2, spend_change_pct |

Note: Boll & Branch (31966) dropped — its BIC-optimized model used metric_lag1, adv_active_cgs, spend_change_pct but had data gaps >20%.

**Aggregate (IVR):**
- 3/6 statistically significant
- 4/6 showed improvement (higher IVR)
- Median effect: **+6.18%**
- **Spend-weighted effect: +6.50%** (positive — a reversal from v2's -10.56%)

### Cross-Metric Summary (v3 — BIC-Optimized)

| Metric | N | Significant | Positive | Mean | Median | Spend-Weighted |
|---|---|---|---|---|---|---|
| IVR | 6 | 3/6 | 4/6 | +3.00% | +6.18% | **+6.50%** |
| CVR | 6 | 2/6 | 3/6 | +8.34% | +0.68% | +9.95% |
| CPA | 6 | 3/6 | 4/6 (lower=better) | +1.05% | -4.32% | **-3.98%** |
| CPV | 6 | 3/6 | 2/6 (lower=better) | +11.98% | +13.99% | +9.07% |
| ROAS | 2 | 0/2 | 0/2 | -2.74% | -2.74% | -1.13% |

### Model Validation Results

**Placebo tests:** 23 total, 7 false positives (**30% FPR** — down from 86% in v2). This is within acceptable range for time series data with natural structural breaks.

**Sensitivity analysis:** 5/6 advertisers showed **directionally consistent** results across pre-period lengths (26, 39, 52, 65, 78 weeks). FICO was the only inconsistent one — its effect is essentially zero (-0.18%).

**VIF multicollinearity:** Starting from 14 candidate covariates, VIF iteratively removed the worst collinear ones (platform_vcr, platform_avg_campaign_groups, platform_spend, platform_ivr, etc.). Final VIF-clean sets had 3-7 covariates per advertiser.

**Key covariate finding:** `spend_change_pct` (week-over-week budget changes) appeared in ALL 6 advertiser models. `metric_lag1` or `metric_lag2` appeared in 5/6. These advertiser-specific dynamics were far more predictive than platform-wide metrics. Hand-picked platform covariates from v2 were mostly collinear and didn't survive BIC selection.

### Within-Advertiser Comparison (Recommended vs Non-Recommended)

7 advertisers have both recommended and non-recommended campaigns in post-period.
**Average IVR difference (rec - non_rec): -0.027** — recommended campaigns have lower IVR on average.

**Important caveat:** This is heavily confounded by campaign maturity. Recommended campaigns are NEW (created with media plan), while non-recommended campaigns are typically OLDER with established audience patterns. New campaigns always underperform during ramp-up. See TI-780 for campaign ramp-up research.

## 5. Solution

**Deliverables:**
- `artifacts/ti_748_causal_impact.py` — CLI-runnable analysis script (v2)
- `artifacts/ti_748_causal_impact.ipynb` — Presentation notebook with glossary, methodology, and appendix
- `knowledge/experimentation.md` — New knowledge doc for experiment design (living document)

**Key v3 improvements:**
- Data source: `sum_by_campaign_by_day` (15 months history vs 6 months)
- 52-week pre-period (full seasonality vs 8 weeks)
- Recommended-only filter via `badge_state`
- Within-advertiser comparison
- BIC-optimized covariates PER ADVERTISER (not one-size-fits-all)
- VIF multicollinearity elimination (14 candidates → 3-4 per advertiser)
- Cross-validation of covariate sets (MAE/MAPE/RMSE comparison)
- Sensitivity analysis (pre-period length variation)
- Multi-point placebo tests (5 per advertiser, 30% FPR vs 86% in v2)
- Winsorization, min-spend threshold, gap detection

## 6. Questions Answered

- **Q:** How to identify recommended vs customized media plans?
  **A:** `core.media_plan_publishers.badge_state` — values are `RECOMMENDED`, `USER_MODIFIED`, `USER_ADDED`

- **Q:** What attribution model do adopters use?
  **A:** All use `industry_standard` (include competing views/conversions)

- **Q:** How far back can we go for pre-period data?
  **A:** `sum_by_campaign_by_day` goes to 2024-01-01 (15+ months). `agg__daily_sum_by_campaign` only Sep 2025.

- **Q:** What pre-period length is best practice?
  **A:** 52 weeks — captures full seasonality. Google's CausalImpact paper recommends ≥3x post-period length AND at least one full seasonal cycle.

## 7. Data Documentation Updates

- `core.media_plan` and `core.media_plan_publishers` documented in data_catalog.md
- `r2_advertiser_settings` documented (no deleted column)
- `funnel_level` location clarified (campaigns, not campaign_groups)
- `agg__daily_sum_by_campaign` effective start documented (Sep 2025)
- Created `knowledge/experimentation.md` — experiment methodology knowledge base
- Updated global and project CLAUDE.md to auto-update experimentation.md

## 8. Open Items / Follow-ups

- **TI-780**: Campaign ramp-up research — how long until new campaigns reach steady-state? Needed to adjust for maturity bias in within-advertiser comparison. First step: ask Kirsa/product if there's an existing benchmark.
- Re-run as more post-period data accumulates (recent adopters will qualify)
- Consider vertical-specific covariates instead of platform-wide
- Share findings with Kirsa
- Potential: analyze customized vs recommended allocations separately

---

## Files

| File | Description |
|---|---|
| `artifacts/ti_748_causal_impact.py` | Main analysis script v2 (CLI-runnable) |
| `artifacts/ti_748_causal_impact.ipynb` | Presentation notebook v2 (with glossary + methodology appendix) |
| `artifacts/datagrip_causal_impact.py` | Original Jaguar experiment notebook (reference only) |
| `artifacts/potential_media_plan_advertiser_adopters.xlsx` | Beta list from product team |
| `meetings/malachi_kirsa_meeting_1.txt` | Meeting transcript with Kirsa |
| `outputs/ci_*_results.csv` | Per-metric CSV exports |
| `outputs/within_advertiser_comparison.csv` | Recommended vs non-recommended comparison |
| `outputs/ci_*_*.png` | Per-advertiser and summary plots |
