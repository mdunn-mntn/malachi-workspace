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
7. ⬜ Covariate significance testing (stepwise selection, VIF, AIC/BIC)
8. ⬜ Add advertiser-specific covariates (active campaign count, budget changes)
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

### IVR Results (v2 — Advertiser-Level, 52wk Pre-Period)

| Advertiser | ID | Pre Wks | Post Wks | Effect | p-value | Significant |
|---|---|---|---|---|---|---|
| FICO | 37056 | 87 | 22 | -14.79% | 0.0060 | Yes |
| Taskrabbit | 34114 | 77 | 22 | +3.23% | 0.1518 | No |
| Lighting New York | 31116 | 94 | 22 | +16.51% | 0.0000 | Yes |
| #CWH: CWRV Sales | 32756 | 95 | 21 | +7.45% | 0.1469 | No |
| Boll & Branch | 31966 | 97 | 19 | -29.42% | 0.0000 | Yes |
| Talkspace | 34094 | 104 | 12 | +3.95% | 0.2088 | No |
| Am. College of Ed | 33667 | 107 | 9 | -25.34% | 0.0070 | Yes |

**Aggregate (IVR):**
- 4/7 statistically significant
- 4/7 showed improvement (higher IVR)
- Median effect: +3.23%
- **Spend-weighted effect: -10.56%** (largest advertisers — Boll & Branch, FICO — had declines)

### Within-Advertiser Comparison (Recommended vs Non-Recommended)

7 advertisers have both recommended and non-recommended campaigns in post-period.
**Average IVR difference (rec - non_rec): -0.027** — recommended campaigns have lower IVR on average.

**Important caveat:** This is heavily confounded by campaign maturity. Recommended campaigns are NEW (created with media plan), while non-recommended campaigns are typically OLDER with established audience patterns. New campaigns always underperform during ramp-up.

### Considerations for Further Work

Documented in `knowledge/experimentation.md`:

1. **Covariate selection rigor:** Need to run formal covariate significance tests (stepwise, VIF, AIC/BIC) rather than hand-picking covariates
2. **Advertiser-specific covariates not yet tested:** Number of active campaigns (prospecting vs retargeting), budget level changes, creative refresh timing
3. **Campaign maturity confound:** The within-advertiser comparison is not apples-to-apples due to new vs mature campaigns
4. **Vertical-specific trends:** Currently using platform-wide covariates; vertical-level would be more precise
5. **Placebo test failure:** 86% false positive rate even with 52wk pre-period — suggests natural structural breaks in advertiser time series

## 5. Solution

**Deliverables:**
- `artifacts/ti_748_causal_impact.py` — CLI-runnable analysis script (v2)
- `artifacts/ti_748_causal_impact.ipynb` — Presentation notebook with glossary, methodology, and appendix
- `knowledge/experimentation.md` — New knowledge doc for experiment design (living document)

**Key v2 improvements over v1:**
- Data source: `sum_by_campaign_by_day` (15 months history vs 6 months)
- 52-week pre-period (full seasonality vs 8 weeks)
- Recommended-only filter via `badge_state`
- Within-advertiser comparison
- Holiday covariates, VCR, active advertiser count, lagged metric
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

- Formal covariate selection (stepwise, VIF, AIC/BIC)
- Add advertiser-specific covariates (campaign count, budget changes)
- Re-run as more post-period data accumulates (recent adopters will qualify)
- Address campaign maturity confound in within-advertiser comparison
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
