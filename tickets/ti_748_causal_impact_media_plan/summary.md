# TI-748: Causal Impact — Media Plan Feature

**Jira:** https://mntn.atlassian.net/browse/TI-748
**Status:** In Progress
**Date Started:** 2026-03-26
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Media Plan is a beta feature giving advertisers control over network allocation (% of spend per publisher like ABC, CBS, ESPN). MNTN auto-recommends allocations based on audience and goals; advertisers can accept or customize. Only applies to new prospecting campaigns.

Kirsa asked for a causal impact analysis to determine whether advertisers who adopted the Media Plan feature saw improved prospecting performance. This is being kept quiet from the broader team — Kirsa wants to surprise them with data.

## 2. The Problem

**Core question:** Did advertisers who adopted Media Plan see improved prospecting performance compared to their pre-adoption baseline?

**Challenges:**
- Staggered adoption (27 eligible, 22 active, each with different adoption date)
- Only ~10 advertisers have sufficient pre/post data for analysis
- Short pre-period (BQ agg data starts 2025-09-01, earliest adopters: Oct 2025)
- No randomized control — selection bias possible
- Feature applies to new campaigns only, but analysis must be at advertiser-level (per Kirsa)

## 3. Plan of Action

1. ✅ Identify adopters via `core.media_plan` (status=3) and get intervention dates
2. ✅ Build weekly prospecting KPI pipeline from `agg__daily_sum_by_campaign`
3. ✅ Create platform-wide non-adopter covariates for CausalImpact
4. ✅ Run per-advertiser CausalImpact for IVR, CVR, CPA, CPV, ROAS
5. ✅ Run placebo tests for validation
6. ⬜ Refine analysis based on findings (address placebo concerns, investigate outliers)
7. ⬜ Share results with Kirsa

## 4. Investigation & Findings

### Adopter Identification
- **22 advertisers** have active media plans (`media_plan_status_id=3`)
- **4 of these** (48628, 47257, 49022, 45983) are NOT on the original Excel beta list — likely internal test accounts
- All adopters use `industry_standard` attribution (confirmed via `r2_advertiser_settings`)

### Intervention Dates
Using `first media_plan.create_time` per advertiser:
- Earliest: 2025-10-02 (RheaRegister10 — test account)
- Latest: 2026-03-24 (IMAGE Skincare)
- 10 advertisers have sufficient pre/post data (≥6 pre-weeks, ≥4 post-weeks)

### Data Quality Issues Found
- `agg__daily_sum_by_campaign` starts 2025-09-01 — limits pre-period to ~8 weeks for early adopters
- Weeks with <1,000 impressions produce extreme IVR values (e.g., IVR=366 for a week with 7 impressions). Caused by VV attribution lag after campaign pauses. Fixed with MIN_WEEKLY_IMPRESSIONS=1000 filter.
- `uniques` column is unreliable at campaign-level aggregation — VVR metric excluded

### IVR Results (Primary Metric)

| Advertiser | ID | Effect | p-value | Significant |
|---|---|---|---|---|
| FICO | 37056 | -24.00% | 0.0000 | Yes |
| Taskrabbit | 34114 | -3.14% | 0.0340 | Yes |
| Lighting New York | 31116 | +79.63% | 0.0000 | Yes |
| Timex | 38363 | -7.95% | 0.0000 | Yes |
| Tempo | 41545 | -7.21% | 0.0759 | No |
| #CWH: CWRV Sales | 32756 | +236.82% | 0.0000 | Yes |
| Sky Zone | 32101 | +152.78% | 0.0000 | Yes |
| Boll & Branch | 31966 | -64.34% | 0.0000 | Yes |
| Talkspace | 34094 | -16.99% | 0.0190 | Yes |
| American College of Ed | 33667 | -14.89% | 0.0240 | Yes |

**Aggregate (IVR):**
- 9/10 statistically significant
- 3/10 showed improvement (higher = better)
- Median effect: -7.58%
- **Spend-weighted effect: -4.99%** (most meaningful — largest advertisers dominate)

### Cross-Metric Summary

| Metric | Significant | Positive | Median Effect | Spend-Weighted |
|---|---|---|---|---|
| IVR | 9/10 | 3/10 | -7.58% | -4.99% |
| CVR | 7/10 | 6/10 | +8.34% | +9.83% |
| CPA | 8/10 | 6/10 | -3.99% | varies |
| CPV | 5/10 | 4/10 | +1.87% | varies |
| ROAS | 5/6 | 4/6 | +11.85% | +32.49% |

### Placebo Test Concerns
High false positive rate (100% for IVR, 50% for CVR). This is expected given the short pre-period (8 weeks split into 4/4). The methodology is directionally useful but individual p-values should be interpreted cautiously.

## 5. Solution

**Deliverables:**
- `artifacts/ti_748_causal_impact.py` — CLI-runnable analysis script
- `artifacts/ti_748_causal_impact.ipynb` — Presentation-ready Jupyter notebook

**Key design decisions:**
- Per-advertiser CausalImpact (not DiD — too few controls)
- Platform-wide non-adopter metrics as covariates
- `pycausalimpact` library (tfcausalimpact hangs on local import)
- Weekly aggregation, prospecting only (funnel_level=1)
- Minimum 1,000 weekly impressions to filter data artifacts

## 6. Questions Answered

- **Q:** How to identify which campaigns use Media Plan?
  **A:** `SELECT DISTINCT advertiser_id FROM core.media_plan WHERE media_plan_status_id=3` (from Tom Manuel via Slack)

- **Q:** What attribution model do adopters use?
  **A:** All 22 use `industry_standard` (confirmed via `r2_advertiser_settings`). Include competing views/conversions.

- **Q:** Is `funnel_level` on `campaign_groups` or `campaigns`?
  **A:** It's on `campaigns` (not campaign_groups). `campaign_groups` has no `funnel_level` column.

- **Q:** How far back does agg data go?
  **A:** `agg__daily_sum_by_campaign` starts 2025-09-01, not 2025-01-01.

## 7. Data Documentation Updates

- `core.media_plan` table documented: schema includes media_plan_id, advertiser_id, campaign_group_id, media_plan_status_id, create_time, original_recommendations (JSON), is_manual
- `r2_advertiser_settings` exists in BQ at `bronze.integrationprod.r2_advertiser_settings`
- `funnel_level` is on `campaigns` table, NOT `campaign_groups`
- `agg__daily_sum_by_campaign` effective start: 2025-09-01 (not Jan 2025)

## 8. Open Items / Follow-ups

- Investigate extreme outliers (CWRV Sales +237%, Sky Zone +153%) — may have other confounders
- Consider extending analysis once more post-period data accumulates (recent adopters will qualify)
- Address placebo test concerns — longer pre-period data would help when available
- Share findings with Kirsa
- Potential follow-up: analyze by network allocation changes (did customizers perform differently than accepters?)

---

## Files

| File | Description |
|---|---|
| `artifacts/ti_748_causal_impact.py` | Main analysis script (CLI-runnable) |
| `artifacts/ti_748_causal_impact.ipynb` | Presentation notebook |
| `artifacts/datagrip_causal_impact.py` | Original Jaguar experiment notebook (reference only) |
| `artifacts/potential_media_plan_advertiser_adopters.xlsx` | Beta list from product team |
| `meetings/malachi_kirsa_meeting_1.txt` | Meeting transcript with Kirsa |
| `outputs/ci_*_results.csv` | Per-metric CSV exports |
| `outputs/ci_*_*.png` | Per-advertiser and summary plots |
