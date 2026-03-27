# Experimentation & Causal Inference — Knowledge Base
Last updated: 2026-03-26 | Started from TI-748 (Media Plan Causal Impact)

This is a living document. Add to it every time we learn something new about experimental design, covariate selection, test methodology, or edge cases at MNTN.

---

## Methodology Selection Guide

| Situation | Method | When to Use | When NOT to Use |
|---|---|---|---|
| Single intervention date, good covariates available | **CausalImpact (BSTS)** | Feature rollout affecting time series metrics | Very short pre-period (<20 weeks) |
| Treatment vs control group exists | **Difference-in-Differences (DiD)** | A/B test with comparable groups | Groups not parallel in pre-period |
| Staggered rollout, no control group | **Per-unit CausalImpact** | Feature adopted at different times by different units | Very small N (<5 units) |
| Need to test for lift on a specific metric | **t-test / Mann-Whitney** | Comparing two distributions | Time-dependent data (use time series methods) |
| Multiple features changed simultaneously | **Regression with interaction terms** | Need to isolate each feature's effect | Multicollinearity between features |
| Want to understand feature importance | **SHAP / permutation importance** | Post-hoc explanation of what drove results | Not for causal claims |

---

## Covariate Selection

### Principles

1. **Use covariates that predict the outcome but are NOT affected by the intervention.** If media plan affects network allocation, don't use network-level metrics as covariates (they're downstream of the treatment).

2. **More data points = more covariates allowed.** Rule of thumb: ≥10 pre-period observations per covariate. With 52 weekly observations, max ~5 covariates.

3. **Don't use the outcome's components as covariates.** If measuring IVR (vv/impressions), don't use VV as a covariate — it's the numerator.

4. **Test covariate significance.** Use forward/backward stepwise selection, AIC/BIC comparison, or cross-validation to determine which covariates actually improve prediction vs add noise.

### Formal Covariate Selection Methods

| Method | What It Does | When to Use |
|---|---|---|
| **Forward stepwise** | Start with no covariates, add one at a time, keep if significant | Quick screening |
| **Backward elimination** | Start with all covariates, remove one at a time | When you have candidate list |
| **AIC/BIC comparison** | Penalizes model complexity; lower = better | Comparing model variants |
| **Lasso regression** | L1 penalty automatically shrinks weak predictors to zero | Many candidate covariates |
| **Cross-validation** | Hold out pre-period data, test prediction accuracy | Gold standard for validation |
| **VIF (Variance Inflation Factor)** | Detects multicollinearity between covariates | Before running any model — drop if VIF > 10 |

### Covariate Candidates for MNTN Experiments

**Platform-level (market trends):**
- Platform-wide IVR/CVR/ROAS (from non-treatment advertisers)
- Platform total spend / impressions
- Platform active advertiser count
- Platform video completion rate (VCR)

**Seasonality:**
- Holiday binary flags (Thanksgiving/Black Friday, Christmas, New Year, Super Bowl)
- Day-of-week effects (if daily data)
- Month/quarter indicators

**Advertiser-specific:**
- Lagged metric (previous period's value — captures autocorrelation)
- Number of active campaigns (total)
- Number of active prospecting vs retargeting campaigns
- Total campaign groups active
- Budget level / budget changes
- Creative refresh frequency
- Audience segment changes

**Competitive / market:**
- Same-vertical competitor spend (if available)
- CTV market prices (CPM trends)

### Covariates Used in TI-748 (Media Plan)

| Covariate | Rationale | Concern |
|---|---|---|
| `platform_ivr` | Controls for market-wide engagement trends | None — good control |
| `platform_spend` | Seasonality / industry spending patterns | None |
| `platform_impressions` | Supply-side changes | None |
| `holiday` | Known seasonal spikes | Only covers major holidays |
| `platform_active_advertisers` | Competition proxy | Coarse measure |
| `platform_vcr` | CTV engagement not affected by network allocation | Good |
| `metric_lag1` | Autocorrelation | Loses first observation |

**Not yet tested (future improvement):**
- Advertiser's active campaign count (prospecting vs retargeting)
- Budget level changes around intervention
- Creative refresh timing
- Vertical-specific trends (instead of platform-wide)

---

## Considerations & Gotchas

### Campaign Maturity Bias
New campaigns (including media plan campaigns) always underperform mature ones during ramp-up. When comparing new media plan campaigns to established campaigns, account for maturity:
- Use advertiser-level analysis (pre/post ALL campaigns) rather than campaign-level comparison
- If doing within-advertiser comparison, note the maturity confound explicitly

### Selection Bias
Advertisers who adopt a new feature may be systematically different:
- More engaged with the platform
- More sophisticated marketers
- Growing faster (or struggling — looking for improvements)
- Managed by specific account teams

Mitigation: Use pre-period trends as covariates, document the bias explicitly.

### Staggered Adoption
When units adopt at different times:
- Run per-unit CausalImpact (not pooled)
- Aggregate results with spend-weighted averages
- Check if early adopters differ from late adopters

### Holiday / Seasonality Effects
CTV advertising has massive seasonality:
- Q4 (Oct-Dec): highest spend, Black Friday/Christmas
- January: post-holiday drop
- Super Bowl week: CTV spike
- Summer: typically lower
Always include holiday flags. Consider using 52-week pre-period to capture full cycle.

### Prospecting vs Retargeting
These behave VERY differently. Always analyze separately:
- Prospecting: new audience, lower conversion rates, more volume
- Retargeting: known audience, higher conversion rates, smaller pool
- Mixing them in one analysis will confound results

### Data Quality Issues Found (TI-748)
- Weeks with <1,000 impressions can produce absurd rate metrics (IVR=366x) due to VV attribution lag after campaign pauses. Always filter.
- `uniques` column in agg tables is unreliable at campaign level.
- Some advertisers have data gaps (paused campaigns). Check for >20% missing weeks.

### Placebo Test Interpretation
- With short pre-periods (< 20 weeks), placebo tests split the data too thin → high false positive rate
- A failed placebo test doesn't invalidate results — it means the pre-period has structural breaks (natural variability)
- With 52-week pre-periods, aim for <20% placebo false positive rate
- **Run multiple placebos** (5+ per unit) at different split points — single placebo is unreliable

### Covariate Selection Lessons (from TI-748 validation)

**Key finding: advertiser-specific dynamics beat platform-wide metrics.**

| Covariate | Appeared in N/6 models | Lesson |
|---|---|---|
| `spend_change_pct` | 6/6 | Week-over-week budget changes are the strongest predictor. Always include. |
| `metric_lag1` or `metric_lag2` | 5/6 | Autocorrelation is real and important. Lagged metric almost always improves the model. |
| `holiday` | 4/6 | Important for Nov-Jan adopters. Less important for others. |
| `adv_active_cgs` | 3/6 | Number of active campaign groups matters for some advertisers. |
| `platform_ivr` | 0/6 | Survived VIF in no models — too collinear with other platform metrics. |
| `platform_spend` | 0/6 | Also removed by VIF. |

**Implication:** Don't hand-pick covariates. Run BIC selection per unit. Platform-wide metrics are mostly collinear and get eliminated. Advertiser-specific dynamics (budget changes, momentum) are far more predictive.

**VIF thresholds:** Start with all candidates, iteratively remove VIF > 10. Typical 14 candidates → 3-7 survivors.

**BIC vs AIC:** BIC penalizes complexity more than AIC. For small N (weekly time series with 50-100 obs), BIC's stronger penalty produces more parsimonious models that generalize better. Prefer BIC.

**Effect of covariate optimization on TI-748:**
- Placebo FPR: 86% → 30% (from 7 hand-picked to BIC-selected per advertiser)
- IVR spend-weighted: -10.56% → +6.50% (FICO went from -14.79% to -0.18% — hand-picked covariates were distorting its prediction)

### Covariates That Were Tested But Not Selected

| Candidate | Why It Was Rejected | Lesson |
|---|---|---|
| `platform_ivr` | Eliminated by VIF (collinear with other platform metrics) | Platform rate metrics are all measuring "market is up/down" — redundant |
| `platform_spend` | Eliminated by VIF | Same as above |
| `platform_vcr` | Highest VIF (3,191) — first to be removed | VCR, IVR, CVR at platform level are all proxies for "market engagement" |
| `platform_impressions` | Eliminated by VIF | Collinear with platform_spend |
| `platform_active_advertisers` | Eliminated by VIF in most advertisers | Correlated with platform_spend |
| `ctv_share` (CTV/display mix) | 3/6 advertisers are 100% CTV (no variance). For the 3 with variance, BIC didn't select it. | Only useful for mixed CTV/display advertisers, and even then not predictive enough |
| `retargeting_cg_count` | Near-zero weekly variance for all advertisers | Static values don't help time series prediction |

### Why Raw Spend Isn't a Direct Covariate

Raw spend (`platform_spend`, `adv_spend`) was eliminated by VIF or not selected by BIC. BUT `spend_change_pct` (week-over-week % change) was selected for ALL 6 advertisers. This makes sense:

- **Raw spend** is collinear with impressions, conversions, and everything else (more spend → more of everything). Including it as a covariate would "explain away" the volume effects we're trying to measure.
- **Spend CHANGE** captures *budget shifts* — "did the advertiser increase/decrease their budget this week?" This is a genuine confound (budget changes affect metrics regardless of media plan) without being mechanically correlated with the outcome.

This is a general principle: **use rate-of-change covariates over level covariates** when the level is mechanically related to the outcome.

---

## MNTN-Specific Experimental Design Notes

### How to Filter Prospecting Campaigns
- Use `campaigns.funnel_level = 1` (NOT objective_id, which is unreliable)
- Always filter `deleted = FALSE AND is_test = FALSE`
- `funnel_level` is on the `campaigns` table, NOT `campaign_groups`

### Attribution Models
- Check `bronze.integrationprod.r2_advertiser_settings.reporting_style`
- `industry_standard` = include competing views/conversions
- `last_touch` = exclude competing views
- All media plan adopters are industry_standard (as of TI-748)

### Best Data Source for Historical KPIs
- `summarydata.sum_by_campaign_by_day` — goes back to 2024-01-01, has all needed columns
- `aggregates.agg__daily_sum_by_campaign` — only from 2025-09-01 (too short for 52-week pre-period)
- `summarydata.sum_by_campaign_group_by_day` — same range as campaign level, useful for CG-level analysis

### Identifying Feature Usage
- `core.media_plan` — `media_plan_status_id = 3` for active plans
- `core.media_plan_publishers` — `badge_state` tracks RECOMMENDED vs USER_MODIFIED vs USER_ADDED
- Join on `media_plan_id` to determine per-plan recommendation status

---

## Experiment Log

| Ticket | Experiment | Method | Outcome | Key Learning |
|---|---|---|---|---|
| TI-748 | Media Plan Causal Impact (v3) | Per-advertiser CausalImpact, BIC-optimized covariates | IVR: 3/6 sig, median +6.2%, spend-weighted +6.5%. Placebo FPR 30%. | BIC covariate selection >> hand-picking. spend_change_pct + metric_lag are universal. Platform metrics are collinear — get eliminated by VIF. Campaign maturity confounds within-advertiser comparison (see TI-780). |
