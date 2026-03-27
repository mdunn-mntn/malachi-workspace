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
| Staggered adoption, small N per unit, need one aggregate estimate | **Panel data model (two-way FE)** | Units adopt at different times, want a single population-level treatment effect | Want per-unit effects, or units have enough data for individual time series models |
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

### Campaign Maturity Bias (TI-780 — Empirically Determined)

New prospecting campaigns reach steady-state IVR in approximately **4 weeks** (N=6,917 campaigns, $10K+ spend):
- **Week 0:** 38% of steady state (just launched)
- **Week 2:** 84% (rapid improvement as bidder learns)
- **Week 4:** 89% — first week with <5% WoW change (ramp-up over)
- **Week 8+:** Fully stabilized

This pattern is consistent across spend tiers (high/mid/low) and is driven by bidder learning, frequency buildup, and delivery footprint exploration.

**Steady-state IVR varies by launch quarter** (0.008–0.013). Campaigns launched in different quarters converge to different baselines. Future analyses should consider cohort-specific baselines (by launch quarter) rather than assuming a single global steady-state value.

**Rule: Exclude the first 4 weeks of any new campaign from causal analysis.**

This applies to:
- CausalImpact post-period start (shift 4 weeks after first delivery)
- Within-advertiser comparisons (only include campaigns with 4+ weeks of delivery)
- Any future experiment comparing new vs existing campaigns

### Selection Bias
Advertisers who adopt a new feature may be systematically different:
- More engaged with the platform
- More sophisticated marketers
- Growing faster (or struggling — looking for improvements)
- Managed by specific account teams

**TI-748 confirmed:** Media Plan beta advertisers are hand-picked by PEX/CS (identified candidates with prior interest) and validated by production ops (Toph) for pacing risk. NOT randomized. This is the strongest form of selection bias short of self-selection.

Mitigation: Use pre-period trends as covariates, document the bias explicitly. For future rollouts, use waitlist control design with randomized wave ordering.

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

### Panel Data Model vs Per-Unit CausalImpact

Two approaches for staggered adoption experiments:

| | Panel Data Model (Two-Way FE) | Per-Unit CausalImpact |
|---|---|---|
| **Use when** | Staggered adoption, small N per unit, need one aggregate treatment estimate | Want per-unit treatment effects, have enough data per unit (20+ pre-period obs) |
| **Output** | Single population-level ATT with one p-value | Per-unit effect sizes, can see heterogeneity |
| **Strengths** | Pools data across units for power; handles short per-unit series; one clean estimate | Transparent per-unit results; can identify which units drove effects; individual placebos |
| **Weaknesses** | Loses per-unit granularity; assumes homogeneous treatment effect; harder to diagnose issues | Requires sufficient pre-period per unit; aggregation choices (spend-weighted vs median) matter |
| **Covariates** | Time FE absorbs common shocks; unit FE absorbs level differences; add unit-varying covariates | Per-unit BIC selection from candidate set |

**Lesson from TI-748:** Panel model (v5) gave +2.06% not significant. Per-unit CausalImpact (v3) showed 3/6 significant with +6.5% spend-weighted. The panel model's homogeneity assumption may wash out real heterogeneous effects. When treatment effects vary across units, per-unit analysis can be more informative despite lower power per unit.

---

## Jira Practices

When creating Jira tickets, always include:
- **Story points** (`customfield_10012`) — required for sprint planning
- **PMO Rep** — assign the appropriate PMO representative
- **Release type** — specify the release type for the ticket

These fields are frequently missed but are required by PMO for sprint tracking and release management.

---

## Experimental Design for Feature Rollouts — Balancing Risk and Statistical Power

### The Core Tension
We need high N for reliable results, but can't roll out risky changes to all advertisers at once. Methods to balance this:

### 1. Staggered Rollout (What We Did for Media Plan)
- Roll out to a small beta group first, expand over time
- **Pros:** Low risk — can stop if early results are bad
- **Cons:** Small N reduces statistical power. Selection bias — early adopters are volunteers who may differ systematically. Staggered dates complicate analysis.
- **When to use:** Opt-in features, low-risk changes, exploratory analysis
- **TI-748 lesson:** N=8 was too small for conclusive results. The analysis was methodologically sound but statistically underpowered.

### 2. Randomized Controlled Trial (Gold Standard)
- Randomly assign advertisers to treatment (feature on) vs control (feature off)
- **Pros:** Eliminates selection bias. Clean comparison. Highest statistical credibility.
- **Cons:** Requires engineering support to randomize. Some advertisers may notice and complain. Can't "unsee" the results if negative.
- **When to use:** High-stakes features, need definitive answer, have engineering support
- **How to size it:** Standard power analysis. For detecting a 5% IVR lift with 80% power and 5% significance: need ~200 advertisers per group (rough estimate — depends on IVR variance).

### 3. Matched Pairs Design
- For each treatment advertiser, find a similar control advertiser (same vertical, spend tier, campaign count)
- **Pros:** Controls for selection bias without full randomization. Smaller N needed than population-level RCT.
- **Cons:** Matching quality depends on observable characteristics — unobservable differences remain. Hard to find good matches with small advertiser pool.
- **When to use:** Can't randomize, but can identify comparable non-adopters

### 4. Waitlist Control
- All advertisers get the feature eventually, but rollout is randomized in waves
- **Pros:** Everyone gets the feature (no ethical concerns). Early waves act as treatment, later waves act as control. Staggered adoption by design.
- **Cons:** Requires coordinated rollout schedule. Late-wave advertisers may learn about the feature from early adopters (contamination).
- **When to use:** Features that will eventually go to everyone. Best balance of risk and power.
- **How it works:** Wave 1 gets it in week 1, Wave 2 in week 5, Wave 3 in week 9. At week 4, compare Wave 1 (treated) to Waves 2+3 (not yet treated).

### 5. Synthetic Control (What CausalImpact Does)
- Each treated unit builds its own counterfactual from covariates
- **Pros:** No explicit control group needed. Works with staggered adoption. Per-unit effects.
- **Cons:** Relies on covariate quality. Short pre-periods reduce reliability. Placebo FPR can be high.
- **When to use:** Observational data, can't randomize, need per-unit effects

### Recommendation for MNTN
For future feature evaluations, **waitlist control** is the ideal approach:
1. Product decides the feature will eventually go to all eligible advertisers
2. Randomly order the rollout (not by request, not by account team preference)
3. First wave is treatment, remaining waves are control
4. After sufficient post-period (4 weeks + ramp-up), analyze treatment vs not-yet-treated
5. Expand to next wave, repeat

This gives us: randomization (no selection bias), adequate N (entire eligible population), ethical soundness (everyone gets it), and clean analysis (DiD with randomized treatment timing).

### Publisher-Level Analysis Lesson (TI-748)
When analyzing features that affect publisher/network allocation:
- `sum_by_ctv_network_by_day` has per-publisher, per-campaign, per-day performance data
- Publisher IVR varies dramatically (Spectrum News 1.09% vs Samsung TV+ 0.48% for Lighting New York)
- High-IVR publishers are often low-volume — the algorithm may optimize for deliverability/reach over IVR
- The benefit of media plan may come from CONCENTRATION (removing the long tail of poor performers) rather than SELECTION (picking the best publishers)
- This distinction matters for product team: should the algorithm optimize for IVR, reach, or some combination?

---

## Experiment Log

| Ticket | Experiment | Method | Outcome | Key Learning |
|---|---|---|---|---|
| TI-748 | Media Plan Causal Impact (v5) | Panel data model (two-way FE), BIC covariate selection + ramp-up exclusion | IVR: +2.06% (not statistically significant). Placebo FPR 24%. BIC + ramp-up integrated. | Panel model gives one aggregate estimate but lost per-unit granularity. BIC covariate selection + 4-week ramp-up exclusion improved placebo FPR from 30% to 24%. Not significant — media plan effect is small or nonexistent at population level. |
| TI-780 | Ramp-up window research | Empirical analysis of campaign maturity curves | 4-week ramp-up window identified (N=6,917 campaigns, $10K+ spend). Week 4 = first week with <5% WoW change. | Consistent across spend tiers. Steady-state IVR varies by launch quarter (0.008–0.013) — future analyses should use cohort-specific baselines rather than a single global baseline. |
