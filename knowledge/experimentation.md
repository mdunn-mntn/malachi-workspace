# Experimentation & Causal Inference — Knowledge Base
Last updated: 2026-03-31 | Started from TI-748 (Media Plan Causal Impact)

This is a living document. Add to it every time we learn something new about experimental design, covariate selection, test methodology, or edge cases at MNTN.

---

## Methodology Selection Guide

| Situation | Method | When to Use | When NOT to Use |
|---|---|---|---|
| Single intervention date, good covariates available | **CausalImpact (BSTS)** | Feature rollout affecting time series metrics | Very short pre-period (<20 weeks) |
| True RCT with control/treatment arms | **Direct comparison (t-test, bootstrap CI)** | Randomized A/B test with matched audiences (e.g., IP bucket split) | Groups not properly randomized |
| Treatment vs control group exists (not randomized) | **Difference-in-Differences (DiD)** | A/B test with comparable groups | Groups not parallel in pre-period |
| Staggered rollout, no control group | **Per-unit CausalImpact** | Feature adopted at different times by different units | Very small N (<5 units) |
| Need to test for lift on a specific metric | **t-test / Mann-Whitney** | Comparing two distributions | Time-dependent data (use time series methods) |
| RCT with skewed metrics or small N, need interpretable probabilistic results | **BEST (Bayesian t-test)** | CPA/ROAS comparison, small advertiser experiments, stakeholder-friendly reporting | Large-N rate metrics where z-test suffices |
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

## Statistical Test Selection for RCTs (TI-504 Lesson)

*Source: TI-504 analysis + Nick Martin (experiment owner), validated 2026-03-31*

### Which test for which metric?

| Metric Type | Test | Why | Example |
|---|---|---|---|
| **Rate metrics (IVR, CVR)** | Two-proportion z-test | Each impression is a Bernoulli trial (visit or not). Large N → normal approximation is valid. | IVR: z-test on (visits/impressions) control vs treatment |
| **Ratio metrics (CPA, ROAS)** | Nonparametric bootstrap (household-level, 5K+ resamples) | Ratio metrics have heavy skew — normal approximations break down. Bootstrap handles the distribution empirically. | CPA: resample households, compute CPA per resample, compare distributions |
| **Daily consistency check** | Welch's t-test on daily rates | Answers "did the effect hold day-to-day?" vs "was the aggregate different?" More conservative, smaller N. | Daily IVR: t-test with N≈21 days |

### Test comparison from TI-504 (same data, different tests):

| Test | Unit | N per group | Significant (of 20) | Notes |
|---|---|---|---|---|
| Proportion z-test | Impressions | 60K-170K | 15/20 | Matches Nick's results. High power detects small effects. |
| Chi-squared (2×2) | Impressions | 60K-170K | 15/20 | Mathematically identical to z-test (z² = χ²) |
| Welch's t-test | Daily IVR | ~21 | 4/20 | Conservative — requires effect to be consistent across days |
| Mann-Whitney U | Daily IVR ranks | ~21 | 4/20 | Non-parametric version of t-test |
| Bootstrap CI | Daily IVR | 10K resamples | — | Confidence interval, not p-value |
| CausalImpact (Bayesian) | Weekly IVR | 20-60 wks | Invalid | Requires population continuity (see gotcha below) |

### Key lessons:
- **Proportion z-test is the standard for IVR in RCTs** — use it as the primary test. Nick (experiment owner) confirmed this is the experimentation team's approach.
- **Bootstrap at the household level for CPA/ROAS** — not at the day level. Household-level preserves correlation structure (one household = multiple impressions). Use 5,000+ resamples.
- **t-test on daily rates is a useful secondary check** — if z-test is significant but t-test isn't, the effect may be driven by a few high-volume days rather than being consistent.
- **Chi-squared and z-test are interchangeable** for 2×2 contingency tables (z² = χ²). No need to run both.
- **Statistical significance ≠ practical significance** — with 170K impressions, even a -0.5% IVR difference is "significant" by z-test. Always report effect size alongside p-values.

### Bayesian Alternative: BEST (Bayesian Estimation Supersedes the t-test)

*Source: Kruschke (2013), J. Experimental Psychology: General 142(2), pp.573-603. Python package: `pip install best` (requires PyMC3).*

**What it is:** A Bayesian replacement for the t-test that models data with Student's t-distribution (not Gaussian), giving it built-in robustness to outliers. Instead of a binary significant/not-significant answer, it produces full posterior distributions for group means, standard deviations, effect size, and their differences.

**Key outputs:**
- **Posterior probability** of any hypothesis — e.g., "87% probability that treatment mean exceeds control by ≥0.5" — far more interpretable than p-values
- **Highest Posterior Density Interval (HDI)** — Bayesian credible interval (e.g., 95% HDI = the narrowest interval containing 95% of the posterior). Unlike frequentist CIs, this IS the probability that the parameter lies in the interval.
- **Effect size** — built-in: (μ₁ - μ₂) / √[(sd₁² + sd₂²) / 2]
- **Normality parameter (ν)** — controls outlier tolerance. ν < 10 = heavy tails (outlier-robust), ν > 30 ≈ normal distribution. Logarithmic scale because shape changes rapidly near ν=3 but stabilizes above 30.

**When to use at MNTN:**
- **Ratio metrics (CPA, ROAS)** where distributions are heavily skewed — BEST's t-distribution handles outliers that break normal-theory tests
- **Small N experiments** (5 advertisers) where p-values are underpowered but posterior probability and HDI still give useful information
- **Stakeholder communication** — "82% probability of improvement" is more intuitive than "p=0.07, not significant"
- As a **complement to (not replacement for)** the proportion z-test for rate metrics — z-test remains primary for IVR given the experimentation team's standard

**When NOT to use:**
- Rate metrics with large N (IVR with 170K impressions) — proportion z-test is standard and sufficient
- When the audience needs traditional frequentist reporting (some stakeholders expect p-values)

**Usage:**
```python
import best
result = best.analyze_two(control_data, treatment_data)
fig = best.plot_all(result)  # generates posterior plots for all parameters
result.posterior_prob('Difference of means', low=0)  # P(treatment > control)
result.hdi('Difference of means', 0.95)  # 95% credible interval
```

**Reporting note:** BEST displays the posterior **mean** for symmetric distributions (group means) and the **mode** for skewed distributions (standard deviations). Always report the HDI alongside either measure.

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

### Algorithm Version as Confound (TI-748 Lesson)
When analyzing a feature's impact over time, **check for algorithm/config changes during the observation window.** In TI-748, a config change on Feb 3, 2026 (`max_networks` 18→25→15, `min_allocation` 1%→0.5%, spend capacity filter added — PERML-412) split our 8 advertisers into two groups — pre-change plans hurt IVR, post-change plans helped.

**Refined understanding (Chris Addy, 2026-03-31):** The initial framing was "config version predicts who benefits." The corrected framing is **"concentration is the mechanism, and the config change made concentration the default."** Lighting New York proves this: they got 16 publishers under the *old* config (Oct 2025) — not from an override, but because natural pruning from their budget/vertical dropped networks below the old 1% `min_allocation` floor. Same concentration, same positive result, different config era. The mechanism is publisher concentration itself, regardless of how it was achieved.

**Rule:** Before attributing results to a feature, check:
1. Were there algorithm updates during the treatment period? (git log the relevant service repo)
2. Did all treated units receive the same version? (query plan metadata for config snapshots)
3. If versions differ, split the analysis by version era.
4. **If the mechanism is identified, test whether it operates independently of version** — as with Lighting NY proving concentration works under any config.

This is especially important for staggered adoption designs where early adopters may be on a different algorithm version than later adopters.

### Runtime Config as Experiment Prerequisite (TI-748 Lesson)
Per-advertiser config overrides are NOT natively supported for the media plan algorithm today. The only advertiser-level control is blacklisted networks. To run a clean A/B test on algorithm parameters (e.g., alpha=7 vs alpha=5), the `MediaPlanConfig` must be made runtime-updateable (Chris Addy is planning this). Without it, the only option is a time-based rollout (change the config globally), which confounds treatment with time.

**Lesson for experiment design:** Before proposing a parameter A/B test, verify:
1. Can the parameter be varied per-unit (per-advertiser, per-campaign)? If not, time-based rollout is the only option.
2. Is there an API-level override available, or does it require a deploy?
3. Who owns the config and can trigger changes? (For media plan: Chris Addy / olympus team)

### Population Continuity — When Synthetic Control Fails (TI-504 Lesson)

**Synthetic control / CausalImpact requires that the pre-period and post-period measure the SAME population.** If the experiment creates new campaigns targeting a different audience subset, you cannot use the parent campaign's history as a pre-period baseline.

**TI-504 example (Fangorn AIS experiment):**
- Nick cloned 5 advertisers' prospecting campaigns into control/treatment arms
- Each arm targeted a specific IP bucket range (e.g., 600-713 for control PP, 714-768 for treatment PP)
- The parent campaigns targeted the FULL audience (buckets 0-599)
- Parent campaign IVR: 0.03-0.12. Experiment campaign IVR: 0.005-0.024. That's a 3-10x gap.
- This gap is NOT a treatment effect — it's a different population (different IP buckets = different people)
- CausalImpact showed -37% to -79% effects with 69% avg placebo FPR. The model was measuring the population difference, not Fangorn's impact.

**When to use CausalImpact vs direct RCT comparison:**

| Scenario | Right Method | Why |
|---|---|---|
| Feature enabled on existing campaigns (same audience, same targeting) | CausalImpact with pre/post | Same entity, same population — pre-period baseline is valid |
| New campaigns created with split audiences (A/B test) | Direct RCT comparison (t-test, bootstrap CI) | Different populations — no valid pre-period exists for the experiment campaigns |
| Staggered rollout to existing campaigns | Per-unit CausalImpact | Same entity across time, different intervention dates |
| Cloned campaigns with holdout groups | Direct comparison of control vs treatment arms | The control group IS the counterfactual — no need to synthesize one |

**The rule:** If you have a real control group, use it directly. CausalImpact is a tool for when you DON'T have a control group and need to build a synthetic one. Using synthetic control when a real control exists is both unnecessary and prone to error.

**How to check:** Before running CausalImpact, verify:
1. Is the pre-period entity the same as the post-period entity? (Same campaign_id, same audience definition, same targeting)
2. If new campaigns were created for the experiment, do they target the same population as the baseline campaigns?
3. If audiences were split (IP hashing, holdout groups), the pre-period campaigns targeted the full audience — you CANNOT use their IVR as a baseline for the subset.

**Self-referencing covariate trap:** In TI-504, setting `control_ivr = response_variable` during the pre-period (because the control arm didn't exist yet) produced artificially perfect pre-period fit. The model looked great but was cheating — the "covariate" was the answer. When removed, the model collapsed to -70% effects with massive uncertainty. Always verify covariates are genuinely external to the response.

**Why TI-748 worked but TI-504 didn't — what's actually different:**
In TI-748, new media plan campaigns REPLACED the old campaigns and targeted the **same full audience**. The pre/post IVR levels were comparable (same order of magnitude). In TI-504, experiment campaigns ran at **8-40% of the advertiser's normal prospecting IVR** (tested with both parent-only AND all-prospecting baselines — same result). The gap is far too large for random IP bucketing to explain. Contributing factors: audience splitting (buckets 600-999 = ~40% of original audience), creative removal ("Only One Creative" for most experiment campaigns), lower budgets (experiment budget vs advertiser's full spend), and fresh campaigns with no optimization history. When pre/post IVR differs by 3-10x, no covariate can bridge the gap — the model just measures the structural difference, not the treatment.

**Note:** `is_test = true` is just a reporting flag — it does NOT affect delivery priority or bidder behavior. The IVR gap is from the experiment setup factors above, not the flag itself. However, `is_test = true` does exclude campaigns from all summary/aggregate tables, so you must query log-level tables (cost_impression_log, clickpass_log) directly.

**HI-tier segmented analysis (follow-up):** To rule out audience composition as the cause of the IVR gap, segmented by intent tier using `advertiser_household_score` (HHST >= 6666 = HI tier). Old prospecting campaigns were 89-99% HI tier traffic. Even within HI tier only, experiment campaigns ran at 0.08-0.53x of historical HI-tier IVR (historical: 0.025-0.117, experiment: 0.008-0.034). This confirms the gap is from experiment setup factors (audience split, creative, budget, maturity), not audience composition differences.

- **Experiment campaigns have structurally different IVR than production campaigns** — due to audience splitting, creative limitations, budget constraints, and campaign maturity. Cannot use production campaign baselines for experiment campaign CausalImpact.

**Lesson:** Before running CausalImpact, check the **IVR ratio** between pre-period and post-period. If it's less than ~0.5x or more than ~2x, the structural gap is too large for synthetic control — something fundamental changed beyond the treatment.

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

## Key Findings from MNTN Experiments (Institutional Knowledge)

*Source: Kirsa (Experimentation Lead), meeting 2026-03-30*

### Frequency — Most Counterintuitive and Consistent Result

Lower frequency = better performance. This has been validated across multiple experiments over several years:

- Capping frequency at very low levels (1 per 14 days, 1 per 30 days) achieved comparable performance to multi-touch campaigns
- The more unique households reached (lower frequency), the better performance is overall
- **Why "ideal frequency" from converter data doesn't work:** Data analysts identified the average frequency of converters and applied it to campaigns — performance got WORSE. Reason: that frequency is optimal for getting an individual household to convert, but all the non-converters also get that same frequency. The wasted impressions/spend on people who never would have converted has more negative impact than the incremental conversions from borderline prospects.
- **Principle:** Reach (unique households) matters more than repetition. Optimize for breadth, not depth.
- Frequency experiments haven't been run in a while — should be re-run every couple of years because platform changes may shift optimal frequency.

### Targeting — Most Impactful Lever (Not Even Close)

Every targeting-based experiment has been the most impactful lever for performance improvement:

- **Mountain Match V1 vs Interest Audiences:** ~500% performance improvement — "insane" by experiment standards where 10-15% is considered a win
- **Fangorn (bottoms-up keywords):** 50-100% peak performance improvements
- **Frequency/other optimizations:** Much more likely to have a very slight change
- Targeting improvements dwarf all other optimization levers

### Experiment Shelf Life — ~6 Months

Experiments have roughly a 6-month expiration date because the platform, feature set, and environment change so often. What was true 6 months ago may no longer hold because the "starting line" has moved.

**Example:** Multi-touch was disabled for new customers after experiments showed TV-only with low frequency performed comparably. But the control baseline was a 2021 multi-touch campaign. By 2024-2025, the platform had changed enough that TV-only no longer outperformed multi-touch against current baselines → multi-touch is being encouraged again.

### Performance vs Scale Tension

The most important tension in experimentation: **it's very easy to improve performance, but not so easy to do it without negatively impacting ability to spend.** Tighter targeting, lower frequency, better audience quality all improve performance metrics — but they shrink the addressable audience, reducing ability to deliver impressions and spend budget.

This is especially acute for retargeting, where audiences are already small. TV-only retargeting showed comparable performance but couldn't deliver at scale. The fix would require a massive increase in scale (more inventory, more targetable IPs, etc.) to offset the audience reduction.

### Metrics Philosophy (from Mark/Leadership)

- **IVR (impression-to-visit rate) is the primary metric** — Mark's rationale: MNTN can only drive visits; conversions depend on the advertiser's site experience. So the metric MNTN should optimize is visit generation.
- **CPA and ROAS are guardrail/supporting metrics** — always reviewed alongside IVR. If IVR improves but CPA gets worse or ROAS declines, that's flagged and caveated.
- **Visit quality matters** — not just visit volume. Visit conversion rate (conversions/visits) and effective conversion rate (conversions/impressions) are also tracked.
- **All metrics are reviewed for every experiment** — the more information the better. Looking for potential negative impacts beyond the primary metric.

### Experiment Budget & Structure

**Two types of experiments:**

| Type | Description | Who Pays | Budget Split |
|---|---|---|---|
| **Customer-facing** | Advertiser agrees to participate, visible in their campaign/reporting | MNTN typically covers 50% to incentivize participation | Shared |
| **Non-customer-facing (hidden)** | Behind-the-scenes experiments (e.g., Fangorn). Hidden campaigns, not visible to advertiser in UI/reporting | MNTN covers 100% | From Kirsa's monthly experiment budget |

**Current constraints:**
- 5 advertisers per experiment (limited by budget and manual setup)
- Budget is a set monthly amount

### Campaign Splits — Key Future Improvement

The ability to split out a portion of a live campaign and experiment with it (e.g., 10% of a campaign's audience gets a different frequency):

- **Eliminates budget constraint** — uses the advertiser's existing budget, no additional MNTN spend
- **Scales to 50+ advertisers per experiment** instead of 5
- **Enables much larger sample sizes** → more definitive causal impact results
- This is a near-term initiative Kirsa and team are planning

### Methodology Improvement (Needed Once Campaign Splits Land)

Once the 5-advertiser constraint is removed, the team needs:
- Power analyses for sample sizing
- Formal methodology for advertiser selection to avoid bias
- Documentation of selection criteria and randomization
- More rigorous statistical approaches that are feasible with larger N

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
- **CRITICAL REQUIREMENT:** The pre-period and post-period must measure the **same entity** (same campaign, same audience, same targeting). If the experiment creates new campaigns with different audience definitions, the pre/post comparison is invalid — you're comparing different populations, not measuring a treatment effect. See "Population Continuity" gotcha below.

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
- High-IVR publishers are often low-volume — the algorithm optimizes for deliverability/reach, not IVR
- **The benefit comes from CONCENTRATION (removing the long tail of poor performers) rather than SELECTION (picking the best publishers)**
- Lighting NY confirms: natural pruning from budget/vertical constraints produced the same 16-publisher concentration and the same positive result as the explicit config change did for later adopters
- Per-publisher scores (score_semantic, score_performance, score_spendability, etc.) are NOT in BQ — stored as JSON in GCS (`media-plan-artifacts` bucket, path `media-plan/{version}/{advertiser_id}/{plan_id}/response.json`). Would need BQ sink or GCS parsing for score-level analysis.
- This distinction matters for product team: the algorithm doesn't need to pick the *best* publishers — it needs to prune the *worst* ones. HHI as a guardrail input would operationalize this.

---

## Experiment Log

| Ticket | Experiment | Method | Outcome | Key Learning |
|---|---|---|---|---|
| TI-748 | Media Plan Causal Impact (v5) | Per-advertiser CausalImpact (BIC covariates, ramp-up exclusion) + panel model | Aggregate IVR near zero (-0.23% spend-weighted). BUT: config change on Feb 3, 2026 (max_networks 18→25→15, PERML-412) explains the split — new-config plans show +10-17% IVR lift, old-config plans show -26 to -31% decline. | **Primary lesson:** Concentration is the mechanism, config change made it the default. Lighting NY (16 pubs under old config from natural budget/vertical pruning) proves concentration works regardless of config era. Also: BIC covariate selection beats hand-picking; 4-week ramp-up exclusion needed for new campaigns; selection bias confirmed (hand-picked beta); per-publisher scores only in GCS (not BQ); runtime config needed for clean A/B tests. |
| TI-780 | Ramp-up window research | Empirical analysis of campaign maturity curves | 4-week ramp-up window identified (N=6,917 campaigns, $10K+ spend). Week 4 = first week with <5% WoW change. | Consistent across spend tiers. Steady-state IVR varies by launch quarter (0.008–0.013) — future analyses should use cohort-specific baselines rather than a single global baseline. |
| TI-504 | Fangorn AIS experiment (RCT) | Direct RCT (proportion z-test, Welch t-test, chi², Mann-Whitney, bootstrap CI) + CausalImpact validation + HI-tier segmentation | Edward Martin PP +123%, MI_PP +70%, Collector Store PP +60%, MI +43% — significant across ALL tests. G-Shock/Reedsy/Zumba no effect by t-test; z-test detects small effects at scale. CausalImpact invalid due to population discontinuity (audience split, creative removal, budget, maturity — NOT `is_test` flag which is just a reporting flag). | **Primary lesson:** (1) Synthetic control fails when experiment creates new campaigns with split audiences — use direct RCT comparison instead. (2) Proportion z-test is the standard for IVR in RCTs (confirmed by Nick Martin, experiment owner). (3) t-test on daily rates is a useful secondary consistency check — if z-test is significant but t-test isn't, effect may not be day-over-day reliable. (4) Bootstrap at household level for ratio metrics (CPA/ROAS). (5) Self-referencing covariates produce artificially perfect fits. (6) `is_test = true` excludes from summary tables but does NOT affect delivery. |
| BUK Exp 1 (Sep 2025) | BUK vs MM V2 — fixed 200 keywords | A/B with audience isolation blocking (old method, had IP contamination) | Control outperformed across ALL 5 advertisers (IVR 1.53% vs 0.67%, CPV $5.07 vs $9.08). 200 keywords pushed vertical coverage to 80-88%. | **Primary lesson:** More keywords ≠ better. Fixed keyword count is wrong approach — need a threshold. Within treatment, IPs overlapping with control keywords performed 5-10x better than non-overlapping — validates keyword quality but not audience sizing strategy. |
| BUK Exp 2 (Nov-Dec 2025) | BUK vs MM V2 — percentile threshold + score adjustments | True A/B via MD5 hash on IP (no bleed). 5 advertisers, 2 weeks (11/21–12/4). Percentile threshold ~top 42%, popularity penalty, advertiser lift. | +26.84% avg relative IVR lift, but inconsistent across advertisers. Hello Molly +137% (audience -83%), Hatch -25% (audience +93%). | **Primary lesson:** Performance inversely correlated with audience size change — size confounding makes results ambiguous. All 5 advertisers show negative rank-performance correlation (higher BUK rank → higher visit rate), confirming rankings are directionally aligned. Next experiment must control for audience size to isolate keyword quality effect. |

---

### BUK Experiment Design Lessons (2026-03-31, Alex Knorr knowledge transfer)

- **Campaign ramp-up period ~1 month**: Beeswax bidding dynamics take ~1 month to stabilize after campaign changes. Cannot reliably compare experiments during this window. For RCTs with simultaneous test/control launch this is less of an issue (both arms ramp together), but for pre/post or sequential designs it's critical.
- **Audience size is the #1 confounding variable in keyword experiments**: Changing keywords almost always changes audience size. Larger high-intent audience → lower IVR (more diluted pool), smaller → higher IVR (more concentrated). Must either (a) size-match BUK and MM V2 audiences within 5%, (b) adjust keyword count to force alignment, or (c) use continuous scoring which sidesteps the size problem.
- **Old A/B split method (audience isolation) has contamination**: IP gets impression from one campaign → added to block list for other campaign. But processing delay means IPs can appear in both groups. New method: MD5 hash on IP → deterministic split into hash buckets → no bleed by design. All future experiments should use this.
- **Optimal cutoff exists between include-all and exclude-bad**: (Malachi's Etsy experience) Removing the worst items/keywords always decreased total ROI past a certain point — there's an optimal threshold, not a binary decision. Directly applicable to BUK keyword count thresholding.
- **Cost vs. performance differential is negligible between intent tiers**: Cost difference between high/mid/low intent is only a few percent, but visit rate difference is 10-50x. Should always bid on highest-value IPs first — continuous scoring enables this without sacrificing audience size.

### Keyword Analysis Methodology Lesson (TI-804, 2026-04-02)
- **Per-advertiser keyword analysis >> global keyword analysis**: Global keyword ranking shows only 3x visit rate range. Per-advertiser ranking shows 184x. Always analyze keyword performance per-advertiser, not globally.
- **"Best keyword rank" approach works better than per-keyword visit rates**: Computing visit rate per individual keyword is too sparse (most keyword-advertiser pairs have 0 visitors in a 10-day window, medians are 0). Instead, find each IP's best-ranked matched keyword, bucket by that rank, and compute visit rates per bucket. This aggregates signal effectively.
- **Temporal separation prevents circularity**: ipdsc DS19 keywords are from PAST browsing behavior. Measure visits in a FUTURE window. Same IP universe, different time periods. No campaign-scoping needed for "does the signal predict?" questions — campaign-scoping needed for "did our ads cause?" questions (TI-806).
- **50-advertiser sample is sufficient for directional findings** but only 15 had >10 visitors in a 10-day window. Scale to 500 for presentation-quality results.

### Exploitation vs Exploration in Optimization (Kale, 2026-03-31)

Purely exploitative optimization — targeting only the highest-intent users — is "somewhat dangerous" (Kale's words). Two risks:
1. **Incrementality problem:** Highest-intent users are also targeted by Google/Meta. When MNTN only bids on these users, incrementality reports look bad because the conversions would have happened anyway via other channels.
2. **Local maximum trap:** Without exploration, the system never discovers potentially better audience segments. Multi-armed bandit framing: 100% exploit means 0% discovery.

**Kale's direction:** Media plan budget allocation, Fangorn scoring, and any future optimization should eventually bake in exploration — allocate a slice of budget to test alternative paths, not just exploit the current best guess. Not prescriptive on shape yet, but conceptually: the system needs to explore.

**Implication for experiments:** When measuring a targeting change, consider both:
- The standard KPI (IVR, ROAS, etc.) — the "exploitation" metric
- An "opposing" metric (Malachi referenced Andy Grove's management principle) — e.g., reach vs performance, incrementality vs conversion rate

### Retargeting Frequency Diminishing Returns (Kirsten, reported 2026-03-31)

Kirsten's observation from past experiments: retargeting the same people repeatedly → performance always went down. More unique people targeted → better. Aligns with the frequency experiments documented above (lower frequency = better). Kirsten caveat: "things change all the time" — worth re-validating.

### Feature Importance Methodology Lessons (TI-790, 2026-03-31, updated 2026-04-01)

- **Pre-visit vs feedback feature leakage**: guid_log and conversion_log features are outcome-adjacent — they exist BECAUSE the IP visited. Including them produces AUC ~0.999 (tautological — `gl_n_events > 0` perfectly identifies visitors by definition). Pre-visit features alone give AUC 0.831. Always separate features by when they become available (bid time vs post-visit).
- **Temporal leakage — use day N-1 features, day N labels**: Same-day features and labels allow post-visit impressions to leak into "pre-visit" features (an IP that visited at 8am has 9pm impressions counted as features). Fixing this (day N-1 features, day N labels) dropped AUC from 0.842 to 0.831 — leakage was real but small (0.011). Rankings were stable across the correction, confirming findings hold. Always enforce strict temporal separation.
- **Sample selection bias in targeting-system evaluation**: When training data is pre-filtered by an existing targeting system (e.g., Fangorn selects which IPs get impressions), you CANNOT claim new features "work independently." Every IP in the sample was already chosen by the system. A NEW-only model (existing features removed) showing 7x lift means the new features add discriminative power *within the Fangorn-selected pool* — not that they'd perform equally on a random untargeted population. To test true independence, you'd need a random sample of IPs that were NOT pre-filtered by targeting, which doesn't exist in production data. Frame claims accordingly: "adds signal on top of current targeting" not "predicts visits independently."
- **Composite importance method works**: XGBoost with gain, weight, cover averaged into composite rank produces stable rankings. SHAP (mean absolute Shapley value) is preferred for final reporting — it captures per-prediction contributions, not just tree-structure importance.
- **Existing Fangorn signals dominate pre-visit features**: `ci_pct_new`, `n_wins_this_adv`, `al_avg_segments` hold top 3 ranks. New bidstream features (device diversity, content genre, clearing price) add incremental signal. When existing features are removed, new features still achieve AUC 0.777 (7x lift at top 1%) within the Fangorn-selected population.
- **Content genre rises with proper sampling**: With 1-hour augmentor/BAE samples, content features ranked mid-tier. With 4-hour augmentor + full-day BAE, `bae_pct_ent` jumped from #26 to #8. Sampling window materially affects count-based AND genre-percentage features. Use full-day data when possible; disclose sampling window when not.
- **Content genre is mid-tier for prediction but high-value for segmentation**: content_genre ranked ~8th for general visit prediction (up from ~25th with proper sampling), and is the best candidate for *vertical classification* (mapping IPs to advertiser categories). Different use case than IVR prediction — both valuable.
- **Scale matters for feature extraction**: augmentor_log = 241 GB/day (~43 GB for 4-hour sample), bidder_auction_events = ~400 GB/day. Always dry-run first. guid_log and win_logs are cheap (~13-75 GB/day).
- **fillna(0) vs NaN for ratio features**: XGBoost handles NaN natively. For ratio/percentage features, 0 is a real value (e.g., avg_price=0 means free inventory), while NaN means "no data." Preserving NaN for ratios and using fillna(0) only for counts is the correct approach.
