# Experimentation & Causal Inference — Knowledge Base
Last updated: 2026-03-26 | Started from TI-748 (Media Plan Causal Impact)

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
When analyzing a feature's impact over time, **check for algorithm/config changes during the observation window.** In TI-748, a config change on Feb 3, 2026 (`max_networks` 25→15) split our 8 advertisers into two groups — pre-change plans hurt IVR, post-change plans helped. The "concentration predicts who benefits" finding was actually "config version predicts who benefits."

**Rule:** Before attributing results to a feature, check:
1. Were there algorithm updates during the treatment period? (git log the relevant service repo)
2. Did all treated units receive the same version? (query plan metadata for config snapshots)
3. If versions differ, split the analysis by version era.

This is especially important for staggered adoption designs where early adopters may be on a different algorithm version than later adopters.

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
- High-IVR publishers are often low-volume — the algorithm may optimize for deliverability/reach over IVR
- The benefit of media plan may come from CONCENTRATION (removing the long tail of poor performers) rather than SELECTION (picking the best publishers)
- This distinction matters for product team: should the algorithm optimize for IVR, reach, or some combination?

---

## Experiment Log

| Ticket | Experiment | Method | Outcome | Key Learning |
|---|---|---|---|---|
| TI-748 | Media Plan Causal Impact (v6) | Per-advertiser CausalImpact (BIC covariates, ramp-up exclusion) + panel model | Aggregate IVR near zero (-0.23% spend-weighted). BUT: config change on Feb 3, 2026 (max_networks 25→15) explains the split — new-config plans show +10-17% IVR lift, old-config plans show -26 to -31% decline. | **Primary lesson:** Algorithm version confound. The analysis spanned two configs — pre-change plans diluted across 25-26 publishers, post-change concentrated to 16. Config era, not inherent concentration, is the differentiator. Also: BIC covariate selection beats hand-picking; 4-week ramp-up exclusion needed for new campaigns; selection bias confirmed (hand-picked beta). |
| TI-780 | Ramp-up window research | Empirical analysis of campaign maturity curves | 4-week ramp-up window identified (N=6,917 campaigns, $10K+ spend). Week 4 = first week with <5% WoW change. | Consistent across spend tiers. Steady-state IVR varies by launch quarter (0.008–0.013) — future analyses should use cohort-specific baselines rather than a single global baseline. |
| TI-504 | Fangorn AIS experiment (RCT) | Direct RCT (proportion z-test, Welch t-test, chi², Mann-Whitney, bootstrap CI) + CausalImpact validation + HI-tier segmentation | Edward Martin PP +123%, MI_PP +70%, Collector Store PP +60%, MI +43% — significant across ALL tests. G-Shock/Reedsy/Zumba no effect by t-test; z-test detects small effects at scale. CausalImpact invalid due to population discontinuity (audience split, creative removal, budget, maturity — NOT `is_test` flag which is just a reporting flag). | **Primary lesson:** (1) Synthetic control fails when experiment creates new campaigns with split audiences — use direct RCT comparison instead. (2) Proportion z-test is the standard for IVR in RCTs (confirmed by Nick Martin, experiment owner). (3) t-test on daily rates is a useful secondary consistency check — if z-test is significant but t-test isn't, effect may not be day-over-day reliable. (4) Bootstrap at household level for ratio metrics (CPA/ROAS). (5) Self-referencing covariates produce artificially perfect fits. (6) `is_test = true` excludes from summary tables but does NOT affect delivery. |
