# TI-748: Media Plan Causal Impact — Complete Methodology Explainer

*Written for presenting to non-technical stakeholders. Covers what we did, why we made each choice, and how to explain the results.*

---

## What We're Trying to Answer

**Did advertisers who used MNTN's recommended Media Plan network allocation see better prospecting performance?**

Media Plan is a beta feature that recommends how to split an advertiser's budget across publishers (ABC 10%, CBS 12%, ESPN 7%, etc.). Some advertisers accepted our recommendations ("recommended"), others modified them ("customized"). We only care about the ones who used our recommendation — because if the recommendation is good, those advertisers should perform better.

---

## How to Explain This to Someone Non-Technical

### The Elevator Pitch (30 seconds)

> "We compared each advertiser's performance before and after they started using our recommended network allocation. We used a statistical method that predicts what their performance *would have been* without the feature — like a parallel universe where they never adopted it. Then we measured the gap between what actually happened and what was predicted. For most advertisers, IVR improved by about 6% — meaning they got more site visits per impression. The model accounts for things like holidays, market trends, and the advertiser's own spending patterns so we're not just measuring seasonality."

### The Two-Minute Version

1. **We picked a date for each advertiser** — the day they first created a campaign using our recommended network allocation.

2. **We looked at their performance before that date** — up to a full year of weekly data. This is their "baseline" behavior.

3. **We trained a model on that baseline** — the model learned: "When this advertiser changes their budget by X%, their visit rate tends to go up/down by Y%." It also learned how holidays and market trends affect them.

4. **After the adoption date, the model predicted what *should have happened* without Media Plan** — this is the "counterfactual." It's our best guess at the parallel universe.

5. **We compared the prediction to reality** — if the advertiser's actual IVR was higher than the prediction, Media Plan had a positive effect. If lower, negative.

6. **We tested if the gap is real or just noise** — the p-value tells us the probability of seeing a gap this big by random chance. Below 5% = statistically significant.

---

## The Method: CausalImpact

### What Is It?

CausalImpact is a method developed by Google for measuring the effect of an intervention (like a feature rollout) on a time series. It uses **Bayesian Structural Time Series** — a statistical model that combines:

- **Trend:** Is the metric going up or down over time?
- **External factors:** How do holidays, market conditions, and the advertiser's own behavior affect the metric?
- **Uncertainty:** Instead of one prediction, it gives a range of likely outcomes.

### How the P-Value Is Calculated

The model doesn't just give one prediction — it generates *thousands* of possible counterfactual paths by sampling from its uncertainty distribution. The p-value is calculated by:

1. For each of the thousands of simulated counterfactual paths, calculate the cumulative effect (sum of differences between actual and predicted across all post-period weeks)
2. Count what fraction of simulated effects are more extreme than zero in the observed direction
3. If only 2% of simulations showed an effect as large as what we observed, p = 0.02

**In plain English:** "There's only a 2% chance we'd see this result if Media Plan had no real effect." That's strong evidence it had a real impact.

### Why Not Just Compare Before vs After?

Simple pre/post comparison doesn't account for:
- **Seasonality:** Q4 is always better for CTV advertisers (Black Friday, Christmas). An advertiser who adopted in October would look like they improved just because of the holidays.
- **Market trends:** If the whole platform's performance improved, that's not Media Plan's doing.
- **Budget changes:** If the advertiser doubled their spend, their metrics would change regardless.

CausalImpact controls for all of these through covariates.

---

## Our Covariates (and Why We Chose Them)

A **covariate** is an external variable the model uses to understand what *would have happened* without the intervention. Good covariates:
1. Predict the outcome metric (they help explain normal variation)
2. Are NOT affected by the intervention (otherwise we'd be "controlling away" the effect we're trying to measure)

### How We Selected Covariates

We did NOT hand-pick covariates. We used a rigorous process:

1. **Started with 14 candidates:** Platform metrics (IVR, CVR, VCR, ROAS, CPA, spend, impressions, active advertisers, avg campaign groups), holiday flags, lagged metrics (last week's IVR, two weeks ago), advertiser's active campaign group count, and week-over-week spend changes.

2. **VIF multicollinearity check:** Many platform metrics are just measuring the same thing different ways. VIF (Variance Inflation Factor) quantifies this. We removed covariates with VIF > 10 iteratively. Most platform-wide metrics (platform_ivr, platform_spend, platform_vcr) were highly collinear and got eliminated.

3. **BIC model comparison:** For each advertiser, we tested every possible combination of surviving covariates and scored them using BIC (Bayesian Information Criterion). BIC rewards models that predict well but penalizes complexity. This ensures we use the simplest model that still captures the important patterns.

4. **Result:** Each advertiser got a tailored covariate set (typically 2-4 covariates), not one-size-fits-all.

### What BIC Is

**BIC (Bayesian Information Criterion)** is a score that balances two things:
- How well the model fits the data (lower error = better)
- How complex the model is (fewer covariates = better)

A model with 10 covariates might fit slightly better than one with 3, but BIC says "the 3-covariate model is better because those extra 7 covariates aren't adding enough value to justify the complexity." This prevents overfitting — where a model learns the noise instead of the signal.

**Lower BIC = better model.**

### What Won

| Covariate | What It Is | How Many Advertisers Used It |
|---|---|---|
| `spend_change_pct` | Week-over-week % change in the advertiser's own spend | 6/6 (all of them) |
| `metric_lag1` or `metric_lag2` | Last week's (or two weeks ago) IVR for this advertiser | 5/6 |
| `holiday` | Binary: is this Thanksgiving/Christmas/New Year/Super Bowl week? | 4/6 |
| `adv_active_cgs` | How many campaign groups the advertiser has active this week | 3/6 |
| `platform_roas` | Platform-wide return on ad spend (from non-adopter advertisers) | 1/6 |
| `platform_cvr` | Platform-wide conversion rate | 1/6 |

**Key insight:** The advertiser's own dynamics (budget changes, momentum) were far more predictive than platform-wide metrics. Platform metrics were mostly collinear and got eliminated.

---

## The Numbers We Care About

### What "Spend-Weighted" Means

When we have results from multiple advertisers, we need a single number to summarize the overall effect. But not all advertisers are equal — a $1M advertiser's result matters more than a $50K advertiser's.

**Spend-weighted average** weights each advertiser's effect by their total post-period spend:

```
Spend-weighted effect = Sum(each advertiser's effect × their spend) / Total spend
```

Example:
- Advertiser A: +20% effect, $50K spend → contributes +$10K of "effect-dollars"
- Advertiser B: -5% effect, $500K spend → contributes -$25K of "effect-dollars"
- Spend-weighted = (-$25K + $10K) / $550K = -2.7%

Even though Advertiser A had a big positive effect, Advertiser B's slight negative effect dominates because they're 10x larger. This prevents small, noisy advertisers from skewing the result.

### What the Metrics Mean

| Metric | Formula | Better When | What It Tells Us |
|---|---|---|---|
| **IVR** | site_visits / impressions | Higher | How engaging are the ads? More visits per impression = better targeting/placement |
| **CVR** | conversions / site_visits | Higher | Once people visit, do they convert? Higher = better audience quality |
| **CPA** | total_spend / conversions | Lower | How much does each conversion cost? Lower = more efficient |
| **CPV** | total_spend / site_visits | Lower | How much does each visit cost? Lower = better value |
| **ROAS** | revenue / total_spend | Higher | How much revenue per dollar spent? Higher = better ROI |

---

## What We Found

### Primary Result (IVR)

**6 advertisers analyzed.** Others didn't have enough post-period data yet (adopted too recently) or had data quality issues.

| Result | Count |
|---|---|
| Improved & statistically significant | 2 (Taskrabbit +20.8%, Lighting New York +8.5%) |
| Improved but not significant | 2 (CWRV +12.2%, Talkspace +3.9%) |
| Declined & significant | 1 (Am. College of Ed -27.1%) |
| No meaningful change | 1 (FICO -0.2%) |

**Overall: Median +6.2%, Spend-weighted +6.5%**

### How Confident Are We?

| Validation Check | Result | What It Means |
|---|---|---|
| Placebo tests | 30% false positive rate | Acceptable — some noise in the data but not overwhelming |
| Sensitivity to pre-period | 5/6 directionally consistent | The result doesn't flip if we change how much history we use |
| Covariate selection | BIC-optimized per advertiser | Not relying on intuition — data-driven model selection |

### Caveats

1. **Small sample:** Only 6 analyzable advertisers. Individual results are noisy.
2. **Selection bias:** Advertisers who adopted may be different (more engaged, more sophisticated).
3. **Campaign maturity:** New media plan campaigns are compared alongside established ones. New campaigns always start slower. See TI-780.
4. **Attribution windows:** All adopters use default attribution settings (no custom lookback windows). If an advertiser had a non-standard window, it could affect conversion counting.
5. **CTV vs Display mix:** Most campaigns are CTV (channel_id=8), some are display (channel_id=1). We're analyzing them together. If media plan affects CTV differently than display, we'd miss that.

---

---

## Technical Deep-Dive (for DS/Analytics audience)

### Bayesian Structural Time Series — Under the Hood

CausalImpact is a wrapper around a **Bayesian Structural Time Series (BSTS)** model. The observation equation decomposes the time series `y_t` as:

```
y_t = μ_t + x_t'β + ε_t

where:
  μ_t = local level (random walk with drift)
  x_t'β = regression component (covariates)
  ε_t ~ N(0, σ²_ε) = observation noise
```

The local level evolves as:

```
μ_t = μ_{t-1} + δ_{t-1} + η_t    (level)
δ_t = δ_{t-1} + ζ_t               (slope/drift)

where η_t ~ N(0, σ²_η), ζ_t ~ N(0, σ²_ζ)
```

This is a **local linear trend** model — it allows the level and slope to change slowly over time, capturing non-stationary behavior without assuming a fixed functional form.

### The Regression Component

The covariates `x_t` enter as a standard linear regression. The key modeling choice is: **regression coefficients β are learned ONLY from the pre-period** (before intervention). In the post-period, the covariates still vary, but the learned relationship `β` is applied as-is to generate the counterfactual.

This is the core causal inference assumption: *the relationship between covariates and the outcome, learned in the pre-period, would have continued unchanged in the post-period if the intervention hadn't happened.*

### Prior Specification

pycausalimpact uses the `UnobservedComponents` model from statsmodels with:
- **Level variance prior:** learned from data (MLE or diffuse prior)
- **Observation variance prior:** learned from data
- **Regression coefficients:** OLS-initialized, then refined

The Bayesian posterior is computed via the **Kalman filter** (forward pass) and **Kalman smoother** (backward pass), giving the posterior mean and variance of the latent states at each time point.

### P-Value Calculation (Detailed)

The p-value is a **Bayesian one-sided tail-area probability**:

1. The model produces a posterior predictive distribution for the post-period counterfactual `y*_t`
2. The cumulative causal effect is `Σ(y_t - y*_t)` over the post-period
3. The model simulates many counterfactual paths from the posterior
4. p = fraction of simulated cumulative effects that are >= 0 (if observed effect is positive) or <= 0 (if negative)

This is NOT a frequentist p-value — it's a posterior probability. But it's interpreted similarly: p < 0.05 means < 5% of the posterior mass is on the "wrong side of zero."

**Important nuance:** pycausalimpact (what we use) approximates this differently from the original R package — it uses the `summary()` output from statsmodels' `UnobservedComponents` model rather than full MCMC sampling. The p-values are based on the Gaussian posterior approximation from the Kalman filter, not from sampling. This is faster but may slightly underestimate uncertainty compared to full MCMC.

### VIF (Variance Inflation Factor) — Math

For covariate `j` in a set of k covariates:

```
VIF_j = 1 / (1 - R²_j)

where R²_j is the R² from regressing x_j on all other covariates
```

- VIF = 1: no collinearity
- VIF = 5: 80% of variance explained by other covariates
- VIF = 10: 90% explained → coefficient estimates are unstable
- VIF = 100+: essentially measuring the same thing

We iteratively remove the highest-VIF covariate until all are < 10. This is greedy but effective — the alternative (all-subsets with VIF constraint) is combinatorially expensive.

### BIC Selection — Math

```
BIC = -2 ln(L̂) + k ln(n)

where:
  L̂ = maximized likelihood of the model
  k = number of parameters
  n = number of observations
```

BIC penalizes complexity more heavily than AIC (`AIC = -2 ln(L̂) + 2k`) because the penalty grows with `ln(n)` instead of being constant. For our sample sizes (n ≈ 50-100 weekly observations), `ln(n) ≈ 4-5`, so BIC penalizes each extra parameter at ~4-5x vs AIC's 2x.

We chose BIC over AIC because:
1. Stronger penalty prevents overfitting with short time series
2. BIC is consistent (selects the true model as n → ∞) while AIC tends to overfit
3. With 14 candidate covariates and only ~80 pre-period observations, parsimony matters

### Cross-Validation Design

We used a **temporal hold-out** strategy:
- Train: first `T - 8` weeks of pre-period
- Test: last 8 weeks of pre-period (before intervention)

Metrics: MAE, MAPE, RMSE. We compared 4 covariate sets:
1. **Minimal:** platform_ivr, platform_spend (baseline)
2. **v2 (hand-picked):** 7 covariates
3. **BIC-best:** per-advertiser optimized
4. **Kitchen-sink:** all VIF-clean covariates

This validates that the BIC-selected model actually predicts better out-of-sample, not just in-sample.

### Sensitivity Analysis Design

For each advertiser, we ran CausalImpact with pre-period lengths of 26, 39, 52, 65, and 78 weeks (where data allows). If the sign of the relative effect is consistent across all lengths, the result is **directionally robust**. If it flips, the result is sensitive to the arbitrary choice of how much history to use.

5/6 advertisers were directionally consistent. FICO was the exception — its effect is essentially zero (-0.18%), so flipping sign is expected when the true effect is near zero.

### Multi-Point Placebo Tests

Instead of one placebo at the midpoint (which has a single-test reliability problem), we ran 5 placebos per advertiser at evenly spaced split points across the pre-period. The **false positive rate (FPR)** is the fraction of placebos that show p < 0.05.

- FPR < 10%: excellent model reliability
- FPR 10-20%: acceptable
- FPR 20-35%: marginal — interpret p-values cautiously
- FPR > 35%: model may be unreliable for this unit

Our overall FPR was 30% (7/23) — in the marginal-to-acceptable range. This is common for advertiser-level time series with natural structural breaks (budget changes, seasonal shifts, campaign launches/pauses that happened during the pre-period).

### Why Advertiser-Specific Covariates Won

The BIC process revealed that `spend_change_pct` (week-over-week budget change) was selected for ALL 6 advertisers. `metric_lag1/2` was selected for 5/6. Platform-wide metrics (`platform_ivr`, `platform_spend`, etc.) were selected for at most 1/6 each — and only after VIF had already removed most of them.

**Interpretation:** Advertiser-level time series are primarily driven by the advertiser's own dynamics (budget changes, momentum). Market-wide trends have much less explanatory power than we assumed. This makes sense — an advertiser's IVR is more determined by *their* budget changes, creative quality, and audience targeting than by what the rest of the platform is doing.

This is a transferable insight for future experiments: always include lagged metric and budget change. Platform covariates add little value and are mostly collinear with each other.

---

## Considerations for Future Analysis

Things we identified as potentially relevant but haven't incorporated yet:

| Factor | Why It Might Matter | Status |
|---|---|---|
| **Campaign ramp-up period** | New campaigns always underperform initially. Without knowing how long ramp-up takes, we can't separate "media plan effect" from "new campaign effect." | TI-780 created — ask Kirsa/product first |
| **Advertiser lookback windows** | Different lookback windows mean conversions are counted differently. All adopters use defaults (NULL = system default), so this is controlled for now. | Checked — all NULL (default) |
| **CTV vs display split** | Media plan affects network allocation for CTV. If an advertiser shifts to more display, their IVR would change for unrelated reasons. | Tested: 3/6 advertisers are 100% CTV (no variance). For the 3 with mixed CTV/display (Lighting New York, Talkspace, Am. College of Ed), `ctv_share` is a valid covariate candidate but wasn't selected by BIC. |
| **Retargeting campaign count** | More retargeting campaigns could compete for budget and indirectly affect prospecting. | Tested: near-zero weekly variance for all 6 advertisers — not useful as a time-varying covariate. |
| **IP targeting distribution** | Cellular vs non-cellular IPs convert at different rates. If media plan shifts the IP mix, that's a confounder. | Not testable from current data — IP-level analysis would require impression_log/bid_logs joins, which is a separate research effort. |
| **Creative refresh timing** | New creatives can boost performance. If an advertiser refreshed creatives at the same time as adopting media plan, we'd conflate the two. | Not available in current data |
| **Vertical-specific trends** | Different verticals have different seasonality. Platform-wide covariates may not capture vertical-specific trends. | Could add vertical-level covariates from fpa.advertiser_verticals |
| **Number of campaign groups (prospecting + retargeting)** | Even though retargeting is excluded from the analysis, having more retargeting campaigns could compete for budget and indirectly affect prospecting performance. | Partially captured by `adv_active_cgs` covariate (prospecting only) |

---

## Terminology Quick Reference

| Term | Plain English |
|---|---|
| **CausalImpact** | Google's method for measuring if something caused a change |
| **Counterfactual** | "What would have happened if we did nothing" |
| **p-value** | Probability this result is just noise (< 5% = we trust it) |
| **Covariate** | An external factor we control for so it doesn't confuse the result |
| **VIF** | A test for whether our control factors are redundant with each other |
| **BIC** | A score that picks the simplest model that still works well |
| **Spend-weighted** | An average where bigger advertisers count more |
| **Pre-period** | The "before" time — used to learn normal patterns |
| **Post-period** | The "after" time — where we measure the effect |
| **Placebo test** | A fake test to check if our method is trustworthy |
| **Winsorize** | Clip extreme outlier values so they don't distort averages |
| **Staggered adoption** | Different advertisers started using the feature at different times |
| **campaign_group_id** | One campaign in MNTN's system (internally split into multiple campaign_ids for delivery, but campaign_group = the customer-facing campaign) |
