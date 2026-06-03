# CausalImpact + DiD — Math Reference

How the experiment-evaluation stack works under the hood. Read this when you want to understand what each number on a CI / DiD tile actually represents.

Companion doc to `knowledge/experimentation.md` § "Standard Analysis Protocol" (the *when* / *how* / *non-negotiables*). This doc is the *math behind* it.

**Canonical implementation:** [`tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py`](../../tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py) — `run_ci_for_tier()`, `did_inference()`, `drop_high_vif()`, `best_subset_by_bic()`.

---

## Contents

1. [CausalImpact (statsmodels UCM)](#1-causalimpact-statsmodels-ucm)
   - 1.1 [The state-space model](#11-the-state-space-model)
   - 1.2 [The Kalman filter](#12-the-kalman-filter--how-mu_t-gets-estimated)
   - 1.3 [MLE — what gets optimized](#13-mle--what-actually-gets-optimized)
   - 1.4 [Forecasting the counterfactual](#14-forecasting-the-post-period-counterfactual)
   - 1.5 [Aggregating to an average effect](#15-aggregating-to-an-average-effect)
   - 1.6 [Effect, CrI, and p-value via simulation](#16-effect-cri-and-p-value--via-simulation)
   - 1.7 [Deprecation note: prior hand-rolled formula](#17-deprecation-note-the-prior-hand-rolled-formula-was-wrong)
2. [DiD with cluster bootstrap](#2-did-with-cluster-bootstrap)
   - 2.1 [The DiD estimator (ratio form)](#21-the-did-estimator-ratio-form)
   - 2.2 [Cluster bootstrap algorithm](#22-cluster-bootstrap-algorithm)
   - 2.3 [SE / CI / p-value from the bootstrap](#23-se--ci--p-value-from-the-bootstrap)
3. [Covariate selection: VIF → BIC](#3-covariate-selection-vif--bic)
   - 3.1 [VIF](#31-vif-variance-inflation-factor)
   - 3.2 [BIC](#32-bic-bayesian-information-criterion)
4. [Worked example — Fangorn Tier 2](#4-worked-example--fangorn-tier-2)
5. [References](#5-references)

---

## 1. CausalImpact (statsmodels UCM)

We use `statsmodels.tsa.statespace.structural.UnobservedComponents` rather than Google's `causalimpact` package — equivalent semantics, easier to install on Databricks clusters.

### 1.1 The state-space model

We model the treated tier's daily visit rate `y_t` with two equations:

**Observation equation:**

$$y_t = \mu_t + \beta^\top X_t + \varepsilon_t, \qquad \varepsilon_t \sim \mathcal{N}(0, \sigma_\varepsilon^2)$$

**State equation (local level — random walk):**

$$\mu_t = \mu_{t-1} + \eta_t, \qquad \eta_t \sim \mathcal{N}(0, \sigma_\eta^2)$$

**Variables:**

| Symbol | Meaning |
|---|---|
| `y_t` | Observed treated-tier visit rate on day `t` |
| `μ_t` | Latent (unobserved) level — captures slow drift |
| `X_t` | Vector of selected exogenous covariates on day `t` (length `k`) |
| `β` | Coefficient vector on the exogenous regressors (length `k`) |
| `ε_t` | Observation noise |
| `η_t` | State innovation (how much the level drifts day-to-day) |
| `σ²_ε` | Observation noise variance |
| `σ²_η` | State innovation variance |

**Three parameters get estimated via MLE on the pre-period only:** `β`, `σ²_ε`, `σ²_η`.

### 1.2 The Kalman filter — how `μ_t` gets estimated

The filter runs recursively through pre-period days, maintaining a running estimate of the latent level and its uncertainty.

For each pre-period day, two steps:

**Predict** (project state forward):

$$\hat{\mu}_{t|t-1} = \hat{\mu}_{t-1|t-1}, \qquad P_{t|t-1} = P_{t-1|t-1} + \sigma_\eta^2$$

**Update** (when `y_t` is observed):

$$
\begin{aligned}
\nu_t &= y_t - \hat{\mu}_{t|t-1} - \beta^\top X_t & &\text{(innovation)} \\
S_t &= P_{t|t-1} + \sigma_\varepsilon^2 & &\text{(innovation variance)} \\
K_t &= P_{t|t-1} / S_t & &\text{(Kalman gain)} \\
\hat{\mu}_{t|t} &= \hat{\mu}_{t|t-1} + K_t \nu_t & &\text{(updated mean)} \\
P_{t|t} &= (1 - K_t) P_{t|t-1} & &\text{(updated variance)}
\end{aligned}
$$

By the end of pre-period day `T_pre`, we have `μ̂_{T_pre|T_pre}` (the model's best estimate of the latent level on the eve of treatment) and `P_{T_pre|T_pre}` (its uncertainty).

### 1.3 MLE — what actually gets optimized

Log-likelihood via the prediction-error decomposition:

$$\ell(\beta, \sigma_\varepsilon^2, \sigma_\eta^2) = -\frac{1}{2}\sum_{t=1}^{T_{pre}} \left[\log(2\pi S_t) + \frac{\nu_t^2}{S_t}\right]$$

`statsmodels` maximizes this numerically (BFGS by default). No closed-form solution.

### 1.4 Forecasting the post-period counterfactual

For each post-period day `t > T_pre`:

**Mean counterfactual:**

$$\hat{y}_t = \hat{\mu}_{T_{pre}|T_{pre}} + \beta^\top X_t$$

**Counterfactual variance:**

$$\text{Var}(\hat{\mu}_t) = P_{T_{pre}|T_{pre}} + (t - T_{pre}) \cdot \sigma_\eta^2$$
$$\text{Var}(\hat{y}_t) = \text{Var}(\hat{\mu}_t) + \sigma_\varepsilon^2$$

The variance grows linearly with horizon — that's why day-21's PI is wider than day-1's.

**95% prediction interval:**

$$\left[ \hat{y}_t - 1.96 \sqrt{\text{Var}(\hat{y}_t)}, \quad \hat{y}_t + 1.96 \sqrt{\text{Var}(\hat{y}_t)} \right]$$

### 1.5 Aggregating to an average effect

Let `n_post = T_end - T_pre`. Average across the post-period:

$$\overline{y_{\text{actual}}} = \frac{1}{n_{post}}\sum_{t > T_{pre}} y_t$$

$$\overline{y_{\text{pred}}} = \frac{1}{n_{post}}\sum_{t > T_{pre}} \hat{y}_t$$

$$\overline{y_{\text{lower}}} = \frac{1}{n_{post}}\sum_{t > T_{pre}} \text{lower}_t$$

$$\overline{y_{\text{upper}}} = \frac{1}{n_{post}}\sum_{t > T_{pre}} \text{upper}_t$$

### 1.6 Effect, CrI, and p-value — via simulation

**Updated 2026-06-03 after TI-961 methodology review** (see deprecation note below).

The post-period actual is **observed and fixed**:

$$\text{abs\_effect} = \overline{y_{\text{actual}}} - \overline{y_{\text{pred}}}, \qquad \text{rel\_effect} = \frac{\overline{y_{\text{actual}}}}{\overline{y_{\text{pred}}}} - 1$$

All the uncertainty is in the counterfactual. To quantify it, draw `N = 2000` sample paths from the fitted UCM's forecast distribution:

```python
sim = res.simulate(nsimulations=n_post, anchor="end",
                   repetitions=N, exog=X_post)
sim_paths = np.asarray(sim).reshape(n_post, N)
```

Each path `s ∈ {1, ..., N}` is a sequence of `n_post` counterfactual daily values drawn from the model's forecast distribution with the correct cross-day covariance (initial-state + state-shock + observation uncertainty). Average each path to get the distribution of the post-period mean counterfactual:

$$\text{avg\_cf}^{(s)} = \frac{1}{n_{\text{post}}} \sum_{t=1}^{n_{\text{post}}} y_{\text{cf}, t}^{(s)}$$

Then the effect distribution is just `actual − avg_cf^(s)` (actual is fixed):

$$\text{abs\_dist}^{(s)} = \overline{y_{\text{actual}}} - \text{avg\_cf}^{(s)}, \qquad \text{rel\_dist}^{(s)} = \frac{\overline{y_{\text{actual}}}}{\text{avg\_cf}^{(s)}} - 1$$

**95% CrI** = percentiles of the effect distribution:

$$\text{abs\_ci} = [\text{P}_{2.5}(\text{abs\_dist}), \text{P}_{97.5}(\text{abs\_dist})]$$

**Two-sided p-value** = tail probability the simulated counterfactual lands on the wrong side of the observed actual:

$$p = 2 \cdot \min\!\left( \frac{1}{N}\sum_s \mathbb{1}[\text{avg\_cf}^{(s)} \geq \overline{y_{\text{actual}}}], \; \frac{1}{N}\sum_s \mathbb{1}[\text{avg\_cf}^{(s)} \leq \overline{y_{\text{actual}}}] \right)$$

**Per-day envelope for the chart band** = per-day percentiles across paths:

$$\text{lower}_t = \text{P}_{2.5}^{(s)}(y_{\text{cf}, t}^{(s)}), \qquad \text{upper}_t = \text{P}_{97.5}^{(s)}(y_{\text{cf}, t}^{(s)})$$

This is the posterior-predictive analogue of what Brodersen et al.'s Bayesian CausalImpact reports — frequentist UCM equivalent, conditioning on the MLE parameters.

### 1.7 Deprecation note: the prior hand-rolled formula was wrong

Pre-2026-06-03 this doc described the inference like so:

$$\text{SE}_{\text{old}} = \frac{\overline{y_{\text{upper}}} - \overline{y_{\text{lower}}}}{2 \times 1.96}, \quad \text{rel\_ci\_lower} = \frac{\overline{y_{\text{actual}}}}{\overline{y_{\text{upper}}}} - 1, \quad p = 2(1 - \Phi(|\text{abs\_effect}| / \text{SE}_{\text{old}}))$$

Three compounding errors made this wrong:

(a) **`SE_old` is the wrong magnitude.** It's the average of per-day forecast SDs, not the SD of the post-period MEAN. It drops the `1/n` scaling AND ignores the strong positive cross-day covariance of a local-level forecast (the unobserved level drifts slowly, so adjacent day forecast errors track each other).

(b) **`actual / bound − 1` has no distributional basis** and explodes as the bound nears zero. This was the literal source of TI-961's +681% upper bound on Tier 1 IVR — the lower forecast bound was tiny/negative, and dividing a fixed actual by a near-zero denominator blew up. It looked like "wide model uncertainty" but it was a math bug.

(c) **The Gaussian z-test** layered on top of a skewed ratio compounds the magnitude error in (a) with the explosion in (b).

The simulation approach in §1.6 fixes all three: real cross-day covariance, no ratio explosion, no normality assumption.

---

## 2. DiD with cluster bootstrap

### 2.1 The DiD estimator (ratio form)

Define four quantities, each a pooled rate computed by summing numerators and denominators across advertisers:

$$t_{\text{pre}} = \frac{\sum_i \text{num}_{\text{pre},i}^{\text{treated}}}{\sum_i \text{den}_{\text{pre},i}^{\text{treated}}}, \quad t_{\text{post}} = \frac{\sum_i \text{num}_{\text{post},i}^{\text{treated}}}{\sum_i \text{den}_{\text{post},i}^{\text{treated}}}$$

$$c_{\text{pre}} = \frac{\sum_i \text{num}_{\text{pre},i}^{\text{control}}}{\sum_i \text{den}_{\text{pre},i}^{\text{control}}}, \quad c_{\text{post}} = \frac{\sum_i \text{num}_{\text{post},i}^{\text{control}}}{\sum_i \text{den}_{\text{post},i}^{\text{control}}}$$

The DiD lift:

$$\text{did\_lift} = \frac{t_{\text{post}} / t_{\text{pre}}}{c_{\text{post}} / c_{\text{pre}}} - 1$$

**Why ratio form?** Both `treated` and `control` get to inflate or deflate together. We're testing whether the *ratio* of post-to-pre changes is different between groups. The `-1` makes it readable as a percentage.

**Additive variant** (also computed in the notebook):

$$\text{did\_additive} = (t_{\text{post}} - t_{\text{pre}}) - (c_{\text{post}} - c_{\text{pre}})$$

### 2.2 Cluster bootstrap algorithm

**Resampling unit = advertiser.** Daily observations within an advertiser are autocorrelated. Resampling days would underestimate variance; resampling whole advertisers preserves within-advertiser correlation structure.

```
For b = 1 to B (B = 1000):
    1. Sample N_t advertisers with replacement from treated tier
    2. Sample N_c advertisers with replacement from control tier
    3. Recompute did_lift_b using the resampled pre/post sums
```

Output: empirical distribution `{did_lift_1, ..., did_lift_B}`.

### 2.3 SE / CI / p-value from the bootstrap

**Standard Error:**

$$\widehat{\text{SE}} = \sqrt{\frac{1}{B-1}\sum_{b=1}^{B}\left(\text{did\_lift}_b - \overline{\text{did\_lift}}\right)^2}$$

In code: `numpy.std(boot)`.

**95% Confidence Interval (percentile method):**

$$\text{ci\_lower} = Q_{0.025}\left(\{\text{did\_lift}_b\}\right), \qquad \text{ci\_upper} = Q_{0.975}\left(\{\text{did\_lift}_b\}\right)$$

In code: `np.percentile(boot, [2.5, 97.5])`.

**Two-sided p-value:**

$$p = 2 \cdot \min\left[\frac{1}{B}\sum_{b=1}^{B}\mathbb{1}(\text{did\_lift}_b \geq 0), \quad \frac{1}{B}\sum_{b=1}^{B}\mathbb{1}(\text{did\_lift}_b \leq 0)\right]$$

**Intuition:** under the null hypothesis of no effect, half the bootstrap samples should be ≥ 0 and half ≤ 0. If 95% of bootstrap samples are ≥ 0, then `P(boot ≥ 0) = 0.95`, `P(boot ≤ 0) = 0.05`, and `p = 2 × 0.05 = 0.10`. The smaller the tail mass crossing zero, the smaller the p-value.

---

## 3. Covariate selection: VIF → BIC

We give the model **4 exogenous-only candidates per (tier, metric)** and let the data choose which survive. **Updated 2026-06-03 after TI-961 review** — see the "Excluded by design" subsection below.

**Candidates at the tier × day grain:**

| Name (IVR / CVR) | Captures |
|---|---|
| `control_vr` / `control_cvr` | Denominator-weighted rate across control tiers (analogue of treated `y`) |
| `control_vr_lag1` / `control_cvr_lag1` | Control rate at t−1 (control-side momentum) |
| `control_imps` / `control_visits` | Control-tier scale covariate (scaled by 1e9 or 1e6 for numerical stability) |
| `holiday` | Binary US-holiday flag |

**Weekly seasonality is NOT a candidate.** It's handled by the state-space model's `freq_seasonal=[{"period": 7, "harmonics": 2}]` component. This is more principled than an `is_weekend` dummy (full weekly cycle, evolves over time, no extra exog state to estimate jointly with the others).

**Excluded by design — and why:**

| Excluded | Why |
|---|---|
| `metric_lag1`, `metric_lag7` (lags of treated `y`) | **Target leakage.** In a counterfactual forecast, conditioning the model on `y[t-1]` and `y[t-7]` from the post-period inserts post-treatment values into the "what-would-have-happened" forecast. This biases the counterfactual toward the actual and shrinks the estimated effect toward zero. (Brodersen et al.'s CausalImpact explicitly forbids lags of `y` as exog for this reason.) Temporal correlation in `y` is handled by the local-level state, not by feeding lags back in. |
| `is_weekend` | Now redundant — `freq_seasonal` captures the same effect with no exog state cost. |

### 3.1 VIF (Variance Inflation Factor)

For each candidate `x_j`, regress it on all the others:

$$x_j = \gamma_0 + \sum_{k \neq j} \gamma_k x_k + u_j$$

Compute `R²_j` from that regression. Then:

$$\text{VIF}_j = \frac{1}{1 - R_j^2}$$

**Interpretation:**

- `VIF = 1` → fully orthogonal to the other covariates
- `VIF > 10` → high collinearity (the cutoff we use)
- `VIF = ∞` → perfect collinearity (the variable is a linear combination of others)

**Iterative drop algorithm:**

```
while max(VIF over candidates) >= 10:
    drop argmax(VIF)
return surviving candidates
```

### 3.2 BIC (Bayesian Information Criterion)

For an OLS fit with `n` observations and `k` regressors (excluding intercept):

$$\text{BIC} = n \cdot \log\!\left(\frac{\text{RSS}}{n}\right) + k \cdot \log(n)$$

where `RSS = Σ(y_i - ŷ_i)²`.

**Penalty structure:** `BIC` = fit term + complexity penalty. The complexity penalty `k · log(n)` is harsher than AIC's `2k`, so BIC tends to pick smaller models.

**Best-subset search:**

```
best_bic = ∞
best_subset = []
For k = 1 to min(5, n_surviving):
    For each subset S of size k:
        Fit OLS y_pre ~ S on the pre-period
        Compute BIC
        If BIC < best_bic: update best_bic, best_subset
Return best_subset
```

The winning subset becomes the `exog` matrix for the UCM in step 1.

---

## 4. Worked example — Fangorn Tier 2

> ⚠️ **Deprecated numbers (2026-05-28 run, pre-methodology-fix).** These
> values came from the implementation that had the target-leakage candidates
> (`metric_lag1`, `metric_lag7`) and the hand-rolled SE formula now described
> as "wrong" in §1.7. The point estimate is probably biased toward zero
> (leakage) and the CrI is the wrong magnitude (SE bug). Kept here only as
> a worked example of the *deprecated* formulas — useful to see how the old
> arithmetic chained together. The Fangorn numbers will change after the
> corrected notebook is re-run; this section will be refreshed then.

From the live Databricks run (2026-05-28, deprecated implementation):

| Symbol | Tier 2 value | Source |
|---|---:|---|
| `n_pre` | 65 days | March 2 → May 5 |
| `n_post` | 21 days | May 7 → May 27 |
| `avg_actual` | 0.0204 (2.04%) | mean of post-period `y_t` |
| `avg_predicted` | 0.0161 (1.61%) | mean of `forecast.predicted_mean` |
| `avg_lower` | ≈ 0.0088 (0.88%) | mean of 95% PI lower |
| `avg_upper` | ≈ 0.0241 (2.41%) | mean of 95% PI upper |
| `rel_effect` | `0.0204 / 0.0161 − 1 = +26.6%` | eq. 1.6 (deprecated) |
| `rel_ci_lower` | `0.0204 / 0.0241 − 1 = −15.4%` | eq. 1.7 (deprecated formula) |
| `rel_ci_upper` | `0.0204 / 0.0088 − 1 = +131.8%` | eq. 1.7 (deprecated formula) |
| `abs_effect` | `0.0204 − 0.0161 = 0.0043 pp` | eq. 1.6 |
| `SE_old` | `(0.0241 − 0.0088) / 3.92 = 0.0039` | eq. 1.7 (deprecated formula) |
| `z` | `0.0043 / 0.0039 = 1.13` | eq. 1.7 (deprecated formula) |
| `p` | `2 × (1 − Φ(1.13)) = 0.258` | reported as 0.255 (deprecated formula) |

And the DiD bootstrap for the same tier:

| Symbol | Value |
|---|---:|
| `t_pre` | 0.0184 |
| `t_post` | 0.0239 |
| `c_pre` | 0.0174 |
| `c_post` | 0.0177 |
| Treated lift | `0.0239/0.0184 − 1 = +30.0%` |
| Control lift | `0.0177/0.0174 − 1 = +2.2%` |
| `did_lift` | `(1.300 / 1.022) − 1 = +27.2%` |
| Bootstrap SE | ≈ 0.27 |
| Bootstrap 95% CI | `[−9.6%, +92.9%]` |
| Bootstrap p | 0.200 |

**The convergence story:** CI's `+26.6%` and DiD's `+27.2%` are within 0.6 pp of each other. Two different statistical machines on the same data, same answer. That's what "verified causal at the convergence level" means.

---

## 5. References

- **Canonical implementation:** [`tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py`](../../tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py)
- **When to apply this stack:** `knowledge/experimentation.md` § "Standard Analysis Protocol"
- **Auto-trigger for future sessions:** workspace `.claude/CLAUDE.md` § "Experiment Analysis Protocol"
- **Library docs:** [statsmodels UnobservedComponents](https://www.statsmodels.org/stable/generated/statsmodels.tsa.statespace.structural.UnobservedComponents.html), [variance_inflation_factor](https://www.statsmodels.org/stable/generated/statsmodels.stats.outliers_influence.variance_inflation_factor.html)
- **External reading:**
  - Brodersen et al. (2015), *Inferring causal impact using Bayesian structural time-series models* — the original Google CausalImpact paper
  - Durbin & Koopman (2012), *Time Series Analysis by State Space Methods* — the textbook for UCM / Kalman filtering
  - Cameron, Gelbach & Miller (2008), *Bootstrap-based improvements for inference with clustered errors* — cluster bootstrap justification
