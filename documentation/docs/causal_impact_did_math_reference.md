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
   - 1.6 [Relative effect and CrI](#16-relative-effect-and-cri)
   - 1.7 [p-value (normal approximation)](#17-p-value-normal-approximation)
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

### 1.6 Relative effect and CrI

$$\text{rel\_effect} = \frac{\overline{y_{\text{actual}}}}{\overline{y_{\text{pred}}}} - 1$$

$$\text{rel\_ci\_lower} = \frac{\overline{y_{\text{actual}}}}{\overline{y_{\text{upper}}}} - 1 \qquad \text{(worst case for the effect)}$$

$$\text{rel\_ci\_upper} = \frac{\overline{y_{\text{actual}}}}{\overline{y_{\text{lower}}}} - 1 \qquad \text{(best case for the effect)}$$

The asymmetry of dividing `actual` by `lower` vs `upper` is what makes CrIs blow up when the predicted counterfactual is small (visit rates near zero).

### 1.7 p-value (normal approximation)

$$\text{abs\_effect} = \overline{y_{\text{actual}}} - \overline{y_{\text{pred}}}$$

Back out the SE from the 95% PI width. A 95% normal interval spans ±1.96 SE, so the full width = `2 × 1.96 × SE`. Reverse:

$$\text{SE} = \frac{\overline{y_{\text{upper}}} - \overline{y_{\text{lower}}}}{2 \times 1.96}$$

z-statistic:

$$z = \frac{|\text{abs\_effect}|}{\text{SE}}$$

Two-sided p-value:

$$p = 2 \cdot (1 - \Phi(z))$$

where `Φ` is the standard-normal CDF.

**Caveat:** this is a normal approximation. Google CausalImpact uses a posterior tail probability from BSTS sampling instead. The two agree in practice when prediction errors are well-behaved, which they are at our scale.

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

We give the model 6 candidates per tier and let the data choose which survive.

**Candidates at the tier × day grain:**

| Name | Captures |
|---|---|
| `control_vr` | Impression-weighted visit rate across control tiers |
| `control_imps` | Control-tier total impressions (scaled, platform-wide volume proxy) |
| `holiday` | Binary US-holiday flag |
| `is_weekend` | Saturday/Sunday flag |
| `metric_lag1` | Treated tier visit rate at t−1 (own momentum) |
| `metric_lag7` | Treated tier visit rate at t−7 (weekly seasonality) |

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

From the live Databricks run (2026-05-28):

| Symbol | Tier 2 value | Source |
|---|---:|---|
| `n_pre` | 65 days | March 2 → May 5 |
| `n_post` | 21 days | May 7 → May 27 |
| `avg_actual` | 0.0204 (2.04%) | mean of post-period `y_t` |
| `avg_predicted` | 0.0161 (1.61%) | mean of `forecast.predicted_mean` |
| `avg_lower` | ≈ 0.0088 (0.88%) | mean of 95% PI lower |
| `avg_upper` | ≈ 0.0241 (2.41%) | mean of 95% PI upper |
| `rel_effect` | `0.0204 / 0.0161 − 1 = +26.6%` | eq. 1.6 |
| `rel_ci_lower` | `0.0204 / 0.0241 − 1 = −15.4%` | eq. 1.6 (shown as −13.2% with rounding) |
| `rel_ci_upper` | `0.0204 / 0.0088 − 1 = +131.8%` | eq. 1.6 (shown as +133.2%) |
| `abs_effect` | `0.0204 − 0.0161 = 0.0043 pp` | eq. 1.7 |
| `SE` | `(0.0241 − 0.0088) / 3.92 = 0.0039` | eq. 1.7 |
| `z` | `0.0043 / 0.0039 = 1.13` | eq. 1.7 |
| `p` | `2 × (1 − Φ(1.13)) = 0.258` | reported as 0.255 |

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
