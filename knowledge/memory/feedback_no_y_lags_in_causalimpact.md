---
name: no-y-lags-in-causalimpact
description: NEVER include lags of the treated outcome y as exog covariates in a CausalImpact / synthetic-control counterfactual setup — target leakage biases the estimated effect toward zero
metadata: 
  node_type: memory
  type: feedback
  originSessionId: e28957cb-95c7-4eeb-a32b-fd1c38ef16fb
doc_type: memory
keywords: [no_y_lags_in_causalimpact, lags, causalimpact, include, treated, outcome, exog, covariates]
domain: [workflow]
lifecycle: active
last_verified: 2026-06-03
---
In any CausalImpact / synthetic-control counterfactual setup using `statsmodels.UnobservedComponents` (or Brodersen-et-al-style BSTS): **NEVER include lags of the treated outcome `y` as exogenous covariates**. Lags from the post-period contain the treatment effect, so conditioning the forecast on them inserts post-treatment values into the "what-would-have-happened" counterfactual. This biases the estimated effect toward zero — symptoms include suspiciously small `rel_effect` magnitudes and counterfactual lines that hug actuals.

**Why:** Brodersen et al.'s CausalImpact spec explicitly forbids it. The UCM's local-level state handles temporal correlation in `y` natively; that's what it's for. Lags of `y` as exog are valid in standard time-series forecasting (no counterfactual framing) but invalid in CausalImpact.

**How to apply:**
- Building a candidate covariate pool for `run_ci_for_tier()` or similar: include only exogenous-only candidates (control series + their lags, holiday/calendar dummies, scaled volumes). Specifically: `control_rate`, `control_rate_lag1`, `control_scale`, `holiday` is the canonical 4-candidate set.
- Weekly seasonality: handle via `freq_seasonal=[{"period": 7, "harmonics": 2}]` on the UCM, NOT via `is_weekend` dummy in exog.
- Inference: use simulation via `res.simulate(...)` with N=2000 paths, NOT a hand-rolled SE from per-day forecast bounds (the per-day-avg SD drops `1/n` scaling and ignores cross-day covariance; ratio-of-bounds for relative CI explodes when counterfactual nears zero).
- See [[reference-causal-impact-pattern]] for the canonical implementation.

Discovered 2026-06-03 during Alex Knorr's review of TI-961 RolloutTierEvaluations. Three compounding bugs I had introduced (lags of y, is_weekend dummy, hand-rolled SE) all corrected. The TI-961 deck-quality numbers from 2026-05-28 through 2026-06-02 are deprecated; only post-correction runs are trustworthy.
