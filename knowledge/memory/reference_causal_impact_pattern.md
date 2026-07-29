---
name: Canonical MNTN CausalImpact + DiD-bootstrap pattern (TI-748 / TI-542 / TI-803 / TI-504 / TI-849 / TI-961)
description: When building any tiered-rollout / experiment lift analysis, follow this validated pipeline; canonical implementation is the TI-961 RolloutTierEvaluations notebook
type: reference
originSessionId: 950de123-5da5-4a85-a3bb-813893617d2f
doc_type: memory
keywords: [causal impact pattern, cluster bootstrap, did, causalimpact, vif bic, tiered rollout, RolloutTierEvaluations, ti-961, fangorn inclusion, visit rate kpi]
domain: [experimentation]
lifecycle: active
last_verified: 2026-05-28
---
The MNTN standard analysis stack for any experiment / tiered rollout has two parallel methods sitting on the same daily panel:

**1. DiD with cluster bootstrap (advertiser-clustered)**
- Per (tier, KPI): aggregate per-advertiser pre/post sums of numerator+denominator
- Resample advertisers with replacement N=1000, recompute pooled `(t_post/t_pre)/(c_post/c_pre)−1` each time
- Report point + 95% CI (2.5/97.5 percentiles) + two-sided p-value (`2 × min(P(boot≥0), P(boot≤0))`)
- Canonical: `_did_bootstrap()` + `did_inference()` in [tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py](tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py)

**2. CausalImpact (statsmodels UCM + VIF→BIC covariate selection)**
- Tier-level series, NOT advertiser-level (TI-849 was per-AID; TI-961 generalized to tier)
- Candidate covariates at tier × day grain: `control_vr`, `control_imps`, `holiday`, `is_weekend`, `metric_lag1`, `metric_lag7`
- VIF iteratively drops covariates with VIF ≥ 10
- BIC best-subset OLS up to size 5; winning subset becomes `exog` for UCM
- statsmodels `UnobservedComponents(level="local level", exog=…)` fit by MLE on pre-period only
- Forecast post-period via `get_forecast()`; SE backed out from PI width; two-sided z-test p-value
- Canonical: `run_ci_for_tier()` + `drop_high_vif()` + `best_subset_by_bic()` in the same file

**Standard input panel:** advertiser × day rows with `(impressions, vv, conversions, order_value, spend)`. Pull via BQ from `silver.summarydata.{impression,visit,conversion,spend}_facts`. Lean-delta pattern for re-pulls: only the incremental pre period not already in `daily_performance_df`; only `imp` + `vv` if you only need IVR — saves ~10× on bytes.

**Standard cohort source:** Postgres `tpa.*_inclusion` table for tiered rollouts (e.g. `tpa.fangorn_advertiser_inclusion` for Fangorn). Auto-detect treated tiers as those with `inclusion_date <= window_end`; control = future-flip tiers (NULL or future inclusion_date). Never substitute "never-flipped" advertisers when proper future-tier controls exist.

**Defaults:**
- `lookback_days` (DiD pre window) = 14
- `ci_pre_days` (CI pre window) = 60 — driven by Kalman filter convergence + BIC stability + ≥2-3× post-period ratio
- `n_boot` = 1000 for DiD bootstrap

**Diagnostic — when DiD ≠ CI:** control drifted in pre→post (DiD over-corrects, CI under-corrects); pre-period covariates don't track treated tightly (CI's local-level state absorbs unexplained variance, DiD doesn't); small N (both noisy, but bootstrap surfaces it); BIC flipping between local minima (run `ci_pre_days = 30/60/90` as robustness check).

**Convergence is the strongest informal-causal argument.** When DiD and CI point estimates agree, that IS the evidence — report it. When they disagree, the gap is diagnostic; investigate before claiming either.

**Visit rate is the headline KPI.** Conversion-based metrics (CVR/CPA/ROAS) are noisy until n_post ≥ 28 days.

**Reference implementations:**
- [tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py](tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py) — current canonical (tier-level CI + DiD bootstrap, both VIF→BIC and inference)
- [tickets/ti_849_fangorn_score_monitoring/artifacts/ti_849_method3_causal_impact.py](tickets/ti_849_fangorn_score_monitoring/artifacts/ti_849_method3_causal_impact.py) — per-advertiser CI variant (TI-849, still useful when per-AID effects are needed)
- [tickets/ti_748_causal_impact_media_plan/artifacts/ti_748_covariate_validation.py](tickets/ti_748_causal_impact_media_plan/artifacts/ti_748_covariate_validation.py) — original advertiser-level covariate-validation pipeline
- Methodology codified in [[experimentation-md-standard-protocol]] (`knowledge/experimentation.md` § "⭐ Standard Analysis Protocol")
- Trigger wired in [[workspace-claude-md-experiment-trigger]] (workspace `.claude/CLAUDE.md`)

Pip: `pip install statsmodels scipy google-cloud-bigquery pandas numpy matplotlib`. No `causalimpact` package — we use statsmodels UCM directly (cluster pip-install for causalimpact often fails; UCM is equivalent for our needs).
