---
name: reference_causal_impact_dashboard
description: "Standardized config-driven Causal Impact dashboard (Mode) for BETA-rollout measurement — architecture, config table, 3 views"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 8a2ab59e-21b9-4781-a3a4-ffadf51e287a
doc_type: memory
keywords: [causal impact dashboard, mode dashboard, beta rollout, causal_experiment, bsts, did, 3-point, experiments config table, ci_pre_days, statsmodels ucm]
domain: [experimentation, project]
lifecycle: active
last_verified: 2026-07-23
---
Standardized Causal Impact dashboard: measures "what did a BETA rollout cause?" for wave-flip features (no in-experiment holdout). Productionizes step 5 of the [[reference_causal_impact_pattern]] Standard Analysis Protocol. Full statistics walkthrough: `documentation/docs/HOW-IT-WORKS.embedded.md`; captured in `knowledge/experimentation.md` § "The standardized Causal Impact dashboard".

- **Onboard a new experiment = seed one config row, no code change.** Unit = advertiser; scope recipe resolves in-scope `campaign_group_id`s at seed time, summed to advertiser×day.
- **Architecture:** config tables `dw-main-silver.experiments.*` → 7 SQL panels (UI-faithful attribution) → ~23-cell notebook (statsmodels/numpy/pandas, NO `causalimpact` lib) → self-contained HTML on Mode. 3 views: BSTS (teal), DiD (violet), 3-Point (amber = naive vs BSTS vs DiD agreement).
- **Config table `dw-main-silver.experiments.causal_experiment`** (1 row/experiment, keyed by `Experiment Key`) holds all knobs: `ci_pre_days`=60, `did_pre_days`=14, `bsts_harmonics`=2, `maturity_days`=14, `min_window_impressions`=1000 (treated+control), `pre_active_window_days`=25 (decoupled from ci_pre_days 2026-07-09), `measurement_end_date`=NULL (date freezes experiment).
- Metrics: IVR, CPV, CVR, CPA, ROAS — all fit independently, both methods. Single eligibility gate applied identically to BSTS+DiD (`ELIGIBLE_AIDS`).
- Staggered cohorts: BSTS aligns on flip-clock, pools within-day by volume then equal-weight days; DiD pools counts not rates (`cf_post = treated_pre_rate × market_drift × treated_post_volume`). Clean-holdout control default.

Consistent with [[feedback_no_naive_pre_post]], [[feedback_no_y_lags_in_causalimpact]], [[reference_causal_impact_pattern]].
