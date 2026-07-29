---
name: Don't propose naive pre/post for advertiser-level analyses
description: Always pair pre/post comparisons with a counterfactual (CausalImpact preferred) since spend changes alone produce false "lift"
type: feedback
originSessionId: 950de123-5da5-4a85-a3bb-813893617d2f
doc_type: memory
keywords: [no_naive_pre_post, naive pre/post, counterfactual, causalimpact, vif bic, synthetic control, advertiser kpi lift, spend confound, augmentor_log, ti-849 fangorn]
domain: [experimentation, incrementality, workflow]
lifecycle: active
last_verified: 2026-05-01
---
When measuring the effect of a feature/rollout/intervention on advertiser-level KPIs (IVR, CVR, ROAS, etc.), do NOT lead with naive pre/post comparisons.

**Why:** User direction 2026-05-01 (during TI-849 Fangorn monitoring): "we don't want to do pre-post without some sort of counterfactuals such as using causal impact, since it's not taking in account spend or anything which ruins all pre/post experiment results." Spend changes after a rollout will move VVs/conversions regardless of whether the feature itself is doing anything — naive pre/post conflates the two and leadership has been burned by inflated "lift" claims this way before.

**How to apply:**
- Default headline methodology = CausalImpact synthetic control with covariate validation (VIF → BIC → CausalImpact). Pattern is canonical in TI-748 / TI-542 / TI-803 / TI-504 / TI-849.
- Covariates should include: platform-aggregate KPIs from non-treated advertisers, holiday/calendar, lagged metric (lag1, lag2), spend or spend_change_pct.
- Pre/post KPIs are still useful as **descriptive volume context**, just don't frame them as a lift claim.
- For non-randomized rollouts (most cases), CausalImpact is the strongest defensible read. Within-AID DiD via TI-835 holdout hash + augmentor_log is theoretically cleaner but **not feasible at daily cadence** — augmentor_log scan is TB-scale (TI-849 D0 confirmed cancelled at 7/11 stages, 1.5 hours wall).
- If post-period is short (<14 days), credible intervals will be wide. Frame honestly: "directionally up, ask again at D+14 / D+30."
