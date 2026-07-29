---
name: cuped-needs-randomization
description: "CUPED variance reduction requires randomized assignment between treated and control. Bolting CUPED onto non-random cohorts like Tier 2 vs Tier 5 (Wave 3 selection-biased holdout) is mathematically incoherent — the unbiasedness condition E[pre_T − pre_C] = 0 is violated by design."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: e28957cb-95c7-4eeb-a32b-fd1c38ef16fb
doc_type: memory
keywords: [cuped, variance reduction, randomization, ti-961, wave3 selection bias, did, cluster bootstrap, parallel trends, theta]
domain: [experimentation]
lifecycle: active
last_verified: 2026-06-10
---
CUPED (Deng et al. 2013) reduces variance on the post-period estimate by adjusting for pre-period covariate values, with theta = Cov(post, pre) / Var(pre) estimated on the pooled sample. The variance-reduction guarantee Var(Y_adj) = Var(Y_post) × (1 − ρ²) holds **only when randomization makes E[pre_T − pre_C] = 0** in expectation.

For non-randomized cohorts with deliberate baseline imbalance (e.g., Tier 2 random sample vs Tier 5 Wave-3 manually-flagged holdout — see [[reference_wave3_selection_bias]]), the unbiasedness condition is violated. Three things go wrong:

1. **Standard CUPED estimator `τ̂_add = ȳ_adj,T − ȳ_adj,C` is biased** — the (pre_T − pre_C) imbalance leaks into the post-period comparison via θ.
2. **Trying to combine CUPED with DiD** (e.g., `τ̂_add = (ȳ_adj,T − ȳ_adj,C) − (ȳ_pre,T − ȳ_pre,C)`) **double-counts the pre-period correction**: the formula algebraically expands to `(ȳ_post,T − ȳ_post,C) − (1+θ)·(ȳ_pre,T − ȳ_pre,C)`, multiplying baseline imbalance by `(1+θ)` instead of either 1 or θ. This generates spurious effects even when treated and control have NO real time trend (verified synthetically — 33.8% spurious lift when pre=post for both groups).
3. **CIs end up wider, not narrower** — the bootstrap propagates the bias-amplified quantity, producing CrIs ~70% wider than raw additive DiD instead of the theoretical √(1−ρ²) tightening.

**How to apply:**

- **Never bolt CUPED onto a quasi-experiment with structural baseline imbalance.** If the design isn't randomized, CUPED-the-textbook doesn't apply.
- For non-random designs, the three theoretically clean options are: (a) raw additive DiD under parallel-trends, no CUPED; (b) CUPED on within-unit deltas Δ_i = post_i − pre_i with a DIFFERENT covariate (pre-pre-period rate, advertiser size, vertical); (c) apply CUPED only to a properly-randomized sub-comparison.
- **For future TI work**: CUPED is one of three components in the canonical variance-reduction stack (CUPED × ghost-ad × stratified per Lewis-Rao 2015 — see `documentation/docs/feature_rollout_experimental_design.md`). All three components require *design discipline upstream*. None of them retrofit cleanly onto a rollout that was already non-randomly assigned.
- **If you find yourself trying to add CUPED to a non-randomized DiD analysis**, stop. The right move is raw cluster-bootstrap DiD + CausalImpact (which is more robust to baseline imbalance) + transparent reporting of the selection-bias caveat. Do NOT add a third estimator just to claim "variance reduction" — it's not actually doing variance reduction in this regime.

Discovered 2026-06-10 during TI-961 CUPED implementation. The first attempt produced point estimates swinging 50-150pp from raw DiD (Tier 2 IVR: raw +11.3% → CUPED −60.2%). A multi-agent verification workflow caught both the formula bug AND the deeper design mismatch — concluded CUPED was incompatible with the Wave 3 holdout design. CUPED tile additions were reverted; methodology stayed at raw DiD + CausalImpact.

**Reference for the future-randomized case:** when the next major TI release uses stratified random assignment per the experimental design doc, CUPED CAN be applied cleanly. The 35-50% variance reduction is real — just only when the upstream design is random.
