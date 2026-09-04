---
name: reference_mde_arm_split
description: In an MNTN ghost-bid MDE calculation the spend-derived IP pool IS the treated arm — the holdout is additional and never served; splitting the pool inflates MDE by 1/sqrt(1-h) and that error is PESSIMISTIC
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [MDE arm split, nTreated nControl, ghost bid holdout unserved, 1/sqrt(1-h), 1.0541x, 11.8% at h=0.20, spend_required, spendRequired 1.1111x, ti_884_mde_calculator, computeMde, impressionsPerIp, totalIps, AUDI-1323 Nick Scialli, AUDI-1213 closed, round-trip check]
domain: [incrementality, experimentation]
lifecycle: active
last_verified: 2026-09-03
---
**Convention: in a ghost-bid MDE calculation the spend-derived IP pool is the TREATED arm. The holdout is additional and is never served.** Ghost-bid holdout IPs cost nothing, so budget buys treated impressions only. `ti_884_mde_calculator.py` (`tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts/`) encodes this in `spend_required` as `impressions = n_total * (1 - h) * imps_per_ip` and is the source of truth.

**The defect this keeps producing.** An implementation derives the pool from spend, then splits it:

```
totalIps  = (monthlyBudget * durationMonths / cpm) * 1000 / impressionsPerIp
nTreated  = totalIps * (1 - h)      # WRONG
nControl  = totalIps * h            # WRONG
```

Correct form: `nTreated = the spend-derived pool; nControl = nTreated * h/(1-h)`.

**Size and direction.** The split form makes the forecast MDE `1/sqrt(1-h)` too large: **1.0541x at h=0.10**, 11.8% too large at h=0.20. Reproduced numerically on a 1M-IP pool at p=0.02, h=0.10: 6.5370% (split form) vs 6.2016% (`ti_884`), ratio 1.054093. **DIRECTION CORRECTION (2026-09-03): the error is PESSIMISTIC, not optimistic — it overstates how hard a test is, and fixing it makes tests look EASIER to power.** Any workspace text implying the 1.0541x error is over-optimistic is wrong. The spend-side mirror of the same mistake (charging the holdout for impressions) runs required spend **1.1111x** high at h=0.10.

**It has now bitten three implementations:** the standalone gist calculator (`ti_xxx_mde_calculator_prefill.html`, fixed 2026-09-03), its `spendRequired` mirror (fixed 2026-09-03), and the in-product `computeMde.ts` shared by `gary-ql src/gql/types/IncrementalityExperiment/resolvers.ts` and `premier-ui src/app/scenes/Testing/ExperimentBuilder/useMdeForecast.ts` (still open, **AUDI-1323**, Nick Scialli's as of 2026-09-03, writeup `tickets/audi_1213_mde_calculator_refresh/artifacts/audi_1213_mde_arm_split_writeup.md`). The two fixed implementations shipped inside **AUDI-1213, closed Done 2026-09-03**; the same fixed code is what the Mode report runs ([[reference_mde_surface_choice]]).

**How to apply — the round-trip check is the one that catches a half-fix.** MDE and required-spend are a pair, so fixing one side leaves the other inconsistent. Verify both: `mdeRel` against `ti_884_mde_calculator.py` (agreed to <1e-11 across h=0.10/0.20 and p=0.0058/0.107/0.1183 on the 2026-09-03 build) AND `spendRequired(computeMDE(budget)) == budget` (<1e-9). Related: [[reference_mde_surface_choice]], [[reference_test_budget_from_rates]], [[feedback_ghost_holdout_not_frequency_capped]].
