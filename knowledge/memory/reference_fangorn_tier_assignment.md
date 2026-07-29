---
name: fangorn-tier-assignment
description: Authoritative map of Fangorn rollout tiers in tpa.fangorn_advertiser_inclusion — only Tier 2 is random; Tier 5 is the permanent holdout; Tier 99 is auto-enrolled (Express / auto-verticals) and should be excluded from rollout causal analysis
metadata: 
  node_type: memory
  type: reference
  originSessionId: e28957cb-95c7-4eeb-a32b-fd1c38ef16fb
doc_type: memory
keywords: [fangorn tier assignment, tpa_fangorn_advertiser_inclusion, fangorn_rollout_tier_num, tier 2 random, tier 5 holdout, tier 99 express, control_tiers, cluster bootstrap DiD, AUDI-1083]
domain: [experimentation, audience-scoring]
lifecycle: active
last_verified: 2026-07-22
---
**Table path (verified 2026-07-22, AUDI-1083):** `dw-main-bronze.integrationprod.tpa_fangorn_advertiser_inclusion`
(NOT `tpa.fangorn_advertiser_inclusion`). Columns: `advertiser_id`, `vertical_id`, `is_express`,
**`fangorn_rollout_tier_num`** (the tier — NOT `tier`), `fangorn_advertiser_inclusion_date`,
`created_at`, `updated_at`.

Canonical state of `integrationprod.tpa_fangorn_advertiser_inclusion` (last verified 2026-06-03):

| Tier | N | Inclusion date | What it is |
|---|---|---|---|
| **1** | 3 | 2026-04-30 | First wave — non-random, N=3 (anecdotal only, no inference) |
| **2** | **49** | 2026-05-06 | **Random 50-advertiser sample — gold-standard causal cohort** |
| **3** | 312 | 2026-05-19 | Wave 3 — non-random |
| **4** | 362 | 2026-06-04 | Wave 4 — non-random, flips 2026-06-04 |
| **5** | **353** | **2099-01-01 (sentinel)** | **Permanent holdout — never-flipped. Use as CONTROL.** |
| **99** | 44 | 2026-06-04 10:36:46 | **Auto-enrollment (Express product / auto-verticals)** — already on Fangorn via a different mechanism, NOT part of the structured rollout. **EXCLUDE from causal analysis.** |

**Confirmed by user 2026-06-03.** Earlier rollout (pre-2026-06-03 my-understanding) had Tier 4 as the active control by future-flip logic — that's no longer correct because the inclusion date semantics differ across tiers.

## How to set `control_tiers` in the RolloutTierEvaluations notebook

- **`control_tiers=5`** — recommended default. Tier 5 is the permanent holdout, methodologically the cleanest control.
- **Do NOT use `auto`** — auto-detection picks any tier with future/null inclusion date, which currently sweeps in Tier 4 (about-to-flip) and Tier 99 (auto-enrolled, already treated). Mixed control populations dilute the comparison.
- **Exclude Tier 99 from analysis entirely** — those advertisers are already on Fangorn via auto-enrollment, not the structured rollout. Use the `exclude_tiers` widget (added 2026-06-03) defaulting to `99`. They'd otherwise either be misclassified as control (auto logic) or pollute treated bootstraps if forced.

## Implications for analysis

- **Tier 2 vs Tier 5 = gold-standard causal claim.** Random sample treated, permanent holdout control. Cluster-bootstrap DiD is the headline. Methods convergence with CausalImpact on this pair is the strongest evidence we can produce.
- **Tier 1, 3, 4 vs Tier 5** = supporting non-random reads. Treat as CausalImpact-primary, DiD-with-caveat. Frame as "consistent across non-random cohorts" in any deck.
- **Tier 99 = separate analysis if anyone cares.** The auto-enrolled cohort behavior could be analyzed independently (Express product impact, vertical-specific dynamics) but doesn't belong in the rollout-causal frame.

## Tier 99 — Express / auto-verticals

Per user 2026-06-03: Tier 99 is auto-enrollment of the Express product and/or "auto-verticals" — half of the verticals were automatically rolled onto Fangorn outside the structured tier plan. 44 advertisers. The inclusion-date field stores a record-creation timestamp (2026-06-04 10:36:46), not a flip date — these advertisers are already on Fangorn at the time of the record.

The fact that the inclusion-date semantics differ between structured-rollout tiers (1-5) and auto-enrolled tiers (99) means **inclusion-date-based logic alone can't distinguish "treated" from "control" reliably**. Explicit tier filtering is required.

## See also

- [[feedback_no_y_lags_in_causalimpact]] — methodology
- [[feedback_bootstrap_must_match_design]] — stratified bootstrap for future designs
- [[reference_causal_impact_pattern]] — canonical analysis stack
- `documentation/docs/feature_rollout_experimental_design.md` — how to design the next major rollout
