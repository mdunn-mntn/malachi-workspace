---
key: AUDI-1173
title: "Adaptive frequency-cap bandit: Phase-0 sizing done, RCT design next"
status: backlog
framing_state: locked
question: "Would an adaptive (bandit-set) household frequency cap beat MNTN's static per-campaign/group caps on incremental visits per dollar, and is the pool large enough to justify building it?"
created: 2026-07-28
jira: https://mntn.atlassian.net/browse/AUDI-1173
---

# AUDI-1173 — Adaptive frequency-cap bandit

## 0. Framing
- **Question:** Would an adaptive (bandit-set) household frequency cap beat MNTN's current static per-campaign / campaign_group caps on incremental visits per dollar, and is the high-frequency pool large enough to matter?
- **Goal / decision:** Decide whether to invest in a lift-aware frequency-cap bandit as MNTN's first practical MAB (vs the HHST intent-gate bandit). Ties to spend efficiency / ROAS. Only worth building if the RCT shows capping recovers value.
- **Objective (done-when):** A household-randomized cap RCT designed (arms, holdout, power, readout) and a go/no-go sizing agreed; bidder-team (rtb-campaign-service) actuation owner identified.
- **Approach:** (1) Observational Phase-0 sizing in BQ (reach-frequency curve + cross-group leakage) to clear the "worth an experiment" bar. (2) Household-randomized cap RCT (cap arms vs suppression holdout; reward = incremental visits per household). (3) If validated, a discounted-Thompson bandit sets the default cap per campaign_group.
- **What would change the answer:** RCT shows capping loses visits proportional to spend (no real diminishing returns) → not worth building. Phase-0 showing negligible high-frequency spend → no experiment needed (it did not; the pool is real but bounded).

## 1. Introduction
Scoping the next practical Multi-Arm Bandit at MNTN. ~24 candidates ranked; the two finalists are the HHST intent-gate bandit and the frequency-cap bandit. **Frequency wins as the FIRST bandit** because its causal reward is a runnable household-randomized RCT, whereas graduated HHST lift is blocked in BQ (needs GCS+Databricks). Background: [[reference_frequency_capping]], [[reference_hhst_efficiency_sizing]].

## 2. Problem
The HHST intent gate is set by a pure-pacing bot (`ddm.hhst_generate_recommendation`); frequency caps are static per campaign / campaign_group, IP-keyed, with **no advertiser rollup** and **fail open** on Redis error. Neither optimizes incremental value.

## 3. Phase-0 findings (verified by 2 adversarial passes; correlational)
- **Prospecting** (has_mm, 7d $3.93M): ~12% of spend at freq ≥8 (~$2M/30d); per-household visit rate plateaus ~1.1% across freq 4-20.
- **Retargeting** (funnel≥2, 7d $1.39M): ~31% of spend at freq ≥8 — biggest pool, but warm-user visits are least incremental.
- **Cross-group leakage:** 4.8% of households / 13% of impressions served by 2+ campaign_groups (the no-rollup control-plane defect) — actionable WITHOUT an RCT; strongest standalone result.
- **Load-bearing caveat:** attributed visits-per-impression decline is partly a mechanical last-touch artifact (~1/n by construction), NOT clean diminishing returns. The curve cannot justify a cap alone; the causal answer needs the RCT. RCT metric = total visits per household + cost-per-household.

## 4. Deliverables
- **Full scope doc (HHST + frequency + retargeting + RCT spec + data appendix):** `artifacts/audi_1173_scope.md`.
- **Queries:** `queries/audi_1173_*.sql` (frequency curves, leakage, HHST context). **Outputs:** `outputs/*.json`. Perf-log ticket label: `freq_cap_sizing`.

## 5. Next
- RCT spec (arms, `MOD(ABS(FARM_FINGERPRINT(advertiser_id:ip)),1000)` household randomization, power ~636K/arm, advertiser-clustered bootstrap) is in scope doc §6.
- Identify the bidder-team (`rtb-campaign-service`) actuation owner as RCT co-owner.
- Follow-ups: combined platform sizing with shared-IP purge + ≥30-45d visit tail; HHST bandit as Phase 2 (reuses the randomized-holdout lift infra).
