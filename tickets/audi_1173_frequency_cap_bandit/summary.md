---
doc_type: ticket
key: AUDI-1173
title: "Adaptive frequency-cap bandit: Phase-0 sizing done, RCT design next"
status: backlog
date: 2026-07-28
summary: "Adaptive household frequency-cap bandit — Phase-0 sizing done; RCT design next (3-arm, total-visit-count primary, ~10-12wk) + a small @SteelHouse/rtb bidder feature. MNTN's candidate first MAB."
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

## 3. Findings — refined 30d delivered sizing (`artifacts/audi_1173_refined_sizing.md`)
Refined the 7d Phase-0 curve into a 30d, all-stage, household-grain delivered curve with a shared-IP-purge dimension (`outputs/audi_1173_delivered_freq_curve.json`, 56 rows). Freq buckets = impressions/household over 30d (not per week — higher shares vs the 7d Phase-0 ~12% are a window effect).
- **High-freq pool (30d delivered, freq≥8):** combined **42.3% raw / 32.2% purged** of spend; prospecting **34.6% raw / 30.4% purged**; retargeting 78.9% raw / 57.3% purged (biggest but least incremental — warm users).
- **Gross-addressable over an 8/30d cap (before incrementality — NEVER a saving):** combined **$3.93M purged** ($9.19M raw); prospecting $3.40M purged. Over-cap-3 combined $8.10M purged; over-cap-12 $2.73M purged.
- **Shared-IP purge (report both; purged is honest):** combined purge drops **~20% of households, ~37% of imps/spend, ~73% of attributed visits.** Retargeting worst (−76% spend, −44% hh) — its raw high-freq pool is mostly a shared-IP artifact. Quote purged only.
- **Frequency-cap capability gap (confirmed):** `advertiser_frequency_caps` is empty (0 rows) — MNTN has no advertiser-level cap; counters are per-campaign / per-campaign_group, IP-keyed, no rollup, so frequency mechanically leaks across an advertiser's groups and stages. This is a control-plane CAPABILITY to build, not a sized bug. **Magnitude WITHDRAWN:** the prior $0.41M–$0.66M/7d over-delivery headline is retracted (rejected excess method on shared-IP-confounded, un-purged `(ip, advertiser)` data; the 7d purge removed only ~0.34% of households, a no-op). Value + the right cap = RCT outputs. See `artifacts/audi_1173_leakage_brief.md`.
- **Load-bearing caveat (headline):** the observational curve CANNOT establish diminishing returns. Attributed visits/1k-imp declines partly as a mechanical last-touch artifact (~1/n); attributed AND total (`guid_log`) visits/household RISE with frequency (heavily-served households visit more — a selection confound). Purged combined attributed CPV is roughly FLAT (~$2.6–2.7) across freq 1–40, so the 7d "7× CPV rise" largely evaporates once shared IPs are purged. Neither observational metric proves capping recovers value — only the household-randomized RCT can. The total-visit observational curve was dropped as both a 968 GB cost-trap AND selection-confounded. RCT metric = total visits per household + cost-per-household.

## 4. Deliverables
- **Sprint-ready implementation plan (pull-and-execute):** `artifacts/audi_1173_implementation_plan.md` — BLUF / Problem / Solution / ordered work-list / Impact / Expected-improvement / ready-to-sprint checklist.
- **RFD (buy-in decision doc):** `artifacts/audi_1173_rfd_draft.md`; also rendered as a claude.ai artifact (`a5cd4a66-2d0d-4159-b121-c81a5aa851e4`, private).
- **RCT design + pre-registration:** `artifacts/audi_1173_rct_design.md`, `audi_1173_rct_prereg.md` (DRAFT-PENDING-LOCK).
- **Ownership + holdout feasibility (bidder code paths):** `artifacts/audi_1173_ownership_feasibility_memo.md`. **Total-visit signal probe:** `audi_1173_total_visit_signal_probe.md`. **Leakage brief:** `audi_1173_leakage_brief.md`. **Bandit + offline-replay design:** `audi_1173_bandit_design.md`.
- **Full scope doc (HHST + frequency + retargeting + RCT spec + data appendix):** `artifacts/audi_1173_scope.md`.
- **Impact model:** frequency capping is MNTN-revenue-neutral under CPM + fixed budgets (redistributes impressions to fresh reach); value = advertiser efficiency / incremental ROAS → retention.
- **Queries:** `queries/audi_1173_*.sql` (frequency curves, leakage, HHST context). **Outputs:** `outputs/*.json`. Perf-log ticket label: `freq_cap_sizing`.

## 5. Next — sizing done; the causal question is RCT-only
- **Sizing clears the "worth an experiment" bar** (real, bounded, purge-surviving pool: ~42% combined / ~35% prospecting spend at freq≥8; ~$3.9M/30d combined purged gross-addressable over an 8-cap). **Go/no-go: GO to the RCT.** The observational curve cannot prove capping recovers any of it — RCT is the only instrument.
- **Run the household-randomized cap RCT** (design + prereg): **3 arms** — A control / B cap-8 / C cap-3 (arm H suppression = Phase-2); randomize on the TI-837-validated `MOD(ABS(CAST(CONCAT('0x',SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING),':',ip))),1,16)) AS INT64)),1000)` (arms 100-399/400-699/700-999, disjoint from the 0-99 holdout); **primary = mean total site visit-days/hh (a COUNT; guid_log deduped, attribution-independent), non-inferiority 5% relative via household bootstrap** + cost/hh superiority; N provisional (off the critical path); prospecting/retargeting separate strata. Needs a small `@SteelHouse/rtb` bidder feature first (arms not config-only).
- Identify the bidder-team (`rtb-campaign-service`) actuation owner as RCT co-owner.
- Phase 2: HHST intent-gate bandit reuses the randomized-holdout lift infra the RCT builds.
