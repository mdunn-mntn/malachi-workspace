---
name: reference_mm_wgu_suitability_argument
description: "Why MM/HHST is unsuitable for a very-high-spend advertiser (WGU 31357) — the argument is supply arithmetic ($5K/day sustainable HI burn vs WGU's $112K/day), NOT audience size; MM leaves BROADEN (OR-join), the shrink is the HHST gate; do not cite Caraway as a large-client saturation precedent"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [MM suitability, Mountain Match talk track, WGU 31357, HHST gate, high intent supply, sustainable HI burn, audience size misstatement, Paulo veto, Tofer frequency control, Caraway precedent, saturation, imps per IP]
domain: [bidding, audience-scoring, business]
lifecycle: active
last_verified: 2026-08-27
---
Built 2026-08-27 to answer a #targeting-squad thread: Paulo hard-vetoed enabling MM for **WGU (31357)** and asked for a talk track; Malachi argued from audience size; Tofer rebutted "we control the amount we reach the audience." Reusable for any very-high-spend advertiser, not just WGU.

## The talk track that survives: supply arithmetic, not audience size
Sustainable high-intent spend is a **flow** limit set by new-HI inflow, and the only case where it has been measured is HexClad: **~$5K/day (~$150–160K/mo)** at clean-gate reach/$ ~34, live 30d HI pool ~3.8M IPs on ~61K new-HI IPs/day ([[project_intent_tier_pacing]]). WGU spends **$3,352,209/30d ≈ $112K/day** — roughly 22x that. A gated WGU therefore either under-delivers or has its gate driven to zero by the pacing controller, which is **MM in name and unscored in delivery**. The MM delivery ceiling is independently documented: FICO at 4x budget bought no incremental scored impressions (`data_knowledge.md` §"MM = has DS19" region).

## Three corrections to the intuitive version of the argument
1. **"Turning MM on limits their audience size" is backwards on the one thing MM controls.** MM include leaves **OR-join — adding a leaf BROADENS membership** ([[reference_mm_component_taxonomy]]; `mntn_business.md` §Audience Expression Logic: non-geo segments OR by default, AND only via the narrowing toggle). What actually shrinks the biddable pool is the **HHST score gate**, a separate campaign-level pacing setting, on for **87.6%** of MM prospecting campaigns ([[reference_hhst_efficiency_sizing]]), cutting the same audiences from mean 51.3M / median 43.1M IPs at any score to **4.77M / 3.55M at HI — ~11x** ([[reference_vertical_hi_sizing_baseline]]). Say *gate*, not *audience size*, or the rebuttal lands cleanly.
2. **"A smaller pool means we hit the same IPs at an absurd rate" is the weak link, and it is the one Tofer's objection targets.** Caps are real and set on 10,428 of 10,430 live campaigns ([[reference_frequency_capping]]). The correct counter is **grain, not tightness**: the Redis counter has no advertiser or cross-group rollup, so WGU's campaign_groups each count the IP independently — which is why WGU already sits at **24.66 imps/IP** (98.4th pct of 1,859 advertisers, median 3.36) with every campaign capped. And where a cap *does* bind on a gated pool at fixed budget, the cost does not vanish, it moves: frequency-to-cap → gate relaxation ([[reference_hhst_pacing_lever]]) → under-delivery.
3. **"Better to target more unique IPs" is a deliverability claim, not a performance claim.** The HI efficiency edge is explicitly correlational with served-vs-holdout ITT lift ≈0% ([[reference_hhst_efficiency_sizing]]). Population-wide, incremental visit lift is **High +0.2% (~0), no_score +0.1% (~0)**, with mid-intent (PP/Mid/MaxReach) carrying the lift (`experimentation.md` §persuadables gradient) — and **WGU today is 100% no_score**. So gating WGU to HI moves them between two incrementally-dead bands at 11x less pool. Argue delivery, and if arguing performance, argue *mid-intent*, not HI.

## Do NOT cite Caraway as a large-client saturation precedent
The remembered "another large client with HI and a 10k score threshold hit saturation and performance fell" is **Caraway (40341)** from AUDI-1070, and it fails on three counts (`tickets/audi_1070_yoy_decline_caraway_avon_hexclad/summary.md`):
- **Not large.** ~7 campaigns, $35K–$97K/mo — roughly 1/40th of WGU.
- **Wrong mechanism.** The documented signature is spend-driven descent **down the score curve** (%-under-8000 rises with that advertiser's own spend and recedes when spend falls), not over-serving a fixed set of households.
- **Confounded.** The Q1-2026 ROAS cliff was largely an **advertiser-side conversion-pixel break ~2026-01-06** (raw `conversion_log` fires −68% at constant impressions); the ticket's own adversarial pass calls the saturation attribution "overstated."
Also note 4 of 5 AUDI-1070 declines were the gate, not over-scaling, and the one genuinely large advertiser in that set (HexClad, $4.72M) **recovered** when the gate was restored to 10000.

## Unmeasured — do not assert
Nobody has sized **WGU's own MM-eligible or gated pool** at any grain, and the workspace states there is no targetable-IP model. The days-to-exhaust formula exists (live pool ÷ daily fresh-HI inflow, [[project_intent_tier_pacing]]) but has not been run for WGU. **Settling analysis:** impressions per `(ip, campaign_id)` and per `(ip, campaign_group_id)` from `silver.logdata.cost_impression_log` for 31357 vs the configured `LEAST()` cap — the delta against the `(ip, advertiser)` rollup is exactly the frequency the caps do not govern. Purge shared IPs first (~37% of impressions, [[reference_frequency_capping]]).

Related: [[reference_mntn_1p_3p_mm_definitions]], [[reference_stable_hi_not_stable_roas]], [[reference_wgu_pixel_case]].
