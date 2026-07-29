---
name: project_intent_tier_pacing
description: "Malachi's proposal (from AUDI-1070): ration HI/PP IPs across a flight via pacing so high-spend advertisers don't exhaust the high-intent pool early and crash. Active idea, AUDI-owned."
metadata: 
  node_type: memory
  type: project
  originSessionId: 06681997-5cb8-4f58-ba1a-517bc7ce83ae
doc_type: memory
keywords: [intent_tier_pacing, intent, tier, pacing, malachi, proposal, audi, 1070]
domain: [project]
lifecycle: active
last_verified: 2026-07-01
---
**Intent-tier pacing** — Malachi's forward proposal coming out of AUDI-1070 (the Caraway/Avon/HexClad YoY decline = spend-driven prospecting saturation). The diagnosis: high-spend advertisers exhaust their high-intent (HI/PP) pool, then delivery spills into unscored/low-VR inventory → performance declines over the flight.

**The fix:** combine a per-advertiser pacing forecast with targeting — set the HI:MI/unscored ratio at campaign start so HI IPs are consumed at a *constant rate* across the whole flight instead of front-loaded. Ex: 30-day flight, $2k/day, if HI exhausts in ~15 days → target 50% HI / 50% MI-unscored throughout so performance stays consistent rather than crashing. Only valid for high-spend advertisers (daily spend > sustainable HI burn).

**Honest framing (so it's not oversold):** mostly *redistributes/smooths* performance (same finite HI pool served either way) → a **revenue-retention / consistency** play, NOT a net-ROAS lift. One real efficiency gain: avoiding early over-frequency on HI IPs. Caveat: HI = demand-harvesting (low incrementality) → stabilizes *reported* metrics, not incremental value.

**Dependencies:** (1) forecast days-to-HI-exhaustion = HI unique-reach × freq cap ÷ daily impressions; (2) a bidder **pacing-aware targeting control** to throttle HI delivery — overlaps the **Permel** universal optimization controller roadmap. Related: [[project_incrementality_experiment]], and the AUDI-1070 saturation diagnostic in `knowledge/experimentation.md`.

**EMPIRICAL VALIDATION (AUDI-1070 HexClad pacing model, 2026-07):** the constraint is a FLOW limit, not the ~7M lifetime figure. **Live 30-day HI pool tops at ~3.8M IPs (~half of 7M; fewer households after CGNAT churn)**, set by new-HI inflow **~61K/day × 30-day TTL**. **Sustainable HI spend ≈ $150-160K/mo (~$5K/day)** at clean-gate reach/$ ~34. HexClad hit the ceiling in **Oct 2025** when spend spiked to $224K/mo (~40% over sustainable) → brand-new share of reach fell 100%→54%, reach/$ rolled over, cumulative crossed 7M Oct 26 = "running on refresh." BUT it's REFRESHABLE (2026 reach/$ recovered ABOVE baseline at +23% spend when the gate permitted) — so pacing HI across the flight is viable. Days-to-exhaust formula confirmed: pool ÷ daily-fresh-reach; fresh HI runs faster than the naive 7M/500K=14d because live pool is ~half. Stage-1 DOES re-serve HI (frequency, distinct from Stage-3 retargeting) → rising HI frequency is the leading tightening signal before tier-down.

**Open follow-ups:** size HI-pool headroom (incl. Fangorn vs current MM) for Caraway/HexClad to get real numbers; confirm whether any HI-rationing knob exists in the bidder today; draft as an AUDI ticket / one-pager. See [[reference_hhst_pacing_lever]].
