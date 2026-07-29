---
name: reference_stable_hi_not_stable_roas
description: "Stable HI-share != stable ROAS. Monthly prospecting ROAS swings widely even on a gated healthy advertiser. AOV flat -> swings are conversions-per-DOLLAR: driven by a diminishing-returns spend envelope (ROAS inversely tracks spend) + small-volume/seasonal/attribution-lag noise, NOT composition."
metadata:
  node_type: memory
  type: reference
  originSessionId: 06681997-5cb8-4f58-ba1a-517bc7ce83ae
doc_type: memory
keywords: [stable hi not stable roas, roas swings, high-intent share, AUDI-1070, diminishing returns spend envelope, AOV flat, conversions per dollar, Avon, HexClad, prospecting roas]
domain: [audience-scoring, incrementality, business]
lifecycle: active
last_verified: 2026-07-01
---
**When a client asks "why does my ROAS swing wildly if we stayed in High-Intent?" (AUDI-1070):** HI-share stability does NOT pin ROAS. Avon stayed gated-HI all window yet its MoM ROAS swung **4.3×–16.8×** — expected and HEALTHY. Decompose `ROAS = conversions × AOV / spend`:

- **AOV is the tell — FLAT ($47–56 all 17 months).** So the swing is **conversions-per-DOLLAR**, not basket size, not audience quality.
- **Driver 1 — diminishing-returns envelope: ROAS moves INVERSELY with spend level.** Avon's two highest-ROAS months (Apr'25 16.06×, Jul'25 16.81×) are its two lowest-spend months (~$7.3k); its highest-spend month (Nov'25 $25,585) sits at 5.92×. Low spend → serve only the cream of HI (high efficiency); spend hard → re-serve / reach deeper into the finite HI pool, marginal ROAS falls. **Same finite-HI-pool saturation mechanic as Caraway** — Avon just runs at the efficient low-spend end (that discipline = why it's healthy).
- **Driver 2 — small-volume + seasonal + view-through-lag noise.** ~740–3,500 conversions/mo; at identical $7.3k spend Apr/Jul booked ~2,300 conversions but Aug/Sep only ~750–950 (summer lull + attribution timing) → scatter around the envelope.

**The line to say out loud:** HI-share tells you WHO you reached (composition); ROAS is revenue-per-DOLLAR (marginal efficiency = how hard you spent into the pool + timing). Frozen composition ≠ frozen ROAS. **Monthly wobble ≠ trend:** aggregate Jan–May prospecting ROAS 7.93× (2025) → 8.59× (2026), +8% on −18% spend = healthy. Contrast HexClad, whose chart doesn't wobble — it steps DOWN after the gate came off + spend blew out to $903k.

Chart `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts/avon_spend_roas_envelope.png`; data `outputs/avon_mom_lt_decomposition.csv`; knowledge `data_knowledge.md` §5k. Related: reference_client_chart_spend_match_id, [[reference_ddp_valuation_framework]], [[reference_hhst_pacing_lever]].
