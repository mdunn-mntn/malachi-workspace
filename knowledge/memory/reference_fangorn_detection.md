---
name: reference_fangorn_detection
description: "Empirical Fangorn-vs-bucketed detector (continuous 8001-9999 score band); advertisers migrate on a rolling schedule, verify per-advertiser date from CIL"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 06681997-5cb8-4f58-ba1a-517bc7ce83ae
doc_type: memory
keywords: [fangorn_detection, fangorn, detection, empirical, bucketed, detector, continuous, 8001]
domain: [reference]
lifecycle: active
last_verified: 2026-07-08
---
Tell whether an advertiser is on Fangorn (continuous XGBoost scores) vs bucketed (discrete) empirically from `logdata.cost_impression_log.household_score` (COALESCE with regex-parse of model_params for pre-2026):

- **Bucketed:** HI = exactly 10000, PP = exactly 8000, Mid = 3333–6665; the **8001–9999** and **6666–7999** bands are EMPTY (platform-wide 7d check 2026-07-08: literally ZERO v1 imps in either band).
- **Fangorn:** High spread **8001–9999**, Peak **6666–7999** — bands densely populated (1,868 / 1,206 distinct values platform-wide).
- **Test:** `COUNTIF(hs BETWEEN 8001 AND 9999)/COUNT(*)` + `COUNT(DISTINCT ...)`. ~0% + 0 distinct = bucketed; non-trivial + many distinct = Fangorn.
- **TWO CAVEATS (2026-07-08, TI-1037):** (1) the **8001–9999 band requires the keyword layer (DS19)** — a "vertical only" DS46 advertiser delivers ONLY the 6666–7999 continuum (tops out at 8000), so test BOTH bands or you'll miss keyword-less Fangorn advertisers. (2) **v2 pins at exactly 8000/10000 exist** (band-top pins, ~2M imps each platform-wide 7d) — a mass at exact 8000/10000 does NOT prove bucketed; band CONTINUITY is the discriminator.

**Rolling migration:** the platform Fangorn date (~May 1 2026) is NOT when a given advertiser flips — they migrate on a rolling schedule. **HexClad flipped Jun 4–5, 2026** (0% continuous through Jun 3 → 22.9% Jun 4 partial → fully migrated Jun 5, exactly-10000 delivery drops to 0%). Always verify the per-advertiser migration date from CIL before crediting/blaming Fangorn. Canonical: AUDI-1070. Related [[reference_hhst_pacing_lever]], [[reference_fangorn_two_model_passes]], [[reference_fangorn_audience_overlay]]. **Bouqs (32147) confirmed still bucketed through May 2026** — `hs = 10000` and `hs >= 8001` give identical distinct-IP counts on its CIL (TI-1037).
