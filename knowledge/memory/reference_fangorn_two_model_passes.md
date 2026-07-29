---
name: reference-fangorn-two-model-passes
description: "Fangorn runs TWO model passes per IP (HI model + PP model). Two raw scores per IP, not one. The HI/PP bands map from each model's respective raw."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 96d997b2-5483-4746-a3f2-c30aeb590522
doc_type: memory
keywords: [fangorn_two_model_passes, fangorn, model, passes, runs, scores, bands, each]
domain: [reference]
lifecycle: active
last_verified: 2026-07-08
---
**Fangorn (DS46) runs twice per IP — once with the HI model, once with the PP model.** Each pass outputs a raw score in [0, 1], so every IP gets two Fangorn raw scores.

**Mapping rule:**
- raw_HI > 0.8 + IP in DS13 vertical ∩ DS19 keywords → **HI band 8000-10000** (remap raw_HI onto the band)
- raw_PP > 0.8 + IP in DS13 vertical, no keyword → **PP band 6666-8000** (remap raw_PP onto the band)
- either raw in 0.6-0.8 → **MI band 3333-6665** (regardless of DS membership)
- both raws < 0.6 or no Fangorn score → **Max Reach 1-3332** (random)

**Why this matters for analysis:**
- Don't describe Fangorn as "one raw score per IP" — that's wrong. Two raws per IP.
- HI score and PP score are not the same number; they come from different model passes.
- DS13/DS19 overlay decides which raw is actually used for band mapping per impression.
- HI and PP are not Fangorn-internal concepts — Fangorn just outputs raws, the HI/PP separation is post-processing.

**Source:** User correction during TI-897 (2026-06-03) — corrects an earlier "single raw per IP" framing I had used based on a partial reading of Ryan's 2026-06-01 notes in `knowledge/data_knowledge.md` § Intent Scoring Architecture.

**EMPIRICALLY CONFIRMED 2026-07-08 (7d delivered CIL, RTC-excluded, all live v1/v2 prospecting; TI-1037):**
- v1 (DS13) delivers fixed points ONLY: exactly 8000 + exactly 10000; ZERO imps at 6666–7999 / 8001–9999.
- v2 (DS46) delivers both bands as continua **with a pin at each band top**: PP band 6666–7999 (1,206 distinct values) + exactly-8000 pin; HI band 8001–9999 (1,868 values) + exactly-10000 pin.
- **The HI band requires the keyword layer**: 100% of >8000 delivery is on DS19-carrying campaigns; DS46-only ("vertical only") campaigns top out at 8000. Post-flip, a keyword-less advertiser's ceiling is the PP band (under v1 the same config delivered categorical 10000s).
- Methodology doc (transform 0.6/0.8 → 3333/6666, proposed Fangorn+BUK additive blend, HI-vs-PP split flagged as open): https://mntn.atlassian.net/wiki/spaces/TAR/pages/3414917161. Query: variant 3 in `ti_1037_mm_ds_cooccurrence.sql`.

**Canonical doc:** `knowledge/audience_products.md` (TI-897). See also [[reference_causal_impact_pattern]], [[reference_bidder_scoring_reality]], [[reference_mm_component_taxonomy]].
