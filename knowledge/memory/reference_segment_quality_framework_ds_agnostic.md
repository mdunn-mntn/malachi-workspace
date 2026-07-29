---
name: reference-segment-quality-framework-ds-agnostic
description: "Alex Knorr's segment-quality scoring framework (targeting-infra-ml/utils/segment_quality_utils) is data-source agnostic — works on any data_source_id, not just DS 35 LiveRamp. Confirmed 2026-06-05. Future use: score DS 19 keywords."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [segment quality, alex knorr, ThirdPartySegmentQuality, targeting-infra-ml, ds19 keywords, ds35 liveramp, ipdsc, buk, ti-956, data source agnostic]
domain: [audience-scoring, repos]
lifecycle: active
last_verified: 2026-06-05
---
**Per Alex Knorr (Slack, 2026-06-05):** the `ThirdPartySegmentQuality` framework
in `SteelHouse/targeting-infra-ml#57` (`utils/segment_quality_utils/`) is NOT
exclusive to DS 35 (LiveRamp). It's written to work with any data source.
Untested across DSes but the design is intentionally agnostic.

> "It could be! I wrote it in such a way that it would be easy to interact with
> and it isn't exclusive to DS 35, I haven't really tested it but should work
> with any data source. Would be interesting to score our DS 19 keywords in the
> same way."

**Architectural implication:** the 9-axis composite (activity, stability, share,
uniqueness, sample, staleness, specificity, targetability, performance) is
designed against the abstract shape `(ip, event_date, data_source_category_ids)`
— so anything in the IPDSC schema or anything that can be projected into that
shape works.

**Concrete future use cases:**

1. **DS 19 keyword quality scoring** — Alex's own suggestion. Buyer keywords
   ranked by per-keyword quality feeds BUK (Behavior Keywords) curation. TI team
   has been working on BUK rollout (TI-887, TI-832 etc.); per-keyword quality is
   a natural extension.
2. **DS 17 (ShareThis), DS 18 (Dstillery) 3P providers** — could rank their
   segments alongside LiveRamp for TI-956's UI surface.
3. **Per-advertiser segment-quality layer (v2 of TI-956)** — same framework with
   advertiser_id added as a partitioning axis.

**Operationally for TI-956 (today):**
- v1 stays DS 35 / LiveRamp only.
- The model class hardcodes `LR_DS_ID = 35` — would need to be parameterized
  (constructor arg) before reusing for other DSes.
- Once packaged as a wheel, multiple model files (`ti_956_..._ds_35.py`,
  `ti_xxx_..._ds_19.py`) could share the same scoring library.

**Why this matters for the wheel-packaging conversation:** Alex's "it could be"
confirms the cross-repo dependency is solvable. Packaging targeting-infra-ml
unlocks ALL these future use cases, not just TI-956. Strong argument for doing
it properly vs vendoring into airflow-ti's utils_model.

**See also:** [[reference-ti-956-per-pattern-application]] (the per-pattern UI
logic TI-999 produced for TI-956); [[project-buk-rebrand]] (BUK = Behavior
Keywords, the DS 19 context); `tickets/ti_956_interest_segment_scoring_schedule/`
for the current TI-956 implementation.
