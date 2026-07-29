---
name: project-ti-999-strategic-goal
description: TI-999 end-goal — propose interest-segment changes (curation / ranking) so buyers who pick 3P segments get better performance. Bucket KPI work supports this case.
metadata: 
  node_type: memory
  type: project
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [ti_999_strategic_goal, 3p segment curation, interest segment ranking, DS17 sharethis, DS18 dstillery, DS35 liveramp, 3p-only baseline, ti_956 scoring, segment quality]
domain: [project, audience-scoring]
lifecycle: active
last_verified: 2026-05-29
---
**End-goal of TI-999:** suggest interest-segment changes so that buyers who choose 3P interest segments get better performance.

**Why:** today MNTN has no quality filter on 3P segments (DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP) — buyers can pick anything. Bad-performing IPs from low-quality segments can land at the top of bid ranking, hurting delivery KPIs.

**Two proposed interventions:**
1. **Limit choices** — only show buyers the best-performing 3P segments (curation).
2. **Rank** — order segments best-to-worst so buyers are naturally pushed toward better choices.

**Why the bucket / KPI work matters:**
- Establish a baseline for what 3P currently delivers.
- Predict aggregate impact if buyer choices shift toward higher-quality segments.
- Identify which 3P segments are bad vs good (the input to TI-956's scoring pipeline).

**Why 3P-only campaigns are the clean baseline:**
- MM + 3P mixes in MM scoring → obscures 3P-specific effects.
- 3P + CRM mixes in CRM polarity behavior.
- **3P-only (no MM, no CRM, no Select) is the only clean signal** for measuring 3P-quality changes.
- Current 3P-only cohort: 404 campaigns / 179 advertisers / $1.23M / 30d / CVR 0.10% / cost-per-conv $29.64.

**Sibling ticket:** [[reference-airflow-ti]] mentions TI-956 — that's the scoring pipeline build TI-999 justifies. See `tickets/ti_956_interest_segment_scoring_schedule/`.

**How to apply:**
- Any future TI-999 analysis should map back to "does this help us evaluate the curation/ranking intervention?"
- When analyzing per-bucket performance, the cleanest comparison is **3P-only as pre-period vs hypothetical-curated-3P as post-period**.
- Within 3P-only, look at per-segment KPI distributions (top decile vs bottom decile) — wide tails mean curation has high prize.
- Per-3P-provider breakdown (LiveRamp vs ShareThis vs Dstillery) is relevant because LiveRamp dominates and is fresh; ShareThis/Dstillery catalogs are 100% >2yr stale.
- See also [[reference-causal-impact-pattern]] for the standard tiered-rollout evaluation if/when curation ships and we need to measure lift.
