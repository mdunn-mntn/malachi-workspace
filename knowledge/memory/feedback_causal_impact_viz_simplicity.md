---
name: CausalImpact / pre-post viz: aggregate-only at chart level
description: For pre/post and CausalImpact result visuals, only chart total aggregate change. Per-advertiser detail belongs in the table, not the chart.
type: feedback
originSessionId: bb7e5bd3-33da-47b0-90cf-096e60626802
doc_type: memory
keywords: [causalimpact, pre post viz, aggregate chart, per-advertiser table, fangorn notebook, TI-921, lift visualization, IVR CVR CPA]
domain: [experimentation, workflow]
lifecycle: active
last_verified: 2026-05-08
---
For pre/post and CausalImpact result visualizations, **only show total aggregate change in charts**. Per-advertiser breakdowns belong at the table level, not in the visualizations. Notebooks should be as simple as possible — long, complex notebooks bury the result.

**Why:** The Fangorn lift notebook (TI-849 / TI-921 handoff) was flagged as "really really long and complex … wasn't clear what's going on" when handed to Alex Knorr 2026-05-08. Per-advertiser charts overwhelm the audience and obscure the headline. The headline metric is the aggregate effect; individual advertisers are appendix-grade detail.

**How to apply:**
- Charts: aggregate KPI panel only (one IVR chart, one CVR chart, one CPA chart — all advertisers pooled).
- Tables: per-advertiser rows OK here.
- Keep notebook structure flat and short. One result per section. Strip exploratory dead-ends before sharing.
- Applies to all incrementality / lift / CausalImpact / pre-post deliverables, not just Fangorn.
