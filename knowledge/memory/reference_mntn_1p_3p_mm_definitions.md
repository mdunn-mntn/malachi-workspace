---
name: reference-mntn-1p-3p-mm-definitions
description: "Core MNTN definitions of 1P, 3P, and MM — and which of them get scored. Per Victor Savitskiy 2026-05-28."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 790f6279-052b-404e-8970-f70d7eb62991
doc_type: memory
keywords: [1p 3p mm definitions, mountain match, scoring, victor savitskiy, ds4, ds17, ds35, household_score, and intersection, ti-956]
domain: [audience-scoring, identity]
lifecycle: active
last_verified: 2026-05-28
---
**Core MNTN definitions (Victor Savitskiy, TI team, 2026-05-28 Slack):**

The 1P / 3P / MM distinction is **about who provided the data**, not about who/what scores it:

| Bucket | What it is | Provided by | Scored by MNTN? |
|---|---|---|---|
| **1P** | Customer/account data the advertiser uploads (CRM lists, IP lists) | The advertiser | **No** |
| **3P** | Bought interest segments from external providers (LiveRamp, ShareThis, Dstillery, etc.) | External 3P vendor | **No** |
| **MM** | Mountain Match — MNTN's targeting product built on verticals, buckets, keywords, and MNTN-derived behavioral signals | MNTN | **Yes** |

**The scoring rule:**
- 1P data — **not scored** (it's the advertiser's own list; you bid on the IPs that match)
- 3P data — **not scored** (it's a filter — the IP is "in the LiveRamp segment" or it's not)
- **MM is what's scored** — verticals, keywords, behavioral models all funnel into MNTN's per-IP scoring (which appears in `cost_impression_log.model_params` as `household_score` etc.)

**Why this matters for bidder analysis:**
- When you see graduated `household_score` values on delivered impressions, that's MM scoring, not 1P or 3P.
- A campaign with only 3P targeting (no MM signal in expression) ends up bidding on IPs with whatever score they happen to have — but 3P itself doesn't bring scored IPs. Empirically (TI-999 Finding 14d), pure-3P prospecting delivers 74% on unscored IPs, identical to no-3P prospecting.
- For 3P delivery to overlap with scored IPs, you need to layer MM signals (RTC, BUK keywords, vertical categories, etc.) into the same campaign — that's the layer that pulls in MM-scored IPs.

**DS-level mapping** (operational, working understanding — verify with Victor/Zach before treating as authoritative):
- **1P** = DS4 (CRM), DS8 (IP List), DS47 (CRM Identity Graph)
- **3P** = DS17 (ShareThis), DS18 (Dstillery), DS35 (LiveRamp IP)
- **MM signals** (scored) = DS13 (Vertical Categorization), DS38 (BUK / UI Audience Keywords), DS46 (ML Audience Intent / Fangorn). **RTC (DS19) is likely a SEPARATE scoring system** (binary `realtime_conquest_score` flag, applies to recent-site visitors only) — not part of MM. Confirm with Victor.
- Other DSes (DS21 Conversion, DS34 Pageview, etc.) are MNTN-tracked retargeting signals — separate from MM scoring.

**Empirical layering rates (TI-999, 30d window ending 2026-05-28):**
- 72% of 3P-only campaigns ALSO use an MM signal in their expression; those drive 83% of 3P-only impressions/spend.
- The "pure-3P-no-MM" scenario is the minority: 28% of 3P-only campaigns, ~17% of impressions. Most "good" 3P delivery you see in the data is MM doing the scoring underneath.
- 35% of 1P-only campaigns also use MM; they drive 52% of 1P-only spend.

**How the bidder combines signals (per Victor 2026-05-28):**
- MM campaigns use **AND-type intersection** for clauses: every targeting filter (geo, 3P segment, MM signal) NARROWS the eligible IP set.
- Example (Victor): "if we add geo to campaign — it will narrow down scored audience." Same logic for layering 3P onto MM: 3P doesn't bring new IPs into the scored set; it narrows the MM-scored set to those that also match the 3P segment filter.
- Within a single source, categories can be OR'd (e.g., `"op":"or"` between LiveRamp segments) — but the top-level combination across sources is AND.
- **Implication for the empirical findings:** the "3P+MM has good scored delivery" pattern is MM scoring its already-scored universe, narrowed by 3P — NOT 3P pulling in scored IPs. Pure-3P (no MM in expression) means the bidder has no scoring signal at all on the filtered set → delivers on whatever bid requests come through, mostly unscored.

**Implication for TI-956 framing:**
- TI-956's per-segment composite scoring would be MNTN's **first scoring layer on 3P**.
- Today 3P is selection-only; campaigns that rely on it without MM signals layered get whatever IPs the audience filter intersects with the bid stream, with no quality differentiation.
- Adding per-segment scores to 3P closes a real gap — it's not "improving an existing scoring layer."

**Naming-pitfall warning** (per Malachi correction in Victor Slack thread, 2026-05-28): when Malachi initially said "1P scoring" he meant **MM scoring**. Be careful — informal usage sometimes uses "1P" loosely to mean "MNTN-derived" (which is actually MM). The strict Victor definitions above are the canonical version; if a conversation says "1P scoring" with no explicit context, clarify whether they mean strict-1P (CRM upload) or MM.

Reference: TI-999 Finding 14 (`tickets/ti_999_interest_segment_sizing/summary.md`); `knowledge/data_knowledge.md` Bidder Scoring Reality section.

Related: [[reference_bidder_scoring_reality]], [[feedback_crm_excluded_from_prospecting]], [[project_ti_999_interest_segment_sizing]].
