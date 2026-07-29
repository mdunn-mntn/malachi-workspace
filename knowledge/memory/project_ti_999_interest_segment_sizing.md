---
name: project-ti-999-interest-segment-sizing
description: TI-999 — interest-segment portfolio sizing (sibling to TI-956 scoring pipeline). Prospecting-only re-cut documented; awaiting Zach + Alex validation.
metadata: 
  node_type: memory
  type: project
  originSessionId: 790f6279-052b-404e-8970-f70d7eb62991
doc_type: memory
keywords: [ti_999_interest_segment_sizing, interest segment sizing, 3P interest, prospecting spend, stale segments, DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp, Zach, Alex]
domain: [project, audience-scoring, incrementality]
lifecycle: active
last_verified: 2026-05-28
---
TI-999 (created 2026-05-28) is the empirical sizing analysis that justifies [[project_buk_loom_request]] / [[project_incrementality_pivot]]-adjacent TI-956 scoring infrastructure.

**Why:** TI-956 builds Alex's segment-quality scoring framework on a schedule. TI-999 quantifies the prize — what fraction of MNTN spend rides on 3P interest segments today, how stale they are, and how 3P performs vs no-3P prospecting.

**How to apply:** When picking up TI-999 in a new chat, read [tickets/ti_999_interest_segment_sizing/summary.md](tickets/ti_999_interest_segment_sizing/summary.md) "Current state" header — it's the canonical handoff. The prospecting-only numbers (Finding 11) are the current headline; all-campaigns numbers (Findings 3-10) are pre-correction historical context only.

**Key headline numbers (prospecting-only, 30d ending 2026-05-28):**
- 13,511 prospecting campaigns / $24.86M / 30d (~$298M/yr).
- 34.6% of prospecting spend uses 3P interest → ~$103M/yr.
- 18.3% touches stale 3P (ShareThis + Dstillery, both >2yr stale) → ~$55M/yr.
- No-3P prospecting converts 2.1x better than fresh-LiveRamp prospecting (0.126% vs 0.059%).

**Operational interest-segment DS set:** `{DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP}`. Borderline `DS49 Publisher Network` flagged.

**Open items:** Zach validates DS set + bucket logic; Alex sanity-checks numbers vs his scoring framework; resolve DS49; new chats picking this up should start with `summary.md` Current State block then `presentation.md`.

**Sibling:** [[project_buk_loom_request]]-adjacent TI-956 (scoring pipeline) — separate ticket folder `tickets/ti_956_interest_segment_scoring_schedule/`.
