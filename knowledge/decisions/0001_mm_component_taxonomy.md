---
doc_type: decision
title: "0001 — Model Match (MM) component taxonomy: DS19 / DS13 / DS46"
summary: "MM = DS19 (Core) + one PP slot (DS13 = PP v1, DS46 = PP v2); counting MM as DS19 alone undercounts ~7.6%"
status: accepted
date: 2026-07-19
last_verified: 2026-07-19
keywords: [MM, model match, DS19, DS13, DS46, PP, purchase propensity, audience taxonomy, data_source_id]
supersedes: null
tags: [audience, targeting]
---

# 0001 — Model Match (MM) component taxonomy: DS19 / DS13 / DS46

## Context
"Model Match" (MNTN's derived, scored audience) is referenced in analysis as if it were a single
`data_source_id`, but delivery spans three data sources, and DS13 vs DS46 were historically conflated.
Getting the set wrong swings "% of AIDs using MM" by ~2× and undercounts MM volume ~7.6%.

## Decision
Treat **MM as a 2×3 grid over three data sources**:
- **DS19 = MM Core** — the always-present Model Match core segment.
- **DS13 = Purchase Propensity (PP) v1**, **DS46 = Purchase Propensity (PP) v2** — one PP *slot* that
  migrated v1→v2; **DS13 and DS46 never co-occur** in the same segment (a rolling migration).

So `MM = DS19 ∪ (DS13 xor DS46)`. Counting "MM = DS19 only" undercounts by ~7.6%.

## Alternatives considered
- **MM = DS19 only** — rejected: undercounts MM volume ~7.6% and misses the entire PP slot.
- **DS13 and DS46 are independent additive components** — rejected: they are one slot (v1 superseded by
  v2); adding both double-counts the migrated population.

## Consequences
- "% of AIDs using MM" is only stable when the PP slot is included; on DS19 alone it swings ~83% → ~47%.
- Fangorn runs **two passes per IP** (HI + PP), producing two raw scores — the PP slot ties to the PP pass.
- **Affected knowledge docs:** [`../bq/integrationprod/audience_data_sources.md`](../bq/integrationprod/audience_data_sources.md), [`../bq/integrationprod/audience_segments.md`](../bq/integrationprod/audience_segments.md), [`../glossary.md`](../glossary.md) (MM/PP entries), `../ds_catalog.md`. Full nuance: `data_knowledge.md` § Audience System Architecture.
