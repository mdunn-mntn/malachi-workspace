---
name: reference-fangorn-audience-overlay
description: Fangorn switch uses the audience-overlay feature — updates audience_segments to reference DS46 but leaves DS13/DS19 in the base audience table. Scoring still uses DS13/DS19 substrate.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [fangorn audience overlay, audience_segments, DS46, DS13, DS19, audiences template, Ryan Kleck, MM undercount, peak performance]
domain: [audience-scoring]
lifecycle: active
last_verified: 2026-07-08
---
**Per Ryan Kleck (TI team, 2026-06-01):** when MNTN switches an advertiser to Fangorn, the change is applied via the **"audience overlay"** feature.

> "when we switch someone to Fangorn we use the 'audience overlay' feature that changes their audience expression in the audience_segments table, but it does NOT change the base expression in audience table... so those will still have DS13 in audience table (if they have peak performance on)"

**Two layers of expression state:**

| Table | What it holds | For Fangorn-on advertiser |
|---|---|---|
| `audience.audiences` (template) | Buyer's MM 2.0 base config | DS13 (Vertical) + DS19 (Keywords) |
| `audience.audience_segments` (active) | Compiled overlaid expression | References DS46 (Fangorn) |

**So DS46 in audience_segments = Fangorn scoring overlay applied on top of an underlying DS13/DS19 MM 2.0 config.** The scoring substrate (which IPs are evaluated) is still DS13 vertical + DS19 keywords. Fangorn just replaces the scoring algorithm.

**Practical implications:**

- Counting "MM-touching" by DS13/19/38/46 in audience_segments correctly captures Fangorn-on campaigns too — they ARE MM, just with Fangorn scoring.
- A campaign showing DS46 alone in audience_segments is NOT a new audience type — it's MM with Fangorn overlay.
- Joining audience_segments to the `audiences` template would reveal the underlying DS13/DS19 config behind any DS46 row.

**Empirical confirmation + refinement (TI-1037, 2026-07-08, live prospecting obj=1/funnel=1 delivered last 45d):** at SEGMENT level DS46 co-occurs with DS13 exactly **never** (0 of 4,610 campaigns) — the flip swaps 13→46 in segments — while **DS19 survives the flip** (DS19+DS46 on 1,314 campaigns / 18.9% of spend; DS46-only on 235 campaigns / 6.5% — those are ex-vertical-only audiences). So "segments show DS46 → template still holds DS13/19" remains the right mental model, but never expect to SEE 13 and 46 together in segments. "MM = has DS19" (Alyson's definition) undercounts MM-scored spend ~7.6% (misses DS46-only + DS13-only). 8-cell table: `knowledge/data_knowledge.md` § `"MM = has DS19" is an undercount`; query `tickets/ti_1037_audience_diagnostic_tool/queries/ti_1037_mm_ds_cooccurrence.sql`.

**Related:** [[reference-prospecting-scores-gcs-monitor]] documents the Fangorn-on vs Non-Fangorn score landscape; [[reference-rtc-hhst-gating]] for the HHST mechanic that gates whether scores even matter at the bidder.
