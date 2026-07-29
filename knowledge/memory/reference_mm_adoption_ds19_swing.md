---
name: reference_mm_adoption_ds19_swing
description: "% of advertisers using Mountain Matched swings ~2x on whether DS19 counts as MM — canonical (incl DS19) ~83% all-active / ~87% prospecting; excl DS19 ~47%"
metadata: 
  node_type: memory
  type: reference
  originSessionId: e0fb9085-36f5-4751-8093-7f26e9fd4f18
doc_type: memory
keywords: [mm_adoption_ds19_swing, adoption, ds19, swing, advertisers, mountain, matched, swings]
domain: [reference]
lifecycle: active
last_verified: 2026-06-16
---
"What % of AIDs use Mountain Matched?" has NO single answer — it swings ~2x on one definitional choice: **does DS19 (MNTN Matched keywords) count as MM?**

Current (window 2026-05-01→06-15, distinct advertisers from live `audience_segments` expressions, deduped):
- **Incl. DS19 (canonical corrected MM = DS13/19/38/46):** **~83% all-active** (1,756/2,119), **~87% prospecting** (1,755/2,015). Campaign-level prospecting: 28% (3,471/12,410).
- **Excl. DS19 (DS13/38/46 only — vertical + Fangorn):** ~47% all-active (988/2,119), ~49% prospecting. Campaign-level: 16%.

DS19 literally *is* "MNTN Matched" (per TI-999 Pass 17 corrected-MM audit + [[reference_mntn_1p_3p_mm_definitions]]), so the canonical answer **includes DS19 → ~83–87%**. The "~half of advertisers" framing only holds if you exclude DS19.

Gotchas that produced wrong/inconsistent historical numbers:
- The old "~50% of advertisers" used excl-DS19; the old "3,200 MM campaigns / 73.1% non-MM" used incl-DS19 — internally inconsistent.
- Count MM from **live `audience_segments`** (actual per-campaign targeting), NOT `audience_audiences` (templates — not what's targeted). Matching at campaign-GROUP level off templates (Alex Knorr's 2026-06 query) over-counts ~5 pts (~88% vs true ~83%).
- Venn-bucket `n_advertisers` columns OVERLAP across buckets — never sum them for an advertiser total; dedupe with `LOGICAL_OR(has_mm) GROUP BY advertiser_id`.
- MM = DS13 (vertical) / 19 (MNTN Matched keywords) / 38 (BUK, ~0 usage) / 46 (Fangorn).

Canonical query: `tickets/ti_999_interest_segment_sizing/queries/ti_999_mm_adoption_current.sql` (full 2×2×2 sensitivity table). Pass17/21 def in [[reference_ti_956_per_pattern_application]] lineage.
