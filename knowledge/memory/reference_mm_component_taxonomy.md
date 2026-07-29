---
name: reference_mm_component_taxonomy
description: "SETTLED MM taxonomy (Matt Brorby 2026-07-08) — DS19=MM Core/keywords, DS13=Peak Performance v1, DS46=PP v2 (Fangorn, same slot as 13, never co-occur); \"MM = has DS19\" undercounts ~7.6% of prospecting spend"
metadata: 
  node_type: memory
  type: reference
  originSessionId: a16447af-e118-4162-9ce4-bcd81ee4ffdb
doc_type: memory
keywords: [mm taxonomy, ds19, ds13, ds46, peak performance, fangorn, mmv2, mmv3, hhst gate, matt brorby, audi-1083]
domain: [audience-scoring]
lifecycle: active
last_verified: 2026-07-23
---
**The MM config space is a 2×3 grid** (settled with Matt Brorby, TI, 2026-07-08): keyword layer (DS19 y/n) × vertical anchor (none / DS13 / DS46). Official component names — each named for the IP score tier its leaf unlocks:

- **DS19 = "MM Core" / Keyword-Only** (→ Max Reach tier). 42.7% of live prospecting spend runs DS19-only.
- **DS13 = "Peak Performance" (v1)** (→ PP 8000 tier). The shipped Oct-'25 PP *product* = DS13+DS19+RTC, HHST 6666.
- **DS46 = "Peak Performance v2"** (Fangorn) — SAME slot as DS13 (identical leaf `{"data_source_id":N,"category_ids":[<6-digit vertical id = the RTC id>]}`); the flip swaps 13→46, so **DS13∧DS46 = 0 campaigns, ever**.
- **"Expanded PP"** = DS13 with bucket (3-digit) ids — named, never shipped; ZERO live leaves carry bucket ids.
- **"Vertical only"** (Matt's colloquialism) = PP-only / PP-v2-only campaigns (no DS19). Tier reach differs by generation: v1 vertical-only delivered HI 10000s + PP 8000s (categorical); **v2 vertical-only tops out at 8000 — the v2 HI band (8001–10000) requires the keyword layer** (verified 2026-07-08: 100% of >8000 delivery on DS19 campaigns).
- **v1 vs v2 scoring mechanism:** v1 = categorical fixed points (8000/10000 exactly; nothing between). v2 = two continuous passes, label follows the SCORE: PP pass → 6666–8000 band, HI pass → 8001–10000, pins at band tops; below-bar structural matches fall to MI/MaxReach. See [[reference-fangorn-two-model-passes]] + methodology page 3414917161.

**Key rules:** include leaves OR-join (adding a leaf broadens); HHST gates delivery by score (10000→HI, 6666→HI+PP, 0→all matched); HI/PP/MI/MaxReach are IP tiers, not campaign types; **MM = any of DS19/DS13/DS46** — a "has DS19" test (Alyson's old working def) misses ~7.6% of prospecting spend / ~157 advertisers. Segment level (`audience.audience_segments` type 2 targeted) is authoritative; template level still shows DS13/DS19 after a Fangorn flip.

**Where the full record lives:** `knowledge/data_knowledge.md` § `"MM = has DS19" is an undercount` (8-cell table + old MM 2.0 IP-state decode) · Confluence https://mntn.atlassian.net/wiki/spaces/TAR/pages/3691708511 (update both if the taxonomy changes) · query `tickets/ti_1037_audience_diagnostic_tool/queries/ti_1037_mm_ds_cooccurrence.sql` · Slack-pasteable card: `artifacts/ti_1037_mm_taxonomy_image.py`.

**TEAM NAMING — the team refers to these by MM VERSION (Alyson, 2026-07-22; IMPLEMENTED 2026-07-23 as in-place `mm_class` renames, NOT a separate column):**
- `mm_keywords_only` (DS19-only) → **mmv2** (DS19 = "MNTN Matched V2").
- `mm_classic` (DS19+DS13) → **mmv3**.
- `vertical_only_legacy` (DS13-only) → **mmv1 if `campaign_created` < ~2024-09-01, else mmv3** (mmv1 shipped ~Dec'23-Jan'24, mmv3 ~Sept'24; AP holds exact date). Empirically validated: DS13-only creation collapses Sept→Oct 2024 (15→3); mmv1 cohort's latest create = 2024-08-30.
- `mm_flagship_fangorn` (DS19+DS46), `fangorn_vertical_only` (DS46-only), `non_mm` KEEP structural names. **Fangorn (DS46) = "updated DS13" (continuous scoring), same vertical slot.**
- **`mmv3` now spans two structural configs** (DS19+DS13 reach HI; DS13-only-post-cutoff caps at PP), so `tiers_reachable` is computed from the raw DS flags, not `mm_class`. New col `campaign_created` exposed. Verified counts: mmv2 3,594 / flagship 1,761 / fangorn_vertical_only 411 / mmv3 393 (312+81) / mmv1 134 / non_mm 8,182.

Related: [[reference-fangorn-audience-overlay]] (the flip mechanic), [[reference_mntn_1p_3p_mm_definitions]], [[reference_fangorn_two_model_passes]], [[reference_audience_intent_scoring_dag]].

**Migration status (Matt Brorby 2026-07-13): most active advertisers ALREADY on DS46; forcing the
DS13 remainder is uncontroversial (Alex owns the tail solutions). DS46 = guid_log only (post-retrain:
aug+guid feature store) → 3P svs vendors have zero Fangorn impact; vendor dependency = DS19 MM Core +
shrinking DS13 tail.**

**DS19 vendor-dependency quantified (AUDI-1089 q13b, 2026-07-15):** vendors uniquely hold 30.3% of
DS19-member IPs but the slice is dark (0.48% serve rate, VR 0.061%, 92% unscored); every scored tier
≥99% free-log-covered INSIDE DS19 — max-reach 99.4%. Keyword UI audience counts shrink ~30% under
free-only; effective scored keyword audience is vendor-independent. DS19 keyword names:
`dw-main-bronze.external.tpa__mntn_matched_taxonomy__v2` (ids ≥900000; don't use `categories`).

**AUDI-1083 classifying view (2026-07-22) operationalizes this grid as a durable campaign-grain
classifier** (`tickets/audi_1083_mm_classifying_view/queries/audi_1083_mm_classifier_view.sql`).
`mm_class` column = the 6 live cells: `mm_flagship_fangorn` (DS19+DS46) · `fangorn_vertical_only`
(DS46-only) · `mm_classic` (DS19+DS13 = PP config) · `vertical_only_legacy` (DS13-only) ·
`mm_keywords_only` (DS19-only = Max Reach) · `non_mm`. Two headline booleans (user decision):
`is_unmodified_mm` = any MM engine + gated + national + no AND-narrow ($14.0M/32.5% of prospecting);
`is_flagship` = that AND `mm_class='mm_flagship_fangorn'` — **flagship = DS19+DS46 SPECIFICALLY**,
DS46-only "fangorn_vertical_only" is unmodified MM but NOT flagship (caps at PP band, no HI band
without the keyword layer) → $2.9M/6.7%. Live headline: 70.9% of prospecting spend is MM-labelled
(`has_mm`) but only 32.5% unmodified / 6.7% flagship. View reproduces this grid's cell %s (DS13
cells now smaller — continued 13→46 migration). See [[reference_fangorn_tier_assignment]] for the
verified BQ Fangorn-tier table path. Team-feedback spec page (draft, TAR space, child of the MM
Taxonomy page): https://mntn.atlassian.net/wiki/spaces/TAR/pages/3712811252 (open questions =
flagship def, gate rule, geo threshold, keyword-only-as-MM, grain, materialization).
