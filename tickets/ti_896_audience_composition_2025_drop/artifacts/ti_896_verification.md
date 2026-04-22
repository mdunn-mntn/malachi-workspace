# TI-896 — Verification record

Pre-scrutiny audit, 2026-04-22. Seven independent checks (V1–V7) against the analysis, each with query evidence.

## Summary

One methodology defect found and corrected during verification — in two iterations:

**Iteration 1:** Initial classifier treated *any* mention of DS13 as "Peak Performance". DS13 ("MNTN Vertical Categorization") has ≥3 years of legacy use, so this over-counted by ~12pp on pre-drop baseline. Replaced with a detector requiring DS13 AND DS19 in the same expression.

**Iteration 2:** DS13+DS19 detector still gave a ~1% pre-launch baseline. Inspection of those pre-launch matches showed they used the old `{"select":[...],"categories":{...}}` schema WITHOUT an RTC score directive — they were legacy hybrid Interest+Keywords audiences, not Peak Performance. Final detector requires `score_type=rtc` AND DS13 AND DS19 together (the signature post-Oct-launch PP audiences carry in segment-archive form). Residual ~1% baseline from June 2025 onwards is attributable to early-access configurations or other RTC+DS13+DS19 combinations — not formal PP.

**Published numbers** use the tightest detector. Absolute adoption framing replaces the earlier multiplier framing since the pre-launch baseline is non-zero but small.

Other checks passed with minor caveats documented below.

---

## V1 — Is DS13 actually Peak Performance?

**Query:** Sample audiences from `archives_audiences_archives` where `LOWER(name) LIKE '%peak performance%'`, inspect their expression JSON.

**Result:** Eight sampled audiences (advertisers 32167, 32233, 32286) across October 2025. All follow the same schema:

```json
{"interest":{"include":[{"or":[
  {"data_source_id":13, "cats":[107000|108000|111004]},
  {"data_source_id":19, "cats":[900095, ...]}
]}]}}
```

DS13 intent layer (small `cats` ids — 107000, 108000, 111004) paired with DS19 keyword ids (900xxx range) inside an OR clause.

**Conclusion:** DS13 is the *intent-score layer* of Peak Performance; DS19 is the *keyword layer*. Peak Performance requires both. Took this to the strict detector in V3.

## V2 — DS13 category-id distribution pre / post Oct 2025

**Query:** `REGEXP_EXTRACT_ALL` of DS13 category_ids in Sep–Dec 2025 expressions, bucketed pre-Oct vs post-Oct.

**Result:** Post-Oct expressions dominated by PP-signature cat_ids in the 100000–135000 range (108000, 111000, 111004, 112001, 114003, 117001, 126005, 132002, 135005). Pre-Oct usage is much smaller (88 expressions), generally older ids.

**Conclusion:** DS13 usage materially changed at category-id level after Oct 2025 — not just volume, but *which* DS13 categories are being used. Consistent with a new product layer launching on top of DS13.

## V3 — Strict PP detector vs DS13-alone

**Query:** Weekly cohort-share of (a) any DS13 mention vs (b) DS13 AND DS19 in same expression.

| Date | n_adv | DS13-alone | Strict PP | DS16 |
|------|------:|-----------:|----------:|-----:|
| 2024-11-04 | 879 | 16.3% | 0.1% | 88.7%† |
| 2025-03-03 | 1,828 | 13.8% | 0.1% | 1.4% |
| 2025-09-22 | 3,146 | 9.6% | 1.1% | 1.0% |
| 2025-09-29 | 3,199 | 12.7% | 4.1% | 1.1% |
| **2025-10-06** (PP launch) | 3,269 | 15.7% | **6.9%** | 1.2% |
| 2025-10-13 | 3,315 | 18.3% | 9.7% | 1.2% |
| 2025-11-17 | 3,515 | 23.1% | 13.9% | 24.3% |
| 2025-12-29 | 3,676 | 24.9% | 15.9% | 28.1% |
| 2026-04-20 | 4,111 | 30.0% | **21.0%** | 35.3% |

† Pre-2025-03 DS16 baseline was inflated by a deprecated audience structure — collapses to ~1% from March onward.

**Conclusion:** Strict detector gives a cleaner baseline (~1% vs ~13%) and a sharper inflection (near-zero → 21% in ~6 months). **Swapped the main query to strict detection.** DS13-alone numbers are recorded in the summary for reference but no longer power the headline.

## V4 — Exclusion (NOT-clause) pollution by DS

**Query:** For November 2025 expressions, count any-mention vs mentions inside `"not","value":{"op":"any","value":{"data_source_id":N}}` structures.

| Bucket | Any | In NOT clause | Pollution |
|--------|----:|-------------:|---------:|
| DS2 (MM)       | 3,719 | 11    | 0.3% |
| DS4 (CRM)      | 1,662 | 41    | 2.5% |
| DS13           | 1,459 | 0     | **0%** |
| DS19 (Keywords)| 4,744 | 0     | **0%** |
| DS35 (3P)      | 2,203 | 0     | **0%** |
| DS16           | 8,764 | 1,095 | **12.5%** |

**Conclusion:** The five primary buckets (MM / 3P / CRM / PP / Keywords) are ≤2.5% polluted by NOT clauses. Safe for headline numbers. **DS16 is materially polluted** (12.5%) — do not cite as an audience-type signal without further parsing of include-vs-exclude structure.

## V5 — Cohort reconcile

**Query:** Distinct advertisers with `impressions > 0` in 2025 from `summarydata.sum_by_campaign_by_day`.

**Result:** ~4,111 advertisers as of 2026-04-22. Matches cohort size in composition output exactly. In Oct–Dec 2025 specifically, 2,380 advertisers were delivering impressions — consistent subset of the YTD cohort.

**Conclusion:** Cohort definition stable and reproducible.

## V6 — Advertiser-level spot-check (AID 32167)

**Query:** All archive rows for advertiser 32167 across Sep–Nov 2025, flagging DS13 / DS19 / DS35 / DS2 presence.

**Result:**
- Sep 3–15 2025: zero DS13 usage across 30+ campaigns. Pre-PP state.
- Oct 1–7 2025: first DS13+DS19 audiences appear (audience_id 50181, 50521, 50579, 50655, 49934 — all version 1). Tight alignment with Oct-6 launch week.
- Oct 28 2025: bulk rollout — version bumps (v2, v3, v4, v5, v7, v11, v15) across 20+ existing campaigns, each adding DS13 (some PP-pattern, some DS13-alone).

**Conclusion:** Advertiser-level timeline matches cohort-level inflection. Cohort aggregate is not a statistical artefact — real advertisers adopted real PP audiences on real dates.

## V7 — WGU (AID 31357) dominance

**Query:** WGU share of spend and campaigns Oct–Dec 2025.

**Result:**
- WGU spend: $4.71M of $48.36M total → **9.7% of spend**
- WGU campaigns: 57 of 19,644 → **0.29% of campaigns**

**Conclusion:** WGU is a large spend outlier but contributes a negligible fraction of the campaign population. Presence-based percentage metrics (what we publish) are not materially affected by WGU. If we later switch to spend-weighted views (planned), we'll need to exclude or separately flag WGU.

---

## Outstanding uncertainties (flagged in deck appendix)

1. **DS16 jump (1% → 35%)** is real in volume but partially polluted by NOT-clause exclusions (12.5%). We don't cite this as a primary audience-type shift until we parse include-vs-exclude at the JSON-structure level. Worth a follow-up ticket.
2. **Classifier maps "Mountain Matched" to DS2 + per-advertiser "First Party Audience" sources.** Could be narrower or wider depending on how MM is defined internally. Bryce did not specify a DS id for MM in the scope post.
3. **3P = DS35 only** per scope. Full 3P ecosystem (DS1, DS3, DS11, DS17, DS18, DS20, DS22, DS29, DS33, DS36, DS39) exists and moved in the same window — not wrong to scope narrowly, but a broader view would show a larger 3P shift. Both views available; we publish the narrow one.
4. **Retargeting-share chart uses `objective_id = 4`.** Known unreliable post-TV-migration (Ray, 2026-03-11). Include `funnel_level` cross-reference in deck.

## Corrections applied

- Classifier upgraded twice:
  - v4: strict (DS13 AND DS19)
  - v5 (final): schema-specific — `score_type=rtc` + DS13 + DS19 in same expression.
- Chart 01 title: "21% of 2025-active advertisers have adopted Peak Performance".
- Chart 01 caption: "PP detector: expression carries score_type=rtc + DS13 + DS19 together".
- Chart 04 title updated.
- Deck Power Line, big-number slide, and close slide rewritten to absolute-adoption framing (no multiplier).

## Final published numbers

- Cohort: **4,109 advertisers** with ≥1 impression in 2025.
- Peak Performance adopters as of 2026-04-20: **858 advertisers** = **21%** of the cohort.
- Pre-launch baseline (June–Sep 2025): ~1% (early-access / legacy RTC+DS13+DS19 configurations).
- MM, Keywords, 3P, CRM, retargeting-share: all flat within ±1pp Sep–Dec 2025.
