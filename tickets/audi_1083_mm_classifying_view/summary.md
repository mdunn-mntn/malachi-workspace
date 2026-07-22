---
doc_type: ticket
title: "AUDI-1083: MNTN Matched classifying view — grade MM vs raw DS presence"
status: in_progress
date: 2026-07-22
summary: "Durable campaign-grain view: what MM engine + how restricted, so 'MM' means flagship not DS-present"
result: ""
---

# AUDI-1083: MNTN Matched classifying view

**Jira:** https://mntn.atlassian.net/browse/AUDI-1083
**Status:** In Progress (design)
**Date Started:** 2026-07-22
**Assignee:** Malachi

---

## 1. Introduction
"MM" cannot be identified by DS13/19/46 presence alone: >74% of MM campaigns are modified
(geo limits, 3P include/exclude, gate off), so a campaign filtered to one ZIP still reads as
"MM" under datasource-existence filtering. As models evolve (Fangorn today, more later) the
label is also generationally ambiguous — a Fangorn campaign and a legacy bucketed campaign are
both called "MM."

Goal: a **durable, campaign-grain DB view** that any analysis can LEFT JOIN on
`campaign_id` / `campaign_group_id` to answer two independent questions —
**(1) which MM engine is this** and **(2) how far has the buyer carved the targetable pool away
from flagship** — with enough raw flags exposed that a downstream analyst can set their own bar
(FLAGSHIP, FANGORN, DS13/19/46, gate, geo, 3P).

## 2. The Problem
Ad-hoc "has DS19" (or "has DS13/19/46") filtering both **over-** and **under-**counts MM:
- Over-counts: a DS19 campaign narrowed to one ZIP, or hard-intersected with a 3P segment
  (AND-include), or with the intent gate OFF, is "MM" in name only.
- Under-counts: DS46-only (Fangorn ex-vertical-only) and DS13-only carry no DS19 → "has DS19"
  misses ~7.6% of prospecting spend / ~157 advertisers (TI-1037).

We need a systematic grading that stays robust as the model generation changes.

## 3. Prior work this builds on (already done — the raw materials)
The view is an assembly of components already built and validated on other tickets:

| Component | Where | What it gives the view |
|---|---|---|
| Polarity-aware AST parser (`parse_expression`, LCA tree-walk `classify_3p_include_semantics`) | `tickets/ti_999.../queries/ti_999_clause_polarity_ast.sql`, `tickets/audi_1141.../queries/audi_1141_cohort_scorecard.sql` | DS presence **by polarity** (pos/neg) + **3P OR-vs-AND** semantics |
| MM taxonomy 2×3 grid (DS19 × {none/DS13/DS46}) | TI-1037 `ti_1037_mm_ds_cooccurrence.sql`; memory `reference_mm_component_taxonomy` | the engine/config classification; DS13∧DS46 never co-occur (flip swaps 13→46) |
| Fangorn band-continuity detector (CIL `household_score`) | `reference_fangorn_detection` | empirical v1-vs-v2 confirmation (calibration only — too expensive for the live view) |
| MM/restricted/3P/Neither classifier w/ geo-narrow + HHST gate | **AUDI-1141 `audi_1141_cohort_scorecard.sql` — the closest existing prototype of this view** | the working template to generalize |
| Geo axis rules (US-only, narrow = state/DMA/city/ZIP/radius) | `feedback_geo_axes` | which geo clauses are "narrowing" vs default |
| Fangorn rollout tier table `dw-main-bronze.tpa.fangorn_advertiser_inclusion` | `reference_fangorn_tier_assignment` | advertiser-level Fangorn tier (1-5,99), permanent holdout = 5 |
| HHST gate `dw-main-silver.dso.household_score_thresholds` (+ `archives.household_score_threshold_archives`) | `reference_rtc_hhst_gating` | the intent gate — most important scoring switch |

## 4. Design — two orthogonal axes, NOT one fabricated %

The central design decision: **do not collapse everything into a single "MM-ness score."** The
buyer did two independent things, and conflating them hides signal. Expose both, plus the raw
components, and one headline boolean.

### Axis A — WHICH engine (exact, categorical, from segment-level DS presence)
`mm_engine`:
- `non_mm` — no DS13/19/38/46 positive
- `mm_core` — DS19 positive, no DS13/DS46 anchor (keyword-only → Max Reach engine)
- `peak_performance_v1` — DS13 anchor (legacy categorical), ± DS19
- `fangorn_v2` — DS46 anchor (continuous two-pass), ± DS19

Plus `mm_config` = the 2×3 cell label (keyword-only / vertical-only / vertical+keyword × v1/v2).

### Axis B — HOW restricted the targetable pool is (the "is it fair to call it MM" axis)
Built from concrete, checkable sub-signals rather than one guessed number:

| Sub-signal | Column | How computed | Quantifiable? |
|---|---|---|---|
| Geo narrowing | `geo_reach_pct` | US-HH (or pop) inside the positive geo include ÷ US total | **EXACT** *if a per-location population/HH table exists* (open item 8.1) |
| Geo narrowest level | `geo_narrowest_type` | country / DMA / state / city / ZIP / radius | exact |
| 3P hard-intersection | `and_3p_narrowed` (bool) | LCA tree-walk = `AND_include`/`mixed` | binary (magnitude not cheaply sizeable) |
| 1P seeded-include | `and_1p_narrowed` (bool) | DS4/8/47 positive AND-joined | binary |
| 3P additive | `or_3p_additive` (bool) | LCA = `OR_include` | cosmetic — **stays flagship** |
| CRM hygiene | `crm_excl_hygiene` (bool) | DS4/8/47 negative | cosmetic — stays flagship |
| Intent gate | `hhst_gated` (bool) | latest `household_score_thresholds.threshold > 0` | exact |

**Why geo is the one real %:** a one-ZIP campaign has `geo_reach_pct ≈ 0.01%` — that single exact
column cleanly catches the motivating example without any heuristic. AND-3P/AND-1P can shrink the
pool 90% while geo stays 100%, so a *pure-geo* % would miss audience narrowing — that is exactly
why B is a set of exposed components + a rule, not a multiplied composite.

### The rollup columns downstream actually filter on
- `restriction_level` (ordinal): `flagship` → `lightly_modified` (only cosmetic layers) →
  `geo_narrowed` / `audience_narrowed` → `ungated`. (Not mutually exclusive in reality — the
  ordinal takes the most-severe; the component booleans stay exposed for custom bars.)
- `is_flagship_mm` (bool) — the 90%-of-joins headline:
  `mm_engine='fangorn_v2' AND hhst_gated AND geo_reach_pct >= τ_geo AND NOT and_3p_narrowed AND NOT and_1p_narrowed`
  (τ_geo TBD by calibration; start ~50%). A "legacy-flagship" variant swaps `fangorn_v2`→any MM engine.

## 5. FINAL view schema (campaign grain) — implemented in queries/audi_1083_mm_classifier_view.sql
```
-- keys / grain
campaign_id, campaign_group_id, advertiser_id, objective_id, funnel_level
-- Axis A: engine (authoritative taxonomy)
mm_class, mm_engine_rank, has_ds13, has_ds19, has_ds38, has_ds46, has_mm
-- scoring / gate / rollout generation
hhst_current, hhst_gated, fangorn_tier, is_express   -- integrationprod.tpa_fangorn_advertiser_inclusion
-- Axis B: restriction components
geo_reach_pct (deferred=NULL), geo_narrowest_type, has_geo_narrow_incl, has_geo_excl,
has_3p_incl, three_p_semantics (or_include/and_include/mixed/three_p_only/none),
and_3p_narrowed, or_3p_additive, and_1p_narrowed, crm_excl_hygiene
-- rollups
restriction_level, is_unmodified_mm, is_flagship
```
Grain = campaign (latest targeted segment rn=1 by update_time; `deleted=FALSE AND is_test=FALSE`).
View classifies ALL campaigns — no prospecting/delivered pre-filter (objective_id/funnel_level are
exposed for downstream). `campaign_group_id` is an attribute for rollup; a group-level verdict is a
GROUP BY (all-flagship vs mixed) — companion view, not baked into the base grain.

**Status: draft view validated end-to-end on live data. Remaining = materialization (SQLMesh) +
the v2 geo_reach_pct / camperbid pool upgrade.**

### 4a-canon. MM definition — CANONICAL (2026-07-22, resolved via two Confluence pages)
- **MM = any of DS13/19/46 (BROAD)** per the canonical taxonomy page §4
  (https://mntn.atlassian.net/wiki/spaces/TAR/pages/3691708511) — "has DS19" UNDERCOUNTS ~7.6%
  (the vertical-only cells ARE MM). **DS19 = "MM Core" is a COMPONENT inside MM, not the definition.**
  So the view's original `has_mm = DS13/19/38/46` was already correct; no change. A DS46-only
  gated/national campaign is correctly `is_unmodified_mm=TRUE`, `is_flagship=FALSE`.
- **Tiers are per-IP; the config's leaves decide which are biddable** (scoring page 3487891474):
  DS19-only reaches HI·MI·MaxReach (NOT PP); DS46-only = PP only; DS13-only = HI·PP; DS19+anchor =
  all four; non_mm = unscored. Captured in new column `tiers_reachable`. (Earlier session claim
  "DS19-only = unscored/max-reach" was wrong — corrected in data_knowledge.md.)
- Added column **`tiers_reachable`** (per taxonomy §3). Confluence spec page updated to v2 with the
  canonical definition + tier profiles + corrected mechanics.

### 4a. Locked decisions (2026-07-22)
- **Quantification = exposed components**, NOT a single composite %. `geo_reach_pct` is the one
  exact number; AND-narrowing + gate are binary flags; `restriction_level` is a rule over them.
- **AND-3P / AND-1P = binary flags only in v1.** No magnitude estimate (can't cheaply size
  |MM∩3P|/|MM| without IP intersection; segment-reach independence approximation too shaky).
- **FLAGSHIP and FANGORN decoupled:** `mm_engine` carries the generation (has the `fangorn_v2`
  value); `is_flagship_mm` is generation-agnostic ("well-configured MM"). Filter
  `is_flagship_mm AND mm_engine='fangorn_v2'` for flagship-Fangorn, or `... AND != 'fangorn_v2'`
  for flagship-legacy.

### 4b. Locked naming (aligned to TI-1037 authoritative 2×3 taxonomy grid)
- **`mm_class`** (the 6 live cells = DS19 kw × anchor {none/DS13/DS46}):
  `mm_flagship_fangorn` (DS19+DS46) · `fangorn_vertical_only` (DS46 only) · `mm_classic`
  (DS19+DS13, the shipped PP config) · `vertical_only_legacy` (DS13 only, MM 1.0) ·
  `mm_keywords_only` (DS19 only → Max Reach band) · `non_mm`.
- `mm_engine_rank`: 3=Fangorn(DS46) · 2=PeakPerformance(DS13) · 1=keywords/MaxReach · 0=non_mm.
- `restriction_level` (what got carved, most-severe wins): `none` | `geo` | `audience` | `geo+audience`.
- **Two booleans (user decision 2026-07-22):**
  - `is_unmodified_mm` = any MM engine, gated, national, no AND-narrow → **$14.0M / 32.5%** of prospecting spend.
  - `is_flagship` = `is_unmodified_mm AND mm_class='mm_flagship_fangorn'` (DS19+DS46 specifically —
    DS46-only "fangorn_vertical_only" is unmodified MM but caps at PP band, so NOT flagship) →
    **$2.9M / 6.7%**. Composable: `is_unmodified_mm AND mm_class='mm_keywords_only'` = Max-Reach flagship, etc.
- geo "narrow" = positive include at location_type ∈ {DMA, state, city, ZIP} or a `geo_radii` clause;
  country-level (US=237) / no-geo = default. `geo_reach_pct` deferred (no HH table — item 8.1).

**Definitive headline (live prospecting, 45d, $43.2M):** MM-labelled (`has_mm`) = **70.9%** ·
unmodified MM = **32.5%** · flagship Fangorn = **6.7%**. Of every MM-labelled dollar, <half is
unmodified. `mm_class` distribution reproduces the TI-1037 grid (keywords-only 42.2% vs 42.7%,
flagship-Fangorn 20.3% vs 18.9%, fangorn-vertical-only 6.8% vs 6.5%); the DS13 cells are smaller
(mm_classic 1.1% vs 4.0%) — continued DS13→DS46 migration, as expected. CSVs in `outputs/`.

### 4c. Draft view SQL
`queries/audi_1083_mm_classifier_view.sql` — full campaign-grain SELECT ready to materialize
(SQLMesh view). Two blockers before it runs clean: BQ re-auth + confirm the per-location HH table
(open item 8.1).

## 4d. Calibration — draft view run on live prospecting (2026-07-22)
View compiles and runs. Distribution over **live prospecting** campaigns (obj∈1,5,6 · funnel=1 ·
delivered 45d · $43.2M spend · `outputs/audi_1083_calibration_prospecting_45d.csv`):

| engine | flagship | non-flagship (restricted/ungated) | engine total |
|---|---|---|---|
| **mm_core** (DS19 keyword-only) | $10.1M / 23.4% | $8.1M | **$18.2M / 42%** |
| **fangorn_v2** (DS46) | $3.9M / 9.0% | $7.8M | **$11.7M / 27%** |
| **peak_performance_v1** (DS13) | $0.07M | $0.6M | $0.7M / 1.6% |
| **non_mm** | — | $12.6M | $12.6M / 29% |

**Headline findings:**
1. **`is_flagship_mm` (current def: any gated, national, un-narrowed MM) = $14.05M / 32.5% of
   prospecting spend.** ~2/3 of prospecting spend is "MM-labelled" but modified or non-MM —
   empirically confirms the ticket thesis (>74% modified).
2. **Geo is the #1 modifier.** Fangorn geo-narrowed ($5.9M) > Fangorn flagship ($3.9M); mm_core
   geo ($5.8M) similar. Most "MM" spend is geo-carved. Validates geo as the primary Axis B signal.
3. **mm_core (keyword-only) is the biggest MM engine ($18.2M, 42%)** and its flagship slice
   (23.4%) alone exceeds all Fangorn. → forces the flagship-definition decision (below).
4. **peak_performance_v1 nearly dead ($0.7M).** DS13→DS46 migration ~complete, as expected.
5. **AND-narrowing (audience) is real but smaller** (~$4M across engines).

**THE decision (needs sign-off):** does `is_flagship_mm` require a **vertical-anchor engine**
(Fangorn/PP) or does **keyword-only mm_core** count?
- Inclusive (current): any MM engine → **$14.05M / 32.5%**
- Vertical-anchor only (Fangorn + PP): **~$3.94M / 9.1%**
- Fangorn-flagship config only (DS46+DS19, gated, national): pinnable if we go strict
The swing is 9% ↔ 32% of prospecting spend. `mm_engine`/`mm_config` stay exposed either way; this
only sets the default boolean.

## 5b. Verified table facts (2026-07-22)
- Fangorn tiers: `dw-main-bronze.integrationprod.tpa_fangorn_advertiser_inclusion`
  (`advertiser_id`, `vertical_id`, `is_express`, **`fangorn_rollout_tier_num`**,
  `fangorn_advertiser_inclusion_date`). NOT `tpa.fangorn_advertiser_inclusion`.
- HHST gate: `dw-main-silver.dso.household_score_thresholds` = **exactly one row per campaign**
  (32,467 campaigns; 10,647 gated; join on `campaign_id`). `campaign_group_id`/`advertiser_id`
  are denormalized attributes, not a separate grain.
- `geos` JSON shape CONFIRMED: `{"op":"any","value":{"location_ids":[...]}}` under an
  and/or/not tree — `parse_geo` UDF validated (positive include extracted, negation-depth polarity).
- geo.location_data has **NO** household/pop column → `geo_reach_pct` deferred (open item 8.1).
- Live per-campaign addressable-pool signal EXISTS but experimental:
  `dw-main-bronze.external.camperbid_prod__hhst_v3__campaign_bucket_population`
  (per-campaign population by intent band, refreshed hourly; v2 `campaign_qualified_rate` is DEAD
  since 2025-11). Candidate v2 upgrade for a real targetable-pool %.

## 6. Open Items / Follow-ups
1. **[EMPIRICAL — blocked on BQ auth]** Does a per-location population/HH table exist so
   `geo_reach_pct` is exact? If not, fall back to a location-type retention heuristic and flag it.
2. **[CALIBRATION]** τ_geo threshold + restriction_level cutoffs — "needs tests" per ticket
   (>=80% retained = flagship was the strawman). Calibrate against a one-time IP-pool study on a
   sample; keep the live view cheap (expression + gate + fangorn tier only).
3. **Naming sign-off** — `mm_engine` values, `restriction_level` values, `is_flagship_mm` semantics.
4. **Where it lives / materialization** — SQLMesh view vs scheduled table; grain confirmed campaign.
5. **Model/version scheme** — how to keep `mm_engine` robust when the next generation (post-Fangorn) ships.

## 6b. Team feedback artifact (2026-07-22)
Shareable spec page published to Confluence (TAR space, child of the MM Taxonomy page):
**https://mntn.atlassian.net/wiki/spaces/TAR/pages/3712811252** — "AUDI-1083: MNTN Matched
Classifying View — Spec for Feedback". Draft-for-feedback banner, problem framing, the mm_class
taxonomy grid, column dictionary, live sample rows, join snippets, and 6 open questions teed up for
inline comments (flagship def, gate rule, geo threshold, keyword-only-as-MM, grain, materialization).
Source HTML: `artifacts/audi_1083_confluence_spec.html`.

## 7. Data Documentation Updates
(pending — will land taxonomy/gate/geo confirmations into data_knowledge.md as the view is built)
