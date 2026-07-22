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

## 5. Proposed view schema (campaign grain)
```
-- keys / grain
campaign_id, campaign_group_id, advertiser_id, objective_id, funnel_level, expression_updated_at
-- Axis A: engine
mm_engine, mm_config, has_ds13, has_ds19, has_ds38, has_ds46, has_mm
-- scoring / gate / generation
hhst_current, hhst_gated, fangorn_tier            -- from tpa.fangorn_advertiser_inclusion
-- Axis B: restriction components
geo_reach_pct, geo_narrowest_type, has_geo_narrow_incl, has_geo_excl,
has_3p_incl, three_p_semantics (OR/AND/mixed/none), and_3p_narrowed, or_3p_additive,
has_1p_incl, and_1p_narrowed, crm_excl_hygiene
-- rollups
restriction_level, is_flagship_mm
```
Grain = campaign (matches `audience_segments`, latest targeted segment rn=1 by update_time).
`campaign_group_id` is an attribute for rollup; a group-level verdict is a GROUP BY (e.g. all-
flagship vs mixed) — provide as a companion view, don't bake into the base grain.

### 4a. Locked decisions (2026-07-22)
- **Quantification = exposed components**, NOT a single composite %. `geo_reach_pct` is the one
  exact number; AND-narrowing + gate are binary flags; `restriction_level` is a rule over them.
- **AND-3P / AND-1P = binary flags only in v1.** No magnitude estimate (can't cheaply size
  |MM∩3P|/|MM| without IP intersection; segment-reach independence approximation too shaky).
- **FLAGSHIP and FANGORN decoupled:** `mm_engine` carries the generation (has the `fangorn_v2`
  value); `is_flagship_mm` is generation-agnostic ("well-configured MM"). Filter
  `is_flagship_mm AND mm_engine='fangorn_v2'` for flagship-Fangorn, or `... AND != 'fangorn_v2'`
  for flagship-legacy.

### 4b. Locked naming
- `mm_engine`: `non_mm` | `mm_core` | `peak_performance_v1` | `fangorn_v2`
- `mm_config`: `keyword_only` | `vertical_only` | `vertical_plus_keyword` (× engine)
- `restriction_level` (what got carved, most-severe wins): `none` | `geo` | `audience` | `geo+audience`
- `is_flagship_mm` (bool) = `mm_engine != 'non_mm' AND hhst_gated AND restriction_level = 'none'`
- geo "narrow" = positive include at location_type ∈ {DMA, state, city, ZIP} or a `geo_radii` clause;
  country-level (US=237) / no-geo = default (`geo_reach_pct = 1.0`).

### 4c. Draft view SQL
`queries/audi_1083_mm_classifier_view.sql` — full campaign-grain SELECT ready to materialize
(SQLMesh view). Two blockers before it runs clean: BQ re-auth + confirm the per-location HH table
(open item 8.1).

## 6. Open Items / Follow-ups
1. **[EMPIRICAL — blocked on BQ auth]** Does a per-location population/HH table exist so
   `geo_reach_pct` is exact? If not, fall back to a location-type retention heuristic and flag it.
2. **[CALIBRATION]** τ_geo threshold + restriction_level cutoffs — "needs tests" per ticket
   (>=80% retained = flagship was the strawman). Calibrate against a one-time IP-pool study on a
   sample; keep the live view cheap (expression + gate + fangorn tier only).
3. **Naming sign-off** — `mm_engine` values, `restriction_level` values, `is_flagship_mm` semantics.
4. **Where it lives / materialization** — SQLMesh view vs scheduled table; grain confirmed campaign.
5. **Model/version scheme** — how to keep `mm_engine` robust when the next generation (post-Fangorn) ships.

## 7. Data Documentation Updates
(pending — will land taxonomy/gate/geo confirmations into data_knowledge.md as the view is built)
