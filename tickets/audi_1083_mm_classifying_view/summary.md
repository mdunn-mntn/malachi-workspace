---
doc_type: ticket
title: "AUDI-1083: MNTN Matched classifying view — grade MM vs raw DS presence"
status: done
date: 2026-07-22
summary: "Durable campaign-grain view: what MM engine + how restricted, so 'MM' means flagship not DS-present"
result: "Shipped LIVE in prod (2026-07-24): dw-main-silver.audience.mm_campaign_classifier (+ _by_group), a daily campaign-grain classifier exposing mm_class (mmv1/mmv2/mmv3 + Fangorn cells), tiers_reachable, restriction_level, is_unmodified_mm, is_flagship. LEFT JOIN on campaign_id/campaign_group_id to grade MM engine + restriction instead of DS13/19/46 presence. 14,512 active Stage 1 campaigns classified (MM 43.5%, flagship 10.8%). SQLMesh PR SteelHouse/sqlmesh#1245 merged."
keywords: ["mm classifier", "mm_class", "is_flagship", "is_unmodified_mm", "restriction_level", "tiers_reachable", "mmv1 mmv2 mmv3", "fangorn engine", "hhst_gated", "geo_reach_pct", "campaign classifier view", "audi_1083"]
question: "Can one durable view classify + grade every MM campaign by engine + restriction, not DS-presence?"
framing_state: locked
---

## TL;DR

**Q:** Build a durable campaign-grain view that grades MM engine + restriction so "MM" means flagship, not raw DS13/19/46 presence.

**A:** Designed a durable campaign-grain classifying view (mm_class engine + restriction flags) so "MM" means flagship not DS-presence; draft validated on live data, SQLMesh model authored on a local branch (not pushed).

**How:** Assembled prior components (polarity-aware AST 3P parser, TI-1037 2x3 taxonomy, HHST gate, Fangorn tier table) into two orthogonal axes: Axis A mm_engine/mm_class from DS-leaf presence, Axis B restriction components (geo binary, AND-3P/AND-1P flags, hhst_gated) rolled up to restriction_level + is_unmodified_mm + is_flagship booleans. Validated end-to-end on live data, then authored as a daily FULL SQLMesh model.

**Tables:** `tpa_fangorn_advertiser_inclusion`, `household_score_thresholds`, `household_score_threshold_archives`, `audience_segments`, `campaigns`, `camperbid_prod__hhst_v3__campaign_bucket_population`, `geo.location_data`

**Learned:**
- `campaign_status_id` 8/9 (Deleted / Legacy Archived) are NOT caught by the deleted boolean; add `campaign_status_id NOT IN (8,9)` so archived campaigns don't pollute rollups (campaign_status_id=3 = Live).
- Definitive headline (active Stage 1, delivered 45d, $39.5M): MM 72.4% / unmodified MM 34.6% / flagship 6.9% of prospecting spend; of every MM-labelled dollar, under half is unmodified.
- MM = 43.5% of campaigns by COUNT vs 72.4% by SPEND (MM campaigns spend bigger); within MM only ~20% unmodified / 80% modified, geo is the #1 modifier.

**Reuse when:**
- How do I classify whether a campaign is real MM vs DS-present
- MM engine version per campaign (mmv1/mmv2/mmv3)
- is_flagship / is_unmodified_mm definition
- how restricted is an MM campaign
- AUDI-1083 classifier view / mm_class taxonomy

# AUDI-1083: MNTN Matched classifying view

**Jira:** https://mntn.atlassian.net/browse/AUDI-1083
**Status:** Done (2026-07-24) — live in prod, resolution Done
**Date Started:** 2026-07-22
**Assignee:** Malachi

---

## 0. Framing
The agreed question, why it matters, and how we plan to answer it. Locked 2026-07-24 via /frame.
- **Question (the unknown):** Can one durable campaign-grain view identify and grade every MM campaign by scoring engine (flagship/Fangorn vs legacy) and degree of targetable-pool restriction, replacing DS13/19/46-presence filtering that misclassifies >74% of modified MM campaigns?
- **Goal (why / the decision):** One authoritative answer to "is this a flagship MM campaign, and how modified?", so every downstream stops rolling its own DS-presence filter. Consumers: reusable analyst infra (agreed w/ Alyson), MM-vs-3P and Fangorn rollout measurement, MM-adoption/engine-mix exec reporting, and incrementality (BER-2250) cohorting. Velocity-multiplier feeding the #1 incrementality priority.
- **Objective (done-when):** A daily campaign-grain SQLMesh view (mm_engine/mm_class plus restriction flags rolled to restriction_level, is_flagship, is_unmodified_mm) merged and running in prod, refreshing daily, with classifications matching a hand-checked validation set. Not closed while the model sits authored on a branch.
- **Approach (how):** Assemble existing parts (polarity-aware AST 3P parser, TI-1037 2x3 MM taxonomy, HHST gate, `tpa_fangorn_advertiser_inclusion` tier table) into two orthogonal axes: Axis A (mm_engine/mm_class from DS-leaf presence), Axis B (geo, AND-3P, AND-1P, hhst_gated rolled to restriction_level). Daily FULL SQLMesh model, validated on live prospecting, then merged and deployed via the airflow-ti path (Ryan wires deps; never push main directly). Thresholds and naming already adopted (2026-07-23); the remaining unknown is prod deploy plus daily-refresh validation.
- **What would change the answer:** If per-campaign restriction/retained-% can't be computed reliably, or the tier thresholds don't separate flagship from modified on live data, the grade axis is noise. Ship only the broad-MM (DS-presence) boolean and drop flagship/tier grading.

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
expression_updated_at, mm_class, tiers_reachable, mm_engine_rank, has_ds13, has_ds19, has_ds38, has_ds46, has_mm
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
  DS19-only reaches HI·MI·MaxReach (NOT PP); vertical-only (DS46-only AND DS13-only) = PP·MI, NO HI
  (see §4h — HI needs keywords, empirically verified; corrects the "DS13-only = HI·PP" first draft);
  DS19+anchor = all four; non_mm = unscored. Captured in new column `tiers_reachable`. (Earlier session claim
  "DS19-only = unscored/max-reach" was wrong — corrected in data_knowledge.md.)
- Added column **`tiers_reachable`** (per taxonomy §3). Confluence spec page updated to v2 with the
  canonical definition + tier profiles + corrected mechanics.

### 4e. Open questions resolved (2026-07-22) — Confluence spec now at v3
- **Gate NOT required for `is_unmodified_mm`** (user: HHST flips daily for pacing, an operational
  lever not a structural modification; exposed separately as `hhst_gated`). Removing it moved the
  headline only 32.5% → **33.3%** unmodified (98% of clean-config MM is gated anyway); flagship
  6.7% → **6.8%**. Updated headline: MM-labelled **70.9%** / unmodified **33.3%** / flagship **6.8%**.
- **Geo = binary in v1** (any positive sub-country include → `restriction_level='geo'`; exclusions
  don't count; `geo_narrowest_type` exposed for a zip/city-only bar). Exact `geo_reach_pct` = v2
  (no clean per-location HH table; candidate = camperbid `campaign_bucket_population`).
- **Grain = campaign** + companion group-level rollup (all-flagship vs mixed, spend-weighted modal class).
- **Materialization = daily SQLMesh `FULL` model** + `snapshot_date`; feature branch in airflow-ti,
  scheduling wired by pipeline owner (Ryan); exact dataset TBD.
- Added **`expression_updated_at`** (classification freshness). `snapshot_date` to be added at materialization.

### 4f. Scope fix — funnel_level = 1 only (2026-07-22, user + verified)
Only Stage 1 (funnel_level=1) campaigns carry the DS audience expression. Verified: delivered
funnel_level 2/3/4 campaigns are **100% non_mm / zero DS leaves** (funnel=1 = 59% has_mm). Stage 2/3
are subsets of Stage 1 and inherit its audience. View now filters `funnel_level = 1`; classifying
Stage 2/3 directly would mislabel ~10K campaigns as non_mm. **Stage 2/3 join path = `campaign_group_id`
→ group rollup** (86.5% of Stage 2/3 share a group with a Stage 1 campaign). Objective_id NOT filtered
(kept exposed). Headline unchanged (calibration already used funnel_level=1). Group-rollup companion
appended to the SQL. Confluence spec → v4.

### 4h. Active filter + tier-mechanics correction (2026-07-22)
- **Active filter:** view now `campaign_status_id = 3` (Live). Headline (active + delivered 45d,
  $39.5M): MM **72.4%** / unmodified **34.6%** / flagship **6.9%** (was 70.9/33.3/6.8 pre-active-filter).
- **DS13-only is PP-capped, NOT HI (empirically verified, corrects the taxonomy).** RTC-excluded 30d
  delivered `household_score`: DS13-only = 83.9% at exactly 8000, **0% at 10000**; DS46-only = 0.1%
  above 8000, 0% at 10000. **Clean rule: HI (10000) needs the keyword layer (DS19); PP (8000) needs
  the vertical anchor.** Both vertical-only configs cap at PP. Canonical taxonomy page 3691708511 §3
  ("DS13-only → HI+PP") and data_knowledge.md's old "PP v1 only → HI 10000" were WRONG — that HI was
  RTC firing. `tiers_reachable`: vertical_only_legacy fixed 'HI·PP' → 'PP·MI (no HI)'. Verify query:
  `queries/audi_1083_ds13_vs_ds46_score_bands.sql`. Confluence spec → v5. Canonical taxonomy page
  3691708511 corrected → v4 (fixed §1 DS13 component cell, §3 DS13-only row, §4 HI rule).
- **DS19-only VERIFIED reaches HI (2026-07-22, 14d RTC-excl):** 69% HI band / ~1% PP / 7% MI / ~22%
  MR-unscored — so `mm_keywords_only = HI·MI·MaxReach (no PP)` is correct. Controls: DS19+DS46 47% HI /
  8.5% PP; DS19+DS13 51% HI / 41% PP (v1 categorical). Spec voice rewritten plain → v6.
- **AUTHORITATIVE MODEL (Ryan Kleck `audience_intent` DAG page; user provided 2026-07-22) — corrects my
  "independent axes" framing.** PROSPECTING scores (the ones we use): **HI (10K) = in Vertical (DS13)
  AND in Keywords (DS19) — BOTH**; PP (8K) = vertical, no keyword; MI = bucket, not vertical; Unscored
  (prev Max Reach) = outside bucket/vertical but INSIDE keywords. NOT independent — HI is the
  intersection. **DS19-only still reaches HI because the Vertical is the ADVERTISER's, always fed into
  the score; Keywords require the DS19 leaf.** That asymmetry is the whole thing. The DS19-only split
  maps 1:1 onto Ryan's four rows (69/1/7/22 = HI/PP/MI/unscored), confirming the model. DAG:
  `audience_intent` in airflow-ti, daily ~3-7 AM UTC, writes prospecting_intent (per campaign, used)
  + advertiser_intent (per advertiser, HI=vertical-only, not used). Spec rule → v8; taxonomy HI clause
  clarified → v5; data_knowledge + data_catalog updated.
- **CAMPAIGN-COUNT DISTRIBUTION (active Stage 1, 14,475 campaigns; `queries/audi_1083_distribution.sql`,
  `outputs/audi_1083_distribution.csv`):** by config — non-MM 56.5% / keyword-only 24.8% / both 14.3% /
  vertical-only 4.3%. **MM = 43.5% by COUNT vs 72.4% by SPEND (MM campaigns spend bigger).** Within MM:
  57% keyword-only, 33% both, 10% vertical-only. Modified vs not — 27.3% national/un-narrowed, 72.7%
  narrowed (geo 59.9% / audience 3.9% / geo+audience 8.9%); within MM only **20% unmodified / 80%
  modified**; flagship (DS19+DS46, unmodified) = 191 = 3% of MM. Added as spec Distribution section → v9.

### 4g. NULL / orphan investigation (2026-07-22) — no live gap
The group-inheritance NULLs (13.5% all-time) are **100% dormant**: of DELIVERED (45d) Stage 2/3
campaigns, **100% (9,794 / $12.2M) resolve to a clean Stage 1** — zero NULLs. The 39,628 orphans
(no Stage 1 ever in group) never delivered: 64% status 9 Legacy Archived, 30% status 8 Deleted, 5%
Ready-drafts, rest paused. **Gotcha found:** `campaign_status_id` 8/9 (Deleted / Legacy Archived) are
NOT caught by the `deleted` boolean — added `campaign_status_id NOT IN (8,9)` to the base view so
archived Stage 1s don't pollute the group rollup. Headline unchanged (70.9/33.3/6.8; -10 dead camps).

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

## 6c. Team naming adopted (Alyson, 2026-07-22; IMPLEMENTED 2026-07-23)
Team refers to MM campaign types by **MM VERSION**. Per Alyson's answers, renamed 3 `mm_class` values
IN PLACE (kept the other 3 structural): `mm_keywords_only`→**mmv2** (DS19-only) · `mm_classic`→**mmv3**
(DS19+DS13) · `vertical_only_legacy`→**mmv1 (created < ~Sept 2024) else mmv3**. `mm_flagship_fangorn`
(DS19+DS46), `fangorn_vertical_only` (DS46-only), `non_mm` unchanged — Fangorn = "updated DS13" (DS46),
its cells keep structural names (user OK).
- **Create-date cutoff:** mmv1 shipped ~Dec 2023-Jan 2024, mmv3 ~Sept 2024 (AP holds exact date). Verified
  empirically: DS13-only creation ramps through 2024, **collapses Sept→Oct 2024 (15→3)**, trickle from late
  2025 — a clean ~2024-09-01 boundary. View uses `IF(campaign_created < TIMESTAMP('2024-09-01'),'mmv1','mmv3')`
  (constant MMV3_CUTOFF, flagged approximate).
- **Structural consequence handled:** `mmv3` now spans DS19+DS13 AND DS13-only-post-cutoff (different tiers),
  so `tiers_reachable` is now computed from the raw DS flags, NOT `mm_class`. Exposed new col `campaign_created`.
- **Verified (2026-07-23):** mmv2 3,594 (all reach HI) · mm_flagship_fangorn 1,761 · fangorn_vertical_only 411
  (PP-capped) · **mmv3 393 = 312 DS19+DS13 (reach HI) + 81 DS13-only post-cutoff (PP-capped)** · **mmv1 134
  (latest_created 2024-08-30, all pre-cutoff, PP-capped)** · non_mm 8,182. Old vertical_only_legacy 215 = 134
  mmv1 + 81 mmv3. Spec page → v10 (label rename + naming note + campaign_created). Query `queries/audi_1083_distribution.sql`.

## 6d. Materialization — SQLMesh model authored (2026-07-23)
Productionized as a SQLMesh model (BQ read-only access = no direct CREATE VIEW; airflow-ti = Spark, wrong
tool). Repo `SteelHouse/sqlmesh`, cloned `~/Developer/work/mntn/sqlmesh`, feature branch
**`audi-1083-mm-classifier`** (committed locally, NOT pushed). Two models under
`models/dw-main-silver/audience/`:
- **`mm_campaign_classifier.sql`** — FULL, `cron '@daily'`, `gateway silver`, `grain campaign_id`,
  owner `targeting-infrastructure`. The validated view SQL verbatim (3 JS UDFs as pre-statements, per the
  `conversion_signal_impressions.sql` precedent). Home chosen = `audience` schema (alongside the existing
  `campaign_segment_history.sql`).
- **`mm_campaign_classifier_by_group.sql`** — FULL group rollup, `grain campaign_group_id` (Stage 2/3 join path).
- **Validated:** sqlglot 30.13 bigquery parse clean (4 + 1 statements, JS triple-quote OK); SELECT logic
  already BQ-verified via the distribution/verification runs.
- **DECISIONS (user, 2026-07-23):** owner = **`targeting-infrastructure`** (true AUDI ownership; NB alerts
  route to #monitor-test — revisit if real paging wanted); scope = active Stage 1 only; `geo_reach_pct` NULL (v2).
- **PLAN RAN + VALIDATED IN DEV (2026-07-24).** Env setup: `python3 -m venv .venv && pip install -r requirements.txt`
  (sqlmesh 0.0.1.dev4506), `export SSL_CERT_FILE=$(python -m certifi)`. `sqlmesh info` → warehouse + state both
  connect. **Gotcha fixed:** `sqlmesh plan` crashed in the repo prod-access guard because ADC quota project
  `mntn-coredw-prod` has Cloud Identity API disabled → `gcloud auth application-default set-quota-project dw-main-bronze`
  (Cloud Identity IS enabled there). `sqlmesh plan dev_malachi --no-prompts --auto-apply` then backfilled both models:
  `audience__dev_malachi.mm_campaign_classifier` (14.5k rows, 14s) + `..._by_group` (13.6k, 4s). Validated: dev-table
  distribution matches (mmv2 all-HI, mmv3 split HI/PP, flagship counts) on live data.
- **PUSHED + PR OPEN 2026-07-24: https://github.com/SteelHouse/sqlmesh/pull/1245** (branch `audi-1083-mm-classifier`).
  Request Ryan Kleck's review → CI → merge → first prod run = next daily cron. Ran the required pre-PR `sqlmesh plan` gate.
- **CI GREEN 2026-07-24 (two fixes):** (1) "Check SQL Formatting" (`sqlmesh format --check`) failed → ran `sqlmesh format`
  (cosmetic: JS UDFs `r"""…"""`→single-quote, lowercase keywords, relocate comments; logic identical), pushed eecf7e8.
  (2) `verify-impact` then failed ("Missing deployable impact snapshot … not applied in any environment for this tree")
  because formatting changed the model fingerprint AFTER my earlier plan → re-ran `sqlmesh plan dev_malachi --auto-apply`
  on the formatted code (snapshot now in state) + `gh run rerun --failed` (no new commit). **All checks pass** (sqlmesh-checks
  4m53s, dataform, TruffleHog, setup-env). LESSON (now in data_knowledge): format BEFORE plan. PR desc also tightened to
  pass Terse Standard `--kind pr`. Node.js-20 annotations = repo CI infra, ignore.
- **MERGED + LIVE IN PROD 2026-07-24.** On merge the clean-name VIEWs were created and the FULL models backfilled:
  `dw-main-silver.audience.mm_campaign_classifier` (14,512 rows) + `..._by_group` are queryable now, refresh daily.
  Prod distribution matches dev exactly (mmv2 3,615 / flagship_fangorn 1,562 / mmv3 642 / fangorn_vertical_only 354 /
  mmv1 141 / non_mm 8,198). **Done-when bar (live in prod + matches hand-check) MET.** Remaining follow-up: confirm exact
  mmv3 cutoff date with AP (constant `2024-09-01` approx); owner alerts route to #monitor-test (revisit if paging wanted).

## 6b. Team feedback artifact (2026-07-22)
Shareable spec page published to Confluence (TAR space, child of the MM Taxonomy page):
**https://mntn.atlassian.net/wiki/spaces/TAR/pages/3712811252** — "AUDI-1083: MNTN Matched
Classifying View — Spec for Feedback". Draft-for-feedback banner, problem framing, the mm_class
taxonomy grid, column dictionary, live sample rows, join snippets, and 6 open questions teed up for
inline comments (flagship def, gate rule, geo threshold, keyword-only-as-MM, grain, materialization).
Source HTML: `artifacts/audi_1083_confluence_spec.html`.

## 7. Data Documentation Updates
(pending — will land taxonomy/gate/geo confirmations into data_knowledge.md as the view is built)
