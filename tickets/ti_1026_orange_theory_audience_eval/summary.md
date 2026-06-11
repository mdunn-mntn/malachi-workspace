# TI-1026: Orange Theory National — Audience Evaluation

**Jira:** https://mntn.atlassian.net/browse/TI-1026
**Status:** In Progress
**Date Started:** 2026-06-11
**Date Completed:**
**Assignee:** Malachi
**Reporter:** Alex Knorr | **Parent:** TI-602 (Q2 Tech Debt) | **Precedent:** TI-928 (ElevenLabs eval)

---

## Current state (read this first if you're a new session)

Sales escalation: Orange Theory National (advertiser **39718**) keeps hitting **audience-sizing** issues, and the agency (Ryan Olivieri, PurposeBrands) reports the **non-MNTN-matched 3P segments perform 8–10x worse on visit rate** and wants to back out of most of them — without hurting reach.

Goal (from ticket): either (a) maintain size with fewer 3P interest segments, or (b) increase size.
Deliverable: recommendation file (Excel/Sheet) like the ElevenLabs eval (TI-928), validated with Kelly Thurlow, passed to Sales/Customer.

**Target audience:** `audience_id = 34668` — "MNTN Matched | New Year's 3P Segments Copy 01" (the live/most-recently-updated prospecting audience; 9 other audiences exist for this advertiser, mostly older copies/tests + retargeting).

---

## 1. Introduction

Orange Theory Fitness (OTF) National runs a geo-fenced national prospecting campaign across its studio
footprint. The audience blends MNTN Matched keywords with bought 3P interest segments, fenced to studio
locations. Sales says it keeps running into sizing limits; the agency separately flagged that the 3P
("non-MNTN matched") segments deliver far worse visit rates than the MNTN Matched portion.

## 2. The Problem

- **Sizing:** audience too small / hard to scale (reporter suspects geo filtering).
- **Performance:** agency email (Ryan Olivieri, 2025-12-29) — "visit rates [of the new non-MNTN matched
  audiences] are 8-10x lower than the MNTN matched segments… needed to back out of the vast majority of
  these audiences… want to expand the audience without hurting visit rate."
- **Tension:** dropping 3P segments could shrink an already-tight audience. Need to show which 3P segments
  can be dropped at **no/low reach cost** (because they're redundant with MNTN Matched or dead), and where
  real size can be recovered.

## 3. Plan of Action

1. ✅ Pull + parse the audience.audiences expression for 34668 (`artifacts/parse_expression.py`).
2. ✅ Resolve every data-source / category name (tpa.categories).
3. ✅ Map audience → campaigns (audience_segments) + pull campaign performance.
4. ⏳ Reach + overlap analysis (IPDSC): MNTN-Matched reach vs 3P reach vs 3P-only incremental reach,
   per-segment redundancy. Drives the "drop at no size cost" recommendation.
5. ⏳ Quality-score the 11 included + 7 excluded 3P segments (Alex's framework / activity+overlap proxy).
6. ⏳ Keyword (DS19) evaluation vs BUK/DAR — flag off-target keywords.
7. ⏳ Geo-fence + exclusion sizing impact (946 studio radii, T-Mobile cellular excl, income/age excl).
8. ⏳ Build recommendation spreadsheet (ElevenLabs format) + validate with Kelly Thurlow.

## 4. Investigation & Findings

### 4.1 — Audience 34668 decomposition (the expression)

Expression (`audience.audiences`, expression_type_id=2, 67KB) decomposes to:

**INCLUDE** (logical OR):
- **DS19 "MNTN Matched"** — **379 keyword categories** (the MNTN Matched / Mountain-Mesh keyword layer)
- **OR DS35 "LiveRamp IP"** — **11 bought 3P interest segments** (the "non-MNTN matched" audiences)

**AND geo** (`geo.radii_include`): **946 studio geo-fences**, each a **7-mile radius** around a lat/long
(OTF studio locations). Plus 21 `radii_exclude`. No state/DMA include/exclude — purely radius-based.

**EXCLUDE** (interest):
| DS | Name | # cats | What's excluded |
|---:|---|---:|---|
| 1 | Oracle | 13 | Low HHI (<$30k bands), very young (18-20), elderly (65+, Silent Gen, Elders 80+) |
| 35 | LiveRamp IP | 7 | Low income (Equifax/Experian/TransUnion <$35k), ages 65+/75+ |
| 4 | CRM | 2 | Customer **suppression lists** (30Dec24, 18Dec25) — hygiene, exclude existing members |
| 2 | MNTN First Party | 3 | MNTN retargeting/conversion exclusions (past visitors/converters) |
| 43 | MNTN ISP Type | 1 | **T-Mobile Cellular** — excludes T-Mobile cellular IPs |

Files: `outputs/ti_1026_audience_34668_expression.json`, `outputs/ti_1026_expression_categories_long.csv`,
`artifacts/parse_expression.py`.

### 4.2 — The 11 included 3P interest segments (DS35 LiveRamp) — the "non-MNTN matched" audiences

| # | Category ID | Provider / Segment | Deprecated? |
|--:|---|---|:--:|
| 1 | 1000997189 | Epsilon > Gym & Fitness Purchasers > **Gym Customers** | **DEPRECATED** |
| 2 | 1000999629 | Epsilon > Gym & Fitness Purchasers > Spend > **Moderate Spenders** | **DEPRECATED** |
| 3 | 1000999639 | Epsilon > Gym & Fitness Purchasers > Spend > **Heavy Spenders** | **DEPRECATED** |
| 4 | 1004396389 | 180byTwo > Location Visitors > Gym > **Corepower Yoga** (competitor) | no |
| 5 | 1006035411 | PlaceIQ > Gyms > **F45 > Recent** (competitor) | no |
| 6 | 1006088981 | Stirista > Frequent Purchasers > Fitness | no |
| 7 | 1009019881 | 180byTwo > Location Visitors > Gym > **Club Pilates** (competitor) | no |
| 8 | 1009501941 | Commerce Signals > Wellness > Fitness > Yoga | no |
| 9 | 1011707151 | Commerce Signals > Wellness > Fitness > Pilates | no |
| 10 | 1011707271 | Commerce Signals > Wellness > Fitness (general) | no |
| 11 | 1011732871 | Adsquare > App Usage > Sports > Yoga Pilates | no |

**Finding: 3 of 11 included 3P segments are DEPRECATED** in the catalog (the Epsilon "Gym Customers /
Moderate / Heavy Spenders" trio) — dead/stale data delivering little or nothing. The other 8 are broad
fitness-purchaser / competitor-gym-visitor segments (Corepower, F45, Club Pilates) — relevant by theme but
broad, and likely heavily overlapping the MNTN Matched keyword layer.

### 4.3 — Campaign performance (90d, campaigns using audience 34668)

6 campaigns map to audience 34668 (319132–319137); only 2 are active in the last 90 days:

| Campaign | Impressions | Unique visitors | **Visit rate** | Conv | Spend | Note |
|---|---:|---:|---:|---:|---:|---|
| 319137 | 7,337,438 | 18,478 | **0.252%** | 225 | $109,316 | main prospecting campaign |
| 319133 | 143,175 | 2,831 | **1.977%** | 61 | $2,087 | small, high VR placement |
| 319136 | 205 | 0 | 0% | 0 | $3 | effectively off |
| 319132/4/5 | — | — | — | — | — | no delivery in 90d |

Visit rate is audience-level (both layers blended) — campaign data can't isolate the 3P-vs-MNTN-Matched
split on its own; that's what the IPDSC reach/overlap (§4.4) and any per-segment delivery work address.

### 4.4 — Reach & overlap (IPDSC) — PENDING

_(MNTN-Matched reach vs 3P reach vs 3P-only incremental reach; per-segment redundancy vs the keyword layer.)_

### 4.5 — 3P segment quality scoring — PENDING

### 4.6 — Keyword (DS19) evaluation vs BUK/DAR — PRELIMINARY

The 379 MNTN Matched keywords are largely fitness/wellness-relevant (Fitness*, Yoga*, Pilates*, Protein*,
Athletic*, Workout*, Strength/Cardio, Nutrition, Recovery, Wellness…) but contain a clear tail of
**off-target keywords**: "Above Ground Pools", "Abrasives", "Antifreeze", "Arcade And Gaming Machines",
"Barcode Reader", "Ballasts", "Bathtubs", "Beer Mugs", "Adhesive Tapes", "Advent Calendars", "Analog
Watches", "Coffee Grinders", "Chef Apparel", "Compact Suv", "Conveyor Belt Products", "Cpus", "Dental And
Medical Adhesives", "Guitar Parts And Accessories", "Ignition", "Motorcycle Lighting And Electrical",
"Montessori", "Outdoor Surveillance Equipment", "Sway Bars", "Suspension Kits", "Transformers" (auto),
"Townhouse", "Strap-on Vibrators", "Spelling And Reading Programs" — plus over-broad single-word terms
("Class", "Power", "Silver", "Experience", "Mirrors", "Pillows", "Socks", "Towels", "Benches", "Hooks").
These dilute the MNTN-Matched portion's relevance and likely drag visit rate. Full list:
`outputs/ti_1026_ds19_keywords.csv`. DAR/BUK comparison pending.

## 5. Solution
_(pending)_

## 6. Questions Answered
_(pending)_

## 7. Data Documentation Updates
- `audience.audiences` expression schema for `expression_type_id=2`: `{interest:{include:[{or:[{data_source_id,cats[]}]}],exclude:[...]}, geo:{include,exclude,radii_include:[{lat,long,radius,unit}],radii_exclude}}`. Distinct from the `audience_segments` AST parsed in TI-999.
- DS19 = "MNTN Matched" (keyword categories live in tpa.categories, ~232k cats, range 100001–979321; near-zero deprecated). DS43 = "MNTN ISP Type" (e.g. 1001 = T-Mobile Cellular). DS1 = Oracle, DS2 = MNTN First Party.
- `ipdsc__v1.data_source_category_ids` is `RECORD{ list: REPEATED RECORD{ element: INT } }` — UNNEST `data_source_category_ids.list` and read `.element`.

## 8. Open Items / Follow-ups
- Validate recommendation with **Kelly Thurlow** before passing to Sales/Customer (acceptance criterion).
- Confirm "DAR" = the keyword recommender to compare against (treated as BUK/Behavior-Keyword recs per TI-928).
