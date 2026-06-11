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

### 4.4 — Reach & overlap (IPDSC): MNTN Matched vs 3P

Method: distinct-IP reach from `ipdsc__v1` (UNNEST `data_source_category_ids.list`), literal `dt`
(partition prune). MM = DS19 (379 keywords); 3P = DS35 (11 segments). Queries: `queries/ti_1026_reach_overlap_7d.sql`, `queries/ti_1026_per_segment_reach_7d.sql`.

**Single-day (2026-06-10) — caught a low 3P day, understates 3P:**
| Layer | Reach (IPs) |
|---|---:|
| MNTN Matched (379 kw) | 4,996,020 |
| 3P (11 segments) | 118,690 |
| 3P ∩ MM overlap | 3,328 (2.8% of 3P) |
| 3P-only incremental | 115,362 |

**3P delivery is highly volatile day-to-day** (key data-quality finding). Same 11 segments, raw IP rows:
| dt | segments present | rows |
|---|---|---|
| 2026-06-06 | none | 0 |
| 2026-06-08 | Stirista Fitness (1006088981) **2,095,515**; Adsquare Yoga Pilates (1011732871) 767,784 | — |
| 2026-06-10 | Adsquare Yoga Pilates only | 118,690 |

→ On any given day most of the 11 3P segments deliver **nothing**, and the ones that do swing 10–20×.
This alone makes them unreliable for targeting.

**7-day windowed reach/overlap (2026-06-04 → 06-10) — HEADLINE numbers:**
| Layer | 7-day reach (IPs) |
|---|---:|
| MNTN Matched (379 kw) | **21,815,337** |
| 3P (11 segments) | **3,040,269** |
| 3P ∩ MM overlap | 386,928 (**12.7%** of 3P) |
| 3P-only incremental | **2,653,341** (**+12.2%** on MM) |

3P adds ~12% incremental weekly reach, but **87% of 3P IPs match NO OTF keyword** — the low-intent cohort
behind the agency's 8–10× worse visit rate. MM keyword layer is **14× larger** than the 3P layer.

### 4.5 — 3P segment quality scoring (per-segment 7-day reach + redundancy)

`outputs/ti_1026_interest_segments_eval.csv`, `queries/ti_1026_per_segment_reach_7d.sql`. Only **5 of 11**
segments delivered any IPs over the week; **2 broad segments carry 99% of all 3P reach**:

| Cat | Provider / Segment | Modality | 7d reach | % match OTF kw |
|---|---|---|---:|---:|
| 1006088981 | Stirista — Frequent Purchasers > Fitness | broad | 2,136,648 | 12.9% |
| 1011732871 | Adsquare — Yoga Pilates app-usage | off (HIIT≠yoga) | 887,558 | 12.3% |
| 1009019881 | 180byTwo — Club Pilates visitors | off | 20,571 | 13.0% |
| 1004396389 | 180byTwo — Corepower Yoga visitors | off | 13,663 | 13.3% |
| 1006035411 | PlaceIQ — F45 Recent visitors | competitor | 978 | 14.2% |
| 1009501941 / 1011707151 / 1011707271 | Commerce Signals — Yoga / Pilates / Fitness | — | 0 | — |
| 1000997189 / 1000999629 / 1000999639 | Epsilon — Gym Customers / Spenders | broad | 0 (DEPRECATED) | — |

Reads: (a) every segment is ~87% non-overlapping with the OTF keyword universe — uniformly low intent;
(b) OTF is a **HIIT** studio, yet the only delivering segments are broad-fitness or **yoga/pilates** (wrong
modality); (c) the closest competitor signal (F45, also HIIT) delivers ~nothing (978 IPs). **All 11 → DROP.**

### 4.7 — Geo-fence sizing impact (reporter's hypothesis)

Geo clause = **946 studio fences, 7-mile radius each** (`radii_include`) + **21 `radii_exclude`** zones
(10–30 mi, carve out markets). Query: `queries/ti_1026_geo_fence_coverage.sql` (MaxMind blocks within 7mi
of any studio; 2026-06-11):

| Measure | Fenced | US total | % fenced |
|---|---:|---:|---:|
| Geolocated network blocks (population-density proxy) | 2,203,886 | 4,458,870 | **49.4%** |
| IPv4 address capacity | 401M | 1,613M | 24.9% |

**Read:** the fence covers ~**half the populated US** (block-count is the better population proxy; IP-capacity
is lower because rural blocks hold huge unused ranges). Geo is a real constraint (it removes ~50–75% of the
country) but is **not the bottleneck** — with 5.0M national MM IPs and the fence retaining roughly half of the
populated US, the in-fence audience is millions of IPs, far more than the ~7.3M impressions / 90d the main
campaign delivers. Crucially, **geo applies equally to the MM and 3P layers**, so it is NOT the cause of the
3P underperformance the agency flagged. (`radii_exclude` not subtracted here — would shave coverage slightly.)

### 4.8 — The demographic exclusions are inert (zero delivery)

Query: `queries/ti_1026_exclusion_bite.sql` (2026-06-08, a high-3P day). Include audience (MM ∪ 3P) =
**7,596,517 IPs** nationally. But:
| Exclusion group | # cats | IPs in ipdsc | Removed from audience |
|---|---:|---:|---:|
| Oracle (DS1) income/age | 13 | **0** | **0** |
| LiveRamp (DS35) income/age | 7 | **0** | **0** |

The 20 demographic exclude segments (low-income <$35k, elderly 65+/75+, very young 18-20) have **zero ipdsc
volume → they exclude nobody.** They look like aggressive targeting but are no-ops (consistent with TI-999:
Oracle DS1 has no ipdsc delivery; same is now confirmed for the Equifax/Experian/TransUnion demographic bands).
**Implication:** (a) they are NOT shrinking the audience, so relaxing them recovers no size; (b) they aren't
doing what the advertiser thinks (not actually filtering by income/age) — cosmetic cleanup candidates.
The exclusions that DO bite: **DS4 CRM suppression** (existing-member lists, advertiser-uploaded — works, keep),
**DS43 T-Mobile Cellular** (applied at bid time, not via ipdsc — appropriate household-stability hygiene for
CTV; keep), **DS2 MNTN First Party** (retargeting/past-visitor exclusions — keep).

### 4.5 — 3P segment quality scoring — see §4.5 above (per-segment 7d reach)

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
These dilute the MNTN-Matched portion's relevance and likely drag visit rate.

Conservative classification (`artifacts/classify_keywords.py` → `outputs/ti_1026_keyword_classification.csv`,
default = KEEP, only high-confidence flags): **285 keep (75%), 51 drop off-target (13%), 43 review over-broad (11%)**.
So ~1 in 4 keywords (94/379) is off-target or over-broad — a real curation gap (the list reads like a generic
consumer-products template, not a BUK/DAR recommendation for orangetheory.com). The exact keep/drop line is for
Kelly/Sales to finalize; the workbook surfaces the candidates.

## 5. Solution — recommendations

Deliverable workbook: `artifacts/ti_1026_orange_theory_audience_recommendations.xlsx`
(tabs: Recommendations · Interest Segments · Keywords · Geo & Exclusions · Methodology). Validate with
**Kelly Thurlow**, then pass to Sales/Customer.

**Recommendations (priority order):**
1. **Remove all 11 3P interest segments.** They add ~12% incremental weekly reach but it's the lowest-intent
   slice (87% match no OTF keyword), it's volatile (6 of 11 deliver nothing; the rest swing 10-20×/day), and
   it's off-modality (delivering segments are broad-fitness or yoga/pilates; OTF is HIIT). This is the cohort
   the agency measured at 8-10× worse visit rate. Dropping it raises visit rate at a ~12% reach cost.
2. **Prune the 51 off-target keywords; review the 43 over-broad terms** (of 379). The keyword list reads like a
   generic consumer-products template (Above Ground Pools, Antifreeze, Beer Mugs, CPUs, Motorcycle Lighting…),
   not a BUK/DAR recommendation for orangetheory.com. Tightening it lifts relevance with little reach loss.
3. **Grow size via the MNTN Matched keyword layer, not bought 3P.** MM reaches 21.8M IPs/week (14× the 3P
   layer) and is the quality engine here. Add on-target HIIT/strength/cardio/recovery keywords to replace the
   pruned off-target ones — net-neutral-to-expanding reach at similar intent.
4. **Geo is not the bottleneck and the demographic exclusions are inert — don't chase either for size.** The
   946×7-mi fence already covers ~half the populated US; the 20 income/age exclude segments remove nobody.
   Keep CRM-suppression, T-Mobile-cellular, and MNTN-FP exclusions (legitimate hygiene). Optionally widen geo
   radius (7→10 mi) only for specifically under-delivering markets.

## 6. Questions Answered
- **Q:** Is the audience-sizing issue caused by geo filtering (reporter's hypothesis)?
  **A:** No. The 946-studio × 7-mi fence covers ~49% of US network blocks (~half the populated US) and applies
  equally to MM and 3P. With 21.8M MM IPs/week nationally, the in-fence audience is millions — not starved.
- **Q:** Why do the "non-MNTN matched" (3P) segments perform 8-10× worse?
  **A:** They're low-intent and off-modality: 87% of 3P IPs match no OTF keyword, and the only delivering
  segments are broad fitness-buyer / yoga-pilates lists (OTF is HIIT). The bidder isn't ranking these by any
  fitness-intent signal — they're essentially untargeted reach.
- **Q:** Can OTF drop most 3P segments without hurting size?
  **A:** Mostly yes. 6 of 11 already deliver nothing; the 5 that do add ~12% weekly reach (the worst slice).
  Recover/grow size by improving the MNTN Matched keyword set, not by re-adding bought 3P.
- **Q:** Are the income/age exclusions trimming reach?
  **A:** No — the 20 Oracle/LiveRamp demographic exclude segments have zero ipdsc delivery; they exclude nobody.
  They're cosmetic (and not actually filtering by income/age as intended).

## 7. Data Documentation Updates
- `audience.audiences` expression schema for `expression_type_id=2`: `{interest:{include:[{or:[{data_source_id,cats[]}]}],exclude:[...]}, geo:{include,exclude,radii_include:[{lat,long,radius,unit}],radii_exclude}}`. Distinct from the `audience_segments` AST parsed in TI-999.
- DS19 = "MNTN Matched" (keyword categories live in tpa.categories, ~232k cats, range 100001–979321; near-zero deprecated). DS43 = "MNTN ISP Type" (e.g. 1001 = T-Mobile Cellular). DS1 = Oracle, DS2 = MNTN First Party.
- `ipdsc__v1.data_source_category_ids` is `RECORD{ list: REPEATED RECORD{ element: INT } }` — UNNEST `data_source_category_ids.list` and read `.element`.

## 8. Open Items / Follow-ups
- Validate recommendation with **Kelly Thurlow** before passing to Sales/Customer (acceptance criterion).
- Confirm "DAR" = the keyword recommender to compare against (treated as BUK/Behavior-Keyword recs per TI-928).
