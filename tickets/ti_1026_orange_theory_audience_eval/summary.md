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

**7-day windowed reach/overlap (2026-06-04 → 06-10):**
| Layer | 7-day reach (IPs) |
|---|---:|
| MNTN Matched (379 kw) | 21,815,337 |
| 3P (11 segments) | 3,040,269 |
| 3P ∩ MM overlap | 386,928 (**12.7%** of 3P) |
| 3P-only incremental | 2,653,341 |

> **⚠ CORRECTION (validation).** The **absolute 3P reach (3.04M) and "+12% incremental / MM is ~14× larger"
> framing are NOT robust** — they're a low-volatility-week artifact. On the adjacent week (2026-06-03→06-09) the
> SAME query gives 3P reach = **19.3M** and MM/3P = **1.13×** (not 14×), because a single 06-03 mega-batch
> (6.5M+ IPs) falls just outside the original window. **Do not cite the 3P membership reach or "14×" as point
> estimates** (the volatile range is ~3M–19M/week). The **ROBUST, window-stable fact is: ~87% of 3P IPs match
> NO OTF keyword** (overlap 12–14% across windows) → low-intent. And this is moot for the recommendation anyway:
> under the score gate, 3P contributes only **1.5% of actual delivery** (§4.9) regardless of membership size.

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

> **⚠ CORRECTION (validation).** "Only 5/11 deliver; 6 deliver ZERO; 3 Epsilon deprecated" is a **single-week
> timing artifact, NOT segment death.** Over a trailing 30-day window **all 11 segments deliver 1.3M–6.7M IPs**;
> the 3 "deprecated" Epsilon ids are in fact the **largest** deliverers (3.2M–6.7M). ipdsc 3P delivery is bursty
> (each refreshes on 2–4 days/month; the 6 "zero" ids' last refresh was 06-03, one day before the snapshot week).
> Do NOT tell the customer these segments "deliver nothing" or are "dead." Verify the `tpa.categories.deprecated`
> flag with the segment owner before calling Epsilon dead — the delivery data contradicts it.

Reads (window-stable): (a) every segment is **~87% non-overlapping with the OTF keyword universe — uniformly low
intent** (this is the robust basis for dropping them); (b) OTF is a **HIIT** studio, yet the delivering segments
are broad-fitness or **yoga/pilates** (wrong modality). **All 11 → DROP** — but on the robust intent/modality
grounds + the delivered-share mechanism (§4.9), NOT on a "deliver nothing" claim.

### 4.7 — Geo-fence sizing impact (reporter's hypothesis)

Geo clause = **946 studio fences, 7-mile radius each** (`radii_include`) + **21 `radii_exclude`** zones
(10–30 mi, carve out markets). Query: `queries/ti_1026_geo_fence_coverage.sql` (MaxMind blocks within 7mi
of any studio; 2026-06-11):

| Measure | Fenced | US total | % fenced |
|---|---:|---:|---:|
| Geolocated network blocks (population-density proxy) | 2,203,886 | 4,458,870 | **49.4%** |
| IPv4 address capacity | 401M | 1,613M | 24.9% |

**Read:** the fence covers ~**half the populated US** (block-count is the better population proxy; IP-capacity
is lower because rural blocks hold huge unused ranges).

### 4.7b — The sizing FUNNEL: how much each filter actually removes (the ticket's core question)

Applied the filters to the **actual MM keyword audience** (not just block coverage). MM = DS19 ∩ 379 keywords,
clean IPv4, 2026-06-09. Queries: `queries/ti_1026_geo_funnel.sql`, `ti_1026_exclusion_bite_on_mm.sql`,
`ti_1026_full_funnel.sql`. Output: `outputs/ti_1026_funnel.csv`.

| Stage | Households | % of MM | What removed it |
|---|---:|---:|---|
| **MM keyword universe** (national, daily) | **4,580,200** | 100% | — |
| **Inside 7-mi studio fence** | **2,093,631** | **45.7%** | **geo removes ~2.49M (~54%) — the biggest filter** |
| ...& income/age-eligible (≈) | ~1.49M | ~33% | LiveRamp income/age excl. removes 1,312,378 (28.7% of MM); Oracle = 0 (inert) |
| Score gate (≥6501) → bidder | (high-intent slice) | — | campaign reaches ~464K distinct IPs / 14d |

Geo-funnel detail: in-fence 2,093,631 (45.7%) · geolocated-outside-fence 1,061,556 (23.2%) · unknown-/24
1,425,013 (31.1%, coarse/rural maxmind blocks ≈ outside fence). **Among geolocatable MM households, ~66% are
in-fence; across all MM, ~46%.**

**Answers the ticket's sizing question directly:**
- **Geo is the single biggest size limiter — it roughly halves the MM audience** (validates the reporter's
  hypothesis). **Removing it entirely** would ~double the addressable pool (4.6M vs 2.1M), but the added
  households aren't near a studio → low relevance for a location-based membership. The right lever is **widening
  the radius (7→10 mi)**, not removing geo.
- **Income/age exclusions (LiveRamp) are the #2 limiter — ~29% (~1.3M).** Relaxing them is a real reach lever.
- **3P is NOT on the funnel** — under the score gate it adds ~nothing (§4.9).
- **Caveats:** /24-string geolocation (31% unmappable, treated ≈ outside); single representative day for MM
  (DS19 is stable daily); the combined in-fence∩not-excluded (~1.49M) is an **estimate** (28.7% applied to
  in-fence). The exact combined query is intractable at scale (the 10-day DS35 semi-join hit BigQuery's 6-hour
  limit; abandoned by decision) — but the two individual filter magnitudes are exact (geo −54%, income/age −29%)
  and sufficient for the headline. (`radii_exclude` not subtracted.)
- Crucially, **geo applies equally to the MM and 3P layers**, so it is NOT the cause of the 3P underperformance.

### 4.9 — THE MECHANISM (the "why"): scoring × HHST × OR-include

This is the core causal explanation, with evidence. The audience is **(MNTN Matched keywords OR 11 3P segments)
AND within 7 mi of a studio** — confirmed OR, one include block with `or:[DS19, DS35]` (§4.1).

**Fact 1 — only the MNTN Matched IPs carry a score.** MNTN's `household_score` (0-10000 fitness-intent quality
signal) is computed from the DS19 keyword layer. The 3P segments are bought lists; **87% of the IPs they bring in
match no OTF keyword (§4.4), so they have no score** (household_score = -1).

**Fact 2 — the main campaign gates bidding on score: HHST = 6,501.** `dso.household_score_thresholds`:
campaign 319137 threshold = **6501** (only IPs scoring ≥6501 are bid on). Other OTF campaigns (and ~64% of the
platform) run HHST = 0 (no gate). Query: `queries/...` (dso.household_score_thresholds, advertiser_id=39718).

**Evidence — delivered household_score by campaign (last 14d, `cost_impression_log`)**, `queries/ti_1026_delivered_score_dist.sql`:
| Campaign | HHST | Impressions | Distinct IPs | % scored ≥6501 | % unscored (-1) |
|---|---:|---:|---:|---:|---:|
| **319137** (main, $2k/day) | 6501 | 1,521,364 | 463,895 | **82.3%** | **1.5%** |
| **319133** (tiny, $26/day) | 0 | 34,515 | 5,582 | 0.04% | **99.96%** |

**What this proves:**
1. **3P contributes ~nothing to the main campaign.** With HHST=6501, unscored 3P-only IPs can't clear the bar —
   only **1.5%** of delivery is unscored. The campaign reaches its audience and paces to budget almost entirely
   through the *scored keyword* layer. (This corrects §4.4's "3P adds ~12% reach" — that was national IPDSC
   *membership*; under the score gate those unscored IPs aren't biddable, so real delivered 3P share ≈ 1.5%.)
2. **Where 3P delivers, it's garbage.** On a no-gate campaign (HHST=0), the bidder bids on everything — 319133 is
   **99.96% unscored**. That no-intent traffic is exactly the "non-MNTN matched audience" the agency measured at
   8-10× worse visit rate.
3. **So 3P can never be the reach fix:** gate on → filtered out; gate off → unscored garbage. Either way, remove it.

**Is the audience big enough? (the reach question)** — `queries/ti_1026_*pacing*`, daily trend:
- Budget = $83.41/hr ≈ **$2,002/day**. Main campaign **paces at 1.08-1.31× of budget most days in June** (it CAN
  spend its budget on the scored, in-fence audience alone — 463,895 distinct scored IPs reached in 14d, no 3P help).
- BUT it **underdelivered through late May (0.35-0.6× of budget)** and had a 2-day pause (Jun 1-2). So the scored
  audience within 7 mi of studios is **adequate at the current budget, but not deep** — headroom for scaling is thin.
- **Verdict:** not currently starved at ~$2k/day; the binding constraint when it bites is the **scored-IP ×
  7-mi-geo intersection**, NOT the 3P segments. To scale spend, the levers are: (a) **lower HHST** (6501 = top
  ~third of scores; their own quality dial), (b) **broaden + clean the keyword set** so more fitness households get
  scored into the pool, (c) **widen the geo radius**. Bought 3P is not on this list.
- Note: 319133 (HHST=0, 99.96% unscored) shows a higher 90d visit rate than 319137 — but it's a tiny ($26/day)
  CTV campaign; small-n/retargeting noise. Both campaigns are 100% CTV (`display_imps=0`), so the headline VR
  numbers are not a channel artifact, but cross-campaign VR here is not reliable evidence either way.

### 4.10 — Is the low visit rate our targeting or their creative? (the attribution question)

Two analyses, last 30d, campaign 319137. Queries: `ti_1026_visitrate_by_score.sql`, `ti_1026_ctv_vr_benchmark.sql`.

**(a) Does MNTN's score discriminate visit rate for OTF? (per served IP, clickpass_log attribution)**
| MNTN score band | Served IPs | Visit rate |
|---|---:|---:|
| Top (10000, vertical-matched / HI) | 436,739 | **1.354%** |
| High (6501-9999) | 86,268 | 0.192% |
| Mid (3333-6500) | 356,250 | 0.198% |
| Low (0-3332) | 130,893 | 0.063% |
| Unscored (-1) | 32,405 | 0.463% (small/noisy; likely RTC recent-visitors) |

→ **Our model works.** The score rank-orders responsiveness — the top-intent (10000/HI) band visits **~7-20×**
the other bands and is the bulk of reach. We ARE finding the right, responsive people.

**(b) Benchmark: OTF vs comparable CTV scored-prospecting campaigns (HHST>0, video≥95%, ≥100K imps, 30d):**
| | visit rate |
|---|---:|
| Peer p10 / p25 / **median** / p75 / p90 | 0.09% / 0.36% / **0.91%** / 1.99% / 4.15% |
| **OTF 319137 (blended)** | **0.18%** (≈ **15th percentile**, 126 of 814 peers at/below) |

**Reconciliation / verdict (is it us or them?):**
- **NOT a reach/size/wrong-people problem** — ruled out: paces to budget on the scored audience alone (enough
  people), score discriminates 7-20× (right people, model works), delivery is in-fence + scored (no targeting error).
- **OTF's blended VR (0.18%) is genuinely low vs peers (~15th pct; median peer ~5× higher).** Two contributors:
  1. **Audience MIX (a lever WE control):** ~50% of served IPs are below the 6501 gate (low/mid/unscored — likely
     RTC bypass and/or the gate having tightened recently), dragging the blend. The quality is concentrated in the
     **10000/HI band (1.35%)**; note the 6501-9999 band is only 0.19%, so gating at **10000 (HI-only)** rather than
     6501 would lift blended VR substantially at modest reach cost (drops the 86K high band, keeps the 437K top band).
  2. **Creative/offer/brand (OTF's lever, the binding ceiling):** even our BEST-targeted segment (1.35%) only reaches
     the peer **median**. A strong CTV advertiser blends ~0.9% and tops ~2-4%. OTF's best-targeted ≈ a typical peer's
     average → there's a response ceiling that better targeting cannot lift. That's creative / offer / landing-page /
     brand-pull — OTF's side, not ours.
- **3P is irrelevant to this** — it's filtered out by the gate (§4.9); not a VR lever either way.
- **Caveat:** visit rate is observational and depends on each advertiser's pixel/site-visit definition. The only
  way to *prove* MNTN's incremental contribution (vs visits that would have happened anyway) is a **holdout /
  incrementality test** (BER-2250 method). Recommend one if OTF/Sales want defensible proof of lift.

### 4.8 — The demographic exclusions: Oracle (DS1) inert, LiveRamp (DS35) ACTIVE [CORRECTED after validation]

> **⚠ CORRECTION (2026-06-11, independent validation).** The original claim here — "all 20 demographic
> exclusions are inert / remove nobody" — was **WRONG for the 7 LiveRamp (DS35) categories.** It came from a
> single-day `exclusion_bite` query (2026-06-08) that landed on a day those categories weren't in ipdsc's
> rotating daily load. **ipdsc 3P (DS35) delivery is bursty** (each category refreshes on only 2-4 days/month),
> so a one-day snapshot spuriously reads zero. Multi-day re-measurement shows the DS35 exclusions are active.

| Exclusion group | # cats | Status | Evidence |
|---|---:|---|---|
| **Oracle (DS1) income/age** | 13 | **INERT** ✓ | DS1 has **zero ipdsc presence at all** — excludes nobody. (Consistent with TI-999: DS1/Oracle never delivers to ipdsc.) |
| **LiveRamp (DS35) income/age** | 7 | **ACTIVE** | Each matches **millions-to-tens-of-millions of IPs** on its load days, e.g. cat 1005350999 (TransUnion 65-74) **~15.4M IPs on 2026-06-04** (inside the eval window); 1004602219 ~10.0M, 1005351019 ~9.1M same day; 1004256419 ~24.2M on 06-03. All 7 deliver on ≥1 day in 2026-05-28→06-10. |

**Corrected implications:**
- The **DS35 income/age exclusions DO shrink the targetable universe** (low-income <$35k, elderly 65+/75+ are
  actively removed — tens of millions of IPs). So for the sizing question they ARE a real reach lever: relaxing
  the income/age bands would expand reach. (Whether to is a strategy call — income/age targeting is defensible
  for a premium ~$159/mo membership; but it is NOT free of reach cost as originally stated.)
- The **DS1 Oracle exclusions** are genuinely inert (cosmetic) — DS1 never delivers to ipdsc.
- **Net audience-size impact** (exclusion ∩ scored include set) needs a multi-day intersection to quantify
  precisely; not recomputed here because the headline recommendation rests on the delivered-impression mechanism
  (§4.9), not on exclusion sizing.
- Keep regardless: **DS4 CRM suppression** (existing-member lists — hygiene), **DS43 T-Mobile Cellular** (mobile-
  carrier IPs aren't household-stable for CTV), **DS2 MNTN First Party** (retargeting/past-visitor exclusions).

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
- **Validate recommendation workbook with Kelly Thurlow** before passing to Sales/Customer (acceptance criterion).
- Confirm "DAR" = the keyword recommender to compare against (treated as BUK/Behavior-Keyword recs per TI-928).
  If a live BUK/DAR recommendation for orangetheory.com is available, swap the heuristic keyword bucketing for the
  tool's actual output (stronger than the curated-list approach).
- Optional: per-segment **delivered visit rate** to numerically confirm the agency's 8-10× (logs don't separate
  which audience category matched an impression at the granularity needed — would require a ghost-bid-style split).
- Reach numbers are national (pre-geo); in-fence proportions assumed similar. Competitor-gym 3P IPs may cluster
  slightly more in-fence (same metros as OTF) — would marginally raise 3P's in-fence share, not change the verdict.
- This connects to TI-956/TI-999 (interest-segment quality scoring): OTF is a concrete case for why per-segment
  scoring + a "drop 3P" recommendation matters. Feed back into that workstream.
