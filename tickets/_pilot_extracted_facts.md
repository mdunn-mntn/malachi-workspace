# Pilot Extracted Facts — Review Queue

Verified `delta_facts` mined from the 5 pilot tickets, grouped by target `home_doc`.
Each fact carries its source ticket + source_line. Review, then promote into the
named knowledge doc (or reject) — nothing here is auto-merged.

Pilot tickets: `audi_1083_mm_classifying_view`, `audi_1141_mm_vs_3p_by_vertical`,
`ti_390_mmv3_performance` (no delta facts), `ti_221_pre_post_analysis`,
`ti_999_interest_segment_sizing`.

---

## → knowledge/data_knowledge.md

### 1. campaign_status_id 8/9 not caught by the `deleted` boolean
**Source:** `audi_1083_mm_classifying_view`
**Fact:** `campaign_status_id` 8/9 (Deleted / Legacy Archived) are NOT caught by the
`deleted` boolean; filter `campaign_status_id NOT IN (8,9)` to exclude archived
campaigns. `campaign_status_id=3` = Live.
**Source line:** "Gotcha found: `campaign_status_id` 8/9 (Deleted / Legacy Archived)
are NOT caught by the `deleted` boolean — added `campaign_status_id NOT IN (8,9)` to
the base view"

### 2. Vertical-only configs cap at PP; HI (10000) requires the keyword layer (DS19)
**Source:** `audi_1083_mm_classifying_view`
**Fact:** DS13-only campaigns are PP-capped (household_score 83.9% at exactly 8000,
0% at 10000), and DS46-only are also PP-capped (0.1% above 8000, 0% at 10000);
vertical-only configs cap at PP, and HI (10000) requires the keyword layer (DS19).
**Source line:** "RTC-excluded 30d delivered `household_score`: DS13-only = 83.9% at
exactly 8000, **0% at 10000**; DS46-only = 0.1% above 8000, 0% at 10000. **Clean
rule: HI (10000) needs the keyword layer (DS19); PP (8000) needs the vertical
anchor.**"

### 3. AUDI-1141 vertical rollup crosswalk (interim, needs RevOps sign-off)
**Source:** `audi_1141_mm_vs_3p_by_vertical`
**Fact:** AUDI-1141 vertical rollup uses an interim crosswalk: advertiser ->
fpa_advertiser_verticals type=0 parent (37 canonical parents) -> 8 sales buckets
(ProServ, Education, Retail/Ecom, Gaming/Entertainment, Telco & Tech,
Restaurants/Dining, CPG & Health, Auto Travel & Hospitality); 3 orphans (News &
Politics, Non-Profits, Holidays & Events) -> Other/Unmapped. Crosswalk is interim
and needs RevOps sign-off.
**Source line:** "advertiser -> fpa_advertiser_verticals type=0 parent (37 canonical
parents) -> 8 sales buckets via a crosswalk in the SQL. 3 orphans (News & Politics,
Non-Profits, Holidays & Events) -> Other/Unmapped."

### 4. AUDI-1141 advertiser-weighted median ROAS (directional only)
**Source:** `audi_1141_mm_vs_3p_by_vertical`
**Fact:** AUDI-1141 advertiser-weighted median ROAS: MM (gated) 0.92, MM (all) 0.92,
MM restricted 0.94, MM (no gate) 0.56, 3P 0.40. ROAS is directional only
(prospecting/last-touch, revenue concentrates in excluded retargeting, pixel
artifacts up to >800x); use median never mean.
**Source line:** "| MM (gated) | 1,262 | 2,248 | 0.46% | $9.13 | 0.92 | ... | 3P |
438 | 1,134 | 0.07% | $37.18 | 0.40 |"

### 5. AUDI-1141 cohort size after classification
**Source:** `audi_1141_mm_vs_3p_by_vertical`
**Fact:** AUDI-1141 cohort after classification: 8,202 campaigns after dropping the
Neither group; 7,138 in the scored MM/3P groups.
**Source line:** "8,202 campaigns after dropping the \"Neither\" group; 7,138 in the
scored MM/3P groups."

### 6. cost_impression_log + ui_visits join pattern for pre/post
**Source:** `ti_221_pre_post_analysis`
**Fact:** The `cost_impression_log` + `ui_visits` join pattern was confirmed for
pre/post analysis.
**Source line:** "Confirmed `cost_impression_log` + `ui_visits` join pattern for
pre/post analysis"

### 7. TI-270 is a related pre/post analysis (Jaguar release)
**Source:** `ti_221_pre_post_analysis`
**Fact:** TI-270 is a related pre/post analysis for the Jaguar release (a separate
feature from TI-221).
**Source line:** "TI-270 is a related pre/post analysis for the Jaguar release
(separate feature)."

### 8. TI-221 vertical classification change likely tied to TI-033
**Source:** `ti_221_pre_post_analysis`
**Fact:** The vertical classification change analyzed in TI-221 was likely tied to
TI-033.
**Source line:** "After a vertical classification update (likely tied to TI-033)"

### 9. 3P inclusion clause is dead weight at current spend for most MM+3P-OR campaigns
**Source:** `ti_999_interest_segment_sizing`
**Fact:** Only ~17.7% of MM+3P-OR-include campaigns (76 campaigns, 26.3% of that
cohort's spend) actually overflow into 3P-added unscored IPs; ~76.3% (328 campaigns,
70.5% of spend) run below the MM ceiling so the 3P inclusion clause is effectively
dead weight at current spend.
**Source line:** Pass 6 — "of the 609 MM+3P_incl_only campaigns, only 17.7% (76
campaigns) are actually overflowing into 3P-added unscored IPs... 76.3% (328
campaigns) are running below MM ceiling — their 3P inclusion clause... is effectively
dead weight at current spend levels."

### 10. MM-ceiling exhaustion is the confirmed mechanistic model
**Source:** `ti_999_interest_segment_sizing`
**Fact:** MM-ceiling exhaustion is the confirmed mechanistic model: FICO's MM-scored
delivery is flat (~60-72K imps/day) regardless of campaign budget; a 4x-larger MM+3P
campaign produced the same scored count with the extra $127K spend going to 236K
unscored 3P-added impressions.
**Source line:** Pass 5 — "FICO's MM-scored delivery is essentially flat (~60-72K
imps/day) regardless of campaign size... extra $127K of spend went to 236K unscored
3P-added impressions, not to incremental scored MM delivery" — "Conclusion:
MM-ceiling exhaustion + bidder-pacing-overflow is the right mechanistic model.
Hypothesis confirmed."

### 11. Per-LiveRamp-dscid CVR spread ~350x with flat spend
**Source:** `ti_999_interest_segment_sizing`
**Fact:** Per-LiveRamp-dscid CVR spread is ~350x (top vs bottom quintile) and ~274x
in cost-per-conversion, while spend is essentially FLAT across quintiles — buyers
spend nearly identical amounts on best and worst segments with no quality signal to
differentiate.
**Source line:** Pass 10 — "The spread is 350x in CVR (top vs bottom quintile) and
274x in cost-per-conversion. The spend distribution is essentially FLAT across
quintiles. Buyers spend nearly identical amounts on the best and worst segments —
they have no quality signal to differentiate."

### 12. Prospecting-only filter over-broad — CRM referenced in negative suppression clauses
**Source:** `ti_999_interest_segment_sizing`
**Fact:** The prospecting-only filter (exclude any campaign referencing DS4 CRM / DS8
IP List / DS47 CRM-IDG) was over-broad: it dropped 296 MM-prospecting campaigns
($1.88M/30d) that reference CRM only in NEGATIVE suppression clauses; a
polarity-aware filter should exclude only positive 1P clauses.
**Source line:** Pass 2 — "the prospecting-only filter (drop any campaign referencing
DS4/8/47) was over-broad. It removed 296 campaigns / $1.88M / 30d of MM-prospecting
that negatively references CRM... A polarity-aware retargeting filter would only
exclude campaigns with 1P in positive clauses."

### 13. No-3P prospecting converts 2.1x better than fresh-LiveRamp prospecting
**Source:** `ti_999_interest_segment_sizing`
**Fact:** In the prospecting-only cut, no-3P prospecting converts 2.1x better than
fresh-LiveRamp prospecting (0.126% vs 0.059% conv rate); the fresh+stale 3P mix
bucket is worst (0.034%).
**Source line:** Finding 11 — "No-3P prospecting converts 2.1x better than
fresh-LiveRamp prospecting (0.126% vs 0.059%)" and "Mix is still worst (0.034%) —
confirms the layering-hurts pattern in a clean prospecting frame."

---

## → knowledge/data_catalog.md

### 1. silver.dso.household_score_thresholds is one row per campaign
**Source:** `audi_1083_mm_classifying_view`
**Fact:** `dw-main-silver.dso.household_score_thresholds` has exactly one row per
campaign (32,467 campaigns; 10,647 gated); join on `campaign_id`. `campaign_group_id`
/ `advertiser_id` are denormalized attributes, not a separate grain.
**Source line:** "HHST gate: `dw-main-silver.dso.household_score_thresholds` =
**exactly one row per campaign** (32,467 campaigns; 10,647 gated; join on
`campaign_id`)"

### 2. geo.location_data has no household/population column
**Source:** `audi_1083_mm_classifying_view`
**Fact:** `geo.location_data` has NO household/population column, so a `geo_reach_pct`
percentage cannot be computed exactly from it. The `geos` JSON shape is
`{"op":"any","value":{"location_ids":[...]}}` under an and/or/not tree.
**Source line:** "geo.location_data has **NO** household/pop column → `geo_reach_pct`
deferred (open item 8.1). ... `geos` JSON shape CONFIRMED:
`{\"op\":\"any\",\"value\":{\"location_ids\":[...]}}` under an and/or/not tree"

### 3. 3P vendor category staleness — ShareThis/Dstillery 100% stale, LiveRamp fresh
**Source:** `ti_999_interest_segment_sizing`
**Fact:** ShareThis (DS17) and Dstillery (DS18) have 100% of active categories with
`updated_date` >2 years old, while LiveRamp (DS35) has 99.6% of active categories
updated in the last 30 days. Caveat: `tpa.categories.updated_date` reflects
category-metadata change, not when IP membership last refreshed.
**Source line:** Finding 2 — "ShareThis and Dstillery are 100% stale. Every one of
their 5,153 active categories has updated_date > 2 years ago" and "LiveRamp is the
clean case. 99.6% of active LiveRamp categories were updated in the last 30 days";
caveat "tpa.categories.updated_date reflects when the category metadata... was last
changed — not when the category's IP membership last refreshed."

---

## → no home doc (ticket-local / infra note)

### 1. AUDI-1083 MM classifier productionized as two daily FULL SQLMesh models
**Source:** `audi_1083_mm_classifying_view`
**Fact:** The AUDI-1083 MM classifier is productionized as two daily FULL SQLMesh
models (`mm_campaign_classifier.sql`, `mm_campaign_classifier_by_group.sql`) under
`models/dw-main-silver/audience/` in SteelHouse/sqlmesh, owner
`targeting-infrastructure`, on local feature branch `audi-1083-mm-classifier` (not
pushed, no PR).
**Source line:** "Two models under `models/dw-main-silver/audience/`:
**`mm_campaign_classifier.sql`** — FULL, `cron '@daily'` ... owner
`targeting-infrastructure`. ... STATUS: LEFT LOCAL — branch not pushed, no PR"
