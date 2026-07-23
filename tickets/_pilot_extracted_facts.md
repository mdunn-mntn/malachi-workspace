# Pilot Extracted Facts — Review Queue

Verified `delta_facts` mined from the pilot tickets, grouped by target `home_doc`.
Each fact carries its source ticket + source_line. Review, then promote into the
named knowledge doc (or reject) — nothing here is auto-merged.

---

## → knowledge/data_knowledge.md

### 1. LiveRamp per-dscid segment quality spread (~350x CVR) with flat spend
**Source:** `ti_999_interest_segment_sizing`
**Fact:** Per-dscid LiveRamp segment CVR spread is ~350x top-vs-bottom quintile
(Q5 avg 0.140% vs Q1 0.0004%) and ~274x in cost-per-conversion ($53 vs $14,525),
while spend is essentially flat (~13-15%) across all five quintiles — ~$1.67M/30d
(~$20M/yr) flows to the worst 201 LiveRamp dscids at near-zero conversions. Buyers
have no quality signal to differentiate segments.
**Source line:** Pass 10 — "The spread is 350x in CVR (top vs bottom quintile) and
274x in cost-per-conversion. The spend distribution is essentially FLAT across
quintiles." and "$1.67M / 30d (~$20M annualized) currently flows to the worst 201
LiveRamp dscids".

### 2. 3P-vs-3P provider IP overlap — diminishing incremental reach
**Source:** `ti_999_interest_segment_sizing`
**Fact:** 3P-vs-3P provider IP overlap (single-day ipdsc 2026-05-26): total 3P
universe 147.9M IPs; LiveRamp 104.0M (60.5M exclusive), ShareThis 64.5M (32.4M
exclusive), Dstillery 32.4M (only 9.4M exclusive). Dstillery is the most-redundant
provider — 64.3% of its IPs are also in LiveRamp; ShareThis adds the most
incremental reach beyond LiveRamp (32.4M ShareThis-only). Only 7.4M IPs are in all
three. Layering a second 3P provider has steeply diminishing incremental reach.
**Source line:** Finding 13 — "64.3% of Dstillery IPs are also in LiveRamp...
ShareThis adds the most incremental reach beyond LiveRamp — 32.4M ShareThis-only
IPs... Only 7.4M IPs are in all three".

### 3. MM (gated) vs 3P — CPV / IVR / ROAS by vertical
**Source:** `audi_1141_mm_vs_3p_by_vertical`
**Fact:** AUDI-1141: median CPV MM-gated $9.13 vs 3P $37.18 (~4x); MM-all $12.52,
MM-no-gate $21.84, MM-restricted $19.02 (advertiser-weighted median, 180d). Median
IVR MM-gated 0.46% vs 3P 0.07% (~6.6x); median ROAS MM-gated 0.92 vs 3P 0.40.
**Source line:** `| MM (gated) | ... | 0.46% | $9.13 | 0.92 | ... | 3P | 438 | 1,134 | 0.07% | $37.18 | 0.40 |`

### 4. Two-lens MM framing — MM(all) vs MM(gated)
**Source:** `audi_1141_mm_vs_3p_by_vertical`
**Fact:** AUDI-1141 two-lens framing: MM (all) blends every MM campaign = realistic
average (IVR 0.28%, n 2,560 adv); MM (gated) = best-configured subset (IVR 0.46%,
n 1,262 adv); MM (all) still beats 3P ~4x IVR / ~3x CPV / ~2.3x ROAS.
**Source line:** "MM (all) blends every MM campaign = the realistic average; MM
(gated) is the best-configured subset. Both beat 3P clearly ... MM (all) still beats
3P ~4x IVR / ~3x CPV / ~2.3x ROAS."

### 5. The MM "gate" = score threshold >0, not the scoring model
**Source:** `audi_1141_mm_vs_3p_by_vertical`
**Fact:** AUDI-1141: an MM campaign only bids on model-scored high-intent IPs when
score threshold >0; advertisers run it at 0 for Max Reach, short flights lowering it
for deliverability, or an exhausted over-narrowed HI pool, which bypasses the model
and bids broadly like a 3P segment. The gate is the threshold setting, not the
scoring model.
**Source line:** "an MM campaign only bids on model-scored high-intent IPs when the
score threshold is >0. Some advertisers run it at 0 (Max Reach; many short flights
that lower it for deliverability; or an over-narrowed HI pool exhausted quickly)"

### 6. 8 MNTN sales verticals + rollup crosswalk
**Source:** `audi_1141_mm_vs_3p_by_vertical`
**Fact:** AUDI-1141: 8 MNTN sales verticals = ProServ, Education, Retail/Ecom,
Gaming/Entertainment, Telco & Tech, Restaurants/Dining, CPG & Health, Auto Travel &
Hospitality; vertical rollup via advertiser->fpa_advertiser_verticals type=0 parent
(37 canonical parents) -> 8 buckets crosswalk (interim, needs RevOps sign-off; 3
orphans -> Other/Unmapped).
**Source line:** "8 sales verticals: ProServ, Education, Retail/Ecom,
Gaming/Entertainment, Telco & Tech, Restaurants/Dining, CPG & Health, Auto Travel &
Hospitality."

### 7. ROAS is directional only — use median, never mean
**Source:** `audi_1141_mm_vs_3p_by_vertical`
**Fact:** AUDI-1141: ROAS is directional only (prospecting/last-touch, revenue
concentrates in excluded retargeting, some cells carry pixel artifacts e.g. one
ProServ advertiser >800x); use median never mean. Median ROAS MM-gated 0.92 vs 3P
0.40.
**Source line:** "ROAS is directional only: prospecting/last-touch, revenue
concentrates in retargeting (excluded), and some cells carry pixel artifacts (one
ProServ advertiser shows >800x). Use median, never mean."

---

## → knowledge/data_catalog.md

### 3. tpa.categories.updated_date is taxonomy-freshness, not IP-membership freshness
**Source:** `ti_999_interest_segment_sizing`
**Fact:** `tpa.categories.updated_date` reflects when the category METADATA (name,
parent, deprecation flag) last changed — NOT when the category's IP membership last
refreshed; so the "100% stale" signal for ShareThis/Dstillery is a taxonomy-freshness
signal only, and those providers may still be delivering fresh IP data daily.
**Source line:** Finding 2 caveat — "tpa.categories.updated_date reflects when the
category metadata (name, parent, deprecation flag) was last changed — not when the
category's IP membership last refreshed."
