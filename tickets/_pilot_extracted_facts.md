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
