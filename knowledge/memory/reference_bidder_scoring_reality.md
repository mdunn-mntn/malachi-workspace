---
name: reference-bidder-scoring-reality
description: "MNTN bidder semantics — three score fields (household_score, advertiser_household_score, realtime_conquest_score), plus polarity-aware clause semantics (inclusion = OR-additive, exclusion = AND-NOT). Per TI-999 Finding 15 empirical 2026-05-28."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 790f6279-052b-404e-8970-f70d7eb62991
doc_type: memory
keywords: [bidder scoring reality, household_score, advertiser_household_score, realtime_conquest_score, clause polarity, MM ceiling, cost_impression_log model_params, TI-999 Finding 15, unscored delivery, TI-956]
domain: [bidding, audience-scoring]
lifecycle: active
last_verified: 2026-05-28
---
The MNTN bidder applies **three separate scoring systems** at impression time, plus **clause-polarity-dependent set semantics**. Both pieces are needed to reason about delivery.

## Score fields (in `cost_impression_log.model_params`)

**1. `household_score` — main per-IP scoring system, graduated 0-10000.**
- This is what "HI / PP / mid-band" tier language refers to.
- Empirically on 2026-05-26: 65% = -1 (unscored), 15% = 10000 top, 11% = 8k-10k HI band, ~10% in middle bands.
- Applied broadly by the bidder — does NOT require the audience expression to opt in.

**2. `advertiser_household_score` — per-advertiser tuning** (Mountain Match-style).
- Mostly binary in delivery (29% = 10000, 70% = -1) with a small graduated tail.

**3. `realtime_conquest_score` — RTC, binary qualifier flag for recent-site visitors only.**
- 10000 (qualifies) or -1 (does not). No middle values — binary BY DESIGN.
- 4.6% of delivered impressions qualify. NOT the same as the bidder's general scoring.

Don't conflate RTC with the bidder's general scoring.

## Clause-polarity + MM-ceiling-overflow bidder model (TI-999 Finding 15, 2026-05-28 PM)

**The bidder is scored-first within campaign pacing, falls through to unscored eligible IPs when MM ceiling is hit. Inclusion is OR-additive in the expression; exclusion is AND-NOT.**

- **Inclusion (positive `op:any`):** Buyer-written OR clauses ADD eligible IPs to the universe. Multiple positives union.
- **Exclusion (positive `op:any` inside `op:not`):** REMOVES IPs from the universe (AND-NOT narrowing).
- **Within the eligible universe, the bidder prefers scored IPs (`household_score > 0`) while they remain available in the bid stream + pacing windows.** When MM-segment scored audience exhausts for the campaign's pacing budget — the **MM ceiling** — the bidder falls through to 3P-added unscored IPs to maintain spend pacing.

**Empirical proof (TI-999 Finding 15 Pass 3 + Pass 5):**
- Cohort-level: `MM_only` 4.2% unscored vs `MM + 3P incl_only` 23.3% unscored vs `MM + 3P excl_only` 0.4% unscored.
- FICO single-advertiser ceiling test: MM_only campaign delivers ~71.5K scored imps/day at $41K spend. MM+3P_incl_only campaign with 4x the spend ($168.5K) delivers ~60.1K scored imps/day — **essentially the same MM ceiling**. The extra $127K in the bigger campaign went to 236K unscored 3P-added impressions, not to incremental scored MM delivery.
- Bucket-level scored-imps-per-$K spend: MM_only 1,054; MM+3P_incl_only 752; MM+1P_excl_only 1,357. Exclusion-only concentrates spend on scored. Inclusion-only spends past the ceiling into 3P-added unscored.

**Implications:**
- Buyers adding 3P inclusion to MM campaigns are **intentionally expanding reach beyond MM's ceiling**. The unscored delivery isn't a bug; it's the buyer's explicit overflow choice.
- 1P clauses: 92% of MM+1P campaigns use 1P as EXCLUSION (CRM suppression-from-prospecting). 1P excl narrows scored MM. Only ~6% use 1P as inclusion (retargeting-with-MM-scoring; tiny cohort).
- Pure-3P prospecting is ~74% unscored — no MM ceiling at all, bidder bids on all 3P-eligible IPs.

## What's missing — per-segment quality scoring

The bidder ranks IPs (via household_score) but has no signal for "this LiveRamp segment is higher-quality than that one." That's the gap TI-956 / Alex's per-dscid composite scoring framework would fill.

**TI-956 prize zone:** ~$50M/year of unscored delivery is reached via 3P inclusion clauses (across MM+3P and pure-3P cohorts). Per-segment quality scoring lets buyers steer that toward more-productive segments — quantifiable value, not theoretical.

**Reference:** [TI-999 Finding 15](tickets/ti_999_interest_segment_sizing/summary.md). Knowledge persisted in `knowledge/data_knowledge.md` under "Bidder Scoring Reality" §8.

**Revision history:**
- v1 (2026-05-28 AM): incorrectly claimed RTC was the only scoring system. Corrected.
- v2 (2026-05-28 PM, TI-999 Finding 15): added clause-polarity set semantics. Refuted AND-intersection verbal model for inclusion; confirmed AND-NOT for exclusion.

Related: [[feedback_crm_excluded_from_prospecting]], [[project_ti_999_interest_segment_sizing]], [[reference_audience_platform_authority]].
