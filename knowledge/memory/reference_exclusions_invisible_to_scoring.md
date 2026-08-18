---
name: reference_exclusions_invisible_to_scoring
description: prospecting_join discards the include flag, so any HI pool from prospecting_intent is pre-exclusion for every campaign — a with-vs-without-exclusion cohort comparison measures account size, not the exclusion's effect
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [exclusions, include false, prospecting_active_campaign_categories, PACC, prospecting_join, pre-exclusion, HI pool, serve time, DS47, bidder, AUDI-1208, audience sizing]
domain: [audience-scoring, bidding]
lifecycle: active
last_verified: 2026-08-18
---
**Audience exclusions never reach the scoring pipeline. Any "HI pool" measured from `prospecting_intent` is the PRE-exclusion pool, for every campaign.** Read from source, AUDI-1208, 2026-08-18.

`airflow-ti models/audience_intent/prospecting_join.py` reads
`gs://household-scoring-prod/output/data_aggregation/prospecting_active_campaign_categories/` (PACC —
one row per campaign x `data_source_id` x `data_source_category_id`, with an `include` BOOL where
**`include = false` IS the exclusion clause**) and immediately does:

```python
.groupBy("advertiser_id", "campaign_group_id", "campaign_id", "campaign_template_id")
.agg(F.count("*").alias("_c")).drop("_c")
```

That **throws away `include`, `data_source_id`, and `data_source_category_id`.** PACC serves only as a
dimension supplying `campaign_template_id` / `funnel_level`, then LEFT-joins to the scores.
`include = false` filters nothing, anywhere, in the scoring path.

Scoring sizes the **addressable** universe; the bidder removes the excluded slice later at serve time,
against the DS the bidder actually evaluates (DS47 since the 2026-07-01 release) —
[[reference_crm_exclusion_serve_time]].

## The trap
Splitting audiences into with- vs without-exclusion cohorts and comparing HI pool sizes LOOKS like it
measures the exclusion's cost. It cannot — both sides are pre-exclusion. Measured 2026-08-17
(funnel 1, n=2,063): **with-exclusion audiences report a HIGHER median HI pool, 3,725,338 vs
3,486,590**, because carrying an exclusion correlates with being a larger, more mature account. Read as
an exclusion effect, that inverts the truth. 721 of 2,063 prospecting audiences (35%) carry at least
one `include = false` clause.

**How to apply:** if asked "how much does excluding cost this audience?" say plainly that
`prospecting_intent` cannot answer it, and scope the real job: resolve each campaign's exclusion sets
against `ipdsc__v1` at the same `dt` and subtract from its HI pool. If asked only for the cohort split
(which is usually what the requester means), deliver it WITH this caveat stated first — the number is
misleading without it. Companion traps: [[reference_prospecting_intent_query_rules]].
Detail: `tickets/audi_1208_vertical_hi_audience_sizing/summary.md` §4.9.
