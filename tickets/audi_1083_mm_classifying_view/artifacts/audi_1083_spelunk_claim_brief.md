# Spelunking claim brief — FICO, campaign group 81053

Answer key for the 8/21 session. Written from BigQuery **before** the hour; do not open it while
diagnosing in the UI. The session rule is reporting UI only (no backend, no DB, no public API).

## The claim

| | |
|---|---|
| Advertiser | FICO (advertiser_id 37056) |
| Campaign group | 81053 — `FY26_Croud_myFICO_US_Direct_MNTN_CTV_CTV_Mixed_3P_PP` |
| Spend on the bad-CPA tab | $220,763 (2026-07-01 to 2026-08-19) |
| CPA vs goal | $41.65 against a $25 goal, goal attainment 0.60 |
| Rank on the list | highest spend of any unclaimed Peak Performance campaign |

Picked because it is the largest-spend PP miss and because three separate, verifiable audience-setup
facts could each explain the miss. That gives the UI something concrete to fail to surface.

## Ground truth (`audience.mm_campaign_classifier`, Stage-1 campaign 325113)

| field | value | what it means |
|---|---|---|
| `mm_class` | `fangorn_vertical_only` | Peak Performance v2 with **no keyword layer** (`has_ds46` true, `has_ds19` false) |
| `tiers_reachable` | `PP·MI (no HI)` | ceiling is the 8000 band; the high-intent band needs the keyword layer |
| `hhst_current` / `hhst_gated` | `0` / false | **no household score threshold** — the bidder bids on everyone, scored or not, so the intent score is inert |
| `three_p_semantics` / `and_3p_narrowed` | `mixed` / true | a 3rd-party segment **AND-narrows** the scored universe |
| `restriction_level` | `audience` | narrowed by audience, not geo |
| `geo_narrowest_type` | `country` | national |
| `fangorn_tier` | 5 | |
| `is_flagship` / `is_unmodified_mm` | false / false | not a clean PP baseline |
| `expression_updated_at` | 2026-07-01 19:10 UTC | expression edited on the first day of the measured window |

The campaign name ends `_3P_PP`, so Peak Performance and the 3P layer are both intentional. The three
mechanisms above are not.

## Delivery in the same window (`summarydata.sum_by_campaign_by_day`, group 81053)

| campaign | stage | spend | visits | conversions |
|---|---|---|---|---|
| 325113 Beeswax Television Prospecting | 1 | $313,748 | 34,251 | 2,834 |
| 325109 Beeswax Television Multi-Touch | 2 | $37,371 | 4,075 | 314 |
| 325108 Beeswax Television Multi-Touch Plus | 3 | $18,572 | 50,320 | 2,335 |
| 325112 Beeswax Television Prospecting - Ego | 4 | $1 | 0 | 0 |

**Open discrepancy:** this sums to $369,692 against the tab's $220,763 for the same dates. The tab reads
`all_facts_by_day_ramp_combined` rolled to the parent group and includes `legacy_spend`. Not resolved.
If the UI shows a third number, that is itself a finding.

## Hypotheses to test in the UI, in order

1. Can an advertiser see which intent band their delivery came from? If not, a vertical-only v2 campaign
   looks identical to one that can reach the top band.
2. Does anything in the UI reveal that no score threshold is set, i.e. that the intent targeting they are
   paying for is not gating any bid?
3. Does the UI show that the 3P segment intersects rather than adds, shrinking the addressable pool?
4. Stage 3 delivered 50,320 visits on $18,572 while Stage 1 delivered 34,251 on $313,748. Is that legible
   anywhere as a prospecting problem, or does the blended view hide it?

Each "no" is an insight. Log the confusion and the click path as it happens; do not reconstruct after.

## Fallbacks if 81053 is taken

| advertiser | group | spend | CPA vs goal | why |
|---|---|---|---|---|
| Ancient Nutrition | 117662 | $149,658 | $213 vs $140 | v2, no keyword layer |
| RushOrderTees | 69600 | $53,116 | $102 vs $100 | v2, no keyword layer, parent of a child group |
| Refills | 115745 | $24,647 | $440 vs $125 | v2, no keyword layer, attainment 0.28 |
