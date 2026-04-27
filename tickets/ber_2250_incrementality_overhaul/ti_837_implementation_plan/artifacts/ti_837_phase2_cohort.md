# TI-837 Phase 2 — Final Cohort

**Date:** 2026-04-27
**Window:** 2026-04-20 → 2026-04-26 UTC (analysis); +3-day post-period through 04-29 (visit window)
**Stratification reference window:** March 2026 (full month)
**Cohort size:** 30 advertisers
**Phase 1 anchors retained:** 2 (Ancient Nutrition, Ferguson Home — the two
NOT tier-collapsed in Phase 1)

## Final cohort

| advertiser_id | company_name | vertical | spend (March) | spend tier | diversity | high-bh-est | rationale |
|---:|---|---|---:|---|---:|---:|---|
| 31455 | Ancient Nutrition | Vitamins, Supplements & Health Stores | $404,746 | high | 0.78 | 324,782 | cell-anchor (high × Vitamins) |
| 42097 | Gruns | Vitamins, Supplements & Health Stores | $288,126 | high | 0.73 | 401,621 | topup |
| 31276 | Ferguson Home | Home Improvement & Hardware | $153,645 | high | 0.79 | 299,026 | cell-anchor (high × Hardware) |
| 32404 | National University | Colleges & Universities | $97,581 | high | 0.74 | 436,589 | cell-anchor (high × Colleges) |
| 38422 | Signature Hardware Account | Home Improvement | $55,757 | high | 0.49 | 722,743 | cell-anchor (high × Home Improvement) |
| 33572 | Jase Medical | Healthcare | $38,429 | high | 0.57 | 537,938 | cell-anchor (high × Healthcare) |
| 33467 | Outback Presents | Arts & Culture | $32,523 | high | 0.80 | 350,640 | cell-anchor (high × Arts) |
| 35573 | Casper | Household Goods | $28,938 | high | 0.92 | 188,477 | cell-anchor (high × Household Goods) |
| 35086 | TurboTenant | B2B - Workflow Automation | $26,695 | high | 0.80 | 773,013 | cell-anchor (high × B2B Workflow) |
| 32527 | Haggar Clothing | Apparel | $21,897 | high | 0.96 | 114,613 | cell-anchor (high × Apparel) |
| 30181 | Longines | Jewelry & Watches | $21,053 | high | 0.49 | 582,101 | cell-anchor (high × Jewelry) |
| 34141 | UD - Daniels College of Business | Education | $20,181 | high | 0.68 | 527,763 | cell-anchor (high × Education) |
| 32320 | Biz2Credit | Finance | $18,641 | high | 0.87 | 97,934 | cell-anchor (high × Finance) |
| 31464 | Fiji Airways | Travel | $17,064 | mid | 0.83 | 258,626 | cell-anchor (mid × Travel) |
| 32899 | Balance of Nature | Fitness & Health | $14,541 | mid | 0.94 | 93,456 | cell-anchor (mid × Fitness) |
| 34862 | Planned Parenthood Federation | Non-Profits | $14,420 | mid | 0.50 | 462,428 | cell-anchor (mid × Non-Profits) |
| 46426 | BoggBag | Apparel | $14,273 | mid | 0.68 | 1,039,853 | topup |
| 31297 | Mountain Mike's Pizza | Fast Casual Dining | $12,357 | mid | 0.39 | 526,912 | cell-anchor (mid × Fast Casual) |
| 30496 | Lofta | Healthcare | $11,659 | mid | 0.93 | 83,619 | topup |
| 35374 | Experience Scottsdale | Travel | $10,474 | mid | 0.74 | 410,922 | topup |
| 30392 | Swatch | Jewelry & Watches | $8,501 | low | 0.63 | 428,558 | topup |
| 33684 | SUMMIT One Vanderbilt | Arts & Culture | $7,663 | low | 0.84 | 274,280 | topup |
| 32244 | Sur La Table | Household Goods | $6,829 | low | 0.78 | 481,697 | topup |
| 34365 | Barbara B. Mann Performing Arts | Live Music & Comedy | $6,152 | low | 0.82 | 322,322 | cell-anchor (low × Live Music) |
| 37222 | NET-A-PORTER | Apparel & Accessories - Luxury | $6,020 | low | 0.91 | 295,267 | cell-anchor (low × Luxury Apparel) |
| 50525 | Overjet | B2B Software & Services | $6,005 | low | 0.99 | 36,145 | cell-anchor (low × B2B Software) |
| 56187 | Ignite Attachments | Apparel | $5,851 | low | 0.89 | 344,498 | topup |
| 37796 | California Grown | Food & Beverage | $5,198 | low | 0.64 | 585,693 | cell-anchor (low × Food & Beverage) |
| 38307 | Re-Bath Horney | Home Improvement | $5,185 | low | 1.00 | 0 | cell-anchor (low × Home Improvement); NOTE: peak/mid only — no high-tier IPs |
| 43996 | JS Health | Fitness & Health | $5,027 | low | 0.82 | 263,367 | topup |

## Stratification summary

**Spend tier (from March 2026 prospecting spend):** 13 high / 7 mid / 10 low.

**Verticals (20 distinct):** Apparel ×3, Home Improvement ×3, Vitamins ×2,
Healthcare ×2, Jewelry & Watches ×2, Household Goods ×2, Fitness & Health ×2,
Arts & Culture ×2, Travel ×2, Hardware, Colleges, Education, B2B Workflow,
B2B Software, Finance, Fast Casual, Non-Profits, Luxury Apparel, Live Music,
Food & Beverage.

**Anchor-vs-topup mix:** 17 cell-anchors + 13 topups (next-best by composite
score = high-tier biddable_holdouts × tier-diversity factor).

## Why the Phase 1 7 mostly didn't make the cut

| advertiser | retained? | reason |
|---|---|---|
| Ancient Nutrition (31455) | yes — cell-anchor | High spend, high diversity (0.78), Vitamins anchor |
| Ferguson Home (31276) | yes — cell-anchor | High spend, high diversity (0.79), Hardware anchor |
| Clayton Homes (34838) | no | Eligible but bumped from Home Improvement cell by Signature Hardware (higher composite score) |
| First Watch (34143) | no | frac_high_only = 1.00 — every IP scored 10000 on day 23. Tier-collapsed. |
| HexClad (34611) | no | frac_high_only = 1.00 — tier-collapsed. |
| Zazzle (37775) | no | frac_high_only = 1.00 — tier-collapsed. |
| Northern Tool (40563) | no | frac_high_only = 1.00 — tier-collapsed. |

The 4 tier-collapsed Phase 1 advertisers reproduce the Phase 1 finding under
a fresh subject-day construction — which validates the collapse signal.
**Phase 1's results on these 4 are reused as a "high-only" validation
cohort,** not re-run.

## Power expectation

Per-tier biddable_holdouts (estimated as `holdouts × biddable_rate_proxy =
0.10 × distinct_ips × 0.30`):

- 27 of 30 advertisers eligible at high (≥5,000 biddable holdouts).
- 30 of 30 eligible at peak.
- 28 of 30 eligible at mid.
- 25 of 30 eligible at all three tiers.

For the high-intent IVW pool the new cohort expected total
biddable_holdouts ≈ 13,000,000 (vs Phase 1's 22,000,000 across 7
advertisers — a smaller per-advertiser footprint but spread across more
advertisers, which mitigates the IVW dominance issue). The Ancient-Nutrition-
style fragility risk is bounded — the largest single advertiser
(BoggBag, 1.04M high-bh-est) is ~8% of the pooled total.

## Methodology caveats

1. **1-day prospecting proxy.** Stage A.1 was forced to run on a single day
   (2026-04-23) due to the external prospecting table's slow scan
   (full-week attempts hit 30+ min wall, 800B+ slot-ms with no bytes
   reported). Tier composition for the actual analysis week may differ
   modestly. Re-validation runs against the actual ATT output.
2. **Biddable-holdout proxy.** Stage A.2 was skipped (full augmentor
   scan ~$250-500). The biddable_rate_proxy = 0.30 was applied uniformly;
   actual rates vary by advertiser. The ATT run produces exact biddable
   counts.
3. **HLL approximation in tier counts.** APPROX_COUNT_DISTINCT was used to
   sidestep per-IP shuffle; counts have ~1-3% error.
4. **Phase 1 window comparability.** Same 7-day window
   (2026-04-20 → 2026-04-26). Augmentor partitions still live (verified
   2026-04-19 onward).

## Next steps

1. Adapt `ti_837_lift_analysis_7adv_7day.sql` to take the 30-advertiser
   list (replace the IN clause; everything else unchanged).
2. Run the full Phase 1 pipeline. Expected cost: $200-400 (proportional
   to advertisers; was ~$90 for 7).
3. Run `ti_837_compute_att.py` on the new cohort. Compare per-tier IVW
   pools and per-advertiser ATT to Phase 1's headline numbers.
4. Update Jira and self-review with the cohort + ATT results.
