# Interest-Segment Portfolio Sizing

**TI-999 · Targeting Infrastructure · Malachi Dunn · 2026-05-28**

A read on how much of MNTN's spend currently flows through third-party interest segments, how fresh those segments are, and what that means for the quality-scoring work in TI-956.

Window: 30 days ending 2026-05-28. Sources: `bronze.integrationprod.data_sources`, `bronze.tpa.categories`, `bronze.external.ipdsc__v1`, `silver.audience.audience_segments`, `silver.summarydata.sum_by_campaign_by_day`.

---

## Headline

- **35.7% of MNTN total spend** flows through campaigns that use ≥1 third-party interest segment.
- **$14.36M / 30 days. Annualized ≈ $172M.**
- Of that, **$7.73M / 30d ($93M annualized) sits on campaigns that reference providers whose taxonomy has not been updated in 2+ years.**
- We do not score third-party segment quality today. Users selecting from "90 segments matching 'households $100K+ income'" choose blind.

---

## The active third-party interest landscape

Of 60 `data_source_id` entries in `data_sources`, **three** carry material daily IP volume *and* belong in the "bought third-party interest" category:

| DS | Provider | IPDSC rows / day | Active categories |
|---:|---|---:|---:|
| 35 | LiveRamp IP | 104M | 213,629 |
| 17 | ShareThis | 65M | 1,850 |
| 18 | Dstillery | 32M | 3,303 |

Most named 3P providers in `data_sources` (Sovrn, Cybba, Bombora, Captify, 33Across, Klickly, Liftlab, Oracle, Experian) currently deliver **zero** IPDSC volume. They exist in the registry but aren't sending data.

One borderline: **DS49 Publisher Network** (82M rows/day, 208 categories) — could be MNTN-internal contextual rather than bought 3P. Flagged for confirmation.

---

## Spend exposure by bucket

![Spend share by bucket](artifacts/ti_999_chart_bucket_spend_share.png)

| Bucket | Camps | % camps | Spend (30d) | % spend |
|---|---:|---:|---:|---:|
| Interest-only (no internal targeting layered) | 664 | 4.3% | $4.61M | 11.4% |
| Interest + internal (CRM 1P / RTC / BUK layered) | 1,311 | 8.4% | $9.75M | 24.2% |
| No interest segments | 13,550 | 87.3% | $25.91M | 64.3% |

12.7% of active campaigns use interest segments. They drive 35.7% of spend. LiveRamp accounts for 97% of the interest-using campaign count; ShareThis 35%; Dstillery 27% (overlapping populations).

---

## Freshness collapses on two of three providers

![Staleness by DS](artifacts/ti_999_chart_staleness_by_ds.png)

`tpa.categories.updated_date` per provider, non-deprecated categories only:

- **LiveRamp IP (DS35):** 99.6% of 213,629 active categories updated in the last 30 days. The provider churns its taxonomy constantly.
- **ShareThis (DS17):** 100% of 1,850 active categories are >2 years stale.
- **Dstillery (DS18):** 100% of 3,303 active categories are >2 years stale.

Caveat: `updated_date` reflects taxonomy metadata changes, not necessarily whether the underlying IP→category data is being refreshed. But we have no positive freshness signal for ShareThis or Dstillery taxonomies.

---

## Stale-3P spend exposure

![Stale-3P spend exposure](artifacts/ti_999_chart_stale_exposure.png)

- **905 campaigns (5.8% of active) reference at least one ShareThis or Dstillery segment in their audience expression.**
- **They drive $7.73M / 30d (19.2% of total spend).**
- Annualized exposure: **~$93M / year**.

Lower bound: **10 campaigns ($59K / 30d, ~$710K / year)** rely *exclusively* on stale-3P — no LiveRamp, no internal-targeting layered. Most stale-3P-touching campaigns mix the stale data with LiveRamp + internal signals, so the bidder isn't *purely* relying on stale signal. But the stale clause is in the expression.

---

## KPI baselines per bucket (descriptive only)

| Bucket | Visit rate (UV/imp) | Conversion rate |
|---|---:|---:|
| Interest-only | 0.55% | 0.038% |
| Interest + internal | 0.41% | 0.029% |
| No interest | 0.45% | 0.098% |

Lower conversion rates in interest buckets reflect bucket composition (interest buckets skew prospecting; `no_interest` is dominated by retargeting / RTC, which is closer-to-conversion by design). **This is not a causal claim** about interest-segment quality — selection effects (advertiser mix, vertical, funnel position) drive these gaps.

---

## What this means for the scoring work

1. **TI-956 (LiveRamp Phase 1) is the right starting point.** LiveRamp is 97% of interest-using campaigns by count and well-maintained — so the value-add from scoring is the *other 8 axes* (uniqueness, specificity, activity, targetability, performance), not staleness.
2. **ShareThis + Dstillery are the highest-staleness-leverage targets for Phase 2.** ~$93M/yr exposure to providers whose taxonomy hasn't refreshed in 2+ years. The staleness axis alone would aggressively filter their active categories down to zero — useful as a UI-level "warning" before this is a "score."
3. **The product question raised by leadership — "use interest segments more" — is grounded.** $172M/yr already runs through them; the scoring infra turns that into something users can navigate instead of guess at.

## Methodology caveats

- DS bucketing relies on regex extraction of `"data_source_id":N` from the audience-expression JSON. This captures every reference, including exclusion clauses (`"op":"not"`). "Uses X" means "X appears in the expression" — not strictly "X drives positive targeting."
- KPI comparisons across buckets are **descriptive**. No causal claim. Bucket composition differs systematically (prospecting vs retargeting mix, advertiser mix).
- `updated_date` measures taxonomy freshness, not data-feed freshness.
- Operational interest-segment DS set (17, 18, 35) excludes borderline DS49 Publisher Network. Verify before any external sharing.

## Next steps

1. Ship TI-956 Phase 1 (LiveRamp scoring on schedule → GCS → admin UI).
2. Validate the operational DS set + bucket logic with the audience-platform team before broadening the audience for this analysis.
3. Phase 2 of TI-956: extend scoring framework to ShareThis (DS17) and Dstillery (DS18). Config change, not a rewrite.
4. Downstream (post-scoring): UI filtering of "90 results down to top-5" — that's the UI-team workstream.
