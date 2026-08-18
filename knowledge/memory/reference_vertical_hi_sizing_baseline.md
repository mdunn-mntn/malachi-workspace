---
name: reference_vertical_hi_sizing_baseline
description: The 2026-08-17 audience-size baseline — 148 verticals mean 9.5M / median 6.6M IPs, MM prospecting HI pool mean 4.8M / median 3.6M — plus where vertical sizes live and why the count is 148 not 152
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [vertical size, audience size, quartiles, 148 verticals, 37 buckets, ip_vertical_associations, vertical_size_monitor, HI pool, high intent size, AUDI-1208, Paulo, fpa_advertiser_verticals, MNTN Matched Audience alias]
domain: [audience-scoring, data-catalog]
lifecycle: active
last_verified: 2026-08-18
---
Answers "how big is a vertical / an audience's High Intent pool" without re-running anything. Snapshot **2026-08-17**, distinct IPs. AUDI-1208 (Paulo Black ask, #targeting-squad).

| cut | n | mean | median | Q1 | Q3 | min | max |
|---|---|---|---|---|---|---|---|
| verticals (6-digit) | 148 | 9,479,187 | 6,557,786 | 3,960,892 | 11,962,638 | 919,345 | 76,274,119 |
| buckets (3-digit) | 37 | 25,952,755 | 20,852,312 | 12,756,804 | 33,091,717 | 2,546,637 | 88,832,335 |
| HI pool / prospecting audience | 2,063 | 4,772,375 | 3,553,726 | 1,649,295 | 5,956,302 | 0 | 41,760,550 |
| — no exclusions | 1,342 | 4,516,518 | 3,486,590 | 1,321,361 | 5,718,105 | 0 | 34,470,335 |
| — with exclusions | 721 | 5,248,601 | 3,725,338 | 2,295,013 | 6,816,196 | 0 | 41,760,550 |

Same 2,063 audiences at ANY score: mean 51,321,823 / median 43,120,471. Largest vertical
`124000 Current Affairs` 76.3M; smallest `101005 Apparel & Accessories - Healthcare` 0.92M.

- **Both distributions are strongly right-skewed** (mean ~1.4x median). Quote quartiles; a bare average
  describes the largest members, not the portfolio.
- **Never add category sizes.** An IP averages ~6.6 verticals, so the 148 sizes sum to ~1.40B against a
  214,079,274-IP base. Buckets are parents of verticals, so those don't sum either.
- **Counting unit is the IP**, not households or people.
- **Quartiles use LINEAR INTERPOLATION** (numpy/R type-7 = Spark `percentile()`), NOT Python's
  `statistics.quantiles` default of `method='exclusive'`. On n=37 buckets the two differ 12% at Q1.
  Pass `method='inclusive'` in Python so numbers agree with the monitor (airflow-ti PR #1204).
- **156 of the 2,063 (7.6%) score exactly ZERO HI** — precisely the audiences with no DS19 keyword layer (156/156, no exceptions). HI needs vertical AND keywords, so a vertical-only campaign caps at PP. Excluding them: mean **5,162,773** / median **3,767,051** (+8.2% on the mean). Say which convention you used.
- **The with-exclusion cohort's larger pool is vertical composition, not exclusions.** Those advertisers sit in verticals 27% bigger at the median; within-vertical the gap vanishes (higher in 12 of 25 verticals, median −2.8%, sign-test p=0.65).

## Where vertical sizes live
`gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=<date>/` — what
Ryan Kleck's `airflow-ti models/monitoring/vertical_size_monitor.py` reads for its daily
`MNTN GCS Vertical Sizes - PROD` email. `COUNT(DISTINCT ip)` per `data_source_category_id`; **6-digit
id = vertical, 3-digit = bucket parent**. `data_source_category_id` is a **FLOAT** — cast to INT64
before matching. Readable from BQ with an inline `--external_table_definition` over one `dt` dir.
`ipdsc__v1` `data_source_id=13` is the downstream copy, agrees to median +1.9% at one day's lag.
**`external_ddm.data_source_category_sizes` is 3P-only and does NOT work.** The monitor computes no
distributional summary — the quartiles above were a fresh calculation.

## Why 148 and not 152
The AUDI-431 roster CSV has 152 rows but **148 distinct `vertical_id`**: `105000` carries both
"MNTN Matched Audience" (an alias) and "Building Materials", plus three apostrophe variants (`126002`
Mens/Men's, `126006` Womens/Women's, `135004` RVs/RV's). The monitor's
`vertical_name != 'MNTN Matched Audience'` filter therefore drops no vertical — it reports 148 too.
**Count and join on `vertical_id`, never `vertical_name`.**

**Cross-source:** Benny built a dashboard Paulo called complementary to this (flagged 2026-08-18, not reviewed). Check it before the next audience-sizing ask so the two don't diverge.

**How to apply:** quote these directly for a sizing ask, then state the date. Before recomputing the HI
half, read [[reference_prospecting_intent_query_rules]] (funnel scoping — the first pass here was 3.8x
too high) and [[reference_exclusions_invisible_to_scoring]] (these pools are pre-exclusion).
Deliverable: `My Drive/Tickets/AUDI-1208/AUDI-1208 Vertical and HI Audience Sizes.xlsx`.
