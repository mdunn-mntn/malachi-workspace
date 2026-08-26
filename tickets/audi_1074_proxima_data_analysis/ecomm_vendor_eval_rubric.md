# Ecomm Transaction-Vendor Evaluation Rubric

Reusable battery for any transaction/conversion data vendor (built on AUDI-1074 Proxima; per AUDI-929 ask 5). Three axes; every row = one metric, one script/query, one threshold. Scripts in `analysis/`, queries in `queries/` adapt by swapping the vendor tables.

## Axis 1 — Predictive-power proxy (heaviest weight)
| Row | Metric | AUDI-1074 result | Flag when |
|---|---|---|---|
| Repurchase cadence | median gap days per category (day-grain dedup, censoring caveat) | 30d | no cycle structure (flat gaps) |
| Cross-category structure | affinity vs any-followup base rate, censoring-safe anchors | 43% vs 24% base (top pair) | top pairs < 1.2x base |
| Intent-cohort separability | 5-fold CV AUC, vendor-only features, served-IP score-band cohorts | 0.506 (chance) | AUC < 0.55 means no model-feature evidence; demand an outcome lift test before paying for features |
| New-to-brand measurability | NTB curve plateau month + steady-state share | month 5, ~44% | no plateau inside the file window |
| Freshness class | pipeline lag + refresh cadence → pre-exposure vs feedback classification | 2d lag but monthly refresh = ~17d mean staleness → feedback-class | vendor sells "real-time" but cadence-dominated |

## Axis 2 — Uniqueness / overlap
| Row | Metric | AUDI-1074 result | Flag when |
|---|---|---|---|
| Addressability | % vendor IPv4 in DS14 gate (1% FARM_FINGERPRINT sample, Wilson CI) | 92.0% | <60% (paying for unreachable households) |
| Served overlap | % in CIL 30d + recency-bucketed churn curve | 40.2%, flat across buckets | steep recency decay (stale IPs) |
| Dark-IP concentration | % of vendor∩served that is never-scored vs base (honor the -1 sentinel) | 42.9% vs 46.7% base (0.92x) | vendor uniqueness concentrated in unscored IPs that never convert (AUDI-1089 pattern) |
| Panel scale/concentration | GMV, brands, top-N shares, HHI, monthly-active trend | $10.1B, 1,112 brands, HHI 0.010 | top-10 > 60% or active-count collapsing |

## Axis 3 — Integration cost
| Row | Check | AUDI-1074 result |
|---|---|---|
| Dictionary vs delivery | column diff, types, headers | 13 columns absent (later: deliberate), VARCHAR ids, headerless file |
| Grain integrity | dup rates, orphans, claimed span | clean: 0% dups, exact 1yr |
| Identity fill | % customers/orders with IP, email, phone; IPv4/v6 split | 45% customers with IP; 3.9% IPv6 |
| Derived-field trust | flag semantics verified against transactions AND vendor (bands vs cumulative!) | dictionary wording wrong; flags are disjoint bands |
| Refresh semantics | append-only vs restated; backfill on onboarding | restated each drop; new brands backfill history |
| Name/key opacity | can rosters join to internal entities | brand names never provided |

## Process rules (learned, non-obvious)
1. QC the customer-id scope FIRST (cross-brand vs per-store decides half the battery).
2. Verify every "impossible" derived-field pattern with the vendor before calling it a defect — AUDI-1074's flag "violations" were a dictionary defect, not a data defect.
3. Load vendor parquet once into duckdb; never views-over-parquet (memory `reference_local_vendor_data_analysis`).
4. Honor score sentinels (-1 ≠ NULL) on every MNTN-side predicate.
5. GO/NOGO must be GO-for-what: measurement/seeding vs bid-time features are different verdicts with different freshness and leakage bars.
