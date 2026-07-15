# AUDI-1089 Query Manifest — validate every number in the vendor-eval workbook

Every value in `outputs/audi_1089_quality_template_filled.xlsx` (and every runbook PNG) traces to
one of the 27 SQL files in this folder, or to an arithmetic combination of their outputs (formulas
printed on the workbook's **index** sheet; combination code = `../charts/fill_template.py`).
Run the queries in the order below; each file's header states its claim, grain, windows, and exact
run command. All queries are **read-only** (temp external tables over GCS parquet; no DDL/DML).

## How to run (for reviewers outside this workspace)

Console-pasteable queries (no external tables) run as-is in the BigQuery console.
Queries marked **svs** need the bq CLI with temp external tables — the generic pattern:

```bash
URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(<DAYS>)))"); do
  URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
bq query --external_table_definition="svs::PARQUET=${URIS}" \
  [--external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet"] \
  [--external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet"] \
  --use_legacy_sql=false --format=csv --max_rows=<N> --project_id=dw-main-silver \
  "$(grep -v '^[[:space:]]*--' <file>.sql)"
```

Each header shows its exact windows: delivery metrics = 30d (`dt 2026-06-02..07-01`);
serving/performance = 37d svs union × valuation week `2026-07-02..08`; bills = June 2026 (×12).
Cost class: **cheap** = seconds-minutes, console-friendly · **BIG** = 5-15 TB scan, ~1h, run in background.

## Run order

| # | File | Answers (workbook section / chart) | Cost |
|---|------|-------------------------------------|------|
| 1 | `q0_roster_cost.sql` | CONTRACT & IDENTITY rows (DS ids, billing types, rates) + ECONOMICS COST bills; registry×meter join, meter check imps×CPM=usage. Chart q0. | cheap |
| 2 | `q0b_meter_regime_evidence.sql` | Billing REGIME proof: % fractional imps by vendor-month — Jan-Apr 2026 ~100% fractional (1/N split credit), May+ 100% integer (single-vendor credit). Underpins "June savings math valid". | cheap, console |
| 3 | `q1_scale_by_day.sql` | FEED SCALE rows: rows/day, liveness, weakest day, % IPv6, % URLs with path. Chart q1. | **BIG** (svs 30d) |
| 4 | `q1b_column_richness.sql` | DATA QUALITY field-population rows (user_agent/url/query_params/advertiser_id %). Charts q1b. | cheap (1h slice) |
| 5 | `q1c_content_quality.sql` | DATA QUALITY junk rows: unparseable/malformed %, bots, concentration, integrity + VERDICT vendor asks. Charts q1c. | cheap (1h slice) |
| 6 | `q1d_billed_usage.sql` | Billed imps/domains per vendor (meter month), % of delivered rows billed, junk-in-billed evidence. Charts q1d. | cheap, console |
| 7 | `q2_window_reach.sql` | FEED SCALE unique IPs / domains / (ip,domain) pairs, 30d. Chart q2. | **BIG** (svs 30d) |
| 8 | `q2b_daily_drops.sql` | USABLE FUNNEL drop decomposition (hard-drop / DS13-blocklist / bot-UA). Chart q2b. | cheap (1d slice) |
| 9 | `q2c_funnel.sql` | USABLE FUNNEL survival rows (rows/IPs/domains/pairs used; the DS13-OR-DS19 usable definition). Charts q2c/q2d. | **BIG** (svs 30d + wcv + pc) |
| 10 | `q3_usable_uniqueness.sql` | UNIQUENESS rows: sole IPs/pairs, pairs-per-IP, net-new vs free (usable-restricted). Chart q3. | **BIG** |
| 11 | `q4_domain_value.sql` | Query A → `q3_pair_recency.csv` (freshness mix rows: sole/freshest/tied/stale). Query B → sole/classified DOMAIN rows + fee-band axis. | **BIG** |
| 12 | `q3b_credit_reassignment.sql` | PORTFOLIO rows: holder-mask histogram (all 2^8 keep-sets exact → frontier, marginal add-order, coverage lost), first-reporter reassignment classes (exact drop savings, destination mix), free-cohold. Charts q9d/q9e; decisions scenarios + ladder universe. | **BIG** |
| 13 | `q3c_visit_grain_uniqueness.sql` | Visit-grain (ip×domain×DATE) rows: new-pair / recency-refresh / same-day-dup per vendor; triple-grain masks → AUDI-1093 exact preemption $ + ladder visit-day columns. | **BIG** |
| 14 | `q3d_score_vertical_coverage.sql` | HI/PP coverage per scenario (score-tier holder masks; k=4 keeps 99.9991% of HI) + per-vertical before/after IP counts (all / free-only / k=4). Charts q3d_*; decisions vertical block. | **BIG** |
| 15 | `q5_score_tiers.sql` | SCORE QUALITY rows: HI/PP/high-grad/mid/max-reach/unscored per vendor, touched AND sole cohorts (incl. sole-HI counts). Chart q5. | **BIG** (svs 37d + CIL wk) |
| 16 | `q6_value_tiers.sql` | SERVING & WON BIDS + PERFORMANCE spend/imps rows (touched & sole media, imps, IPs served) → T1/T2 dependency revenue. | **BIG** |
| 17 | `q6b_sole_by_funnel.sql` | Attribution row: % of sole serves via prospecting-family (vendor-dependent; 97-99%) + dependent-revenue-at-risk per scenario. Chart q6b. | **BIG** |
| 18 | `q7_sole_vr.sql` | PERFORMANCE sole visits + IVR (+ vs 0.0223% no-svs baseline; Poisson CI). | **BIG** |
| 19 | `q7b_perf_by_cohort.sql` | Avg household score + visits/IVR per touched/sole cohort (SCORE QUALITY + PERFORMANCE rows). | **BIG** |
| 20 | `q7c_conversions.sql` | PERFORMANCE conversions/revenue rows (touched & sole; last-touch dedup, no assists/disputed) → CVR/AOV/ROAS. | **BIG** |
| 21 | `q7d_platform_week.sql` | Platform-week anchors: 398.3M won imps / 28.03M served IPs / $3.53M media (denominator for "% of platform served IPs"). | cheap |
| 22 | `q7e_vr_baseline.sql` | Platform VR calibration by funnel×scored bucket (2.89 / 1.11 / 0.72%) — proves sole-cohort VRs are 40x below the coldest bucket. | cheap-ish |
| 23 | `q7f_sole_ip_activity.sql` | Adjudication: sole IPs are genuinely DARK (unconditional clickpass activity, no attribution join — 25 of 33Across's 99K served sole IPs active vs 1.43% guid-sole). | **BIG** |
| 24 | `v01_visits_source_validation.sql` | VALIDATION: deduped ui_visits == clickpass_log within +0.5% (visit basis is sound). | cheap, console |
| 25 | `v02_conversion_model_fanout.sql` | VALIDATION: ui_conversions fans out per attribution model (~3-4x) — why q7c dedups. | cheap, console |
| 26 | `q8a_solo_stock.sql` | SOLO sheet stock rows: each vendor as the ONLY paid source (overlap counted vs free logs only) — solo pairs/IPs/domains/classified + freshness-vs-free splits (pair + visit-day grain). | **BIG** (svs 30d + wcv + pc) |
| 27 | `q8b_solo_perf.sql` | SOLO sheet measured serving/performance: solo cohort (V's IPs neither free log touched) served IPs, imps, media, T1/T2 inputs, visits, conversions, HI/PP tier counts. | **BIG** (svs 37d + CIL wk + clickpass + ui_conversions) |

## Computed rows (no additional SQL — arithmetic over the CSVs above)

Formulas are printed per-row on the workbook's **index** sheet; implementation =
`../charts/fill_template.py` (deterministic; rerun = identical workbook). The main ones:
- **ECONOMICS WORTH:** T2 = q6 `media_sole`×52; T1 = q6 `media_sole_scored`×52; max justified CPMs =
  T2×30% ÷ (q1 rows or q1d billed imps, annualized); flat equivalents = T1×15% / T2×20% / T2×30%;
  fee band = q4 `sole_classified` × $3-13; scale-normalized $/1M pairs = netnew% × T2/sole_pairs.
- **PORTFOLIO:** drop savings = q1d bill×12 × (1 − metered-reassignment share from q3b), with the
  metered deduction waived when no metered destination survives; preemption recoverable = bill ×
  visit-day free-cohold share (q3c masks).
- **Decisions ladder:** net-of-free universe = q3b/q3c masks with free bits removed; greedy add-order
  (optima verified nested = exhaustive); $ = pairs (or visit-days) × vendor's T2 density; pay range = ×10-30%.
- **Composite score (q9b):** 0.40·V + 0.15·R + 0.15·Q + 0.10·D + 0.20·P, curved to best=100
  (components from q4/q3_pair_recency/q5/q6/q7).
- **Post-preemption (AUDI-1093 applied):** bill_after = bill × (1 − visit-day free-cohold share)
  (q3c masks; cross-validated by q8a fresh_day same-day-dup splits). Roster $812K → $539K
  (−$273.7K, −33.7%). Pay ranges unchanged by construction. Decisions block 5 + numbers/solo row.
- **SOLO sheet:** counterfactual keep-set = {vendor} + free logs. Mask-exact rows = Σ q3b/q3c/q3d
  mask records with the vendor's bit set and the OTHER-free bits clear (other_free(23)=bit5,
  other_free(30)=bit0, else bits 0|5). T2_solo = q8b media×52; T1_solo = q8b media_scored×52;
  pay range = T2_solo ×10–30%. Solo bill is a bounded estimate: low = today's run-rate (credit is
  monotone in roster shrinkage), high = total metered bills × vendor's share of paid-held
  visit-days (q3c masks; proportional-consumption assumption, all metered CPMs $0.50).

## Reproduction & validation anchors

Full rebuild: run 1-23 into `outputs/run_<date>/`, then `python3 ../charts/fill_template.py
run_<date> <bill-YYYY-MM>` (must print `empty: none`). Anchors a reviewer can check independently:
q0 meter check (imps×CPM=usage exactly); q3b single-bit masks = q3 sole_pairs; q3c vendor rows =
its mask totals; q3d HI-with-33Across = q5 touched hi_10000 (ratio 1.000); q7b sole imps = q6
exactly; q7c imps = q7b imps exactly; v01 within +0.5%. SOLO anchors: q8a solo_pairs == Σ q3b
solo masks == q3 netnew_vs_free_pairs (paid vendors, exact; wcv/pc snapshot drift tolerated
<0.1%); q8b media/ips/imps ≥ q6 sole (solo ⊇ sole — passed all 10 sources 2026-07-14);
q8b tier hi/pp vs Σ q3d solo masks is a DIAGNOSTIC comparison, not an equality — raw vs
usable membership lenses (clean vendors 3-10% low in q8b; Sovrn +55-68% HIGH = junk-carried
IPs on malformed URLs that never reach a usable domain);
q8a fresh_day solo_new_pair + refresh == Σ q3c solo masks.
