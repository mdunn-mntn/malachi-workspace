# AUDI-1089 Validation Guide — read this first

This folder is the complete, reproducible query set behind the DDP vendor-evaluation
workbook (AUDI-1089). Every number in the deliverable traces to one of these files or to
arithmetic over their outputs (`MANIFEST.md` maps each query to the rows/charts it feeds,
in run order). Everything is **read-only**: temp external tables over GCS parquet plus
SELECTs on internal BQ tables — no DDL/DML anywhere.

## What is being measured (30 seconds of context)

MNTN ingests third-party "site visit" data from 8 paid vendors plus two internal free logs
into one substrate (`site_visit_signal`, "svs"). Two consumers use it: **DS13** (domain →
vertical classification) and **DS19** (URL → product-category keywords). The evaluation
asks, per vendor: how much data arrives, how much survives to a consumer, how much is
unique vs redundant, does the unique slice ever serve/convert, and what is that worth
against the vendor's bill.

## Glossary (the terms the headers use)

| Term | Meaning |
|---|---|
| **ds / data_source_id** | The feed id. Paid: 24 Justuno, 25 5x5, 26 Predactiv, 28 33Across, 33 Sovrn, 36 Cybba, 39 Klickly, 40 33Across API. Free internal: 23 guid_log (MNTN pixel), 30 augmentor (bid-time log). |
| **pair** | Distinct (IP, registered domain) combination — the unit of uniqueness and billing credit. |
| **visit-day** | Distinct (IP, domain, DATE) — the meter's grain; a new date on a known pair is a distinct billable/valuable event. |
| **usable** | The row's domain is consumable by DS13 (in `website_crawl_verticals`, minus a webmail blocklist) OR DS19 (composite key in `product_categorization` with category id >= 900000; NO blocklist — the permissive path). Usable = creditable. |
| **touched** | An IP the source delivered at least once in the 37-day membership window. |
| **sole** | Touched by that source and by NO other source (free logs included) — the strictest uniqueness cohort. |
| **solo** | Touched by that source and by neither FREE log (other paid vendors ignored) — "the vendor as the only paid source". |
| **free_logs union (ds 99)** | guid_log + augmentor treated as ONE source; its "sole" = no PAID vendor delivered the IP. Union counts are NOT the two per-log columns summed. |
| **T2 / T1** | Dependency value: T2 = media spend on the source's sole IPs × 52 (the ceiling: everything served on IPs only it knew); T1 = the subset where a high score gated the serve (the provable floor). |
| **holder mask** | Per pair/visit-day/IP, a 10-bit integer of which sources delivered it (bit order: ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9). Mask histograms let ANY keep-set's coverage be computed exactly without rescanning. |
| **CIL** | `dw-main-silver.logdata.cost_impression_log` — won/served impressions with media spend and household_score. |
| **MM / MNTN Match** | The audience product this data feeds (`used_in_mntn_match` in the vendor registry). "MM-targeted serve" = an impression on an MM-audience campaign; a row is creditable only if it reached DS13 or DS19 (the MM consumers). |
| **Metered / flat / free** | Billing classes: metered $0.50-CPM vendors = ds 24, 28, 33, 36, 40; flat-fee = 25, 26, 39; free internal = 23, 30. (q3b's reassignment classes and several headers rely on this split.) |
| **RTC** | Realtime Conquest — a bid path that stamps score 10000 via `model_params` (`realtime_conquest_score=10000`). "Scored non-RTC" excludes it so T1 only counts organically high-scored serves. |
| **Fangorn / DS46** | "Peak Performance v2" — the newer vertical-scoring component. Built from guid_log only (vendor-insulated); relevant to claims about what vendor removal cannot affect. |
| **VR / IVR** | Visit rate: site visits per won impression (from `clickpass_log` joined per ad_served_id). |
| **household_score tiers** | HI = 10000, PP = 8000, high-graduated 6666–9999 (excl. 8000), mid 3333–6665, max-reach 1–3332, unscored <= 0. RT rows carry −1. |

## Windows (identical across the whole package)

- Delivery/stock metrics: **30 days**, `dt 2026-06-02 .. 2026-07-01`.
- Serving/performance: **37-day svs membership union** (2026-06-02 .. 07-08) × **valuation
  week** `2026-07-02 .. 07-08` in CIL; visit/conversion trails run to 07-10.
- Bills: June 2026 meter month, annualized ×12.

## How to run

- Queries marked **console** in MANIFEST paste directly into the BigQuery console
  (standard SQL, project `dw-main-silver`).
- Queries using `svs`/`wcv`/`pc` need the bq CLI with temp external table definitions —
  the exact command is in each file's header (`-- Run` block); the generic pattern is in
  MANIFEST §"How to run". You need GCS read access to `gs://mntn-data-archive-prod/`.
- **The `deck_d1..d7` files are the fast path**: seven self-contained queries, one per
  deck-sheet block, whose Run blocks use plain `bq query` and run AS-IS from the folder
  holding the files (prereqs stated per file). Start there if you're validating the deck.
- **The q*.sql headers invoke `bq_run.sh` — an internal wrapper you don't have.** It only
  adds perf logging around `bq query`. To run any q*.sql command: substitute `bq query`
  for `bash .claude/scripts/bq_run.sh`, drop the `--ticket`/`--label` flags, and point the
  `.sql`/output paths at your copy of this folder. Nothing else changes.
- Cost classes are in MANIFEST — the **BIG** ones scan 5–40 TB and take 1–3 hours; dry-run
  first (`bq query --dry_run ...`) and run them deliberately.
- `q14_gcs_ingest_bytes.sh` is a gsutil measurement (not SQL); its recorded samples and
  integration arithmetic are embedded in its own header.

## Independent checks you can run (the package's own anchors)

1. **Meter identity**: q0 — billed impressions × contract CPM must equal billed usage exactly.
2. **Mask consistency**: single-bit mask rows in q3b must reproduce q3's sole_pairs; q3d's
   HI counts (masks) match q5's touched HI at ratio ~1.000.
3. **Cohort supersets**: q8b's solo cohort must be >= q6's sole cohort on every metric.
   (q8b's HI/PP tier counts vs q3d's mask counts is a DIAGNOSTIC comparison, not an
   equality — raw vs usable membership lenses differ by 3-10% for clean vendors and
   +55-68% for Sovrn; see MANIFEST anchors.)
4. **Cross-scan totals**: q7b imps == q6 imps; q7c imps == q7b imps (same cohorts, same week).
5. **Boundary identity**: dropping ALL metered vendors must recover exactly the total
   metered bills ($812,397/yr in the June snapshot).
6. **Visit basis**: v01 shows deduplicated `ui_visits` == `clickpass_log` within +0.5%;
   v02 shows why conversions must be deduplicated per attribution model (~3–4× fan-out).

## If your numbers differ slightly

`website_crawl_verticals` and `product_categorization` are **live snapshots** — reruns on a
different day can drift the usable-domain universe by <0.1–0.5%. The billing table
(`coredw.usage_reporting_data`) has **month-end snapshots only** (mid-month `dt` filters
return empty), and its credit regime changed in May 2026 (fractional split before, integer
single-credit after) — never mix regimes. Larger disagreements: check your window bounds
first, then the IPv4 filter (`ip NOT LIKE '%:%'` on BOTH join sides).

## Reading order for a full validation

Run/skim in MANIFEST order (q0 → q15c). For a spot-check instead: q0 (bills), q2c (usable
funnel), q3/q3b (uniqueness + masks), q6 (T2 values), q7/q7f (sole-cohort performance and
the darkness adjudication), then any of the scenario queries (q8, q13, q15 families).
