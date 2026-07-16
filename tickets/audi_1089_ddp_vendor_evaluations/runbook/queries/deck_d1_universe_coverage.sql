-- ============================================================================
-- AUDI-1089 DECK QUERY D1 of 7: per-source coverage of the visit-day universe
-- FILLS: deck sheet BLOCK 1 (rows 1-7) cols B "Total IP x Domain x Date Pairs",
--        C "% of total universe", D "cumulative % of the universe";
--        plus the free-cohold % that BLOCK 3 (rows 18-24) multiplies against D3's
--        bills: bill_after = bill_annualized x (1 - free_cohold_pct/100).
--
-- Claim: ONE 30d scan builds the triple-grain (ip x REG_DOMAIN(url) x date)
-- holder-mask histogram over USABLE domains (consumable by DS13 or DS19 —
-- mirrors the workbook's q3c exactly), then derives per source:
--   trips_total        = distinct usable triples the source delivered
--   pct_universe       = trips_total / universe (universe = distinct triples
--                        across ALL 10 sources)
--   cum_pct_universe   = UNION coverage of the top-N sources ranked by
--                        trips_total desc, ties broken by ds (deduplicated —
--                        NOT a running sum of pct_universe, which would
--                        double-count overlap)
--   trips_standalone   = triples held by the source and NEITHER free log (free
--                        logs themselves: vs the other free log) — the renewal
--                        counterfactual column
--   free_cohold_pct    = % of the source's triples a free log ALSO holds — the
--                        AUDI-1093 preemption share (feeds BLOCK 3)
-- rec='free_union' = guid+augmentor as ONE source (fills the free_logs row: its
-- total = free coverage of the universe; standalone = held by NO paid vendor).
-- rec='universe' = the denominator row.
--
-- ARCHITECTURE NOTE (cost): CTE re-references RE-READ temp external tables in
-- BigQuery (house-measured: a 3-reference layout tripled a ~10TB scan). So the
-- histogram is collapsed to a single ARRAY-carrying row (hist, <= 1024 mask
-- entries) and every downstream step is pure array arithmetic threaded through
-- a LINEAR chain — each CTE that transitively reads svs/wcv/pc is referenced
-- EXACTLY ONCE. The externals are scanned once, as advertised.
--
-- Grain/hygiene: IPv4 only (ip NOT LIKE '%:%'); usable_dom = wcv minus the
-- webmail blocklist OR pc composite-key domains with category id >= 900000
-- (verbatim q3c).
--
-- Expected reconciliation vs the measured outputs/run_2026_07_10 (live wcv/pc
-- snapshots drift <0.1% between run days): universe 13,286,670,656; augmentor
-- total 6,483,729,112 / standalone 6,464,053,715; 33Across standalone
-- 2,153,592,512; free-union total 7,887,061,977 (59.4%).
--
-- BIG SCAN (svs 30d + wcv + pc, single pass; ~1-1.5h) — dry-run first, run in
-- background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; GCS read on mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' deck_d1_universe_coverage.sql)" \
--     > deck_d1_universe_coverage.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30
-- Bit order (house convention): ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9;
-- free bits mask = 33 (guid bit 0 + augmentor bit 5); paid bits mask = 990.
-- ============================================================================

WITH usable_dom AS (
  SELECT DISTINCT domain_name AS dom
  FROM wcv
  WHERE domain_name NOT IN ('yahoo.com', 'aol.com', 'easybrain.com')
  UNION DISTINCT
  SELECT DISTINCT NET.REG_DOMAIN(composite_key) AS dom
  FROM pc
  WHERE NET.REG_DOMAIN(composite_key) IS NOT NULL
    AND (SELECT COUNT(*) FROM UNNEST(data_source_category_id.list) x
         WHERE SAFE_CAST(x.element AS INT64) >= 900000) > 0
),

trips AS (
  SELECT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom,
    s.dt
  FROM svs s
  JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
  GROUP BY 1, 2, 3, 4
),

trip_g AS (
  SELECT ip, dom, dt,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS tmask
  FROM trips
  GROUP BY ip, dom, dt
),

mh AS (
  SELECT tmask, COUNT(*) AS n
  FROM trip_g
  GROUP BY tmask
),

-- collapse the histogram into ONE array-carrying row; the only reference to mh
hist AS (
  SELECT ARRAY_AGG(STRUCT(tmask AS m, n)) AS h
  FROM mh
),

-- per-source aggregates as array arithmetic (no external reads from here down)
per AS (
  SELECT
    h,
    (SELECT SUM(x.n) FROM UNNEST(h) x) AS universe,
    ARRAY(
      SELECT AS STRUCT
        s.ds,
        s.bit,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE ((x.m >> s.bit) & 1) = 1) AS total,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE ((x.m >> s.bit) & 1) = 1 AND (x.m & s.freebits) = 0) AS standalone,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE ((x.m >> s.bit) & 1) = 1 AND (x.m & s.freebits) != 0) AS free_cohold
      FROM UNNEST([STRUCT(23 AS ds, 0 AS bit, 32 AS freebits), (24, 1, 33), (25, 2, 33),
                   (26, 3, 33), (28, 4, 33), (30, 5, 1), (33, 6, 33), (36, 7, 33),
                   (39, 8, 33), (40, 9, 33)]) s
    ) AS pds
  FROM hist
),

ranked AS (
  SELECT
    h,
    universe,
    ARRAY(SELECT AS STRUCT p.*,
                 ROW_NUMBER() OVER (ORDER BY p.total DESC, p.ds) AS rn
          FROM UNNEST(pds) p) AS pds
  FROM per
)

SELECT o.rec, o.ds, o.rank_by_total, o.trips_total, o.pct_universe,
       o.cum_trips, o.cum_pct_universe, o.trips_standalone,
       o.standalone_pct_universe, o.free_cohold_pct
FROM ranked r,
UNNEST(ARRAY_CONCAT(
  ARRAY(
    SELECT AS STRUCT
      'source' AS rec,
      p.ds AS ds,
      p.rn AS rank_by_total,
      p.total AS trips_total,
      ROUND(100 * p.total / r.universe, 2) AS pct_universe,
      (SELECT SUM(x.n) FROM UNNEST(r.h) x
       WHERE (x.m & (SELECT SUM(1 << q.bit) FROM UNNEST(r.pds) q WHERE q.rn <= p.rn)) != 0
      ) AS cum_trips,
      ROUND(100 * (SELECT SUM(x.n) FROM UNNEST(r.h) x
       WHERE (x.m & (SELECT SUM(1 << q.bit) FROM UNNEST(r.pds) q WHERE q.rn <= p.rn)) != 0
      ) / r.universe, 2) AS cum_pct_universe,
      p.standalone AS trips_standalone,
      ROUND(100 * p.standalone / r.universe, 2) AS standalone_pct_universe,
      ROUND(100 * p.free_cohold / p.total, 2) AS free_cohold_pct
    FROM UNNEST(r.pds) p
  ),
  [STRUCT(
    'free_union' AS rec,
    99 AS ds,
    CAST(NULL AS INT64) AS rank_by_total,
    (SELECT SUM(x.n) FROM UNNEST(r.h) x WHERE (x.m & 33) != 0) AS trips_total,
    ROUND(100 * (SELECT SUM(x.n) FROM UNNEST(r.h) x WHERE (x.m & 33) != 0)
          / r.universe, 2) AS pct_universe,
    CAST(NULL AS INT64) AS cum_trips,
    CAST(NULL AS FLOAT64) AS cum_pct_universe,
    (SELECT SUM(x.n) FROM UNNEST(r.h) x
     WHERE (x.m & 33) != 0 AND (x.m & 990) = 0) AS trips_standalone,
    ROUND(100 * (SELECT SUM(x.n) FROM UNNEST(r.h) x
     WHERE (x.m & 33) != 0 AND (x.m & 990) = 0) / r.universe, 2) AS standalone_pct_universe,
    CAST(NULL AS FLOAT64) AS free_cohold_pct
  )],
  [STRUCT(
    'universe' AS rec,
    CAST(NULL AS INT64) AS ds,
    CAST(NULL AS INT64) AS rank_by_total,
    r.universe AS trips_total,
    100.0 AS pct_universe,
    CAST(NULL AS INT64) AS cum_trips,
    CAST(NULL AS FLOAT64) AS cum_pct_universe,
    CAST(NULL AS INT64) AS trips_standalone,
    CAST(NULL AS FLOAT64) AS standalone_pct_universe,
    CAST(NULL AS FLOAT64) AS free_cohold_pct
  )]
)) o
ORDER BY o.rec, o.rank_by_total, o.ds;
