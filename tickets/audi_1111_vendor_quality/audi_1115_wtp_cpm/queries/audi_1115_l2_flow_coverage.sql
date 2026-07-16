-- ============================================================================
-- AUDI-1115 L2: flow-filtered free-log coverage → vendor-unique triples
--
-- Claim: ONE 60d svs scan (30d measurement window 2026-06-02..2026-07-01 +
-- 30d lookback runway 2026-05-03..2026-06-01) computes, per paid vendor, the
-- unique usable triples (ip x REG_DOMAIN(url) x date) under THREE free-log
-- credit rules, plus the free-log coverage of the universe under each rule:
--
--   sameday_cnt : free log covers triple (ip,dom,D) iff it delivered the pair
--                 ON day D (the deck_d1 convention — the ANCHOR: vendor rows
--                 must reproduce deck_d1 trips_standalone on the same window)
--   flow_cnt    : free log covers (ip,dom,D) iff it delivered (ip,dom) on any
--                 day in [D-30, D-1] (the 2026-07-16 meeting rule: same-day
--                 presence earns NO credit — everything we bid on is
--                 definitionally in augmentor_log that day, so day-of
--                 presence is circular; prior-window presence is the
--                 non-circular evidence we already knew the pair). THE L2
--                 UNIT COUNT per vendor = its flow_cnt.
--   strict_cnt  : covered under NEITHER rule (no prior-window AND no same-day
--                 presence) for vendor rows; covered under BOTH for free rows.
--                 strict_cnt <= LEAST(sameday_cnt, flow_cnt) always.
--
-- NOTE sameday vs flow are NOT ordered a priori: a triple whose pair was
-- never seen before today but is in aug_log today counts unique-under-flow
-- but covered-under-sameday, and vice versa. Report all three; do not assume
-- flow >= sameday per vendor.
--
-- Rows: rec='vendor' (8 paid: ds 24,25,26,28,33,36,39,40) with cnt = vendor
-- triples NOT covered by free logs under each rule; rec='free' (ds 23=guid,
-- 30=augmentor, 99=union) with cnt = universe triples COVERED under each
-- rule; rec='universe' = denominator (measurement window only).
--
-- ARCHITECTURE NOTE (cost): CTE re-references RE-READ temp external tables in
-- BigQuery, so the chain is LINEAR — usable_dom -> trips -> trip_g -> flow ->
-- mh -> hist -> final; every CTE that transitively reads svs/wcv/pc is
-- referenced EXACTLY ONCE. The lookback is implemented as an analytic
-- MAX(...) OVER (PARTITION BY ip, dom ORDER BY UNIX_DATE(dt) RANGE BETWEEN
-- 30 PRECEDING AND 1 PRECEDING) so no self-join re-reads the externals. The
-- measurement-window filter is applied AFTER the analytic (lookback days are
-- frame input only).
--
-- Grain/hygiene: verbatim deck_d1 — IPv4 only; usable_dom = wcv minus webmail
-- blocklist OR pc composite-key domains with category id >= 900000.
--
-- Expected reconciliation: universe ~= deck_d1 universe (13,286,674,041 on
-- 2026-07-16 snapshots; live wcv/pc drift <0.1%); per-vendor sameday_cnt ==
-- deck_d1 trips_standalone (e.g. ds 28 ~= 2,153,594,075); free-union sameday
-- coverage ~= 7,887,062,821 (59.36%).
--
-- BIG SCAN (svs 60d + wcv + pc, single pass + billions-of-partitions analytic
-- shuffle; ~2-3h) — dry-run first, run in background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,5,3); print(' '.join(str(s+t.timedelta(i)) for i in range(60)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --location=us-central1 --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' audi_1115_l2_flow_coverage.sql)" \
--     > audi_1115_l2_flow_coverage.csv
--
-- Parameters: LOOKBACK_START = 2026-05-03, WINDOW_START = 2026-06-02,
--             WINDOW_END = 2026-07-01, FLOW_DAYS = 30
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
    CAST(s.dt AS DATE) AS dt
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

-- lagged free-log presence per pair: seen in [D-30, D-1]?
flow AS (
  SELECT ip, dom, dt, tmask,
         MAX(IF((tmask & 1)  != 0, 1, 0)) OVER w30 AS guid_prior,
         MAX(IF((tmask & 32) != 0, 1, 0)) OVER w30 AS aug_prior
  FROM trip_g
  WINDOW w30 AS (PARTITION BY ip, dom ORDER BY UNIX_DATE(dt)
                 RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING)
),

-- measurement-window histogram over (paid mask, same-day free mask, flow free mask)
mh AS (
  SELECT (tmask & 990) AS pm,
         (tmask & 33)  AS sf,
         (IFNULL(guid_prior, 0) * 1 + IFNULL(aug_prior, 0) * 32) AS ff,
         COUNT(*) AS n
  FROM flow
  WHERE dt >= DATE '2026-06-02'
  GROUP BY 1, 2, 3
),

hist AS (
  SELECT ARRAY_AGG(STRUCT(pm, sf, ff, n)) AS h
  FROM mh
),

per AS (
  SELECT
    h,
    (SELECT SUM(x.n) FROM UNNEST(h) x) AS universe,
    ARRAY(
      SELECT AS STRUCT s.ds, s.bit,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE ((x.pm >> s.bit) & 1) = 1) AS total,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE ((x.pm >> s.bit) & 1) = 1 AND x.sf = 0) AS uniq_sameday,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE ((x.pm >> s.bit) & 1) = 1 AND x.ff = 0) AS uniq_flow,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE ((x.pm >> s.bit) & 1) = 1 AND x.ff = 0 AND x.sf = 0) AS uniq_strict
      FROM UNNEST([STRUCT(24 AS ds, 1 AS bit), (25, 2), (26, 3), (28, 4),
                   (33, 6), (36, 7), (39, 8), (40, 9)]) s
    ) AS pds,
    ARRAY(
      SELECT AS STRUCT f.ds, f.fbits,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE (x.sf & f.fbits) != 0) AS cov_sameday,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE (x.ff & f.fbits) != 0) AS cov_flow,
        (SELECT SUM(x.n) FROM UNNEST(h) x
         WHERE (x.sf & f.fbits) != 0 AND (x.ff & f.fbits) != 0) AS cov_strict
      FROM UNNEST([STRUCT(23 AS ds, 1 AS fbits), (30, 32), (99, 33)]) f
    ) AS fds
  FROM hist
)

SELECT o.rec, o.ds, o.trips_total, o.pct_universe,
       o.sameday_cnt, o.flow_cnt, o.strict_cnt, o.pct_flow
FROM per r,
UNNEST(ARRAY_CONCAT(
  ARRAY(
    SELECT AS STRUCT
      'vendor' AS rec,
      p.ds AS ds,
      p.total AS trips_total,
      ROUND(100 * p.total / r.universe, 2) AS pct_universe,
      p.uniq_sameday AS sameday_cnt,
      p.uniq_flow AS flow_cnt,
      p.uniq_strict AS strict_cnt,
      ROUND(100 * p.uniq_flow / r.universe, 2) AS pct_flow
    FROM UNNEST(r.pds) p
  ),
  ARRAY(
    SELECT AS STRUCT
      'free' AS rec,
      f.ds AS ds,
      CAST(NULL AS INT64) AS trips_total,
      CAST(NULL AS FLOAT64) AS pct_universe,
      f.cov_sameday AS sameday_cnt,
      f.cov_flow AS flow_cnt,
      f.cov_strict AS strict_cnt,
      ROUND(100 * f.cov_flow / r.universe, 2) AS pct_flow
    FROM UNNEST(r.fds) f
  ),
  [STRUCT(
    'universe' AS rec,
    CAST(NULL AS INT64) AS ds,
    r.universe AS trips_total,
    100.0 AS pct_universe,
    CAST(NULL AS INT64) AS sameday_cnt,
    CAST(NULL AS INT64) AS flow_cnt,
    CAST(NULL AS INT64) AS strict_cnt,
    CAST(NULL AS FLOAT64) AS pct_flow
  )]
)) o
ORDER BY o.rec, o.trips_total DESC, o.ds
