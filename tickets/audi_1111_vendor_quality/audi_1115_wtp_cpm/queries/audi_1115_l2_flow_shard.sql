-- ============================================================================
-- AUDI-1115 L2 (SHARDED): flow-filtered free-log coverage — histogram shard
--
-- WHY SHARDED: the single-query variant (audi_1115_l2_flow_coverage.sql) hit
-- BigQuery's hard 6-hour limit on 2026-07-16 — the per-(ip,dom) RANGE-window
-- sort over ~27B daily rows is too heavy for one job. This variant removes the
-- window sort entirely and splits the work 4 ways:
--   1. DAY-BITMASK: days are indexed 0..59 from 2026-05-03; per (ip,dom) the
--      guid/aug delivery days collapse into one INT64 bitmask each (BIT_OR of
--      1<<di). Flow credit for measurement day di (30..59) = mask AND the
--      window bits [di-30, di-1]:  wm = ((1<<di)-1) ^ ((1<<(di-30))-1).
--      Pure hash aggregation — no per-partition sort.
--   2. IP HASH SHARD: WHERE MOD(ABS(FARM_FINGERPRINT(ip)), 4) = __SHARD__.
--      Every (pm, sf, ff) histogram cell is additive across shards because
--      all rows of an (ip,dom) pair land in the same shard.
--
-- Output per shard: the raw histogram (pm = paid-vendor mask, sf = same-day
-- free mask, ff = flow free mask, n = triples), <= 4096 rows. The merge script
-- artifacts/audi_1115_l2_merge.py sums the 4 shards and emits the SAME final
-- table (and file name) the single-query variant would have produced, then
-- runs the deck_d1 anchors.
--
-- Definitions (identical to the single-query variant): measurement window
-- 2026-06-02..07-01 (di 30..59, upper bound BAKED IN); usable_dom verbatim
-- deck_d1; IPv4; flow credit = free log delivered the (ip,dom) on any day in
-- [D-30, D-1] (same-day earns nothing); sf = same-day free mask (deck_d1
-- anchor convention).
--
-- ARCHITECTURE NOTE (cost): externals read once per shard (4x total scan
-- bytes — accepted; the reservation runs shards concurrently). Linear CTE
-- chain; no CTE that reads externals is referenced twice.
--
-- BIG SCAN x4 (svs 60d each) — dry-run shard 0, then launch all 4 in
-- parallel, background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,5,3); print(' '.join(str(s+t.timedelta(i)) for i in range(60)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   for k in 0 1 2 3; do \
--     bq query \
--       --external_table_definition="svs::PARQUET=${URIS}" \
--       --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--       --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--       --use_legacy_sql=false --location=us-central1 --format=csv --max_rows=5000 --project_id=dw-main-silver \
--       "$(grep -v '^[[:space:]]*--' audi_1115_l2_flow_shard.sql | sed "s/__SHARD__/${k}/")" \
--       > audi_1115_l2_shard${k}.csv & \
--   done; wait
--   python3 ../artifacts/audi_1115_l2_merge.py   # writes audi_1115_l2_flow_coverage.csv + anchors
--
-- Parameters: LOOKBACK_START = 2026-05-03 (di 0), WINDOW = di 30..59
-- (2026-06-02..2026-07-01), NSHARDS = 4, SHARD = __SHARD__.
-- Bit order: ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9; free mask 33;
-- paid mask 990.
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
    AND MOD(ABS(FARM_FINGERPRINT(s.ip)), 4) = __SHARD__
  GROUP BY 1, 2, 3, 4
),

trip_g AS (
  SELECT ip, dom,
         DATE_DIFF(dt, DATE '2026-05-03', DAY) AS di,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS tmask
  FROM trips
  GROUP BY 1, 2, 3
),

pairs AS (
  SELECT ip, dom,
         ARRAY_AGG(STRUCT(di, tmask)) AS days,
         BIT_OR(IF((tmask & 1)  != 0, 1 << di, 0)) AS guid_days,
         BIT_OR(IF((tmask & 32) != 0, 1 << di, 0)) AS aug_days
  FROM trip_g
  WHERE di BETWEEN 0 AND 59
  GROUP BY ip, dom
)

SELECT
  (d.tmask & 990) AS pm,
  (d.tmask & 33)  AS sf,
  ( IF((p.guid_days & (((1 << d.di) - 1) ^ ((1 << (d.di - 30)) - 1))) != 0, 1,  0)
  + IF((p.aug_days  & (((1 << d.di) - 1) ^ ((1 << (d.di - 30)) - 1))) != 0, 32, 0)) AS ff,
  COUNT(*) AS n
FROM pairs p, UNNEST(p.days) d
WHERE d.di BETWEEN 30 AND 59
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
