-- ============================================================================
-- DDP quality-score runbook, STEP 3b: credit reassignment + holder-set signatures
-- Claim: dropping a metered vendor does NOT save its bill — credits reassign to the
-- next-first reporter; savings only where that holder is FREE or absent. This scan
-- measures, per pair, WHO the earliest other holder is, and captures the full
-- holder-set signature histogram so any roster subset (2^10) can be evaluated
-- exactly without rescanning.
--
-- Grain: (ip, REG_DOMAIN(url)) over the 30d window, usable domains only (wcv∪pc,
-- OR-semantics, mirrors q3). Ordering: MIN(dt) per (pair, source) approximates
-- first-reporter (true billing = per (ip,url,DATE) first report; see calibration).
-- Tie-break on equal min_dt: free > flat-fee > metered (cost-optimistic; disclosed).
--
-- Output: ONE CSV, three record types (rec column):
--   rec='mask'     k1=holder bitmask (bit order ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9)
--                  n_pairs, n_ips  -> the sufficient statistic for all-subset simulation
--   rec='reassign' k1=metered vendor ds (metered = 24,28,33,36,40; flat-fee =
--                  25,26,39; free = 23,30 — see guide glossary), k2=earliest-other-holder class
--                  {none, free_first, free_later, flat_fee, metered}, n_pairs
--   rec='tie'      k1=ds a, k2=ds b, n_pairs where a and b share the same min_dt (co-first)
--
-- Validation anchor: mask rows with single-bit masks must reproduce q3 sole_pairs.
-- Calibration follow-up (cheap, run after): 1-2% IP-hash sample at day+full-URL grain --
--   WHERE MOD(ABS(FARM_FINGERPRINT(ip)), 100) < 2, per (ip, url, dt) first-reporter --
--   to measure how often per-day first differs from pair-grain MIN(dt) first, and the
--   free logs' same-day win rate. Escalate to full day-grain only if divergence >10-15%.
--
-- THE BIG SCAN (~8.5 TB, ~1h) — launch in background, never preempt.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q3b credit reassignment" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=2000 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q3b_credit_reassignment.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q3b_credit_reassignment.csv
--
-- Parameters (in the URIS loop above): SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30
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

pairs AS (
  SELECT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom,
    MIN(s.dt) AS min_dt
  FROM svs s
  JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
  GROUP BY 1, 2, 3
),

pairs_g AS (
  SELECT
    ip,
    dom,
    ARRAY_AGG(STRUCT(ds, min_dt)) AS hs,
    SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                      WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                      WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS holder_mask
  FROM pairs
  GROUP BY ip, dom
),

mask_agg AS (
  SELECT holder_mask, COUNT(*) AS n_pairs, APPROX_COUNT_DISTINCT(ip) AS n_ips
  FROM pairs_g
  GROUP BY holder_mask
),

reassign_agg AS (
  SELECT
    v.ds AS vds,
    IF(ARRAY_LENGTH(hs) = 1, 'none',
       (SELECT CASE
                 WHEN o.ds IN (23, 30) AND o.min_dt <= v.min_dt THEN 'free_first'
                 WHEN o.ds IN (23, 30) THEN 'free_later'
                 WHEN o.ds IN (25, 26, 39) THEN 'flat_fee'
                 ELSE 'metered'
               END
        FROM UNNEST(hs) o
        WHERE o.ds != v.ds
        ORDER BY o.min_dt,
                 CASE WHEN o.ds IN (23, 30) THEN 0 WHEN o.ds IN (25, 26, 39) THEN 1 ELSE 2 END
        LIMIT 1)) AS cls,
    COUNT(*) AS n_pairs
  FROM pairs_g, UNNEST(hs) v
  WHERE v.ds IN (24, 28, 33, 36, 40)
  GROUP BY vds, cls
),

tie_agg AS (
  SELECT v.ds AS a, o.ds AS b, COUNT(*) AS n_pairs
  FROM pairs_g, UNNEST(hs) v, UNNEST(hs) o
  WHERE o.ds != v.ds AND o.min_dt = v.min_dt
  GROUP BY a, b
)

SELECT 'mask' AS rec, CAST(holder_mask AS STRING) AS k1, '' AS k2, n_pairs, n_ips
FROM mask_agg
UNION ALL
SELECT 'reassign', CAST(vds AS STRING), cls, n_pairs, 0
FROM reassign_agg
UNION ALL
SELECT 'tie', CAST(a AS STRING), CAST(b AS STRING), n_pairs, 0
FROM tie_agg
ORDER BY rec, k1, k2;
