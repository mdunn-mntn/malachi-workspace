-- ============================================================================
-- DDP quality-score runbook, STEP 3d: HI/PP coverage masks + per-vertical before/after
-- Claim: pair coverage isn't the KPI that breaks when vendors drop — HI/PP audience
-- coverage is (MM/PP audiences carry size scores; verticals shrink). This scan makes
-- score-tier coverage subset-evaluable and sizes each DS13 vertical before/after.
--   1. Tier holder-mask histograms: for every SERVED-SCORED IP (CIL valuation-week
--      per-IP MAX(household_score) — same definition as q5's HI/PP workbook rows:
--      HI = 10000, PP = 8000, high-grad 6666-9999 excl 8000), the bitmask of svs
--      sources that delivered the IP in the 37d union -> ANY keep-set's HI/PP
--      coverage = mask-histogram lookup (q3b machinery at score grain).
--   2. Vertical sizes: per wcv vertical_name, unique svs IPs reachable under
--      {all sources | free logs only | k4 keep-set (5x5+Predactiv+33Across+33A API)}.
--      Proxy for DS13 vertical audience size (svs-reachable IPs on the vertical's domains).
--
-- Output: ONE CSV, record types (rec, k1 STRING, k2 STRING, n):
--   rec='hi'|'pp'|'hg'  k1=holder bitmask (bit order ds 23,24,25,26,28,30,33,36,39,40
--                       = bits 0..9), k2=NULL, n=IPs in that tier with that mask
--   rec='vert'          k1=vertical_name, k2 in {all, free, k4}, n=unique IPs
--
-- Validation anchors: Σ hi-mask n across masks containing source s ≈ q5 touched hi_10000
-- for s (same windows); vert 'all' totals rank-consistent with q4 domain classification.
--
-- BIG SCAN (svs 37d + wcv + pc + CIL week; ~1h) — background, never preempt.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q3d score+vertical coverage" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=5000 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q3d_score_vertical_coverage.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q3d_score_vertical_coverage.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 37; VALUE week 2026-07-02..08
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

sv AS (
  SELECT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom
  FROM svs s
  JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
  GROUP BY 1, 2, 3
),

ip_mask AS (
  SELECT ip,
         SUM(DISTINCT 1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                                    WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                                    WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m
  FROM (SELECT DISTINCT ds, ip FROM sv)
  GROUP BY ip
),

scored AS (
  SELECT ip, MAX(household_score) AS sc
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

tiered AS (
  SELECT im.m,
         CASE WHEN s.sc = 10000 THEN 'hi'
              WHEN s.sc = 8000 THEN 'pp'
              WHEN s.sc BETWEEN 6666 AND 9999 AND s.sc <> 8000 THEN 'hg'
         END AS tier
  FROM ip_mask im
  JOIN scored s USING (ip)
  WHERE s.sc >= 6666
),

vert_ip AS (
  SELECT w.vertical_name AS vert, v.ip,
         SUM(DISTINCT 1 << (CASE v.ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                                      WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                                      WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m
  FROM sv v
  JOIN (SELECT DISTINCT domain_name, vertical_name FROM wcv
        WHERE domain_name NOT IN ('yahoo.com', 'aol.com', 'easybrain.com')) w
    ON v.dom = w.domain_name
  GROUP BY 1, 2
)

SELECT tier AS rec, CAST(m AS STRING) AS k1, CAST(NULL AS STRING) AS k2, COUNT(*) AS n
FROM tiered
WHERE tier IS NOT NULL
GROUP BY 1, 2

UNION ALL

SELECT 'vert', vert, 'all', COUNT(DISTINCT ip)
FROM vert_ip
GROUP BY 2

UNION ALL

SELECT 'vert', vert, 'free', COUNT(DISTINCT ip)
FROM vert_ip
WHERE m & ((1 << 0) | (1 << 5)) != 0
GROUP BY 2

UNION ALL

SELECT 'vert', vert, 'k4', COUNT(DISTINCT ip)
FROM vert_ip
WHERE m & ((1 << 0) | (1 << 5) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 9)) != 0
GROUP BY 2

ORDER BY rec, k1, k2;
