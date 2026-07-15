-- ============================================================================
-- DDP quality-score runbook, STEP 13a: DS19-ONLY universe — coverage + category sizes
-- Claim: every prior coverage number (free logs 60.4% pair / 59.4% visit-day) is on the
-- DS13-OR-DS19 UNION universe. DS19 ("MM Core" / Keyword-Only — unlocks the Max Reach
-- tier) is the permissive consumer (no blocklist, no reg-domain parse gate), so its
-- free-log coverage can differ materially. This scan builds the DS19-ONLY universe:
-- holder masks for any keep-set, the true path-grain check, and per-keyword-category
-- audience sizes under {all | free-only | k=4}.
--
-- DS19 membership (mirrors q2c/airflow-ti exactly): url non-empty, not infra
-- (steelhouse/googlesyndication/gtm), composite_key = CONCAT(SPLIT(url,'?')[0],'_1')
-- IN product_categorization WHERE any data_source_category_id >= 900000.
--
-- Output: ONE CSV (rec, k1, k2, v1, v2, v3):
--   rec='pair'  k1=10-bit holder mask (ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9)
--               v1 = (ip, REG_DOMAIN(composite-matched url)) pairs on DS19-consumable
--               domains (IPv4) -> ANY keep-set's DS19 pair coverage
--   rec='trip'  same at (ip, dom, dt) visit-day grain
--   rec='path'  k1='pairs': v1 = distinct (ip, composite_key) TRUE-grain matched pairs
--               (all sources), v2 = covered by free logs (bits 0|5), v3 = covered by k4
--               (free + 5x5 + Predactiv + 33Across + 33A API = mask 573)
--   rec='cat'   k1 = data_source_category_id, k2 = keyword name (taxonomy join; '' if
--               unmapped), v1/v2/v3 = member IPs under all / free-only / k4
--   rec='ds'    k1 = vendor ds, v1 = DS19-matched rows (EXACT q2c ds19_cat replica:
--               NO ip filter) -> anchor vs q2c rows_ds19_cat
--
-- Validation anchors: rec ds == q2c rows_ds19_cat per vendor (same 30d window; pc is a
-- live snapshot - tolerate <0.5%); free coverage from 'pair' vs 'path' = the domain-vs-
-- path grain-fidelity gap (report, expect same order); Sigma cat v1 >= path v1 (fan-out).
--
-- BIG SCAN — PRICED HONESTLY (adversarial review 2026-07-15): 5 svs root-to-leaf reads
-- + ~6 pc reads (pc alone is ~1.08TB/read) ~= 40TB, ~2-3h. Background, never preempt.
-- Post-run check REQUIRED: the CSV must contain all 5 rec types (ds/path/pair/trip/cat)
-- and >=1 'ds' row — bq truncates silently at --max_rows.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q13a ds19 universe" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=60000 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q13a_ds19_universe.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q13a_ds19_universe.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30
-- ============================================================================

WITH pc_k AS (
  SELECT DISTINCT composite_key
  FROM pc
  WHERE (SELECT COUNT(*) FROM UNNEST(data_source_category_id.list) x
         WHERE SAFE_CAST(x.element AS INT64) >= 900000) > 0
),

pc_cat AS (
  SELECT DISTINCT composite_key, SAFE_CAST(x.element AS INT64) AS dsc
  FROM pc, UNNEST(data_source_category_id.list) x
  WHERE SAFE_CAST(x.element AS INT64) >= 900000
),

matched AS (
  -- svs rows that reach DS19 (q2c ds19_cat gates exactly; NO ip filter here)
  SELECT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom,
    s.dt,
    k.composite_key AS ck
  FROM svs s
  JOIN pc_k k ON CONCAT(SPLIT(s.url, '?')[SAFE_OFFSET(0)], '_1') = k.composite_key
  WHERE NOT (s.url IS NULL OR s.url = '')
    AND NOT (s.url LIKE '%steelhouse.com%' OR s.url LIKE '%googlesyndication.com%'
             OR s.url LIKE '%gtm-msr.appspot.com/render%')
),

ds_anchor AS (
  SELECT ds, COUNT(*) AS n FROM matched GROUP BY ds
),

trip_g AS (
  SELECT ip, dom, dt,
         SUM(DISTINCT 1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                                    WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                                    WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m
  FROM (SELECT DISTINCT ds, ip, dom, dt FROM matched
        WHERE ip IS NOT NULL AND ip NOT LIKE '%:%' AND dom IS NOT NULL)
  GROUP BY 1, 2, 3
),

pair_g AS (
  SELECT ip, dom, BIT_OR(m) AS m
  FROM trip_g
  GROUP BY 1, 2
),

path_g AS (
  SELECT ip, ck,
         SUM(DISTINCT 1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                                    WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                                    WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m
  FROM (SELECT DISTINCT ds, ip, ck FROM matched
        WHERE ip IS NOT NULL AND ip NOT LIKE '%:%')
  GROUP BY 1, 2
),

cat_ip AS (
  SELECT c.dsc, p.ip, BIT_OR(p.m) AS m
  FROM path_g p
  JOIN pc_cat c ON p.ck = c.composite_key
  GROUP BY 1, 2
),

cat_agg AS (
  SELECT dsc,
         COUNT(*) AS ips_all,
         COUNTIF(m & 33 != 0) AS ips_free,
         COUNTIF(m & 573 != 0) AS ips_k4
  FROM cat_ip
  GROUP BY dsc
)

SELECT 'pair' AS rec, CAST(m AS STRING) AS k1, '' AS k2,
       COUNT(*) AS v1, 0 AS v2, 0 AS v3
FROM pair_g
GROUP BY m

UNION ALL

SELECT 'trip', CAST(m AS STRING), '', COUNT(*), 0, 0
FROM trip_g
GROUP BY m

UNION ALL

SELECT 'path', 'pairs', '',
       COUNT(*),
       COUNTIF(m & 33 != 0),
       COUNTIF(m & 573 != 0)
FROM path_g

UNION ALL

SELECT 'cat', CAST(a.dsc AS STRING), COALESCE(t.name, ''),
       a.ips_all, a.ips_free, a.ips_k4
FROM cat_agg a
LEFT JOIN `dw-main-bronze.external.tpa__mntn_matched_taxonomy__v2` t
  ON a.dsc = t.data_source_category_id

UNION ALL

SELECT 'ds', CAST(ds AS STRING), '', n, 0, 0
FROM ds_anchor

-- small/critical rec types FIRST: pc has 25,861 distinct DS19 category ids (verified),
-- so 'cat' alone can brush a too-small --max_rows; anchors must survive any truncation
ORDER BY CASE rec WHEN 'ds' THEN 0 WHEN 'path' THEN 1 WHEN 'pair' THEN 2
                  WHEN 'trip' THEN 3 ELSE 4 END, k1, k2;
