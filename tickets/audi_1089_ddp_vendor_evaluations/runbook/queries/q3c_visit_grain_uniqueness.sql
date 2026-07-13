-- ============================================================================
-- DDP quality-score runbook, STEP 3c: VISIT-grain (ip x domain x DATE) uniqueness
-- Claim: the true unit of vendor value is the visit triple (ip, domain, date) —
-- a vendor re-delivering a known (ip,domain) pair on a NEW date is a recency
-- refresh (real targeting value: MM scores on a 30d recency window; the meter
-- credits per (ip,url,DAY)); the same triple from two vendors on the SAME date
-- is pure duplication. Pair-grain coverage (q3/q3b) gives refreshes zero credit —
-- this scan differentiates, per vendor:
--   sole_new_pair   = visit-day on a pair ONLY this vendor holds (brand-new signal)
--   sole_refresh    = date unique to this vendor on a pair others also hold
--   shared_same_day = same ip x domain x date delivered by >=1 other source (waste)
-- Plus the triple-grain holder-mask histogram -> coverage frontier in unique
-- VISITS (answers "free logs alone = X% of visits", not just X% of pairs).
--
-- Grain: (ip, REG_DOMAIN(url), dt) over the 30d window, usable domains only
-- (wcv OR pc, mirrors q3/q3b). IPv4 only.
--
-- Output: ONE CSV, two record types (rec column):
--   rec='mask'   k1=triple holder bitmask (bit order ds 23,24,25,26,28,30,33,36,39,40
--                = bits 0..9), k2=NULL, n=triples
--   rec='vendor' k1=ds, k2=class {sole_new_pair, sole_refresh, shared_same_day}, n=triples
--
-- Validation anchors: Σ vendor rows per ds = ds's distinct usable triples;
-- mask single-bit totals ≥ q3 sole_pairs (a sole pair has ≥1 sole triple).
--
-- THE BIG SCAN (larger than q3b — triple grain; ~1-1.5h) — background, never preempt.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q3c visit-grain uniqueness" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=2000 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q3c_visit_grain_uniqueness.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q3c_visit_grain_uniqueness.csv
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
  SELECT
    ip, dom, dt,
    SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                      WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                      WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS tmask,
    COUNT(*) AS nh
  FROM trips
  GROUP BY ip, dom, dt
),

pair_g AS (
  SELECT ip, dom, BIT_OR(tmask) AS pmask
  FROM trip_g
  GROUP BY ip, dom
),

classified AS (
  SELECT
    t.ds,
    CASE
      WHEN tg.nh = 1 AND pg.pmask = tg.tmask THEN 'sole_new_pair'
      WHEN tg.nh = 1 THEN 'sole_refresh'
      ELSE 'shared_same_day'
    END AS cls
  FROM trips t
  JOIN trip_g tg USING (ip, dom, dt)
  JOIN pair_g pg USING (ip, dom)
)

SELECT 'mask' AS rec, tmask AS k1, CAST(NULL AS STRING) AS k2, COUNT(*) AS n
FROM trip_g
GROUP BY 2

UNION ALL

SELECT 'vendor', ds, cls, COUNT(*)
FROM classified
GROUP BY 2, 3

ORDER BY rec, k1, k2;
