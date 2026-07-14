-- ============================================================================
-- DDP quality-score runbook, STEP 8a: SOLO counterfactual — stock & freshness vs free logs
-- Claim: the numbers sheet's uniqueness rows measure each vendor against ALL other
-- sources (7 other paid vendors + both free logs), so paid-vendor overlap depresses
-- everyone. This scan computes each vendor's stock as if it were the ONLY paid source:
-- keep-set = {V} + free logs (guid_log DS23, augmentor DS30). "Solo" = held by V and
-- by NEITHER free log (for the free-log columns themselves: vs the OTHER free log —
-- one formula: other_free(23)=bit5, other_free(30)=bit0, else bits 0|5).
--
-- Grains (mirror the rows they fill):
--   stock solo_pairs / solo_ips : usable pairs/IPs, 30d (q3/q3b world — usable_dom gate)
--   stock solo_domains / solo_classified : ALL parsed domains, NO ip filter (q4B world;
--     classified = domain present in wcv, dedup'd, blocklist NOT removed — q4B parity)
--   fresh_pair : RAW pairs, IPv4 (q4A/q3_pair_recency world) — V's MAX(dt) vs the other
--     free logs' MAX(dt) on co-held pairs: fresher_than_free / tied_with_free / stale_vs_free
--   fresh_day  : usable visit-day triples (q3c world) — per triple held by V:
--     same_day_dup_with_free (other free co-holds the SAME triple), else
--     solo_new_pair (pair untouched by other free), else refresh_of_free_pair
--
-- Output: ONE CSV (rec, ds, k, v):
--   rec='stock'      k in {solo_pairs, solo_ips, solo_domains, solo_classified}
--   rec='fresh_pair' k in {fresher_than_free, tied_with_free, stale_vs_free}
--   rec='fresh_day'  k in {solo_new_pair, refresh_of_free_pair, same_day_dup_with_free}
--
-- Validation anchors:
--   solo_pairs(V) == Σ q3b mask rows with bit_V set & other-free bits clear
--                 == q3_usable_uniqueness.netnew_vs_free_pairs (paid vendors)
--   solo_new_pair + refresh_of_free_pair == Σ q3c mask rows bit_V set & other-free clear
--   solo_domains(V) >= q4.sole_domains(V) (solo ⊇ sole, same q4B universe)
--
-- BIG SCAN (svs 30d referenced by pair/triple/domain subtrees + wcv + pc; ~1.5-2h)
-- — background, never preempt.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q8a solo stock" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=2000 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q8a_solo_stock.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q8a_solo_stock.csv
--
-- Parameters (URIS loop): SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30
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

wcv_all AS (
  SELECT DISTINCT domain_name AS dom FROM wcv
),

rows30 AS (
  SELECT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom,
    s.dt,
    (u.dom IS NOT NULL) AS usable
  FROM svs s
  LEFT JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
    AND NET.REG_DOMAIN(s.url) IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
),

pairs AS (
  SELECT ds, ip, dom, LOGICAL_OR(usable) AS usable, MAX(dt) AS dtm
  FROM rows30
  GROUP BY 1, 2, 3
),

pair_m AS (
  SELECT ip, dom, LOGICAL_OR(usable) AS usable,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS pmask,
         ARRAY_AGG(STRUCT(ds, dtm)) AS arr
  FROM pairs
  GROUP BY ip, dom
),

stock_pairs AS (
  SELECT ds, COUNT(*) AS n
  FROM pair_m, UNNEST([23, 24, 25, 26, 28, 30, 33, 36, 39, 40]) AS ds
  WHERE usable
    AND (pmask >> (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) & 1 = 1
    AND (pmask & (CASE WHEN ds = 23 THEN 32 WHEN ds = 30 THEN 1 ELSE 33 END)) = 0
  GROUP BY ds
),

ip_m AS (
  SELECT ip, BIT_OR(pmask) AS im
  FROM pair_m
  WHERE usable
  GROUP BY ip
),

stock_ips AS (
  SELECT ds, COUNT(*) AS n
  FROM ip_m, UNNEST([23, 24, 25, 26, 28, 30, 33, 36, 39, 40]) AS ds
  WHERE (im >> (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                        WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                        WHEN 39 THEN 8 WHEN 40 THEN 9 END)) & 1 = 1
    AND (im & (CASE WHEN ds = 23 THEN 32 WHEN ds = 30 THEN 1 ELSE 33 END)) = 0
  GROUP BY ds
),

fresh_pair AS (
  SELECT v.ds,
         CASE WHEN v.dtm > fo.free_dtm THEN 'fresher_than_free'
              WHEN v.dtm = fo.free_dtm THEN 'tied_with_free'
              ELSE 'stale_vs_free'
         END AS cls,
         COUNT(*) AS n
  FROM pair_m, UNNEST(arr) AS v,
       UNNEST([(SELECT AS STRUCT MAX(o.dtm) AS free_dtm
                FROM UNNEST(arr) o
                WHERE o.ds IN (23, 30) AND o.ds != v.ds)]) AS fo
  WHERE fo.free_dtm IS NOT NULL
  GROUP BY v.ds, cls
),

trip2 AS (
  SELECT ip, dom, dt,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS tmask
  FROM rows30
  WHERE usable
  GROUP BY 1, 2, 3
),

pairfree AS (
  SELECT ip, dom, BIT_OR(tmask) AS pmask_u
  FROM trip2
  GROUP BY 1, 2
),

fresh_day AS (
  SELECT ds,
         CASE WHEN (t.tmask & (CASE WHEN ds = 23 THEN 32 WHEN ds = 30 THEN 1 ELSE 33 END)) != 0
                THEN 'same_day_dup_with_free'
              WHEN (p.pmask_u & (CASE WHEN ds = 23 THEN 32 WHEN ds = 30 THEN 1 ELSE 33 END)) = 0
                THEN 'solo_new_pair'
              ELSE 'refresh_of_free_pair'
         END AS cls,
         COUNT(*) AS n
  FROM trip2 t
  JOIN pairfree p USING (ip, dom),
  UNNEST([23, 24, 25, 26, 28, 30, 33, 36, 39, 40]) AS ds
  WHERE (t.tmask >> (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                             WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                             WHEN 39 THEN 8 WHEN 40 THEN 9 END)) & 1 = 1
  GROUP BY ds, cls
),

dsd AS (
  SELECT DISTINCT CAST(data_source_id AS INT64) AS ds, NET.REG_DOMAIN(url) AS dom
  FROM svs
  WHERE NET.REG_DOMAIN(url) IS NOT NULL
),

dom_m AS (
  SELECT d.dom,
         SUM(1 << (CASE d.ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                             WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                             WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS dmask,
         LOGICAL_OR(w.dom IS NOT NULL) AS classified
  FROM dsd d
  LEFT JOIN wcv_all w ON w.dom = d.dom
  GROUP BY d.dom
),

stock_doms AS (
  SELECT ds, COUNT(*) AS n_dom, COUNTIF(classified) AS n_cls
  FROM dom_m, UNNEST([23, 24, 25, 26, 28, 30, 33, 36, 39, 40]) AS ds
  WHERE (dmask >> (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) & 1 = 1
    AND (dmask & (CASE WHEN ds = 23 THEN 32 WHEN ds = 30 THEN 1 ELSE 33 END)) = 0
  GROUP BY ds
)

SELECT 'stock' AS rec, ds, 'solo_pairs' AS k, CAST(n AS FLOAT64) AS v FROM stock_pairs
UNION ALL
SELECT 'stock', ds, 'solo_ips', CAST(n AS FLOAT64) FROM stock_ips
UNION ALL
SELECT 'stock', ds, 'solo_domains', CAST(n_dom AS FLOAT64) FROM stock_doms
UNION ALL
SELECT 'stock', ds, 'solo_classified', CAST(n_cls AS FLOAT64) FROM stock_doms
UNION ALL
SELECT 'fresh_pair', ds, cls, CAST(n AS FLOAT64) FROM fresh_pair
UNION ALL
SELECT 'fresh_day', ds, cls, CAST(n AS FLOAT64) FROM fresh_day
ORDER BY rec, ds, k;
