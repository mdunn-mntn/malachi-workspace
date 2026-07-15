-- ============================================================================
-- DDP quality-score runbook, STEP 15b: free_logs COMBINED pseudo-vendor — stock,
-- uniqueness and freshness of the guid+augmentor UNION measured vs the PAID roster
-- Claim: the workbook's combined free_logs column (ds 99) compares the union of the
-- two internal logs against ALL PAID VENDORS (never against its own halves). Union
-- unique counts are not derivable from per-source scans; this measures them, plus
-- the union's sole stock (nothing paid has it) and freshness vs the paid roster.
-- Mirror of q8a with roles reversed: there "vendor vs free logs"; here "free union
-- vs paid roster" (other-mask = the 8 paid bits instead of the free bits).
--
-- Grains (mirror the rows they fill):
--   reach : RAW 30d union uniques (q2 grain, IPv4) — unique IPs / domains / pairs
--   stock : usable 30d (q3 grain) — usable IPs, sole IPs/pairs (no paid vendor)
--   doms  : all parsed domains (q4B grain) — sole domains + sole classified (in wcv)
--   fresh_pair : RAW pairs co-held with ANY paid vendor — union MAX(dt) vs paid
--                MAX(dt): fresher_than_paid / tied_with_paid / stale_vs_paid
--   fresh_day  : usable visit-day triples of the union — sole_new_pair /
--                refresh_of_paid_pair / same_day_dup_with_paid
--
-- Output: ONE CSV (rec, k1, v):
--   rec='reach' k1 in {ips_30d, domains_30d, ip_domain_pairs_30d}
--   rec='stock' k1 in {usable_ips, sole_ips, usable_pairs, sole_pairs}
--   rec='doms'  k1 in {total_domains, sole_domains, sole_classified}
--   rec='fresh_pair' k1 in {fresher_than_paid, tied_with_paid, stale_vs_paid}
--   rec='fresh_day'  k1 in {sole_new_pair, refresh_of_paid_pair, same_day_dup_with_paid}
--
-- Validation anchors: stock usable_pairs == Sigma q3b masks with free bits (exact,
-- fill_template asserts); stock sole_pairs == Sigma masks free-and-no-paid; reach
-- bounds: max(q2[23],q2[30]) <= union <= q2[23]+q2[30].
--
-- BIG SCAN (svs 30d, multiple subtrees + wcv + pc; ~1.5-2h) — background, never preempt.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q15b free union stock" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q15b_free_union_stock.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q15b_free_union_stock.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30
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
  -- one deduped relation; is_free groups the union, paid bits kept for the "other" side
  SELECT
    (CAST(s.data_source_id AS INT64) IN (23, 30)) AS is_free,
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

reach AS (
  SELECT COUNT(DISTINCT ip) AS ips_30d,
         COUNT(DISTINCT dom) AS domains_30d,
         COUNT(DISTINCT CONCAT(ip, '|', dom)) AS pairs_30d
  FROM rows30
  WHERE is_free
),

pair_g AS (
  SELECT ip, dom, LOGICAL_OR(usable) AS usable,
         LOGICAL_OR(is_free) AS has_free,
         LOGICAL_OR(NOT is_free) AS has_paid,
         MAX(IF(is_free, dt, NULL)) AS free_dtm,
         MAX(IF(NOT is_free, dt, NULL)) AS paid_dtm
  FROM rows30
  GROUP BY ip, dom
),

pairstats AS (
  -- pair-grain scalars in ONE pass (branches re-read svs): stock + freshness-vs-paid
  SELECT COUNTIF(usable AND has_free) AS usable_pairs,
         COUNTIF(usable AND has_free AND NOT has_paid) AS sole_pairs,
         COUNTIF(has_free AND has_paid AND free_dtm > paid_dtm) AS fresher,
         COUNTIF(has_free AND has_paid AND free_dtm = paid_dtm) AS tied,
         COUNTIF(has_free AND has_paid AND free_dtm < paid_dtm) AS stale
  FROM pair_g
),

ip_g AS (
  SELECT ip, LOGICAL_OR(has_free) AS f, LOGICAL_OR(has_paid) AS p
  FROM pair_g
  WHERE usable
  GROUP BY ip
),

stock_ip AS (
  SELECT COUNTIF(f) AS usable_ips, COUNTIF(f AND NOT p) AS sole_ips
  FROM ip_g
),

dom_g AS (
  SELECT g.dom, LOGICAL_OR(g.has_free) AS f, LOGICAL_OR(g.has_paid) AS p,
         LOGICAL_OR(w.dom IS NOT NULL) AS classified
  FROM pair_g g
  LEFT JOIN wcv_all w ON w.dom = g.dom
  GROUP BY g.dom
),

doms AS (
  SELECT COUNTIF(f) AS total_domains,
         COUNTIF(f AND NOT p) AS sole_domains,
         COUNTIF(f AND NOT p AND classified) AS sole_classified
  FROM dom_g
),

trip_g AS (
  SELECT ip, dom, dt,
         LOGICAL_OR(is_free) AS has_free,
         LOGICAL_OR(NOT is_free) AS has_paid
  FROM rows30
  WHERE usable
  GROUP BY 1, 2, 3
),

pair_paid AS (
  SELECT ip, dom, LOGICAL_OR(has_paid) AS pair_has_paid
  FROM trip_g
  GROUP BY 1, 2
),

fresh_day AS (
  SELECT COUNTIF(NOT t.has_paid AND NOT p.pair_has_paid) AS sole_new_pair,
         COUNTIF(NOT t.has_paid AND p.pair_has_paid) AS refresh_of_paid_pair,
         COUNTIF(t.has_paid) AS same_day_dup_with_paid
  FROM trip_g t
  JOIN pair_paid p USING (ip, dom)
  WHERE t.has_free
)

SELECT 'reach' AS rec, kv.k AS k1, kv.v
FROM reach, UNNEST([
  STRUCT('ips_30d' AS k, CAST(ips_30d AS FLOAT64) AS v),
  STRUCT('domains_30d', CAST(domains_30d AS FLOAT64)),
  STRUCT('ip_domain_pairs_30d', CAST(pairs_30d AS FLOAT64))
]) AS kv

UNION ALL

SELECT 'stock', kv.k, kv.v
FROM pairstats, UNNEST([
  STRUCT('usable_pairs' AS k, CAST(usable_pairs AS FLOAT64) AS v),
  STRUCT('sole_pairs', CAST(sole_pairs AS FLOAT64))
]) AS kv

UNION ALL

SELECT 'fresh_pair', kv.k, kv.v
FROM pairstats, UNNEST([
  STRUCT('fresher_than_paid' AS k, CAST(fresher AS FLOAT64) AS v),
  STRUCT('tied_with_paid', CAST(tied AS FLOAT64)),
  STRUCT('stale_vs_paid', CAST(stale AS FLOAT64))
]) AS kv

UNION ALL

SELECT 'stock', kv.k, kv.v
FROM stock_ip, UNNEST([
  STRUCT('usable_ips' AS k, CAST(usable_ips AS FLOAT64) AS v),
  STRUCT('sole_ips', CAST(sole_ips AS FLOAT64))
]) AS kv

UNION ALL

SELECT 'doms', kv.k, kv.v
FROM doms, UNNEST([
  STRUCT('total_domains' AS k, CAST(total_domains AS FLOAT64) AS v),
  STRUCT('sole_domains', CAST(sole_domains AS FLOAT64)),
  STRUCT('sole_classified', CAST(sole_classified AS FLOAT64))
]) AS kv

UNION ALL

SELECT 'fresh_day', kv.k, kv.v
FROM fresh_day, UNNEST([
  STRUCT('sole_new_pair' AS k, CAST(sole_new_pair AS FLOAT64) AS v),
  STRUCT('refresh_of_paid_pair', CAST(refresh_of_paid_pair AS FLOAT64)),
  STRUCT('same_day_dup_with_paid', CAST(same_day_dup_with_paid AS FLOAT64))
]) AS kv

ORDER BY rec, k1;
