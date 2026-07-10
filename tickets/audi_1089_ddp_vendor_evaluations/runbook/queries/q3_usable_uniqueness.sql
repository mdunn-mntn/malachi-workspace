-- ============================================================================
-- DDP quality-score runbook, STEP 3: uniqueness + freshness + density of the USABLE pool
-- Claim: restricted to usable signal (DS13-classifiable OR DS19-categorizable domains),
-- how much of each source's contribution is truly its own (sole), how fresh is the
-- shared part (freshest / tied / stale), and how deep is its per-IP coverage
-- (pairs per IP -- a source with fewer IPs but more domains per IP tells us more per household).
--
-- Usable-domain set (OR-semantics, mirrors q2c):
--   wcv domains (minus the DS13 blocklist yahoo/aol/easybrain)
--   UNION reg-domains of product_categorization composite keys with dsc >= 900000
--   (pc side has no blocklist -- yahoo etc. usable via DS19, matching billing reality).
--   Domain-grain approximation of row-grain usable: a domain with any categorized URL
--   marks all its pairs usable.
--
-- Pair recency classes per (ip, domain) per source over the window (vs ALL other sources,
-- internal 23/30 included -- they compete for first-reporter credit):
--   sole     = no other source has the pair
--   freshest = shared, this source's MAX(dt) strictly newest
--   tied     = shared, at the pair max dt together with others
--   stale    = another source has a strictly fresher report
--   netnew_vs_free = pair absent from BOTH internal free logs (23 guid_log, 30 augmentor)
-- IP soleness: IP seen by no other source (among usable pairs).
--
-- THE BIG SCAN (~8.5 TB over 30 days) -- launch in background, never preempt (15 min - 2.6 h).
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q3 usable uniqueness" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q3_usable_uniqueness.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q3_usable_uniqueness.csv
--
-- Parameters (in the URIS loop above, not in the SQL):
--   SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30
-- Validation anchor: raw-pair run (Jul 9) had DS25 ~69-70% sole; usable-restricted will differ
--   but should stay the roster's high-sole outlier.
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
    MAX(s.dt) AS max_dt
  FROM svs s
  JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
  GROUP BY 1, 2, 3
),

pairx AS (
  SELECT
    ds, ip, dom, max_dt,
    COUNT(*) OVER w AS n_ds,
    MAX(max_dt) OVER w AS pair_max,
    MAX(IF(ds IN (23, 30), 1, 0)) OVER w AS has_free
  FROM pairs
  WINDOW w AS (PARTITION BY ip, dom)
),

pairc AS (
  SELECT
    *,
    SUM(IF(max_dt = pair_max, 1, 0)) OVER (PARTITION BY ip, dom) AS n_at_max
  FROM pairx
),

ip_stats AS (
  SELECT ip, COUNT(DISTINCT ds) AS ip_nds
  FROM pairs
  GROUP BY ip
)

SELECT
  p.ds,
  APPROX_COUNT_DISTINCT(p.ip) AS usable_ips,
  APPROX_COUNT_DISTINCT(IF(i.ip_nds = 1, p.ip, NULL)) AS sole_ips,
  COUNT(*) AS usable_pairs,
  ROUND(COUNT(*) / APPROX_COUNT_DISTINCT(p.ip), 2) AS pairs_per_ip,
  COUNTIF(p.n_ds = 1) AS sole_pairs,
  COUNTIF(p.n_ds > 1 AND p.max_dt = p.pair_max AND p.n_at_max = 1) AS freshest_pairs,
  COUNTIF(p.n_ds > 1 AND p.max_dt = p.pair_max AND p.n_at_max >= 2) AS tied_pairs,
  COUNTIF(p.max_dt < p.pair_max) AS stale_pairs,
  COUNTIF(p.has_free = 0) AS netnew_vs_free_pairs
FROM pairc p
JOIN ip_stats i ON p.ip = i.ip
GROUP BY p.ds
ORDER BY sole_pairs DESC;
