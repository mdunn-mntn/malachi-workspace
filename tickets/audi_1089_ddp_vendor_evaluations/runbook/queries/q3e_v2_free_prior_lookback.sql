-- ============================================================================
-- DDP runbook STEP 3e-v2: FAIR free-log preemption with a FULL 30-day lookback (no edge effect).
--
-- Supersedes q3e (which measured free coverage only WITHIN the 30d window, so bids early in
-- the window lost most of their 30d lookback off the edge -> free coverage undercounted).
-- Fix: scan 37 days (2026-05-26..07-01) but MEASURE vendor triples only in the last 7 days
-- (2026-06-25..07-01). Every measured triple's full [D-30, D-1] targeting window is then
-- entirely inside the scanned range -> zero edge effect. ALL IPs (no sampling -> small vendors
-- exact). 7-day measurement is a time-window sample of the SHARE (stable day-to-day).
--
-- WHY (user challenge 2026-07-20): augmentor_log (DS30) is the SSP bid stream, so it necessarily
-- logs an IP on the DAY it is bid on -> "free co-held this (ip,domain,DATE)" (q3c same-day, the
-- basis of the $273.7K) is circular for augmentor. The fair test is: did a free log (23 guid,
-- 30 augmentor) have the (ip,domain) on a PRIOR day within the 30-day targeting window? And
-- recency: if the vendor is the FRESHEST source it still adds value even when free had an older copy.
--
-- Per vendor triple (ip, dom, D) in the measurement window, using the pair's full free date set:
--   free_sameday          = a free log has (ip,dom,D)                 [reproduces q3c same-day; sanity check ~52.5% for DS28]
--   free_prior30          = a free log has (ip,dom) in [D-30, D-1]    [FAIR: household targetable from prior free data]
--     · free_prior_dominant       = free ALSO has a date >= D (free is as-fresh-or-fresher)  -> fully preemptable
--     · free_prior_vendor_fresher = free's latest date < D (vendor is now the freshest)       -> recency value to vendor
--   free_noprior          = free has the pair but NOT in [D-30,D-1] (only same-day/later/older) -> not prior-covered
--   no_free               = no free log delivered the pair in the 37d scan -> vendor-unique
--
-- FAIR preemptable share = free_prior_dominant / triples (free had it in-window AND is still as fresh:
-- the vendor adds neither coverage nor recency). free_prior_vendor_fresher is the judgment slice.
--
-- GRAIN CAVEAT (conservative; user note 2026-07-20): this matches on the exact REG_DOMAIN, but TARGETING
-- keys off the CATEGORY the domain falls into (DS13 vertical via wcv / DS19 keyword via pc) — i.e. "did
-- this IP have a prior visit in this vertical/keyword". A free log seeing a DIFFERENT same-category domain
-- prior already makes the IP targetable, but the (ip,domain,date) grain does NOT count it -> this UNDER-counts
-- free coverage. So free_prior_dominant here is a FLOOR; a category-grain variant (q3f: replace `dom` with
-- the vertical/keyword-category the domain maps to, per consumer) would recover MORE. Domain grain kept here
-- because the meter credits per (ip,url,date); the category grain is the targeting-truthful upper view.
--
-- Grain (ip, REG_DOMAIN(url), dt); usable (wcv OR pc); IPv4. Scan 37d, measure 7d. BIG (~25-35m).
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,5,26); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "q3e-v2 fair prior-day free coverage (full lookback)" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=200 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q3e_v2_free_prior_lookback.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/q3e_v2_free_prior_lookback.csv
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
  SELECT DISTINCT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom,
    SAFE_CAST(s.dt AS DATE) AS dd
  FROM svs s
  JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
),

free_pair AS (
  SELECT ip, dom, ARRAY_AGG(DISTINCT dd) AS fdates, MAX(dd) AS free_last
  FROM trips
  WHERE ds IN (23, 30)
  GROUP BY ip, dom
),

vt AS (
  SELECT ds, ip, dom, dd
  FROM trips
  WHERE ds NOT IN (23, 30)
    AND dd >= DATE '2026-06-25'          -- measurement window: last 7 days (full 30d lookback inside scan)
),

cls AS (
  SELECT
    v.ds,
    (fp.ip IS NOT NULL)                                                       AS free_ever,
    (fp.fdates IS NOT NULL AND v.dd IN UNNEST(fp.fdates))                     AS free_sameday,
    (fp.fdates IS NOT NULL AND EXISTS(
        SELECT 1 FROM UNNEST(fp.fdates) f
        WHERE f >= DATE_SUB(v.dd, INTERVAL 30 DAY) AND f < v.dd))             AS free_prior30,
    (fp.free_last IS NOT NULL AND fp.free_last >= v.dd)                       AS free_asfresh
  FROM vt AS v
  LEFT JOIN free_pair fp USING (ip, dom)
)

SELECT
  ds,
  COUNT(*)                                                    AS triples,
  COUNTIF(free_sameday)                                       AS free_sameday,
  COUNTIF(free_prior30)                                       AS free_prior30,
  COUNTIF(free_prior30 AND free_asfresh)                      AS free_prior_dominant,
  COUNTIF(free_prior30 AND NOT free_asfresh)                  AS free_prior_vendor_fresher,
  COUNTIF(NOT free_prior30 AND free_ever)                     AS free_noprior,
  COUNTIF(NOT free_ever)                                      AS no_free,
  ROUND(COUNTIF(free_sameday) / COUNT(*) * 100, 1)            AS pct_sameday_old,
  ROUND(COUNTIF(free_prior30) / COUNT(*) * 100, 1)            AS pct_prior30_fair,
  ROUND(COUNTIF(free_prior30 AND free_asfresh) / COUNT(*) * 100, 1) AS pct_prior_dominant
FROM cls
GROUP BY ds
ORDER BY ds;
