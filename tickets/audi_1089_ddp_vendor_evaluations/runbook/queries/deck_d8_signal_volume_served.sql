-- ============================================================================
-- AUDI-1089 DECK QUERY D8 of 8: SIGNAL VOLUME on IPs that actually got bid on
-- FILLS: deck workbook table "SIGNAL VOLUME on SERVED IPs only" — triple-grain
--        (ip x domain x date) free coverage restricted to IPs that received won
--        impressions in the valuation week, per tier: HI / PP / other served
--        tiers / not served; the ALL-SERVED row = the three served tiers summed
--        (sheet arithmetic).
--
-- Claim: the all-triples SIGNAL VOLUME table (from deck_d4) mixes two
-- populations in its "other" bucket — triples on served-but-lower-tier IPs and
-- triples on IPs never served that week. This query splits them: of the signal
-- volume sitting on IPs we ACTUALLY spent money on, how much do the free logs
-- hold? (HI/PP rows are definitionally served — a scored IP was served — so
-- they should reproduce deck_d4's hi/pp triple splits within snapshot drift.)
--
-- Tier = per-IP MAX(household_score) over the CIL valuation week: 2_hi_10000
-- (= 10000) / 3_pp_8000 (= 8000) / 4_served_other_tiers (served, any other
-- score incl. unscored 0 and RT -1) / 5_not_served_this_week (no CIL row).
--
-- Grain: usable triples, 30d (verbatim deck_d1/d4 trip machinery); free-covered
-- = a free-log bit set on the triple's holder mask (mask & 33), vendor-only =
-- paid bits only. IPv4 both sides.
--
-- Expected reconciliation: total across the 4 rows == deck_d4's today
-- trips_kept 13,286,674,041 (within live wcv/pc snapshot drift); 2_hi_10000 row
-- ~= d4 hi (2,831,854,122 total / 65.74% free-covered); 3_pp_8000 ~= d4 pp
-- (70,685,402 / 59.21%).
--
-- ARCHITECTURE NOTE (cost): the external-reading chain trips -> trip_g -> final
-- references each CTE exactly once (CTE re-references re-read temp external
-- tables — house-measured trap); CIL is a single native pass.
--
-- BIG SCAN (svs 30d + wcv + pc single pass + CIL week; ~1-1.5h) — dry-run,
-- background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=10 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' deck_d8_signal_volume_served.sql)" \
--     > deck_d8_signal_volume_served.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30; VALUE week 2026-07-02..08
-- Bit order: ds 23,24,25,26,28,30,33,36,39,40 = bits 0..9; free mask = 33.
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
  SELECT ip, dom, dt,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS tmask
  FROM trips
  GROUP BY ip, dom, dt
),

ip_tier AS (
  SELECT ip,
         CASE MAX(household_score)
           WHEN 10000 THEN '2_hi_10000'
           WHEN 8000 THEN '3_pp_8000'
           ELSE '4_served_other_tiers'
         END AS tier
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
)

SELECT
  COALESCE(t.tier, '5_not_served_this_week') AS tier_row,
  COUNT(*) AS triples_total,
  COUNTIF((g.tmask & 33) != 0) AS triples_free_covered,
  COUNTIF((g.tmask & 33) = 0) AS triples_vendor_only,
  ROUND(100 * COUNTIF((g.tmask & 33) != 0) / COUNT(*), 2) AS pct_free_covered
FROM trip_g g
LEFT JOIN ip_tier t USING (ip)
GROUP BY tier_row
ORDER BY tier_row;
