-- ============================================================================
-- AUDI-1089 DECK QUERY D2 of 7: won impressions on IPs each source touched
-- FILLS: deck workbook BLOCK 1 cols "Touched won imps (valuation wk)" and
--        "Share of ALL our won imps that landed on this source's IPs"
--        (denominator = the rec='platform' row). NOTE the share is a REACH
--        share, NOT a win rate: bids-won/bids-touched is not computable from
--        this package (bid-request grain lives in bidder_bid_events, 90d TTL,
--        out of scope) — don't retitle the column to anything win-rate-like.
--
-- Claim: "touched bids that won" = WON impressions (cost_impression_log rows) in
-- the valuation week landing on IPs the source delivered at least once in the
-- 37d membership window. Cohorts overlap across sources by construction (an IP
-- many sources delivered counts for each) — the column is NOT additive.
--
-- rec='ds' one row per source; rec='free_union' (ds 99) = IP touched by EITHER
-- free log; rec='platform' = ALL won imps in the week (IPv4), including imps on
-- IPs NO source delivered — the honest % denominator.
--
-- ARCHITECTURE NOTE (cost): the svs 37d membership scan is read EXACTLY ONCE —
-- one LEFT JOIN of the CIL week to the membership mask, one aggregation over a
-- (ds, mask) spec array; the platform row is the spec entry with a NULL mask
-- (matches every IP, member or not). CTE re-references would re-scan the
-- external table (house-measured cost trap) — do not restructure into UNION ALL
-- branches.
--
-- Grain: RAW 37d svs membership (IPv4 both sides, NO usable-domain gate — the
-- serving-cohort convention shared with the workbook's q6/q8b/q15).
--
-- Expected reconciliation vs the measured outputs/run_2026_07_10: platform week
-- 398,301,655; augmentor touched 390,253,648 (98.0%); free-union 395,021,931
-- (99.2%); Cybba 205,469,975 (51.6%).
--
-- BIG SCAN (svs 37d ip pass + CIL week; ~30-45min) — background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; GCS read on mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' deck_d2_touched_won_bids.sql)" \
--     > deck_d2_touched_won_bids.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 37; VALUE week 2026-07-02..08
-- Spec masks below = 1 << bit for the house bit order (ds 23,24,25,26,28,30,33,
-- 36,39,40 = bits 0..9); ds 99 = free mask 33; the NULL entry = platform total.
-- ============================================================================

WITH mem AS (
  SELECT ip,
         SUM(1 << (CASE ds WHEN 23 THEN 0 WHEN 24 THEN 1 WHEN 25 THEN 2 WHEN 26 THEN 3
                           WHEN 28 THEN 4 WHEN 30 THEN 5 WHEN 33 THEN 6 WHEN 36 THEN 7
                           WHEN 39 THEN 8 WHEN 40 THEN 9 END)) AS m
  FROM (SELECT DISTINCT CAST(data_source_id AS INT64) AS ds, ip
        FROM svs
        WHERE ip IS NOT NULL AND ip NOT LIKE '%:%')
  GROUP BY ip
),

ipimps AS (
  SELECT ip, COUNT(*) AS imps
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

labeled AS (
  -- LEFT JOIN keeps served IPs no source delivered (mask 0) so the NULL-mask
  -- spec row totals the whole platform
  SELECT i.imps, IFNULL(m.m, 0) AS m
  FROM ipimps i
  LEFT JOIN mem m USING (ip)
),

agg AS (
  SELECT s.ds,
         SUM(IF(s.mask IS NULL OR (l.m & s.mask) != 0, l.imps, 0)) AS imps_touched
  FROM labeled l
  CROSS JOIN UNNEST([STRUCT(23 AS ds, 1 AS mask), (24, 2), (25, 4), (26, 8), (28, 16),
                     (30, 32), (33, 64), (36, 128), (39, 256), (40, 512), (99, 33),
                     (CAST(NULL AS INT64), CAST(NULL AS INT64))]) AS s
  GROUP BY s.ds
)

SELECT
  CASE WHEN ds IS NULL THEN 'platform'
       WHEN ds = 99 THEN 'free_union'
       ELSE 'ds' END AS rec,
  ds,
  imps_touched,
  ROUND(100 * imps_touched
        / MAX(IF(ds IS NULL, imps_touched, NULL)) OVER (), 2) AS pct_of_platform_won_imps
FROM agg
ORDER BY rec, ds;
