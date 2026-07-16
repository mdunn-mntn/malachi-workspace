-- ============================================================================
-- AUDI-1089 DECK QUERY D6 of 7: free-vs-vendor-only IP coverage by score tier —
-- only IPs that actually got bid on (delivery-reality lens)
-- FILLS: deck sheet BLOCK 6 (rows 46-53) "Of IPs things that actually got bid
--        on": B "free-covered IPs", C "vendor-only IPs", D "% free covered".
--        tier='1_all_ips' row = the "Free Logs ONLY" line; tier rows = HI10000
--        .. Unscored.
--
-- Claim: same split as D5 (free-covered vs vendor-only member IPs) restricted to
-- IPs that received at least one WON impression in the valuation week (INNER JOIN
-- to cost_impression_log — the package's serving canon; true bid-request grain
-- lives in bidder_bid_events, 90d TTL, outside this package). This is the
-- delivery-reality lens: of the households we actually spent money on, how many
-- would the free logs still know?
--
-- Read D5's POPULATION NOTE. The pair of blocks intentionally shows both answers:
-- audience-count coverage (D5) can drop sharply while served-IP coverage (D6)
-- stays near-total, because delivery concentrates on the overlap-heavy core.
--
-- Tier here never means "not served" (every IP in this population was served):
-- unscored = per-IP MAX(household_score) <= 0 (RT rows carry -1, unscored 0).
-- Same tier boundaries as D5; labels prefixed for sheet row order.
--
-- Grain: RAW 37d svs membership x CIL valuation week, IPv4 both sides.
--
-- Expected reconciliation: free_covered IPs on the 1_all_ips row = 27,413,105
-- (q15 union touched served IPs, exact). NOTE the denominator basis:
-- pct_free_covered = free_covered / (free_covered + vendor_only) over MEMBER-
-- and-served IPs — expect ~98.x%. The deck's 97.8% uses q7d's 28,031,422
-- platform served IPs instead, which includes served IPs NO source delivered
-- (invisible to this query's INNER JOIN); both are correct on their own basis.
-- HI free share >= 99.9%.
--
-- BIG SCAN (svs 37d ip pass + CIL week, single pass — the all-IPs row comes from
-- a GROUPING SETS superaggregate, NOT a second read; ~30-45min) — background.
-- D5 and D6 could share one combined scan; kept separate so each sheet block has
-- exactly one supporting query.
--
-- Run (from this folder — queries and outputs paths are relative to it):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=20 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' deck_d6_tier_free_coverage_bid_ips.sql)" \
--     > ../../outputs/run_<YYYY_MM_DD>/deck_d6_tier_free_coverage_bid_ips.csv
--
-- Parameters: SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 37; VALUE week 2026-07-02..08
-- ============================================================================

WITH mem AS (
  SELECT ip,
         LOGICAL_OR(ds IN (23, 30)) AS has_free,
         LOGICAL_OR(ds NOT IN (23, 30)) AS has_paid
  FROM (SELECT DISTINCT CAST(data_source_id AS INT64) AS ds, ip
        FROM svs
        WHERE ip IS NOT NULL AND ip NOT LIKE '%:%')
  GROUP BY ip
),

ip_score AS (
  SELECT ip, MAX(household_score) AS hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

lab AS (
  -- INNER JOIN = only member IPs that actually received won impressions
  SELECT
    m.has_free,
    m.has_paid,
    CASE
      WHEN s.hs = 10000 THEN '2_hi_10000'
      WHEN s.hs = 8000 THEN '3_pp_8000'
      WHEN s.hs BETWEEN 6666 AND 9999 THEN '4_high_graduated'
      WHEN s.hs BETWEEN 3333 AND 6665 THEN '5_mid'
      WHEN s.hs BETWEEN 1 AND 3332 THEN '6_max_reach'
      ELSE '7_unscored'  -- served but score <= 0 (RT rows = -1, unscored = 0)
    END AS tier
  FROM mem m
  JOIN ip_score s USING (ip)
)

-- one aggregation: the () superaggregate is the all-IPs row. The output alias
-- must NOT be named `tier` — it would shadow the column in GROUP BY and the
-- superaggregate row would print empty instead of '1_all_ips'.
SELECT COALESCE(tier, '1_all_ips') AS tier_row,
       COUNTIF(has_free) AS free_covered_ips,
       COUNTIF(has_paid AND NOT has_free) AS vendor_only_ips,
       ROUND(100 * COUNTIF(has_free) / COUNT(*), 2) AS pct_free_covered
FROM lab
GROUP BY GROUPING SETS ((tier), ())
ORDER BY tier_row;
