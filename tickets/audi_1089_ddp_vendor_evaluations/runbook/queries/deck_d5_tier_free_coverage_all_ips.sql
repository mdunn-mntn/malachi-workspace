-- ============================================================================
-- AUDI-1089 DECK QUERY D5 of 7: free-vs-vendor-only IP coverage by score tier —
-- ALL member IPs (audience-size lens)
-- FILLS: deck workbook table "Of ALL member IPs, served or not (audience-size
--        coverage)" (block 5): "free-covered IPs", "vendor-only IPs", "% free
--        covered". The tier_row='1_all_ips' row fills the "Free Logs ONLY (all
--        IPs)" line (= the whole-population split under free-logs-only); the
--        tier rows fill HI10000 .. Unscored.
--        LANDED-CSV NOTE: outputs/run_2026_07_10's CSV predates the alias fix
--        below — its column is named `tier` and the all-IPs row's label is an
--        EMPTY string. A re-run emits tier_row='1_all_ips'. Counts identical.
--
-- Claim: every IP any source delivered in the 37d window is either free-covered
-- (guid_log or augmentor delivered it — kept under free-logs-only) or vendor-only
-- (only paid vendors delivered it — LOST under free-logs-only). Splitting that by
-- the IP's score tier answers "whose audience shrinks if we drop all vendors".
--
-- POPULATION NOTE (the trap this block exists to expose): this is the AUDIENCE-
-- SIZE lens — ALL member IPs, served or not. Block 6 (D6) is the same split
-- restricted to IPs that actually received won impressions; the two answer
-- different questions and their percentages differ legitimately (an audience can
-- shrink 30% while the IPs actually served stay >99% covered).
--
-- Tier = per-IP MAX(household_score) over the CIL valuation week:
--   hi_10000 (= 10000) | pp_8000 (= 8000) | high_graduated (6666-9999 excl 8000)
--   | mid (3333-6665) | max_reach (1-3332) | unscored (score <= 0 — includes
--   RT rows at -1 — OR the IP was never served that week, which for THIS all-IPs
--   lens means "no score exists"). Tier labels are prefixed 1_..7_ so the CSV
--   sorts in sheet row order.
--
-- Grain: RAW 37d svs membership, IPv4 (serving-cohort convention; no usable gate).
--
-- Expected reconciliation: free-covered HI share >= 99.9% (workbook q3d: free-only
-- keeps 99.94% of HI, 99.25% of PP); the all_ips free share lands near the IP-
-- grain free coverage (~2/3), far below the served-IP share in D6 — that gap is
-- the audience-size-vs-delivery-reality story.
--
-- BIG SCAN (svs 37d ip pass + CIL week, single pass — the all-IPs row comes from
-- a GROUPING SETS superaggregate, NOT a second read; CTE re-references would
-- re-scan the external table; ~30-45min) — background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(37)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=20 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' deck_d5_tier_free_coverage_all_ips.sql)" \
--     > deck_d5_tier_free_coverage_all_ips.csv
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
  SELECT
    m.has_free,
    m.has_paid,
    CASE
      WHEN s.hs = 10000 THEN '2_hi_10000'
      WHEN s.hs = 8000 THEN '3_pp_8000'
      WHEN s.hs BETWEEN 6666 AND 9999 THEN '4_high_graduated'
      WHEN s.hs BETWEEN 3333 AND 6665 THEN '5_mid'
      WHEN s.hs BETWEEN 1 AND 3332 THEN '6_max_reach'
      ELSE '7_unscored'  -- score <= 0 (RT = -1) or never served in the week
    END AS tier
  FROM mem m
  LEFT JOIN ip_score s USING (ip)
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
