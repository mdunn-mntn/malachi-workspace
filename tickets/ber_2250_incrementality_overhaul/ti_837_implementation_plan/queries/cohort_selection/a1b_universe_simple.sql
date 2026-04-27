-- TI-837 Phase 2 cohort selection — Stage A.1b (simpler)
-- Per-advertiser universe size + MAX-tier composition only (drop tier-diversity)
-- ----------------------------------------------------------------
-- Window: 2026-04-20 → 2026-04-26 UTC.
--
-- Drops the per-IP tier-diversity flags (which forced a per-IP grouping
-- with massive shuffle). Just computes:
--   - distinct_ips per advertiser
--   - per-tier IP counts under MAX(score) construction
--
-- The tier-diversity score (frac_multi_tier) is computed in a SEPARATE
-- query (A.1c) restricted to top-K eligible advertisers (cheaper join).
-- ----------------------------------------------------------------

WITH
ip_max AS (
  SELECT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip,
    MAX(CAST(household_score AS INT64)) AS max_score
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE year  = '2026'
    AND month = '04'
    AND day IN ('20','21','22','23','24','25','26')
    AND ip IS NOT NULL AND ip != '0.0.0.0'
  GROUP BY advertiser_id, ip
)

SELECT
  advertiser_id,
  COUNT(*)                                                            AS distinct_ips,
  COUNTIF(max_score = 10000)                                          AS max_tier_high,
  COUNTIF(max_score BETWEEN 7000 AND 9999)                            AS max_tier_peak,
  COUNTIF(max_score BETWEEN 3333 AND 6999)                            AS max_tier_mid,
  COUNTIF(max_score < 3333)                                           AS max_tier_max_reach
FROM ip_max
GROUP BY advertiser_id
HAVING distinct_ips >= 100
ORDER BY distinct_ips DESC
