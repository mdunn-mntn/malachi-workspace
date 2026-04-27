-- TI-837 Phase 2 cohort selection — Stage A.3
-- Treatment-side delivery per advertiser (window 2026-04-20 → 2026-04-26 UTC)
-- ----------------------------------------------------------------
-- Source: silver.logdata.cost_impression_log
-- One row per advertiser with served IP count + impression count.
-- We use this as the "active in window" filter and as a proxy for the
-- biddable_rate signal (by hash-symmetry, biddable_holdouts/holdouts ≈
-- served_treatment/targeted, modulo win rate).
-- ----------------------------------------------------------------

SELECT
  CAST(advertiser_id AS INT64) AS advertiser_id,
  COUNT(DISTINCT ip)            AS served_distinct_ips,
  COUNT(*)                      AS impressions
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
  AND DATE(time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
  AND ip IS NOT NULL AND ip != '0.0.0.0'
GROUP BY advertiser_id
HAVING served_distinct_ips >= 100
ORDER BY served_distinct_ips DESC
