-- TI-884: Step 1 — top-50 advertisers by April 2026 spend.
-- Purpose: produce the candidate list for per-advertiser MDE analysis.
-- Source: cost_impression_log (sum_by_campaign_by_day stale through 2026-04-14).
-- Window: April 1-30, 2026 (full month).
-- Filters: exclude AID 90 (PSA — intentionally serves to holdouts).
-- Spend definition: media_spend + data_spend + platform_spend (advertiser-facing total).
-- Output: ~50 rows. Saved to outputs/ti_884_top50_spend_ranking.json.
-- Cost estimate (dry run): ~300 GB scanned.

SELECT
  advertiser_id,
  SUM(COALESCE(media_spend, 0) + COALESCE(data_spend, 0) + COALESCE(platform_spend, 0)) AS total_spend,
  SUM(COALESCE(media_cost, 0)) AS media_cost_only,
  COUNT(*) AS impressions,
  APPROX_COUNT_DISTINCT(ip) AS approx_distinct_ips
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE DATE(time) BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
  AND advertiser_id IS NOT NULL
  AND advertiser_id != 90
GROUP BY advertiser_id
ORDER BY total_spend DESC
LIMIT 50
