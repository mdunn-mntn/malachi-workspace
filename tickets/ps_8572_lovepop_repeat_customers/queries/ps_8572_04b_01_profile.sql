-- PS-8572 Task B: cheap profile of cost_impression_log, advertiser 58797, 2026-06-01..2026-08-04
-- COUNT(*) + APPROX_COUNT_DISTINCT(ip) by campaign_id x DATE
SELECT
  campaign_id,
  DATE(time) AS d,
  COUNT(*) AS imps,
  APPROX_COUNT_DISTINCT(ip) AS approx_ips
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE advertiser_id = 58797
  AND DATE(time) BETWEEN '2026-06-01' AND '2026-08-04'
GROUP BY 1, 2
ORDER BY 2, 1
