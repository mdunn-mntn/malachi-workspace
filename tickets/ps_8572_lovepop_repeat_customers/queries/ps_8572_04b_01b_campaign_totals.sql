-- PS-8572 Task B: campaign totals + objective/cg via LEFT JOIN campaigns dim; campaign_id=-3 shows NULL dim
SELECT
  i.campaign_id,
  c.objective_id,
  c.campaign_group_id,
  CASE i.campaign_id
    WHEN 614193 THEN 'S1' WHEN 614191 THEN 'S2' WHEN 614192 THEN 'S3'
    WHEN 637329 THEN 'RT' WHEN 637330 THEN 'RT' WHEN 637331 THEN 'RT' WHEN 637332 THEN 'RT'
    ELSE 'other' END AS stage,
  COUNT(*) AS imps,
  APPROX_COUNT_DISTINCT(i.ip) AS approx_ips,
  MIN(DATE(i.time)) AS first_day,
  MAX(DATE(i.time)) AS last_day
FROM `dw-main-silver.logdata.cost_impression_log` i
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON i.campaign_id = c.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
WHERE i.advertiser_id = 58797
  AND DATE(i.time) BETWEEN '2026-06-01' AND '2026-08-04'
GROUP BY 1, 2, 3, 4
ORDER BY imps DESC
