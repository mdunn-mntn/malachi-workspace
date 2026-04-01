-- TI-797: Beta pre/post performance comparison (BQ version)
-- Equivalent to Alex's Greenplum query on sum_by_campaign_group_by_day
-- Uses sum_by_campaign_by_day summed to CG level
-- Visit definition: clicks + views + competing_views (Alex's definition)

WITH cg_campaigns AS (
  SELECT c.campaign_id, c.campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.campaign_group_id IN (107024, 104020)  -- West Bend, Samy's Camera
    AND c.deleted = FALSE
)
SELECT
  CASE cc.campaign_group_id
    WHEN 107024 THEN 'West Bend Insurance'
    ELSE 'Samys Camera'
  END AS advertiser_name,
  cc.campaign_group_id,
  CASE
    WHEN cc.campaign_group_id = 107024 AND d.day < DATE('2026-02-27') THEN 'pre_buk'
    WHEN cc.campaign_group_id = 104020 AND d.day < DATE('2026-03-04') THEN 'pre_buk'
    ELSE 'post_buk'
  END AS period,
  MIN(d.day) AS period_start,
  MAX(d.day) AS period_end,
  COUNT(DISTINCT d.day) AS days,
  SUM(d.impressions) AS impressions,
  SUM(d.clicks + d.views + COALESCE(d.competing_views, 0)) AS visits,
  SAFE_DIVIDE(
    SUM(d.clicks + d.views + COALESCE(d.competing_views, 0)),
    SUM(d.impressions)
  ) AS ivr,
  SUM(d.media_spend + d.data_spend + d.platform_spend) / 1e9 AS spend_usd
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
JOIN cg_campaigns cc ON d.campaign_id = cc.campaign_id
WHERE d.day BETWEEN DATE('2026-01-01') AND CURRENT_DATE()
GROUP BY 1, 2, 3
ORDER BY 1, 3
