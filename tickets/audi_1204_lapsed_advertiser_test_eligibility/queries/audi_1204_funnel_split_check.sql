WITH camp AS (
  SELECT campaign_id, objective_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = 39568
),
served AS (
  SELECT
    CASE WHEN c.objective_id IN (1,5,6) THEN 'prospecting'
         WHEN c.objective_id = 4 THEN 'retargeting'
         ELSE CONCAT('obj_', CAST(c.objective_id AS STRING)) END AS funnel,
    cil.ip,
    COUNT(*) AS imps,
    SUM(COALESCE(cil.media_spend,0)+COALESCE(cil.data_spend,0)+COALESCE(cil.platform_spend,0)) AS spend
  FROM `dw-main-silver.logdata.cost_impression_log` cil
  JOIN camp c USING (campaign_id)
  WHERE DATE(cil.time) BETWEEN DATE '2026-04-07' AND DATE '2026-05-06'
    AND cil.advertiser_id = 39568
  GROUP BY 1,2
),
vis AS (
  SELECT DISTINCT ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE '2026-04-07' AND DATE '2026-05-06'
    AND advertiser_id = 39568
)
SELECT
  s.funnel,
  COUNT(*)                                  AS served_ips,
  SUM(s.imps)                               AS impressions,
  ROUND(SUM(s.spend),0)                     AS spend,
  ROUND(SAFE_DIVIDE(SUM(s.spend),SUM(s.imps))*1000,2) AS cpm,
  ROUND(SAFE_DIVIDE(SUM(s.imps),COUNT(*)),2)          AS imps_per_ip,
  COUNTIF(v.ip IS NOT NULL)                 AS visiting_ips,
  ROUND(SAFE_DIVIDE(COUNTIF(v.ip IS NOT NULL), COUNT(*))*100,3) AS ivr_pct
FROM served s
LEFT JOIN vis v USING (ip)
GROUP BY 1
ORDER BY served_ips DESC
