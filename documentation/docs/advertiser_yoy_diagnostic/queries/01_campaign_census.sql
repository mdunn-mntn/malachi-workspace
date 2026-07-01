-- Campaign census: client-facing GROUPS (campaign_group_id) + internal funnel-stage sub-campaigns.
-- Params: {{AID}} {{WIN_START}} {{WIN_END}}
WITH d AS (
  SELECT campaign_id, MIN(day) fd, MAX(day) ld, SUM(impressions) imps,
         ROUND(SUM(media_spend+data_spend+platform_spend),0) spend, COUNT(DISTINCT day) active_days
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE advertiser_id={{AID}} AND day>="{{WIN_START}}" AND day<"{{WIN_END}}"
  GROUP BY campaign_id HAVING SUM(impressions)>0
)
SELECT c.campaign_group_id AS grp, g.name AS group_name, c.campaign_id, c.name AS camp_name,
  c.objective_id AS obj, c.funnel_level AS funnel,
  CASE c.objective_id WHEN 1 THEN "Prosp" WHEN 4 THEN "Retgt" WHEN 5 THEN "MT2" WHEN 6 THEN "MT3" WHEN 7 THEN "Ego" ELSE CAST(c.objective_id AS STRING) END AS role,
  d.fd AS first_day, d.ld AS last_day, d.active_days, ROUND(d.imps/1e6,2) imps_M, d.spend
FROM d JOIN `dw-main-bronze.integrationprod.campaigns` c USING(campaign_id)
LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g ON g.campaign_group_id=c.campaign_group_id
WHERE c.advertiser_id={{AID}} AND c.deleted=FALSE
ORDER BY d.spend DESC
