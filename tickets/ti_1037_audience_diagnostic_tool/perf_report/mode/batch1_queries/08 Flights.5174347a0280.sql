-- Module 08 -- Scheduled FLIGHTS per prospecting campaign_group (from core_flights)
-- Scheduled flights (Start/End) set manually per campaign. NOT delivery -- a group can
-- deliver continuously yet be many short back-to-back flights. Short flights auto-ungate.
-- Scope: groups running a live prospecting campaign in the window; flights overlapping it.
-- Prospecting derived dynamically as objective_id=1, funnel_level=1, deleted=FALSE.
-- Spend per group joined from sum_by_campaign_by_day over the recent period, used to rank groups.
WITH prosp_groups AS (
  SELECT DISTINCT c.campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN `dw-main-silver.summarydata.sum_by_campaign_by_day` d ON d.campaign_id = c.campaign_id
  WHERE c.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
    AND d.advertiser_id = {{ Advertiser_ID }}
    AND d.day >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND d.day <  DATE('{{ Period_End }}')
    AND d.impressions > 0
),
grp_name AS (
  SELECT campaign_group_id, name AS group_name
  FROM `dw-main-bronze.integrationprod.campaign_groups`
),
grp_spend AS (
  SELECT c.campaign_group_id, SUM(d.media_spend + d.data_spend + d.platform_spend) AS prosp_spend
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN `dw-main-silver.summarydata.sum_by_campaign_by_day` d ON d.campaign_id = c.campaign_id
  WHERE c.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
    AND d.advertiser_id = {{ Advertiser_ID }}
    AND d.day >= DATE('{{ Period_Start }}')
    AND d.day <  DATE('{{ Period_End }}')
  GROUP BY c.campaign_group_id
)
SELECT
  f.campaign_group_id,
  g.group_name,
  f.flight_id,
  DATE(f.start_time)                                       AS flight_start,
  DATE(f.end_time)                                         AS flight_end,
  DATE_DIFF(DATE(f.end_time), DATE(f.start_time), DAY) + 1 AS flight_days,
  f.budget,
  f.status_id,
  COALESCE(s.prosp_spend, 0)                               AS group_prosp_spend,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)    AS win_start,
  DATE('{{ Period_End }}')                                 AS win_end,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)    AS p1_start,
  DATE_SUB(DATE('{{ Period_End }}'),   INTERVAL 1 YEAR)    AS p1_end,
  DATE('{{ Period_Start }}')                               AS p2_start,
  DATE('{{ Period_End }}')                                 AS p2_end
FROM `dw-main-bronze.integrationprod.core_flights` f
JOIN prosp_groups p USING (campaign_group_id)
LEFT JOIN grp_name  g USING (campaign_group_id)
LEFT JOIN grp_spend s USING (campaign_group_id)
WHERE f.start_time <  TIMESTAMP(DATE('{{ Period_End }}'))
  AND f.end_time  >= TIMESTAMP(DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR))
ORDER BY f.campaign_group_id, f.start_time
