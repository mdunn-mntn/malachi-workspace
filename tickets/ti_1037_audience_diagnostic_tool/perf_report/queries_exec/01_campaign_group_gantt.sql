-- Module 01 — Campaign-group Gantt (running span per client-facing campaign)
-- One row per campaign_group_id: delivery span (first->last active day), active-day
-- count, total spend, impressions. Bars clipped to [WIN_START, WIN_END) by the chart.
-- Grain: campaign_group_id. Source: summarydata.sum_by_campaign_by_day (daily).
-- Params: {{AID}} {{WIN_START}} {{WIN_END}} (WIN_END exclusive).
WITH camp_day AS (
  SELECT
    c.campaign_group_id                              AS campaign_group_id,
    d.day                                            AS day,
    d.impressions                                    AS imps,
    (d.media_spend + d.data_spend + d.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = d.campaign_id
  WHERE d.advertiser_id = {{AID}}
    AND c.advertiser_id = {{AID}}
    AND c.deleted = FALSE
    AND d.day >= "{{WIN_START}}"
    AND d.day <  "{{WIN_END}}"
)
SELECT
  cd.campaign_group_id,
  g.name                                             AS group_name,
  MIN(cd.day)                                        AS first_active_day,
  MAX(cd.day)                                        AS last_active_day,
  DATE_DIFF(MAX(cd.day), MIN(cd.day), DAY) + 1       AS span_days,
  COUNT(DISTINCT cd.day)                             AS active_days,
  ROUND(SUM(cd.spend), 0)                            AS total_spend,
  ROUND(SUM(cd.imps) / 1e6, 3)                       AS imps_m
FROM camp_day cd
LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
  ON g.campaign_group_id = cd.campaign_group_id
GROUP BY cd.campaign_group_id, g.name
HAVING SUM(cd.imps) > 0
ORDER BY first_active_day, total_spend DESC