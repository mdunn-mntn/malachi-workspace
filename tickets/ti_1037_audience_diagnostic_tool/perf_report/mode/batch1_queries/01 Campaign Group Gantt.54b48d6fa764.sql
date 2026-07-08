-- Dynamic param defaults (Mode date params are static-only, so sentinels map in SQL):
--   Period_Start = 1900-01-01 (the default) -> Jan 1 of the CURRENT year; any other date honored.
--   Period_End is CLAMPED to the first day of the current month (exclusive end ->
--   data through the last FULL month); the far-future default (2099-01-01) relies on this.
-- Module 01 — Campaign-group Gantt (running span per client-facing campaign)
-- One row per campaign_group_id: delivery span first to last active day, active-day
-- count, total spend, impressions. Bars clipped to the trend window by the chart.
-- Grain: campaign_group_id. Source: summarydata.sum_by_campaign_by_day (daily).
WITH camp_day AS (
  SELECT
    c.campaign_group_id                              AS campaign_group_id,
    d.day                                            AS day,
    d.impressions                                    AS imps,
    (d.media_spend + d.data_spend + d.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = d.campaign_id
  WHERE d.advertiser_id = {{ Advertiser_ID }}
    AND c.advertiser_id = {{ Advertiser_ID }}
    AND c.deleted = FALSE
    AND DATE(d.day) >= DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR)
    AND DATE(d.day) <  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))
)
SELECT
  cd.campaign_group_id,
  g.name                                             AS group_name,
  MIN(cd.day)                                        AS first_active_day,
  MAX(cd.day)                                        AS last_active_day,
  DATE_DIFF(MAX(cd.day), MIN(cd.day), DAY) + 1       AS span_days,
  COUNT(DISTINCT cd.day)                             AS active_days,
  ROUND(SUM(cd.spend), 0)                            AS total_spend,
  ROUND(SUM(cd.imps) / 1e6, 3)                       AS imps_m,
  DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR) AS win_start,
  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))                           AS win_end
FROM camp_day cd
LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
  ON g.campaign_group_id = cd.campaign_group_id
GROUP BY cd.campaign_group_id, g.name
HAVING SUM(cd.imps) > 0
ORDER BY total_spend DESC, first_active_day
