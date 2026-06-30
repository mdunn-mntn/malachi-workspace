-- AUDI-1070 case — Avon (31921) campaign list, ranked by total spend, H1-2025 vs H1-2026 activity.
-- Drive off perf (every campaign that actually delivered) LEFT JOIN latest dim version.
WITH perf AS (
  SELECT campaign_id, MIN(day) first_day, MAX(day) last_day,
    SUM(IF(day BETWEEN '2025-01-01' AND '2025-06-30', impressions,0)) imps_25,
    SUM(IF(day BETWEEN '2026-01-01' AND '2026-06-30', impressions,0)) imps_26,
    SUM(IF(day BETWEEN '2025-01-01' AND '2025-06-30', views+clicks,0)) vis_25,
    SUM(IF(day BETWEEN '2026-01-01' AND '2026-06-30', views+clicks,0)) vis_26,
    ROUND(SUM(IF(day BETWEEN '2025-01-01' AND '2025-06-30', media_spend+platform_spend+data_spend,0))) sp_25,
    ROUND(SUM(IF(day BETWEEN '2026-01-01' AND '2026-06-30', media_spend+platform_spend+data_spend,0))) sp_26,
    ROUND(SUM(IF(day BETWEEN '2025-01-01' AND '2025-06-30', view_order_value+click_order_value,0))) rev_25,
    ROUND(SUM(IF(day BETWEEN '2026-01-01' AND '2026-06-30', view_order_value+click_order_value,0))) rev_26,
    ROUND(SUM(media_spend+platform_spend+data_spend)) spend_all
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE advertiser_id=31921 AND day >= '2024-01-01' GROUP BY campaign_id ),
meta AS (
  SELECT campaign_id, name, objective_id, funnel_level, channel_id, campaign_group_id, deleted
  FROM `dw-main-bronze.integrationprod.archives_campaign_archives`
  WHERE advertiser_id=31921
  QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY version DESC)=1 )
SELECT p.campaign_id, m.name, m.objective_id obj, m.funnel_level fl, m.channel_id ch,
  p.first_day, p.last_day, p.imps_25, p.imps_26, p.vis_25, p.vis_26,
  p.sp_25, p.sp_26, p.rev_25, p.rev_26, p.spend_all, m.deleted
FROM perf p LEFT JOIN meta m USING(campaign_id) ORDER BY p.spend_all DESC;
