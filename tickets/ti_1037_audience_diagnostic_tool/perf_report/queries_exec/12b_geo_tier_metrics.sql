-- Module 12b — per prospecting campaign_group performance, P1 vs P2.
-- Groups are derived dynamically (prospecting = objective_id=1, funnel_level=1, deleted=FALSE);
-- never hardcode campaign_group_ids. visits = views+clicks, conv = click+view conversions,
-- spend = media+data+platform, revenue = click+view order value. Lookback bound satisfies
-- partition elimination. The chart tiers these groups by geo include-set size (national geo
-- collapses to a single "national" bucket; only geo-sliced advertisers split into tiers).
WITH d AS (
  SELECT c.campaign_group_id AS grp,
    CASE WHEN s.day BETWEEN "{{P1_START}}" AND "{{P1_END}}" THEN "P1"
         WHEN s.day BETWEEN "{{P2_START}}" AND "{{P2_END}}" THEN "P2" END AS period,
    s.impressions, s.views, s.clicks, s.click_conversions, s.view_conversions,
    s.click_order_value, s.view_order_value, s.media_spend, s.data_spend, s.platform_spend, s.day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{AID}}
    AND s.day BETWEEN "{{P1_START}}" AND "{{P2_END}}"
    AND c.campaign_group_id IN (
      SELECT DISTINCT campaign_group_id FROM `dw-main-bronze.integrationprod.campaigns`
      WHERE advertiser_id = {{AID}} AND deleted = FALSE
        AND objective_id = 1 AND funnel_level = 1)
)
SELECT grp AS campaign_group_id, period,
  MIN(day) first_day, MAX(day) last_day,
  SUM(impressions) imps, SUM(views+clicks) visits,
  SUM(click_conversions+view_conversions) conv,
  ROUND(SUM(media_spend+data_spend+platform_spend),0) spend,
  ROUND(SUM(click_order_value+view_order_value),0) revenue,
  ROUND(1000*SUM(views+clicks)/NULLIF(SUM(impressions),0),3) vr_permille,
  ROUND(100*SUM(click_conversions+view_conversions)/NULLIF(SUM(views+clicks),0),2) cvr_pct,
  ROUND(SUM(click_order_value+view_order_value)/NULLIF(SUM(media_spend+data_spend+platform_spend),0),2) roas
FROM d WHERE period IS NOT NULL
GROUP BY grp, period ORDER BY grp, period
