SELECT c.campaign_id, c.campaign_group_id AS grp, g.name AS group_name, c.name AS camp_name,
   c.objective_id AS obj, c.funnel_level AS funnel, c.channel_id AS chan,
   MIN(s.day) first_day, MAX(s.day) last_day, SUM(s.impressions) imps,
   HLL_COUNT.MERGE(s.uniques) reach,
   ROUND(SUM(s.media_spend+s.data_spend+s.platform_spend),0) spend,
   SUM(s.click_conversions+s.view_conversions) conv,
   ROUND(SUM(s.click_order_value+s.view_order_value),0) revenue
 FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
 JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id=s.campaign_id
 LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g ON g.campaign_group_id=c.campaign_group_id
 WHERE s.advertiser_id={{AID}} AND s.day BETWEEN "{{WIN_START}}" AND "{{WIN_END}}" AND c.deleted=FALSE
 GROUP BY 1,2,3,4,5,6,7 HAVING imps>0 ORDER BY imps DESC
