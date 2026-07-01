/* Prospecting rate metrics for two periods (visit rate, ROAS, conv, AOV, OV). | Params: {{AID}} {{P1_START}} {{P1_END}} {{P2_START}} {{P2_END}} */
WITH camp AS (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id={{AID}} AND objective_id IN (1,5,6) AND deleted=FALSE),
p AS (
  SELECT CASE WHEN day>="{{P1_START}}" AND day<"{{P1_END}}" THEN "P1_earlier"
              WHEN day>="{{P2_START}}" AND day<"{{P2_END}}" THEN "P2_later" END period,
    SUM(impressions) imps, SUM(views+clicks) visits, SUM(media_spend+data_spend+platform_spend) spend,
    SUM(click_conversions+view_conversions) conv, SUM(click_order_value+view_order_value) rev
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE campaign_id IN (SELECT campaign_id FROM camp)
    AND ((day>="{{P1_START}}" AND day<"{{P1_END}}") OR (day>="{{P2_START}}" AND day<"{{P2_END}}"))
  GROUP BY period)
SELECT period, ROUND(spend,0) spend, imps, visits, ROUND(100*visits/NULLIF(imps,0),3) visit_rate_pct,
  conv, ROUND(rev/NULLIF(spend,0),2) roas, ROUND(rev/NULLIF(conv,0),2) aov, ROUND(rev,0) order_value
FROM p WHERE period IS NOT NULL ORDER BY period
