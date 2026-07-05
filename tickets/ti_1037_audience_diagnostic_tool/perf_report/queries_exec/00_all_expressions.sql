WITH act AS (SELECT DISTINCT campaign_id FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
   WHERE advertiser_id={{AID}} AND day BETWEEN "{{P2_START}}" AND "{{P2_END}}"),
 ranked AS (SELECT a.campaign_id, a.audience_id, a.expression,
     ROW_NUMBER() OVER (PARTITION BY a.campaign_id ORDER BY a.update_time DESC) rn
   FROM `dw-main-silver.audience.audience_segments` a WHERE a.campaign_id IN (SELECT campaign_id FROM act))
 SELECT campaign_id, audience_id, expression FROM ranked WHERE rn=1
