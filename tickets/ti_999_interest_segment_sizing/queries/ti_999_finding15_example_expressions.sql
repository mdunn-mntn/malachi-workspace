-- TI-999 Finding 15 proof — pull actual audience expressions for example
-- campaigns illustrating each pattern:
--   - MM + 3P incl_only with high unscored delivery (FICO, Global X ETFs)
--   - MM + 3P incl_only with low unscored delivery (Cheddar's)
--   - MM + 1P excl_only typical (Zazzle, HexClad)
--   - MM_only baseline (LongHorn Steakhouse)
--
-- Output: campaign_id, advertiser_name, expression (truncated for readability).

WITH active_campaigns AS (
  SELECT campaign_id, advertiser_id,
         SUM(impressions) AS impressions_30d,
         SUM(media_spend + data_spend + platform_spend) AS spend_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2 HAVING SUM(impressions) > 0
),
candidate_campaigns AS (
  SELECT ac.campaign_id, ac.advertiser_id, a.company_name AS advertiser_name,
         ac.impressions_30d, ROUND(ac.spend_30d / 1e3, 1) AS spend_30d_K
  FROM active_campaigns ac
  JOIN `dw-main-bronze.integrationprod.advertisers` a USING (advertiser_id)
  WHERE ac.advertiser_id IN (37056, 35312, 34834, 37775, 34611, 34835)  -- FICO, Global X, Cheddar's, Zazzle, HexClad, LongHorn
)
SELECT
  cc.campaign_id, cc.advertiser_name, cc.advertiser_id,
  cc.impressions_30d, cc.spend_30d_K,
  asg.expression
FROM candidate_campaigns cc
JOIN (
  SELECT campaign_id, expression
  FROM (
    SELECT campaign_id, expression,
           ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
  ) WHERE rn = 1
) asg USING (campaign_id)
ORDER BY cc.advertiser_id, cc.spend_30d_K DESC;
