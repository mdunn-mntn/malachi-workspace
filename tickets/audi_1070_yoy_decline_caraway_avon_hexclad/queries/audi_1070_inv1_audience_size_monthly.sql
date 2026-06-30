WITH camp AS (
  SELECT campaign_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id IN (40341,31921,34611) AND deleted=FALSE AND is_test=FALSE
    AND funnel_level = 1
),
daily AS (
  SELECT c.advertiser_id, a.rpt_day,
         MAX(a.total_audience_size) AS day_pool
  FROM `dw-main-silver.perml.flight_cid_day_audience_sizes` a
  JOIN camp c ON a.campaign_id = c.campaign_id
  GROUP BY 1,2
)
SELECT advertiser_id,
       DATE_TRUNC(rpt_day, MONTH) mo,
       COUNT(*) n_days,
       ROUND(AVG(day_pool)) avg_pool,
       ROUND(MIN(day_pool)) min_pool,
       ROUND(MAX(day_pool)) max_pool
FROM daily
GROUP BY 1,2
ORDER BY 1,2
