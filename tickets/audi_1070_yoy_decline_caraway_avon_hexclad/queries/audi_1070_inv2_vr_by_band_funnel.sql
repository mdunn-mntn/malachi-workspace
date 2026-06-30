WITH fmap AS (
  SELECT campaign_id,
    CASE WHEN objective_id = 1 AND funnel_level = 1 THEN 'prospecting'
         WHEN objective_id = 7 THEN 'ego'
         ELSE 'retarget_mt' END AS funnel_role
  FROM dw-main-bronze.integrationprod.archives_campaign_archives
  WHERE advertiser_id = AID_PLACEHOLDER
  GROUP BY campaign_id, funnel_role
),
imps AS (
  SELECT
    ad_served_id,
    FORMAT_TIMESTAMP('%Y-%m', time) AS mo,
    CASE
      WHEN household_score IS NULL OR household_score < 0 THEN '0_unscored'
      WHEN household_score = 0 THEN '0_unscored'
      WHEN household_score BETWEEN 1 AND 3332 THEN '1_1_3332'
      WHEN household_score BETWEEN 3333 AND 6665 THEN '2_3333_6665'
      WHEN household_score BETWEEN 6666 AND 7999 THEN '3_6666_7999'
      WHEN household_score BETWEEN 8000 AND 9999 THEN '4_8000_9999'
      WHEN household_score = 10000 THEN '5_10000'
    END AS band,
    COALESCE(f.funnel_role, 'unknown') AS funnel_role
  FROM dw-main-silver.logdata.cost_impression_log c
  LEFT JOIN fmap f USING (campaign_id)
  WHERE c.advertiser_id = AID_PLACEHOLDER
    AND c.time >= TIMESTAMP('2025-06-01') AND c.time < TIMESTAMP('2026-06-01')
    AND c.model_params NOT LIKE '%realtime_conquest_score=10000%'
),
vis AS (
  SELECT ad_served_id, COUNT(*) AS visits
  FROM dw-main-silver.logdata.clickpass_log
  WHERE advertiser_id = AID_PLACEHOLDER
    AND time >= TIMESTAMP('2025-06-01') AND time < TIMESTAMP('2026-08-01')
    AND ad_served_id IS NOT NULL
  GROUP BY ad_served_id
)
SELECT
  i.mo, i.funnel_role, i.band,
  COUNT(*) AS impressions,
  SUM(IFNULL(v.visits,0)) AS visits,
  ROUND(100*SUM(IFNULL(v.visits,0))/COUNT(*), 4) AS vr_pct
FROM imps i
LEFT JOIN vis v USING (ad_served_id)
GROUP BY i.mo, i.funnel_role, i.band
ORDER BY i.mo, i.funnel_role, i.band
