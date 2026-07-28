-- Frequency Phase-0 / Q2: cross-group leakage. Per household (ip x advertiser), how many distinct
-- campaign_groups served it (fcap has no advertiser rollup -> each group counts the IP independently).
WITH cohort AS (
  SELECT campaign_id, campaign_group_id FROM `dw-main-silver.audience.mm_campaign_classifier`
  WHERE has_mm = TRUE AND objective_id = 1
),
imp AS (
  SELECT c.ip, c.advertiser_id, co.campaign_group_id
  FROM `dw-main-silver.logdata.cost_impression_log` c
  JOIN cohort co USING (campaign_id)
  WHERE DATE(c.time) BETWEEN '2026-07-06' AND '2026-07-12'
    AND c.advertiser_id <> 31357
    AND c.ip IS NOT NULL AND c.ip <> '0.0.0.0'
),
hh AS (
  SELECT ip, advertiser_id,
    COUNT(DISTINCT campaign_group_id) AS n_groups,
    COUNT(*) AS imps
  FROM imp GROUP BY ip, advertiser_id
)
SELECT
  CASE WHEN n_groups=1 THEN '1_group' WHEN n_groups=2 THEN '2_groups'
       WHEN n_groups=3 THEN '3_groups' WHEN n_groups<=5 THEN '4-5_groups'
       ELSE '6+_groups' END AS groups_bucket,
  COUNT(*)                                        AS n_households,
  ROUND(100*COUNT(*)/SUM(COUNT(*)) OVER(),2)      AS hh_share_pct,
  SUM(imps)                                       AS impressions,
  ROUND(100*SUM(imps)/SUM(SUM(imps)) OVER(),2)    AS imp_share_pct,
  ROUND(AVG(imps),2)                              AS avg_imps_per_hh
FROM hh GROUP BY groups_bucket ORDER BY groups_bucket
