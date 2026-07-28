-- Q4: universe reconciliation for the has_mm prospecting cohort, 7d (2026-07-06..07-12).
-- Per household_score band: impressions & spend WITH RTC included vs non-RTC (AHS!=10000).
-- No impression_id / no visit join -> cheaper scan. Reconciles Q3's shrunk universe vs Q1 denominator.
WITH cohort AS (
  SELECT campaign_id
  FROM `dw-main-silver.audience.mm_campaign_classifier`
  WHERE has_mm = TRUE AND objective_id = 1
),
cil AS (
  SELECT
    CASE WHEN COALESCE(c.household_score,-1) >= 6666 THEN '1_HI_6666_10000'
         WHEN COALESCE(c.household_score,-1) >= 3333 THEN '2_MI_3333_6665'
         WHEN COALESCE(c.household_score,-1) >= 1    THEN '3_MaxReach_PP_1_3332'
         ELSE '4_unscored' END AS band,
    (c.media_spend + c.data_spend + c.platform_spend) AS spend,
    (COALESCE(c.advertiser_household_score,-1) = 10000) AS is_rtc
  FROM `dw-main-silver.logdata.cost_impression_log` c
  JOIN cohort USING (campaign_id)
  WHERE DATE(c.time) BETWEEN '2026-07-06' AND '2026-07-12'
    AND c.advertiser_id <> 31357
)
SELECT
  band,
  COUNT(*)                                            AS imps_all,
  ROUND(SUM(spend),2)                                 AS spend_all,
  COUNTIF(NOT is_rtc)                                 AS imps_nonrtc,
  ROUND(SUM(IF(NOT is_rtc, spend, 0)),2)              AS spend_nonrtc,
  ROUND(100*SUM(IF(is_rtc,spend,0))/SUM(spend),2)     AS pct_spend_rtc
FROM cil
GROUP BY band
ORDER BY band;
