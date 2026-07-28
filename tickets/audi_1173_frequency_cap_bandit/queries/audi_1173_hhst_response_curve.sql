-- Q3v2 (CORRECTED): HS-band response curve, has_mm prospecting, exclude GENUINE RTC (realtime_conquest_score=10000).
-- Band by household_score (what the HHST gate acts on). WGU excluded. Pooled + adv-median IVR/CPV.
WITH cohort AS (
  SELECT campaign_id FROM `dw-main-silver.audience.mm_campaign_classifier`
  WHERE has_mm = TRUE AND objective_id = 1
),
imp AS (
  SELECT c.impression_id, c.advertiser_id,
    CASE WHEN COALESCE(c.household_score,-1) >= 6666 THEN '1_HI_6666_10000'
         WHEN COALESCE(c.household_score,-1) >= 3333 THEN '2_MI_3333_6665'
         WHEN COALESCE(c.household_score,-1) >= 1    THEN '3_MaxReach_PP_1_3332'
         ELSE '4_unscored' END AS band,
    (c.media_spend + c.data_spend + c.platform_spend) AS spend
  FROM `dw-main-silver.logdata.cost_impression_log` c
  JOIN cohort USING (campaign_id)
  WHERE DATE(c.time) BETWEEN @d0 AND @d1
    AND c.advertiser_id <> 31357
    AND COALESCE(SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'realtime_conquest_score=(-?\d+)') AS INT64),-1) <> 10000
),
vis AS (
  SELECT impression_id, COUNT(*) AS visits FROM (
    SELECT DISTINCT advertiser_id, guid, epoch, impression_id
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE DATE(time) BETWEEN @d0 AND @dvis
      AND source_type = 'last_tv_touch_visits' AND impression_id IS NOT NULL
  ) GROUP BY impression_id
),
joined AS (SELECT imp.band, imp.advertiser_id, imp.spend, COALESCE(v.visits,0) AS visits FROM imp LEFT JOIN vis v USING (impression_id)),
per_adv AS (
  SELECT band, advertiser_id, COUNT(*) imps, SUM(spend) spend, SUM(visits) visits,
    SAFE_DIVIDE(SUM(visits),COUNT(*)) ivr, SAFE_DIVIDE(SUM(spend),NULLIF(SUM(visits),0)) cpv
  FROM joined GROUP BY band, advertiser_id
)
SELECT band, SUM(imps) impressions,
  ROUND(100*SUM(imps)/SUM(SUM(imps)) OVER (),2) imp_share_pct,
  ROUND(SUM(spend),2) media_spend,
  ROUND(100*SUM(spend)/SUM(SUM(spend)) OVER (),2) spend_share_pct,
  SUM(visits) visits,
  ROUND(100*SAFE_DIVIDE(SUM(visits),SUM(imps)),4) ivr_pooled_pct,
  ROUND(SAFE_DIVIDE(SUM(spend),NULLIF(SUM(visits),0)),4) cpv_pooled,
  ROUND(100*APPROX_QUANTILES(ivr,100)[OFFSET(50)],4) ivr_adv_median_pct,
  ROUND(APPROX_QUANTILES(cpv,100)[OFFSET(50)],4) cpv_adv_median,
  COUNT(DISTINCT advertiser_id) n_advertisers
FROM per_adv GROUP BY band ORDER BY band
