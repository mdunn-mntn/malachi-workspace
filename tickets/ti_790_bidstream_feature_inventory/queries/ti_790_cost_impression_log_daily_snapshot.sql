-- TI-790: cost_impression_log daily IP snapshot
-- Net-new signals: recency_elapsed_time, cost breakdown (media/data/platform),
--   household_score, ott_device, partner_ad_format, supply_vendor
-- Cost: ~7 GB/day, ~21s wall time. Tested 2026-03-31.
-- Note: recency_elapsed_time is INTERVAL type — extract to seconds
-- Note: household_score = -1 means unscored, advertiser_household_score = 10000 means RTC

SELECT
  ip, DATE(time) AS event_date,
  COUNT(*) AS n_impressions,

  -- Recency (UNIQUE — time between impressions)
  AVG(EXTRACT(SECOND FROM recency_elapsed_time)
    + EXTRACT(MINUTE FROM recency_elapsed_time) * 60
    + EXTRACT(HOUR FROM recency_elapsed_time) * 3600) AS avg_recency_seconds,

  -- Fangorn scores (UNIQUE — potentially circular, use carefully)
  -- household_score = -1 means unscored
  -- advertiser_household_score = 10000 means RTC conquest
  AVG(CASE WHEN SAFE_CAST(household_score AS FLOAT64) > 0 THEN SAFE_CAST(household_score AS FLOAT64) END) AS avg_household_score_scored,
  AVG(CASE WHEN SAFE_CAST(advertiser_household_score AS FLOAT64) > 0
       AND SAFE_CAST(advertiser_household_score AS FLOAT64) < 10000
       THEN SAFE_CAST(advertiser_household_score AS FLOAT64) END) AS avg_adv_household_score_non_rtc,
  ROUND(COUNTIF(SAFE_CAST(advertiser_household_score AS FLOAT64) = 10000) / COUNT(*), 4) AS pct_rtc_impressions,

  -- Cost breakdown (UNIQUE)
  SUM(SAFE_CAST(media_cost AS FLOAT64)) AS total_media_cost,
  AVG(SAFE_CAST(media_cost AS FLOAT64)) AS avg_media_cost,

  -- OTT device (UNIQUE classification)
  COUNT(DISTINCT ott_device) AS n_distinct_ott_devices,

  -- Ad format (UNIQUE — authoritative VIDEO vs BANNER)
  ROUND(COUNTIF(partner_ad_format = 'VIDEO') / COUNT(*), 4) AS pct_video_format,
  ROUND(COUNTIF(partner_ad_format = 'BANNER') / COUNT(*), 4) AS pct_banner_format,

  -- Supply vendor (UNIQUE)
  COUNT(DISTINCT supply_vendor) AS n_distinct_supply_vendors,

  -- New visitor flag
  ROUND(COUNTIF(SAFE_CAST(is_new AS BOOL)) / COUNT(*), 4) AS pct_new_impressions,

  -- PMP
  COUNTIF(private_marketplace_id IS NOT NULL
    AND private_marketplace_id != ''
    AND private_marketplace_id != '0') AS n_pmp_impressions

FROM `dw-main-silver.logdata.cost_impression_log`
WHERE DATE(time) = '2026-03-30'  -- replace with target date
  AND ip IS NOT NULL AND ip != ''
GROUP BY ip, event_date
;
