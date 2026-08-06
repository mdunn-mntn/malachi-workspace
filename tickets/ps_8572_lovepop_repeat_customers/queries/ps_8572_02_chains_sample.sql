/* PS-8572 step 2, sample-first: one order chain from ui_conversions (order 12206853849161).
   Lags computed downstream from raw timestamps, never conversion_day (clamped 1-14/NULL). */
SELECT
  order_id,
  time AS conversion_time,
  event_time,
  ip AS conversion_ip,
  ad_served_id,
  first_touch_ad_served_id,
  impression_id,
  impression_time,
  impression_ip,
  campaign_id,
  group_id,
  order_amt,
  source_type,
  conversion_type,
  conversion_day,
  is_cross_device,
  attribution_model_id
FROM `dw-main-silver.summarydata.ui_conversions`
WHERE advertiser_id = 58797
  AND DATE(time) BETWEEN '2026-06-01' AND '2026-08-05'
  AND order_id = '12206853849161'
LIMIT 100
