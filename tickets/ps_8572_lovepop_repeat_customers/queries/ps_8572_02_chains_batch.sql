/* PS-8572 step 2 batch: reconstruct 10 sample order chains.
   conv = ui_conversions rows for the 10 orders (order_amt, never order_amt_usd).
   cp   = clickpass_log rows joined on ad_served_id (day-partitioned; date range carried).
   shared = conversions per ad_served_id over the full complaint window (shared-pair claim).
   Lags computed from raw timestamps, never conversion_day (clamped 1-14/NULL).
   Matchback CSV timestamps are EDT (UTC-4); BQ is UTC. */
WITH conv AS (
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
    AND order_id IN ('12181567668297','12173057753161','12175650553929','12186375454793',
                     '12197698535497','12202725376073','12189170991177','12163229089865',
                     '12181317353545','12206853849161')
),
cp AS (
  SELECT
    ad_served_id,
    time AS visit_time,
    impression_time AS cp_impression_time,
    ip AS visit_ip,
    is_new,
    is_cross_device AS cp_is_cross_device,
    campaign_id AS cp_campaign_id
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN '2025-12-01' AND '2026-08-05'
    AND advertiser_id = 58797
),
shared AS (
  SELECT ad_served_id,
         COUNT(*) AS conversions_on_asid,
         COUNT(DISTINCT order_id) AS orders_on_asid
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE advertiser_id = 58797
    AND DATE(time) BETWEEN '2026-06-01' AND '2026-08-05'
  GROUP BY ad_served_id
)
SELECT
  conv.*,
  cp.visit_time,
  cp.cp_impression_time,
  cp.visit_ip,
  cp.is_new,
  cp.cp_is_cross_device,
  cp.cp_campaign_id,
  shared.conversions_on_asid,
  shared.orders_on_asid,
  TIMESTAMP_DIFF(cp.visit_time, conv.impression_time, SECOND) / 86400.0 AS imp_to_visit_days,
  TIMESTAMP_DIFF(conv.conversion_time, cp.visit_time, SECOND) / 86400.0 AS visit_to_conv_days
FROM conv
LEFT JOIN cp ON cp.ad_served_id = conv.ad_served_id
LEFT JOIN shared ON shared.ad_served_id = conv.ad_served_id
ORDER BY conv.order_id, cp.visit_time
LIMIT 200
