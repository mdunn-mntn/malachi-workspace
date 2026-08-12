SELECT
  MAX(day)                                      AS last_active_day,
  MIN(day)                                      AS first_active_day,
  COUNT(*)                                      AS delivering_days,
  SUM(COALESCE(media_spend,0) + COALESCE(data_spend,0) + COALESCE(platform_spend,0)) AS lifetime_spend
FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
WHERE day BETWEEN DATE '{{HISTORY_FLOOR}}' AND DATE '{{TODAY}}'
  AND advertiser_id = {{ADVERTISER_ID}}
  AND impressions > 0
