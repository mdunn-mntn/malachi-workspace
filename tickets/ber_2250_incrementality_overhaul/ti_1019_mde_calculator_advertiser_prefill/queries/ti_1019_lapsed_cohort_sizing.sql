WITH per_adv AS (
  SELECT
    advertiser_id,
    MAX(day) AS last_active_day,
    MIN(day) AS first_active_day,
    COUNT(*) AS delivering_days,
    SUM(COALESCE(media_spend,0) + COALESCE(data_spend,0) + COALESCE(platform_spend,0)) AS lifetime_spend
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN DATE '2024-01-01' AND CURRENT_DATE()
    AND impressions > 0
  GROUP BY advertiser_id
),
tagged AS (
  SELECT
    *,
    DATE_DIFF(CURRENT_DATE(), last_active_day, DAY) AS days_since,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), last_active_day, DAY) <= 30  THEN '0_delivering_30d'
      WHEN DATE_DIFF(CURRENT_DATE(), last_active_day, DAY) <= 90  THEN '1_lapsed_31_90d'
      WHEN DATE_DIFF(CURRENT_DATE(), last_active_day, DAY) <= 180 THEN '2_lapsed_91_180d'
      WHEN DATE_DIFF(CURRENT_DATE(), last_active_day, DAY) <= 365 THEN '3_lapsed_181_365d'
      ELSE '4_lapsed_over_365d'
    END AS bucket
  FROM per_adv
)
SELECT
  bucket,
  COUNT(*) AS advertisers,
  COUNTIF(lifetime_spend >= 10000) AS adv_lifetime_10k_plus,
  COUNTIF(delivering_days >= 28) AS adv_28d_plus_delivery,
  ROUND(SUM(lifetime_spend)/1e6, 2) AS lifetime_spend_musd,
  ROUND(APPROX_QUANTILES(lifetime_spend, 100)[OFFSET(50)], 0) AS median_lifetime_spend
FROM tagged
GROUP BY bucket
ORDER BY bucket
