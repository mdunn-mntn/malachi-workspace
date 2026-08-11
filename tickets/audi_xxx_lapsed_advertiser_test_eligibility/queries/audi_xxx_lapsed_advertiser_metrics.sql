WITH
ad_ip_win AS (
  SELECT
    advertiser_id,
    ip,
    COUNT(*) AS impressions_ip,
    SUM(COALESCE(media_spend,0) + COALESCE(data_spend,0) + COALESCE(platform_spend,0)) AS spend_ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE '{{WIN_START}}' AND DATE '{{WIN_END}}'
    AND advertiser_id = {{ADVERTISER_ID}}
  GROUP BY 1, 2
),
visiting_win AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE '{{WIN_START}}' AND DATE '{{WIN_END}}'
    AND advertiser_id = {{ADVERTISER_ID}}
  GROUP BY 1, 2
),
converting_win AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN DATE '{{WIN_START}}' AND DATE '{{WIN_END}}'
    AND advertiser_id = {{ADVERTISER_ID}}
  GROUP BY 1, 2
),
served_56d AS (
  SELECT advertiser_id, APPROX_COUNT_DISTINCT(ip) AS distinct_ips_56d
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE '{{WIN56_START}}' AND DATE '{{WIN_END}}'
    AND advertiser_id = {{ADVERTISER_ID}}
  GROUP BY 1
),
window_rollup AS (
  SELECT
    a.advertiser_id,
    SUM(a.spend_ip)                 AS spend_win,
    SUM(a.impressions_ip)           AS impressions_win,
    COUNT(*)                        AS distinct_ips_win,
    SUM(IF(v.ip IS NOT NULL, 1, 0)) AS visiting_ips_win,
    SUM(IF(c.ip IS NOT NULL, 1, 0)) AS converting_ips_win
  FROM ad_ip_win        a
  LEFT JOIN visiting_win   v USING (advertiser_id, ip)
  LEFT JOIN converting_win c USING (advertiser_id, ip)
  GROUP BY 1
  HAVING SUM(a.impressions_ip) > 0
),
monthly_spend AS (
  SELECT
    advertiser_id,
    DATE_TRUNC(day, MONTH) AS month_start,
    SUM(COALESCE(media_spend,0) + COALESCE(data_spend,0) + COALESCE(platform_spend,0)) AS month_spend
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN DATE '{{SPEND_HIST_START}}' AND DATE '{{WIN_END}}'
    AND advertiser_id = {{ADVERTISER_ID}}
  GROUP BY 1, 2
  HAVING SUM(COALESCE(media_spend,0) + COALESCE(data_spend,0) + COALESCE(platform_spend,0)) > 1000
),
active_month_stats AS (
  SELECT
    advertiser_id,
    APPROX_QUANTILES(month_spend, 100)[OFFSET(50)] AS typical_active_month_spend,
    MAX(month_spend)                               AS max_month_spend,
    COUNT(*)                                       AS active_months_count
  FROM monthly_spend
  GROUP BY 1
),
vertical_buckets AS (
  SELECT
    advertiser_id,
    LOGICAL_OR(vertical_name = 'B2B Software & Services') AS is_b2b,
    STRING_AGG(DISTINCT vertical_name, ' | ' ORDER BY vertical_name) AS vertical_buckets
  FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
  WHERE type = 0
    AND advertiser_id = {{ADVERTISER_ID}}
  GROUP BY 1
)
SELECT
  w.advertiser_id,
  adv.company_name                                            AS advertiser_name,
  COALESCE(adv.active, FALSE)                                 AS active,
  COALESCE(adv.is_test, FALSE)                                AS is_test,
  COALESCE(vb.is_b2b, FALSE)                                  AS is_b2b,
  vb.vertical_buckets,
  DATE '{{WIN_START}}'                                        AS window_start,
  DATE '{{WIN_END}}'                                          AS window_end,
  w.spend_win                                                 AS spend_30d,
  w.impressions_win                                           AS impressions_30d,
  w.distinct_ips_win                                          AS distinct_ips_30d,
  COALESCE(s56.distinct_ips_56d, w.distinct_ips_win)          AS distinct_ips_56d,
  w.visiting_ips_win                                          AS visiting_ips_30d,
  w.converting_ips_win                                        AS converting_ips_30d,
  SAFE_DIVIDE(w.spend_win, w.impressions_win) * 1000          AS cpm,
  SAFE_DIVIDE(w.impressions_win, w.distinct_ips_win)          AS imps_per_ip,
  SAFE_DIVIDE(w.visiting_ips_win,   w.distinct_ips_win)       AS p_visit,
  SAFE_DIVIDE(w.converting_ips_win, w.distinct_ips_win)       AS p_cvr,
  COALESCE(ams.typical_active_month_spend, w.spend_win)       AS typical_active_month_spend,
  COALESCE(ams.max_month_spend,            w.spend_win)       AS max_month_spend,
  COALESCE(ams.active_months_count,        1)                 AS active_months_count
FROM window_rollup w
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` adv USING (advertiser_id)
LEFT JOIN active_month_stats ams USING (advertiser_id)
LEFT JOIN served_56d         s56 USING (advertiser_id)
LEFT JOIN vertical_buckets   vb  USING (advertiser_id)
WHERE COALESCE(adv.deleted, FALSE) = FALSE
