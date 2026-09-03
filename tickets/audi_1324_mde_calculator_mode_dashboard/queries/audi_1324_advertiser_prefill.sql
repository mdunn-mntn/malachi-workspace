-- Advertiser Prefill — the Mode dataset behind the MDE calculator.
-- Query name must stay "Advertiser Prefill"; index.html resolves window.datasets by it.
-- One row per advertiser that delivered in the last 365 days, delivering or lapsed.
-- Rate window is each advertiser's own last 30 delivering days, so a lapsed account is
-- measured on how it actually performed rather than on silence.
-- ~2.9 TB per run, 53s wall on the reservation; schedule weekly.

WITH activity AS (
  SELECT advertiser_id, MAX(day) AS last_active_day
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND CURRENT_DATE()
    AND impressions > 0
    AND advertiser_id IS NOT NULL AND advertiser_id != 9090
  GROUP BY 1
),
cohort AS (
  SELECT
    advertiser_id,
    last_active_day,
    DATE_SUB(last_active_day, INTERVAL 29 DAY) AS win_start,
    last_active_day                            AS win_end,
    last_active_day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS is_delivering
  FROM activity
),
ad_ip AS (
  SELECT
    l.advertiser_id,
    l.ip,
    COUNT(*) AS impressions_ip,
    SUM(COALESCE(l.media_spend,0)+COALESCE(l.data_spend,0)+COALESCE(l.platform_spend,0)) AS spend_ip
  FROM `dw-main-silver.logdata.cost_impression_log` l
  JOIN cohort c ON c.advertiser_id = l.advertiser_id
  WHERE DATE(l.time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND CURRENT_DATE()
    AND DATE(l.time) BETWEEN c.win_start AND c.win_end
  GROUP BY 1, 2
),
visiting AS (
  SELECT v.advertiser_id, v.ip
  FROM `dw-main-silver.logdata.clickpass_log` v
  JOIN cohort c ON c.advertiser_id = v.advertiser_id
  WHERE DATE(v.time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND CURRENT_DATE()
    AND DATE(v.time) BETWEEN c.win_start AND c.win_end
  GROUP BY 1, 2
),
converting AS (
  SELECT u.advertiser_id, u.ip
  FROM `dw-main-silver.summarydata.ui_conversions` u
  JOIN cohort c ON c.advertiser_id = u.advertiser_id
  WHERE DATE(u.time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND CURRENT_DATE()
    AND DATE(u.time) BETWEEN c.win_start AND c.win_end
  GROUP BY 1, 2
),
win_rollup AS (
  SELECT
    a.advertiser_id,
    SUM(a.spend_ip)                 AS spend_win,
    SUM(a.impressions_ip)           AS impressions_win,
    COUNT(*)                        AS distinct_ips_win,
    SUM(IF(v.ip IS NOT NULL, 1, 0)) AS visiting_ips_win,
    SUM(IF(c2.ip IS NOT NULL, 1, 0)) AS converting_ips_win
  FROM ad_ip a
  LEFT JOIN visiting   v  USING (advertiser_id, ip)
  LEFT JOIN converting c2 USING (advertiser_id, ip)
  GROUP BY 1
  HAVING SUM(a.impressions_ip) > 0
),
monthly AS (
  SELECT
    s.advertiser_id,
    DATE_TRUNC(s.day, MONTH) AS month_start,
    SUM(COALESCE(s.media_spend,0)+COALESCE(s.data_spend,0)+COALESCE(s.platform_spend,0)) AS month_spend
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day` s
  JOIN cohort c ON c.advertiser_id = s.advertiser_id
  WHERE s.day BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY) AND CURRENT_DATE()
    AND s.day <= c.last_active_day
  GROUP BY 1, 2
  HAVING SUM(COALESCE(s.media_spend,0)+COALESCE(s.data_spend,0)+COALESCE(s.platform_spend,0)) > 1000
),
month_stats AS (
  SELECT
    advertiser_id,
    APPROX_QUANTILES(month_spend, 100)[OFFSET(50)] AS typical_active_month_spend,
    MAX(month_spend)                               AS max_month_spend,
    COUNT(*)                                       AS active_months_count
  FROM monthly
  GROUP BY 1
)
SELECT
  r.advertiser_id,
  adv.company_name                                        AS advertiser_name,
  c.is_delivering,
  c.last_active_day,
  DATE_DIFF(CURRENT_DATE(), c.last_active_day, DAY)       AS days_since_active,
  r.spend_win                                             AS spend_30d,
  r.impressions_win                                       AS impressions_30d,
  r.distinct_ips_win                                      AS distinct_ips_30d,
  r.visiting_ips_win                                      AS visiting_ips_30d,
  r.converting_ips_win                                    AS converting_ips_30d,
  SAFE_DIVIDE(r.spend_win, r.impressions_win) * 1000      AS cpm,
  SAFE_DIVIDE(r.impressions_win, r.distinct_ips_win)      AS imps_per_ip,
  SAFE_DIVIDE(r.visiting_ips_win,   r.distinct_ips_win)   AS p_visit,
  SAFE_DIVIDE(r.converting_ips_win, r.distinct_ips_win)   AS p_cvr,
  COALESCE(ms.typical_active_month_spend, r.spend_win)    AS typical_active_month_spend,
  COALESCE(ms.max_month_spend,            r.spend_win)    AS max_month_spend,
  COALESCE(ms.active_months_count,        1)              AS active_months_count,
  CURRENT_DATE()                                          AS data_pull_date
FROM win_rollup r
JOIN cohort c USING (advertiser_id)
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` adv USING (advertiser_id)
LEFT JOIN month_stats ms USING (advertiser_id)
WHERE adv.company_name IS NOT NULL
  AND COALESCE(adv.deleted, FALSE) = FALSE
  AND COALESCE(adv.is_test, FALSE) = FALSE
ORDER BY r.spend_win DESC
