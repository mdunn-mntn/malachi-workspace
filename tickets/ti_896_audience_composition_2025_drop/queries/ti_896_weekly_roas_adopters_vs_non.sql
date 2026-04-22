-- TI-896 Fix Section-4 #3 — Weekly cohort-median ROAS time series.
-- Two-window comparison hides the *shape* of the trajectory. This query computes
-- weekly per-advertiser ROAS within new_adopter and non_adopter cohorts, then
-- takes the median per cohort per week across the Aug 2025 -> Apr 2026 window.
--
-- Cohorts defined identically to Track C (per-window-comparison query):
--   - new_adopter: PP delivery share <1% Aug-Sep AND >=5% Dec, >=1000 VVs each window
--   - non_adopter: PP delivery share <5% Dec, >=1000 VVs each window
--
-- Median of per-advertiser ROAS each week. Per-advertiser ROAS = order_value/media_cost
-- summed across all the advertiser's campaigns that week (only weeks the advertiser delivered).

WITH
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE() AND impressions > 0
),

camp_last_active AS (
  SELECT campaign_id, MAX(day) AS last_active_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-06-01') AND CURRENT_DATE()
    AND impressions > 0
  GROUP BY campaign_id
),

archive_rows AS (
  SELECT
    asa.campaign_id,
    asa.update_time,
    LEAST(
      COALESCE(LEAD(asa.update_time) OVER (PARTITION BY asa.campaign_id ORDER BY asa.update_time, asa.version),
               CURRENT_TIMESTAMP()),
      COALESCE(TIMESTAMP_ADD(TIMESTAMP(la.last_active_day), INTERVAL 1 DAY),
               CURRENT_TIMESTAMP())
    ) AS next_update_time,
    REGEXP_CONTAINS(asa.expression, r'"score_type"\s*:\s*"rtc"')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*13\b')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*19\b') AS is_pp_expr
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` asa
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  JOIN cohort USING (advertiser_id)
  LEFT JOIN camp_last_active la USING (campaign_id)
  WHERE asa.expression_type_id = 2 AND asa.is_targeted = TRUE
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND asa.update_time >= TIMESTAMP('2025-06-01')
),

-- Per (campaign, day) — was a PP segment active that day?
camp_day_pp AS (
  SELECT
    s.advertiser_id, s.campaign_id, s.day,
    s.view_viewed, s.click_conversions, s.view_conversions,
    s.click_order_value, s.view_order_value, s.media_cost,
    LOGICAL_OR(ar.is_pp_expr) AS is_pp_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN cohort USING (advertiser_id)
  LEFT JOIN archive_rows ar
    ON ar.campaign_id = s.campaign_id
    AND TIMESTAMP(s.day) >= ar.update_time
    AND TIMESTAMP(s.day) < ar.next_update_time
  WHERE s.day BETWEEN DATE('2025-08-01') AND DATE('2025-12-31')
    AND s.impressions > 0
  GROUP BY s.advertiser_id, s.campaign_id, s.day,
           s.view_viewed, s.click_conversions, s.view_conversions,
           s.click_order_value, s.view_order_value, s.media_cost
),

-- Two-window aggregation to assign cohort labels
per_window AS (
  SELECT
    advertiser_id,
    CASE WHEN day BETWEEN DATE('2025-08-01') AND DATE('2025-09-28') THEN 'baseline'
         WHEN day BETWEEN DATE('2025-12-01') AND DATE('2025-12-31') THEN 'post'
         ELSE NULL END AS win_label,
    SUM(view_viewed) AS vvs,
    SAFE_DIVIDE(SUM(IF(is_pp_day, view_viewed, 0)), SUM(view_viewed)) AS pp_share
  FROM camp_day_pp
  WHERE (day BETWEEN DATE('2025-08-01') AND DATE('2025-09-28'))
     OR (day BETWEEN DATE('2025-12-01') AND DATE('2025-12-31'))
  GROUP BY advertiser_id, win_label
),

cohort_label AS (
  SELECT
    advertiser_id,
    MAX(IF(win_label='baseline', vvs, 0)) AS vvs_base,
    MAX(IF(win_label='post', vvs, 0))     AS vvs_post,
    MAX(IF(win_label='baseline', pp_share, 0)) AS pp_base,
    MAX(IF(win_label='post', pp_share, 0))     AS pp_post
  FROM per_window
  GROUP BY advertiser_id
),

cohort_assigned AS (
  SELECT
    advertiser_id,
    CASE
      WHEN vvs_base >= 1000 AND vvs_post >= 1000
           AND pp_base < 0.01 AND pp_post >= 0.05 THEN 'new_adopter'
      WHEN vvs_base >= 1000 AND vvs_post >= 1000
           AND pp_post < 0.05 THEN 'non_adopter'
      ELSE NULL
    END AS cohort
  FROM cohort_label
),

-- Per-advertiser per-week ROAS over the full Aug 2025 -> Apr 2026 window
weekly_per_adv AS (
  SELECT
    DATE_TRUNC(s.day, WEEK(MONDAY)) AS week_start,
    s.advertiser_id,
    ca.cohort,
    SUM(s.click_order_value + s.view_order_value) AS order_value,
    SUM(s.media_cost) AS spend,
    SUM(s.view_viewed) AS vvs
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN cohort_assigned ca USING (advertiser_id)
  WHERE ca.cohort IS NOT NULL
    AND s.day BETWEEN DATE('2025-08-01') AND CURRENT_DATE()
    AND s.impressions > 0
  GROUP BY week_start, s.advertiser_id, ca.cohort
),

weekly_per_adv_roas AS (
  SELECT
    week_start, advertiser_id, cohort,
    SAFE_DIVIDE(order_value, spend) AS roas,
    spend, vvs
  FROM weekly_per_adv
  WHERE spend > 0
    AND vvs >= 100  -- per-week noise floor
)

SELECT
  week_start,
  cohort,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  COUNTIF(roas IS NOT NULL)     AS n_with_roas,
  APPROX_QUANTILES(roas, 100)[OFFSET(50)] AS median_roas,
  APPROX_QUANTILES(roas, 100)[OFFSET(25)] AS p25_roas,
  APPROX_QUANTILES(roas, 100)[OFFSET(75)] AS p75_roas,
  -- Also report spend-weighted ROAS for sanity check vs median
  SAFE_DIVIDE(SUM(roas * spend), SUM(spend)) AS spend_weighted_roas,
  SUM(spend) AS total_spend
FROM weekly_per_adv_roas
GROUP BY week_start, cohort
ORDER BY week_start, cohort
