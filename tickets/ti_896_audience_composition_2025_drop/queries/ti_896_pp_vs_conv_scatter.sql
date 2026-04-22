-- TI-896 Track C — per-advertiser scatter: Δ(PP adoption) vs Δ(conv rate) Aug-Sep → Dec 2025
--
-- Cohort: 2025-active advertisers that delivered in both the baseline (Aug 1 – Sep 28 2025)
-- and the post-launch (Dec 1 – Dec 31 2025) windows, with >=1,000 view_viewed in each window
-- (noise floor to avoid extreme rates on tiny delivery).
--
-- Conv rate = (click_conversions + view_conversions) / view_viewed  -- per advertiser per window
-- PP share  = share of advertiser's campaign-days in each window where the active segment
--             expression contains PP (score_type=rtc + DS13 + DS19)
--
-- Uses summarydata.sum_by_campaign_by_day (no TTL issues; covers back to 2024-01-01) so
-- we don't need clickpass_log / ui_conversions (both 90-day TTL, pre-Dec 2025 expired).

WITH
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE() AND impressions > 0
),

-- Archive-window flags: per (campaign, day) is this day covered by a PP segment version?
archive_rows AS (
  SELECT
    asa.campaign_id,
    asa.update_time,
    COALESCE(
      LEAD(asa.update_time) OVER (PARTITION BY asa.campaign_id ORDER BY asa.update_time, asa.version),
      CURRENT_TIMESTAMP()
    ) AS next_update_time,
    REGEXP_CONTAINS(asa.expression, r'"score_type"\s*:\s*"rtc"')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*13\b')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*19\b') AS is_pp_expr
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` asa
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  JOIN cohort USING (advertiser_id)
  WHERE asa.expression_type_id = 2 AND asa.is_targeted = TRUE
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND asa.update_time >= TIMESTAMP('2025-06-01')
),

-- Per (campaign, day): was any version active on that day a PP segment?
-- (LOGICAL_OR across overlapping versions, if any)
camp_day_is_pp AS (
  SELECT
    s.advertiser_id,
    s.campaign_id,
    s.day,
    s.view_viewed,
    s.click_conversions,
    s.view_conversions,
    s.click_order_value,
    s.view_order_value,
    s.media_cost,
    LOGICAL_OR(ar.is_pp_expr) AS is_pp_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN cohort USING (advertiser_id)
  LEFT JOIN archive_rows ar
    ON ar.campaign_id = s.campaign_id
    AND TIMESTAMP(s.day) >= ar.update_time
    AND TIMESTAMP(s.day) <  ar.next_update_time
  WHERE s.day BETWEEN DATE('2025-08-01') AND DATE('2025-12-31')
    AND s.impressions > 0
  GROUP BY s.advertiser_id, s.campaign_id, s.day,
           s.view_viewed, s.click_conversions, s.view_conversions,
           s.click_order_value, s.view_order_value, s.media_cost
),

-- Aggregate per advertiser per window
per_window AS (
  SELECT
    advertiser_id,
    CASE
      WHEN day BETWEEN DATE('2025-08-01') AND DATE('2025-09-28') THEN 'baseline'
      WHEN day BETWEEN DATE('2025-12-01') AND DATE('2025-12-31') THEN 'post'
      ELSE NULL END AS win_label,
    SUM(view_viewed)                                            AS vvs,
    SUM(click_conversions + view_conversions)                   AS convs,
    SUM(click_order_value + view_order_value)                   AS order_value,
    SUM(media_cost)                                             AS spend,
    -- PP-day share: proportion of VVs on campaign-days where PP was the active expression
    SAFE_DIVIDE(SUM(IF(is_pp_day, view_viewed, 0)), SUM(view_viewed)) AS pp_delivery_share
  FROM camp_day_is_pp
  WHERE (day BETWEEN DATE('2025-08-01') AND DATE('2025-09-28'))
     OR (day BETWEEN DATE('2025-12-01') AND DATE('2025-12-31'))
  GROUP BY advertiser_id, win_label
),

pivoted AS (
  SELECT
    advertiser_id,
    MAX(IF(win_label = 'baseline', vvs, NULL))                AS vvs_base,
    MAX(IF(win_label = 'post',     vvs, NULL))                AS vvs_post,
    MAX(IF(win_label = 'baseline', convs, NULL))              AS convs_base,
    MAX(IF(win_label = 'post',     convs, NULL))              AS convs_post,
    MAX(IF(win_label = 'baseline', order_value, NULL))        AS ov_base,
    MAX(IF(win_label = 'post',     order_value, NULL))        AS ov_post,
    MAX(IF(win_label = 'baseline', spend, NULL))              AS spend_base,
    MAX(IF(win_label = 'post',     spend, NULL))              AS spend_post,
    MAX(IF(win_label = 'baseline', pp_delivery_share, NULL))  AS pp_share_base,
    MAX(IF(win_label = 'post',     pp_delivery_share, NULL))  AS pp_share_post
  FROM per_window
  WHERE win_label IS NOT NULL
  GROUP BY advertiser_id
)

SELECT
  advertiser_id,
  vvs_base, vvs_post,
  convs_base, convs_post,
  spend_base, spend_post,
  ov_base, ov_post,

  -- Conversion rate (conversions / VVs)
  SAFE_DIVIDE(convs_base, vvs_base)   AS conv_rate_base,
  SAFE_DIVIDE(convs_post, vvs_post)   AS conv_rate_post,
  SAFE_DIVIDE(SAFE_DIVIDE(convs_post, vvs_post),
              SAFE_DIVIDE(convs_base, vvs_base)) - 1 AS delta_conv_rate_rel,

  -- ROAS (order value / media spend) — the war-room metric
  SAFE_DIVIDE(ov_base, spend_base)    AS roas_base,
  SAFE_DIVIDE(ov_post, spend_post)    AS roas_post,
  SAFE_DIVIDE(SAFE_DIVIDE(ov_post, spend_post),
              SAFE_DIVIDE(ov_base, spend_base)) - 1 AS delta_roas_rel,

  -- Order value per conversion (AOV) — tests "order amounts dropping" hypothesis
  SAFE_DIVIDE(ov_base, convs_base)    AS aov_base,
  SAFE_DIVIDE(ov_post, convs_post)    AS aov_post,
  SAFE_DIVIDE(SAFE_DIVIDE(ov_post, convs_post),
              SAFE_DIVIDE(ov_base, convs_base)) - 1 AS delta_aov_rel,

  pp_share_base, pp_share_post,
  (pp_share_post - pp_share_base)     AS delta_pp_share,

  -- Adopter label: was <1% in baseline AND >=5% in post
  (COALESCE(pp_share_base, 0) < 0.01 AND COALESCE(pp_share_post, 0) >= 0.05) AS is_pp_new_adopter,
  -- Already-using label: was >=5% in both windows
  (COALESCE(pp_share_base, 0) >= 0.05 AND COALESCE(pp_share_post, 0) >= 0.05) AS is_pp_continuing,
  -- Non-adopter: never got to 5%
  (COALESCE(pp_share_post, 0) < 0.05) AS is_non_adopter
FROM pivoted
WHERE vvs_base >= 1000 AND vvs_post >= 1000  -- noise floor
ORDER BY delta_pp_share DESC
