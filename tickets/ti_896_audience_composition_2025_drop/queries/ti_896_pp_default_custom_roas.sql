-- TI-896 Fix M8 / Section-4 #2 — Default-PP vs custom-PP ROAS deltas.
-- Intersects Track B template classification (default vs custom PP audience templates)
-- with Track C window methodology (Aug-Sep 2025 baseline -> Dec 2025 post).
--
-- For each new_adopter advertiser (PP delivery share <1% baseline AND >=5% post),
-- classify their dominant PP usage by which TEMPLATE class they ran the most VVs through:
--   - default_only: >=80% of their PP-VVs come from pure-DS13+DS19 templates
--   - custom_only: >=80% of PP-VVs from layered (custom) templates
--   - mixed: between
--
-- Then report median delta-conv-rate, delta-ROAS, delta-AOV for each class,
-- and report n_total / n_with_valid_roas per class so the medians are honest.

WITH
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE() AND impressions > 0
),

-- Last delivery day per campaign for LEAD-cap (Fix M10)
camp_last_active AS (
  SELECT campaign_id, MAX(day) AS last_active_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-06-01') AND CURRENT_DATE()
    AND impressions > 0
  GROUP BY campaign_id
),

-- Template-level PP classification (same logic as Track B weekly query)
pp_template_versions AS (
  SELECT
    aa.audience_id,
    ROW_NUMBER() OVER (PARTITION BY aa.audience_id ORDER BY aa.update_time DESC) AS rn,
    aa.expression
  FROM `dw-main-bronze.integrationprod.archives_audiences_archives` aa
  WHERE aa.expression_type_id = 2 AND aa.is_test = FALSE
    AND REGEXP_CONTAINS(aa.expression, r'"data_source_id"\s*:\s*13\b')
    AND REGEXP_CONTAINS(aa.expression, r'"data_source_id"\s*:\s*19\b')
),
pp_template_latest AS (SELECT * FROM pp_template_versions WHERE rn = 1),
pp_template_class AS (
  SELECT
    audience_id,
    CASE WHEN ds_set = '13,19' THEN 'default' ELSE 'custom' END AS template_class
  FROM (
    SELECT
      audience_id,
      STRING_AGG(m, ',' ORDER BY CAST(m AS INT64)) AS ds_set
    FROM (
      SELECT DISTINCT audience_id, m
      FROM pp_template_latest,
      UNNEST(REGEXP_EXTRACT_ALL(expression, r'"data_source_id"\s*:\s*(\d+)[,}\s]')) AS m
    )
    GROUP BY audience_id
  )
),

-- Segment-level archive rows (per-campaign), with effective windows + PP detector
archive_rows AS (
  SELECT
    asa.campaign_id,
    asa.audience_id,
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

-- Per (campaign, day) is the active version PP, and what template class?
camp_day_pp AS (
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
    LOGICAL_OR(ar.is_pp_expr) AS is_pp_day,
    -- Pull template_class for the PP segments; if multiple PP segments active same day,
    -- pick the one whose audience_id classifies as 'default' if any is default,
    -- else 'custom', else 'unclassified'.
    MAX(CASE WHEN ar.is_pp_expr AND ptc.template_class = 'default' THEN 'default'
             WHEN ar.is_pp_expr AND ptc.template_class = 'custom' THEN 'custom'
             WHEN ar.is_pp_expr THEN 'unclassified'
             ELSE NULL END) AS pp_template_class_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN cohort USING (advertiser_id)
  LEFT JOIN archive_rows ar
    ON ar.campaign_id = s.campaign_id
    AND TIMESTAMP(s.day) >= ar.update_time
    AND TIMESTAMP(s.day) < ar.next_update_time
  LEFT JOIN pp_template_class ptc ON ptc.audience_id = ar.audience_id
  WHERE s.day BETWEEN DATE('2025-08-01') AND DATE('2025-12-31')
    AND s.impressions > 0
  GROUP BY s.advertiser_id, s.campaign_id, s.day,
           s.view_viewed, s.click_conversions, s.view_conversions,
           s.click_order_value, s.view_order_value, s.media_cost
),

-- Per advertiser per window: VV-weighted template_class breakdown of PP days
per_window AS (
  SELECT
    advertiser_id,
    CASE
      WHEN day BETWEEN DATE('2025-08-01') AND DATE('2025-09-28') THEN 'baseline'
      WHEN day BETWEEN DATE('2025-12-01') AND DATE('2025-12-31') THEN 'post'
      ELSE NULL END AS win_label,
    SUM(view_viewed)                          AS vvs,
    SUM(click_conversions + view_conversions) AS convs,
    SUM(click_order_value + view_order_value) AS order_value,
    SUM(media_cost)                           AS spend,
    SAFE_DIVIDE(SUM(IF(is_pp_day, view_viewed, 0)), SUM(view_viewed)) AS pp_delivery_share,
    SUM(IF(pp_template_class_day = 'default', view_viewed, 0))      AS pp_vv_default,
    SUM(IF(pp_template_class_day = 'custom',  view_viewed, 0))      AS pp_vv_custom,
    SUM(IF(pp_template_class_day = 'unclassified', view_viewed, 0)) AS pp_vv_unclass
  FROM camp_day_pp
  WHERE (day BETWEEN DATE('2025-08-01') AND DATE('2025-09-28'))
     OR (day BETWEEN DATE('2025-12-01') AND DATE('2025-12-31'))
  GROUP BY advertiser_id, win_label
),

pivoted AS (
  SELECT
    advertiser_id,
    MAX(IF(win_label='baseline', vvs, NULL))         AS vvs_base,
    MAX(IF(win_label='post', vvs, NULL))             AS vvs_post,
    MAX(IF(win_label='baseline', convs, NULL))       AS convs_base,
    MAX(IF(win_label='post', convs, NULL))           AS convs_post,
    MAX(IF(win_label='baseline', order_value, NULL)) AS ov_base,
    MAX(IF(win_label='post', order_value, NULL))     AS ov_post,
    MAX(IF(win_label='baseline', spend, NULL))       AS spend_base,
    MAX(IF(win_label='post', spend, NULL))           AS spend_post,
    MAX(IF(win_label='baseline', pp_delivery_share, NULL)) AS pp_share_base,
    MAX(IF(win_label='post', pp_delivery_share, NULL))     AS pp_share_post,
    -- Post-window template-class shares (for adopter classification)
    MAX(IF(win_label='post', pp_vv_default, 0))  AS pp_vv_default_post,
    MAX(IF(win_label='post', pp_vv_custom, 0))   AS pp_vv_custom_post,
    MAX(IF(win_label='post', pp_vv_unclass, 0))  AS pp_vv_unclass_post
  FROM per_window
  WHERE win_label IS NOT NULL
  GROUP BY advertiser_id
),

scored AS (
  SELECT
    advertiser_id,
    vvs_base, vvs_post, convs_base, convs_post, spend_base, spend_post, ov_base, ov_post,
    pp_share_base, pp_share_post,
    SAFE_DIVIDE(convs_base, vvs_base) AS conv_rate_base,
    SAFE_DIVIDE(convs_post, vvs_post) AS conv_rate_post,
    SAFE_DIVIDE(SAFE_DIVIDE(convs_post, vvs_post),
                SAFE_DIVIDE(convs_base, vvs_base)) - 1 AS delta_conv_rate_rel,
    SAFE_DIVIDE(ov_base, spend_base) AS roas_base,
    SAFE_DIVIDE(ov_post, spend_post) AS roas_post,
    SAFE_DIVIDE(SAFE_DIVIDE(ov_post, spend_post),
                SAFE_DIVIDE(ov_base, spend_base)) - 1 AS delta_roas_rel,
    SAFE_DIVIDE(ov_base, convs_base) AS aov_base,
    SAFE_DIVIDE(ov_post, convs_post) AS aov_post,
    SAFE_DIVIDE(SAFE_DIVIDE(ov_post, convs_post),
                SAFE_DIVIDE(ov_base, convs_base)) - 1 AS delta_aov_rel,
    -- Template-class label: dominant template (>=80% of post-window PP VVs)
    pp_vv_default_post, pp_vv_custom_post, pp_vv_unclass_post,
    SAFE_DIVIDE(pp_vv_default_post,
                pp_vv_default_post + pp_vv_custom_post + pp_vv_unclass_post) AS share_default_post,
    SAFE_DIVIDE(pp_vv_custom_post,
                pp_vv_default_post + pp_vv_custom_post + pp_vv_unclass_post) AS share_custom_post,
    -- Adopter flag (same as Track C)
    (COALESCE(pp_share_base, 0) < 0.01 AND COALESCE(pp_share_post, 0) >= 0.05) AS is_pp_new_adopter,
    (COALESCE(pp_share_post, 0) < 0.05) AS is_non_adopter
  FROM pivoted
  WHERE vvs_base >= 1000 AND vvs_post >= 1000
)

SELECT
  *,
  CASE
    WHEN NOT is_pp_new_adopter THEN NULL
    WHEN share_default_post >= 0.80 THEN 'default_dominant'
    WHEN share_custom_post  >= 0.80 THEN 'custom_dominant'
    ELSE 'mixed'
  END AS pp_template_dominant_post
FROM scored
ORDER BY is_pp_new_adopter DESC, advertiser_id
