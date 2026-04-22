-- TI-896 Track A — spend-weighted composition time series
-- Answers: "how much MNTN spend is actually flowing through Peak Performance
-- (and each other bucket)?" Complements the presence-based view by killing the
-- "attached-but-not-delivered" noise.
--
-- Method:
--   1) Same cohort + archive-window reconstruction as ti_896_composition_by_week.sql.
--   2) For each (campaign_id, archive effective window), flag the category set
--      (MM/3P/CRM/PP/Keywords) carried by that expression version.
--   3) Join to sum_by_campaign_by_day on (campaign_id, day) where day falls in
--      the effective window.
--   4) Weekly rollup: sum(media_cost WHERE has_X) / sum(media_cost) per bucket.
--
-- Detectors are identical to the presence-based query — including the strict
-- PP detector (score_type=rtc + DS13 + DS19).

WITH
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE() AND impressions > 0
),

-- Last delivery day per campaign — caps LEAD windows for paused campaigns (Fix M10).
camp_last_active AS (
  SELECT campaign_id, MAX(day) AS last_active_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2024-11-01') AND CURRENT_DATE()
    AND impressions > 0
  GROUP BY campaign_id
),

ds_class AS (
  SELECT
    data_source_id, name,
    CASE
      WHEN data_source_id = 19                        THEN 'Keywords'
      WHEN data_source_id = 35                        THEN '3P'
      WHEN data_source_id = 4                         THEN 'CRM'
      WHEN data_source_id = 2                         THEN 'MM'
      WHEN name LIKE '% - First Party Audience'       THEN 'MM'
      ELSE 'Other'
    END AS category
  FROM `dw-main-bronze.integrationprod.data_sources`
),

archive_rows AS (
  SELECT
    asa.campaign_id,
    c.advertiser_id,
    asa.version,
    asa.update_time,
    LEAST(
      COALESCE(
        LEAD(asa.update_time) OVER (PARTITION BY asa.campaign_id ORDER BY asa.update_time, asa.version),
        CURRENT_TIMESTAMP()
      ),
      COALESCE(TIMESTAMP_ADD(TIMESTAMP(la.last_active_day), INTERVAL 1 DAY),
               CURRENT_TIMESTAMP())
    ) AS next_update_time,
    asa.expression,
    REGEXP_CONTAINS(asa.expression, r'"score_type"\s*:\s*"rtc"')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*13\b')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*19\b') AS is_pp_expr
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` asa
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  JOIN cohort USING (advertiser_id)
  LEFT JOIN camp_last_active la USING (campaign_id)
  WHERE asa.expression_type_id = 2
    AND asa.is_targeted = TRUE
    AND c.deleted = FALSE
    AND c.is_test = FALSE
    AND asa.update_time >= TIMESTAMP('2024-11-01')
),

archive_ds AS (
  SELECT
    ar.campaign_id, ar.advertiser_id, ar.version,
    ar.update_time, ar.next_update_time, ar.is_pp_expr,
    CAST(m_ds_id AS INT64) AS data_source_id
  FROM archive_rows ar,
  UNNEST(REGEXP_EXTRACT_ALL(ar.expression, r'"data_source_id"\s*:\s*(\d+)[,}\s]')) AS m_ds_id
),

-- Per (campaign, version) — set of categories present + is_pp_expr flag
camp_version_cats AS (
  SELECT
    ar.campaign_id, ar.advertiser_id, ar.version,
    ar.update_time, ar.next_update_time,
    ANY_VALUE(ar.is_pp_expr) AS is_pp_expr,
    LOGICAL_OR(dsc.category = 'MM')       AS has_mm,
    LOGICAL_OR(dsc.category = '3P')       AS has_3p,
    LOGICAL_OR(dsc.category = 'CRM')      AS has_crm,
    LOGICAL_OR(dsc.category = 'Keywords') AS has_keywords
  FROM archive_ds ar
  JOIN ds_class dsc USING (data_source_id)
  GROUP BY ar.campaign_id, ar.advertiser_id, ar.version, ar.update_time, ar.next_update_time
),

-- Join spend to archive windows: day must fall inside [update_time, next_update_time).
-- This assigns every delivered impression-day to the expression-version active on that day.
camp_day_spend AS (
  SELECT
    s.day,
    s.campaign_id,
    s.advertiser_id,
    s.media_cost,
    cvc.is_pp_expr,
    cvc.has_mm,
    cvc.has_3p,
    cvc.has_crm,
    cvc.has_keywords
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN cohort USING (advertiser_id)
  JOIN camp_version_cats cvc
    ON cvc.campaign_id = s.campaign_id
    AND TIMESTAMP(s.day) >= cvc.update_time
    AND TIMESTAMP(s.day) <  cvc.next_update_time
  WHERE s.day BETWEEN DATE('2024-11-01') AND CURRENT_DATE()
    AND s.impressions > 0
    AND s.media_cost > 0
),

-- Weekly rollup
weekly_spend AS (
  SELECT
    DATE_TRUNC(day, WEEK(MONDAY)) AS week_start,
    SUM(media_cost) AS total_spend,
    SUM(IF(has_mm,       media_cost, 0)) AS spend_mm,
    SUM(IF(has_3p,       media_cost, 0)) AS spend_3p,
    SUM(IF(has_crm,      media_cost, 0)) AS spend_crm,
    SUM(IF(is_pp_expr,   media_cost, 0)) AS spend_pp,
    SUM(IF(has_keywords, media_cost, 0)) AS spend_keywords,
    -- Exclude WGU sensitivity variant
    SUM(IF(advertiser_id != 31357, media_cost, 0)) AS total_spend_ex_wgu,
    SUM(IF(advertiser_id != 31357 AND is_pp_expr, media_cost, 0)) AS spend_pp_ex_wgu
  FROM camp_day_spend
  GROUP BY week_start
)

SELECT
  week_start,
  total_spend,
  spend_mm       / NULLIF(total_spend, 0) AS pct_spend_mm,
  spend_3p       / NULLIF(total_spend, 0) AS pct_spend_3p,
  spend_crm      / NULLIF(total_spend, 0) AS pct_spend_crm,
  spend_pp       / NULLIF(total_spend, 0) AS pct_spend_pp,
  spend_keywords / NULLIF(total_spend, 0) AS pct_spend_keywords,
  -- WGU sensitivity
  spend_pp_ex_wgu / NULLIF(total_spend_ex_wgu, 0) AS pct_spend_pp_ex_wgu
FROM weekly_spend
WHERE week_start >= DATE('2024-11-04')
ORDER BY week_start
