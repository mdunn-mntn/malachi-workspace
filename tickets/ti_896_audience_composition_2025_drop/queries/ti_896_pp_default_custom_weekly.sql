-- TI-896 Track B — weekly default-vs-custom PP adoption rollup.
--
-- Classifier (best-effort per V9 discovery, applied at AUDIENCE-TEMPLATE level):
--   default_pp  = audience template (archives_audiences_archives) whose expression carries
--                 ONLY data_source_ids {13, 19} — the minimal PP pattern.
--   custom_pp   = audience template whose expression has DS13+DS19 AND other DS ids layered
--                 (exclusions, overlays, CRM lists, extra keywords, etc.).
--
-- Discovery (1000-row sample from Sep 2025+): 25% pure / 52% layered / 23% heavily-layered.
-- Name-pattern heuristic failed (only 7/1000 audiences named "Peak Performance"). user_id
-- showed one service account (122462) with 28% of audiences but no clean ≥80% boundary.
--
-- Template-level classification is the right grain because:
--   - audiences_archives uses the compact `{"interest":{"include":[...]}}` schema
--   - audience_segment_archives stores the TRANSLATED form which always adds auxiliary DS ids
--     (DS14 global flag, holdout structure, etc.), so the "pure DS13+DS19" signal vanishes
--     if classified at segment level.
--
-- After classification, propagate template_class to segments via audience_id, then rollup.
-- Per-advertiser-week classification:
--   "default-only" if every PP audience they run is pure template
--   "custom-only"  if every PP audience is layered template
--   "both"         if they mix
-- Reports weekly cohort %s and share-of-adopters for each class.

WITH
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE() AND impressions > 0
),

-- 1. Template-level classification: latest version of each audience in audiences_archives
--    that has DS13+DS19 (i.e., is a PP template).
pp_template_versions AS (
  SELECT
    aa.audience_id,
    aa.advertiser_id,
    aa.name,
    aa.user_id,
    aa.update_time,
    aa.expression,
    ROW_NUMBER() OVER (PARTITION BY aa.audience_id ORDER BY aa.update_time DESC) AS rn
  FROM `dw-main-bronze.integrationprod.archives_audiences_archives` aa
  WHERE aa.expression_type_id = 2
    AND aa.is_test = FALSE
    AND REGEXP_CONTAINS(aa.expression, r'"data_source_id"\s*:\s*13\b')
    AND REGEXP_CONTAINS(aa.expression, r'"data_source_id"\s*:\s*19\b')
),

pp_template_latest AS (
  SELECT * FROM pp_template_versions WHERE rn = 1
),

-- 2. DS-id set per template → pure vs layered classification
pp_template_ds AS (
  SELECT
    audience_id, advertiser_id, name, user_id,
    COUNT(*) AS n_distinct_ds_ids,
    STRING_AGG(m, ',' ORDER BY CAST(m AS INT64)) AS ds_set
  FROM (
    SELECT DISTINCT audience_id, advertiser_id, name, user_id, m
    FROM pp_template_latest,
    UNNEST(REGEXP_EXTRACT_ALL(expression, r'"data_source_id"\s*:\s*(\d+)[,}\s]')) AS m
  )
  GROUP BY audience_id, advertiser_id, name, user_id
),

pp_template_class AS (
  SELECT
    audience_id, advertiser_id,
    CASE
      WHEN ds_set = '13,19' THEN 'default_pp'
      ELSE                       'custom_pp'
    END AS template_class
  FROM pp_template_ds
),

-- 3. Join to archive_rows (segment archives), filter to PP-detecting segments only.
archive_rows AS (
  SELECT
    asa.campaign_id,
    c.advertiser_id,
    asa.audience_id,
    asa.version,
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
    AND asa.update_time >= TIMESTAMP('2024-11-01')
),

pp_segments AS (
  SELECT
    ar.advertiser_id, ar.campaign_id, ar.audience_id,
    ar.update_time, ar.next_update_time,
    COALESCE(ptc.template_class, 'unclassified_pp') AS template_class
  FROM archive_rows ar
  LEFT JOIN pp_template_class ptc USING (audience_id)
  WHERE ar.is_pp_expr = TRUE
),

-- 4. Explode segment windows to weeks
weekly AS (
  SELECT
    ps.advertiser_id, ps.campaign_id, ps.template_class, week_start
  FROM pp_segments ps,
  UNNEST(GENERATE_DATE_ARRAY(
    GREATEST(DATE_TRUNC(DATE(update_time), WEEK(MONDAY)), DATE('2024-11-04')),
    LEAST(DATE_TRUNC(DATE(next_update_time), WEEK(MONDAY)), DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))),
    INTERVAL 1 WEEK
  )) AS week_start
),

-- 5. Advertiser-week classification
adv_week_class AS (
  SELECT
    week_start, advertiser_id,
    LOGICAL_OR(template_class = 'default_pp') AS has_default,
    LOGICAL_OR(template_class = 'custom_pp')  AS has_custom,
    LOGICAL_OR(template_class = 'unclassified_pp') AS has_unclassified
  FROM weekly
  GROUP BY week_start, advertiser_id
),

adv_week_bucket AS (
  SELECT
    week_start, advertiser_id,
    CASE
      WHEN has_default AND has_custom THEN 'both'
      WHEN has_default                THEN 'default_only'
      WHEN has_custom                 THEN 'custom_only'
      WHEN has_unclassified           THEN 'unclassified'
      ELSE                                 NULL
    END AS pp_adopter_class
  FROM adv_week_class
),

cohort_size_weekly AS (
  SELECT
    DATE_TRUNC(day, WEEK(MONDAY)) AS week_start,
    COUNT(DISTINCT advertiser_id) AS n_cohort
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  JOIN cohort USING (advertiser_id)
  WHERE day BETWEEN DATE('2024-11-04') AND CURRENT_DATE() AND impressions > 0
  GROUP BY week_start
)

SELECT
  c.week_start,
  c.n_cohort,
  COUNTIF(awc.pp_adopter_class IS NOT NULL)            AS n_pp_adopters,
  COUNTIF(awc.pp_adopter_class = 'default_only')       AS n_default_only,
  COUNTIF(awc.pp_adopter_class = 'custom_only')        AS n_custom_only,
  COUNTIF(awc.pp_adopter_class = 'both')               AS n_both,
  COUNTIF(awc.pp_adopter_class = 'unclassified')       AS n_unclassified,

  COUNTIF(awc.pp_adopter_class IS NOT NULL)   / NULLIF(c.n_cohort, 0) AS pct_pp_adopters,
  COUNTIF(awc.pp_adopter_class = 'default_only') / NULLIF(c.n_cohort, 0) AS pct_default_only,
  COUNTIF(awc.pp_adopter_class = 'custom_only')  / NULLIF(c.n_cohort, 0) AS pct_custom_only,
  COUNTIF(awc.pp_adopter_class = 'both')         / NULLIF(c.n_cohort, 0) AS pct_both,

  SAFE_DIVIDE(COUNTIF(awc.pp_adopter_class = 'default_only'),
              COUNTIF(awc.pp_adopter_class IS NOT NULL)) AS share_default_of_adopters,
  SAFE_DIVIDE(COUNTIF(awc.pp_adopter_class = 'custom_only'),
              COUNTIF(awc.pp_adopter_class IS NOT NULL)) AS share_custom_of_adopters,
  SAFE_DIVIDE(COUNTIF(awc.pp_adopter_class = 'both'),
              COUNTIF(awc.pp_adopter_class IS NOT NULL)) AS share_both_of_adopters
FROM cohort_size_weekly c
LEFT JOIN adv_week_bucket awc USING (week_start)
GROUP BY c.week_start, c.n_cohort
ORDER BY c.week_start
