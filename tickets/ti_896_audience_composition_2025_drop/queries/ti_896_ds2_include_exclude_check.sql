-- TI-896: empirical check of DS2 usage in include vs exclude clauses.
-- Per Zach (Slack 2026-04-22): DS2 is an OPM segment, not Mountain Matched.
-- Per Ryan's example: DS2 appears in interest.exclude clauses (CRM-exclusion-style usage).
-- This query parses the JSON structure to count DS2 in interest.include vs interest.exclude
-- across cohort segment-archive expressions.

WITH
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE() AND impressions > 0
),

archive_recent AS (
  SELECT
    asa.campaign_id,
    c.advertiser_id,
    asa.expression,
    -- DS2 anywhere in expression
    REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*2\b') AS ds2_anywhere,
    -- DS2 in interest.include subtree
    REGEXP_CONTAINS(
      IFNULL(TO_JSON_STRING(JSON_QUERY(asa.expression, '$.interest.include')), ''),
      r'"data_source_id"\s*:\s*2\b'
    ) AS ds2_in_include,
    -- DS2 in interest.exclude subtree
    REGEXP_CONTAINS(
      IFNULL(TO_JSON_STRING(JSON_QUERY(asa.expression, '$.interest.exclude')), ''),
      r'"data_source_id"\s*:\s*2\b'
    ) AS ds2_in_exclude
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` asa
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  JOIN cohort USING (advertiser_id)
  WHERE asa.expression_type_id = 2 AND asa.is_targeted = TRUE
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND asa.update_time >= TIMESTAMP('2026-04-01')
)

SELECT
  COUNT(*) AS n_total_expressions,
  COUNTIF(ds2_anywhere) AS n_with_ds2_anywhere,
  COUNTIF(ds2_in_include) AS n_with_ds2_in_include,
  COUNTIF(ds2_in_exclude) AS n_with_ds2_in_exclude,
  COUNTIF(ds2_in_include AND NOT ds2_in_exclude) AS n_ds2_include_only,
  COUNTIF(ds2_in_exclude AND NOT ds2_in_include) AS n_ds2_exclude_only,
  COUNTIF(ds2_in_include AND ds2_in_exclude) AS n_ds2_both,
  COUNTIF(ds2_anywhere AND NOT ds2_in_include AND NOT ds2_in_exclude) AS n_ds2_neither_subtree,
  COUNT(DISTINCT IF(ds2_in_include AND NOT ds2_in_exclude, advertiser_id, NULL)) AS n_advertisers_ds2_include_only,
  COUNT(DISTINCT IF(ds2_in_exclude AND NOT ds2_in_include, advertiser_id, NULL)) AS n_advertisers_ds2_exclude_only
FROM archive_recent
