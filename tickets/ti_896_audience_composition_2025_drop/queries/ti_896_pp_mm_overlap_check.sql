-- TI-896: Empirical check — does the PP detector overlap with the MM detector?
-- Returns one row per PP-detecting expression with the full DS-id set and
-- DS2-presence flag, plus MM-name detection. Run, then inspect distribution.

WITH
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE() AND impressions > 0
),

-- Sample 100 PP segment expressions
pp_segments AS (
  SELECT
    asa.campaign_id,
    asa.audience_id,
    asa.update_time,
    REGEXP_EXTRACT_ALL(asa.expression, r'"data_source_id"\s*:\s*(\d+)[,}\s]') AS ds_ids_raw,
    asa.expression
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` asa
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  JOIN cohort USING (advertiser_id)
  WHERE asa.expression_type_id = 2 AND asa.is_targeted = TRUE
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND asa.update_time >= TIMESTAMP('2025-10-15')
    AND REGEXP_CONTAINS(asa.expression, r'"score_type"\s*:\s*"rtc"')
    AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*13\b')
    AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*19\b')
  ORDER BY RAND()
  LIMIT 100
)

SELECT
  audience_id,
  campaign_id,
  (SELECT STRING_AGG(m, ',' ORDER BY CAST(m AS INT64))
   FROM (SELECT DISTINCT m FROM UNNEST(ds_ids_raw) m)) AS distinct_ds_ids,
  EXISTS(SELECT 1 FROM UNNEST(ds_ids_raw) m WHERE CAST(m AS INT64) = 2) AS contains_ds2,
  EXISTS(SELECT 1 FROM UNNEST(ds_ids_raw) m WHERE CAST(m AS INT64) BETWEEN 1000 AND 99999) AS contains_per_advertiser_ds_id,
  REGEXP_CONTAINS(expression, r'(?i)first[\s_]?party') AS mentions_first_party,
  REGEXP_CONTAINS(expression, r'(?i)mountain.?matched|mntn.?match') AS mentions_mntn_matched
FROM pp_segments
ORDER BY contains_ds2 DESC, distinct_ds_ids
