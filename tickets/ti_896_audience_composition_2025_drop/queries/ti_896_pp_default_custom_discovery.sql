-- TI-896 Track B discovery — find the default-vs-custom boundary for PP audiences.
-- Runs three heuristics on the SAME set of PP-detecting audiences:
--   (1) user_id histogram on archives_audiences_archives — look for MNTN system accounts
--   (2) audience name pattern — canonical "Peak Performance" vs custom-named variants
--   (3) expression structural test — does expression ONLY carry DS13+DS19+RTC, or layered additions
--
-- Output: one row per audience_id with the three heuristic signals side by side,
-- so we can manually inspect alignment and pick the cleanest classifier.

WITH
pp_audiences AS (
  -- Audiences (template level) where the audiences_archives expression matches PP pattern.
  -- audiences_archives uses the {"interest":{"include":[...]}} schema, different from segment_archives.
  SELECT DISTINCT
    aa.audience_id,
    aa.advertiser_id,
    aa.name,
    aa.user_id,
    aa.is_test,
    aa.expression,
    -- latest version of this audience
    ROW_NUMBER() OVER (PARTITION BY aa.audience_id ORDER BY aa.update_time DESC) AS rn
  FROM `dw-main-bronze.integrationprod.archives_audiences_archives` aa
  WHERE aa.expression_type_id = 2
    AND aa.is_test = FALSE
    AND REGEXP_CONTAINS(aa.expression, r'"data_source_id"\s*:\s*13\b')
    AND REGEXP_CONTAINS(aa.expression, r'"data_source_id"\s*:\s*19\b')
    AND aa.update_time >= TIMESTAMP('2025-09-01')
),

-- Count DS ids present in each audience's expression (structural test)
ds_ids_in_expr AS (
  SELECT
    audience_id,
    COUNT(*) AS n_distinct_ds_ids,
    STRING_AGG(m, ',' ORDER BY CAST(m AS INT64)) AS ds_ids_present_str  -- sorted comma-joined
  FROM (
    SELECT DISTINCT audience_id, m
    FROM pp_audiences,
    UNNEST(REGEXP_EXTRACT_ALL(expression, r'"data_source_id"\s*:\s*(\d+)[,}\s]')) AS m
    WHERE rn = 1
  )
  GROUP BY audience_id
)

SELECT
  pp.audience_id,
  pp.advertiser_id,
  pp.user_id,
  pp.name,

  -- Heuristic 1 — user_id: is this a small set (MNTN system) or many distinct (advertiser users)?
  COUNT(*) OVER (PARTITION BY pp.user_id) AS n_audiences_by_user,

  -- Heuristic 2 — name pattern: strict "Peak Performance" vs anything else
  CASE
    WHEN LOWER(TRIM(pp.name)) = 'peak performance'                             THEN 'name_default_strict'
    WHEN REGEXP_CONTAINS(LOWER(pp.name), r'^peak performance($|\s|\s*-)')      THEN 'name_default_prefix'
    WHEN REGEXP_CONTAINS(LOWER(pp.name), r'peak performance')                  THEN 'name_contains'
    ELSE                                                                            'name_other'
  END AS name_class,

  -- Heuristic 3 — expression structural test
  dse.n_distinct_ds_ids,
  dse.ds_ids_present_str,
  CASE
    -- "Pure" PP: only DS13 + DS19 in expression
    WHEN dse.ds_ids_present_str = '13,19'                                    THEN 'expr_pure_pp'
    WHEN dse.n_distinct_ds_ids <= 2                                          THEN 'expr_two_ds'
    WHEN dse.n_distinct_ds_ids <= 4                                          THEN 'expr_layered'
    ELSE                                                                          'expr_heavily_layered'
  END AS expr_class
FROM pp_audiences pp
JOIN ds_ids_in_expr dse USING (audience_id)
WHERE pp.rn = 1
-- Random sample (Fix M7) — was ORDER BY user_id, which clustered samples
-- by service-account groupings and biased the structural-split discovery.
ORDER BY RAND()
LIMIT 1000
