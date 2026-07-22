WITH seg AS (
  SELECT s.campaign_id,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r'"data_source_id":13[^0-9]')) AS ds13,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r'"data_source_id":19[^0-9]')) AS ds19,
    LOGICAL_OR(REGEXP_CONTAINS(s.expression, r'"data_source_id":46[^0-9]')) AS ds46
  FROM `dw-main-silver.audience.audience_segments` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
    AND c.deleted = FALSE AND c.is_test = FALSE AND c.funnel_level = 1 AND c.campaign_status_id = 3
  GROUP BY 1
),
cls AS (
  SELECT campaign_id,
    CASE WHEN ds13 AND NOT ds19 AND NOT ds46 THEN 'DS13_only'
         WHEN ds46 AND NOT ds19 AND NOT ds13 THEN 'DS46_only' END AS cls
  FROM seg
  WHERE (ds13 AND NOT ds19 AND NOT ds46) OR (ds46 AND NOT ds19 AND NOT ds13)
)
SELECT cls.cls,
  CASE WHEN l.household_score <= 0 THEN 'a.unscored'
       WHEN l.household_score BETWEEN 1 AND 3332 THEN 'b.MaxReach'
       WHEN l.household_score BETWEEN 3333 AND 6665 THEN 'c.MI'
       WHEN l.household_score BETWEEN 6666 AND 7999 THEN 'd.6666-7999(PPcont)'
       WHEN l.household_score = 8000 THEN 'e.8000(PP pin)'
       WHEN l.household_score BETWEEN 8001 AND 9999 THEN 'f.8001-9999(HIcont)'
       WHEN l.household_score = 10000 THEN 'g.10000(HI pin)' END AS band,
  COUNT(*) AS imps,
  COUNT(DISTINCT l.campaign_id) AS n_camps
FROM `dw-main-silver.logdata.cost_impression_log` l
JOIN cls USING (campaign_id)
WHERE l.time >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
  AND (l.model_params IS NULL OR l.model_params NOT LIKE '%realtime_conquest_score=10000%')
GROUP BY 1,2 ORDER BY 1,2
