-- Module 12 — DS35 (LiveRamp 3P) segment name decode for this advertiser's prospecting campaigns.
-- Extracts every DS35 category_ids array from the latest targeted expression of each prospecting
-- campaign (funnel_level=1, objective_id=1, deleted=FALSE), then joins tpa.categories for names.
-- The regex may over-capture a superset of ids; the DS35 join drops any non-DS35 id, so the
-- result is exactly the DS35 segments this advertiser targets. Chart uses this only for display
-- names (falls back to the raw id when absent), so a superset is harmless.
WITH prosp AS (
  SELECT DISTINCT c.campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND c.funnel_level = 1 AND c.objective_id = 1
),
seg AS (
  SELECT a.expression,
         ROW_NUMBER() OVER (PARTITION BY a.campaign_id ORDER BY a.update_time DESC) AS rn
  FROM `dw-main-silver.audience.audience_segments` a
  WHERE a.campaign_id IN (SELECT campaign_id FROM prosp)
    AND a.expression_type_id = 2 AND a.is_targeted = TRUE
),
arrays AS (
  SELECT arr
  FROM seg,
       UNNEST(REGEXP_EXTRACT_ALL(expression,
         r"(?s)\"data_source_id\":\s*35\s*,\s*\"category_ids\":\s*\[([\s0-9,]*?)\]")) AS arr
  WHERE rn = 1
),
cats AS (
  SELECT DISTINCT CAST(TRIM(cid) AS INT64) AS category_id
  FROM arrays, UNNEST(SPLIT(arr, ",")) AS cid
  WHERE TRIM(cid) != ""
)
SELECT t.data_source_category_id AS category_id, t.name
FROM cats
JOIN `dw-main-bronze.tpa.categories` t
  ON t.data_source_category_id = cats.category_id AND t.data_source_id = 35
ORDER BY category_id
