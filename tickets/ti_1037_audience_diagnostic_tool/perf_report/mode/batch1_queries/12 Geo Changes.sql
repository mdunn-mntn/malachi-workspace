-- Dynamic param defaults (Mode date params are static-only, so sentinels map in SQL):
--   Period_Start = 1900-01-01 (the default) -> Jan 1 of the CURRENT year; any other date honored.
--   Period_End is CLAMPED to the first day of the current month (exclusive end ->
--   data through the last FULL month); the far-future default (2099-01-01) relies on this.
-- Module 12 -- GEO targeting change history per prospecting campaign group.
-- Geo lives INSIDE the audience expression JSON under "geos" -> location_ids:
--   location_id 237 = United States (national, type 2); DMA slice = type-4 ids
--   (461-672), 210 US DMAs total. Summary per version: national vs "N/210 DMAs".
-- One row per moment the geo signature changed (type-2 targeted archive versions,
-- capped at Period_End) so the render can forward-fill P1 state -> changes -> final,
-- exactly like the audience change-log. Spend = unified whole-group window basis.
WITH sel AS (
  -- FILTERS (Nick): campaign multiselect ('ALL' keeps everything) + minimum share of
  -- window spend (whole-group basis; total computed BEFORE selection so shares are
  -- of the advertiser's full window spend, not of the kept subset)
  SELECT campaign_group_id FROM (
    SELECT c.campaign_group_id,
           SUM(s.media_spend + s.data_spend + s.platform_spend) AS gs,
           SUM(SUM(s.media_spend + s.data_spend + s.platform_spend)) OVER () AS ts
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
    WHERE s.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE AND c.objective_id != 4
      AND s.day >= IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10)))
      AND s.day <  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))
    GROUP BY 1
  )
  WHERE ('ALL' IN ({{ Campaign_Groups }}) OR CAST(campaign_group_id AS STRING) IN ({{ Campaign_Groups }}))
    AND (ts <= 0 OR gs / ts >= LEAST(GREATEST(IFNULL(SAFE_CAST('{{ Min_Spend_Pct }}' AS FLOAT64), 0), 0), 100) / 100)
),
camp AS (
  SELECT campaign_id, campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id IN (1, 5, 6)
    AND ('ALL' IN ({{ Stages }}) OR CAST(funnel_level AS STRING) IN ({{ Stages }}))
    AND campaign_group_id IN (SELECT campaign_group_id FROM sel)
    AND campaign_id IN (
      SELECT DISTINCT campaign_id
      FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
      WHERE advertiser_id = {{ Advertiser_ID }}
        AND day >= IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10)))
        AND day <  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))
        AND impressions > 0
    )
),
grp_name AS (
  SELECT campaign_group_id, name AS group_name
  FROM `dw-main-bronze.integrationprod.campaign_groups`
),
-- WHOLE-GROUP window spend (all funnel stages, retargeting excluded) — unified % basis
grp_spend AS (
  SELECT c2.campaign_group_id, SUM(s.media_spend + s.data_spend + s.platform_spend) AS grp_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c2 ON c2.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{ Advertiser_ID }}
    AND s.day >= IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10)))
    AND s.day <  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))
    AND c2.deleted = FALSE AND c2.objective_id != 4
  GROUP BY 1
),
v AS (
  SELECT
    a.campaign_id, a.create_time,
    (SELECT COUNT(DISTINCT CAST(TRIM(x) AS INT64))
     FROM UNNEST(REGEXP_EXTRACT_ALL(a.expression, r'"location_ids":\[([^\]]*)\]')) arr,
          UNNEST(SPLIT(arr, ',')) x
     WHERE TRIM(x) != '' AND SAFE_CAST(TRIM(x) AS INT64) BETWEEN 461 AND 672) AS n_dma,
    REGEXP_CONTAINS(a.expression, r'"location_ids":\[[^\]]*\b237\b') AS has_us,
    (SELECT COUNT(DISTINCT TRIM(x))
     FROM UNNEST(REGEXP_EXTRACT_ALL(a.expression, r'"location_ids":\[([^\]]*)\]')) arr,
          UNNEST(SPLIT(arr, ',')) x
     WHERE TRIM(x) != '') AS n_ids
  FROM `dw-main-silver.archives.audience_segment_archives` a
  JOIN camp c USING (campaign_id)
  WHERE a.expression_type_id = 2 AND a.is_targeted = TRUE
    AND a.create_time < TIMESTAMP(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)))
),
sig AS (
  SELECT v.*,
    CONCAT(CAST(n_dma AS STRING), '|', CAST(has_us AS STRING), '|', CAST(n_ids AS STRING)) AS geo_sig
  FROM v
),
chg AS (
  SELECT sig.*,
    LAG(geo_sig) OVER (PARTITION BY campaign_id ORDER BY create_time) AS prev_sig
  FROM sig
)
SELECT
  c.campaign_group_id                                   AS geo_group_id,
  g.group_name,
  DATE(chg.create_time)                                 AS geo_changed_on,
  chg.n_dma,
  chg.has_us,
  chg.n_ids,
  COALESCE(gs.grp_spend, 0)                             AS geo_grp_spend,
  IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10))) AS p1_start,
  IF(DATE(LEFT('{{ P1_End }}', 10)) = DATE '1900-01-01', DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 YEAR), LEAST(DATE(LEFT('{{ P1_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))) AS p1_end,
  IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10)))                            AS p2_start,
  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))                              AS p2_end,
  IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10))) AS win_start,
  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))                              AS win_end
FROM chg
JOIN camp c USING (campaign_id)
LEFT JOIN grp_name g USING (campaign_group_id)
LEFT JOIN grp_spend gs USING (campaign_group_id)
WHERE chg.prev_sig IS NULL OR chg.geo_sig != chg.prev_sig
ORDER BY c.campaign_group_id, chg.create_time
