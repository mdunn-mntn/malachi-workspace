-- Dynamic param defaults (Mode date params are static-only, so sentinels map in SQL):
--   Period_Start = 1900-01-01 (the default) -> Jan 1 of the CURRENT year; any other date honored.
--   Period_End is CLAMPED to the first day of the current month (exclusive end ->
--   data through the last FULL month); the far-future default (2099-01-01) relies on this.
-- Module 07 / 07b -- Prospecting audience-expression CHANGE HISTORY (per campaign).
-- Every distinct audience config a prospecting campaign (obj=1, funnel=1) ran over the trend window,
-- from the type-2 archive, collapsed to the moments the DS set OR the audience_id changed.
-- DS ids regex-extracted from expression JSON. Ordered by create_time.
-- Also carries per-group prospecting spend over the window so the render can rank sections by spend.
WITH camp AS (
  SELECT campaign_id, campaign_group_id, name AS camp_name
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
    AND campaign_id IN (
      SELECT DISTINCT campaign_id
      FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
      WHERE advertiser_id = {{ Advertiser_ID }}
        AND day >= DATE_SUB(IF(DATE('{{ Period_Start }}') = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE('{{ Period_Start }}')), INTERVAL 1 YEAR)
        AND day <  LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH))
        AND impressions > 0
    )
),
grp_spend AS (
  -- WHOLE-GROUP window spend (all funnel stages, retargeting excluded) — the UNIFIED
  -- % basis shared by every module. Changelog events stay stage-1.
  SELECT c2.campaign_group_id, SUM(s.media_spend + s.data_spend + s.platform_spend) AS grp_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c2 ON c2.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{ Advertiser_ID }}
    AND s.day >= DATE_SUB(IF(DATE('{{ Period_Start }}') = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE('{{ Period_Start }}')), INTERVAL 1 YEAR)
    AND s.day <  LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH))
    AND c2.deleted = FALSE AND c2.objective_id != 4
  GROUP BY 1
),
v AS (
  SELECT
    a.campaign_id, a.audience_id, a.segment_id, a.create_time,
    ARRAY_TO_STRING(ARRAY(
      SELECT DISTINCT x FROM UNNEST(REGEXP_EXTRACT_ALL(a.expression, r'"data_source_id":([0-9]+)')) x
      ORDER BY CAST(x AS INT64)), ",")                 AS ds_ids
  FROM `dw-main-silver.archives.audience_segment_archives` a
  JOIN camp c USING (campaign_id)
  WHERE a.expression_type_id = 2 AND a.is_targeted = TRUE
    -- cap at window end so the last row per campaign IS the final in-window state
    AND a.create_time < TIMESTAMP(LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)))
),
chg AS (
  SELECT v.*,
    LAG(ds_ids)      OVER (PARTITION BY campaign_id ORDER BY create_time) AS prev_ds,
    LAG(audience_id) OVER (PARTITION BY campaign_id ORDER BY create_time) AS prev_aud
  FROM v
)
SELECT
  chg.campaign_id                                   AS clog_campaign_id,
  c.campaign_group_id                               AS clog_group_id,
  c.camp_name                                       AS clog_camp_name,
  DATE(chg.create_time)                             AS clog_changed_on,
  chg.audience_id                                   AS clog_audience_id,
  chg.segment_id                                    AS clog_segment_id,
  chg.ds_ids                                        AS clog_ds_ids,
  COALESCE(gs.grp_spend, 0)                         AS clog_grp_spend,
  DATE_SUB(IF(DATE('{{ Period_Start }}') = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE('{{ Period_Start }}')), INTERVAL 1 YEAR) AS p1_start,
  DATE_SUB(LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)),   INTERVAL 1 YEAR) AS p1_end,
  IF(DATE('{{ Period_Start }}') = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE('{{ Period_Start }}'))                            AS p2_start,
  LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH))                              AS p2_end,
  DATE_SUB(IF(DATE('{{ Period_Start }}') = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE('{{ Period_Start }}')), INTERVAL 1 YEAR) AS win_start,
  LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH))                              AS win_end
FROM chg
JOIN camp c ON c.campaign_id = chg.campaign_id
LEFT JOIN grp_spend gs ON gs.campaign_group_id = c.campaign_group_id
WHERE chg.prev_ds IS NULL OR chg.ds_ids != chg.prev_ds OR chg.audience_id != chg.prev_aud
ORDER BY c.campaign_group_id, chg.create_time
