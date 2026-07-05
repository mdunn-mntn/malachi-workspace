-- Module 07 / 07b — Prospecting audience-expression CHANGE HISTORY (per campaign).
-- Every distinct audience config a prospecting campaign (obj=1, funnel=1) ran over time,
-- from the type-2 archive, collapsed to the moments the DS set OR the audience_id changed.
-- DS ids regex-extracted from expression JSON. Ordered by create_time (archive version non-monotonic).
-- Restricted to campaigns that delivered in [WIN_START, WIN_END). No hardcoded ids.
WITH camp AS (
  SELECT campaign_id, campaign_group_id, name AS camp_name
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
    AND campaign_id IN (
      SELECT DISTINCT campaign_id
      FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
      WHERE advertiser_id = {{AID}}
        AND day >= "{{WIN_START}}" AND day < "{{WIN_END}}" AND impressions > 0
    )
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
),
chg AS (
  SELECT v.*,
    LAG(ds_ids)      OVER (PARTITION BY campaign_id ORDER BY create_time) AS prev_ds,
    LAG(audience_id) OVER (PARTITION BY campaign_id ORDER BY create_time) AS prev_aud
  FROM v
)
SELECT
  chg.campaign_id,
  c.campaign_group_id,
  c.camp_name,
  DATE(chg.create_time) AS changed_on,
  chg.audience_id,
  chg.segment_id,
  chg.ds_ids
FROM chg
JOIN camp c ON c.campaign_id = chg.campaign_id
WHERE prev_ds IS NULL OR chg.ds_ids != prev_ds OR chg.audience_id != prev_aud
ORDER BY chg.campaign_id, chg.create_time
