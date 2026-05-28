WITH
window_ AS (SELECT DATE '2026-04-29' AS lo, DATE '2026-05-28' AS hi),

campaign_rollup AS (
  SELECT
    advertiser_id, campaign_id,
    SUM(impressions)                               AS impressions_30d,
    SUM(media_spend + data_spend + platform_spend) AS total_spend_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`, window_
  WHERE day BETWEEN window_.lo AND window_.hi
  GROUP BY advertiser_id, campaign_id
  HAVING SUM(impressions) > 0
),

campaign_ds AS (
  SELECT
    s.campaign_id,
    ARRAY_AGG(DISTINCT ds_id IGNORE NULLS) AS ds_array
  FROM `dw-main-silver.audience.audience_segments` s,
       UNNEST(REGEXP_EXTRACT_ALL(s.expression, r'"data_source_id":(\d+)')) AS ds_str_raw,
       UNNEST([SAFE_CAST(ds_str_raw AS INT64)])                            AS ds_id
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
  GROUP BY s.campaign_id
),

flagged AS (
  SELECT
    r.advertiser_id, r.campaign_id, r.impressions_30d, r.total_spend_30d,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (17, 18)) AS uses_stale_3p
  FROM campaign_rollup r
  LEFT JOIN campaign_ds d USING (campaign_id)
),

per_advertiser AS (
  SELECT
    advertiser_id,
    SUM(IF(uses_stale_3p, impressions_30d, 0))  AS stale_impressions,
    SUM(IF(uses_stale_3p, total_spend_30d, 0))  AS stale_spend
  FROM flagged
  GROUP BY advertiser_id
  HAVING SUM(IF(uses_stale_3p, total_spend_30d, 0)) > 0
)

SELECT
  p.advertiser_id,
  a.company_name,
  p.stale_impressions,
  p.stale_spend
FROM per_advertiser p
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a USING (advertiser_id)
ORDER BY p.stale_spend DESC
LIMIT 30;
