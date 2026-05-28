WITH
window_ AS (SELECT DATE '2026-04-29' AS lo, DATE '2026-05-28' AS hi),

campaign_rollup AS (
  SELECT
    advertiser_id, campaign_id,
    SUM(impressions)                               AS impressions_30d,
    SUM(media_spend + data_spend + platform_spend) AS total_spend_30d,
    SUM(click_conversions + view_conversions)      AS conversions_30d
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
    r.advertiser_id, r.campaign_id, r.impressions_30d, r.total_spend_30d, r.conversions_30d,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (17, 18))    AS uses_stale_3p,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x = 35)           AS uses_fresh_3p,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (17, 18, 35)) AS uses_interest
  FROM campaign_rollup r
  LEFT JOIN campaign_ds d USING (campaign_id)
),

bucketed AS (
  SELECT
    *,
    CASE
      WHEN NOT uses_interest                       THEN 'a_no_interest'
      WHEN uses_fresh_3p AND NOT uses_stale_3p     THEN 'b_only_fresh_liveramp'
      WHEN uses_stale_3p AND NOT uses_fresh_3p     THEN 'c_only_stale_3p'
      WHEN uses_fresh_3p AND uses_stale_3p         THEN 'd_fresh_and_stale_mix'
    END AS bucket
  FROM flagged
)

SELECT
  bucket,
  COUNT(DISTINCT campaign_id)                                   AS n_campaigns,
  COUNT(DISTINCT advertiser_id)                                 AS n_advertisers,
  SUM(impressions_30d)                                          AS impressions_30d,
  SUM(total_spend_30d)                                          AS total_spend_30d,
  SUM(conversions_30d)                                          AS conversions_30d,
  SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d))       AS conversion_rate
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
