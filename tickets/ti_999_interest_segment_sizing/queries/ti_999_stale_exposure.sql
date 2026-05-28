WITH
window_ AS (SELECT DATE '2026-04-29' AS lo, DATE '2026-05-28' AS hi),

campaign_rollup AS (
  SELECT
    advertiser_id, campaign_id,
    SUM(impressions)                            AS impressions_30d,
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
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (17, 18))       AS uses_stale_3p,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x = 17)              AS uses_sharethis,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x = 18)              AS uses_dstillery,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x = 35)              AS uses_liveramp,
    EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (4, 19, 38))    AS uses_internal_targeting
  FROM campaign_rollup r
  LEFT JOIN campaign_ds d USING (campaign_id)
)

SELECT
  'all_active'                                                   AS subset,
  COUNT(*)                                                       AS n_campaigns,
  SUM(impressions_30d)                                           AS impressions_30d,
  SUM(total_spend_30d)                                           AS total_spend_30d
FROM flagged
UNION ALL SELECT 'uses_stale_3p_any',          COUNT(*), SUM(impressions_30d), SUM(total_spend_30d) FROM flagged WHERE uses_stale_3p
UNION ALL SELECT 'uses_sharethis_any',         COUNT(*), SUM(impressions_30d), SUM(total_spend_30d) FROM flagged WHERE uses_sharethis
UNION ALL SELECT 'uses_dstillery_any',         COUNT(*), SUM(impressions_30d), SUM(total_spend_30d) FROM flagged WHERE uses_dstillery
UNION ALL SELECT 'uses_liveramp_any',          COUNT(*), SUM(impressions_30d), SUM(total_spend_30d) FROM flagged WHERE uses_liveramp
UNION ALL SELECT 'stale_only_no_liveramp_no_internal',
  COUNT(*), SUM(impressions_30d), SUM(total_spend_30d) FROM flagged WHERE uses_stale_3p AND NOT uses_liveramp AND NOT uses_internal_targeting
ORDER BY subset;
