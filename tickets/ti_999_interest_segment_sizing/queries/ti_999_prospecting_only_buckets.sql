-- TI-999 prospecting-only re-cut: exclude campaigns referencing any list-style targeting.
--   Exclusion set: DS4 (CRM), DS8 (IP List), DS47 (CRM Identity Graph) — all "list of known IPs/customers."
-- Among the remaining "prospecting" campaigns, bucket by 3P usage AND stale-vs-fresh.
-- Produces:
--   1) Headline bucket KPI: 3P-prospecting vs no-3P-prospecting
--   2) Stale-vs-fresh sub-buckets within 3P-prospecting
--   3) Spend totals + conv rates per bucket

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

campaign_ds_counts AS (
  SELECT
    s.campaign_id,
    ARRAY(
      SELECT AS STRUCT
        SAFE_CAST(REGEXP_EXTRACT(m, r'"data_source_id":(\d+)') AS INT64)             AS ds_id,
        ARRAY_LENGTH(SPLIT(REGEXP_EXTRACT(m, r'"category_ids":\[([\d,]+)\]'), ','))  AS n_cats
      FROM UNNEST(REGEXP_EXTRACT_ALL(
             s.expression,
             r'"data_source_id":\d+,"category_ids":\[[\d,]+\]')) AS m
    ) AS ds_cat_pairs,
    ARRAY_AGG(DISTINCT SAFE_CAST(REGEXP_EXTRACT(m_simple, r'"data_source_id":(\d+)') AS INT64) IGNORE NULLS) AS ds_set
  FROM `dw-main-silver.audience.audience_segments` s,
       UNNEST(REGEXP_EXTRACT_ALL(s.expression, r'"data_source_id":\d+')) AS m_simple
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
  GROUP BY s.campaign_id, s.expression
),

campaign_features AS (
  SELECT
    campaign_id,
    ds_set,
    COALESCE((SELECT SUM(n_cats) FROM UNNEST(ds_cat_pairs) WHERE ds_id IN (17, 18, 35)), 0) AS n_3p_dscids,
    COALESCE((SELECT SUM(n_cats) FROM UNNEST(ds_cat_pairs) WHERE ds_id = 35), 0)            AS n_liveramp_dscids,
    COALESCE((SELECT SUM(n_cats) FROM UNNEST(ds_cat_pairs) WHERE ds_id IN (17, 18)), 0)     AS n_stale_3p_dscids
  FROM campaign_ds_counts
),

joined AS (
  SELECT
    r.advertiser_id, r.campaign_id, r.impressions_30d, r.total_spend_30d, r.conversions_30d,
    COALESCE(f.n_3p_dscids, 0)        AS n_3p_dscids,
    COALESCE(f.n_liveramp_dscids, 0)  AS n_liveramp_dscids,
    COALESCE(f.n_stale_3p_dscids, 0)  AS n_stale_3p_dscids,
    -- Retargeting/list exclusion flags
    EXISTS(SELECT 1 FROM UNNEST(f.ds_set) x WHERE x IN (4, 8, 47)) AS uses_list_retargeting,
    -- 3P usage
    EXISTS(SELECT 1 FROM UNNEST(f.ds_set) x WHERE x IN (17, 18, 35)) AS uses_3p,
    EXISTS(SELECT 1 FROM UNNEST(f.ds_set) x WHERE x IN (17, 18))     AS uses_stale_3p,
    EXISTS(SELECT 1 FROM UNNEST(f.ds_set) x WHERE x = 35)            AS uses_liveramp
  FROM campaign_rollup r
  LEFT JOIN campaign_features f USING (campaign_id)
),

prospecting AS (
  SELECT *
  FROM joined
  WHERE NOT uses_list_retargeting  -- Drop any campaign touching CRM / IP List / CRM Identity Graph
),

bucketed AS (
  SELECT *,
    CASE
      WHEN NOT uses_3p                          THEN 'a_no_3p_prospecting'
      WHEN uses_liveramp AND NOT uses_stale_3p  THEN 'b_only_fresh_liveramp'
      WHEN uses_stale_3p AND NOT uses_liveramp  THEN 'c_only_stale_3p'
      WHEN uses_liveramp AND uses_stale_3p      THEN 'd_fresh_and_stale_mix'
    END AS bucket
  FROM prospecting
)

SELECT
  bucket,
  COUNT(DISTINCT campaign_id)                                       AS n_campaigns,
  COUNT(DISTINCT advertiser_id)                                     AS n_advertisers,
  SUM(impressions_30d)                                              AS impressions_30d,
  SUM(total_spend_30d)                                              AS total_spend_30d,
  SUM(conversions_30d)                                              AS conversions_30d,
  SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d))           AS conversion_rate,
  AVG(n_3p_dscids)                                                  AS avg_n_3p_dscids,
  APPROX_QUANTILES(n_3p_dscids, 100)[OFFSET(50)]                    AS median_n_3p_dscids
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
