-- TI-999: 1P-uploaded vs 3P-interest bucket KPI comparison.
-- 1P_UPLOADED = {4 CRM, 8 IP List} — advertiser uploads of their own customer data.
-- 3P_INTEREST = {17 ShareThis, 18 Dstillery, 35 LiveRamp IP} — bought third-party.
-- Note: this is independent of MNTN-internal signals (RTC, pageview, conversion, BUK).
--   A campaign in any bucket may still layer internal signals.

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

-- Extract per-campaign per-DS dscid counts.
-- Pattern matches "data_source_id":N,"category_ids":[X,Y,Z] inside the expression JSON.
campaign_ds_counts AS (
  SELECT
    s.campaign_id,
    -- For each DS-categories block, capture DS id and count of category ids.
    ARRAY(
      SELECT AS STRUCT
        SAFE_CAST(REGEXP_EXTRACT(m, r'"data_source_id":(\d+)') AS INT64)                 AS ds_id,
        ARRAY_LENGTH(SPLIT(REGEXP_EXTRACT(m, r'"category_ids":\[([\d,]+)\]'), ','))      AS n_cats
      FROM UNNEST(REGEXP_EXTRACT_ALL(
             s.expression,
             r'"data_source_id":\d+,"category_ids":\[[\d,]+\]')) AS m
    ) AS ds_cat_pairs
  FROM `dw-main-silver.audience.audience_segments` s
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
),

campaign_features AS (
  SELECT
    campaign_id,
    -- # of dscids referenced per category
    COALESCE(SUM(IF(ds_id IN (4, 8),       n_cats, 0)), 0) AS n_1p_dscids,
    COALESCE(SUM(IF(ds_id IN (17, 18, 35), n_cats, 0)), 0) AS n_3p_dscids,
    -- per-DS detail
    COALESCE(SUM(IF(ds_id = 4,  n_cats, 0)), 0)            AS n_crm_dscids,
    COALESCE(SUM(IF(ds_id = 35, n_cats, 0)), 0)            AS n_liveramp_dscids,
    COALESCE(SUM(IF(ds_id = 17, n_cats, 0)), 0)            AS n_sharethis_dscids,
    COALESCE(SUM(IF(ds_id = 18, n_cats, 0)), 0)            AS n_dstillery_dscids
  FROM campaign_ds_counts, UNNEST(ds_cat_pairs)
  GROUP BY campaign_id
),

joined AS (
  SELECT
    r.advertiser_id, r.campaign_id, r.impressions_30d, r.total_spend_30d, r.conversions_30d,
    COALESCE(f.n_1p_dscids, 0)        AS n_1p_dscids,
    COALESCE(f.n_3p_dscids, 0)        AS n_3p_dscids,
    COALESCE(f.n_crm_dscids, 0)       AS n_crm_dscids,
    COALESCE(f.n_liveramp_dscids, 0)  AS n_liveramp_dscids,
    COALESCE(f.n_sharethis_dscids, 0) AS n_sharethis_dscids,
    COALESCE(f.n_dstillery_dscids, 0) AS n_dstillery_dscids,
    CASE
      WHEN (COALESCE(f.n_1p_dscids, 0) > 0 AND COALESCE(f.n_3p_dscids, 0) > 0) THEN 'c_both_1p_and_3p'
      WHEN COALESCE(f.n_1p_dscids, 0) > 0 THEN 'a_1p_only'
      WHEN COALESCE(f.n_3p_dscids, 0) > 0 THEN 'b_3p_only'
      ELSE                                     'd_neither_1p_nor_3p'
    END AS bucket
  FROM campaign_rollup r
  LEFT JOIN campaign_features f USING (campaign_id)
)

SELECT
  bucket,
  COUNT(DISTINCT campaign_id)                                  AS n_campaigns,
  COUNT(DISTINCT advertiser_id)                                AS n_advertisers,
  SUM(impressions_30d)                                         AS impressions_30d,
  SUM(total_spend_30d)                                         AS total_spend_30d,
  SUM(conversions_30d)                                         AS conversions_30d,
  SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d))      AS conversion_rate,
  AVG(n_1p_dscids)                                             AS avg_n_1p_dscids,
  AVG(n_3p_dscids)                                             AS avg_n_3p_dscids,
  APPROX_QUANTILES(n_3p_dscids, 100)[OFFSET(50)]               AS median_n_3p_dscids,
  APPROX_QUANTILES(n_1p_dscids, 100)[OFFSET(50)]               AS median_n_1p_dscids
FROM joined
GROUP BY bucket
ORDER BY bucket;
