-- TI-999: % of advertisers using 3P, broken by spend tier.
-- Bucket advertisers by total 30d spend, then compute usage rates.

WITH
window_ AS (SELECT DATE '2026-04-29' AS lo, DATE '2026-05-28' AS hi),

campaign_rollup AS (
  SELECT advertiser_id, campaign_id,
         SUM(impressions)                               AS impressions_30d,
         SUM(media_spend + data_spend + platform_spend) AS total_spend_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`, window_
  WHERE day BETWEEN window_.lo AND window_.hi
  GROUP BY advertiser_id, campaign_id
  HAVING SUM(impressions) > 0
),

campaign_ds AS (
  SELECT s.campaign_id,
         ARRAY_AGG(DISTINCT ds_id IGNORE NULLS) AS ds_array
  FROM `dw-main-silver.audience.audience_segments` s,
       UNNEST(REGEXP_EXTRACT_ALL(s.expression, r'"data_source_id":(\d+)')) AS ds_str_raw,
       UNNEST([SAFE_CAST(ds_str_raw AS INT64)])                            AS ds_id
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
  GROUP BY s.campaign_id
),

flagged AS (
  SELECT r.advertiser_id, r.campaign_id, r.impressions_30d, r.total_spend_30d,
         EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (17, 18, 35)) AS uses_3p,
         EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (4, 8))       AS uses_1p,
         EXISTS(SELECT 1 FROM UNNEST(d.ds_array) x WHERE x IN (17, 18))     AS uses_stale_3p
  FROM campaign_rollup r
  LEFT JOIN campaign_ds d USING (campaign_id)
),

per_advertiser AS (
  SELECT
    advertiser_id,
    SUM(total_spend_30d)                                     AS total_spend_30d,
    SUM(impressions_30d)                                     AS impressions_30d,
    COUNT(*)                                                 AS n_campaigns,
    LOGICAL_OR(uses_3p)                                      AS adv_uses_3p,
    LOGICAL_OR(uses_1p)                                      AS adv_uses_1p,
    LOGICAL_OR(uses_stale_3p)                                AS adv_uses_stale_3p,
    SUM(IF(uses_3p, total_spend_30d, 0))                     AS spend_via_3p,
    SUM(IF(uses_1p, total_spend_30d, 0))                     AS spend_via_1p,
    SUM(IF(uses_stale_3p, total_spend_30d, 0))               AS spend_via_stale_3p
  FROM flagged
  GROUP BY advertiser_id
),

tiered AS (
  SELECT
    *,
    CASE
      WHEN total_spend_30d >= 100000 THEN 'a_enterprise_100K+'
      WHEN total_spend_30d >=  20000 THEN 'b_mid_20K_100K'
      WHEN total_spend_30d >=   5000 THEN 'c_smb_5K_20K'
      ELSE                                'd_micro_under_5K'
    END AS spend_tier
  FROM per_advertiser
)

SELECT
  spend_tier,
  COUNT(*)                                                                                AS n_advertisers,
  SUM(total_spend_30d)                                                                    AS tier_total_spend,
  ROUND(100.0 * COUNTIF(adv_uses_3p) / COUNT(*), 1)                                       AS pct_advs_use_3p,
  ROUND(100.0 * COUNTIF(adv_uses_1p) / COUNT(*), 1)                                       AS pct_advs_use_1p,
  ROUND(100.0 * COUNTIF(adv_uses_stale_3p) / COUNT(*), 1)                                 AS pct_advs_use_stale_3p,
  ROUND(100.0 * SUM(spend_via_3p) / SUM(total_spend_30d), 1)                              AS pct_spend_via_3p,
  ROUND(100.0 * SUM(spend_via_1p) / SUM(total_spend_30d), 1)                              AS pct_spend_via_1p,
  ROUND(100.0 * SUM(spend_via_stale_3p) / SUM(total_spend_30d), 1)                        AS pct_spend_via_stale_3p
FROM tiered
GROUP BY spend_tier
ORDER BY spend_tier;
