-- TI-896: empirical check of "MM and PP are the same product" hypothesis.
-- Per-advertiser, in week of 2025-10-20 (pre-cliff) and 2025-11-10 (post-cliff):
--   - Did this advertiser have DS2-flagged delivery (MM-detector)?
--   - Did this advertiser have DS13+DS19+rtc-flagged delivery (PP-detector)?
-- Then compute the 2x2 cross-tab: gained PP & lost MM, etc.
-- Tests whether the MM-spend cliff is the same advertisers gaining PP, or different ones.

WITH
cohort AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-01-01') AND CURRENT_DATE() AND impressions > 0
),

camp_last_active AS (
  SELECT campaign_id, MAX(day) AS last_active_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2025-08-01') AND CURRENT_DATE() AND impressions > 0
  GROUP BY campaign_id
),

archive_rows AS (
  SELECT
    asa.campaign_id,
    c.advertiser_id,
    asa.update_time,
    LEAST(
      COALESCE(LEAD(asa.update_time) OVER (PARTITION BY asa.campaign_id ORDER BY asa.update_time, asa.version),
               CURRENT_TIMESTAMP()),
      COALESCE(TIMESTAMP_ADD(TIMESTAMP(la.last_active_day), INTERVAL 1 DAY),
               CURRENT_TIMESTAMP())
    ) AS next_update_time,
    REGEXP_CONTAINS(asa.expression, r'"score_type"\s*:\s*"rtc"')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*13\b')
      AND REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*19\b') AS is_pp_expr,
    REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*2\b') AS has_ds2,
    REGEXP_CONTAINS(asa.expression, r'"data_source_id"\s*:\s*13\b') AS has_ds13_anywhere
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` asa
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  JOIN cohort USING (advertiser_id)
  LEFT JOIN camp_last_active la USING (campaign_id)
  WHERE asa.expression_type_id = 2 AND asa.is_targeted = TRUE
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND asa.update_time >= TIMESTAMP('2025-08-01')
),

-- Per (campaign, day) flags
camp_day_flags AS (
  SELECT
    s.advertiser_id,
    s.campaign_id,
    s.day,
    s.media_cost,
    LOGICAL_OR(ar.is_pp_expr) AS is_pp_day,
    LOGICAL_OR(ar.has_ds2) AS has_ds2_day,
    LOGICAL_OR(ar.has_ds13_anywhere) AS has_ds13_day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN cohort USING (advertiser_id)
  LEFT JOIN archive_rows ar
    ON ar.campaign_id = s.campaign_id
    AND TIMESTAMP(s.day) >= ar.update_time
    AND TIMESTAMP(s.day) < ar.next_update_time
  WHERE s.day BETWEEN DATE('2025-10-13') AND DATE('2025-11-23')
    AND s.impressions > 0
  GROUP BY s.advertiser_id, s.campaign_id, s.day, s.media_cost
),

-- Per advertiser per period: any PP delivery? any DS2 delivery? total spend?
adv_period AS (
  SELECT
    advertiser_id,
    CASE
      WHEN day BETWEEN DATE('2025-10-13') AND DATE('2025-10-19') THEN 'pre'   -- week before cliff
      WHEN day BETWEEN DATE('2025-11-03') AND DATE('2025-11-09') THEN 'post'  -- week after cliff
      ELSE NULL END AS period,
    LOGICAL_OR(is_pp_day) AS adv_has_pp,
    LOGICAL_OR(has_ds2_day) AS adv_has_ds2,
    LOGICAL_OR(has_ds13_day) AS adv_has_ds13,
    SUM(media_cost) AS spend
  FROM camp_day_flags
  WHERE day BETWEEN DATE('2025-10-13') AND DATE('2025-10-19')
     OR day BETWEEN DATE('2025-11-03') AND DATE('2025-11-09')
  GROUP BY advertiser_id, period
),

pivoted AS (
  SELECT
    advertiser_id,
    LOGICAL_OR(period='pre' AND adv_has_pp) AS pre_has_pp,
    LOGICAL_OR(period='post' AND adv_has_pp) AS post_has_pp,
    LOGICAL_OR(period='pre' AND adv_has_ds2) AS pre_has_ds2,
    LOGICAL_OR(period='post' AND adv_has_ds2) AS post_has_ds2,
    LOGICAL_OR(period='pre' AND adv_has_ds13) AS pre_has_ds13,
    LOGICAL_OR(period='post' AND adv_has_ds13) AS post_has_ds13,
    SUM(IF(period='pre',  spend, 0)) AS pre_spend,
    SUM(IF(period='post', spend, 0)) AS post_spend
  FROM adv_period
  WHERE period IS NOT NULL
  GROUP BY advertiser_id
)

-- Cross-tab outputs
SELECT
  CAST(pre_has_pp AS STRING)  AS pre_has_pp,
  CAST(post_has_pp AS STRING) AS post_has_pp,
  CAST(pre_has_ds2 AS STRING) AS pre_has_ds2,
  CAST(post_has_ds2 AS STRING) AS post_has_ds2,
  COUNT(*) AS n_advertisers,
  ROUND(SUM(pre_spend),  0) AS pre_spend,
  ROUND(SUM(post_spend), 0) AS post_spend
FROM pivoted
WHERE pre_spend > 0 OR post_spend > 0
GROUP BY pre_has_pp, post_has_pp, pre_has_ds2, post_has_ds2
ORDER BY n_advertisers DESC
