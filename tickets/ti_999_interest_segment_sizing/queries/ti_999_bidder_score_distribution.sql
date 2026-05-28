-- TI-999 / Slack-thread investigation: how does the bidder treat 3P targeting?
-- Specifically: do 3P-only IPs get bid on, or do they wait for HI/PP scored IPs to exhaust?
-- Sources:
--   audience.audience_segments — what score_type does the expression declare
--   cost_impression_log.model_params — what RTC score was attached to each delivered impression
-- Two outputs:
--   (a) distribution of score_type across all active TPA expressions
--   (b) distribution of realtime_conquest_score in delivered impressions, sliced by campaign class

-- Output A: score_type distribution
WITH score_dist AS (
  SELECT
    IFNULL(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(expression, r'"score_type":"([a-z_]+)"'), ','), '(none)') AS score_types,
    COUNT(*) AS n
  FROM `dw-main-silver.audience.audience_segments`
  WHERE expression_type_id = 2 AND is_targeted = TRUE
  GROUP BY 1
),
-- Output B: RTC score band by campaign class on a recent day of delivery
campaign_ds AS (
  SELECT s.campaign_id,
         LOGICAL_OR(ds_id IN (17, 18, 35)) AS uses_3p,
         LOGICAL_OR(ds_id IN (4, 8, 47))   AS uses_list_retargeting
  FROM `dw-main-silver.audience.audience_segments` s,
       UNNEST(REGEXP_EXTRACT_ALL(s.expression, r'"data_source_id":(\d+)')) AS ds_str,
       UNNEST([SAFE_CAST(ds_str AS INT64)]) AS ds_id
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
  GROUP BY s.campaign_id
),
imps AS (
  SELECT
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'campaign_id=(\d+)')               AS INT64) AS campaign_id,
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'realtime_conquest_score=(-?\d+)') AS INT64) AS rtc_score
  FROM `dw-main-silver.logdata.cost_impression_log` c
  WHERE DATE(c.time) = '2026-05-26'
)
SELECT
  'campaign_class_x_rtc_band' AS report,
  CASE
    WHEN d.uses_list_retargeting THEN 'retargeting (uses CRM/IP-list)'
    WHEN d.uses_3p               THEN 'prospecting + 3P'
    WHEN d.uses_3p IS NULL       THEN 'no audience expression'
    ELSE                              'prospecting, no 3P'
  END AS campaign_class,
  IF(i.rtc_score = 10000, 'rtc_10000', 'rtc_minus1_or_other') AS rtc_band,
  COUNT(*) AS n_impressions
FROM imps i
LEFT JOIN campaign_ds d ON i.campaign_id = d.campaign_id
GROUP BY 2, 3
UNION ALL
SELECT 'score_type_distribution', score_types, NULL, n FROM score_dist
ORDER BY report, campaign_class, rtc_band;

-- ============================================================================
-- Finding 14d follow-up: split prospecting+3P by whether RTC/BUK is also used.
-- Tests whether the "67% scored" pattern for 3P was driven by RTC mixing.
-- ============================================================================
WITH
  campaign_ds AS (
    SELECT s.campaign_id,
           LOGICAL_OR(ds_id IN (17, 18, 35)) AS uses_3p,
           LOGICAL_OR(ds_id IN (19))         AS uses_rtc,
           LOGICAL_OR(ds_id IN (38))         AS uses_buk,
           LOGICAL_OR(ds_id IN (4, 8, 47))   AS uses_list_retargeting
    FROM `dw-main-silver.audience.audience_segments` s,
         UNNEST(REGEXP_EXTRACT_ALL(s.expression, r'"data_source_id":(\d+)')) AS ds_str,
         UNNEST([SAFE_CAST(ds_str AS INT64)]) AS ds_id
    WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
    GROUP BY s.campaign_id
  ),
  prospecting_3p AS (
    SELECT campaign_id, uses_rtc, uses_buk
    FROM campaign_ds
    WHERE uses_3p AND NOT uses_list_retargeting
  ),
  imps AS (
    SELECT
      SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'campaign_id=(\d+)')           AS INT64) AS campaign_id,
      SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'household_score=(-?\d+)')     AS INT64) AS hh_score
    FROM `dw-main-silver.logdata.cost_impression_log` c
    WHERE DATE(c.time) = '2026-05-26'
  )
SELECT
  CASE
    WHEN p.uses_rtc AND p.uses_buk        THEN '3P_with_RTC_AND_BUK'
    WHEN p.uses_rtc AND NOT p.uses_buk    THEN '3P_with_RTC_only'
    WHEN NOT p.uses_rtc AND p.uses_buk    THEN '3P_with_BUK_only'
    ELSE                                       '3P_PURE_no_other_targeting'
  END AS bucket,
  CASE WHEN i.hh_score = -1 THEN '-1 (unscored)'
       WHEN i.hh_score < 1000 THEN '1-999'
       WHEN i.hh_score < 5000 THEN '1000-4999'
       WHEN i.hh_score < 8000 THEN '5000-7999'
       WHEN i.hh_score < 10000 THEN '8000-9999'
       WHEN i.hh_score = 10000 THEN '10000'
       ELSE '>10000' END AS hh_band,
  COUNT(*) AS n_imps,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 1), 2) AS pct_within_bucket
FROM imps i JOIN prospecting_3p p USING (campaign_id)
GROUP BY 1, 2
ORDER BY bucket, hh_band;
