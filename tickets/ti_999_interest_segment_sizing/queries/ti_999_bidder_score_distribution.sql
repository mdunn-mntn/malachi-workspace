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
