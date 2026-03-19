-- TI-650: MIN lookback analysis — VV-only resolution per advertiser
-- Finds the actual MAX gap between S3 VV and nearest prior S1/S2 VV
-- in the same campaign_group_id. Uses ONLY clickpass_log (~33 GB).
--
-- Purpose: determine the tightest possible lookback window per advertiser
-- before running the expensive full trace (event_log = ~14 TB).
--
-- No time filter on prior VVs — scans all history to find absolute MAX gap.
-- Audit window: 2026-02-04 to 2026-02-11

WITH all_cp AS (
  SELECT
    SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS ip,
    cp.time AS vv_time,
    c.campaign_group_id,
    c.funnel_level,
    cp.advertiser_id
  FROM `dw-main-silver.logdata.clickpass_log` cp
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND c.funnel_level IN (1, 2, 3)
    AND c.objective_id IN (1, 5, 6)
  WHERE cp.advertiser_id IN (37056, 35573, 34094, 32230)
    AND cp.ip IS NOT NULL
),
s3_vvs AS (
  SELECT ip, vv_time, campaign_group_id, advertiser_id
  FROM all_cp
  WHERE funnel_level = 3
    AND vv_time >= TIMESTAMP('2026-02-04') AND vv_time < TIMESTAMP('2026-02-11')
),
prior_vvs AS (
  SELECT ip, vv_time AS prior_vv_time, campaign_group_id, funnel_level
  FROM all_cp
  WHERE funnel_level IN (1, 2)
)
SELECT
  s.advertiser_id,
  a.company_name,
  COUNT(*) AS total_s3_vvs,
  COUNTIF(p.prior_vv_time IS NOT NULL) AS has_prior_vv,
  COUNTIF(p.prior_vv_time IS NULL) AS no_prior_vv,
  ROUND(100.0 * COUNTIF(p.prior_vv_time IS NOT NULL) / COUNT(*), 2) AS vv_resolved_pct,
  MAX(TIMESTAMP_DIFF(s.vv_time, p.prior_vv_time, DAY)) AS max_gap_days,
  APPROX_QUANTILES(TIMESTAMP_DIFF(s.vv_time, p.prior_vv_time, DAY), 100)[OFFSET(50)] AS p50_gap_days,
  APPROX_QUANTILES(TIMESTAMP_DIFF(s.vv_time, p.prior_vv_time, DAY), 100)[OFFSET(90)] AS p90_gap_days,
  APPROX_QUANTILES(TIMESTAMP_DIFF(s.vv_time, p.prior_vv_time, DAY), 100)[OFFSET(99)] AS p99_gap_days
FROM s3_vvs s
LEFT JOIN (
  SELECT s3.ip, s3.campaign_group_id, s3.vv_time,
    MAX(pv.prior_vv_time) AS prior_vv_time
  FROM s3_vvs s3
  JOIN prior_vvs pv
    ON pv.ip = s3.ip
    AND pv.campaign_group_id = s3.campaign_group_id
    AND pv.prior_vv_time < s3.vv_time
  GROUP BY s3.ip, s3.campaign_group_id, s3.vv_time
) p ON p.ip = s.ip AND p.campaign_group_id = s.campaign_group_id AND p.vv_time = s.vv_time
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON s.advertiser_id = a.advertiser_id AND a.deleted = FALSE
GROUP BY s.advertiser_id, a.company_name
ORDER BY total_s3_vvs DESC;
