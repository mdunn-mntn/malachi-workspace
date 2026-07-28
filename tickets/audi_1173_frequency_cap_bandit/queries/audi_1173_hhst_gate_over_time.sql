-- Q2: HHST gate-state over time, MM prospecting cohort (has_mm), 30d.
-- Time-weighted + spend-weighted share of prospecting time the gate sits at each threshold bucket.
-- Confirms the pacing-relaxation mechanism (how much time/spend runs with the gate lowered vs held high).
-- Archive is a CHANGE-LOG; validity = source_timestamp (ms) LEAD-bounded. Unpartitioned (~2GB/run).
DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP('2026-06-27 00:00:00+00');
DECLARE win_end   TIMESTAMP DEFAULT TIMESTAMP('2026-07-27 00:00:00+00');
WITH cohort AS (
  SELECT campaign_id
  FROM `dw-main-silver.audience.mm_campaign_classifier`
  WHERE has_mm = TRUE AND objective_id = 1
),
changes AS (
  SELECT a.campaign_id, a.threshold,
    TIMESTAMP_MILLIS(a.datastream_metadata.source_timestamp) AS valid_from,
    LEAD(TIMESTAMP_MILLIS(a.datastream_metadata.source_timestamp)) OVER (
      PARTITION BY a.campaign_id
      ORDER BY a.datastream_metadata.source_timestamp, a.household_score_threshold_archives_id) AS valid_to
  FROM `dw-main-bronze.integrationprod.archives_household_score_threshold_archives` a
  JOIN cohort c USING (campaign_id)
),
clipped AS (
  SELECT campaign_id, threshold,
    GREATEST(valid_from, win_start)             AS seg_start,
    LEAST(COALESCE(valid_to, win_end), win_end) AS seg_end
  FROM changes
  WHERE valid_from < win_end AND COALESCE(valid_to, win_end) > win_start
),
seg AS (
  SELECT campaign_id,
    CASE
      WHEN threshold < 0                    THEN '9_neg_unset'
      WHEN threshold = 0                    THEN '0_nogate'
      WHEN threshold BETWEEN 1 AND 3332     THEN '1_pp_1_3332'
      WHEN threshold BETWEEN 3333 AND 6665  THEN '2_mi_3333_6665'
      WHEN threshold = 6666                 THEN '3_hi_6666'
      WHEN threshold BETWEEN 6667 AND 10000 THEN '4_gt6666_le10000'
      ELSE                                       '8_gt10000_outofrange'
    END AS bucket,
    TIMESTAMP_DIFF(seg_end, seg_start, SECOND) AS secs
  FROM clipped WHERE seg_end > seg_start
),
spend AS (
  SELECT campaign_id, SUM(media_spend + data_spend + platform_spend) AS window_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day >= DATE(win_start) AND day < DATE(win_end)
  GROUP BY campaign_id
),
camp_bucket AS ( SELECT campaign_id, bucket, SUM(secs) AS secs FROM seg GROUP BY campaign_id, bucket ),
camp_total  AS ( SELECT campaign_id, SUM(secs) AS total_secs FROM seg GROUP BY campaign_id )
SELECT cb.bucket,
  ROUND(100 * SUM(cb.secs) / SUM(SUM(cb.secs)) OVER (), 2) AS pct_campaign_time,
  COUNT(DISTINCT cb.campaign_id)                          AS n_campaigns,
  ROUND(SUM(sp.window_spend * SAFE_DIVIDE(cb.secs, ct.total_secs)), 2) AS spend_weighted_usd,
  ROUND(100 * SUM(sp.window_spend * SAFE_DIVIDE(cb.secs, ct.total_secs))
        / SUM(SUM(sp.window_spend * SAFE_DIVIDE(cb.secs, ct.total_secs))) OVER (), 2) AS pct_spend
FROM camp_bucket cb JOIN camp_total ct USING (campaign_id)
LEFT JOIN spend sp USING (campaign_id)
GROUP BY cb.bucket ORDER BY cb.bucket;
