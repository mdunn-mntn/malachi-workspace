-- Module 11 — VV (Verified-Visit) lookback window change log
-- One row per change event to an advertiser's VV lookback windows (a MEASUREMENT change).
-- Source of truth: archives_advertiser_archives (version history) + live advertisers row.
--   PRO_Window (prospecting VV lookback) = clickpass_acquisition_ttl
--   RT_Window  (retargeting VV lookback) = clickpass_click_ttl
--   conversion window (separate)         = conversion_window
-- Params: {{AID}} only (full history; cheap). No campaign_group/campaign hardcoding.
WITH hist AS (
  SELECT update_time, version,
    EXTRACT(DAY FROM clickpass_acquisition_ttl) AS pro_window_days,
    EXTRACT(DAY FROM clickpass_click_ttl)        AS rt_window_days,
    EXTRACT(DAY FROM conversion_window)          AS conversion_window_days
  FROM `dw-main-bronze.integrationprod.archives_advertiser_archives`
  WHERE advertiser_id = {{AID}}
  UNION ALL
  -- CURRENT LIVE state — the archive lags the live edit. The live `advertisers` table stores
  -- the TTL as a STRING ('N days'), so parse the leading integer. Max version => sorts last
  -- within its update_time.
  SELECT update_time, 2147483647 AS version,
    CAST(REGEXP_EXTRACT(CAST(clickpass_acquisition_ttl AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(clickpass_click_ttl        AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(conversion_window          AS STRING), r"([0-9]+)") AS INT64)
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE advertiser_id = {{AID}}
),
flagged AS (
  SELECT update_time, pro_window_days, rt_window_days, conversion_window_days,
    LAG(pro_window_days)        OVER (ORDER BY update_time, version) AS prev_pro,
    LAG(rt_window_days)         OVER (ORDER BY update_time, version) AS prev_rt,
    LAG(conversion_window_days) OVER (ORDER BY update_time, version) AS prev_conv
  FROM hist
)
SELECT
  DATE(update_time)      AS change_date,
  update_time,
  pro_window_days        AS pro_window,
  rt_window_days         AS rt_window,
  conversion_window_days AS conversion_window,
  prev_pro, prev_rt, prev_conv
FROM flagged
WHERE prev_pro IS NULL
   OR pro_window_days        IS DISTINCT FROM prev_pro
   OR rt_window_days         IS DISTINCT FROM prev_rt
   OR conversion_window_days IS DISTINCT FROM prev_conv
ORDER BY update_time