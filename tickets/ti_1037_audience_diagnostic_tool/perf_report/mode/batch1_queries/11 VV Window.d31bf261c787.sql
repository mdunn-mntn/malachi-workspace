-- Period_End is CLAMPED to the first day of the current month (exclusive end ->
-- data through the last FULL month). The far-future param default relies on this;
-- any user-picked earlier date is honored as-is.
-- Module 11 -- VV (Verified-Visit) lookback window change log
-- One row per change event to an advertiser's VV lookback windows (a MEASUREMENT change).
-- Source of truth: archives_advertiser_archives (version history) + live advertisers row.
--   PRO_Window (prospecting VV lookback) = clickpass_acquisition_ttl
--   RT_Window  (retargeting VV lookback) = clickpass_click_ttl
--   conversion window (separate)         = conversion_window
-- Period boundaries and the trend window are carried on every row so the render
-- can forward-fill PRO/RT values at P1 and P2 starts and ends without params.
WITH hist AS (
  SELECT update_time, version,
    EXTRACT(DAY FROM clickpass_acquisition_ttl) AS pro_window_days,
    EXTRACT(DAY FROM clickpass_click_ttl)        AS rt_window_days,
    EXTRACT(DAY FROM conversion_window)          AS conversion_window_days
  FROM `dw-main-bronze.integrationprod.archives_advertiser_archives`
  WHERE advertiser_id = {{ Advertiser_ID }}
  UNION ALL
  -- CURRENT LIVE state -- the archive lags the live edit. The live advertisers table stores
  -- the TTL as a STRING (like 'N days'), so parse the leading integer. Max version sorts last
  -- within its update_time.
  SELECT update_time, 2147483647 AS version,
    CAST(REGEXP_EXTRACT(CAST(clickpass_acquisition_ttl AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(clickpass_click_ttl        AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(conversion_window          AS STRING), r"([0-9]+)") AS INT64)
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE advertiser_id = {{ Advertiser_ID }}
),
flagged AS (
  SELECT update_time, pro_window_days, rt_window_days, conversion_window_days,
    LAG(pro_window_days)        OVER (ORDER BY update_time, version) AS prev_pro,
    LAG(rt_window_days)         OVER (ORDER BY update_time, version) AS prev_rt,
    LAG(conversion_window_days) OVER (ORDER BY update_time, version) AS prev_conv
  FROM hist
),
changes AS (
  SELECT
    DATE(update_time)      AS vv_change_date,
    update_time            AS vv_update_time,
    pro_window_days        AS pro_window,
    rt_window_days         AS rt_window,
    conversion_window_days AS conversion_window,
    prev_pro, prev_rt, prev_conv
  FROM flagged
  WHERE prev_pro IS NULL
     OR pro_window_days        IS DISTINCT FROM prev_pro
     OR rt_window_days         IS DISTINCT FROM prev_rt
     OR conversion_window_days IS DISTINCT FROM prev_conv
)
SELECT
  vv_change_date,
  vv_update_time,
  pro_window,
  rt_window,
  conversion_window,
  prev_pro, prev_rt, prev_conv,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR) AS p1_start,
  DATE_SUB(LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)),   INTERVAL 1 YEAR) AS p1_end,
  DATE('{{ Period_Start }}')                            AS p2_start,
  LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH))                              AS p2_end,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR) AS win_start,
  LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH))                              AS win_end
FROM changes
ORDER BY vv_update_time
