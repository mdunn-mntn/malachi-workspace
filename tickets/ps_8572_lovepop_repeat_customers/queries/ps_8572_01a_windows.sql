/* ============================================================================
   PS-8572 CHECK 1a — VV (Verified-Visit) lookback window change log, Lovepop
   Adapted from ti_1037 perf_report/queries/11_vv_window_change_log.sql
   advertiser_id = 58797
   ----------------------------------------------------------------------------
   SOURCE OF TRUTH: VV windows live on the ADVERTISER row and its version
   history in `archives_advertiser_archives` (NOT advertiser_configurations /
   r2_advertiser_settings). Archive columns are INTERVAL, magnitude in the DAY
   component:
     PRO_Window (prospecting VV lookback) = clickpass_acquisition_ttl
     RT_Window  (retargeting VV lookback) = clickpass_click_ttl
     conversion windows (SEPARATE fields) = conversion_window,
       click_conversion_window, view_conversion_window, invoice_conversion_window
   ORDER BY update_time (version non-monotonic). LAG-collapse to change events.
   Archive LAGS the live row -> UNION the live `advertisers` row, whose TTLs
   are STRINGs like '180 days' (parse leading int). ~0.06GB expected.
   ============================================================================ */
WITH hist AS (
  SELECT update_time, version,
    EXTRACT(DAY FROM clickpass_acquisition_ttl)   AS pro_window_days,   -- PRO_Window
    EXTRACT(DAY FROM clickpass_click_ttl)         AS rt_window_days,    -- RT_Window
    EXTRACT(DAY FROM conversion_window)           AS conversion_window_days,
    EXTRACT(DAY FROM click_conversion_window)     AS click_conv_window_days,
    EXTRACT(DAY FROM view_conversion_window)      AS view_conv_window_days,
    EXTRACT(DAY FROM invoice_conversion_window)   AS invoice_conv_window_days
  FROM `dw-main-bronze.integrationprod.archives_advertiser_archives`
  WHERE advertiser_id = 58797
  UNION ALL
  SELECT update_time, 2147483647 AS version,
    CAST(REGEXP_EXTRACT(CAST(clickpass_acquisition_ttl AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(clickpass_click_ttl        AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(conversion_window          AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(click_conversion_window    AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(view_conversion_window     AS STRING), r"([0-9]+)") AS INT64),
    CAST(REGEXP_EXTRACT(CAST(invoice_conversion_window  AS STRING), r"([0-9]+)") AS INT64)
  FROM `dw-main-bronze.integrationprod.advertisers`
  WHERE advertiser_id = 58797
),
flagged AS (
  SELECT update_time, pro_window_days, rt_window_days, conversion_window_days,
    click_conv_window_days, view_conv_window_days, invoice_conv_window_days,
    LAG(pro_window_days)          OVER (ORDER BY update_time, version) AS prev_pro,
    LAG(rt_window_days)           OVER (ORDER BY update_time, version) AS prev_rt,
    LAG(conversion_window_days)   OVER (ORDER BY update_time, version) AS prev_conv,
    LAG(click_conv_window_days)   OVER (ORDER BY update_time, version) AS prev_click_conv,
    LAG(view_conv_window_days)    OVER (ORDER BY update_time, version) AS prev_view_conv,
    LAG(invoice_conv_window_days) OVER (ORDER BY update_time, version) AS prev_invoice_conv
  FROM hist
)
SELECT
  DATE(update_time)        AS change_date,
  update_time,
  pro_window_days          AS pro_window,
  rt_window_days           AS rt_window,
  conversion_window_days   AS conversion_window,
  click_conv_window_days   AS click_conversion_window,
  view_conv_window_days    AS view_conversion_window,
  invoice_conv_window_days AS invoice_conversion_window,
  prev_pro, prev_rt, prev_conv, prev_click_conv, prev_view_conv, prev_invoice_conv
FROM flagged
WHERE prev_pro IS NULL
   OR pro_window_days          IS DISTINCT FROM prev_pro
   OR rt_window_days           IS DISTINCT FROM prev_rt
   OR conversion_window_days   IS DISTINCT FROM prev_conv
   OR click_conv_window_days   IS DISTINCT FROM prev_click_conv
   OR view_conv_window_days    IS DISTINCT FROM prev_view_conv
   OR invoice_conv_window_days IS DISTINCT FROM prev_invoice_conv
ORDER BY update_time
