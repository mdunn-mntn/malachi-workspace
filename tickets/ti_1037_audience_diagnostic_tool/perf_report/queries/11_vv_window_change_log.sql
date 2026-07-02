/* ============================================================================
   Module 11 — VV (Verified-Visit) lookback window change log
   ----------------------------------------------------------------------------
   Flags every change to an advertiser's VV lookback windows over time — a
   MEASUREMENT change that can move visits/conversions/CVR/ROAS across a YoY/MoM
   window and must be reconciled before attributing a performance change.

   SOURCE OF TRUTH (TI-1037 + workflow, verified 2026-07-02): the VV windows live on
   the ADVERTISER row and its version history in `archives_advertiser_archives`
   (NOT advertiser_configurations / r2_advertiser_settings). All INTERVAL, magnitude
   in the DAY component:
     PRO_Window (prospecting VV lookback) = clickpass_acquisition_ttl
     RT_Window  (retargeting VV lookback) = clickpass_click_ttl
     conversion window (SEPARATE field)   = conversion_window
   ORDER BY update_time (version is non-monotonic if unsorted; create_time is the
   account-creation stamp, not the edit time). LAG-collapse to actual change events.

   WHY IT MATTERS (workflow finding, high confidence): a UI-reported conversion
   (from_verified_impression=TRUE) is attributed to a verified-visit impression via the
   SAME VVS engine — 100% of such conversions co-occur with a VV on the same ad_served_id.
   And `conversion_lookback_window` is NULL per-advertiser (the VV/page-view window is the
   only populated lookback). So shortening the VV window shrinks the connectable-conversion
   pool -> can lower conversions/CVR/ROAS, on a ~lookback-length LAG (old long-tail
   attributions age out only after the new window cycles through).

   Params : {{AID}}   (no date filter — full history; cheap ~0.06GB)
   ============================================================================ */
WITH hist AS (
  SELECT update_time, version,
    EXTRACT(DAY FROM clickpass_acquisition_ttl) AS pro_window_days,   -- PRO_Window
    EXTRACT(DAY FROM clickpass_click_ttl)        AS rt_window_days,   -- RT_Window
    EXTRACT(DAY FROM conversion_window)          AS conversion_window_days
  FROM `dw-main-bronze.integrationprod.archives_advertiser_archives`
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
