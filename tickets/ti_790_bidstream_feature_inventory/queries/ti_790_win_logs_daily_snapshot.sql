-- TI-790: win_logs daily IP snapshot
-- Net-new signals: video engagement (VCR, skip, mute), viewability, IVT flags,
--   device make/model, content language, clearing price, clicks
-- Cost: ~13 GB/day, ~19s wall time. Tested 2026-03-31.

SELECT
  ip, DATE(time) AS event_date,
  COUNT(*) AS n_wins,

  -- Video engagement (UNIQUE — strongest behavioral signal)
  SUM(SAFE_CAST(video_plays AS INT64)) AS total_video_plays,
  SUM(SAFE_CAST(video_completes AS INT64)) AS total_video_completes,
  SUM(SAFE_CAST(video_skips AS INT64)) AS total_video_skips,
  SUM(SAFE_CAST(video_midpoints AS INT64)) AS total_video_midpoints,
  SUM(SAFE_CAST(video_q1s AS INT64)) AS total_video_q1s,
  SUM(SAFE_CAST(video_q3s AS INT64)) AS total_video_q3s,
  ROUND(SAFE_DIVIDE(SUM(SAFE_CAST(video_completes AS INT64)), NULLIF(SUM(SAFE_CAST(video_plays AS INT64)), 0)), 4) AS video_completion_rate,
  ROUND(SAFE_DIVIDE(SUM(SAFE_CAST(video_skips AS INT64)), NULLIF(SUM(SAFE_CAST(video_plays AS INT64)), 0)), 4) AS video_skip_rate,

  -- Video interaction (UNIQUE — granular engagement)
  SUM(SAFE_CAST(video_mutes AS INT64)) AS total_video_mutes,
  SUM(SAFE_CAST(video_unmutes AS INT64)) AS total_video_unmutes,
  SUM(SAFE_CAST(video_pauses AS INT64)) AS total_video_pauses,
  SUM(SAFE_CAST(video_fullscreens AS INT64)) AS total_video_fullscreens,

  -- Viewability (UNIQUE)
  COUNTIF(SAFE_CAST(in_view AS BOOL)) AS n_viewable,
  COUNTIF(SAFE_CAST(is_measurable AS BOOL)) AS n_measurable,
  ROUND(SAFE_DIVIDE(COUNTIF(SAFE_CAST(in_view AS BOOL)), NULLIF(COUNTIF(SAFE_CAST(is_measurable AS BOOL)), 0)), 4) AS viewability_rate,
  AVG(SAFE_CAST(in_view_time_ms AS INT64)) AS avg_in_view_time_ms,

  -- IVT flags (UNIQUE — fraud detection)
  COUNTIF(SAFE_CAST(invalid_impression AS BOOL)) AS n_invalid_impressions,
  COUNTIF(SAFE_CAST(invalid_automated_browser AS BOOL)) AS n_invalid_automated_browser,
  COUNTIF(SAFE_CAST(invalid_data_center_traffic AS BOOL)) AS n_invalid_data_center,

  -- Device details (shared with bid_logs only)
  COUNT(DISTINCT platform_device_make) AS n_distinct_device_makes,
  COUNT(DISTINCT platform_device_model) AS n_distinct_device_models,
  COUNT(DISTINCT platform_device_screen_size) AS n_distinct_screen_sizes,

  -- Content metadata (shared with bid_logs only)
  COUNT(DISTINCT content_language) AS n_distinct_content_languages,

  -- Pricing (UNIQUE)
  AVG(SAFE_CAST(clearing_price_micros_usd AS FLOAT64)) / 1000000 AS avg_clearing_price_usd,
  AVG(SAFE_CAST(win_cost_micros_usd AS FLOAT64)) / 1000000 AS avg_win_cost_usd,

  -- Clicks (UNIQUE)
  SUM(SAFE_CAST(clicks AS INT64)) AS total_clicks,

  -- Conversions (UNIQUE at win level)
  SUM(SAFE_CAST(conversions AS INT64)) AS total_conversions,
  SUM(SAFE_CAST(conversion_value AS FLOAT64)) AS total_conversion_value

FROM `dw-main-silver.logdata.win_logs`
WHERE DATE(time) = '2026-03-30'  -- replace with target date
  AND ip IS NOT NULL AND ip != ''
GROUP BY ip, event_date
;
