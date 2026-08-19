-- incr_75_entry_cohort_byday_window.sql — day-by-day audit showing why the window stops at 2026-07-07.
-- Confirms ghost_frac sits at the ~0.10 design point on every interior day and shows
-- the two contaminated edges (2026-06-22 stock, and the right-censored last 7 days).
WITH e AS (
  SELECT advertiser_id, campaign_id, ip, dt, arm, visited,
    ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
),
entry AS (SELECT dt AS entry_dt, arm, visited FROM e WHERE rn = 1),
byday AS (
  SELECT entry_dt,
    COUNTIF(arm="submitted") n_t, COUNTIF(arm="ghost") n_h,
    COUNTIF(arm="submitted" AND visited) v_t, COUNTIF(arm="ghost" AND visited) v_h
  FROM entry GROUP BY entry_dt
),
edge AS (SELECT MAX(entry_dt) AS max_dt FROM byday)
SELECT entry_dt, n_t, n_h,
  ROUND(SAFE_DIVIDE(n_h, n_t+n_h), 4) AS ghost_frac,
  ROUND(100*SAFE_DIVIDE(v_t,n_t), 4) AS vr_t_pct,
  ROUND(100*SAFE_DIVIDE(v_h,n_h), 4) AS vr_h_pct,
  ROUND(100*SAFE_DIVIDE(SAFE_DIVIDE(v_t,n_t)-SAFE_DIVIDE(v_h,n_h), SAFE_DIVIDE(v_h,n_h)), 2) AS rel_pct,
  CASE WHEN entry_dt = "2026-06-22" THEN "left edge (stock)"
       WHEN entry_dt > DATE_SUB(edge.max_dt, INTERVAL 7 DAY) THEN "right-censored"
       ELSE "clean" END AS window_status
FROM byday, edge ORDER BY entry_dt
