WITH e AS (
  SELECT advertiser_id, campaign_id, ip, dt, arm, visited,
    ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
),
entry AS ( SELECT dt AS entry_dt, arm, visited FROM e WHERE rn = 1 ),
byday AS (
  SELECT entry_dt,
    COUNTIF(arm="submitted") n_t, COUNTIF(arm="ghost") n_h,
    COUNTIF(arm="submitted" AND visited) v_t, COUNTIF(arm="ghost" AND visited) v_h
  FROM entry GROUP BY entry_dt
)
SELECT entry_dt, n_t, n_h,
  ROUND(SAFE_DIVIDE(n_h, n_t+n_h),4) ghost_frac,
  ROUND(100*SAFE_DIVIDE(v_t,n_t),4) vr_t_pct,
  ROUND(100*SAFE_DIVIDE(v_h,n_h),4) vr_h_pct,
  ROUND(100*(SAFE_DIVIDE(v_t,n_t)-SAFE_DIVIDE(v_h,n_h)),4) itt_pp,
  ROUND(100*SAFE_DIVIDE(SAFE_DIVIDE(v_t,n_t)-SAFE_DIVIDE(v_h,n_h), SAFE_DIVIDE(v_h,n_h)),1) rel_pct,
  ROUND((SAFE_DIVIDE(v_t,n_t)-SAFE_DIVIDE(v_h,n_h))/
     SQRT(SAFE_DIVIDE(v_t/n_t*(1-v_t/n_t),n_t)+SAFE_DIVIDE(v_h/n_h*(1-v_h/n_h),n_h)),2) z,
  IF(entry_dt >= "2026-06-24","TRUNC","full") window_obs
FROM byday ORDER BY entry_dt
