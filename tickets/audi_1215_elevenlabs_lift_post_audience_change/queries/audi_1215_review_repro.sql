-- AUDI-1215 adversarial reproduction (independent SQL, run 2026-08-21 via bq_run.sh)
-- Output: outputs/audi_1215_review_repro_itt.csv

-- 1) Instrument A entry-cohort ITT (240.3GB scan, dry-run verified; flat-rate us-central1 reservation)
WITH anchors AS (
  SELECT ip,
    ARRAY_AGG(STRUCT(dt, arm, visited, converted, campaign_id) ORDER BY dt, arm LIMIT 1)[OFFSET(0)] a
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
  WHERE dt BETWEEN '2026-06-22' AND '2026-08-20'
    AND campaign_group_id = 122748 AND partner_id = 8
  GROUP BY ip
)
SELECT
  CASE WHEN a.dt = '2026-06-22' THEN '0_excluded_first_day'
       WHEN a.dt BETWEEN '2026-06-23' AND '2026-06-30' THEN '1_pre'
       WHEN a.dt BETWEEN '2026-07-01' AND '2026-07-10' THEN '2_blackout'
       WHEN a.dt BETWEEN '2026-07-11' AND '2026-08-13' THEN '3_post'
       ELSE '4_excluded_edge' END period,
  a.arm, COUNT(*) n_ip, COUNTIF(a.visited) visited, COUNTIF(a.converted) converted,
  COUNT(DISTINCT a.campaign_id) n_campaigns, MIN(a.dt) min_dt, MAX(a.dt) max_dt
FROM anchors GROUP BY period, arm ORDER BY period, arm;

-- 2) Instrument B conversion counts by window and arm (0.15GB)
SELECT begin_date, control,
  CASE WHEN DATE(time,'America/New_York') BETWEEN '2026-06-01' AND '2026-06-30' THEN 'pre'
       WHEN DATE(time,'America/New_York') BETWEEN '2026-07-01' AND '2026-07-10' THEN 'blackout'
       WHEN DATE(time,'America/New_York') BETWEEN '2026-07-11' AND '2026-07-31' THEN 'post'
       ELSE 'other' END win,
  COUNT(*) conv, COUNT(DISTINCT ip) converters, COUNTIF(campaign_group_id=122748) conv_cg122748
FROM `dw-main-gold.reporting.v_lift__conversions`
WHERE advertiser_id=51660 AND objective_id=1 AND begin_date IN ('2026-06-01','2026-07-01')
GROUP BY 1,2,3 ORDER BY 1,3,2;

-- 3) Instrument B monthly run grain (one row per run confirmed)
SELECT begin_date, campaign_group_id, objective_impressions, objective_visits, control_visits,
  weighted_control_visits, objective_conversions, weighted_control_conversions,
  incremental_conversions, incremental_cpa, users_reached, control_users, multiplier
FROM `dw-main-gold.reporting.v_lift__results_by_month`
WHERE advertiser_id=51660 AND objective_id=1 ORDER BY begin_date;

-- 4) Instrument C spot-check
SELECT stratum_type, stratum_value, n_treatment, n_holdout, rate_treatment, rate_holdout,
  abs_itt, rel_itt, p_value
FROM `dw-main-gold.reporting.lift__ghost_bid_results`
WHERE campaign_group_id=122748 AND partner_id=8
  AND (stratum_type='overall' OR stratum_value IN ('no_score','11+')) LIMIT 20;
