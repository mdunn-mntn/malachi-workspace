-- audi_1215_holdout_prepost.sql
-- AUDI-1215 Instrument B: holdout lineage pre/post for AID 51660 / CGID 122748
-- Run via: bash .claude/scripts/bq_run.sh --nouse_legacy_sql "<SQL>"
-- Windows (America/New_York, the lineage's time_zone): PRE 2026-06-01..06-30 (Jun run),
-- BLACKOUT 2026-07-01..07-10 excluded, POST 2026-07-11..07-31 (Jul run). No Aug run exists.

-- 1. Monthly results, all columns (outputs/audi_1215_lift_by_month.csv)
SELECT * FROM `dw-main-gold.reporting.v_lift__results_by_month`
WHERE advertiser_id = 51660 AND begin_date >= '2026-02-01'
ORDER BY begin_date, objective_id LIMIT 100;

-- latest run check (returned 2026-07-01)
SELECT MAX(begin_date) AS max_run FROM `dw-main-gold.reporting.v_lift__results_by_month`;

-- 2. Conversion arm semantics + timestamp bounds per run x arm x probattr x campaign_group
SELECT begin_date, control, is_probattr, campaign_group_id, COUNT(*) c, COUNT(DISTINCT ip) ips,
       MIN(time) min_t, MAX(time) max_t, MIN(event_time) min_et, MAX(event_time) max_et
FROM `dw-main-gold.reporting.v_lift__conversions`
WHERE advertiser_id = 51660 AND begin_date IN ('2026-06-01','2026-07-01')
GROUP BY 1,2,3,4 ORDER BY 1,2,3,4 LIMIT 200;

-- 3. Conversions per arm in pre/blackout/post, with attributed-event-date split for treated
WITH conv AS (
  SELECT begin_date, control, campaign_group_id,
         DATE(time,'America/New_York') AS conv_date,
         DATE(event_time,'America/New_York') AS event_date, ip
  FROM `dw-main-gold.reporting.v_lift__conversions`
  WHERE advertiser_id = 51660 AND begin_date IN ('2026-06-01','2026-07-01'))
SELECT begin_date, control,
  CASE WHEN begin_date='2026-06-01' AND conv_date BETWEEN '2026-06-01' AND '2026-06-30' THEN 'pre'
       WHEN begin_date='2026-07-01' AND conv_date BETWEEN '2026-07-01' AND '2026-07-10' THEN 'blackout'
       WHEN begin_date='2026-07-01' AND conv_date BETWEEN '2026-07-11' AND '2026-07-31' THEN 'post'
       ELSE 'other' END AS win,
  COUNT(*) conv_all, COUNT(DISTINCT ip) ips_all,
  COUNTIF(campaign_group_id=122748) conv_cg122748,
  COUNT(DISTINCT IF(campaign_group_id=122748, ip, NULL)) ips_cg122748,
  COUNTIF(control=false AND event_date <= '2026-06-30') ev_le_0630,
  COUNTIF(control=false AND event_date BETWEEN '2026-07-01' AND '2026-07-10') ev_blackout,
  COUNTIF(control=false AND event_date >= '2026-07-11') ev_ge_0711
FROM conv GROUP BY 1,2,3 ORDER BY 1,2,3 LIMIT 100;

-- 4. Control-arm visitors (one row per ip per run) in pre/blackout/post
SELECT begin_date,
  CASE WHEN begin_date='2026-06-01' AND DATE(time,'America/New_York') BETWEEN '2026-06-01' AND '2026-06-30' THEN 'pre'
       WHEN begin_date='2026-07-01' AND DATE(time,'America/New_York') BETWEEN '2026-07-01' AND '2026-07-10' THEN 'blackout'
       WHEN begin_date='2026-07-01' AND DATE(time,'America/New_York') BETWEEN '2026-07-11' AND '2026-07-31' THEN 'post'
       ELSE 'other' END AS win,
  COUNT(*) visit_rows, COUNT(DISTINCT ip) ips, MIN(time) min_t, MAX(time) max_t
FROM `dw-main-silver.enriched.lift__holdout_visits`
WHERE advertiser_id = 51660 AND begin_date IN ('2026-06-01','2026-07-01')
GROUP BY 1,2 ORDER BY 1,2 LIMIT 100;