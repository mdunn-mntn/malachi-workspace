-- TI-837 BQ baseline for Databricks benchmark — Phase 0
-- ----------------------------------------------------------------
-- Mirrors queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql but:
--   * 3 advertisers (subset of v5 cohort)
--   * 1 day  (2026-04-23 impression window, +3 day visit window)
--
-- ADVERTISERS  : 31276 (Ferguson Home), 31455 (Ancient Nutrition), 38422 (TurboTenant)
-- ANALYSIS_WIN : 2026-04-23 00:00:00 UTC → 2026-04-24 00:00:00 UTC
-- VISIT_WIN    : 2026-04-23 00:00:00 UTC → 2026-04-26 00:00:00 UTC (+3d)
--
-- Used as the apples-to-apples baseline for the Spark port in
-- artifacts/spark_lift_3adv_1day.py — same cohort, same window, same hash,
-- same methodology. Compare per-cell ATT, bytes, wall time, slot time.
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
-- Per-advertiser per-segment empirical win_rates (carried over from v5)
win_rates AS (
  SELECT * FROM UNNEST([
    STRUCT(31276 AS advertiser_id, 0.093003 AS wr_all, 0.062059 AS wr_prosp, 0.042767 AS wr_stage1, 0.032330 AS wr_rtg),
    STRUCT(31455 AS advertiser_id, 0.131959 AS wr_all, 0.125461 AS wr_prosp, 0.072254 AS wr_stage1, 0.006904 AS wr_rtg),
    STRUCT(38422 AS advertiser_id, 0.062331 AS wr_all, 0.051848 AS wr_prosp, 0.036034 AS wr_stage1, 0.010877 AS wr_rtg)
  ])
),

campaign_dim AS (
  SELECT campaign_id, advertiser_id, objective_id, funnel_level
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
    AND advertiser_id IN (31276, 31455, 38422)
),

prospecting AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip,
    CAST(household_score AS INT64) AS household_score
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE CAST(advertiser_id AS INT64) IN (31276, 31455, 38422)
    AND year  = '2026' AND month = '04' AND day = '23'
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
ip_max_score AS (
  SELECT advertiser_id, ip, MAX(household_score) AS max_household_score
  FROM prospecting GROUP BY advertiser_id, ip
),
ip_assigned AS (
  SELECT
    s.advertiser_id, s.ip,
    CASE
      WHEN s.max_household_score = 10000 THEN 'high'
      WHEN s.max_household_score BETWEEN 7000 AND 9999 THEN 'peak'
      WHEN s.max_household_score BETWEEN 3333 AND 6999 THEN 'mid'
      ELSE 'max_reach'
    END AS intent_tier,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(s.advertiser_id AS STRING), ':', s.ip)))) AS bucket,
    MOD(
      ABS(FARM_FINGERPRINT(CONCAT(CAST(s.advertiser_id AS STRING), ':wr:', s.ip))),
      100000
    ) AS wr_bucket
  FROM ip_max_score s
),
holdouts  AS (SELECT advertiser_id, ip, intent_tier, wr_bucket FROM ip_assigned WHERE bucket BETWEEN 0 AND 99),
targeted  AS (SELECT advertiser_id, ip, intent_tier               FROM ip_assigned WHERE bucket BETWEEN 100 AND 999),

-- Augmentor scan — 1 day only
augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) = DATE(TIMESTAMP('2026-04-23 00:00:00 UTC'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

biddable_holdouts_full AS (
  SELECT h.advertiser_id, h.ip, h.intent_tier, h.wr_bucket,
         wr.wr_all, wr.wr_prosp, wr.wr_stage1, wr.wr_rtg
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
  INNER JOIN win_rates wr USING (advertiser_id)
),

cost_imp_pairs AS (
  SELECT DISTINCT
    CAST(ci.advertiser_id AS INT64) AS advertiser_id,
    ci.ip,
    c.objective_id, c.funnel_level
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN campaign_dim c ON ci.campaign_id = c.campaign_id
  WHERE DATE(ci.time) = DATE(TIMESTAMP('2026-04-23 00:00:00 UTC'))
    AND ci.advertiser_id IN (31276, 31455, 38422)
    AND ci.ip IS NOT NULL AND ci.ip != '0.0.0.0'
),

served_treatment_all    AS (SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip)),
served_treatment_prosp  AS (SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id IN (1, 5, 6)),
served_treatment_stage1 AS (SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id IN (1, 5, 6) AND c.funnel_level = 1),
served_treatment_rtg    AS (SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id = 4),

cp_pairs AS (
  SELECT DISTINCT
    CAST(cp.advertiser_id AS INT64) AS advertiser_id,
    cp.ip,
    c.objective_id, c.funnel_level
  FROM `dw-main-silver.logdata.clickpass_log` cp
  INNER JOIN campaign_dim c ON cp.campaign_id = c.campaign_id
  WHERE DATE(cp.time) >= DATE(TIMESTAMP('2026-04-23 00:00:00 UTC'))
    AND DATE(cp.time) <  DATE(TIMESTAMP('2026-04-26 00:00:00 UTC'))
    AND cp.advertiser_id IN (31276, 31455, 38422)
    AND cp.ip IS NOT NULL AND cp.ip != '0.0.0.0'
),

clickpass_all    AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs),
clickpass_prosp  AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id IN (1, 5, 6)),
clickpass_stage1 AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id IN (1, 5, 6) AND funnel_level = 1),
clickpass_rtg    AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id = 4),

guid_visits AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-23 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-26 00:00:00 UTC'))
    AND advertiser_id IN (31276, 31455, 38422)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

bh_all    AS (SELECT advertiser_id, ip, intent_tier FROM biddable_holdouts_full WHERE wr_all    > 0 AND wr_bucket < CAST(wr_all    * 100000 AS INT64)),
bh_prosp  AS (SELECT advertiser_id, ip, intent_tier FROM biddable_holdouts_full WHERE wr_prosp  > 0 AND wr_bucket < CAST(wr_prosp  * 100000 AS INT64)),
bh_stage1 AS (SELECT advertiser_id, ip, intent_tier FROM biddable_holdouts_full WHERE wr_stage1 > 0 AND wr_bucket < CAST(wr_stage1 * 100000 AS INT64)),
bh_rtg    AS (SELECT advertiser_id, ip, intent_tier FROM biddable_holdouts_full WHERE wr_rtg    > 0 AND wr_bucket < CAST(wr_rtg    * 100000 AS INT64)),

all_subjects AS (
  SELECT 'all' AS segment, 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM bh_all
  UNION ALL
  SELECT 'all', 'treated_served', advertiser_id, ip, intent_tier FROM served_treatment_all
  UNION ALL
  SELECT 'prosp', 'holdout_biddable', advertiser_id, ip, intent_tier FROM bh_prosp
  UNION ALL
  SELECT 'prosp', 'treated_served', advertiser_id, ip, intent_tier FROM served_treatment_prosp
  UNION ALL
  SELECT 'stage1', 'holdout_biddable', advertiser_id, ip, intent_tier FROM bh_stage1
  UNION ALL
  SELECT 'stage1', 'treated_served', advertiser_id, ip, intent_tier FROM served_treatment_stage1
  UNION ALL
  SELECT 'rtg', 'holdout_biddable', advertiser_id, ip, intent_tier FROM bh_rtg
  UNION ALL
  SELECT 'rtg', 'treated_served', advertiser_id, ip, intent_tier FROM served_treatment_rtg
),

clickpass_with_segment AS (
  SELECT 'all'    AS segment, advertiser_id, ip FROM clickpass_all
  UNION ALL SELECT 'prosp',  advertiser_id, ip FROM clickpass_prosp
  UNION ALL SELECT 'stage1', advertiser_id, ip FROM clickpass_stage1
  UNION ALL SELECT 'rtg',    advertiser_id, ip FROM clickpass_rtg
)

SELECT
  s.segment, s.advertiser_id, s.group_name, s.intent_tier,
  COUNT(DISTINCT s.ip)                                        AS n_ips,
  COUNT(DISTINCT cv.ip)                                       AS clickpass_visitors,
  COUNT(DISTINCT gv.ip)                                       AS guid_visitors,
  SAFE_DIVIDE(COUNT(DISTINCT cv.ip), COUNT(DISTINCT s.ip))    AS clickpass_visit_rate,
  SAFE_DIVIDE(COUNT(DISTINCT gv.ip), COUNT(DISTINCT s.ip))    AS guid_visit_rate
FROM all_subjects s
LEFT JOIN clickpass_with_segment cv
  ON cv.segment = s.segment AND cv.advertiser_id = s.advertiser_id AND cv.ip = s.ip
LEFT JOIN guid_visits gv
  ON gv.advertiser_id = s.advertiser_id AND gv.ip = s.ip
GROUP BY s.segment, s.advertiser_id, s.group_name, s.intent_tier
ORDER BY s.segment, s.advertiser_id, s.group_name, s.intent_tier;
