-- TI-837: 30-advertiser ghost-bidding lift — v4 (prospecting-only, consistent win_rates)
-- ----------------------------------------------------------------
-- Same as v3 but with PROSPECTING-ONLY win_rates, internally consistent
-- with prospecting-only served_treatment definition. v3 had mismatched
-- win_rates (computed on v1's full-campaign served), which would have
-- biased the holdout subsampling rate.
--
-- Two methodology fixes vs v1 (Phase 2 deck v3 numbers, superseded):
--
-- 1. WIN-RATE CORRECTION (Alex Knorr 2026-04-28). Subsample biddable_holdouts
--    by per-advertiser empirical win_rate so the denominator matches the
--    treated arm's "actually-served" condition, not just "biddable."
--    Win_rates: prospecting_served_n / (biddable_holdouts_n × 9).
--    Range 0.001 to 0.126. Median 0.008. Hardcoded STRUCT literal.
--
-- 2. PROSPECTING-CAMPAIGN FILTER (caught 2026-04-28 afternoon). v1/v2 counted
--    ALL impressions for the advertiser as served_treatment, including
--    retargeting campaigns (objective_id=4). v4 filters cost_impression_log
--    and clickpass_log to objective_id IN (1,5,6) — prospecting only
--    (Stage 1 + Multi-Touch Stage 2 + Multi-Touch Full Funnel Stage 3).
--    guid_log stays unfiltered (it's just "did the IP visit," not campaign-
--    attributed).
--
-- Methodology note (2026-04-28). Random hash subsampling at win_rate
-- preserves the EXPECTED visit rate (smaller N, same rate). It only
-- changes the lift estimate if bidder selection bias correlates with
-- visit propensity (would-have-been-served IPs are systematically
-- visit-prone). We do the sampling regardless as the empirical equivalence
-- check Alex requested — if v4 lift differs from a no-sampling version,
-- selection bias is detected.
--
-- ADVERTISERS  = 30 advertisers (Phase 2 cohort, 2026-04-27)
-- ANALYSIS_WIN = 2026-04-20 00:00:00 UTC → 2026-04-27 00:00:00 UTC
-- VISIT_WIN    = 2026-04-20 00:00:00 UTC → 2026-04-29 00:00:00 UTC
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
-- Hardcoded per-advertiser win_rates (PROSPECTING-ONLY, computed 2026-04-28).
-- win_rate = prospecting_served_n / (biddable_holdouts_n × 9).
-- Range 0.001 to 0.126. Median 0.008. Mean 0.019.
win_rates AS (
  SELECT * FROM UNNEST([
    STRUCT(30181 AS advertiser_id, 0.028779 AS win_rate),
    STRUCT(30392 AS advertiser_id, 0.010374 AS win_rate),
    STRUCT(30496 AS advertiser_id, 0.009095 AS win_rate),
    STRUCT(31276 AS advertiser_id, 0.062059 AS win_rate),
    STRUCT(31297 AS advertiser_id, 0.006001 AS win_rate),
    STRUCT(31455 AS advertiser_id, 0.125461 AS win_rate),
    STRUCT(31464 AS advertiser_id, 0.010203 AS win_rate),
    STRUCT(32244 AS advertiser_id, 0.004415 AS win_rate),
    STRUCT(32320 AS advertiser_id, 0.027176 AS win_rate),
    STRUCT(32404 AS advertiser_id, 0.022260 AS win_rate),
    STRUCT(32527 AS advertiser_id, 0.008352 AS win_rate),
    STRUCT(32899 AS advertiser_id, 0.008437 AS win_rate),
    STRUCT(33467 AS advertiser_id, 0.008384 AS win_rate),
    STRUCT(33572 AS advertiser_id, 0.030331 AS win_rate),
    STRUCT(33684 AS advertiser_id, 0.006614 AS win_rate),
    STRUCT(34141 AS advertiser_id, 0.002632 AS win_rate),
    STRUCT(34365 AS advertiser_id, 0.001181 AS win_rate),
    STRUCT(34862 AS advertiser_id, 0.008211 AS win_rate),
    STRUCT(35086 AS advertiser_id, 0.010637 AS win_rate),
    STRUCT(35374 AS advertiser_id, 0.005573 AS win_rate),
    STRUCT(35573 AS advertiser_id, 0.017137 AS win_rate),
    STRUCT(37222 AS advertiser_id, 0.002945 AS win_rate),
    STRUCT(37796 AS advertiser_id, 0.003480 AS win_rate),
    STRUCT(38307 AS advertiser_id, 0.004045 AS win_rate),
    STRUCT(38422 AS advertiser_id, 0.051848 AS win_rate),
    STRUCT(42097 AS advertiser_id, 0.070751 AS win_rate),
    STRUCT(43996 AS advertiser_id, 0.009821 AS win_rate),
    STRUCT(46426 AS advertiser_id, 0.004446 AS win_rate),
    STRUCT(50525 AS advertiser_id, 0.000822 AS win_rate),
    STRUCT(56187 AS advertiser_id, 0.003817 AS win_rate)
  ])
),

-- Prospecting campaign IDs for the cohort (objective_id IN 1,5,6 only)
prospecting_campaigns AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE
    AND is_test = FALSE
    AND objective_id IN (1, 5, 6)
    AND advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
),

-- Step 1: targetable IP universe — union of 7 daily prospecting partitions
prospecting AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip,
    CAST(household_score AS INT64) AS household_score,
    CASE
      WHEN CAST(household_score AS INT64) = 10000 THEN 'high'
      WHEN CAST(household_score AS INT64) BETWEEN 7000 AND 9999 THEN 'peak'
      WHEN CAST(household_score AS INT64) BETWEEN 3333 AND 6999 THEN 'mid'
      ELSE 'max_reach'
    END AS intent_tier
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE CAST(advertiser_id AS INT64) IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND year  = '2026'
    AND month = '04'
    AND day IN ('20','21','22','23','24','25','26')
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Step 2: 10% holdout / 90% targeted assignment (deterministic hash)
ip_max_score AS (
  SELECT advertiser_id, ip, MAX(household_score) AS max_household_score
  FROM prospecting
  GROUP BY advertiser_id, ip
),
ip_assigned AS (
  SELECT
    s.advertiser_id,
    s.ip,
    s.max_household_score,
    CASE
      WHEN s.max_household_score = 10000 THEN 'high'
      WHEN s.max_household_score BETWEEN 7000 AND 9999 THEN 'peak'
      WHEN s.max_household_score BETWEEN 3333 AND 6999 THEN 'mid'
      ELSE 'max_reach'
    END AS intent_tier,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(s.advertiser_id AS STRING), ':', s.ip)))) AS bucket
  FROM ip_max_score s
),

holdouts AS (
  SELECT advertiser_id, ip, intent_tier
  FROM ip_assigned
  WHERE bucket BETWEEN 0 AND 99
),
targeted AS (
  SELECT advertiser_id, ip, intent_tier
  FROM ip_assigned
  WHERE bucket BETWEEN 100 AND 999
),

-- Step 3: augmentor scan (advertiser-agnostic; single pass)
augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Step 4: WIN-RATE-CORRECTED biddable_holdouts.
-- Subsample at per-advertiser win_rate using a fresh hash bucket
-- ('wr:' salt makes it independent of the original holdout assignment).
biddable_holdouts AS (
  SELECT h.advertiser_id, h.ip, h.intent_tier
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
  INNER JOIN win_rates wr USING (advertiser_id)
  WHERE wr.win_rate > 0
    AND MOD(
          ABS(FARM_FINGERPRINT(
            CONCAT(CAST(h.advertiser_id AS STRING), ':wr:', h.ip))),
          100000
        ) < CAST(wr.win_rate * 100000 AS INT64)
),

-- Step 5: served_treatment, FILTERED to prospecting campaigns
cost_imp_advertiser_ips AS (
  SELECT DISTINCT ci.advertiser_id, ci.ip
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN prospecting_campaigns pc
    ON ci.campaign_id = pc.campaign_id
  WHERE DATE(ci.time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(ci.time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ci.advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND ci.ip IS NOT NULL AND ci.ip != '0.0.0.0'
),
served_treatment AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t
  INNER JOIN cost_imp_advertiser_ips c
    USING (advertiser_id, ip)
),

-- Step 6: clickpass_log, FILTERED to prospecting campaigns
clickpass_visits AS (
  SELECT DISTINCT cp.advertiser_id, cp.ip
  FROM `dw-main-silver.logdata.clickpass_log` cp
  INNER JOIN prospecting_campaigns pc
    ON cp.campaign_id = pc.campaign_id
  WHERE DATE(cp.time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(cp.time) <  DATE(TIMESTAMP('2026-04-30 00:00:00 UTC'))
    AND cp.advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND cp.ip IS NOT NULL AND cp.ip != '0.0.0.0'
),

-- Step 7: guid_log — visits to advertiser site, NO campaign filter
-- (guid is "did this IP visit," not campaign-attributed; cause-agnostic)
guid_visits AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-30 00:00:00 UTC'))
    AND advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

subjects AS (
  SELECT 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM biddable_holdouts
  UNION ALL
  SELECT 'treated_served'   AS group_name, advertiser_id, ip, intent_tier FROM served_treatment
)

SELECT
  s.advertiser_id,
  s.group_name,
  s.intent_tier,
  COUNT(DISTINCT s.ip) AS n_ips,
  COUNT(DISTINCT cv.ip) AS clickpass_visitors,
  COUNT(DISTINCT gv.ip) AS guid_visitors,
  SAFE_DIVIDE(COUNT(DISTINCT cv.ip), COUNT(DISTINCT s.ip)) AS clickpass_visit_rate,
  SAFE_DIVIDE(COUNT(DISTINCT gv.ip), COUNT(DISTINCT s.ip)) AS guid_visit_rate
FROM subjects s
LEFT JOIN clickpass_visits cv
  ON s.ip = cv.ip AND s.advertiser_id = cv.advertiser_id
LEFT JOIN guid_visits gv
  ON s.ip = gv.ip AND s.advertiser_id = gv.advertiser_id
GROUP BY s.advertiser_id, s.group_name, s.intent_tier
ORDER BY s.advertiser_id, s.group_name, s.intent_tier;
