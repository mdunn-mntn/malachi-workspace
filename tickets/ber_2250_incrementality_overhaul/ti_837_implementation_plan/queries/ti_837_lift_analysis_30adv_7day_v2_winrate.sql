-- TI-837: Multi-Advertiser Ghost-Bidding Lift Analysis — v2 with WIN-RATE CORRECTION
-- ----------------------------------------------------------------
-- Same pipeline as ti_837_lift_analysis_30adv_7day.sql, but applies the
-- methodological fix Alex Knorr flagged 2026-04-28:
--
-- The biddable_holdouts denominator was artificially large. We were treating
-- "appeared in augmentor_log" as equivalent to "would have been served an
-- impression" — but the bidder doesn't win every auction. The treated arm's
-- denominator is "actually served" (a SUBSET of biddable, filtered by win
-- rate); the holdout arm's denominator should match.
--
-- Fix: deterministically subsample biddable_holdouts at the per-advertiser
-- empirical win_rate = served_treatment_n / biddable_targeted_n.
--
-- ADVERTISERS  = 30 advertisers (Phase 2 cohort, 2026-04-27); see artifacts/ti_837_phase2_cohort.md
-- ANALYSIS_WIN = 2026-04-20 00:00:00 UTC → 2026-04-27 00:00:00 UTC
-- VISIT_WIN    = 2026-04-20 00:00:00 UTC → 2026-04-29 00:00:00 UTC (analysis end + 3d)
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
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

hashed AS (
  SELECT
    p.*,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(p.advertiser_id AS STRING), ':', p.ip)))) AS bucket
  FROM prospecting p
),

ip_max_score AS (
  SELECT advertiser_id, ip, MAX(household_score) AS max_household_score
  FROM hashed
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

-- Step 4: augmentor scan over the analysis window
augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- "Biddable" sets — the IPs we COULD have bid on
biddable_holdouts_raw AS (
  SELECT DISTINCT h.advertiser_id, h.ip, h.intent_tier
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
),
biddable_targeted AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t
  INNER JOIN augmentor_ips a USING (ip)
),

-- Step 5: served treatment IPs (what we actually won and served)
cost_imp_advertiser_ips AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
served_treatment AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t
  INNER JOIN cost_imp_advertiser_ips c
    USING (advertiser_id, ip)
),

-- Step 6: per-advertiser empirical win rate
-- win_rate = (IPs we actually served) / (IPs we could have bid on, on the targeted side)
-- Bounded to (0, 1] to avoid divide-by-zero or pathological values.
win_rates AS (
  SELECT
    bt.advertiser_id,
    COUNT(DISTINCT bt.ip)        AS biddable_targeted_n,
    COUNT(DISTINCT s.ip)         AS served_treatment_n,
    SAFE_DIVIDE(COUNT(DISTINCT s.ip), COUNT(DISTINCT bt.ip)) AS win_rate
  FROM biddable_targeted bt
  LEFT JOIN served_treatment s
    USING (advertiser_id, ip)
  GROUP BY bt.advertiser_id
),

-- Step 7: WIN-RATE-CORRECTED biddable holdouts
-- Deterministic subsample: keep IPs where a fresh hash bucket falls below
-- win_rate × 100000. Independent of the original holdout-assignment hash
-- (different salt 'wr:'). Each advertiser uses its own win_rate.
biddable_holdouts AS (
  SELECT
    bhr.advertiser_id, bhr.ip, bhr.intent_tier
  FROM biddable_holdouts_raw bhr
  INNER JOIN win_rates wr USING (advertiser_id)
  WHERE wr.win_rate > 0
    AND MOD(
          ABS(FARM_FINGERPRINT(
            CONCAT(CAST(bhr.advertiser_id AS STRING), ':wr:', bhr.ip))),
          100000
        ) < CAST(wr.win_rate * 100000 AS INT64)
),

-- Step 8: visit outcomes (analysis window + 3-day post-period)
clickpass_visits AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-30 00:00:00 UTC'))
    AND advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
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
),

-- Per-cell rates (advertiser × group × tier)
cells AS (
  SELECT
    s.advertiser_id,
    s.group_name,
    s.intent_tier,
    COUNT(DISTINCT s.ip)                                          AS n_ips,
    COUNT(DISTINCT cv.ip)                                         AS clickpass_visitors,
    COUNT(DISTINCT gv.ip)                                         AS guid_visitors,
    SAFE_DIVIDE(COUNT(DISTINCT cv.ip), COUNT(DISTINCT s.ip))      AS clickpass_visit_rate,
    SAFE_DIVIDE(COUNT(DISTINCT gv.ip), COUNT(DISTINCT s.ip))      AS guid_visit_rate
  FROM subjects s
  LEFT JOIN clickpass_visits cv
    ON s.ip = cv.ip AND s.advertiser_id = cv.advertiser_id
  LEFT JOIN guid_visits gv
    ON s.ip = gv.ip AND s.advertiser_id = gv.advertiser_id
  GROUP BY s.advertiser_id, s.group_name, s.intent_tier
),

-- Per-advertiser win rate as a separate diagnostic row
diag_winrates AS (
  SELECT
    advertiser_id,
    'WIN_RATE_DIAGNOSTIC' AS group_name,
    'all' AS intent_tier,
    biddable_targeted_n AS n_ips,
    NULL AS clickpass_visitors,
    served_treatment_n AS guid_visitors,  -- repurposed: served_treatment_n
    NULL AS clickpass_visit_rate,
    win_rate AS guid_visit_rate           -- repurposed: win_rate
  FROM win_rates
)

SELECT * FROM cells
UNION ALL
SELECT * FROM diag_winrates
ORDER BY advertiser_id, group_name, intent_tier;
