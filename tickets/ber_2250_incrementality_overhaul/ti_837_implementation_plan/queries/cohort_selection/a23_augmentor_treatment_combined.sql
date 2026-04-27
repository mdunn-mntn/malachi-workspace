-- TI-837 Phase 2 cohort selection — Stage A.2 + A.3 combined
-- Augmentor coverage AND treatment-side delivery per (advertiser, tier)
-- ----------------------------------------------------------------
-- Window: 2026-04-20 → 2026-04-26 UTC.
-- Combined into one query so the dominant augmentor_log scan
-- (~18-20 TB / Phase 1) is paid once across all advertisers.
--
-- Output: per (advertiser_id, intent_tier) row with:
--   - holdouts_n              : holdout-bucket IPs in prospecting universe
--   - biddable_holdouts_n     : holdouts ∩ augmentor_log appearance
--   - targeted_n              : non-holdout IPs in prospecting universe
--   - served_treatment_n      : targeted IPs ∩ cost_impression_log
--   - exposure_rate           : served_treatment_n / targeted_n
--                               (treatment-side win rate)
--   - biddable_rate           : biddable_holdouts_n / holdouts_n
--                               (holdout-side biddability)
--
-- Column semantics match Phase 1 ti_837_lift_analysis_7adv_7day.sql Steps
-- 4–5 verbatim. NO advertiser filter — gets per-advertiser characterization
-- for the entire prospecting universe.
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
-- Step 1: prospecting universe
prospecting AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip,
    CAST(household_score AS INT64) AS household_score
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE year  = '2026'
    AND month = '04'
    AND day IN ('20','21','22','23','24','25','26')
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

ip_max_score AS (
  SELECT advertiser_id, ip, MAX(household_score) AS max_score
  FROM prospecting
  GROUP BY advertiser_id, ip
),

ip_assigned AS (
  SELECT
    advertiser_id,
    ip,
    CASE
      WHEN max_score = 10000                  THEN 'high'
      WHEN max_score BETWEEN 7000 AND 9999    THEN 'peak'
      WHEN max_score BETWEEN 3333 AND 6999    THEN 'mid'
      ELSE 'max_reach'
    END AS intent_tier,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip)))) AS bucket
  FROM ip_max_score
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

-- Step 2: augmentor scan (THE costly scan — paid once)
augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

biddable_holdouts AS (
  SELECT DISTINCT h.advertiser_id, h.ip, h.intent_tier
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
),

-- Step 3: cost_impression scan, joined per-advertiser
cost_imp_advertiser_ips AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

served_treatment AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t
  INNER JOIN cost_imp_advertiser_ips c
    USING (advertiser_id, ip)
),

-- Aggregate to per (advertiser, tier)
agg_holdouts AS (
  SELECT advertiser_id, intent_tier, COUNT(DISTINCT ip) AS holdouts_n
  FROM holdouts
  GROUP BY advertiser_id, intent_tier
),
agg_biddable AS (
  SELECT advertiser_id, intent_tier, COUNT(DISTINCT ip) AS biddable_holdouts_n
  FROM biddable_holdouts
  GROUP BY advertiser_id, intent_tier
),
agg_targeted AS (
  SELECT advertiser_id, intent_tier, COUNT(DISTINCT ip) AS targeted_n
  FROM targeted
  GROUP BY advertiser_id, intent_tier
),
agg_served AS (
  SELECT advertiser_id, intent_tier, COUNT(DISTINCT ip) AS served_treatment_n
  FROM served_treatment
  GROUP BY advertiser_id, intent_tier
)

SELECT
  COALESCE(h.advertiser_id, t.advertiser_id) AS advertiser_id,
  COALESCE(h.intent_tier,   t.intent_tier)   AS intent_tier,
  IFNULL(h.holdouts_n, 0)                                                  AS holdouts_n,
  IFNULL(b.biddable_holdouts_n, 0)                                         AS biddable_holdouts_n,
  IFNULL(t.targeted_n, 0)                                                  AS targeted_n,
  IFNULL(s.served_treatment_n, 0)                                          AS served_treatment_n,
  SAFE_DIVIDE(b.biddable_holdouts_n, h.holdouts_n)                         AS biddable_rate,
  SAFE_DIVIDE(s.served_treatment_n, t.targeted_n)                          AS exposure_rate
FROM agg_holdouts h
FULL OUTER JOIN agg_biddable b USING (advertiser_id, intent_tier)
FULL OUTER JOIN agg_targeted t USING (advertiser_id, intent_tier)
FULL OUTER JOIN agg_served   s USING (advertiser_id, intent_tier)
WHERE COALESCE(h.holdouts_n, 0) + COALESCE(t.targeted_n, 0) >= 100
ORDER BY advertiser_id, intent_tier
