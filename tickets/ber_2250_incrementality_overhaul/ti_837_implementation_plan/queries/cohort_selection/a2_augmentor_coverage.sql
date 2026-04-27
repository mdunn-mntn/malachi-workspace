-- TI-837 Phase 2 cohort selection — Stage A.2
-- Augmentor coverage per advertiser per tier (biddable-holdout candidates)
-- ----------------------------------------------------------------
-- Window: 2026-04-20 → 2026-04-26 UTC.
-- Methodology mirrors Phase 1 ti_837_lift_analysis_7adv_7day.sql Step 4:
-- biddable_holdouts = prospecting holdouts ∩ augmentor IPs over the window.
-- Tier assignment uses the same MAX(household_score) construction.
--
-- Output: per (advertiser_id, intent_tier) row with:
--   - holdouts_n       : holdout-bucket IPs in prospecting universe
--   - biddable_holdouts: holdout IPs that ALSO appeared in augmentor_log
--   - biddable_rate    : biddable_holdouts / holdouts_n
-- The biddable_holdouts column drives the per-tier statistical floor.
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
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
    max_score,
    CASE
      WHEN max_score = 10000                       THEN 'high'
      WHEN max_score BETWEEN 7000 AND 9999         THEN 'peak'
      WHEN max_score BETWEEN 3333 AND 6999         THEN 'mid'
      ELSE 'max_reach'
    END AS intent_tier,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip)))) AS bucket
  FROM ip_max_score
),

holdouts AS (
  SELECT advertiser_id, ip, intent_tier
  FROM ip_assigned
  WHERE bucket BETWEEN 0 AND 99       -- 10% holdout
),

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
)

SELECT
  h.advertiser_id,
  h.intent_tier,
  COUNT(DISTINCT h.ip)                                                    AS holdouts_n,
  COUNT(DISTINCT b.ip)                                                    AS biddable_holdouts_n,
  SAFE_DIVIDE(COUNT(DISTINCT b.ip), COUNT(DISTINCT h.ip))                 AS biddable_rate
FROM holdouts h
LEFT JOIN biddable_holdouts b
  ON h.advertiser_id = b.advertiser_id AND h.ip = b.ip
GROUP BY h.advertiser_id, h.intent_tier
ORDER BY h.advertiser_id, h.intent_tier
