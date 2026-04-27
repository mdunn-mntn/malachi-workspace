-- TI-837: Multi-Advertiser Ghost-Bidding Lift Analysis
-- ----------------------------------------------------------------
-- Computes Average Treatment on the Treated (ATT) for 7 advertisers in a
-- single batched query, exploiting that augmentor_log is advertiser-agnostic
-- (one full scan amortizes across all 7 advertisers).
--
-- Pipeline (per advertiser):
--   prospecting_intent → targetable IP universe (per advertiser)
--   augmentor_log      → biddability proof for holdouts (advertiser-agnostic)
--   cost_impression_log→ actually-served (treatment) IPs (per advertiser)
--   clickpass_log      → clickpass visit outcomes (per advertiser)
--   guid_log           → guid visit outcomes (per advertiser)
--
-- Methodology corrections (2026-04-22 meeting):
--   - Holdout IPs DO appear in augmentor_log, but mntn_segments does NOT
--     contain the segment they're a holdout of. Targetable audience is
--     reconstructed externally via prospecting_intent.
--   - AID 90 (PSA) is intentionally served to holdouts; none of the 7
--     advertisers below is AID 90, so the rule is implicit.
--
-- Holdout assignment (per-advertiser per-IP):
--   bucket = uint64(MD5('{AID}:{IP}')[0:16]) mod 1000
--   bucket 0-99   = holdout (10%)
--   bucket 100-999 = targeted (90%)
--
-- Parameters (Stage 1 — 1-day smoke):
--   ADVERTISERS  = 31276 Ferguson, 31455 Ancient Nutrition, 34143 First Watch,
--                  34611 HexClad, 34838 Clayton Homes, 37775 Zazzle,
--                  40563 Northern Tool
--   WINDOW_START = 2026-04-23 00:00:00 UTC
--   WINDOW_END   = 2026-04-24 00:00:00 UTC  (analysis & visit window co-extensive
--                                            for Stage 1 — 3-day post-period
--                                            applies in Stage 2 only)
--   PROSPECTING partition: year='2026' AND month='04' AND day='23'
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
-- Step 1: targetable IP universe from prospecting_intent (per advertiser)
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
  WHERE CAST(advertiser_id AS INT64) IN (31276, 31455, 34143, 34611, 34838, 37775, 40563)
    AND year  = '2026'
    AND month = '04'
    AND day = '23'
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Step 2: assign holdout vs targeted (deterministic per (advertiser, ip))
hashed AS (
  SELECT
    p.*,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(p.advertiser_id AS STRING), ':', p.ip)))) AS bucket
  FROM prospecting p
),

-- Step 3a: candidate holdouts (bucket 0-99)
holdouts AS (
  SELECT advertiser_id, ip, intent_tier, household_score
  FROM hashed
  WHERE bucket BETWEEN 0 AND 99
),

-- Step 3b: target side (bucket 100-999)
targeted AS (
  SELECT advertiser_id, ip, intent_tier, household_score
  FROM hashed
  WHERE bucket BETWEEN 100 AND 999
),

-- Step 4: augmentor_log scan (advertiser-agnostic — one scan amortized across all 7)
augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-23 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-24 00:00:00 UTC'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Step 4b: biddable holdouts (per-advertiser holdouts that appeared in augmentor)
biddable_holdouts AS (
  SELECT DISTINCT h.advertiser_id, h.ip, h.intent_tier
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
),

-- Step 5: served treatment IPs from cost_impression_log (per advertiser)
cost_imp_advertiser_ips AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-23 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-24 00:00:00 UTC'))
    AND advertiser_id IN (31276, 31455, 34143, 34611, 34838, 37775, 40563)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
served_treatment AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t
  INNER JOIN cost_imp_advertiser_ips c
    USING (advertiser_id, ip)
),

-- Step 6: visit outcomes (per advertiser)
clickpass_visits AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-23 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-24 00:00:00 UTC'))
    AND advertiser_id IN (31276, 31455, 34143, 34611, 34838, 37775, 40563)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
guid_visits AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-23 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-24 00:00:00 UTC'))
    AND advertiser_id IN (31276, 31455, 34143, 34611, 34838, 37775, 40563)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Step 7: subjects in each group (advertiser-keyed)
subjects AS (
  SELECT 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM biddable_holdouts
  UNION ALL
  SELECT 'treated_served'   AS group_name, advertiser_id, ip, intent_tier FROM served_treatment
)

-- Step 8: aggregate visit rates per advertiser × group × intent_tier
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
