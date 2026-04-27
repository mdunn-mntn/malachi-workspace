-- TI-837: Ad-hoc Ghost-Bidding Lift Analysis
-- ----------------------------------------------------------------
-- Computes Average Treatment on the Treated (ATT) for one advertiser using:
--   - prospecting_intent (Parquet, household-scoring-prod) → targetable IP universe
--   - augmentor_log → biddability proof for holdouts
--   - cost_impression_log → actually-served (treatment) IPs
--   - clickpass_log + guid_log → visit outcomes
--
-- Methodology corrections (2026-04-22 meeting):
--   - Holdout IPs DO appear in augmentor_log, but mntn_segments does NOT
--     contain the segment they're a holdout of. The targetable audience must
--     therefore be reconstructed externally — here we use prospecting_intent.
--   - AID 90 (MNTN PSA advertiser) is intentionally served to holdouts;
--     this query targets a single non-PSA advertiser so the rule is implicit.
--
-- Holdout assignment (per-advertiser per-IP):
--   bucket = uint64(MD5('{AID}:{IP}')[0:16]) mod 1000
--   bucket 0-99   = holdout (10%)
--   bucket 100-999 = targeted (90%)
--
-- Parameters (current run):
--   ADVERTISER_ID = 37775 (Zazzle — TI-835 signal + 12 campaigns + 74M IPs in prospecting feed)
--   WINDOW_START  = 2026-04-24 00:00:00 UTC (1-day smoke test window)
--   WINDOW_END    = 2026-04-25 00:00:00 UTC
--   PROSPECTING table = dw-main-bronze.external.household_scoring__prospecting_intent__v1
--     filter: year='2026' AND month='04' AND day = '24'
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
-- Step 1: targetable IP universe from prospecting_intent (one or more daily files)
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
  WHERE CAST(advertiser_id AS INT64) = 37775
    AND year  = '2026'
    AND month = '04'
    AND day = '24'
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Step 2: assign holdout vs targeted
hashed AS (
  SELECT
    p.*,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(p.advertiser_id AS STRING), ':', p.ip)))) AS bucket
  FROM prospecting p
),

-- Step 3a: candidate holdouts (bucket 0-99)
holdouts AS (
  SELECT ip, intent_tier, household_score
  FROM hashed
  WHERE bucket BETWEEN 0 AND 99
),

-- Step 3b: target side (bucket 100-999)
targeted AS (
  SELECT ip, intent_tier, household_score
  FROM hashed
  WHERE bucket BETWEEN 100 AND 999
),

-- Step 4: holdout IPs that actually appeared in augmentor_log during window → biddable holdouts
biddable_holdouts AS (
  SELECT DISTINCT h.ip, h.intent_tier
  FROM holdouts h
  INNER JOIN (
    SELECT DISTINCT ip
    FROM `dw-main-bronze.raw.augmentor_log`
    WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-24 00:00:00 UTC'))
      AND DATE(time) <  DATE(TIMESTAMP('2026-04-25 00:00:00 UTC'))
      AND ip IS NOT NULL AND ip != '0.0.0.0'
  ) a USING (ip)
),

-- Step 5: served treatment IPs from cost_impression_log
served_treatment AS (
  SELECT DISTINCT t.ip, t.intent_tier
  FROM targeted t
  INNER JOIN (
    SELECT DISTINCT ip
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-24 00:00:00 UTC'))
      AND DATE(time) <  DATE(TIMESTAMP('2026-04-25 00:00:00 UTC'))
      AND advertiser_id = 37775
      AND ip IS NOT NULL AND ip != '0.0.0.0'
  ) c USING (ip)
),

-- Step 6: visit outcomes
clickpass_visits AS (
  SELECT DISTINCT ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-24 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-25 00:00:00 UTC'))
    AND advertiser_id = 37775
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
guid_visits AS (
  SELECT DISTINCT ip
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-24 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-25 00:00:00 UTC'))
    AND advertiser_id = 37775
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Step 7: subjects in each group
subjects AS (
  SELECT 'holdout_biddable' AS group_name, ip, intent_tier FROM biddable_holdouts
  UNION ALL
  SELECT 'treated_served'   AS group_name, ip, intent_tier FROM served_treatment
)

-- Step 8: aggregate visit rates per group × intent_tier
SELECT
  s.group_name,
  s.intent_tier,
  COUNT(DISTINCT s.ip) AS n_ips,
  COUNT(DISTINCT cv.ip) AS clickpass_visitors,
  COUNT(DISTINCT gv.ip) AS guid_visitors,
  SAFE_DIVIDE(COUNT(DISTINCT cv.ip), COUNT(DISTINCT s.ip)) AS clickpass_visit_rate,
  SAFE_DIVIDE(COUNT(DISTINCT gv.ip), COUNT(DISTINCT s.ip)) AS guid_visit_rate
FROM subjects s
LEFT JOIN clickpass_visits cv ON s.ip = cv.ip
LEFT JOIN guid_visits gv ON s.ip = gv.ip
GROUP BY s.group_name, s.intent_tier
ORDER BY s.group_name, s.intent_tier;

-- Notes:
--   - Coverage rates per intent_tier give us the served fraction (analogous to TI-835 14-16%).
--   - Raw ATT per intent_tier = visit_rate(treated) - visit_rate(holdout_biddable).
--   - Two-proportion z-test on each cell for significance.
--   - Propensity matching at intent_tier level is implicit in this stratified output —
--     compute weighted ATT externally using treated counts as weights.
