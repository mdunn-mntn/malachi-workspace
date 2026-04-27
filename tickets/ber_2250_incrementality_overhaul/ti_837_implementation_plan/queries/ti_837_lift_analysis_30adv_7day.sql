-- TI-837: Multi-Advertiser Ghost-Bidding Lift Analysis — 7-day primary
-- ----------------------------------------------------------------
-- Stage 2 of the execution ladder. Same pipeline as the 1-day smoke
-- (ti_837_lift_analysis_30adv.sql) but:
--   - Analysis window: 2026-04-20 → 2026-04-26 UTC (7 days, inclusive/exclusive)
--   - Visit observation window: 2026-04-20 → 2026-04-29 UTC (3-day post-period
--     for cross-day attribution; treatment-side and biddable-holdout side both
--     get the same observation window so the comparison stays apples-to-apples)
--   - Prospecting partitions: day IN ('20','21','22','23','24','25','26')
--     DISTINCT (advertiser_id, ip, household_score) across the union — an IP
--     that scored on day 22 but not day 23 still belongs in the targetable
--     universe for the full week.
--
-- Window-strategy rationale (full discussion in execution_plan.md §2):
--   Treatment-side (cost_impression_log) is filtered to the analysis window
--   so we only count IPs the advertiser actually served during the analysis
--   week. Visit-side (clickpass + guid) extends 3 days past the analysis
--   window so a Day-26 impression's Day-29 visit is captured. Augmentor scan
--   uses the analysis window only — it's the biddability proof, not a visit.
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

-- Step 2: assign holdout vs targeted (deterministic per (advertiser, ip))
hashed AS (
  SELECT
    p.*,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(p.advertiser_id AS STRING), ':', p.ip)))) AS bucket
  FROM prospecting p
),

-- Note: an IP can appear at multiple intent_tiers across the 7-day window if
-- its household_score changed. Take the MAX score per (advertiser, ip) so
-- each subject is assigned a single tier (the strongest observed intent).
-- max(household_score) → max → tier mapping retained downstream via JOIN.
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

-- Step 4: augmentor scan over the analysis window (advertiser-agnostic)
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

-- Step 5: served treatment IPs (analysis window only — visits later)
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

-- Step 6: visit outcomes (analysis window + 3-day post-period)
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
