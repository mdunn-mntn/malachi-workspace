-- TI-837 v5: Multi-segment ghost-bidding lift analysis
-- ----------------------------------------------------------------
-- Single megaquery producing per-cell ATT data for 4 campaign segments:
--   1. all      — all impressions (no objective filter)
--   2. prosp    — prospecting all stages (objective_id IN 1, 5, 6)
--   3. stage1   — Stage 1 only (objective_id IN 1, 5, 6 AND funnel_level = 1)
--   4. rtg      — retargeting only (objective_id = 4)
--
-- Same hash, same biddable holdout pool (per advertiser), same window. Each
-- segment has its own:
--   - served_treatment definition (filtered cost_impression)
--   - clickpass_visits (filtered same as cost_impression)
--   - biddable_holdouts subsample (at segment-specific win_rate)
--
-- guid_log is segment-agnostic — visits are visits regardless of campaign.
--
-- ADVERTISERS  = 30 advertisers (Phase 2 cohort, 2026-04-27)
-- ANALYSIS_WIN = 2026-04-20 00:00:00 UTC → 2026-04-27 00:00:00 UTC
-- VISIT_WIN    = 2026-04-20 00:00:00 UTC → 2026-04-29 00:00:00 UTC (+3d)
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
-- Per-advertiser per-segment empirical win_rates
-- (computed from upstream query 2026-04-28 — biddable_holdouts × 9 hash-symmetry)
win_rates AS (
  SELECT * FROM UNNEST([
    STRUCT(30181 AS advertiser_id, 0.031983 AS wr_all, 0.028779 AS wr_prosp, 0.023709 AS wr_stage1, 0.003282 AS wr_rtg),
    STRUCT(30392 AS advertiser_id, 0.014309 AS wr_all, 0.010374 AS wr_prosp, 0.008216 AS wr_stage1, 0.003969 AS wr_rtg),
    STRUCT(30496 AS advertiser_id, 0.009985 AS wr_all, 0.009095 AS wr_prosp, 0.006968 AS wr_stage1, 0.001021 AS wr_rtg),
    STRUCT(31276 AS advertiser_id, 0.093003 AS wr_all, 0.062059 AS wr_prosp, 0.042767 AS wr_stage1, 0.032330 AS wr_rtg),
    STRUCT(31297 AS advertiser_id, 0.006001 AS wr_all, 0.006001 AS wr_prosp, 0.004032 AS wr_stage1, 0.000000 AS wr_rtg),
    STRUCT(31455 AS advertiser_id, 0.131959 AS wr_all, 0.125461 AS wr_prosp, 0.072254 AS wr_stage1, 0.006904 AS wr_rtg),
    STRUCT(31464 AS advertiser_id, 0.011672 AS wr_all, 0.010203 AS wr_prosp, 0.007859 AS wr_stage1, 0.001615 AS wr_rtg),
    STRUCT(32244 AS advertiser_id, 0.006241 AS wr_all, 0.004415 AS wr_prosp, 0.003107 AS wr_stage1, 0.001840 AS wr_rtg),
    STRUCT(32320 AS advertiser_id, 0.027982 AS wr_all, 0.027176 AS wr_prosp, 0.016939 AS wr_stage1, 0.000997 AS wr_rtg),
    STRUCT(32404 AS advertiser_id, 0.027089 AS wr_all, 0.022260 AS wr_prosp, 0.021229 AS wr_stage1, 0.004852 AS wr_rtg),
    STRUCT(32527 AS advertiser_id, 0.009047 AS wr_all, 0.008352 AS wr_prosp, 0.006301 AS wr_stage1, 0.000787 AS wr_rtg),
    STRUCT(32899 AS advertiser_id, 0.011530 AS wr_all, 0.008437 AS wr_prosp, 0.007143 AS wr_stage1, 0.003233 AS wr_rtg),
    STRUCT(33467 AS advertiser_id, 0.008384 AS wr_all, 0.008384 AS wr_prosp, 0.006993 AS wr_stage1, 0.000000 AS wr_rtg),
    STRUCT(33572 AS advertiser_id, 0.031206 AS wr_all, 0.030331 AS wr_prosp, 0.019327 AS wr_stage1, 0.001198 AS wr_rtg),
    STRUCT(33684 AS advertiser_id, 0.007191 AS wr_all, 0.006614 AS wr_prosp, 0.006026 AS wr_stage1, 0.000595 AS wr_rtg),
    STRUCT(34141 AS advertiser_id, 0.002632 AS wr_all, 0.002632 AS wr_prosp, 0.002282 AS wr_stage1, 0.000000 AS wr_rtg),
    STRUCT(34365 AS advertiser_id, 0.001370 AS wr_all, 0.001181 AS wr_prosp, 0.001069 AS wr_stage1, 0.000189 AS wr_rtg),
    STRUCT(34862 AS advertiser_id, 0.009768 AS wr_all, 0.008211 AS wr_prosp, 0.005423 AS wr_stage1, 0.001851 AS wr_rtg),
    STRUCT(35086 AS advertiser_id, 0.012843 AS wr_all, 0.010637 AS wr_prosp, 0.009290 AS wr_stage1, 0.002387 AS wr_rtg),
    STRUCT(35374 AS advertiser_id, 0.006408 AS wr_all, 0.005573 AS wr_prosp, 0.004279 AS wr_stage1, 0.000841 AS wr_rtg),
    STRUCT(35573 AS advertiser_id, 0.017137 AS wr_all, 0.017137 AS wr_prosp, 0.009883 AS wr_stage1, 0.000000 AS wr_rtg),
    STRUCT(37222 AS advertiser_id, 0.002945 AS wr_all, 0.002945 AS wr_prosp, 0.002246 AS wr_stage1, 0.000000 AS wr_rtg),
    STRUCT(37796 AS advertiser_id, 0.004111 AS wr_all, 0.003480 AS wr_prosp, 0.002354 AS wr_stage1, 0.000714 AS wr_rtg),
    STRUCT(38307 AS advertiser_id, 0.004045 AS wr_all, 0.004045 AS wr_prosp, 0.002584 AS wr_stage1, 0.000000 AS wr_rtg),
    STRUCT(38422 AS advertiser_id, 0.062331 AS wr_all, 0.051848 AS wr_prosp, 0.036034 AS wr_stage1, 0.010877 AS wr_rtg),
    STRUCT(42097 AS advertiser_id, 0.074984 AS wr_all, 0.070751 AS wr_prosp, 0.046942 AS wr_stage1, 0.004405 AS wr_rtg),
    STRUCT(43996 AS advertiser_id, 0.010285 AS wr_all, 0.009821 AS wr_prosp, 0.006756 AS wr_stage1, 0.000534 AS wr_rtg),
    STRUCT(46426 AS advertiser_id, 0.005455 AS wr_all, 0.004446 AS wr_prosp, 0.003020 AS wr_stage1, 0.001043 AS wr_rtg),
    STRUCT(50525 AS advertiser_id, 0.000822 AS wr_all, 0.000822 AS wr_prosp, 0.000604 AS wr_stage1, 0.000000 AS wr_rtg),
    STRUCT(56187 AS advertiser_id, 0.003987 AS wr_all, 0.003817 AS wr_prosp, 0.002577 AS wr_stage1, 0.000194 AS wr_rtg)
  ])
),

-- Campaign dim with objective + funnel_level for filtering
campaign_dim AS (
  SELECT campaign_id, advertiser_id, objective_id, funnel_level
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
    AND advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
),

-- Step 1: prospecting universe + holdout assignment (same as v4)
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
    AND year  = '2026' AND month = '04'
    AND day IN ('20','21','22','23','24','25','26')
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
ip_max_score AS (
  SELECT advertiser_id, ip, MAX(household_score) AS max_household_score
  FROM prospecting
  GROUP BY advertiser_id, ip
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
    -- Pre-compute fresh subsample hash bucket (independent of holdout assignment)
    MOD(
      ABS(FARM_FINGERPRINT(CONCAT(CAST(s.advertiser_id AS STRING), ':wr:', s.ip))),
      100000
    ) AS wr_bucket
  FROM ip_max_score s
),
holdouts AS (
  SELECT advertiser_id, ip, intent_tier, wr_bucket
  FROM ip_assigned WHERE bucket BETWEEN 0 AND 99
),
targeted AS (
  SELECT advertiser_id, ip, intent_tier
  FROM ip_assigned WHERE bucket BETWEEN 100 AND 999
),

-- Step 2: augmentor scan (segment-agnostic; single pass)
augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- biddable_holdouts: holdouts ∩ augmentor, plus the wr_bucket and per-segment win_rates
-- joined for downstream segment filtering
biddable_holdouts_full AS (
  SELECT
    h.advertiser_id, h.ip, h.intent_tier, h.wr_bucket,
    wr.wr_all, wr.wr_prosp, wr.wr_stage1, wr.wr_rtg
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
  INNER JOIN win_rates wr USING (advertiser_id)
),

-- Step 3: cost_impression scan ONCE with campaign attributes attached
cost_imp_pairs AS (
  SELECT DISTINCT
    CAST(ci.advertiser_id AS INT64) AS advertiser_id,
    ci.ip,
    c.objective_id, c.funnel_level
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN campaign_dim c ON ci.campaign_id = c.campaign_id
  WHERE DATE(ci.time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(ci.time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ci.advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND ci.ip IS NOT NULL AND ci.ip != '0.0.0.0'
),

-- Per-segment served_treatment
served_treatment_all AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip)
),
served_treatment_prosp AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip)
  WHERE c.objective_id IN (1, 5, 6)
),
served_treatment_stage1 AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip)
  WHERE c.objective_id IN (1, 5, 6) AND c.funnel_level = 1
),
served_treatment_rtg AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip)
  WHERE c.objective_id = 4
),

-- Step 4: clickpass scan ONCE with campaign attributes
cp_pairs AS (
  SELECT DISTINCT
    CAST(cp.advertiser_id AS INT64) AS advertiser_id,
    cp.ip,
    c.objective_id, c.funnel_level
  FROM `dw-main-silver.logdata.clickpass_log` cp
  INNER JOIN campaign_dim c ON cp.campaign_id = c.campaign_id
  WHERE DATE(cp.time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(cp.time) <  DATE(TIMESTAMP('2026-04-30 00:00:00 UTC'))
    AND cp.advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND cp.ip IS NOT NULL AND cp.ip != '0.0.0.0'
),

-- Per-segment clickpass visits
clickpass_all AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs),
clickpass_prosp AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id IN (1, 5, 6)),
clickpass_stage1 AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id IN (1, 5, 6) AND funnel_level = 1),
clickpass_rtg AS (SELECT DISTINCT advertiser_id, ip FROM cp_pairs WHERE objective_id = 4),

-- guid_visits — segment-agnostic
guid_visits AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-30 00:00:00 UTC'))
    AND advertiser_id IN (30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Per-segment biddable holdouts (subsampled at segment-specific win_rate)
bh_all AS (
  SELECT advertiser_id, ip, intent_tier
  FROM biddable_holdouts_full
  WHERE wr_all > 0 AND wr_bucket < CAST(wr_all * 100000 AS INT64)
),
bh_prosp AS (
  SELECT advertiser_id, ip, intent_tier
  FROM biddable_holdouts_full
  WHERE wr_prosp > 0 AND wr_bucket < CAST(wr_prosp * 100000 AS INT64)
),
bh_stage1 AS (
  SELECT advertiser_id, ip, intent_tier
  FROM biddable_holdouts_full
  WHERE wr_stage1 > 0 AND wr_bucket < CAST(wr_stage1 * 100000 AS INT64)
),
bh_rtg AS (
  SELECT advertiser_id, ip, intent_tier
  FROM biddable_holdouts_full
  WHERE wr_rtg > 0 AND wr_bucket < CAST(wr_rtg * 100000 AS INT64)
),

-- 4 union-combined "subjects" tables, one per segment
all_subjects AS (
  SELECT 'all' AS segment, 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM bh_all
  UNION ALL
  SELECT 'all' AS segment, 'treated_served' AS group_name, advertiser_id, ip, intent_tier FROM served_treatment_all
  UNION ALL
  SELECT 'prosp' AS segment, 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM bh_prosp
  UNION ALL
  SELECT 'prosp' AS segment, 'treated_served' AS group_name, advertiser_id, ip, intent_tier FROM served_treatment_prosp
  UNION ALL
  SELECT 'stage1' AS segment, 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM bh_stage1
  UNION ALL
  SELECT 'stage1' AS segment, 'treated_served' AS group_name, advertiser_id, ip, intent_tier FROM served_treatment_stage1
  UNION ALL
  SELECT 'rtg' AS segment, 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM bh_rtg
  UNION ALL
  SELECT 'rtg' AS segment, 'treated_served' AS group_name, advertiser_id, ip, intent_tier FROM served_treatment_rtg
),

-- Clickpass with segment label (UNION ALL the 4 per-segment tables)
clickpass_with_segment AS (
  SELECT 'all'    AS segment, advertiser_id, ip FROM clickpass_all
  UNION ALL
  SELECT 'prosp'  AS segment, advertiser_id, ip FROM clickpass_prosp
  UNION ALL
  SELECT 'stage1' AS segment, advertiser_id, ip FROM clickpass_stage1
  UNION ALL
  SELECT 'rtg'    AS segment, advertiser_id, ip FROM clickpass_rtg
)

SELECT
  s.segment,
  s.advertiser_id,
  s.group_name,
  s.intent_tier,
  COUNT(DISTINCT s.ip)                                                 AS n_ips,
  COUNT(DISTINCT cv.ip)                                                AS clickpass_visitors,
  COUNT(DISTINCT gv.ip)                                                AS guid_visitors,
  SAFE_DIVIDE(COUNT(DISTINCT cv.ip), COUNT(DISTINCT s.ip))             AS clickpass_visit_rate,
  SAFE_DIVIDE(COUNT(DISTINCT gv.ip), COUNT(DISTINCT s.ip))             AS guid_visit_rate
FROM all_subjects s
LEFT JOIN clickpass_with_segment cv
  ON cv.segment = s.segment AND cv.advertiser_id = s.advertiser_id AND cv.ip = s.ip
LEFT JOIN guid_visits gv
  ON gv.advertiser_id = s.advertiser_id AND gv.ip = s.ip
GROUP BY s.segment, s.advertiser_id, s.group_name, s.intent_tier
ORDER BY s.segment, s.advertiser_id, s.group_name, s.intent_tier;
