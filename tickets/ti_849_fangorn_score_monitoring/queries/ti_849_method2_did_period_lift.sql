/* DEPRECATED 2026-05-01 — augmentor_log scan is too expensive (TB-scale)
   and not feasible to re-run daily. Replaced by Method 3 (CausalImpact
   with platform covariates, TI-748 pattern). Keeping file as
   methodology trail; do NOT run.

   ========================================================================
   TI-849 Method 2 (Within-AID DiD) — period-lift building block

   Adapted from TI-837 v3 (`ti_837_lift_analysis_30adv_7day_v3.sql`).
   Computes the holdout-vs-targeted visit rate gap for ONE period window.
   Run twice — pre-Fangorn (DS13 era) and post-Fangorn (DS46 era) — then
   take the ratio of post_gap / pre_gap as the within-AID DiD effect.

   Cohort definitions (per-AID per-IP, deterministic):
     bucket = MD5('{AID}:{IP}') % 1000
     - bucket 0-99    → holdout cohort (would not have been targeted)
     - bucket 100-999 → targeted cohort (eligible for serving)

   Win-rate correction (TI-837 Phase 2 methodology, Alex Knorr 2026-04-28):
     Subsample biddable_holdouts at per-AID empirical win_rate so the
     denominator matches treatment's "actually-served" condition.
     Win_rate = served_treatment / (biddable_targeted = biddable_holdouts × 9).

   Outputs visit rates from BOTH guid_log (cause-agnostic site visits) and
   clickpass_log (MNTN-attributed visits) — the "two stories" must both
   be reported per the TI-835 finding.

   Date and AID parameters at the top — change before each run.
   ======================================================================== */

-- Period window (inclusive start, exclusive end)
DECLARE period_start TIMESTAMP DEFAULT TIMESTAMP '2026-04-20 00:00:00 UTC';
DECLARE period_end   TIMESTAMP DEFAULT TIMESTAMP '2026-04-30 00:00:00 UTC';
-- Visit window can be wider than the period (catch attribution-window visits)
DECLARE visit_end    TIMESTAMP DEFAULT TIMESTAMP '2026-05-01 00:00:00 UTC';
-- Target advertisers — Tier 1 launch (auto-detect via vertical_data_source = 46
-- once 49 more land Monday 2026-05-04)
DECLARE target_aids ARRAY<INT64> DEFAULT [32320, 38659, 32233];

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS '''
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt('0x' + hex16);
  return Number(val % BigInt(1000));
''';

WITH
-- Per-AID empirical win rates (computed offline from TI-837 v1; for any AID
-- not in this table, default to median 0.010). Update when 49 new AIDs land.
win_rates AS (
  SELECT * FROM UNNEST([
    STRUCT(32320 AS advertiser_id, 0.023215 AS win_rate),  -- Biz2Credit (TI-837 v3)
    STRUCT(38659 AS advertiser_id, 0.010    AS win_rate),  -- Big Blue Bubble (default)
    STRUCT(32233 AS advertiser_id, 0.010    AS win_rate)   -- UNW Ohio (default)
  ])
),

prospecting_campaigns AS (
  SELECT campaign_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
    AND objective_id IN (1, 5, 6)
    AND advertiser_id IN UNNEST(target_aids)
),

-- Build prospecting universe by scanning daily partitions in the period
prospecting AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip,
    CAST(household_score AS INT64) AS household_score
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE CAST(advertiser_id AS INT64) IN UNNEST(target_aids)
    AND PARSE_DATE('%Y-%m-%d', CONCAT(year, '-', month, '-', day))
        BETWEEN DATE(period_start) AND DATE(TIMESTAMP_SUB(period_end, INTERVAL 1 DAY))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

ip_max_score AS (
  SELECT advertiser_id, ip, MAX(household_score) AS max_household_score
  FROM prospecting
  GROUP BY advertiser_id, ip
),

ip_assigned AS (
  SELECT
    advertiser_id, ip, max_household_score,
    CASE
      WHEN max_household_score = 10000                THEN 'high'
      WHEN max_household_score BETWEEN 7000 AND 9999  THEN 'peak'
      WHEN max_household_score BETWEEN 3333 AND 6999  THEN 'mid'
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

-- Augmentor scan establishes "biddable" (IP appeared in bid stream)
augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE time >= period_start
    AND time <  period_end
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Win-rate-corrected biddable holdouts (matches treatment's served condition)
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

-- Treatment side: IPs actually served prospecting impressions in the period
cost_imp_aids AS (
  SELECT DISTINCT ci.advertiser_id, ci.ip
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN prospecting_campaigns pc
    ON ci.campaign_id = pc.campaign_id
  WHERE ci.time >= period_start AND ci.time < period_end
    AND ci.advertiser_id IN UNNEST(target_aids)
    AND ci.ip IS NOT NULL AND ci.ip != '0.0.0.0'
),
served_treatment AS (
  SELECT DISTINCT t.advertiser_id, t.ip, t.intent_tier
  FROM targeted t
  INNER JOIN cost_imp_aids c USING (advertiser_id, ip)
),

-- MNTN-attributed visits (clickpass_log)
clickpass_visits AS (
  SELECT DISTINCT cp.advertiser_id, cp.ip
  FROM `dw-main-silver.logdata.clickpass_log` cp
  INNER JOIN prospecting_campaigns pc
    ON cp.campaign_id = pc.campaign_id
  WHERE cp.time >= period_start AND cp.time < visit_end
    AND cp.advertiser_id IN UNNEST(target_aids)
    AND cp.ip IS NOT NULL AND cp.ip != '0.0.0.0'
),

-- Cause-agnostic site visits (guid_log) — the honest VR signal
guid_visits AS (
  SELECT DISTINCT advertiser_id, ip
  FROM `dw-main-silver.logdata.guid_log`
  WHERE time >= period_start AND time < visit_end
    AND advertiser_id IN UNNEST(target_aids)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

subjects AS (
  SELECT 'holdout_biddable' AS group_name, advertiser_id, ip, intent_tier FROM biddable_holdouts
  UNION ALL
  SELECT 'treated_served'    AS group_name, advertiser_id, ip, intent_tier FROM served_treatment
)

SELECT
  FORMAT_TIMESTAMP('%Y-%m-%d', period_start) AS period_start,
  FORMAT_TIMESTAMP('%Y-%m-%d', period_end)   AS period_end,
  s.advertiser_id,
  s.group_name,
  s.intent_tier,
  COUNT(DISTINCT s.ip) AS n_ips,
  COUNT(DISTINCT cv.ip) AS clickpass_visitors,
  COUNT(DISTINCT gv.ip) AS guid_visitors,
  SAFE_DIVIDE(COUNT(DISTINCT cv.ip), COUNT(DISTINCT s.ip)) AS clickpass_vr,
  SAFE_DIVIDE(COUNT(DISTINCT gv.ip), COUNT(DISTINCT s.ip)) AS guid_vr
FROM subjects s
LEFT JOIN clickpass_visits cv
  ON s.ip = cv.ip AND s.advertiser_id = cv.advertiser_id
LEFT JOIN guid_visits gv
  ON s.ip = gv.ip AND s.advertiser_id = gv.advertiser_id
GROUP BY s.advertiser_id, s.group_name, s.intent_tier
ORDER BY s.advertiser_id, s.group_name, s.intent_tier;
