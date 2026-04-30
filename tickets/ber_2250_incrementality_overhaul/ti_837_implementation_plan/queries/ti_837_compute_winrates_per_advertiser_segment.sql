-- TI-837: Upstream computation of per-(advertiser, segment) win_rates
-- ----------------------------------------------------------------
-- Computes the win_rates that v5 hardcoded as a STRUCT literal in
-- queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql.
--
-- For Alex Knorr's question (2026-04-29): "Can you share the code how you got
-- to ~1% win rate? I think this is a really important part so lets validate /
-- double check it."
--
-- Formula (per advertiser, per segment):
--
--     win_rate = served_treatment_n_unique_ips
--                / (biddable_holdouts_n_unique_ips × 9)
--
-- Why × 9:
--     The hash partitions IPs as 90% targeted (buckets 100-999) and 10%
--     holdout (buckets 0-99). For any uniform per-IP filter applied
--     symmetrically (e.g., "appears in augmentor_log") the targeted-side
--     count = 9 × holdout-side count by hash symmetry. So:
--
--         biddable_targeted_estimate ≈ biddable_holdouts × 9
--         served_targeted = COUNT_DISTINCT(targeted ∩ cost_impression_log)
--         win_rate = served_targeted / biddable_targeted_estimate
--
-- This is the per-segment denominator-matching ratio used to subsample
-- biddable_holdouts so the comparison-arm denominator matches the treated-arm
-- "actually-served" condition.
--
-- Per-advertiser median win_rates from v5 (04-20 → 04-26):
--     wr_all    : ~1.0%
--     wr_prosp  : ~0.84%
--     wr_stage1 : ~0.69%
--     wr_rtg    : ~0.10%
--
-- Cohort range:
--     Lowest:  advertiser 50525 — ~0.08% (small advertiser, low served volume)
--     Highest: Ancient Nutrition (31455) — ~13.2% on wr_all
--
-- Adjustable parameters at the top of the query:
--   - ADVERTISERS    : 30-advertiser v5 cohort (or a subset)
--   - DT_START / END : window dates (impression window only)
-- ----------------------------------------------------------------

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
v5_cohort AS (
  SELECT advertiser_id FROM UNNEST([
    30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404,
    32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374,
    35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187
  ]) AS advertiser_id
),

campaign_dim AS (
  SELECT campaign_id, advertiser_id, objective_id, funnel_level
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
    AND advertiser_id IN (SELECT advertiser_id FROM v5_cohort)
),

-- Step 1: prospecting universe (one row per (advertiser, IP) over the window)
prospecting AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE CAST(advertiser_id AS INT64) IN (SELECT advertiser_id FROM v5_cohort)
    AND year  = '2026' AND month = '04'
    AND day IN ('20','21','22','23','24','25','26')
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

ip_assigned AS (
  SELECT
    advertiser_id, ip,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip)))) AS bucket
  FROM prospecting
),

holdouts AS (SELECT advertiser_id, ip FROM ip_assigned WHERE bucket BETWEEN 0 AND 99),
targeted AS (SELECT advertiser_id, ip FROM ip_assigned WHERE bucket BETWEEN 100 AND 999),

-- Step 2: biddable — any augmentor appearance in the window
augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

biddable_holdouts AS (
  SELECT h.advertiser_id, h.ip
  FROM holdouts h INNER JOIN augmentor_ips a USING (ip)
),

-- Step 3: cost_impression with campaign attributes for segment filtering
cost_imp_pairs AS (
  SELECT DISTINCT
    CAST(ci.advertiser_id AS INT64) AS advertiser_id,
    ci.ip,
    c.objective_id, c.funnel_level
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN campaign_dim c ON ci.campaign_id = c.campaign_id
  WHERE DATE(ci.time) >= DATE(TIMESTAMP('2026-04-20 00:00:00 UTC'))
    AND DATE(ci.time) <  DATE(TIMESTAMP('2026-04-27 00:00:00 UTC'))
    AND ci.advertiser_id IN (SELECT advertiser_id FROM v5_cohort)
    AND ci.ip IS NOT NULL AND ci.ip != '0.0.0.0'
),

-- Per-(advertiser, segment) served counts
served_counts AS (
  SELECT advertiser_id, 'all' AS segment, COUNT(DISTINCT ip) AS served_n
  FROM (SELECT DISTINCT t.advertiser_id, t.ip FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip))
  GROUP BY advertiser_id
  UNION ALL
  SELECT advertiser_id, 'prosp' AS segment, COUNT(DISTINCT ip) AS served_n
  FROM (SELECT DISTINCT t.advertiser_id, t.ip FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id IN (1, 5, 6))
  GROUP BY advertiser_id
  UNION ALL
  SELECT advertiser_id, 'stage1' AS segment, COUNT(DISTINCT ip) AS served_n
  FROM (SELECT DISTINCT t.advertiser_id, t.ip FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id IN (1, 5, 6) AND c.funnel_level = 1)
  GROUP BY advertiser_id
  UNION ALL
  SELECT advertiser_id, 'rtg' AS segment, COUNT(DISTINCT ip) AS served_n
  FROM (SELECT DISTINCT t.advertiser_id, t.ip FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip) WHERE c.objective_id = 4)
  GROUP BY advertiser_id
),

-- Per-advertiser biddable_holdouts count (segment-agnostic — augmentor is too)
biddable_holdouts_n AS (
  SELECT advertiser_id, COUNT(DISTINCT ip) AS biddable_holdouts_n
  FROM biddable_holdouts
  GROUP BY advertiser_id
)

SELECT
  s.advertiser_id,
  s.segment,
  s.served_n,
  b.biddable_holdouts_n,
  b.biddable_holdouts_n * 9 AS biddable_targeted_estimate,
  ROUND(SAFE_DIVIDE(s.served_n, b.biddable_holdouts_n * 9), 6) AS win_rate
FROM served_counts s
INNER JOIN biddable_holdouts_n b USING (advertiser_id)
ORDER BY s.advertiser_id, s.segment;
