-- TI-837 Phase 2 cohort selection — Stage A.1
-- Universe size + per-advertiser tier distribution + score variance
-- ----------------------------------------------------------------
-- Window: 2026-04-20 → 2026-04-26 UTC (matches Phase 1).
-- Source: dw-main-bronze.external.household_scoring__prospecting_intent__v1
--
-- Output: one row per advertiser_id with:
--   - distinct IP count over the week
--   - distinct (IP, day) count
--   - per-tier IP counts under MAX(household_score) construction (the same
--     subject construction Phase 1 used)
--   - per-IP score variance across the week (signal of tier diversity vs.
--     stuck-at-10000 collapse)
--   - count of IPs whose score is constant across all observed days
--     (collapse signature)
--   - count of IPs that show ≥1 day at high (10000), ≥1 day at peak
--     (7000-9999), ≥1 day at mid (3333-6999) — the "tier-diverse" subset
--
-- Tier boundaries match the Phase 1 SQL exactly:
--   high      : 10000
--   peak      : 7000-9999
--   mid       : 3333-6999
--   max_reach : <3333
-- ----------------------------------------------------------------

WITH
prospecting AS (
  SELECT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip,
    CAST(household_score AS INT64) AS household_score,
    day
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE year  = '2026'
    AND month = '04'
    AND day IN ('20','21','22','23','24','25','26')
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

-- Per-IP daily detail collapsed to per-IP summary
ip_summary AS (
  SELECT
    advertiser_id,
    ip,
    COUNT(DISTINCT day)                                  AS days_observed,
    MAX(household_score)                                 AS max_score,
    MIN(household_score)                                 AS min_score,
    COUNT(DISTINCT household_score)                      AS distinct_scores,
    -- per-IP tier flags (any day)
    LOGICAL_OR(household_score = 10000)                                  AS hit_high,
    LOGICAL_OR(household_score BETWEEN 7000 AND 9999)                    AS hit_peak,
    LOGICAL_OR(household_score BETWEEN 3333 AND 6999)                    AS hit_mid,
    LOGICAL_OR(household_score < 3333)                                   AS hit_max_reach
  FROM prospecting
  GROUP BY advertiser_id, ip
)

SELECT
  advertiser_id,

  -- Volume (ip is already unique within advertiser in ip_summary, so
  -- COUNT(*) is equivalent to COUNT(DISTINCT ip) but dramatically cheaper)
  COUNT(*)                                                            AS distinct_ips,
  SUM(days_observed)                                                  AS ip_day_observations,
  SAFE_DIVIDE(SUM(days_observed), COUNT(*))                           AS avg_days_per_ip,

  -- MAX-tier composition (the subject construction Phase 1 uses)
  COUNTIF(max_score = 10000)                                          AS max_tier_high,
  COUNTIF(max_score BETWEEN 7000 AND 9999)                            AS max_tier_peak,
  COUNTIF(max_score BETWEEN 3333 AND 6999)                            AS max_tier_mid,
  COUNTIF(max_score < 3333)                                           AS max_tier_max_reach,

  -- Tier collapse signature
  COUNTIF(distinct_scores = 1)                                        AS ips_with_constant_score,
  COUNTIF(distinct_scores = 1 AND max_score = 10000)                  AS ips_stuck_at_10000,
  SAFE_DIVIDE(COUNTIF(distinct_scores = 1 AND max_score = 10000),
              COUNT(*))                                               AS frac_stuck_at_10000,

  -- Tier diversity (IPs that span ≥2 tiers across the week — preserves
  -- per-tier breakouts under MAX collapse only when this is high)
  COUNTIF(hit_high AND hit_peak)                                      AS ips_high_and_peak,
  COUNTIF(hit_peak AND hit_mid)                                       AS ips_peak_and_mid,
  COUNTIF(hit_high AND hit_peak AND hit_mid)                          AS ips_all_three_tiers,
  SAFE_DIVIDE(
    COUNTIF((CAST(hit_high AS INT64)
           + CAST(hit_peak AS INT64)
           + CAST(hit_mid  AS INT64)) >= 2),
    COUNT(*)
  )                                                                   AS frac_multi_tier,

  -- Score range
  AVG(max_score - min_score)                                          AS avg_score_range_per_ip
FROM ip_summary
GROUP BY advertiser_id
HAVING distinct_ips >= 100  -- drop noise-level advertisers
ORDER BY distinct_ips DESC
