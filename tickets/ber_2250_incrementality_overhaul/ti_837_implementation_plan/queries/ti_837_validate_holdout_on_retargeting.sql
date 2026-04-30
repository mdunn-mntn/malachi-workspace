-- TI-837: Validate whether retargeting campaigns (objective_id=4) enforce
-- the 10% production holdout hash, in response to Alex Knorr feedback.
--
-- Method:
--   1. Pull DISTINCT (advertiser_id, ip) served by retargeting campaigns
--      (objective_id=4) for v5 cohort over 1 day (2026-04-23).
--   2. For each, compute the production holdout bucket
--      MD5(advertiser_id : ip) mod 1000.
--   3. Bin and aggregate.
--
-- Interpretation:
--   - bucket 0-99 fraction ≈ 10% → holdouts NOT enforced for retargeting
--     (i.e., the bidder serves retargeting impressions to holdout IPs).
--   - bucket 0-99 fraction ≈ 0% → holdouts ARE enforced.
--
-- Same query rerun for a prospecting (objective_id IN 1, 5, 6) baseline
-- as a control: prospecting is known to enforce, so should show ~0% in
-- bucket 0-99.

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

served_ips AS (
  SELECT DISTINCT
    CAST(ci.advertiser_id AS INT64) AS advertiser_id,
    ci.ip,
    c.objective_id,
    CASE
      WHEN c.objective_id = 4 THEN 'rtg'
      WHEN c.objective_id IN (1, 5, 6) THEN 'prosp'
      ELSE 'other'
    END AS segment
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN campaign_dim c
    ON ci.campaign_id = c.campaign_id
  WHERE DATE(ci.time) = DATE('2026-04-23')
    AND ci.advertiser_id IN (SELECT advertiser_id FROM v5_cohort)
    AND ci.ip IS NOT NULL AND ci.ip != '0.0.0.0'
    AND c.objective_id IN (1, 4, 5, 6)
),

hash_buckets AS (
  SELECT
    segment,
    advertiser_id,
    ip,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip)))) AS bucket
  FROM served_ips
)

SELECT
  segment,
  CASE WHEN bucket < 100 THEN 'holdout_bucket_0_99' ELSE 'targeted_bucket_100_999' END AS bucket_class,
  COUNT(*) AS n_served_ips,
  ROUND(
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY segment),
    5
  ) AS frac_within_segment
FROM hash_buckets
GROUP BY segment, bucket_class
ORDER BY segment, bucket_class;
