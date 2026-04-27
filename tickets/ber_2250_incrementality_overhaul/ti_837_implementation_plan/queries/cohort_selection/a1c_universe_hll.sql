-- TI-837 Phase 2 cohort selection — Stage A.1c (HLL-based, fastest)
-- ----------------------------------------------------------------
-- The per-IP MAX-score grouping in A.1/A.1b was too expensive on the
-- external prospecting table (≥30 min, ≥800B slot-ms, no bytes reported).
--
-- This version uses APPROX_COUNT_DISTINCT (HLL) which avoids per-IP
-- shuffle entirely. Single-level GROUP BY advertiser_id.
--
-- "Per-tier IP" semantics differ from MAX-tier:
--   - ips_ever_high: distinct IPs at score=10000 on ANY day
--   - ips_ever_peak: distinct IPs at score 7000-9999 on ANY day
--   - ips_ever_mid:  distinct IPs at score 3333-6999 on ANY day
--   - ips_ever_max_reach: distinct IPs at score <3333 on ANY day
--
-- These OVERLAP (an IP at high on day 1 and mid on day 5 is counted in
-- both). For cohort selection volume estimates, this overestimates per-
-- tier n; for collapse detection, the high-only fraction
-- (ips_ever_high / distinct_ips_total) is approximately equal to the
-- MAX-tier high-only fraction.
--
-- Filter: top-500 advertisers by March 2026 prospecting spend (from A.4
-- output). This bounds the per-advertiser cardinality of the GROUP BY.
-- ----------------------------------------------------------------

SELECT
  CAST(advertiser_id AS INT64) AS advertiser_id,
  APPROX_COUNT_DISTINCT(ip) AS distinct_ips_total,
  APPROX_COUNT_DISTINCT(IF(CAST(household_score AS INT64) = 10000, ip, NULL))
    AS ips_ever_high,
  APPROX_COUNT_DISTINCT(IF(CAST(household_score AS INT64) BETWEEN 7000 AND 9999, ip, NULL))
    AS ips_ever_peak,
  APPROX_COUNT_DISTINCT(IF(CAST(household_score AS INT64) BETWEEN 3333 AND 6999, ip, NULL))
    AS ips_ever_mid,
  APPROX_COUNT_DISTINCT(IF(CAST(household_score AS INT64) < 3333, ip, NULL))
    AS ips_ever_max_reach
FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
WHERE year  = '2026'
  AND month = '04'
  AND day IN ('20','21','22','23','24','25','26')
  AND ip IS NOT NULL AND ip != '0.0.0.0'
  AND CAST(advertiser_id AS INT64) IN (__IDS__)
GROUP BY advertiser_id
HAVING distinct_ips_total >= 100
ORDER BY distinct_ips_total DESC
