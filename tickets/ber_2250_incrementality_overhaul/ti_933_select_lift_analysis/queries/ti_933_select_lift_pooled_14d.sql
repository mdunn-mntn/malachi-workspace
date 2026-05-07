/*
  TI-933 Phase 3: Pooled MNTN Select lift analysis, 14-day window.
  Replicates TI-917 v5 segment query, but:
   - Cohort = ALL active Select advertisers (campaign_groups.product_id = 2)
   - Single segment ("select_all") — Select is awareness-only, all prospecting,
     zero retargeting campaigns observed in Phase 1 across all 38 advertisers
   - Window = 2026-04-22 -> 2026-05-05 (14 days; augmentor_log TTL ~15d)
   - Visit window extends +3 days through 2026-05-08 to catch attribution lag
   - win_rates computed inline per-advertiser (segment-agnostic for Select since single segment)
   - Output: per-advertiser AND pooled rows; visit rates from clickpass + guid

  Outputs ATT lift = treated_rate - holdout_rate per arm/advertiser.
  Pool to compute the headline number; per-advertiser for the power slide.
*/

CREATE TEMP FUNCTION holdout_bucket(hex_str STRING)
RETURNS INT64
LANGUAGE js AS r"""
  var hex16 = hex_str.substring(0, 16);
  var val = BigInt("0x" + hex16);
  return Number(val % BigInt(1000));
""";

WITH
select_groups AS (
  SELECT campaign_group_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaign_groups`
  WHERE product_id = 2 AND deleted = FALSE AND is_test = FALSE
),
select_cohort AS (
  SELECT DISTINCT advertiser_id FROM select_groups
),
campaign_dim AS (
  SELECT c.campaign_id, c.advertiser_id, c.objective_id, c.funnel_level
  FROM `dw-main-bronze.integrationprod.campaigns` c
  INNER JOIN select_groups g USING (campaign_group_id)
  WHERE c.deleted = FALSE AND c.is_test = FALSE
),

prospecting_apr AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE year = '2026' AND month = '04'
    AND day IN ('22','23','24','25','26','27','28','29','30')
    AND CAST(advertiser_id AS INT64) IN (SELECT advertiser_id FROM select_cohort)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
prospecting_may AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip
  FROM `dw-main-bronze.external.household_scoring__prospecting_intent__v1`
  WHERE year = '2026' AND month = '05'
    AND day IN ('01','02','03','04','05')
    AND CAST(advertiser_id AS INT64) IN (SELECT advertiser_id FROM select_cohort)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),
prospecting AS (
  SELECT DISTINCT advertiser_id, ip FROM (
    SELECT * FROM prospecting_apr UNION ALL SELECT * FROM prospecting_may
  )
),
ip_assigned AS (
  SELECT
    advertiser_id, ip,
    holdout_bucket(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip)))) AS bucket,
    MOD(
      ABS(FARM_FINGERPRINT(CONCAT(CAST(advertiser_id AS STRING), ':wr:', ip))),
      100000
    ) AS wr_bucket
  FROM prospecting
),
holdouts AS (
  SELECT advertiser_id, ip, wr_bucket
  FROM ip_assigned WHERE bucket BETWEEN 0 AND 99
),
targeted AS (
  SELECT advertiser_id, ip
  FROM ip_assigned WHERE bucket BETWEEN 100 AND 999
),

augmentor_ips AS (
  SELECT DISTINCT ip
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE DATE(time) >= DATE '2026-04-22'
    AND DATE(time) <  DATE '2026-05-06'
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

biddable_holdouts AS (
  SELECT h.advertiser_id, h.ip, h.wr_bucket
  FROM holdouts h
  INNER JOIN augmentor_ips a USING (ip)
),

cost_imp_pairs AS (
  SELECT DISTINCT
    CAST(ci.advertiser_id AS INT64) AS advertiser_id,
    ci.ip
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN campaign_dim c ON ci.campaign_id = c.campaign_id
  WHERE DATE(ci.time) >= DATE '2026-04-22'
    AND DATE(ci.time) <  DATE '2026-05-06'
    AND ci.advertiser_id IN (SELECT advertiser_id FROM select_cohort)
    AND ci.ip IS NOT NULL AND ci.ip != '0.0.0.0'
),

served_treatment AS (
  SELECT DISTINCT t.advertiser_id, t.ip
  FROM targeted t INNER JOIN cost_imp_pairs c USING (advertiser_id, ip)
),

served_n_per_adv AS (
  SELECT advertiser_id, COUNT(DISTINCT ip) AS served_n
  FROM served_treatment
  GROUP BY advertiser_id
),
biddable_holdouts_n_per_adv AS (
  SELECT advertiser_id, COUNT(DISTINCT ip) AS bh_n
  FROM biddable_holdouts
  GROUP BY advertiser_id
),
win_rates AS (
  SELECT
    s.advertiser_id,
    s.served_n,
    b.bh_n,
    SAFE_DIVIDE(s.served_n, b.bh_n * 9) AS wr
  FROM served_n_per_adv s
  INNER JOIN biddable_holdouts_n_per_adv b USING (advertiser_id)
),

bh_subsampled AS (
  SELECT bh.advertiser_id, bh.ip
  FROM biddable_holdouts bh
  INNER JOIN win_rates wr ON wr.advertiser_id = bh.advertiser_id
  WHERE wr.wr > 0 AND bh.wr_bucket < CAST(wr.wr * 100000 AS INT64)
),

cp_pairs AS (
  SELECT DISTINCT
    CAST(cp.advertiser_id AS INT64) AS advertiser_id,
    cp.ip
  FROM `dw-main-silver.logdata.clickpass_log` cp
  INNER JOIN campaign_dim c ON cp.campaign_id = c.campaign_id
  WHERE DATE(cp.time) >= DATE '2026-04-22'
    AND DATE(cp.time) <  DATE '2026-05-09'
    AND cp.advertiser_id IN (SELECT advertiser_id FROM select_cohort)
    AND cp.ip IS NOT NULL AND cp.ip != '0.0.0.0'
),

guid_visits AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) >= DATE '2026-04-22'
    AND DATE(time) <  DATE '2026-05-09'
    AND advertiser_id IN (SELECT advertiser_id FROM select_cohort)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

ui_conv AS (
  SELECT DISTINCT
    CAST(advertiser_id AS INT64) AS advertiser_id,
    ip
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) >= DATE '2026-04-22'
    AND DATE(time) <  DATE '2026-05-09'
    AND advertiser_id IN (SELECT advertiser_id FROM select_cohort)
    AND ip IS NOT NULL AND ip != '0.0.0.0'
),

subjects AS (
  SELECT 'holdout_biddable' AS arm, advertiser_id, ip FROM bh_subsampled
  UNION ALL
  SELECT 'treated_served'   AS arm, advertiser_id, ip FROM served_treatment
)

-- Per-(advertiser, arm) only. Pooled stats reconstructed in Python by
-- summing across advertisers (mathematically identical because (aid, ip)
-- pairs are unique across advertisers — pooled_n_ips = SUM(per_adv_n_ips),
-- pooled_visitors = SUM(per_adv_visitors), pooled_rate = SUM(visitors)/SUM(ips)).
-- Drops a 4-way LEFT JOIN re-shuffle that doubled S15: Output cardinality.
SELECT
  s.advertiser_id,
  s.arm,
  COUNT(DISTINCT s.ip)                                     AS n_ips,
  COUNT(DISTINCT cp.ip)                                    AS clickpass_visitors,
  COUNT(DISTINCT gv.ip)                                    AS guid_visitors,
  COUNT(DISTINCT uc.ip)                                    AS ui_converters,
  SAFE_DIVIDE(COUNT(DISTINCT cp.ip), COUNT(DISTINCT s.ip)) AS clickpass_rate,
  SAFE_DIVIDE(COUNT(DISTINCT gv.ip), COUNT(DISTINCT s.ip)) AS guid_rate,
  SAFE_DIVIDE(COUNT(DISTINCT uc.ip), COUNT(DISTINCT s.ip)) AS ui_conv_rate
FROM subjects s
LEFT JOIN cp_pairs   cp ON cp.advertiser_id = s.advertiser_id AND cp.ip = s.ip
LEFT JOIN guid_visits gv ON gv.advertiser_id = s.advertiser_id AND gv.ip = s.ip
LEFT JOIN ui_conv    uc ON uc.advertiser_id = s.advertiser_id AND uc.ip = s.ip
GROUP BY s.advertiser_id, s.arm
ORDER BY s.advertiser_id, s.arm;
