-- TI-884: Measure CUPED rho on MNTN data — replaces literature midpoint in
-- post-stack MDE numbers.
--
-- Method:
--   1. For each of 3 large advertisers (WGU 31357, Vivint 30506, Ferguson 31276),
--      identify IPs treated in BOTH Feb 2026 and Mar 2026 (Stage 1).
--   2. For each such IP, compute:
--        feb_visited (0/1) — did the IP visit in Feb?
--        mar_visited (0/1) — did the IP visit in Mar?
--   3. Compute Pearson correlation rho between feb_visited and mar_visited per advertiser.
--   4. CUPED SE multiplier = sqrt(1 - rho^2). Apply to post-stack MDE.
--
-- Output: per-advertiser rho, n_ips_both_periods, mean_pre, mean_post.

WITH advs AS (
  SELECT advertiser_id FROM UNNEST([31357, 30506, 31276]) AS advertiser_id
),
stage1_campaigns AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE funnel_level = 1 AND deleted = FALSE AND is_test = FALSE
    AND advertiser_id IN (SELECT advertiser_id FROM advs)
),
treated_feb AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE '2026-02-01' AND DATE '2026-02-28'
    AND advertiser_id IN (SELECT advertiser_id FROM advs)
    AND campaign_id IN (SELECT campaign_id FROM stage1_campaigns)
  GROUP BY 1, 2
),
treated_mar AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE '2026-03-01' AND DATE '2026-03-31'
    AND advertiser_id IN (SELECT advertiser_id FROM advs)
    AND campaign_id IN (SELECT campaign_id FROM stage1_campaigns)
  GROUP BY 1, 2
),
visit_feb AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE '2026-02-01' AND DATE '2026-02-28'
    AND advertiser_id IN (SELECT advertiser_id FROM advs)
  GROUP BY 1, 2
),
visit_mar AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE '2026-03-01' AND DATE '2026-03-31'
    AND advertiser_id IN (SELECT advertiser_id FROM advs)
  GROUP BY 1, 2
),
-- IPs treated in both periods
both_periods AS (
  SELECT f.advertiser_id, f.ip
  FROM treated_feb f
  INNER JOIN treated_mar m USING (advertiser_id, ip)
),
joined AS (
  SELECT
    b.advertiser_id,
    b.ip,
    IF(vf.ip IS NOT NULL, 1.0, 0.0) AS feb_visited,
    IF(vm.ip IS NOT NULL, 1.0, 0.0) AS mar_visited
  FROM both_periods b
  LEFT JOIN visit_feb vf USING (advertiser_id, ip)
  LEFT JOIN visit_mar vm USING (advertiser_id, ip)
)
SELECT
  advertiser_id,
  COUNT(*) AS n_ips_both_periods,
  AVG(feb_visited) AS mean_feb_visit_rate,
  AVG(mar_visited) AS mean_mar_visit_rate,
  CORR(feb_visited, mar_visited) AS pearson_rho,
  POW(CORR(feb_visited, mar_visited), 2) AS r_squared,
  SQRT(1 - POW(CORR(feb_visited, mar_visited), 2)) AS cuped_se_multiplier
FROM joined
GROUP BY 1
ORDER BY n_ips_both_periods DESC
