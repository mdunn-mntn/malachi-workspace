-- TI-917: per-advertiser per-IP revenue mean & stddev for iROAS MDE extension.
-- Population: top-50 advertisers by April 2026 Stage 1 spend (TI-884 cohort).
-- Unit: per (advertiser, treated IP). Revenue = SUM(order_amt) over April 2026.
-- Outcome: continuous (revenue per IP, dollars). Most IPs have zero revenue.
-- Use: feeds mde_continuous(n_t, n_c, mu, sigma, ...) in ti_884_mde_calculator.py
-- Saves to outputs/ti_917_revenue_sigma_per_advertiser.json.
-- Memory gotcha: ui_conversions uses `order_amt` (NOT order_amt_usd, which is NULL).

WITH top_advertisers AS (
  SELECT advertiser_id FROM UNNEST([
    31357,30506,31276,37775,49868,31455,34143,36232,34838,51660,
    40563,34249,32404,41034,42097,34835,54196,38422,37056,38059,
    38652,34114,9090,33389,57322,41057,37115,37158,32147,49753,
    32058,42357,45921,40598,39036,35872,34611,34094,33518,38579,
    32756,31901,34991,37880,34834,35312,45458,46020,36507,47272
  ]) AS advertiser_id
),
stage1_campaigns AS (
  SELECT campaign_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE funnel_level = 1 AND deleted = FALSE AND is_test = FALSE
    AND advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
),
treated_ips AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
    AND advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
    AND campaign_id IN (SELECT campaign_id FROM stage1_campaigns)
  GROUP BY 1, 2
),
conversions_per_ip AS (
  SELECT advertiser_id, ip, SUM(order_amt) AS rev_per_ip
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
    AND advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
    AND order_amt IS NOT NULL
  GROUP BY 1, 2
),
per_ip AS (
  SELECT
    t.advertiser_id,
    t.ip,
    COALESCE(c.rev_per_ip, 0) AS rev_per_ip
  FROM treated_ips t
  LEFT JOIN conversions_per_ip c USING (advertiser_id, ip)
)
SELECT
  advertiser_id,
  COUNT(*) AS treated_ips,
  COUNTIF(rev_per_ip > 0) AS converting_ips,
  SUM(rev_per_ip) AS total_revenue,
  AVG(rev_per_ip) AS mu_rev_per_ip,
  STDDEV(rev_per_ip) AS sigma_rev_per_ip,
  APPROX_QUANTILES(rev_per_ip, 100)[OFFSET(50)] AS p50_rev_per_ip,
  APPROX_QUANTILES(rev_per_ip, 100)[OFFSET(95)] AS p95_rev_per_ip,
  MAX(rev_per_ip) AS max_rev_per_ip
FROM per_ip
GROUP BY 1
ORDER BY total_revenue DESC
