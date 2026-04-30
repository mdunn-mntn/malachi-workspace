-- TI-884: Step 2 — per-advertiser Stage 1 metrics for top-50.
-- Population: top-50 advertisers by April 2026 spend (from step 1).
-- Window: April 1-30, 2026.
-- Filter: Stage 1 campaigns only (funnel_level=1) for treated population.
-- Visits/conversions: any campaign of the advertiser within window (any-attribution).
-- Output cols:
--   advertiser_id
--   treated_ips      = distinct IPs served Stage 1 ads
--   visiting_treated_ips   = treated IPs that visited the advertiser site within window
--   converting_treated_ips = treated IPs that converted within window
-- Dry-run cost: ~285 GB.
-- Saved to outputs/ti_884_top50_per_advertiser_metrics.json.

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
  SELECT campaign_id
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
visiting_ips AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
    AND advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
  GROUP BY 1, 2
),
converting_ips AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
    AND advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
  GROUP BY 1, 2
)
SELECT
  t.advertiser_id,
  COUNT(DISTINCT t.ip) AS treated_ips,
  COUNT(DISTINCT IF(v.ip IS NOT NULL, t.ip, NULL)) AS visiting_treated_ips,
  COUNT(DISTINCT IF(c.ip IS NOT NULL, t.ip, NULL)) AS converting_treated_ips
FROM treated_ips t
LEFT JOIN visiting_ips v USING (advertiser_id, ip)
LEFT JOIN converting_ips c USING (advertiser_id, ip)
GROUP BY 1
ORDER BY treated_ips DESC
