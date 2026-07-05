-- Module 10 — Addressable prospecting audience size / HI coverage (monthly)
-- Addressable pool = per day, MAX(total_audience_size) across the AID's stage-1
-- (funnel=1/obj=1) prospecting campaigns, averaged across the month.
-- total_audience_size (UI number) overstates deliverable by ~5x (data_knowledge),
-- so deliverable_est = pool/5. Date floor ~2025-02 (no 2024 pool history).
-- Source: dw-main-silver.perml.flight_cid_day_audience_sizes (keyed by campaign_id).
-- Params: {{AID}} {{WIN_START}} {{WIN_END}} (WIN_END EXCLUSIVE)
WITH s AS (
  SELECT a.rpt_day, a.total_audience_size
  FROM `dw-main-silver.perml.flight_cid_day_audience_sizes` a
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = a.campaign_id
  WHERE c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
    AND a.rpt_day >= "{{WIN_START}}" AND a.rpt_day < "{{WIN_END}}"
    AND a.total_audience_size IS NOT NULL
),
daily AS (
  SELECT rpt_day, MAX(total_audience_size) AS day_pool
  FROM s GROUP BY rpt_day
)
SELECT
  FORMAT_DATE("%Y-%m", rpt_day) AS mo,
  ROUND(AVG(day_pool))          AS addressable_pool,
  ROUND(AVG(day_pool) / 5.0)    AS deliverable_est,
  COUNT(*)                      AS days
FROM daily
GROUP BY mo
ORDER BY mo