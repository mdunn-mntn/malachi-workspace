/* ============================================================================
   Module 10 — Addressable audience size / HI coverage (monthly)
   ----------------------------------------------------------------------------
   The supply-side denominator for the recirculation story: how big is the
   addressable prospecting pool, is it shrinking, and how much of it have we reached?

   Addressable prospecting pool = per day, MAX(total_audience_size) across the AID's
   stage-1 (funnel=1/obj=1) campaigns (picks the real prospecting campaign over tiny
   TV-retargeting stage-1s), averaged across the month. `total_audience_size` = the UI
   audience number; it OVERSTATES the deliverable by ~5x (data_knowledge), so the render
   also shows a deliverable ≈ pool/5 reference and overlays module 09's cumulative HI reach.

   Caveats: date floor = 2025-02-01 (no 2024 pool history); the size is the TOTAL addressable
   (keyword + 3P), not the HI-only subset — HI coverage is approximate.
   Source : dw-main-silver.perml.flight_cid_day_audience_sizes (keyed by campaign_id; join campaigns for AID).
   Params : {{AID}} {{WIN_START}} {{WIN_END}}   (WIN_END EXCLUSIVE)
   ============================================================================ */
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
  ROUND(AVG(day_pool))          AS addressable_pool,       -- total addressable (UI number)
  ROUND(AVG(day_pool) / 5.0)    AS deliverable_est,        -- ~5x UI overstatement -> deliverable
  COUNT(*)                      AS days
FROM daily
GROUP BY mo
ORDER BY mo
