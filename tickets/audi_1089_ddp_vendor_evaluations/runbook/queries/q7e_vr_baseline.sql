-- ============================================================================
-- DDP quality-score runbook, STEP 7e: platform VR calibration by funnel x scored
-- Claim: sanity-calibrates the sole-cohort visit rates (q7/q7b). Computes the
-- valuation week's PLATFORM-WIDE visit rate split by campaign OBJECTIVE bucket
-- (obj_bucket in the output — prospecting-family per objective ids; funnel_level
-- does NOT appear in the result) and by
-- whether the impression carried a household score. If cold unscored prospecting
-- inventory platform-wide sits near the no-svs baseline (~0.02%), then vendor
-- sole-IP VRs of 0.01-0.03% are normal for that inventory class — not a join bug.
-- Same visit join as q7/q7b (CIL LEFT JOIN clickpass per ad_served_id, trail
-- truncated 2026-07-10) so levels are directly comparable.
--
-- Grain: obj_bucket x scored. No svs scan — CIL + clickpass + campaigns only.
--
-- Run (from workspace root; foreground, minutes):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q7e vr baseline" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q7e_vr_baseline.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q7e_vr_baseline.csv
-- ============================================================================

WITH imps AS (
  SELECT ad_served_id, campaign_id,
         IF(household_score > 0, 'scored', 'unscored') AS scored
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
),

vis AS (
  SELECT ad_served_id, COUNT(*) AS visits
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE time >= TIMESTAMP('2026-07-02') AND time < TIMESTAMP('2026-07-10')
    AND ad_served_id IS NOT NULL
  GROUP BY ad_served_id
),

camp AS (
  SELECT campaign_id, objective_id
  FROM `dw-main-bronze.integrationprod.public_campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
)

SELECT
  CASE WHEN c.objective_id IN (1, 5, 6) THEN 'prospecting_family'
       WHEN c.objective_id = 4 THEN 'retargeting'
       WHEN c.objective_id IS NULL THEN 'unmatched'
       ELSE 'other' END AS obj_bucket,
  i.scored,
  COUNT(*) AS imps,
  SUM(COALESCE(v.visits, 0)) AS visits,
  ROUND(100 * SUM(COALESCE(v.visits, 0)) / COUNT(*), 4) AS vr_pct
FROM imps i
LEFT JOIN vis v USING (ad_served_id)
LEFT JOIN camp c USING (campaign_id)
GROUP BY 1, 2
ORDER BY 1, 2;
