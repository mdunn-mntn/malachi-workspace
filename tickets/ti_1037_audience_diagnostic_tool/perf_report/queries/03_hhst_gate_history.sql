/* ============================================================================
   Module 03 — HHST gate history (prospecting campaigns)
   ----------------------------------------------------------------------------
   Every HHST gate-change event for each prospecting campaign (funnel_level=1 AND
   objective_id=1), from the type-2 archive. Returns ALL history up to WIN_END —
   NOT filtered to WIN_START — so the chart can forward-fill the gate value that was
   in effect ENTERING the window (the last change before it). charts/03 draws a
   step-line of threshold over time, clipped to [WIN_START, WIN_END).

   Gate semantics (household_score_threshold):
     <= 0  (0 / -1 / -100) = NO gate (bidder serves any score, incl. unscored)
     6666                  = HI + PP floor
     10000                 = HI-only (must be max-scored / MM-qualified)
     1 .. 9999             = graduated / auto-paced HHST — the gate is an actively-tuned
                             control, not a fixed 3-state flag. Expect intermediate values + thrash.
   Source : archives.household_score_threshold_archives (order by update_time; version non-monotonic).
   Params : {{AID}} {{WIN_END}}   (WIN_END EXCLUSIVE; WIN_START intentionally unused here)
   ============================================================================ */
WITH camp AS (
  SELECT c.campaign_id, c.name AS camp_name, c.campaign_group_id, g.name AS group_name
  FROM `dw-main-bronze.integrationprod.campaigns` c
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
    ON g.campaign_group_id = c.campaign_group_id
  WHERE c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
),
chg AS (
  SELECT campaign_id, update_time, threshold,
         LAG(threshold) OVER (PARTITION BY campaign_id ORDER BY update_time) AS prev
  FROM `dw-main-silver.archives.household_score_threshold_archives`
  WHERE advertiser_id = {{AID}} AND update_time < TIMESTAMP("{{WIN_END}}")
)
SELECT
  c.campaign_group_id,
  c.group_name,
  c.campaign_id,
  c.camp_name,
  chg.update_time,
  chg.threshold
FROM chg JOIN camp c USING (campaign_id)
WHERE chg.prev IS NULL OR chg.threshold != chg.prev      -- collapse no-op re-writes to real changes
ORDER BY c.campaign_group_id, chg.update_time
