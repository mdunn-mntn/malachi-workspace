-- ============================================================================
-- VALIDATION v01 (not a pipeline step): visit basis — ui_visits vs clickpass_log
-- Claim: the runbook's visit counts (q7/q7b/q7e, clickpass_log per ad_served_id)
-- match the dedicated attributed-visits table. ui_visits fans out one row PER
-- attribution model per visit event; deduped to one row per (advertiser_id,
-- guid, epoch) preferring last-touch (type 0 treated as 1), it reproduces
-- clickpass_log within +0.5% (verified 2026-07-13: 9,812,770 vs 9,763,586).
-- Probabilistic attribution (is_pa) adds only ~14K rows/wk — negligible.
--
-- Run (console-pasteable, standard SQL; ~1 min):
-- ============================================================================

WITH dedup AS (
  SELECT * EXCEPT(rn) FROM (
    SELECT advertiser_id, guid, epoch, ad_served_id,
           ROW_NUMBER() OVER (
             PARTITION BY advertiser_id, guid, epoch
             ORDER BY IF(attribution_model_type_id = 0, 1, attribution_model_type_id),
                      attribution_model_id
           ) AS rn
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE time >= TIMESTAMP('2026-07-02') AND time < TIMESTAMP('2026-07-10')  -- PARAM visit window
  ) WHERE rn = 1
),
cp AS (
  SELECT COUNT(*) AS cp_events, COUNT(DISTINCT ad_served_id) AS cp_asids
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE time >= TIMESTAMP('2026-07-02') AND time < TIMESTAMP('2026-07-10')
    AND ad_served_id IS NOT NULL
)
SELECT
  (SELECT COUNT(*) FROM dedup) AS ui_visits_events_deduped,
  (SELECT COUNT(DISTINCT ad_served_id) FROM dedup) AS ui_asids,
  cp.cp_events AS clickpass_events,
  cp.cp_asids AS clickpass_asids,
  ROUND(100 * ((SELECT COUNT(*) FROM dedup) / cp.cp_events - 1), 2) AS pct_diff
FROM cp;
