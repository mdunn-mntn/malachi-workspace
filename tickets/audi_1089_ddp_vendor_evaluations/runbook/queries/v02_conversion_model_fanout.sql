-- ============================================================================
-- VALIDATION v02 (not a pipeline step): ui_conversions attribution-model fan-out
-- Claim: justifies q7c's dedup design. ui_conversions carries one row PER
-- attribution model per conversion event (models 1-16+ observed) — raw COUNT(*)
-- overcounts conversions ~3-4x. q7c dedups to one row per (advertiser_id, guid,
-- event_epoch) preferring last-touch (attribution_model_type_id 0 treated as 1,
-- lowest model id tiebreak), excluding assists + disputed. This query shows the
-- per-model multiplicity on one day so a reviewer can see WHY the dedup exists.
--
-- Run (console-pasteable, standard SQL; seconds):
-- ============================================================================

SELECT
  attribution_model_id,
  attribution_model_type_id,
  is_pa,
  conversion_assist,
  COUNT(*) AS rows_,
  COUNT(DISTINCT order_id) AS distinct_orders,
  ROUND(SUM(order_amt), 0) AS order_amt_sum,
  COUNTIF(ad_served_id IS NOT NULL) AS with_ad_served_id
FROM `dw-main-silver.summarydata.ui_conversions`
WHERE DATE(time) = '2026-07-06'  -- PARAM sample day
GROUP BY 1, 2, 3, 4
ORDER BY rows_ DESC
LIMIT 40;
