-- ti_1313_fcap_stability.sql: when each prospecting campaign group's frequency cap was last edited.
-- dso.frequency_caps is current state with no history, so a cap edited inside the measurement
-- window mislabels that campaign. This flags the ones whose cap predates the window.

WITH prospecting_campaigns AS (
  SELECT c.campaign_id, c.campaign_group_id
  FROM `dw-main-silver.public.campaigns` c
  WHERE c.deleted = FALSE AND c.is_test = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
)
SELECT
  pc.campaign_group_id,
  MAX(DATE(f.update_time)) AS fcap_last_edited,
  MAX(DATE(f.update_time)) < '2026-06-22' AS fcap_stable_in_window
FROM prospecting_campaigns pc
JOIN `dw-main-silver.dso.frequency_caps` f USING (campaign_id)
GROUP BY 1
