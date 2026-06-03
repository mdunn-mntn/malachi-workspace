-- Verify: do flight updates propagate to parent campaign_group.update_time?
-- If yes, rule 2 (cg.update_time <24h) catches flight-level changes.
-- If no, rule 2 has a gap and needs to also check ui_ui_flights.update_time.

WITH cg_latest AS (
  SELECT campaign_group_id, update_time AS cg_update
  FROM `dw-main-bronze.integrationprod.campaign_groups`
  WHERE deleted = FALSE
),
flight_latest AS (
  SELECT campaign_group_id, MAX(update_time) AS flight_max_update
  FROM `dw-main-bronze.integrationprod.ui_ui_flights`
  WHERE update_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
  GROUP BY 1
),
joined AS (
  SELECT
    c.campaign_group_id,
    c.cg_update,
    f.flight_max_update,
    TIMESTAMP_DIFF(f.flight_max_update, c.cg_update, MINUTE) AS flight_minus_cg_min
  FROM cg_latest c JOIN flight_latest f USING (campaign_group_id)
)
SELECT
  COUNT(*) AS n_pairs,
  COUNTIF(flight_minus_cg_min <= 1)  AS n_within_1m,
  COUNTIF(flight_minus_cg_min <= 60) AS n_within_1h,
  COUNTIF(flight_minus_cg_min > 60 AND flight_minus_cg_min <= 1440) AS n_flight_1h_to_24h_newer,
  COUNTIF(flight_minus_cg_min > 1440 AND flight_minus_cg_min <= 10080) AS n_flight_1d_to_7d_newer,
  COUNTIF(flight_minus_cg_min > 10080) AS n_flight_7d_plus_newer,
  COUNTIF(flight_minus_cg_min < -1) AS n_cg_newer
FROM joined;
