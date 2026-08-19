-- INCR-75 rerun (2026-08-19): per-advertiser ghost-bid ITT over the full clean window.
--
-- Method (Matt Brorby, settled 2026-07-02): entry-cohort anchor — one row per
-- (advertiser, campaign, ip) at its FIRST bid date — then two window guards:
--   * exclude 2026-06-22, the table floor. Every IP already active before the table
--     existed first-appears there, so day 1 is a STOCK not a flow; it over-represents
--     accumulated holdout IPs (ghost_frac 0.12+) and manufactures a negative lift.
--   * exclude entries after MAX(dt) - 7d. `visited` = visited within 7 days of first
--     bid, so a cohort younger than 7 days is right-censored (falsely negative).
--
-- The 2026-06 run had only 06-23..07-01 (9 usable days). This window is 06-23..08-11
-- (50 days) because the ghost-bid tables now accumulate with no TTL.
--
-- Source: dw-main-silver.enriched.lift__ghost_bid_visits — the BEESWAX/JVM leg
-- (bid_price_log). The MNTN Rust bidder leg is not folded in yet.
WITH bounds AS (
  SELECT DATE_SUB(MAX(dt), INTERVAL 7 DAY) AS last_mature_dt FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
),
e AS (
  SELECT
    advertiser_id, campaign_id, ip, dt, arm, visited, converted, eff_score,
    ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
),
entry AS (
  SELECT e.* FROM e, bounds
  WHERE e.rn = 1
    AND e.dt >= "2026-06-23"
    AND e.dt <= bounds.last_mature_dt
)
SELECT
  advertiser_id,
  COUNTIF(arm = "submitted")                        AS n_t,
  COUNTIF(arm = "ghost")                            AS n_h,
  COUNTIF(arm = "submitted" AND visited)            AS v_t,
  COUNTIF(arm = "ghost"     AND visited)            AS v_h,
  COUNTIF(arm = "submitted" AND converted)          AS c_t,
  COUNTIF(arm = "ghost"     AND converted)          AS c_h,
  COUNTIF(eff_score IS NULL OR eff_score = 0)       AS n_no_score,
  MIN(dt)                                           AS first_entry_dt,
  MAX(dt)                                           AS last_entry_dt
FROM entry
GROUP BY advertiser_id
