-- incr_75_entry_cohort_clean.sql — measured ghost-bid lift per advertiser, clean 2026-06-23..07-07 window.
--
-- Window = entry dates 2026-06-23..2026-07-07, Beeswax leg (partner_id 8).
-- Why not the full 06-23..08-11 that the table now holds: the entry-cohort design
-- EXHAUSTS the holdout arm. A holdout IP never wins, so it never leaves the bidding
-- pool and is captured by its first-bid anchor almost immediately; treatment IPs churn
-- and new ones keep arriving. Later entry cohorts are therefore increasingly
-- treatment-only — the observed holdout share falls 0.105 (06-23) -> 0.084 (08-11)
-- against a fixed 10% platform holdout, and measured lift inflates in lockstep
-- (+3% -> +25%). Same signature as the June left-edge artifact, opposite direction.
-- The method is only valid while the observed holdout share sits in the clean 0.09-0.11
-- band, which ends 2026-07-07. A longer window makes this estimate worse, not better.
--
-- partner_id 79 (MNTN Rust bidder leg) entered the table the week of 2026-07-05 and is
-- excluded: its holdout share runs 0.066-0.083 from the start and it reads +128% to +290%.
WITH e AS (
  SELECT
    advertiser_id, campaign_id, ip, dt, arm, visited, converted, eff_score,
    ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
  WHERE partner_id = 8
),
entry AS (
  SELECT * FROM e WHERE rn = 1 AND dt BETWEEN "2026-06-23" AND "2026-07-07"
)
SELECT
  advertiser_id,
  COUNTIF(arm = "submitted")                   AS n_t,
  COUNTIF(arm = "ghost")                       AS n_h,
  COUNTIF(arm = "submitted" AND visited)       AS v_t,
  COUNTIF(arm = "ghost"     AND visited)       AS v_h,
  COUNTIF(arm = "submitted" AND converted)     AS c_t,
  COUNTIF(arm = "ghost"     AND converted)     AS c_h,
  COUNTIF(eff_score IS NULL OR eff_score = 0)  AS n_no_score
FROM entry
GROUP BY advertiser_id
