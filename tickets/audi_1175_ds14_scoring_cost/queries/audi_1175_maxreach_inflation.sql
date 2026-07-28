-- AUDI-1175 shadow query 1: Max-Reach inflation baseline
-- Q: what fraction of the HHST recommender's bucket population is ALREADY unscored
--    (score-failure -> coalesce hh_score=0 -> Max Reach bucket), before any DS14 gate?
-- Table: dw-main-bronze.raw.bid_price_log (Beeswax) — 697B rows / 1 PB, HOUR-partition on `time`,
--   requirePartitionFilter=TRUE, clustered by ip. ~194 GB per 1-hour prospecting slice.
-- Run via .claude/scripts/bq_run.sh --location=us-central1.
--
-- Result 2026-07-27 18:00-19:00 UTC, objective_id IN (1,5,6) (prospecting):
--   has_price=TRUE (valid score, real bid): 29.5M rows / 3.15M IPs
--   intent-score failures -> Max Reach: invalidCampaignIntentScore 1.77B/6.35M ip,
--     missingIntentScore 532M/5.30M ip, invalidAdvertiserIntentScore 7.6M/0.62M ip
--   household-score failures (invalidHouseholdScore / invalidAdvertiserHouseholdScoreFailure): absent (not in top 40)
--   pacing/floor/geo/ghost (NOT in recommender denominator): ~740M rows
-- Read: Max Reach already dominates the denominator (~99% of rows unscored) with no gate.
--   Our gate keeps scoring all DS14-addressable IPs, so the priced arm (3.15M ip/hr) is preserved;
--   the IPs we stop scoring are non-DS14 and fail the availability AND before the score check.
--   Residual = scoring-snapshot vs 8-day-serving-window alignment (gate on the 8d DS14 union; RTC covers intra-run new IPs).

SELECT has_price, threshold_failure_reasons, COUNT(*) AS n, COUNT(DISTINCT ip) AS ips
FROM `dw-main-bronze.raw.bid_price_log`
WHERE time >= TIMESTAMP("2026-07-27 18:00:00") AND time < TIMESTAMP("2026-07-27 19:00:00")
  AND objective_id IN (1,5,6)   -- prospecting
GROUP BY 1,2 ORDER BY n DESC LIMIT 40;
