-- incr_75_band_lift_clean.sql — measured lift split by intent band, same clean window.
-- lift by intent band, same clean cohort as the per-advertiser sheet.
--
-- Pooled at advertiser x band, then combined with INVERSE-VARIANCE WEIGHTS. A naive
-- count pool across heterogeneous advertisers is Simpson-confounded: it previously read
-- the unscored band at +29% when the inverse-variance estimate was ~0.
--
-- Band cutpoints (AUDI-1083, verified 2026-07-22): High Intent 8001-10000 (in the
-- advertiser's vertical AND matching its keywords) · Peak Performance 6666-8000
-- (in the vertical, no keyword) · Mid 3333-6665 (in the bucket, not the vertical) ·
-- Max Reach 1-3332 (keyword-only, outside the bucket) · Unscored (not MM-scored).
WITH e AS (
  SELECT advertiser_id, campaign_id, ip, dt, arm, visited, converted, eff_score,
    ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
  WHERE partner_id = 8
),
entry AS (
  SELECT
    CASE
      WHEN eff_score IS NULL OR eff_score < 1 THEN "Unscored"
      WHEN eff_score >= 8001 THEN "High Intent"
      WHEN eff_score >= 6666 THEN "Peak Performance"
      WHEN eff_score >= 3333 THEN "Mid Intent"
      ELSE "Max Reach"
    END AS band,
    advertiser_id, arm, visited, converted
  FROM e WHERE rn = 1 AND dt BETWEEN "2026-06-23" AND "2026-07-07"
)
SELECT advertiser_id, band,
  COUNTIF(arm="submitted") n_t, COUNTIF(arm="ghost") n_h,
  COUNTIF(arm="submitted" AND visited) v_t, COUNTIF(arm="ghost" AND visited) v_h,
  COUNTIF(arm="submitted" AND converted) c_t, COUNTIF(arm="ghost" AND converted) c_h
FROM entry GROUP BY advertiser_id, band
HAVING COUNTIF(arm="ghost") >= 1000
