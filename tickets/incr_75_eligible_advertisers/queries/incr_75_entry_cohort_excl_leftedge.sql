WITH e AS (
  SELECT advertiser_id, campaign_id, ip, dt, arm, visited,
    ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
),
entry AS ( SELECT advertiser_id, arm, visited FROM e WHERE rn=1 AND dt >= "2026-06-23" )  -- exclude 06-22 left edge (Matt)
SELECT advertiser_id,
  COUNTIF(arm="submitted") n_t, COUNTIF(arm="ghost") n_h,
  COUNTIF(arm="submitted" AND visited) v_t, COUNTIF(arm="ghost" AND visited) v_h
FROM entry GROUP BY advertiser_id
