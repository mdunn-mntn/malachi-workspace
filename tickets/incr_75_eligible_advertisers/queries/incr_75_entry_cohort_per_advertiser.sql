WITH e AS (
  SELECT advertiser_id, campaign_id, ip, dt, arm, visited,
    ROW_NUMBER() OVER (PARTITION BY advertiser_id, campaign_id, ip ORDER BY dt) AS rn
  FROM `dw-main-silver.enriched.lift__ghost_bid_visits`
),
entry AS ( SELECT advertiser_id, dt AS entry_dt, arm, visited FROM e WHERE rn = 1 )
SELECT advertiser_id, entry_dt,
  COUNTIF(arm="submitted") n_t, COUNTIF(arm="ghost") n_h,
  COUNTIF(arm="submitted" AND visited) v_t, COUNTIF(arm="ghost" AND visited) v_h
FROM entry
WHERE entry_dt IN ("2026-06-23","2026-06-24")   -- clean (full) + first truncated, for sensitivity
GROUP BY advertiser_id, entry_dt
