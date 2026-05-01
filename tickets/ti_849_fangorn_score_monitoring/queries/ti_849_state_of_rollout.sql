/* ========================================================================
   TI-849 — Fangorn Rollout: State of advertisers flipped to vertical_data_source = 46

   Authoritative inclusion source: bronze.integrationprod.audience_advertiser_configurations
   Run daily to detect any new flips by Ryan/Sean. The Mode dashboard uses
   this as the AID list (so it auto-picks up Tier 1 expansion).

   2026-05-01: 3 advertisers flipped (Biz2Credit, Big Blue Bubble, Univ. NW Ohio).
   ======================================================================== */

SELECT
  c.advertiser_id,
  a.company_name,
  v.vertical_id,
  v.vertical_name,
  c.vertical_data_source,
  c.update_time AS flip_time,
  DATETIME_DIFF(CURRENT_DATETIME(), DATETIME(c.update_time), DAY) AS days_since_flip
FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations` c
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON c.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE
  AND a.is_test = FALSE
LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` v
  ON c.advertiser_id = v.advertiser_id
  AND v.type = 1  -- sub-vertical (most specific)
WHERE c.vertical_data_source = 46
ORDER BY c.update_time;
