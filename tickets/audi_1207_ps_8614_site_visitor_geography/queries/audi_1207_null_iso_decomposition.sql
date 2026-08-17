WITH r AS (
  SELECT dt, advertiser_id, ip, iso_code,
         ARRAY_LENGTH(location_ids.list) AS n_loc,
         EXISTS(SELECT 1 FROM UNNEST(location_ids.list) e WHERE e.element = '237') AS has_us
  FROM ggr
  WHERE dt = '2026-08-16'
),
c AS (
  SELECT
    CASE WHEN advertiser_id = 33129 THEN 'ADV_33129' ELSE 'OTHER_ADVERTISERS' END AS scope,
    CASE
      WHEN iso_code IS NOT NULL AND iso_code <> '' THEN '1_us_state_resolved'
      WHEN n_loc IS NULL OR n_loc = 0 THEN '2_no_geo_match_at_all'
      WHEN has_us THEN '3_geo_matched_US_but_no_state'
      ELSE '4_geo_matched_non_US'
    END AS bucket,
    advertiser_id, ip
  FROM r
)
SELECT scope, bucket, COUNT(DISTINCT CONCAT(CAST(advertiser_id AS STRING),'|',ip)) AS ip_adv_pairs
FROM c
GROUP BY scope, bucket
ORDER BY scope, bucket
