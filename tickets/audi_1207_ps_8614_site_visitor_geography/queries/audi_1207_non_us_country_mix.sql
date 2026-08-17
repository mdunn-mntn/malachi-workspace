WITH r AS (
  SELECT ip, location_ids.list[SAFE_OFFSET(0)].element AS country_loc_id
  FROM ggr
  WHERE dt = '2026-08-16'
    AND advertiser_id = 33129
    AND (iso_code IS NULL OR iso_code = '')
    AND ARRAY_LENGTH(location_ids.list) > 0
  GROUP BY ip, country_loc_id
),
ctry AS (
  SELECT DISTINCT location_id, location, country_iso_code
  FROM `dw-main-bronze.geo.location_data`
  WHERE location_type_id = 2
)
SELECT COALESCE(c.location, CONCAT('loc_id ', r.country_loc_id)) AS country,
       c.country_iso_code,
       COUNT(*) AS distinct_ips,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_nonus
FROM r LEFT JOIN ctry c ON CAST(r.country_loc_id AS INT64) = c.location_id
GROUP BY 1,2
ORDER BY distinct_ips DESC
LIMIT 20
