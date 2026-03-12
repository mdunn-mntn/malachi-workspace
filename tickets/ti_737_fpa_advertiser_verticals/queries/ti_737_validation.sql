-- TI-737: fpa.advertiser_verticals validation queries
-- Validates BQ parity with CoreDW source

-- 1. Basic profile
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT advertiser_id) AS distinct_advertisers,
  COUNT(DISTINCT vertical_id) AS distinct_verticals,
  COUNT(DISTINCT vertical_name) AS distinct_vertical_names,
  MIN(created_time) AS earliest_created,
  MAX(created_time) AS latest_created,
  MIN(updated_time) AS earliest_updated,
  MAX(updated_time) AS latest_updated
FROM `dw-main-silver.fpa.advertiser_verticals`;

-- 2. Duplicate PK check
SELECT id, COUNT(*) AS cnt
FROM `dw-main-silver.fpa.advertiser_verticals`
GROUP BY id
HAVING COUNT(*) > 1;

-- 3. NULL / empty check
SELECT
  COUNTIF(id IS NULL) AS null_id,
  COUNTIF(advertiser_id IS NULL) AS null_advertiser_id,
  COUNTIF(advertiser_name IS NULL) AS null_advertiser_name,
  COUNTIF(vertical_name IS NULL) AS null_vertical_name,
  COUNTIF(vertical_id IS NULL) AS null_vertical_id,
  COUNTIF(type IS NULL) AS null_type,
  COUNTIF(created_time IS NULL) AS null_created_time,
  COUNTIF(updated_time IS NULL) AS null_updated_time,
  COUNTIF(advertiser_name = '') AS empty_advertiser_name,
  COUNT(*) AS total
FROM `dw-main-silver.fpa.advertiser_verticals`;

-- 4. Type distribution
SELECT type, COUNT(*) AS cnt, COUNT(DISTINCT advertiser_id) AS distinct_adv
FROM `dw-main-silver.fpa.advertiser_verticals`
GROUP BY type
ORDER BY type;

-- 5. Referential integrity: orphan advertiser_ids
SELECT COUNT(DISTINCT av.advertiser_id) AS orphan_advertiser_count
FROM `dw-main-silver.fpa.advertiser_verticals` av
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON av.advertiser_id = a.advertiser_id
WHERE a.advertiser_id IS NULL;

-- 6. Vertical name collisions (same name, different IDs)
SELECT vertical_name,
  COUNT(DISTINCT vertical_id) AS id_count,
  ARRAY_AGG(DISTINCT vertical_id ORDER BY vertical_id) AS ids
FROM `dw-main-silver.fpa.advertiser_verticals`
GROUP BY vertical_name
HAVING COUNT(DISTINCT vertical_id) > 1;

-- 7. Top verticals by advertiser count
SELECT vertical_id, vertical_name, COUNT(*) AS cnt
FROM `dw-main-silver.fpa.advertiser_verticals`
GROUP BY vertical_id, vertical_name
ORDER BY cnt DESC
LIMIT 20;
