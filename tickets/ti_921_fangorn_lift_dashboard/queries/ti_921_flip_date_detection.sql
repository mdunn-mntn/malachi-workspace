/* ========================================================================
   TI-921 — Best-effort flip-date detection from CDC archive

   `audience_advertiser_configurations` is a snapshot table — it tells us
   the CURRENT vertical_data_source per advertiser, but not the day the
   value changed to 46.

   This query uses the Datastream `_archive` history table (if present)
   to find the earliest day each advertiser had vertical_data_source = 46.

   USE CASE:
     - Cross-check against artifacts/wave_config.csv (the manual source
       of truth) to make sure no flip slipped through.
     - One-time backfill if wave_config.csv goes missing.

   IF THE ARCHIVE TABLE DOESN'T EXIST in your environment, the query
   fails gracefully — fall back to the manual wave_config.csv.

   To check whether the archive table exists:
     bq ls dw-main-bronze:integrationprod | grep -i archive
   ======================================================================== */

WITH flip_history AS (
  -- Look at every historical row version where vertical_data_source = 46.
  -- Use datastream_metadata.source_timestamp for true source-of-change
  -- (per the data_knowledge.md gotcha: source_timestamp != update_time).
  SELECT
    advertiser_id,
    vertical_data_source,
    datastream_metadata.source_timestamp AS source_ts,
    DATE(datastream_metadata.source_timestamp) AS source_date
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations_archive`
  WHERE vertical_data_source = 46
),

first_46 AS (
  SELECT
    advertiser_id,
    MIN(source_ts)   AS first_46_ts,
    MIN(source_date) AS first_46_date
  FROM flip_history
  GROUP BY advertiser_id
),

current_46 AS (
  SELECT advertiser_id, vertical_data_source AS current_value
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations`
  WHERE vertical_data_source = 46
)

SELECT
  COALESCE(f.advertiser_id, c.advertiser_id) AS advertiser_id,
  a.company_name,
  v.vertical_name,
  f.first_46_date AS detected_flip_date,
  CASE
    WHEN f.advertiser_id IS NOT NULL AND c.advertiser_id IS NOT NULL THEN 'flipped_and_still_treated'
    WHEN f.advertiser_id IS NOT NULL AND c.advertiser_id IS NULL     THEN 'flipped_then_rolled_back'
    WHEN f.advertiser_id IS NULL     AND c.advertiser_id IS NOT NULL THEN 'currently_treated_no_history'
    ELSE 'unexpected'
  END AS state
FROM first_46 f
FULL OUTER JOIN current_46 c USING (advertiser_id)
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON COALESCE(f.advertiser_id, c.advertiser_id) = a.advertiser_id
  AND a.deleted = FALSE AND a.is_test = FALSE
LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` v
  ON COALESCE(f.advertiser_id, c.advertiser_id) = v.advertiser_id AND v.type = 1
ORDER BY detected_flip_date NULLS LAST, advertiser_id;
