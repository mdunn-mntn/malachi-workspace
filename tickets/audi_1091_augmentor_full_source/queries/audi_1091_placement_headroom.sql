-- AUDI-1091 [SPIKE] Full augmentor_log as a free site-visit source
-- Question: DS30-in-svs is augmentor_log filtered to BANNER placements only
--   (see SteelHouse/airflow-ti spark/fpa/dsid30_augmentor_log_processing.py:
--    PLACEMENT_TYPES = ("BANNER","BANNER_AND_VIDEO"); URL from page OR referrer).
--   How much site-visit signal is in the DROPPED placements (VIDEO/other)?
-- Site-visit usability requires a URL (svs is keyed on ip x url). The DS30 pipeline
--   extracts the URL from page/referrer only, so we test page-or-referrer presence.
-- Sample: one representative hour (placement mix is stable hour-to-hour); ~67 GB on
--   the us-central1 reservation. Full-day confirm deferred (table ~1.54 TB/day).
SELECT
  placement_type,
  COUNT(*)                         AS n_rows,
  APPROX_COUNT_DISTINCT(ip)        AS approx_ips,
  COUNTIF((page IS NOT NULL AND TRIM(page)!="")
       OR (referrer IS NOT NULL AND TRIM(referrer)!="")) AS rows_with_any_url,
  APPROX_COUNT_DISTINCT(
    CASE WHEN (page IS NOT NULL AND TRIM(page)!="")
           OR (referrer IS NOT NULL AND TRIM(referrer)!="") THEN ip END) AS approx_ips_with_url
FROM `dw-main-bronze.raw.augmentor_log`
WHERE time >= TIMESTAMP("2026-07-20 18:00:00") AND time < TIMESTAMP("2026-07-20 19:00:00")
  AND ip IS NOT NULL AND TRIM(ip) != ""
GROUP BY placement_type
ORDER BY n_rows DESC;
