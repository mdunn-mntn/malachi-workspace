/* ============================================================================
   Module 12 — Geo DMA reference (for the campaign audience deep-dive)
   ----------------------------------------------------------------------------
   location_id -> DMA name + Nielsen metro code, for decoding the geo `location_ids`
   in audience expressions. charts/12 parses each prospecting campaign's expression
   (geo include/exclude location_ids + MM/3P logic) and joins these names to characterize
   the geo tier (Top-20 / Mid / Low) and name the target markets.

   `location_type_id = 4` = Nielsen DMA (exactly 210 US DMAs). `location` = human name
   (e.g. "New York, NY"), `metro_id` = Nielsen 3-digit DMA code (501=NY, 803=LA, 602=Chicago...).
   NOTE: no population/rank column exists in the geo dataset — tier by DMA-set size + Nielsen
   rank of metro_id. Use `dw-main-silver.geo.location_data` (the BQ geo dim; NOT geo.locations,
   which 404s). This reference is advertiser-agnostic (all US DMAs); the render filters to those used.
   Params : none (full DMA list; ~tiny).
   ============================================================================ */
SELECT
  location_id,
  location            AS dma_name,
  metro_id            AS nielsen_dma_code
FROM `dw-main-silver.geo.location_data`
WHERE location_type_id = 4
ORDER BY location_id
