-- Module 12 — Geo DMA reference: location_id -> DMA name + Nielsen metro code.
-- Advertiser-agnostic (all 210 US Nielsen DMAs, location_type_id=4). The chart filters
-- to the location_ids that actually appear in this advertiser's geo expressions; national
-- targets (location_id 237 = "United States", location_type_id=2) are not DMAs and simply
-- do not decode here (correctly N/A for national advertisers like Bouqs).
SELECT
  location_id,
  location AS dma_name,
  metro_id AS nielsen_dma_code
FROM `dw-main-silver.geo.location_data`
WHERE location_type_id = 4
ORDER BY location_id
