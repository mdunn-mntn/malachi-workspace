-- Module 12b — DMA decode: location_id -> DMA name + Nielsen code (all 210 US DMAs).
-- Advertiser-agnostic. Feeds the tier reference (expression location_ids -> names) and the
-- Nielsen bridge (nielsen_code === cost_impression_log.metro_id) for per-DMA delivery.
SELECT location_id, location AS dma_name, metro_id AS nielsen_code
FROM `dw-main-silver.geo.location_data`
WHERE location_type_id = 4
ORDER BY location_id
