-- Module 12b — per-DMA delivery for one recent in-TTL month (cost_impression_log is 90d rolling,
-- the only DMA-grain source; P1 is out of window). Row = one impression. metro_id is the Nielsen
-- DMA code (bridge to 12b_geo_dma_decode.nielsen_code). Groups derived dynamically (prospecting).
-- For national advertisers (single location_id 237) impressions still carry a metro_id, so this
-- shows geographic delivery spread even when targeting is national (chart treats it as context).
SELECT c.campaign_group_id AS grp, cil.metro_id, m.name AS dma_name,
       COUNT(*) AS imps, ROUND(SUM(cil.media_cost),0) AS media_cost
FROM `dw-main-silver.logdata.cost_impression_log` cil
JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = cil.campaign_id
LEFT JOIN `dw-main-silver.summarydata.metros` m ON m.metro_id = cil.metro_id
WHERE cil.advertiser_id = {{AID}}
  AND DATE(cil.time) BETWEEN "{{DELIV_MONTH_START}}" AND "{{DELIV_MONTH_END}}"
  AND c.campaign_group_id IN (
    SELECT DISTINCT campaign_group_id FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = {{AID}} AND deleted = FALSE
      AND objective_id = 1 AND funnel_level = 1)
GROUP BY 1,2,3 ORDER BY imps DESC
