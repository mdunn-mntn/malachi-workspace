/* ============================================================================
   Module 12b — GEO / DMA tier deep-dive
   ----------------------------------------------------------------------------
   Module 12 proved the prospecting campaigns are GEO-SLICED by DMA. This module
   opens that up: it NAMES the DMAs in each population tier, and quantifies how the
   geo footprint EXPANDED YoY and how each tier performs — the "geo-mix dilution"
   check made concrete.

   KEY FACT (Kindred, verified 2026-07-02): prospecting slices all 210 US Nielsen
   DMAs into three POPULATION tiers, named in the campaigns themselves:
     HIGH POP =  20 DMAs (top markets: NY, LA, Chicago, Dallas, ...)   <- P1 ran ONLY this
     MID  POP =  38 DMAs
     LOW  POP = 152 DMAs (long tail)
   20 + 38 + 152 = 210.  "High Pop" is NOT all-of-US — it is the top-20 markets.

   TWO ID SYSTEMS (gotcha): the audience EXPRESSION targets `location_id`
   (internal geo.location_data id); delivery logs (CIL) carry `metro_id` (the
   Nielsen DMA code). They DIFFER (loc 541 = NY, Nielsen 501 = NY). Bridge them
   with geo.location_data.metro_id === cost_impression_log.metro_id === metros.metro_id.

   Params: {{AID}} {{P1_START}} {{P1_END}} {{P2_START}} {{P2_END}}
           {{DELIV_MONTH_START}} {{DELIV_MONTH_END}} (a recent in-CIL-TTL month; CIL is 90d rolling)
   ============================================================================ */

-- ---------------------------------------------------------------------------
-- (A) DMA decode — location_id -> DMA name + Nielsen code (all 210 US DMAs).
--     Feeds the tier reference (expression location_ids -> names) AND the
--     nielsen bridge to delivery.  -> 12b_geo_dma_decode.csv
-- ---------------------------------------------------------------------------
SELECT location_id, location AS dma_name, metro_id AS nielsen_code
FROM `dw-main-silver.geo.location_data`
WHERE location_type_id = 4
ORDER BY location_id;

-- ---------------------------------------------------------------------------
-- (B) Per-tier (campaign_group) performance, P1 vs P2. Groups are the geo tiers;
--     visits = views+clicks, conv = click+view conversions, spend = media+data+platform,
--     revenue = click+view order value.  Lookback bound satisfies partition-elim.
--     -> 12b_geo_tier_metrics.csv
-- ---------------------------------------------------------------------------
WITH d AS (
  SELECT c.campaign_group_id AS grp,
    CASE WHEN s.day BETWEEN "{{P1_START}}" AND "{{P1_END}}" THEN "P1"
         WHEN s.day BETWEEN "{{P2_START}}" AND "{{P2_END}}" THEN "P2" END AS period,
    s.impressions, s.views, s.clicks, s.click_conversions, s.view_conversions,
    s.click_order_value, s.view_order_value, s.media_spend, s.data_spend, s.platform_spend, s.day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{AID}}
    AND s.day BETWEEN "{{P1_START}}" AND "{{P2_END}}"
    AND c.campaign_group_id IN (69884,96108,109926,115943,115945,115946)
)
SELECT grp AS campaign_group_id, period,
  MIN(day) first_day, MAX(day) last_day,
  SUM(impressions) imps, SUM(views+clicks) visits,
  SUM(click_conversions+view_conversions) conv,
  ROUND(SUM(media_spend+data_spend+platform_spend),0) spend,
  ROUND(SUM(click_order_value+view_order_value),0) revenue,
  ROUND(1000*SUM(views+clicks)/NULLIF(SUM(impressions),0),3) vr_permille,
  ROUND(100*SUM(click_conversions+view_conversions)/NULLIF(SUM(views+clicks),0),2) cvr_pct,
  ROUND(SUM(click_order_value+view_order_value)/NULLIF(SUM(media_spend+data_spend+platform_spend),0),2) roas
FROM d WHERE period IS NOT NULL
GROUP BY grp, period ORDER BY grp, period;

-- ---------------------------------------------------------------------------
-- (C) Per-DMA delivery, one recent in-TTL month (CIL is the only DMA-grain
--     source; 90d rolling so P1 is out of window). Row = one impression.
--     Join metro name via summarydata.metros; attribute to tier via campaign_group.
--     -> 12b_per_dma_delivery_may26.csv   (nielsen bridge: cil.metro_id)
-- ---------------------------------------------------------------------------
SELECT c.campaign_group_id AS grp, cil.metro_id, m.name AS dma_name,
       COUNT(*) AS imps, ROUND(SUM(cil.media_cost),0) AS media_cost
FROM `dw-main-silver.logdata.cost_impression_log` cil
JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = cil.campaign_id
LEFT JOIN `dw-main-silver.summarydata.metros` m ON m.metro_id = cil.metro_id
WHERE cil.advertiser_id = {{AID}}
  AND DATE(cil.time) BETWEEN "{{DELIV_MONTH_START}}" AND "{{DELIV_MONTH_END}}"
  AND c.campaign_group_id IN (69884,96108,109926,115943,115945,115946)
GROUP BY 1,2,3 ORDER BY imps DESC;
