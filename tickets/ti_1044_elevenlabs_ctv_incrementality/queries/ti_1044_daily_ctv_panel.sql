-- TI-1044: ElevenLabs (AID 51660) daily CTV panel from summarydata.all_facts (pre-aggregated).
-- channel_id 8 = CTV. uniques/site_visitors/new_site_visitors are HLL sketches (BYTES) -> HLL_COUNT.MERGE.
-- visit rate = site_visitors / uniques (per advertised unique, matches calc's "per advertised IP").
-- CVR = (view_conversions + click_conversions) / uniques (MNTN-attributed conversions per advertised unique).
SELECT
  DATE(hour)                                         AS dt,
  SUM(ctv_impressions)                               AS ctv_imps,
  SUM(ctv_spend)                                     AS ctv_spend_raw,
  SUM(media_spend)                                   AS media_spend_raw,
  HLL_COUNT.MERGE(uniques)                           AS adv_uniques,
  HLL_COUNT.MERGE(site_visitors)                     AS site_visitors,
  HLL_COUNT.MERGE(new_site_visitors)                 AS new_site_visitors,
  SUM(views)                                         AS views,
  SUM(clicks)                                        AS clicks,
  SUM(view_conversions)                              AS view_conv,
  SUM(click_conversions)                             AS click_conv,
  SUM(view_order_value)  + SUM(click_order_value)    AS order_value_usd
FROM `dw-main-silver.summarydata.all_facts`
WHERE advertiser_id = 51660
  AND channel_id = 8
  AND DATE(hour) >= '2026-02-15'
GROUP BY 1
ORDER BY 1;
