-- PS-8572 check 1b: current advertiser block config for Lovepop (58797)
-- Source of truth: dw-main-silver.audience.advertiser_configurations (view over
-- bronze integrationprod.audience_advertiser_configurations, fresh daily).
-- Absence of a row = defaults ON at 30/30.
SELECT
  advertiser_id,
  block_conversion,
  block_first_party,
  block_prospecting,
  conversion_lookback_window,
  page_view_lookback_window,
  enable_taxonomy_block,
  enable_advertiser_verticals,
  enable_retargeting_mutual_exclusion,
  enable_audience_isolation,
  rt_campaign_isolation_enabled,
  vertical_data_source,
  create_time,
  update_time
FROM `dw-main-silver.audience.advertiser_configurations`
WHERE advertiser_id = 58797
LIMIT 10
