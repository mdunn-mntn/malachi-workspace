-- PS-8572 check 1b: change history of block config for Lovepop (58797)
-- Archive of integrationprod.audience_advertiser_configurations (schema match:
-- block_* + lookback windows + version; no audience_-prefixed archives table exists).
-- Archive row create_time = when that superseded version was archived.
-- Full history pulled (dim-scale table) so "since when" is provable; Jan 2026..now
-- window inspected downstream for toggles inside 2026-06-01..2026-08-04.
SELECT
  advertiser_configuration_archive_id,
  version,
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
  create_time
FROM `dw-main-bronze.integrationprod.archives_advertiser_configuration_archives`
WHERE advertiser_id = 58797
ORDER BY create_time, version
LIMIT 200
