# Spark fleet optimizer backlog — 2026-08-10

Source: gs://mntn-data-archive-prod/spark-events (newest 40 logs, cap 40).

Fleet optimization: 37 jobs scanned, 50 findings, 34 high-impact.

- Populate fangorn_score_monitor.FangornScoreMonitor (app-20260810071933818-0388.zstd) [8 high, 8 total; code, infra] -> top: Stage 12 straggler: slowest task 22.0x the median on uniform data
- Populate intent_score_household_map.IntentScoreHouseholdMap (app-20260810074055628-0046.zstd) [5 high, 7 total; code, infra] -> top: Stage 10 wide shuffle (3675 GiB, ~3763 MiB/partition)
- Populate intent_score_map.IntentScoreMap (eventlog_v2_batch-84b74adf-4a10-48d1-b4b3-a9eb03142d28) [5 high, 5 total; code] -> top: Stage 2 spilled 2456.0 GiB to disk (14001 GiB in-memory at spill time)
- Populate site_network_hourly.SiteNetworkHourly (app-20260810145126045-0866.zstd) [2 high, 3 total; code, infra] -> top: Stage 35 spends 56% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260810055114094-0232.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 58% of task time waiting on shuffle fetch
- Populate advertiser_score_distribution_monitor.AdvertiserScoreDistributionMonitor (app-20260810071212658-0599.zstd) [2 high, 2 total; code, infra] -> top: Stage 1 wide shuffle (221 GiB, ~1772 MiB/partition)
- Populate site_network_hourly.SiteNetworkHourly (app-20260810115119684-0850.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 62% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810161700598-0555.zstd) [1 high, 3 total; code, infra] -> top: Executors 12% utilized: ~30 idle executor-hours held
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810081620008-0980.zstd) [1 high, 2 total; code, infra] -> top: Executors 6% utilized: ~47 idle executor-hours held
- Populate site_network_hourly.SiteNetworkHourly (app-20260810155117423-0946.zstd) [1 high, 2 total; code, infra] -> top: Executors 3% utilized: ~205 idle executor-hours held
- Populate tpa_export_enrich.TpaExportEnrich (app-20260810073935658-0021.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 73% of task time waiting on shuffle fetch
- Populate aug_log_ip_hourly.AugLogIpHourly (app-20260810081626644-0603.zstd) [1 high, 1 total; infra] -> top: Executors 8% utilized: ~48 idle executor-hours held
- Populate site_network_hourly.SiteNetworkHourly (app-20260810135215692-0594.zstd) [1 high, 1 total; code] -> top: Stage 9 spends 52% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810141701853-0507.zstd) [1 high, 1 total; infra] -> top: Stage 7 straggler: slowest task 36.8x the median on uniform data
- Populate aug_log_ip_hourly.AugLogIpHourly (app-20260810161706965-0069.zstd) [1 high, 1 total; infra] -> top: Executors 7% utilized: ~28 idle executor-hours held
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810091614906-0706.zstd) [0 high, 2 total; code] -> top: Stage 11 spends 30% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810061628907-0682.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 34% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260810075200154-0659.zstd) [0 high, 1 total; code] -> top: Stage 9 spends 46% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810101630731-0024.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 34% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810131635599-0983.zstd) [0 high, 1 total; code] -> top: Stage 35 spends 33% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810151625110-0440.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 32% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260810165132171-0753.zstd) [0 high, 1 total; code] -> top: Stage 9 spends 50% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260810171622372-0067.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 36% of task time waiting on shuffle fetch
- app-20260810061526473-0432.zstd: clean
- app-20260810061630664-0645.zstd: clean
- app-20260810071615440-0944.zstd: clean
- app-20260810071628575-0510.zstd: clean
- app-20260810091625151-0254.zstd: clean
- app-20260810101627746-0129.zstd: clean
- app-20260810111618270-0410.zstd: clean
- app-20260810111633643-0139.zstd: clean
- app-20260810121623586-0529.zstd: clean
- app-20260810121628637-0307.zstd: clean
- app-20260810131639718-0173.zstd: clean
- app-20260810141709338-0288.zstd: clean
- app-20260810151629166-0632.zstd: clean
- app-20260810171619140-0914.zstd: clean
