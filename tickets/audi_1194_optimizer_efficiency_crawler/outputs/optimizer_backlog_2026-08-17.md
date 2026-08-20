# Spark fleet optimizer backlog — 2026-08-17

Source: gs://mntn-data-archive-prod/spark-events (newest 40 logs, cap 40).

Fleet optimization: 37 jobs scanned, 59 findings, 42 high-impact.

- Populate fangorn_score_monitor.FangornScoreMonitor (app-20260817065653428-0581.zstd) [8 high, 8 total; code, infra] -> top: Stage 12 straggler: slowest task 13.9x the median on uniform data
- Populate intent_score_household_map.IntentScoreHouseholdMap (app-20260817071809692-0670.zstd) [7 high, 7 total; code] -> top: Stage 10 wide shuffle (3602 GiB, ~3688 MiB/partition)
- Populate intent_score_map.IntentScoreMap (eventlog_v2_batch-4c03c747-54a0-4f3d-8352-bf6f63d6a6ef) [5 high, 5 total; code] -> top: Stage 2 spilled 3366.4 GiB to disk (18886 GiB in-memory at spill time)
- Populate advertiser_score_distribution_monitor.AdvertiserScoreDistributionMonitor (app-20260817064946826-0673.zstd) [2 high, 2 total; code, infra] -> top: Stage 1 wide shuffle (200 GiB, ~1600 MiB/partition)
- Populate site_network_hourly.SiteNetworkHourly (app-20260817065122856-0420.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 73% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817085115734-0691.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 64% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817095127441-0216.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 50% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817115117599-0801.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 59% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817135123674-0184.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 57% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817165118897-0713.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 62% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817141619431-0922.zstd) [1 high, 3 total; code, infra] -> top: Executors 7% utilized: ~28 idle executor-hours held
- Populate prospecting_join.ProspectingJoin (app-20260817054824176-0461.zstd) [1 high, 2 total; code, infra] -> top: Stage 57 wide shuffle (10103 GiB, ~517 MiB/partition)
- Populate tpa_export_enrich.TpaExportEnrich (app-20260817071825701-0182.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 71% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817111625858-0655.zstd) [1 high, 1 total; infra] -> top: Executors 5% utilized: ~21 idle executor-hours held
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817121650938-0621.zstd) [1 high, 1 total; infra] -> top: Executors 4% utilized: ~27 idle executor-hours held
- Populate aug_log_ip_hourly.AugLogIpHourly (app-20260817121710341-0910.zstd) [1 high, 1 total; infra] -> top: Executors 9% utilized: ~31 idle executor-hours held
- Populate aug_log_ip_hourly.AugLogIpHourly (app-20260817141614963-0137.zstd) [1 high, 1 total; infra] -> top: Executors 11% utilized: ~29 idle executor-hours held
- Populate aug_log_ip_hourly.AugLogIpHourly (app-20260817171621746-0191.zstd) [1 high, 1 total; infra] -> top: Executors 7% utilized: ~52 idle executor-hours held
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817091611166-0973.zstd) [0 high, 2 total; code] -> top: Stage 11 spends 34% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817101626633-0778.zstd) [0 high, 2 total; code] -> top: Stage 11 spends 33% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817145127941-0376.zstd) [0 high, 2 total; code, infra] -> top: Stage 9 spends 44% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817161621872-0879.zstd) [0 high, 2 total; code] -> top: Stage 11 spends 37% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817071623218-0643.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 34% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817081628269-0209.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 45% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817125114709-0168.zstd) [0 high, 1 total; code] -> top: Stage 9 spends 44% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817131624980-0202.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 31% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817151619594-0605.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 32% of task time waiting on shuffle fetch
- Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly (app-20260817171629738-0984.zstd) [0 high, 1 total; code] -> top: Stage 11 spends 38% of task time waiting on shuffle fetch
- app-20260817071620906-0254.zstd: clean
- app-20260817081700913-0329.zstd: clean
- app-20260817091617966-0978.zstd: clean
- app-20260817101634523-0484.zstd: clean
- app-20260817105117932-0664.zstd: clean
- app-20260817111626229-0347.zstd: clean
- app-20260817131639658-0670.zstd: clean
- app-20260817151617006-0236.zstd: clean
- app-20260817161632995-0646.zstd: clean
