# Spark fleet optimizer backlog — 2026-08-20

Source: gs://mntn-data-archive-prod/spark-events (3 site_network logs) + 22 PHS batch log(s).

Fleet optimization: 25 jobs scanned, 26 findings, 22 high-impact.

- Populate site_network_hourly.SiteNetworkHourly (app-20260817065122856-0420.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 73% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817085115734-0691.zstd) [2 high, 2 total; code, infra] -> top: Stage 9 spends 64% of task time waiting on shuffle fetch
- materialize_mntn_select_16 (app-20260820174646502-0205.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 78% of task time waiting on shuffle fetch
- materialize_mntn_select_12 (app-20260820134653536-0627.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 61% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[19] (app-20260820205134832-0290.zstd) [1 high, 1 total; code] -> top: Stage 2 spends 64% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[12] (app-20260820135140323-0777.zstd) [1 high, 1 total; code] -> top: Stage 2 spends 67% of task time waiting on shuffle fetch
- materialize_mntn_select_13 (app-20260820144649865-0012.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 63% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[14] (app-20260820155132581-0098.zstd) [1 high, 1 total; code] -> top: Stage 2 spends 63% of task time waiting on shuffle fetch
- materialize_mntn_select_10 (app-20260820114650301-0055.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 56% of task time waiting on shuffle fetch
- materialize_mntn_select_17 (app-20260820184637652-0845.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 74% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[13] (app-20260820145136534-0170.zstd) [1 high, 1 total; code] -> top: Stage 2 spends 55% of task time waiting on shuffle fetch
- materialize_mntn_select_14 (app-20260820154654275-0468.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 59% of task time waiting on shuffle fetch
- materialize_mntn_select_15 (app-20260820164645232-0699.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 62% of task time waiting on shuffle fetch
- materialize_mntn_select_9 (app-20260820104710077-0018.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 56% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[18] (app-20260820195137884-0911.zstd) [1 high, 1 total; code] -> top: Stage 2 spends 56% of task time waiting on shuffle fetch
- materialize_mntn_select_19 (app-20260820204650525-0558.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 62% of task time waiting on shuffle fetch
- materialize_mntn_select_11 (app-20260820124642589-0416.zstd) [1 high, 1 total; code] -> top: Stage 6 spends 69% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[11] (app-20260820125147587-0479.zstd) [1 high, 1 total; code] -> top: Stage 2 spends 54% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[17] (app-20260820185135863-0790.zstd) [1 high, 1 total; code] -> top: Stage 2 spends 62% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[16] (app-20260820175132412-0211.zstd) [1 high, 1 total; code] -> top: Stage 2 spends 56% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[10] (app-20260820115147012-0124.zstd) [0 high, 1 total; code] -> top: Stage 2 spends 36% of task time waiting on shuffle fetch
- materialize_mntn_select_18 (app-20260820194653466-0919.zstd) [0 high, 1 total; code] -> top: Stage 6 spends 40% of task time waiting on shuffle fetch
- segment-updates-to-parquet-2026-08-20-[15] (app-20260820165235535-0994.zstd) [0 high, 1 total; code] -> top: Stage 2 spends 44% of task time waiting on shuffle fetch
- Populate site_network_hourly.SiteNetworkHourly (app-20260817125114709-0168.zstd) [0 high, 1 total; code] -> top: Stage 9 spends 44% of task time waiting on shuffle fetch
- app-20260820105133544-0789.zstd: clean
