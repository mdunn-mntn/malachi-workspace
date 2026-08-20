Spark optimizer — 2026-08-20
25 Spark jobs scanned, 26 findings, 22 high. 38 active DAGs had no Spark task to profile.

New today
- HIGH site_network_hourly (https://cmcvcbd3j03vk01p91ksvm1vd.astronomer.run/dags/site_network_hourly) — Stage 9 spends 73% of task time waiting on shuffle fetch
- HIGH site_network_hourly (https://cmcvcbd3j03vk01p91ksvm1vd.astronomer.run/dags/site_network_hourly) — Executors 16% utilized: ~31 idle executor-hours held
- HIGH materialize_mntn_select (https://cmcvcbd3j03vk01p91ksvm1vd.astronomer.run/dags/materialize_mntn_select) — Stage 6 spends 78% of task time waiting on shuffle fetch
- HIGH `segment-updates-to-parquet` — Stage 2 spends 64% of task time waiting on shuffle fetch

Full backlog: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_2026-08-20.md`
Not scanned: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_coverage_2026-08-20.md`
