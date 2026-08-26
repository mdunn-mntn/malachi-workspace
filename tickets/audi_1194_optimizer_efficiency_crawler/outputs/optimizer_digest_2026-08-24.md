Spark optimizer — 2026-08-24
217 Spark jobs scanned, 281 findings, 194 high. 39 active DAGs had no Spark task to profile.

New today
- HIGH `Run Single-Day TPA Export for 2026-08-23` — Stage 40 spends 72% of task time waiting on shuffle fetch
- HIGH `Run Single-Day TPA Export for 2026-08-23` — Stage 45 spends 71% of task time waiting on shuffle fetch
- HIGH `Run Single-Day TPA Export for 2026-08-23` — Stage 52 spends 92% of task time waiting on shuffle fetch
- HIGH `Run Single-Day TPA Export for 2026-08-23` — Stage 61 spends 94% of task time waiting on shuffle fetch
- HIGH `Run Single-Day TPA Export for 2026-08-23` — Stage 72 spends 94% of task time waiting on shuffle fetch
- HIGH `Run Single-Day TPA Export for 2026-08-23` — Stage 85 spends 95% of task time waiting on shuffle fetch
- HIGH `Run Single-Day TPA Export for 2026-08-23` — Stage 100 spends 78% of task time waiting on shuffle fetch
- HIGH `Run Single-Day TPA Export for 2026-08-23` — Stage 117 spends 96% of task time waiting on shuffle fetch
- _…23 more in the full backlog_

Chronic
- HIGH `site_network_hourly` — Stage 9 spends 71% of task time waiting on shuffle fetch  _day 3_
- HIGH `site_network_hourly` — Executors 11% utilized: ~52 idle executor-hours held  _day 3_

Full backlog: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_2026-08-24.md`
Not scanned: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_coverage_2026-08-24.md`
