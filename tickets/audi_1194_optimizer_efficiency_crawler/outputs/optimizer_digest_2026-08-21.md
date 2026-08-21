Spark optimizer — 2026-08-21
194 Spark jobs scanned, 269 findings, 183 high. 38 active DAGs had no Spark task to profile.

New today
- HIGH `fangorn_score_monitor` — Stage 12 straggler: slowest task 12.1x the median on uniform data
- HIGH `fangorn_score_monitor` — Stage 17 spilled 1151.9 GiB to disk (3970 GiB in-memory at spill time)
- HIGH `fangorn_score_monitor` — Stage 17 wide shuffle (564 GiB, ~2255 MiB/partition)
- HIGH `fangorn_score_monitor` — Stage 19 spilled 714.1 GiB to disk (2662 GiB in-memory at spill time)
- HIGH `fangorn_score_monitor` — Stage 19 wide shuffle (395 GiB, ~1578 MiB/partition)
- HIGH `fangorn_score_monitor` — Stage 23 spilled 1573.6 GiB to disk (8581 GiB in-memory at spill time)
- HIGH `fangorn_score_monitor` — Stage 26 spilled 1707.7 GiB to disk (8867 GiB in-memory at spill time)
- HIGH `fangorn_score_monitor` — Executors 7% utilized: ~422 idle executor-hours held
- _…108 more in the full backlog_

Full backlog: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_2026-08-21.md`
Not scanned: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_coverage_2026-08-21.md`
