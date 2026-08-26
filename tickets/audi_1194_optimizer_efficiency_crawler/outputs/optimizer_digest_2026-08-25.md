Spark optimizer — 2026-08-25
214 Spark jobs scanned, 277 findings, 179 high. 40 DAGs had no Spark task to profile.

New today
- HIGH `intent_score_household_map` — Stage 19 straggler: slowest task 11.7x the median on uniform data
- HIGH `ipdsc_third_party_audience_builder` — Stage 5 wide shuffle (340 GiB, ~681 MiB/partition)
- HIGH `ipdsc_third_party_audience_builder` — Stage 14 spends 81% of task time waiting on shuffle fetch
- HIGH `ipdsc_third_party_audience_builder` — Stage 113 spends 62% of task time waiting on shuffle fetch
- HIGH `ipdsc_14_monitor` — Stage 14 spends 76% of task time waiting on shuffle fetch
- HIGH `identity_targeted_signal` — Stage 1 straggler: slowest task 10.8x the median on uniform data
- MED `site_visit_signal_advertiser_id_dsc_id` — Stage 8 skewed 5.1x (max vs median task)
- MED `ipdsc_third_party_audience_builder` — Stage 44 spends 46% of task time waiting on shuffle fetch
- _…4 more in the full backlog_

Chronic
- HIGH `ipdsc_ds_67` — Stage 3 spilled 83.2 GiB to disk (127 GiB in-memory at spill time)  _day 3_
- HIGH `ipdsc_ds_67` — Stage 5 spilled 80.5 GiB to disk (126 GiB in-memory at spill time)  _day 3_
- HIGH `ipdsc_ds_67` — Stage 13 straggler: slowest task 38.3x the median on uniform data  _day 3_
- HIGH `ipdsc_ds_67` — Stage 13 spends 62% of task time waiting on shuffle fetch  _day 3_
- HIGH `ipdsc_ds_67` — Stage 23 skewed 245.1x (max vs median task)  _day 3_
- HIGH `ipdsc_ds_67` — Stage 37 straggler: slowest task 40.4x the median on uniform data  _day 3_
- HIGH `ipdsc_ds_67` — Stage 37 spends 73% of task time waiting on shuffle fetch  _day 3_
- HIGH `fangorn_score_monitor` — Stage 17 spilled 1138.4 GiB to disk (3980 GiB in-memory at spill time)  _day 3_
- _…97 more in the full backlog_

Full backlog: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_2026-08-25.md`
Not scanned: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_coverage_2026-08-25.md`
