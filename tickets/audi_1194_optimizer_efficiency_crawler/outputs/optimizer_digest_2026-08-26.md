Spark optimizer — 2026-08-26
252 Spark jobs scanned, 269 findings, 159 high. 35 DAGs had no Spark task to profile.

  What  Stage 17 spilled 1138.4 GiB to disk (3980 GiB in-memory at spill time) (+6 more findings on this DAG)
  Where `fangorn_score_monitor` · `app-20260825071531026-0250.zstd`
  Why   HIGH impact, 433 executor-hours in its worst run, firing 4 sweeps running
  How   Raise spark.sql.shuffle.partitions (smaller partitions) first; if it persists, raise executor memory.

  What  Stage 2 straggler: slowest task 118.8x the median on uniform data (+3 more findings on this DAG)
  Where `ipdsc_ds_35` · `app-20260826033629022-0370.zstd`
  Why   HIGH impact, 348 executor-hours in its worst run, firing 4 sweeps running
  How   Enable spark.speculation=true (with spark.speculation.quantile ~0.9) so a straggling task is re-launched on an idle executor instead of pinning the stage.

  What  Stage 9 spends 68% of task time waiting on shuffle fetch (+4 more findings on this DAG)
  Where `site_network_hourly` · `app-20260825065124173-0803.zstd`
  Why   HIGH impact, 276 executor-hours in its worst run, firing 5 sweeps running
  How   Check which executors hold the map output before changing partition counts. If it is concentrated (the map stage ran while the fleet was still scaling up), raise dynamicAllocation.initialExecutors so the map stage spreads its output; raising spark.sql.shuffle.partitions then makes it WORSE by multiplying block count. Raise partitions only when the blocks themselves are large.

Stopped firing — advertiser_join, intent_score_household_map, ipdsc_46_monitor, prospecting_join

Full backlog: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_2026-08-26.md`
Not scanned: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_coverage_2026-08-26.md`
