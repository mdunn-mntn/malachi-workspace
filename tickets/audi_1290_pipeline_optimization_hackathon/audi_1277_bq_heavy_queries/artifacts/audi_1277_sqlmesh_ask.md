# Ask to Data Platform: materialize `logdata.spend_pacing` (draft, not sent)

To: Data Platform (SQLMesh model owner `ber`), cc Forrest Bajbek (Pacing, consumer via `bos__spend`)
Channel: #data-platform
Ticket: AUDI-1277 (epic AUDI-1290, hackathon cost lever)

---

1. Can Data Platform change `models/dw-main-silver/logdata/spend_pacing.sql` from `kind VIEW` to an incremental table (`INCREMENTAL_BY_TIME_RANGE` on `time_hr`, `cron '*/15 * * * *'`, lookback covering the 2-day `buffer_dt`), or add a sibling `logdata.spend_pacing_materialized` with that kind so consumers can repoint?

2. If yes, do you want the SQLMesh PR from us (we have push on `SteelHouse/sqlmesh`) or would you rather author it? Nothing in the view's business logic changes: same 24h live window, same margins, unlinked and PSA handling.

Why: `bos__spend` re-evaluates the view 96 times a day. One run of its `campaign_summary_hourly.create` job is 6.9 slot-hours over 0.23 TiB, and 81 percent of that is the view itself (three hive-partitioned external parquet reads, `impression__v1` read three times for 1.5 billion rows, 17 dimension joins, 259 plan stages). Across the day that is about 517 slot-hours, all spent recomputing the same 2-day window. Every other reader of the view pays the same per read.

Evidence: `tickets/audi_1290_pipeline_optimization_hackathon/audi_1277_bq_heavy_queries/outputs/audi_1277_plan_csh.json` (full plan of one run) and `audi_1277_csh_stage_attribution.txt` (slot attribution per source: 76.2 percent view logs and dims, 4.7 percent dims only, 13.3 percent after the union with the cost log, 5.7 percent cost log).

What we will not touch: the 24h live window (billing safety buffer, agreed in #data-platform 2026-06-08) and the 5-day cost log lookback (PER-6212).
