# Optimization implementation queue — one PR per item, 2026-08-27

Ranked by executor-hours at stake. Every item names the repo and the file the change lands in,
and its gate: READY means the mechanism is settled and a PR can be drafted; OWNER means the
mechanism needs the owner's answer first (the site_network_hourly lesson: two recommendations
were refuted by measurement before reaching an owner, so nothing ships on a detector's stock
text alone). The tool itself never opens these PRs; each is authored and gauntleted by a session.

| # | Target | Finding (measured) | Change | Repo / file | Gate |
|---|---|---|---|---|---|
| 1 | `site_network_hourly` (tpa_ipdsc_export family) | 21,200 exec-h / 23 days, 86% idle tail, peak saturates 1,988 slots | Whatever holds executors after the burst: idle-timeout / shuffle-tracking answer decides | airflow-ti `spark/` ipdsc site_network config (via `ipdsc_emr_cluster.py` path) | OWNER: draft ask staged `audi_1194_slack_ryan_idle_tail.md` |
| 2 | `ddp_vertical_classification_api` dbt tests | 4 tests = 98.6% of an $850/week warehouse, 5.13 TB scan for 1 row | Partition-prunable filter (literal latest `load_ts`, or dbt var) and/or drop to daily | SteelHouse/dbt `ml_squad` models tests | OWNER: draft ask staged `audi_1194_slack_ddp_test_cost.md` |
| 3 | `audience_intent / fangorn_score_monitor` | Stage 17 spills 1,138 GiB to disk, 954 exec-h worst run, 4 sweeps running | Raise `spark.sql.shuffle.partitions`; then executor memory if it persists | airflow-ti `models/monitoring/fangorn_score_monitor.py` | DONE on branch `audi-1194-model-tuning` (own PR): 256 -> 2048 in decorator and builder |
| 4 | `tpa_ipdsc_export / ipdsc_ds_35` | Stage 2 straggler 118.8x median on uniform data, 348 exec-h worst run | `spark.speculation=true`, `spark.speculation.quantile=0.9` | airflow-ti `models/ipdsc/ipdsc_ds_35.py` | DONE on branch `audi-1194-model-tuning` (own PR): speculation on, quantile 0.9 |
| 5 | `audience_intent_scoring_staging_ds46` | Stage 20 spills 2,210 GiB, 218 exec-h worst run | Shuffle partitions first, then memory | airflow-ti scoring staging model | READY after the two-DAG name ambiguity is pinned to one DAG |
| 6 | `Update Vertical Categorization` | Chronic stage-0 skew 10-242x on every run | Salt or repartition the skewed key; owner routing via Sean/DDP | SteelHouse/dbt DDP model | OWNER |
| 7 | `aug_log_ip*` hourly family | 2-8% utilization, 20-61 idle exec-h per run, chronic | Same idle-tail mechanism as #1; answer likely transfers | airflow-ti feature-store configs | OWNER (rides on #1's answer) |

Working rules for this queue: verify the mechanism from the event log before writing any config
number; DAG-fix PRs stay separate from tool PRs (user rule, 2026-08-27); gauntlet once at the end; the fix lands with
`python3 -m airflow_optimizer.ledger applied <dag> <key> <pr> <date>` so the savings log starts
measuring it the next sweep.
