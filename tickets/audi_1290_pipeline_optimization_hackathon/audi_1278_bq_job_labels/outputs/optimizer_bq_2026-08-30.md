# BigQuery cost — 2026-08-30

Slot-hours per dag and task, from the job history of the fleet's own service account. Jobs submitted without airflow labels appear as `unattributed`.

| DAG | task | jobs | slot-hours | TiB billed |
|---|---|---:|---:|---:|
| `bos__spend` | `campaign_summary_hourly-create` | 288 | 1,275.2 | 43.40 |
| `unattributed` | `-` | 607 | 1,184.8 | 66.93 |
| `intent_score_threshold_v4` | `population_histogram` | 4 | 1,075.4 | 99.06 |
| `bos__spend` | `flight_metrics_per2388-create` | 96 | 976.9 | 1,347.42 |
| `category_taxonomy` | `prepare_categories-liveramp_categories` | 14 | 5.9 | 1.00 |
| `category_taxonomy` | `prepare_categories-copy_view` | 1 | 0.7 | 0.02 |
