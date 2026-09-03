# BigQuery cost — 2026-09-01

Slot-hours per dag and task, from the job history of the fleet's own service account. Jobs submitted without airflow labels appear as `unattributed`.

| DAG | task | jobs | slot-hours | TiB billed |
|---|---|---:|---:|---:|
| `bos__spend` | `campaign_summary_hourly-create` | 279 | 1,083.7 | 37.81 |
| `bos__spend` | `flight_metrics_per2388-create` | 93 | 1,022.6 | 1,307.72 |
| `unattributed` | `-` | 620 | 977.6 | 53.62 |
| `intent_score_threshold_v4` | `population_histogram` | 4 | 844.7 | 90.85 |
| `category_taxonomy` | `prepare_categories-liveramp_categories` | 14 | 2.1 | 0.23 |
| `category_taxonomy` | `prepare_categories-copy_view` | 1 | 0.7 | 0.02 |
