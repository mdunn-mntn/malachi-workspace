# BigQuery cost — 2026-08-28

Slot-hours per dag and task, from the job history of the fleet's own service account. Jobs submitted without airflow labels appear as `unattributed`.

| DAG | task | jobs | slot-hours | TiB billed |
|---|---|---:|---:|---:|
| `bos__spend` | `campaign_summary_hourly-create` | 288 | 1,245.8 | 47.20 |
| `intent_score_threshold_v4` | `population_histogram` | 4 | 1,231.6 | 98.78 |
| `unattributed` | `-` | 601 | 1,169.4 | 69.00 |
| `bos__spend` | `flight_metrics_per2388-create` | 96 | 979.3 | 1,344.70 |
| `category_taxonomy` | `prepare_categories-liveramp_categories` | 14 | 5.6 | 0.82 |
| `category_taxonomy` | `prepare_categories-copy_view` | 1 | 0.7 | 0.02 |
