# BigQuery cost — 2026-08-29

Slot-hours per dag and task, from the job history of the fleet's own service account. Jobs submitted without airflow labels appear as `unattributed`.

| DAG | task | jobs | slot-hours | TiB billed |
|---|---|---:|---:|---:|
| `bos__spend` | `campaign_summary_hourly-create` | 288 | 1,270.4 | 43.94 |
| `unattributed` | `-` | 611 | 1,182.0 | 67.43 |
| `intent_score_threshold_v4` | `population_histogram` | 4 | 987.4 | 96.74 |
| `bos__spend` | `flight_metrics_per2388-create` | 96 | 939.1 | 1,346.05 |
| `category_taxonomy` | `prepare_categories-liveramp_categories` | 14 | 6.9 | 1.28 |
| `category_taxonomy` | `prepare_categories-copy_view` | 1 | 0.6 | 0.02 |
