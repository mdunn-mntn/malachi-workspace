# BigQuery cost — 2026-08-31

Slot-hours per dag and task, from the job history of the fleet's own service account. Jobs submitted without airflow labels appear as `unattributed`.

| DAG | task | jobs | slot-hours | TiB billed |
|---|---|---:|---:|---:|
| `intent_score_threshold_v4` | `population_histogram` | 4 | 1,120.7 | 100.39 |
| `bos__spend` | `campaign_summary_hourly-create` | 282 | 1,100.2 | 40.51 |
| `unattributed` | `-` | 592 | 1,009.8 | 62.09 |
| `bos__spend` | `flight_metrics_per2388-create` | 94 | 833.5 | 1,320.59 |
| `category_taxonomy` | `prepare_categories-liveramp_categories` | 14 | 1.9 | 0.31 |
| `category_taxonomy` | `prepare_categories-copy_view` | 1 | 0.6 | 0.02 |
