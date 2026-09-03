# BigQuery cost — 2026-09-02

Slot-hours per dag and task, from the job history of the fleet's own service account. Jobs submitted without airflow labels appear as `unattributed`.

| DAG | task | jobs | slot-hours | TiB billed |
|---|---|---:|---:|---:|
| `intent_score_threshold_v4` | `population_histogram` | 4 | 1,056.6 | 103.06 |
| `bos__spend` | `campaign_summary_hourly-create` | 240 | 844.9 | 30.08 |
| `bos__spend` | `flight_metrics_per2388-create` | 80 | 786.1 | 1,125.77 |
| `unattributed` | `-` | 527 | 779.7 | 47.43 |
| `category_taxonomy` | `prepare_categories-liveramp_categories` | 14 | 14.7 | 2.59 |
| `category_taxonomy` | `prepare_categories-copy_view` | 1 | 0.7 | 0.02 |
