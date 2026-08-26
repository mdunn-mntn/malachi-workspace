# ddp_vertical_classification_api: four dbt tests scan the same 5 TiB, 20x a day

Measured 2026-08-26 from `system.query.history` joined to `system.billing.usage` and
`system.billing.list_prices`. Reproduce with `airflow_optimizer.databricks.query_costs`.

## What it costs

| | |
|---|---|
| tests | 4 (`failure_rate`, `status_code__not_null`, `ecomm__response_content`, `lookup__response_content`) |
| runs per test, 7 days | 148 (~20/day, steady across all 7 days) |
| execution time per run | **~1,940 s** (32 min), of which queue time is 10 s |
| bytes read per run | **~5.1 TiB** |
| files read per run | **2,218,500** |
| partitions read per run | 1,028 |
| rows produced per run | **1** |
| warehouse-hours, 7 days | **327** on `14b311ac86ee2ca2` |
| list-price dollars, 7 days | **~$870** |

The dollar figure is the warehouse's daily bill apportioned to each statement by its share of
that day's query time. A warehouse bills by the hour, never per statement, so this attributes
rather than measures, and a contract rate below list makes the real number lower.

## The mechanism

All four tests read `prod.ml.ddp_vertical_classification_api` under the same snapshot filter:

```sql
where load_ts = (select max(load_ts) from `prod`.`ml`.`ddp_vertical_classification_api`)
```

Two facts follow from the numbers, not from reading the SQL:

- **The four runs are identical scans.** Read bytes, file count and partition count match to
  within 0.3% across all four tests. Each independently re-reads the whole table to emit one row,
  so three of the four scans are duplicated work.
- **The snapshot filter prunes nothing.** 1,028 partitions and 2.2M files are read on every run.
  Whatever the table is partitioned on, it is not being used to isolate the newest `load_ts`.

## What is not established

Why the filter fails to prune. The table is dropped and recreated ~20x/day
(`model.ml_squad.ddp_vertical_classification_api` is a bare `drop table if exists`, and the
dbt step itself takes 1 s), so it did not exist at measurement time and neither
`DESCRIBE DETAIL` nor `information_schema.columns` returns anything for it. The partition column
is therefore unconfirmed. That also means `EXPLAIN COST` cannot replay these statements: the
table a historical query referenced is gone by the time the plan is asked for.

## Routing

Owner not identified. The dbt profile is `ml_squad`, target `prod_warehouse_small` for the tests
and `prod_warehouse_2xs` for the model; every run executes as service principal
`397d710b-4c85-4a96-b009-a07c1d373204`.
