# ddp_vertical_classification_api: four dbt tests re-scan the same table 20x a day

Measured 2026-08-26 from `system.query.history` and `system.billing` (usage + list_prices),
window 2026-08-20 to 2026-08-26. Every figure was re-derived independently before being recorded;
the first draft of this document overstated several and they are corrected here.

## Per run, per test

| | |
|---|---|
| tests | 4 (`failure_rate`, `status_code__not_null`, `ecomm__response_content`, `lookup__response_content`) |
| runs per test | 142 over 7 days, **20.3/day** |
| execution time | **1,902 s** mean (1,884 s over the 550 runs that finished) |
| queue time | **0.00 s** — `waiting_for_compute` and `waiting_at_capacity` are both zero |
| bytes read | **5.13 TB (4.67 TiB)** |
| files read | **2.15M**, of which **0 pruned** |
| partitions read | **997** |
| rows produced | **1** |

Volume grows across the window as `load_ts` partitions accumulate: 4.64 TB / 2.02M files / 935
partitions on 08-20, to 6.27 TB / 2.29M files / 1,059 partitions on 08-26.

## What it costs

The warehouse is `14b311ac86ee2ca2`, a SMALL single-cluster serverless SQL warehouse with a
5-minute auto-stop. Its **full 7-day list cost is $850** (1,214.82 DBU x $0.70, no discount;
~$866 extrapolating the 3 hours of usage records not yet landed).

Charging that to these four tests is an attribution, not a measurement — `system.billing` carries
no per-query DBU. It is defensible because the four tests are **98.6% of the warehouse's query
execution time and 99.9% of its bytes read**, so the warehouse would otherwise be near idle and
the marginal figure is close to the full $850. Allocating strictly by wall-clock share gives $698.

**Do not sum the four tests' durations.** They launch within 30 ms of each other and run
concurrently on one cluster, so summing double-counts roughly 4x: 297 execution-hours summed
against a **union of 83.0 wall-clock hours**, and 101.2 warehouse running hours implied by
billing. An earlier draft reported "327 warehouse-hours"; that was summed query duration over 8
calendar days and is wrong by 3.2x.

Not counted above: the companion node `model.ml_squad.ddp_vertical_classification_api` issues its
DROPs on warehouse `fa27430dfc609e6d`, another **404.59 DBU / $283** over the same window.

## The mechanism

All four tests read `prod.ml.ddp_vertical_classification_api` under the same snapshot filter:

```sql
where load_ts = (select max(load_ts) from `prod`.`ml`.`ddp_vertical_classification_api`)
```

Two things follow from the numbers rather than from reading the SQL:

- **The four runs are the same scan, four times.** Read bytes, file count and partition count
  match to within 0.3% across all four. Each independently re-reads the whole table to emit one
  row, so three of the four scans are duplicated work.
- **The snapshot filter prunes nothing.** `pruned_files` is 0 on every run; all 2.15M files and
  997 partitions are read. Whatever the table is partitioned on is not isolating the newest
  `load_ts`.

## What is not established

Why the filter fails to prune. The dbt step for the model is a bare
`drop table if exists` (1 s), and 150 such DROPs land over 7 days, 20.7/day across the six full
days. The table therefore did not exist at measurement time and neither `DESCRIBE DETAIL` nor
`information_schema.columns` returns anything for it, so the partition column is unconfirmed.

The recreate is inferred, not observed: 582 successful SELECTs run against the table over the
same window, but `system.query.history` records no CREATE, CTAS, REPLACE or INSERT for it, so the
rebuild runs off the SQL warehouse. That is also why `EXPLAIN COST` cannot replay these
statements — the table a historical query referenced is gone by the time the plan is asked for.

## Routing

Owner not identified. The dbt profile is `ml_squad`, target `prod_warehouse_small` for the tests
and `prod_warehouse_2xs` for the model; every run executes as service principal
`397d710b-4c85-4a96-b009-a07c1d373204`.
