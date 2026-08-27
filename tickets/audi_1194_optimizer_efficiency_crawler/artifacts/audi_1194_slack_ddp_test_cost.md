# Slack draft, ml_squad owner (route via Sean Yang), re: ddp_vertical_classification_api dbt tests

Owner unidentified: dbt profile `ml_squad`, service principal `397d710b-4c85-4a96-b009-a07c1d373204`.
Evidence: `audi_1194_ddp_api_test_cost.md` (all figures re-derived before recording).
Everything below the marker is the message.

## Message

Hey, who owns the dbt tests on ddp_vertical_classification_api? Two asks:

1. Can the four tests run against just the latest load_ts partition instead of scanning the table? Each run reads 5.13 TB / 2.15M files with zero partitions pruned to return one row, because the filter is `WHERE load_ts = (SELECT max(load_ts) FROM <same table>)`, which Databricks can't prune on.

2. Do they need to run 20x a day? They run with every model build.

Together they're 98.6% of the query time on warehouse 14b311ac86ee2ca2, which lists at $850/week. The models themselves are about an hour of that.

## Evidence for follow-ups

- 142 runs per test over 7 days (20.3/day), 1,902 s mean execution, 0 s queue.
- Read growth tracks partition accumulation: 4.64 TB on 08-20 to 6.27 TB on 08-26.
- Cost is an attribution (billing has no per-query DBU): the four tests are 98.6% of the
  warehouse's query time; list price, no discount applied.
- The table is dropped and recreated ~21x/day (149 DROPs in 7 days), so EXPLAIN COST replay
  can never plan these statements after the fact.
