# One Databricks grant ask, everything the optimizer will ever need

Run once, by an **account admin**, in the Databricks **SQL Editor**
(`https://1262887251702944.4.gcp.databricks.com` → SQL Editor → pick any running warehouse →
paste → Run all). Anyone below that tier gets
`PERMISSION_DENIED: User is not an account admin for Account`.

Two principals: `malachi@mountain.com` (a person, for analysis) and
`07f36af7-614d-4d57-8143-2dbcd3cb58c2` (the `spark_optimizer` service principal, for the
scheduled job). Both are needed; the SP is what runs unattended.

## The statements — everything, one paste

```sql
-- system schemas: cost, allocation, lineage, table size
GRANT USE SCHEMA ON SCHEMA system.billing TO `malachi@mountain.com`;
GRANT SELECT     ON SCHEMA system.billing TO `malachi@mountain.com`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;
GRANT SELECT     ON SCHEMA system.billing TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;

GRANT USE SCHEMA ON SCHEMA system.compute TO `malachi@mountain.com`;
GRANT SELECT     ON SCHEMA system.compute TO `malachi@mountain.com`;
GRANT USE SCHEMA ON SCHEMA system.compute TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;
GRANT SELECT     ON SCHEMA system.compute TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;

GRANT USE SCHEMA ON SCHEMA system.access TO `malachi@mountain.com`;
GRANT SELECT     ON SCHEMA system.access TO `malachi@mountain.com`;
GRANT USE SCHEMA ON SCHEMA system.access TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;
GRANT SELECT     ON SCHEMA system.access TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;

GRANT USE SCHEMA ON SCHEMA system.storage TO `malachi@mountain.com`;
GRANT SELECT     ON SCHEMA system.storage TO `malachi@mountain.com`;
GRANT USE SCHEMA ON SCHEMA system.storage TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;
GRANT SELECT     ON SCHEMA system.storage TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;

-- prod catalog, read only, for the service principal only.
-- EXPLAIN COST plans a query against the real tables, so without this the scheduled
-- job reads the SQL and then cannot plan it. Malachi already has this via producers_dev.
GRANT USE CATALOG ON CATALOG prod TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;
GRANT SELECT      ON CATALOG prod TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;

SHOW GRANTS ON SCHEMA system.billing;
SHOW GRANTS ON SCHEMA system.compute;
SHOW GRANTS ON SCHEMA system.access;
SHOW GRANTS ON SCHEMA system.storage;
SHOW GRANTS ON CATALOG prod;
```

The five `SHOW GRANTS` are the point: a GRANT that does nothing still prints `OK`, and three of
four lines landing silently is the failure we hit twice.

## Why each

| Object | Tables that matter | What the optimizer gets |
|---|---|---|
| `system.billing` | `usage`, `attributed_usage`, `list_prices`, `account_prices` | What a job actually cost, instead of executor-hours as a proxy |
| `system.compute` | `node_timeline`, `clusters`, `warehouses`, `warehouse_events`, `node_types` | Per-node CPU and memory over time - the utilization signal the Spark event log only implies |
| `system.access` | `table_lineage`, `column_lineage` | What depends on an expensive table. Four dbt tests on one model burned 131 warehouse-hours in two days; lineage says whether that is load-bearing |
| `system.storage` | `table_metrics_history`, `predictive_optimization_operations_history` | Table size and file-count history, which is what a missing-statistics finding needs to be actionable |
| `CATALOG prod` | all | `EXPLAIN COST` plans against the real tables; the SP holds nothing on `prod` today, so the scheduled job would read the SQL and fail to plan it |

## Who can run which

| Object | Required role | Who has it |
|---|---|---|
| `system.*` schema grants | **account admin** | Alyson Lefkowitz |
| `CATALOG prod` | `MANAGE` on prod = **`producers_prod`** | Alyson Lefkowitz, and the `prod_runner` service account |
| `CATALOG dev` | `MANAGE` on dev = **`producers_dev`** | Malachi - self-serve, no ask needed |

Ryan Kleck is not an account admin: he returned
`PERMISSION_DENIED: User does not have MANAGE on Schema 'system.billing'`.

## Deliberately NOT asked for
`system.ai_gateway` (LLM spend), `system.alert`, `system.mlflow`, `system.serving`, `system.tags` -
none answers a question about Spark or warehouse efficiency. `CATALOG dev` and
`CATALOG audience_acuity_mntn` are not requested for the SP: the sweep reads production history,
and dev is self-serve if that ever changes.

## Already granted, do not re-ask
`system.lakeflow` (2026-08-25), `system.query` (2026-08-26), both principals, both verified by
reading. `system.information_schema` needs no grant. The SP already holds `CAN_USE` on warehouse
`14b311ac86ee2ca2` and `USE_CATALOG` on `system`.

## Verifying afterwards
Do NOT use `databricks grants get schema <s>` - it served stale data for over an hour after a live
grant on 2026-08-26 and sent us chasing a phantom. Run `SHOW GRANTS` through a SQL warehouse, or
just read a table. Ladder and error-to-rung mapping: memory
`reference_databricks_system_schema_grants`.
