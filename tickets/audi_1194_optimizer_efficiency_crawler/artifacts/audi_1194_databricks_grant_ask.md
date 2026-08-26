# One Databricks grant ask, everything the optimizer will ever need

Run once, by an **account admin**, in the Databricks **SQL Editor**
(`https://1262887251702944.4.gcp.databricks.com` → SQL Editor → pick any running warehouse →
paste → Run all). Anyone below that tier gets
`PERMISSION_DENIED: User is not an account admin for Account`.

Two principals: `malachi@mountain.com` (a person, for analysis) and
`07f36af7-614d-4d57-8143-2dbcd3cb58c2` (the `spark_optimizer` service principal, for the
scheduled job). Both are needed; the SP is what runs unattended.

## The statements

```sql
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

SHOW GRANTS ON SCHEMA system.billing;
SHOW GRANTS ON SCHEMA system.compute;
SHOW GRANTS ON SCHEMA system.access;
SHOW GRANTS ON SCHEMA system.storage;
```

The four `SHOW GRANTS` at the end are the point: a GRANT that does nothing still prints `OK`, and
three of four lines landing silently is the failure we hit twice. Each schema should list both
principals with `SELECT` and `USE SCHEMA`.

## Why each schema

| Schema | Tables that matter | What the optimizer gets |
|---|---|---|
| `billing` | `usage`, `attributed_usage`, `list_prices`, `account_prices` | What a job actually cost, instead of executor-hours as a proxy |
| `compute` | `node_timeline`, `clusters`, `warehouses`, `warehouse_events`, `node_types` | `node_timeline` is per-node CPU and memory over time - the utilization signal the Spark event log only implies |
| `access` | `table_lineage`, `column_lineage` | What depends on an expensive table. Four dbt tests on one model burned 131 warehouse-hours in two days; lineage is how you tell whether that is load-bearing |
| `storage` | `table_metrics_history`, `predictive_optimization_operations_history` | Table size and file-count history, which is what "missing table statistics" and small-file findings need to be actionable |

## Deliberately NOT asked for
`ai_gateway` (LLM spend), `alert`, `mlflow`, `serving`, `tags`. None answers a question about
Spark or warehouse efficiency, and a grant nobody uses is a grant nobody can justify.

## Already granted, do not re-ask
`system.lakeflow` (2026-08-25) and `system.query` (2026-08-26), both principals, both verified by
reading. `system.information_schema` needs no grant.

## Verifying afterwards
Do NOT use `databricks grants get schema <s>` - it served stale data for over an hour after a live
grant on 2026-08-26 and sent us chasing a phantom. Run `SHOW GRANTS` through a SQL warehouse, or
just read a table. Ladder and error-to-rung mapping: memory
`reference_databricks_system_schema_grants`.
