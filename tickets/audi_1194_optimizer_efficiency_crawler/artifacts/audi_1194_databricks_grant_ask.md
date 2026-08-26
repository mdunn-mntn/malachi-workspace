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

SHOW GRANTS ON SCHEMA system.billing;
SHOW GRANTS ON SCHEMA system.compute;
```

The two `SHOW GRANTS` at the end are the point: a GRANT that does nothing still prints `OK`, and
three of four lines landing silently is the failure we hit twice. Each schema should list both
principals with `SELECT` and `USE SCHEMA`.

## Why each schema

| Schema | What the optimizer gets |
|---|---|
| `system.billing` | `usage` and `account_prices` - what a job actually cost, instead of executor-hours as a proxy |
| `system.compute` | `clusters`, `node_types`, warehouse events - what a job was given, so over-allocation is visible without parsing an event log |

## Already granted, do not re-ask
`system.lakeflow` (2026-08-25) and `system.query` (2026-08-26), both principals, both verified by
reading. `system.information_schema` needs no grant.

## Not asked for, and why
`access` (audit), `ai_gateway`, `alert`, `mlflow`, `serving`, `storage`, `tags`. None of them
answers a question about Spark efficiency, and a grant nobody uses is a grant nobody can justify.

## Verifying afterwards
Do NOT use `databricks grants get schema <s>` - it served stale data for over an hour after a live
grant on 2026-08-26 and sent us chasing a phantom. Run `SHOW GRANTS` through a SQL warehouse, or
just read a table. Ladder and error-to-rung mapping: memory
`reference_databricks_system_schema_grants`.
