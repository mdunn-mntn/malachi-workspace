# Slack DM draft, Alyson Lefkowitz, re: system.query grants

Same shape as the `system.lakeflow` grants she ran on 2026-08-25. Account admin is required for
every schema-level grant under `system`; metastore admin does not cover it (tested 2026-08-24,
`PERMISSION_DENIED: User is not an account admin for Account`).
Everything below the marker is the message.

## Message

Hey Alyson, can you run the same two grants you did for `system.lakeflow` on `system.query`, for `malachi@mountain.com` and the `spark_optimizer` service principal?

```
GRANT USE SCHEMA ON SCHEMA system.query TO `malachi@mountain.com`;
GRANT SELECT     ON SCHEMA system.query TO `malachi@mountain.com`;
GRANT USE SCHEMA ON SCHEMA system.query TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;
GRANT SELECT     ON SCHEMA system.query TO `07f36af7-614d-4d57-8143-2dbcd3cb58c2`;
```

`system.query.history` carries the SQL text of each statement that ran. Four of the optimizer's checks need a query plan to work and are dead without it, so this is the one thing standing between them and running.

## Why it matters, if asked

The optimizer reads Spark event logs. Those carry plan TEXT (4.7 MB across a 300-run sample)
but no table-size annotations, so `parse_plan_text` extracts **0** table nodes from all of it
and four checks — missing table statistics, broadcast candidate, window full sort, repeated
scan — cannot fire at all. Measured 2026-08-26 across every archived log.

`EXPLAIN COST` on Databricks emits the annotations the checks need, and it is already validated
against warehouse `14b311ac86ee2ca2`. The missing piece was the SQL to run it on:
`system.lakeflow.job_run_timeline` gives the model name and duration but never the statement.
`system.query.history` has the statement.

## State as of 2026-08-26

- `system.query` schema exists and is listed by `system.information_schema.schemata`.
- `SELECT ... FROM system.query.history` returns
  `INSUFFICIENT_PERMISSIONS: User does not have USE SCHEMA on Schema 'system.query'`.
- `USE CATALOG ON CATALOG system` is already held (metastore admin group, 2026-08-25).
- Confirm afterwards with
  `databricks api get /api/2.1/unity-catalog/permissions/schema/system.query`.
