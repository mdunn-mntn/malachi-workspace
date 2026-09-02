---
name: reference_databricks_system_schema_grants
description: The exact ladder to get a person or service principal read access to a Databricks system schema (lakeflow, query, billing, access) - who runs what, in which order, and how to verify it - plus the warehouse Can-use step that SQL grants cannot do. system.billing granted 2026-09-02 (usage/list_prices for cost queries); the INSUFFICIENT_PERMISSIONS securable detail sits on the SECOND log line.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [databricks grants, system schema, unity catalog, USE CATALOG, USE SCHEMA, SELECT, metastore admin, account admin, system.lakeflow, system.query, system.billing, system.access, query history, job_run_timeline, SHOW GRANTS, grants get stale, INSUFFICIENT_PERMISSIONS, PERMISSION_DENIED, not an account admin, service principal, spark_optimizer, Alyson Lefkowitz, metastore_admins, warehouse Can use, SQL Warehouses permissions, Permissions API, warehouse not sql-grantable, fa27430dfc609e6d, prod_runner 397d710b, billing.usage, list_prices, SQLSTATE 42501, securable on second log line, billing granted 2026-09-02]
domain: [infra, routing-people]
lifecycle: active
last_verified: 2026-09-02
---
Getting anyone read access to a Databricks `system` schema takes **two different admin tiers** and
they do not substitute. Worked twice: `system.lakeflow` (2026-08-25) and `system.query` (2026-08-26).

## The ladder

| Step | Statement | Who can run it |
|---|---|---|
| 1 | `GRANT USE CATALOG ON CATALOG system TO \`<principal>\`;` | **metastore admin** |
| 2 | `GRANT USE SCHEMA ON SCHEMA system.<schema> TO \`<principal>\`;` | **account admin** |
| 3 | `GRANT SELECT ON SCHEMA system.<schema> TO \`<principal>\`;` | **account admin** |

Step 1 is once per principal, not per schema. Steps 2 and 3 repeat for every schema.
Backticks around the principal are required. A service principal is granted by its **appId**.

**On this metastore (`c5dc6763-eaae-4d6c-9ae2-7af6147595bb`, workspace `1262887251702944`):**
`account users` already holds `USE CATALOG ON CATALOG system`, so step 1 is usually already done.
Metastore admin is the group `metastore_admins`; **Alyson Lefkowitz holds both tiers** and is the
person to ask. Workspace `admins` membership confers neither - it returns
`PERMISSION_DENIED: User is not an account admin for Account` on steps 2 and 3, and
`PERMISSION_DENIED: User does not have MANAGE on Catalog 'system'` on step 1.

## The warehouse step (NOT SQL-grantable)

The 3-step ladder covers only catalog/schema/table. **Warehouse access is a workspace ACL, not
a Unity Catalog grant - no GRANT statement reaches it.** A principal with the full SELECT
ladder still cannot execute a query without "Can use" on a SQL warehouse. Two ways to set it:
- **UI:** SQL Warehouses -> the warehouse -> Permissions -> add the principal -> **"Can use"**.
- **Permissions API** (`/api/2.0/permissions/sql/warehouses/<id>`).

**2026-09-02: full paste sent to Alyson for `prod_runner` (appId `397d710b`)** - the
`system.lakeflow` + `system.query` SELECT ladders plus warehouse `fa27430dfc609e6d`
(`sql_warehouse_2xs`, MAIN workspace) Can-use. **Executed same day - and the paste was MISSING
`system.billing`**, which the optimizer's cost queries join (`billing.usage` +
`billing.list_prices`). Alyson granted USE SCHEMA + SELECT on `system.billing` 2026-09-02; the
dbx cost report went live the same day (top row `Generate Graph & Metrics - PRODUCTION`,
10,528 DBU, $1,579 list/7d). The warehouse Can-use rung needed no separate check - **any query
that reaches the SQL engine proves warehouse access** (the INSUFFICIENT_PERMISSIONS error came
FROM the engine). ([[project_airflow_optimizer]] dbx surface unblocked.)

## Verify with SHOW GRANTS, never with the CLI

**`databricks grants get schema <s>` served STALE data for over an hour after a grant went live**
(2026-08-26). It cost four re-runs, a wrong "she must be on a different metastore" theory, and a
round trip asking her to screenshot her console. The authoritative check runs through a SQL
warehouse:

```sql
SHOW GRANTS ON SCHEMA system.query;
```

Better still, just read the table - permission is what you actually wanted, not a grants row:

```sql
SELECT count(*) FROM system.query.history WHERE start_time > current_date() - INTERVAL 2 DAYS;
```

**Read the error text, it names the missing rung.** `does not have USE SCHEMA on Schema '<s>'` is
step 2 missing; `does not have SELECT on Table '<s>.<t>'` is step 3 missing with step 2 already
done. The two are one line apart and easy to confuse. **The securable detail sits on the SECOND
log line** - the first line of the 2026-09-02 billing error ended at the colon
(`INSUFFICIENT_PERMISSIONS: User does not have USE SCHEMA on Schema:`, SQLSTATE 42501) and
`'system.billing'` arrived on the next line, so a single-line grep never sees WHICH securable is
missing. Read the next line before diagnosing.

## Ask shape that works

Send the person **one statement per line, all four lines, principal spelled out** - a partial paste
is the failure mode we actually hit (three of four lines landed, twice). Ask them to run
`SHOW GRANTS` in the same session and paste the result, so a silent no-op is caught before you go
looking for it. A GRANT that does nothing still prints `OK`.

## Schemas and what they are for
- **`system.lakeflow`** - `job_run_timeline` enumerates ephemeral dbt `SUBMIT_RUN` jobs that
  `jobs list` cannot see. Gotchas in [[reference_databricks]].
- **`system.query`** - `history` carries `statement_text`, the SQL that actually ran. It is the
  input `EXPLAIN COST` needs, and the only way to reach the optimizer's four plan checks
  ([[project_airflow_optimizer]]).
- **`system.billing`** - `usage` + `list_prices`, the join behind Databricks dollar costing
  ([[reference_databricks_billing_cost]]). **Granted 2026-09-02** (Alyson, USE SCHEMA + SELECT).
- **`system.access`** - same ladder, same admin tier, not yet granted.
