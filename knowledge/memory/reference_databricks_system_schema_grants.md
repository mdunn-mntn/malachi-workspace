---
name: reference_databricks_system_schema_grants
description: The exact ladder to get a person or service principal read access to a Databricks system schema (lakeflow, query, billing, access) - who runs what, in which order, and how to verify it.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [databricks grants, system schema, unity catalog, USE CATALOG, USE SCHEMA, SELECT, metastore admin, account admin, system.lakeflow, system.query, system.billing, system.access, query history, job_run_timeline, SHOW GRANTS, grants get stale, INSUFFICIENT_PERMISSIONS, PERMISSION_DENIED, not an account admin, service principal, spark_optimizer, Alyson Lefkowitz, metastore_admins]
domain: [infra, routing-people]
lifecycle: active
last_verified: 2026-08-26
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
done. The two are one line apart and easy to confuse.

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
- **`system.billing`**, **`system.access`** - same ladder, same admin tier, not yet granted.
