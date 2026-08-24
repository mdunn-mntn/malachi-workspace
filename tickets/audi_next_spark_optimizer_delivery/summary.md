---
doc_type: ticket
title: "AUDI-XXXX: Spark optimizer delivery, coverage, and Databricks"
status: backlog
date: 2026-08-21
summary: "Slack delivery, per-DAG digest ranking, fix the Airflow 3 coverage pass, batch the download, and land the Databricks EXPLAIN COST bridge"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-XXXX: Spark optimizer delivery, coverage, and Databricks

**Jira:** https://mntn.atlassian.net/browse/AUDI
**Status:** backlog
**Date Started:** 2026-08-21
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** {the single, falsifiable question — a stranger could tell whether it's been answered}
- **Goal (why / the decision):** {the decision or outcome the answer serves + who's waiting on it + north-star tie}
- **Objective (done-when):** {the concrete deliverable + the bar that closes it — binary: it exists and clears the bar, or it doesn't}
- **Approach (how):** {data sources, method/protocol, and the key assumptions to resolve empirically first}
- **What would change the answer:** {the smallest result that flips the conclusion — the kill criteria that keep scope honest}

## 1. Introduction / Context

Follow-on to AUDI-1194, which shipped `spark_optimizer_daily` to prod airflow-ti on 2026-08-21.
That ticket's job was to make the sweep run unattended on a non-human identity, and it does:
run 1 scanned 215 Spark jobs and produced 290 findings, 196 high-impact, published to
`gs://mntn-data-archive-prod/optimizer/`.

What it is not yet is a product anyone reads. Nothing delivers the digest, the digest ranks per
finding so one bad DAG fills the page, and two of the three planned inputs are missing.

## 2. Problem / Scope

**Delivery (the point of the ticket).**
- Post the daily digest to Slack. `digest.render()` already emits Slack markup, and
  `compass-slack` in mntn-devops is the transport, so this is plumbing, not a rewrite.
- Rank per DAG, not per finding. Run 1 opened with eight consecutive `fangorn_score_monitor`
  lines because the worst job monopolises a per-finding sort (IMP-046). One line per DAG with
  its worst finding, and the count behind it.
- The digest cites the container's `/tmp` path for the full backlog instead of the GCS URL.

**Fix what run 1 broke.**
- **Coverage is dead on Airflow 3.** `collect_local` reads paused state from the metadata DB and
  a task gets `airflow session use is forbidden in this context`. Go back through the REST API
  with the deployment token Ryan Kleck minted, or find a Task-SDK call that exposes paused
  state. Without it the sweep cannot say which active DAGs it is blind to.
- **The download is 200 serial `gsutil` invocations**, each paying interpreter startup. On the
  Astro default pod (0.25 CPU / 0.5 Gi, because the DAG sets no `executor_config`) run 1 took
  ~19 minutes and process spawn dominated the parse. Batch into one `gsutil -m cp -I` first;
  raise CPU only if that is not enough.

**Databricks, the missing engine.**
- **The blocker is a missing metastore admin, not a Databricks-side enable.** Corrected
  2026-08-24 by David Qiu (Databricks). `system.lakeflow` is Databricks-managed and enabled
  automatically, so `PUT .../systemschemas/lakeflow` always rejects with
  `lakeflow system schema can only be enabled by Databricks` and that error is a red herring.
  The real one is `User does not have MANAGE on Schema 'system.lakeflow'`: granting on system
  schemas needs **metastore admin**, which account admin does not confer. Accounts created
  after Nov 2023 ship with no metastore admin assigned, which is why nobody at MNTN has MANAGE.
- **Assigning it is console-only.** An account admin sets it in the account console under
  Catalog > metastore `c5dc6763-eaae-4d6c-9ae2-7af6147595bb` > Metastore Admin > Edit. It must
  be a **group**, and there is no API or Terraform path today. Then a member of that group runs
  the grants.
- **Three grants, not two.** The original ask omitted catalog-level access:
  `GRANT USE CATALOG ON CATALOG system`, `GRANT USE SCHEMA ON SCHEMA system.lakeflow`, and
  `GRANT SELECT ON SCHEMA system.lakeflow`, each to `07f36af7-614d-4d57-8143-2dbcd3cb58c2`.
- **Enumeration is one table, no joins.** `system.lakeflow.job_run_timeline` filtered
  `run_type = 'SUBMIT_RUN'` returns the ephemeral submissions, with `run_name` carrying whatever
  `runs/submit` was given (the dbt identifier). **Do not join `system.lakeflow.jobs` for a job
  name** — ephemeral runs have no row there, so the join silently drops 100% of them. Join
  `job_task_run_timeline` on `job_run_id` only for per-task detail. System tables are batch
  ingested: expect ~10 minutes of lag before a run appears.
- Service principal `spark_optimizer` (`07f36af7-614d-4d57-8143-2dbcd3cb58c2`) exists and
  `EXPLAIN COST` is validated against warehouse `14b311ac86ee2ca2`, so only run enumeration is
  blocked.
- **The query-path error does not tell you what is enabled.** Probed 2026-08-21 as
  malachi@mountain.com: `system.query.history`, `system.access.audit`, `system.billing.usage`,
  `system.compute.clusters` and `system.lakeflow.job_run_timeline` all return the identical
  `INSUFFICIENT_PERMISSIONS ... does not have USE SCHEMA on Schema 'system.<x>'`. Missing grant
  and not-enabled are indistinguishable from a query, and every one of them is gated behind the
  same missing metastore admin.

**Debt this inherits.**
- `include/spark_optimizer/` ships multi-line rationale comments throughout, which its own
  `lint_comments.py` would now fail. Own PR, before anything else lands on the package.

## 3. Not in scope

Acting on findings. The `site_network_hourly` and `aug_log_ip*` recommendations stay in
AUDI-1194 until the tool that produced them is trustworthy enough to send from.

## 2. The Problem
What exactly is broken, unclear, or needed? Include:
- Symptoms observed
- Who reported it / who it affects
- Impact (data quality, revenue, user experience, etc.)

## 3. Plan of Action
Numbered steps of the approach taken. Updated as the plan evolves.
1. Step one
2. Step two
3. ...

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
