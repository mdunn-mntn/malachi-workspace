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
- `USE SCHEMA` on `system.lakeflow` needs a Databricks **account** admin. Workspace admin is not
  enough (`grants update` returns `User is not an account admin`), which corrects the assumption
  we started from. Service principal `spark_optimizer` (`07f36af7-614d-4d57-8143-2dbcd3cb58c2`)
  exists and `EXPLAIN COST` is validated against warehouse `14b311ac86ee2ca2`.
- Then bridge `artifacts/audi_1194_databricks_explain_cost.py` into the sweep. The enumeration
  gap is unchanged: mapping an ephemeral dbt run to the query it ran.

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
