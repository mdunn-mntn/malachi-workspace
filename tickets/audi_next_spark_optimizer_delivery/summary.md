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
- **UNBLOCKED 2026-08-25.** `system.lakeflow.job_run_timeline` is readable by the
  `spark_optimizer` SP and by `malachi@mountain.com`, both holding `SELECT` + `USE_SCHEMA`.
  Verification query returns 1,505 `SUBMIT_RUN` rows over 7 days. The grant ladder and the
  five-day blocker chain are recorded in memory `reference_databricks`; the short version is
  that it needed a metastore admin GROUP (console only, account admin assigns it) for
  `USE CATALOG ON CATALOG system`, and then an ACCOUNT admin for the schema-level grants,
  which metastore admin does not confer. Alyson Lefkowitz did both.
- **Three query gotchas, all confirmed on the first real read.** `run_duration_seconds` is 0
  on every `SUBMIT_RUN` row, so derive it from `period_end_time - period_start_time`.
  `run_name` carries a per-run uuid suffix, so strip
  `-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$` before grouping; what
  remains is the dbt identifier as `prod-<schema>-<model>`. `compute_ids` is empty, so this
  table does NOT bridge a submission to its cluster.
- **What is left on the Databricks half:** bridge
  `artifacts/audi_1194_databricks_explain_cost.py` into the sweep, keyed on the stripped
  `run_name`. `EXPLAIN COST` is already validated against warehouse `14b311ac86ee2ca2`. The
  enumeration gap that blocked this for five days is closed.

**Slack delivery spec (agreed 2026-08-25).**

Detection stays deterministic. The 14 detectors already emit a `why:` and a `fix:` per finding
with measured numbers, so nothing about *finding* the problem needs a model. A model is only
worth adding for the last mile: turning a finding plus the offending source into a proposed
diff, and writing the one-line summary. Scope it there and the sweep stays reproducible.

**Credential.** Use the company OpenAI key the team already owns in Vault at
`teams/team-engineering-targeting/openai` (`SteelHouse/mntn-team-credentials`,
`secrets/team-engineering-targeting/openai/teamsecret.yaml`). Do NOT carry Malachi's personal
`ANTHROPIC_API_KEY` into a scheduled job; AUDI-1191's debugger runs on it today and moves to
the same Vault entry. SOP 052 prefers identity over a secret, but a third-party API key fails
its first condition, so the Vault/ESO path is correct here.

**Channel.** New channel plus a bot token, both requested through devops. The digest posts once
a day after the sweep.

**Every post is the same four-block shape, BLUF.** Headline first: the one thing a reader who
stops there must get. Then, per finding, at most three findings per post:
- **What** — the finding in one line, with the measured number that makes it real.
- **Where** — the DAG's Astro/Airflow run URL, plus a GitHub permalink to the exact lines
  (`blob/<sha>/<path>#L<start>-L<end>`), never a bare file name.
- **Why** — the cost in the unit we can defend: idle executor-hours, GiB spilled, share of task
  time. Carry the CUD caveat on any dollar figure.
- **How** — the proposed change. A diff only when it is small; past a threshold, link the lines
  and describe the change in a sentence. A giant diff in Slack is not readable and is not the
  deliverable.

Same block order, same headings, every day, so a reader learns the shape once. Anything that
does not fit those four blocks does not go in the post.

**Two defects found triggering a manual run 2026-08-25.**
- **The DAG cannot be triggered without an explicit `logical_date`.** Airflow 3 gives a manual
  run created with `logical_date: null` no data interval at all, so the task raises
  `KeyError('ds')` in seconds. The 9am schedule is unaffected because a scheduled run always
  carries one. Anyone testing a change has to pass
  `{"logical_date": "<ISO8601>"}`, which is a trap worth removing: default the date to
  `data_interval_end or run_after` inside the task rather than templating `{{ ds }}`.
- **The failure callback's Slack post is broken:** `chat.postMessage` returns
  `{'ok': False, 'error': 'channel_not_found'}`. So a failed sweep currently notifies nobody.
  Fix this alongside the digest delivery work, since both need the same bot token and channel.

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
