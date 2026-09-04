---
doc_type: ticket
title: "AUDI-1327: Pin the debugger replies to real logs, then fix the downstream-cause parser"
status: in_progress
date: 2026-09-04
summary: "Real-log fixture corpus per signature class, then fix ordering, collapsed stacks, db_unreachable"
result: "not started"
question: "Can the debugger's replies be pinned to logs in the shape production emits, so the 2026-09-03 conversion_signal_backfill failure renders its real cause and a fixture-shape regression fails CI rather than Slack?"
framing_state: locked
---

# AUDI-1327: Pin the debugger replies to real logs, then fix the downstream-cause parser

**Jira:** https://mntn.atlassian.net/browse/AUDI-1327
**Status:** backlog
**Date Started:** 2026-09-04
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Can the debugger's replies be pinned to logs in the shape production
  actually emits, such that the 2026-09-03 `conversion_signal_backfill_workflow/submit_batch_dsid_21`
  failure renders its real cause and a fixture-shape regression fails CI rather than Slack?
- **Goal (why / the decision):** PR #1285 shipped with 53 of 53 tests green and does nothing on the
  exact production failure it was written for. Every reply defect to date was caught by a person
  seeing a bad Slack post. Until the corpus exists, the test suite certifies the wrong thing, and
  nothing downstream (AUDI-1328's validation, AUDI-1329's coverage, the AUDI-1325 LLM layer) can
  trust a reply.
- **Objective (done-when):** That failure renders `java.net.SocketTimeoutException: Connect timed
  out`, raised through `org.postgresql.util.PSQLException`, at `spark_read_host.py:27`, from the
  captured real log, pinned in CI; and every signature class that has a real prod example carries
  one fixture and one golden rendered reply.
- **Approach (how):** Corpus first, parser second, so the same class of defect cannot ship green
  again. Sources: 44 signature classes in `include/airflow_debugger/signatures.py`, ~600 real task
  logs already pulled under `on-call/airflow_logs/` (432 on 2026-09-03), and Cloud Logging for the
  Dataproc driver logs. Assumptions to resolve empirically first: how many of the 44 classes have a
  real prod example at all, and whether Cloud Logging's shape (descending order, tab-joined stacks)
  is stable across engines and fetch paths or varies by API call.
- **What would change the answer:** If most of the 44 classes have no real prod example, the corpus
  cannot be complete and the deliverable becomes "every class that fires in practice" plus a named
  gap list, not "every class". If the descending order turns out to be a flag on our own fetch call
  rather than the API's behaviour, the ordering half is a one-line fix and the ticket shrinks to the
  tab-collapsed stacks plus the corpus.

## 1. Introduction
Brief context: what system/feature/data is involved, and why this ticket exists.

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
