---
doc_type: ticket
title: "AUDI-1280: Debugger alerting tag coverage: fleet audit and CI check"
status: backlog
date: 2026-09-02
summary: "Audit every alerting DAG tag vs PAGING_TAGS, fix misses, add a CI check that blocks regressions"
result: "not started"
question: "Does every alerting DAG in airflow-ti carry a tag on the debugger's PAGING_TAGS watch list, and can a CI check block any DAG that does not?"
framing_state: locked
---

# AUDI-1280: Debugger alerting tag coverage: fleet audit and CI check

**Jira:** https://mntn.atlassian.net/browse/AUDI-1280
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does every alerting DAG in airflow-ti carry a tag on the debugger's PAGING_TAGS watch list, and can a CI check block any DAG that does not?
- **Goal (why / the decision):** Two August alerts got no debugger reply because their tags were unwatched. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** An audit table in outputs/ (dag_id, alert route, tags, watched yes/no), a PR (branch AUDI-1280) fixing every miss and adding a CI test that fails when an alerting DAG carries no watched tag; audit clean and CI merged.
- **Approach (how):** Parse dags/ on airflow-ti main for failure callbacks and Slack routes, compare tags with include/airflow_debugger/daily.py PAGING_TAGS; CI as a pytest in the repo's existing suite; define 'alerting DAG' as any DAG with a failure callback that posts to a channel.
- **What would change the answer:** If 'alerting DAG' cannot be detected structurally, the CI check narrows to a maintained allow-list and the ticket records why.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Two August alerts got no debugger reply because their DAGs' tags were not on the debugger's watch list. Make that gap impossible to reintroduce.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the debugger only scans DAGs carrying watched tags ([PAGING_TAGS](https://github.com/SteelHouse/airflow-ti/blob/main/include/airflow_debugger/daily.py#L35), widened once in [PR 1248](https://github.com/SteelHouse/airflow-ti/pull/1248)). Any new alerting DAG with an unwatched tag becomes invisible again.

**Task:** audit every alerting DAG's tags against the watch list, fix misses, and add a CI check that fails when an alerting DAG carries no watched tag.

**Done-when:** audit clean and CI check merged.

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
