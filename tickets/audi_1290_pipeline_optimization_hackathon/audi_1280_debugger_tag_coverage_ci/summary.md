---
doc_type: ticket
title: "AUDI-1280: Debugger alerting tag coverage: fleet audit and CI check"
status: backlog
date: 2026-09-02
summary: "Audit every alerting DAG tag vs PAGING_TAGS, fix misses, add a CI check that blocks regressions"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1280: Debugger alerting tag coverage: fleet audit and CI check

**Jira:** https://mntn.atlassian.net/browse/AUDI-1280
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** {the single, falsifiable question — a stranger could tell whether it's been answered}
- **Goal (why / the decision):** {the decision or outcome the answer serves + who's waiting on it + north-star tie}
- **Objective (done-when):** {the concrete deliverable + the bar that closes it — binary: it exists and clears the bar, or it doesn't}
- **Approach (how):** {data sources, method/protocol, and the key assumptions to resolve empirically first}
- **What would change the answer:** {the smallest result that flips the conclusion — the kill criteria that keep scope honest}

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
