---
doc_type: ticket
title: "AUDI-1279: OpenAI batch pipeline observability: dead-cohort alarm and status logging"
status: backlog
date: 2026-09-02
summary: "shopper_graph: log per-batch status at every transition, alarm when 0 of N progress"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1279: OpenAI batch pipeline observability: dead-cohort alarm and status logging

**Jira:** https://mntn.atlassian.net/browse/AUDI-1279
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

The Aug 27-30 OpenAI batch outage ran for days before anyone could see why. Give the pipeline eyes so the next one pages on day one.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** batch statuses and error messages live only on OpenAI's side. The Airflow logs never show them, and nothing alarms when a whole day's batches silently die after submission (0 of N ever progress).

**Task:** in shopper_graph:
- [batch_transitioner.py](https://github.com/SteelHouse/shopper_graph/blob/main/openai/openai_wrapper/batch_transitioner.py): print each batch's status and error at every transition check
- [batch_fetcher.py](https://github.com/SteelHouse/shopper_graph/blob/main/openai/openai_wrapper/batch_fetcher.py): alarm when 0 of N batches have progressed N hours after submit

**Done-when:** alarm fires in a staged test and per-batch statuses appear in Airflow logs.

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
