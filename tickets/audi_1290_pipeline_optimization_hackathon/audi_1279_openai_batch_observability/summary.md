---
doc_type: ticket
title: "AUDI-1279: OpenAI batch pipeline observability: dead-cohort alarm and status logging"
status: backlog
date: 2026-09-02
summary: "shopper_graph: log per-batch status at every transition, alarm when 0 of N progress"
result: "not started"
question: "Can the shopper_graph batch pipeline log every batch's OpenAI-side status and error at each transition check, and alarm when 0 of N batches have progressed N hours after submit?"
framing_state: locked
---

# AUDI-1279: OpenAI batch pipeline observability: dead-cohort alarm and status logging

**Jira:** https://mntn.atlassian.net/browse/AUDI-1279
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Can the shopper_graph batch pipeline log every batch's OpenAI-side status and error at each transition check, and alarm when 0 of N batches have progressed N hours after submit?
- **Goal (why / the decision):** The Aug 27-30 outage ran for days unseen; this pages on day one. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** One shopper_graph PR merged touching batch_transitioner.py and batch_fetcher.py; a staged test shows the alarm firing and per-batch statuses in Airflow logs.
- **Approach (how):** Read both wrappers and the existing alerting path in shopper_graph and its airflow-ti DAG; unit test with a mocked OpenAI client for the dead-cohort case; stage in the dev environment, never prod.
- **What would change the answer:** If OpenAI-side status is not reachable from the fetcher the alarm falls back to Airflow-side timing only, and the ticket says so.

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
