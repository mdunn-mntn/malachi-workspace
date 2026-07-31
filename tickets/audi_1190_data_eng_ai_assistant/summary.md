---
doc_type: ticket
title: "Evaluate the Data Eng AI assistant for on-call + Airflow"
status: backlog
date: 2026-07-31
summary: "Spike: adopt SteelHouse/mntn-data-eng-assistant (data-eng-ai.in.mountain.com) for our on-call + Airflow triage, or not."
result: "not started"
question: "Should we take over Ryan's data-eng-ai assistant (MCP/agentic Spark+Airflow debugger) for our on-call, and if so which capability first, given the access constraints?"
framing_state: locked
---

# Evaluate the Data Eng AI assistant for on-call + Airflow

**Jira:** https://mntn.atlassian.net/browse/AUDI-1190
**Status:** backlog
**Date Started:** 2026-07-31
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Should we take over and stand up Ryan's data-eng-ai assistant (the MCP/agentic Spark + Airflow debugger, `SteelHouse/mntn-data-eng-assistant`, `data-eng-ai.in.mountain.com`) for our on-call, and if so which capability first — given the access revocations (Airflow API tokens via Vault, Databricks access, no Slack bots)?
- **Goal (why / the decision):** Decide invest-vs-build-vs-skip on a tool being handed over. Victor (the Spark/Databricks expert) has left; on-call Spark/Dataproc debugging is slow, manual, and single-person-dependent (this meeting = 30 min just to *find* a failed vendor-payment job). A working AI debugger cuts MTTR and de-risks the bus factor. North-star tie: cost reduction + velocity multiplier.
- **Objective (done-when):** A brief exists covering (a) what the tool does today, (b) roadmap/plans, (c) Databricks vs GCP Dataproc scope, (d) gap/overlap with our stack (`airflow_pull.sh` + `/oncall`), (e) an explicit adopt / integrate / skip recommendation. Binary: brief delivered and lands a call.
- **Approach (how):** Deep-read the repo (tools, CHANGELOG/roadmap, `spark_job_monitor.py`, slack-bot, airflow diagnosis) via `gh`; mine the 2026-07-31 Ryan handover meeting; compare against our on-call tooling; resolve the access blockers empirically (what's actually revoked vs recoverable).
- **What would change the answer:** If the access revocations are permanent and unwaivable (no Airflow API tokens, no Databricks access, no Slack bots), the fully-automated sensor→PR vision is dead and the recommendation collapses to a manual-invoke MCP tool only — or skip until access is restored.

## 1. Introduction
Ryan Kleck built `SteelHouse/mntn-data-eng-assistant` — an MCP/AI platform for Dataproc/Spark batch diagnosis, GCS inspection, and Airflow diagnosis, hosted at `data-eng-ai.in.mountain.com` (memory `reference_data_eng_mcp`; Harvey Yau also involved). In a 2026-07-31 1:1 (meetings/01) he offered to hand the repo to Malachi to take over and revive, because he's no longer using it and its Airflow/MCP access was revoked. Ryan's vision: an agentic on-call debugger — a sensor on every Airflow DAG that, on failure, auto-reads the Spark logs/UI, finds the stuck stage, pulls the explain plan, and emits a summary + candidate PR. This spike decides whether we adopt it.

## 2. The Problem
- **Symptoms observed (meeting):** 30 min spent just to *locate* a failed job — the vendor-payment ("give money to our vendors") Dataproc job that timed out at 50 min / pod died, likely from a GCP autoscaling change (Brian owns the fix) making long-running DAGs time out. Spark UI navigation is manual and expert-dependent.
- **Who it affects:** all data engineers on-call. Victor (the Spark/Databricks expert who wrote the framework) has left — bus factor is now acute. Ryan usually defers this job to Victor/Sean.
- **Access blockers:** Airflow API tokens no longer issued (Vault); the MCP tool's Airflow access revoked; Databricks access limited (Malachi lacked permissions mid-call); Slack bots deleted by security ("security/devops is the new DBAs") — same policy that decommissioned our own slack_bot 2026-06-10.
- **Impact:** slow MTTR on prod pipeline failures, single-person dependency, and a revenue-adjacent job (vendor payments) that no one on the call could confidently debug.

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
