---
doc_type: ticket
title: "AUDI-1222: vv trace tables"
status: backlog
date: 2026-08-25
summary: "vv trace tables"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1222: vv trace tables

**Jira:** https://mntn.atlassian.net/browse/AUDI-1222
**Status:** backlog
**Date Started:** 2026-08-25
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

AUDI-1222: build permanent trace tables mapping every stage 2/3 verified visit to its stage 1 impression, then DDM monitors on untraceable-row state and metrics from observed rates. Zach Schoenberger asked for this 2026-08-25 after AUDI-802 closed with the tables unbuilt. Lineage: TI-650 audit built the traversal method and found ~30 all-time untraceable rows (no root cause); AUDI-802 was the partial follow-up; IMP-069 held the residual and is promoted to this ticket. Backlog until prioritized.
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
