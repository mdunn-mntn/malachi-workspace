---
doc_type: ticket        # ticket | epic  (epic = this folder holds 2+ child ticket folders)
title: "{TICKET-ID}: {Short Title}"
status: in_progress     # backlog | in_progress | blocked | done
date: {YYYY-MM-DD}      # last meaningful update — tickets/INDEX.md sorts newest-first on this
summary: "{one line: what this ticket is about — <= 90 chars}"
result: "{one line: the blessed final answer/finding — <= 90 chars; leave as '' until done}"
question: "{one line: the single falsifiable question this ticket answers — <= 90 chars; '' until framed}"
framing_state: draft    # draft | locked | skip: <reason>  — /frame locks it; gate blocks status:in_progress while draft
---

# {TICKET-ID}: {Title}

**Jira:** {link}
**Status:** In Progress | Complete | Blocked
**Date Started:** YYYY-MM-DD
**Date Completed:** YYYY-MM-DD
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
