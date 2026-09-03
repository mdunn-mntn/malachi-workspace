---
doc_type: ticket
title: "AUDI-1321: OpenAI storage sweep could not see past the newest 10,000 files"
status: backlog
date: 2026-09-03
summary: "The MNTN Matched keyword pipeline stalled 2026-08-28 on the OpenAI project's 2.5TB storage ceiling. Root cause: the nightly cleanup lists files newest-first and the API caps a page at 10,000, so on a heavy day every slot on that page is younger than the sweep's 48h delete floor and it frees nothing — exactly when churn is highest. Fix shipped in shopper_graph #306 (order=asc + explicit paging); the six blocked days still need backfilling. Split out of AUDI-1191, which caught it live."
result: "not started — #306 merged and deployed 2026-09-03 (first live sweep deleted 1,132 of 1,132 with 0 skips, where every run since 08-29 deleted 0). Submit run for logical 2026-09-02 in flight as the quota test; backfill of dt=2026-08-27..09-01 pending its outcome."
question: "Why did the OpenAI file cleanup stop freeing storage, and what unblocks the six stalled days of the MNTN Matched keyword pipeline?"
framing_state: draft
---

# AUDI-1321: openai storage quota unblock

**Jira:** https://mntn.atlassian.net/browse/AUDI-1321
**Status:** backlog
**Date Started:** 2026-09-03
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
