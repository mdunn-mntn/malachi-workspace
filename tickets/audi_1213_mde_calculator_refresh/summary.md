---
doc_type: ticket
title: "AUDI-1213: mde calculator refresh"
status: backlog
date: 2026-08-20
summary: "mde calculator refresh"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1213: mde calculator refresh

**Jira:** https://mntn.atlassian.net/browse/AUDI-1213
**Status:** backlog
**Date Started:** 2026-08-20
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** {the single, falsifiable question — a stranger could tell whether it's been answered}
- **Goal (why / the decision):** {the decision or outcome the answer serves + who's waiting on it + north-star tie}
- **Objective (done-when):** {the concrete deliverable + the bar that closes it — binary: it exists and clears the bar, or it doesn't}
- **Approach (how):** {data sources, method/protocol, and the key assumptions to resolve empirically first}
- **What would change the answer:** {the smallest result that flips the conclusion — the kill criteria that keep scope honest}

**Jira:** [AUDI-1213](https://mntn.atlassian.net/browse/AUDI-1213) · Task · 8 pts · requested by Al Beretta (Slack 2026-08-20)

**Scoping doc (read first):** `tickets/ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill/artifacts/ti_1019_refresh_scope.md` — 18 verified deltas, re-run requirements, delivery-option comparison, open questions.

**Why this exists:** Al asked for the TI-1019 MDE calculator again. It cannot be re-run as-is: its source table `agg__daily_sum_by_campaign` was deleted 2026-08-19, its CPM is on `media_cost` against advertiser-facing spend everywhere else (3.105x median gap), and required spend charges the unserved holdout for impressions (1.1111x). Data is frozen at 2026-06-04.

**Settled scope:** everybody, no spend floor. 1,863 delivering + 4,369 lapsed = 6,232 rows (`outputs/ti_1019_lapsed_cohort_size.md` under TI-1019). That population, carrying $468.13M of former-customer lifetime spend, is why delivery moves off the unauthenticated gist to Mode.

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
