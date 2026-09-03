---
doc_type: ticket
title: "AUDI-1324: Port the MDE calculator to a daily-refreshed Mode dashboard"
status: backlog
date: 2026-09-03
summary: "Port the MDE calculator to Mode so the advertiser data refreshes daily instead of by hand"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1324: Port the MDE calculator to a daily-refreshed Mode dashboard

**Jira:** https://mntn.atlassian.net/browse/AUDI-1324
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

## 1. Introduction
Split out of AUDI-1213, whose approach listed a Mode port but whose 2026-09-03 delivery was the
data refresh alone. Requested 2026-09-03 after Edgar von Trotha hit the limit that forces this:
the in-product Testing tab pins the forecast to an already-live campaign group's budget, so it
cannot answer "what budget would this test need?" Only the standalone calculator does what-if
budget exploration, and it is a static gist that goes stale the day it is built.

Source artifact: `tickets/audi_1213_mde_calculator_refresh/artifacts/audi_1213_mde_calculator.html`
(2026-09-03 build, 1,859 delivering advertisers, all three known defects fixed).

## 2. The Problem
The calculator embeds its advertiser data as static JSON. Refreshing it means a person re-running
`incr_75_advertiser_metrics.sql`, rebuilding the HTML and re-pushing the gist. It has now gone
stale twice (2026-06-04 build re-run 2026-09-03, 91 days). Edgar is fielding a rising number of
customers asking for lift-test budget recommendations, so the staleness compounds.

The gist is also a secret-but-unauthenticated URL carrying 1,859 named advertisers with
advertiser-facing spend. Mode puts it behind auth.

## 3. Plan of Action
1. Create the report ONCE in the Mode UI. Mode will not create a report from a hand-authored
   GitHub folder (verified 2026-07-07, `reference_mode_dashboard_porting`), so this step cannot be
   scripted and is the gating dependency.
2. Port `incr_75_advertiser_metrics.sql` in as the report's query. Its output columns already map
   1:1 onto the calculator's `window.ADVERTISERS` fields.
3. Port the calculator's HTML/JS to `index.html`, replacing the embedded `window.ADVERTISERS` and
   `window.COHORT` with a read of `window.datasets` resolved by query name. Cohort medians move
   from build-time Python into the query or into JS over the loaded rows.
4. Charts are already Chart.js from a CDN, so they lift over unchanged. Mode cannot render
   matplotlib, which is not a constraint here.
5. Set a WEEKLY schedule on the report (user decision 2026-09-03: ~472 GB per run makes daily
   scanning hard to justify against a dataset whose inputs move on a monthly cadence).
6. Deploy via the REST API path (`deploy_mode.sh`), not paste.
7. Retire or redirect the gist once the Mode report is live.

## 4. Investigation & Findings
- The calculator UI is entirely client-side JS, so the port is a data-source swap, not a rewrite.
- `incr_75_advertiser_metrics.sql` dry-runs at 471.7 GB. Resolved 2026-09-03: run it weekly, not
  daily. The reservation absorbs it either way; weekly is the proportionate cadence for inputs
  (trailing-30d rates, 12-month spend pattern) that barely move day to day.

## 5. Solution
Not started.

## 8. Open Items / Follow-ups
- **Blocker: Mode access.** AUDI-1213 gated the port on Al Beretta's seat. Confirm who needs to
  view this (Edgar von Trotha is now a primary user) and that they have seats.
- **Blocker: report must be born in the Mode UI.** Needs someone with create rights in the
  "🗂️ Audience Intelligence" space to create an empty report and paste in
  `queries/audi_1324_advertiser_prefill.sql` as a query named exactly **Advertiser Prefill**.
  Once it exists and has been pushed to GitHub, the rest deploys over the REST API.
- The lapsed 2,546-advertiser cohort stays on AUDI-1213; decide whether the Mode report serves both
  cohorts or only delivering.
