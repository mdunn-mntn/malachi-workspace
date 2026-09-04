---
doc_type: ticket
title: "AUDI-1328: Measure whether the optimizer recommendations actually work"
status: backlog
date: 2026-09-04
summary: "Score the 60 fixes shipped 2026-09-03 by detector and signature class"
result: "blocked until 2026-09-06: no resolved fixes yet, estimator unmerged"
question: ""
framing_state: draft
---

# AUDI-1328: Measure whether the optimizer recommendations actually work

**Jira:** https://mntn.atlassian.net/browse/AUDI-1328
**Status:** backlog
**Date Started:** 2026-09-04
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
The optimizer names a knob for each finding. Whether the knob helped has never been measured per
recommendation. The 60 fixes shipped 2026-09-03 across 8 PRs are a labelled set that collects its
own evidence, so this ticket scores them by detector and by signature class.

## 2. The Problem
"The recommendations are right" is currently an assertion. AUDI-1194's own history shows why that is
not safe: the fangorn #1231 fix was reported as holding, then did not — the finding went chronic
again and the ~$900 cumulative saving turned out to be a blind-window artifact. Nobody knows the rate
at which a shipped fix keeps firing, and nobody knows whether some detectors are reliably right while
others are reliably wrong.

## 3. Plan of Action — BLOCKED until 2026-09-06, deliberately

**Two preconditions, neither of which is met on 2026-09-04:**

1. **The evidence does not exist yet.** Checked against the live ledger on 2026-09-04: the newest
   ledger date is still `2026-09-03`, and all 60 manifest keys sit at `applied` 50 / `chronic` 7 /
   `recurring` 3. Zero have resolved. The next sweep runs 09-05 09:00 UTC, and forward replay of the
   patched code says the manifest keys clear on the sweep dated **2026-09-06**, not 09-05.
2. **The estimator is not trustworthy yet.** AUDI-1326 / PR #1286 is open, not merged. Until it is,
   `savings()` reports a per-DAG mean difference off an n=1 baseline with no variance test, so any
   per-class precision computed from it would inherit that error.

**Do not start before both clear.** Starting early produces numbers that look like an answer and are
not.

When both clear:
1. Per applied key, measure whether the finding went quiet and whether executor-hours moved, with an
   interval rather than a point estimate.
2. Split by detector and by signature class, so precision is known per class rather than in aggregate.
3. Report the rate at which a shipped fix kept firing (`fix_not_working`).
4. Write the per-class numbers to `knowledge/`.

## 4. Investigation & Findings

**Method caveats already established, which shape the measurement:**
- Resolution counts distinct ledger DATES, not sweep executions. A quiet fleet, an empty crawl, or a
  run of `complete=False` sweeps does not advance the window at all. The ledger already has a
  three-day gap (2026-08-22 to 08-24) despite digests publishing on those days. So 09-06 is an
  estimate, not a date to plan against; confirm against the ledger before starting.
- Ten of the 60 carry a detector state (`chronic` 7, `recurring` 3) rather than `applied`, because
  `record()` rewrites the same-day row. Attribution survives, so all 60 are still scorable, but the
  state field is not the right filter — a non-empty `fix_pr` is.
- 50 of the 60 keys did not fire on the 2026-09-03 crawl at all, so for six DAGs every 09-03 row was
  synthetic before PR #1286 removed the fabricated hours. Score against the post-#1286 series.

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
