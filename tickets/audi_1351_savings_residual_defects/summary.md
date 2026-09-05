---
doc_type: ticket
title: "AUDI-1351: savings residual defects"
status: done
date: 2026-09-05
summary: "Three savings defects PR #1286 left behind: no multiplicity correction on the summed headline, a stale resolved outcome, and an unbounded before-window. One of them crashes the sweep."
result: "PR #1290 open. Aggregate false-positive rate on the savings headline drops from 67.5% to 4.5%, and a KeyError that would have failed the daily sweep is gone."
question: "Do the three savings defects that survived PR #1286 change what the optimizer publishes, and can any of them break the sweep?"
framing_state: "skip: direct follow-on to the AUDI-1326 audit, which named all three defects with evidence before work started"
---

# AUDI-1351: savings residual defects

**Jira:** https://mntn.atlassian.net/browse/AUDI-1351
**Status:** done
**Date Started:** 2026-09-05
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

---
## Record

**The branch is named for the wrong key.** This work was scaffolded as AUDI-1330 before the Jira
issue existed. **AUDI-1330 was already taken** by a triage Bug the debugger's service account
auto-filed ("[TRIAGE] guid_geos_summary_to_integration/build_tables.build_guid_geos_summary"). The
real key is **AUDI-1351**, filed 2026-09-05. The branch stays `audi-1330-savings-residual-defects`
because it is already pushed and PR #1290 is open against it; the PR title and this folder carry the
correct key. This is the same PR-number/AUDI-number offset trap recorded in `reference_airflow_ti` —
**always resolve the branch with `gh pr view <N> --json headRefName`, never by the number.**

The code shipped as PR #1290 against `SteelHouse/airflow-ti`, base `016e161`.

### What was wrong

All three came out of the 2026-09-04 Mode savings audit
(`tickets/audi_1325_debugger_optimizer_adoption/outputs/audi_1325_mode_savings_audit_2026_09_04.md`,
defect 12), found by an adversarial probe rather than by the AUDI-1326 ticket that shipped the code.

1. **No multiplicity correction on the summed all-time headline.** `savings()` admitted each job on
   its own 90% interval and then summed the admitted jobs. A 200-rep null simulation where every job
   truly saved zero (20 jobs per rep, 7 sweep-days each side, `exec_h ~ N(100, 12)`, seed 20260905)
   reported a positive total with an interval excluding zero in **135 of 200 reps at `016e161`**, mean
   131.5 exec-h claimed against a truth of zero. The per-job rate was a correct 5.1%; the aggregate was
   67.5%. It is forced, not chance: `total > sum(half_i) >= sqrt(sum(half_i^2)) = half`.
2. **`shipped()` never reset `outcome` once set to `resolved`,** so a job later marked `wont_fix` was
   frozen as a permanent win while running at full pre-fix hours.
3. **A fix's before-window had no lower bound,** so on a job with two successive fixes the later fix's
   baseline averaged in the era before the first.

### What shipped

- `_holm_level(dict)` returns the single one-sided p threshold Holm's step-down rejects at
  (`ALPHA/(k-m+1)`, or `ALPHA/k` when `m=0`), which is exactly Holm's rejection region. Aggregate
  false-positive rate **67.5% -> 4.5%**. Single-job calibration is preserved bit-for-bit: 2,000 reps
  with one job each gives 101/2000 = 5.0% before and after.
- **`_T90`/`_t_crit` deleted.** Admission used the exact `_t_sf` tail while the published half-width
  came from `_t_crit`'s linear interpolation over a convex table, so the two disagreed in BOTH
  directions: interpolating a convex function overstates it below `df=60` (peak error +0.00465 at
  df=42) and the flat 1.645 fallback understates it above (-0.0248 at df=62). New `_t_inv(level, df)`
  inverts `_t_sf` by bisection and every published half-width is `_t_inv(level, df) * se`. **195 of
  4,000 simulated rows printed an interval clear of zero that the gate had rejected; now zero do.**
- Labels made honest: `savings()` returns `level`, and the headline, header and CI column print
  `_coverage(level)` rather than a hardcoded "90%".
- `shipped()` resets a stale `resolved` outcome. **The gauntlet narrowed this**: the first cut reset on
  any `STICKY` state, but `owner_notified` does not contradict a fix having worked, so it dropped
  genuine resolutions. Now only `wont_fix` undoes a resolve.
- `before_days` is lower-bounded at the previous `applied_date` for that `(dag_id, surface)`.

### The crash this caught

`ledger.py:734` indexed `fixed_on[fix[:2]]` unguarded. A resolved shipped row whose `applied_date` is
empty never enters `fixed_on`, so `savings()` raised
`KeyError: ('site_network_hourly', 'spark')`. **Reachable from prod**: `apply_manifest()` passes
`row.get("applied", "")`, `mark_applied()` validates `fix_pr` but never the date, `_mark_resolved()`
copies `applied_date=""` onto the resolved entry, and `sweep.py:306` calls `savings()` unguarded in the
daily task. **One manifest line missing `applied` would have failed the sweep permanently.** `016e161`
returns cleanly on the same fixture, so this was introduced by the first repair pass and caught by the
adversarial verifier before it left the worktree. `savings()` now skips such a row from measurement and
surfaces it in the not-yet-measurable rows with "no applied date recorded, so there is no before and
after to compare".

`sweep.py:306` still calls `savings()` with no `try`/`except`. That is deliberate: a blanket except
there would mask the next real bug the same way.

### Validation

248 tests pass (`python -m pytest include/spark_optimizer/tests/ -q`), ruff clean against
`include/spark_optimizer/ruff.toml`, `compileall` clean. Three new tests fail at `016e161` and pass
here. `/pr_gauntlet` at `medium`: round 1 found 3, refuted 2, confirmed and fixed 1; round 2 empty.

### Open

- Merge PR #1290.
- File the Jira issue, on Malachi's yes.
- IMP-128 and IMP-129 (the `exec_h` per-DAG-day invariant, and `mark_applied` cloning the prior
  `exec_h`) are related ledger defects that this branch does NOT fix.
