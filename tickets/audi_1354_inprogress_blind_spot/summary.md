---
doc_type: ticket
title: "AUDI-1354: inprogress blind spot"
status: done
date: 2026-09-06
summary: "Unfinalized Spark event logs were invisible to the sweep, so 8 DAGs were never read and their findings resolved from non-observation."
result: "PR #1291 open, gauntlet PASS at thorough. Coverage of readable executor-hours 73.3% to 91.2%; two false resolves prevented on the 09-07 run."
question: "Does the sweep ever resolve a finding because it never read the job, rather than because the job got better?"
framing_state: "skip: IMP-130 named the defect with evidence before work started"
---

# AUDI-1354: inprogress blind spot

**Jira:** https://mntn.atlassian.net/browse/AUDI-1354
**Status:** done
**Date Started:** 2026-09-06
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

Branch `audi-1352-inprogress-blind-spot`, PR #1291, base `3b5a458`. **The branch is named for the
wrong key** — see the naming note at the end.

### The defect

`crawl.crawl` short-circuited any path ending `.inprogress`, and `fetch.newest_logs` never even
listed such an object (`if it["name"].endswith(".zstd")`). Because the resolution logic reads "no
finding emitted" as "the finding went quiet", **a job whose runs never finalize resolved its findings
from non-observation.**

Scale: **473 of 8,207 objects** under `gs://mntn-data-archive-prod/spark-events` carry the suffix.
**Eight DAGs were 100% invisible** (`vertical_size_monitor`, `household_score_distribution_monitor`,
`core_derived_advertiser_id`, `recency`, `page_views`, `ipdsc_ds_46`,
`fangorn_conversions_predictions_vertical`, `fangorn_household_predictions_vertical`).
`site_network_hourly` was half-blind: 19 runs read, 18 unread, the unread including one at **1,292
executor-hours**. Two findings would have falsely resolved on the 2026-09-07 run.

### What shipped

The rule is **"can this log still grow"**, not the filename:
- not appendable (renamed out of `.inprogress`, or a rolling log with its `appstatus_*` marker) -> billable
- `SparkListenerApplicationEnd` present -> billable whatever the name says
- appendable, no end marker, silent for `QUIET_H = 6.0` -> the writer is gone, measure to its last event

Two predicates, because **measurement and evidence are different questions**: `billable()` gates
`exec_h`, `dcu_h` and stage-metric baselines; `observed()` gates resolution, the observed-dag set and
the regression guard. A job with no observed run is held `unobservable`, and retired `lapsed` after
`UNOBSERVABLE_DAYS = 14`. Coverage of readable executor-hours goes **73.3% -> 91.2%**.

### The five rounds, and why they matter more than the diff

Each round fixed one defect and introduced the next. **All four self-inflicted defects were the same
confusion**, restated in a new place:

1. Made `.inprogress` logs visible. **Introduced:** a truncated log was parsed for cost, so a
   half-written run reported **3.767 executor-hours against the run's true 8.876** — and the number
   GROWS on each re-read, so the same run reports a different cost on consecutive sweep-days. A false
   *savings* generator, replacing a false *resolve* generator.
2. Required `ApplicationEnd` for measurement. **Introduced:** a genuinely finalized log with no end
   marker (a killed run) was discarded — `app-20260905035133235-0397.zstd`, **1,135.797 executor-hours,
   13.4% of the window**. Killed runs cost real money and are exactly the failures this tool exists to
   find, so discarding them biases the number toward looking good.
3. Widened to the three-state rule. **Introduced:** `sweep.py` fed the regression guard's dag set from
   `measurable` instead of `ended`, so a killed-only dag was un-held and **falsely resolved** — the
   original defect, one layer up.
4. Fixed the invariant, then added a retroactive savings basis. **Introduced:** its premise was
   measured false. Pre-branch rows never used an ended-cleanly basis (`crawl.py:93` at `3b5a458`
   excluded only `.inprogress` by NAME, and `crawl.py:79` measured a killed run to its last event), so
   the "shared basis" was not shared and the fallback **fabricated roughly 4,500 hours**.
5. **A reduction, not a patch.** Cut round 4 entirely: `savings()` is byte-identical to `3b5a458`
   again (md5 `89b85570df96f247762da5526204b491` over the JSON, headline and rendered table).
   `ledger.py`'s footprint on main dropped 186 -> 107 changed lines, `test_ledger.py` 60 -> 7.

**The lesson is the pattern, not any one bug: when three consecutive fixes break the same thing, the
fix is in the wrong place.** Rounds 1-3 patched symptoms of one confusion. Round 4 finally named it as
two predicates, and round 5 removed what should never have been attempted in the same branch.

### Verification

Five independent adversarial verifiers, one per round, each re-deriving from live data rather than
trusting the prior agent. Round 5's verifier called it "one test away" and was right: the cut had
removed the only ledger-level proof that a killed run's hours reach the dag total. Restored and
**mutation-checked** — swapping `measurable` for `ended` in `_hours_by_dag` now fails that test.

`/pr_gauntlet` at `thorough`: 12 agents, 2 rounds, 11 findings, 4 confirmed and fixed, round 2 clean.
292 tests pass, ruff and `compileall` green.

**One gauntlet fix reverted deliberately.** The stylist replaced `QUIET_H`'s derivation comment
("2.7x the longest silence measured inside a run here, 133.9 min over 188 runs") with "Stall
threshold: logs with no events for this duration are treated as finalized". The replacement restates
the code; a magic constant's measured basis is precisely the thing a reader cannot derive, which is
the one case the comment rule allows.

### Known limitation, not solved here

**IMP-131.** This changes what `exec_h` counts at a point in time. Every `applied` row in the live
ledger (54 of them, 50 applied 2026-09-03 and 4 applied 2026-08-27) has a before-window closing on or
before 2026-09-02, entirely under the old rule, and an after-window entirely under the new one. So
`savings()` will compare windows measured two ways. The two real options are to recompute the
pre-cutover window from archived logs, or to restart the savings series at the cutover. Both are
follow-up work; the retroactive column was tried in round 4 and cut because its premise was false.

### Naming

**The branch says `audi-1352` and that key belongs to someone else** — a triage Bug the debugger's
service account auto-filed (`[TRIAGE] mntn_match_tpa_export_prep/batch_prep - unclassified`). The real
key is **AUDI-1354**, filed 2026-09-06 after the work was done. This is the second time in two days:
`AUDI-1330` was taken the same way. **The triage account files into AUDI continuously, so any guessed
key is probably taken. File the Jira issue FIRST and name the branch from the key it returns.**
The branch stays as-is because PR #1291 is already open against it; the PR title and this folder carry
the correct key. Resolve a branch with `gh pr view <N> --json headRefName`, never by the number.
