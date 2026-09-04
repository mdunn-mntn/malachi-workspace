---
doc_type: ticket
title: "AUDI-1326: Fix the optimizer savings figure and the retry that mass-resolves findings"
status: backlog
date: 2026-09-04
summary: "Gate the savings estimate, fix the retry grace window, fix digest buckets"
result: "not started"
question: "Can the optimizer's savings figure and resolution rule be made to say only what the data supports, so a retried sweep changes nothing and no dollar figure appears without evidence?"
framing_state: locked
---

# AUDI-1326: Fix the optimizer savings figure and the retry that mass-resolves findings

**Jira:** https://mntn.atlassian.net/browse/AUDI-1326
**Status:** backlog
**Date Started:** 2026-09-04
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Can the optimizer's published savings figure and its resolution rule be made to say only what the data supports, so that a retried sweep changes nothing and no dollar figure appears without evidence behind it?
- **Goal (why / the decision):** The digest is a leadership-facing surface. It published "115 hours all-time, ~$32 all-time" on 2026-09-04 while all 60 fixes shipped 09-03 were still `watching` with zero days observed. Until this is right, every downstream measurement (AUDI-1328) and every claim about the tools' value rests on a number nobody can defend. Malachi is waiting on it; it gates AUDI-1328.
- **Objective (done-when):** A simulated retry of a sweep resolves zero keys beyond the first try; the digest names the shipped PR against every finding carrying `fix_pr`; the savings surface renders an interval or states it lacks evidence; the `recurring` state is bucketed. Each of the four has a test that fails on origin/main and passes after.
- **Approach (how):** Design each fix against the live prod ledger copy (1692 rows) rather than from code reading alone, simulating before/after numbers for each. Four defects, four patches, one PR through /pr_gauntlet. Assumptions to resolve empirically first: the correct grace-window boundary given that a sweep writes rows dated the previous day; the minimum n per DAG that makes the savings estimate stable, derived from the ledger's own per-DAG daily variance rather than picked.
- **What would change the answer:** If the per-DAG variance in the ledger turns out low enough that n=1 is defensible, the savings gate is unnecessary and only the retry and digest defects remain. If the retry path cannot actually persist the ledger and then fail (verify the upload order in `sweep.run()`), the 209-key mass-resolve is not reachable in prod and drops in priority.

## 1. Introduction
The Spark optimizer (`include/spark_optimizer/`) runs daily at 09:00 UTC as `spark_optimizer_daily`,
writes an append-only ledger to `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`, and
posts a digest to Slack. The digest carries a savings figure in hours and dollars. This ticket is four
correctness defects in that path, found by a verification pass on 2026-09-04 and confirmed by
independent adversarial replay against the live 1,692-row ledger.

## 2. The Problem

**1. The grace window counts the sweep's own rows, so findings resolve a sweep early. Already fired in
prod.** `classify()` builds `seen_dates` from the ledger as it stands when `record()` is called, not as
it stood before the sweep (`ledger.py:121`, no `< date` filter). `_mark_resolved` then takes the last
`RESOLVE_SWEEPS - 1` dates as the grace window, so any row already dated today slides the window from
{D-2, D-1} to {D-1, D} and a key quiet for two sweeps resolves instead of three.

Three live paths put today's rows there: a retry (the ledger uploads to GCS before the digest render,
the second publish and the Slack post, so try 1 can persist and then fail); `apply_manifest()` running
immediately before `record()` and writing `applied` rows dated the sweep day; and `record()` being
called once per surface, so bq, dbx and pod are each one sweep early even on try 1.

Path two already fired. The 2026-09-04 run succeeded on try 1, and the 60-row manifest wrote 50
`applied` rows dated 2026-09-03 before `record()`. That run stamped **28 keys `resolved`. All 28 last
fired 2026-09-01, two quiet sweeps, not three.** Those rows are in the live ledger and the digest
reported them cleared. Replay of the real ledger reproduces exactly 28, matching prod.

A second fault on the same path: `last = past[-1]` reads the sweep's own resolved row, so a retry skips
the key entirely and the retried digest silently drops its whole Resolved section. Replayed on
2026-09-02: 139 resolutions on try 1, 0 on a retry.

**2. The published savings figure is unsound.** The 2026-09-04 sweep published "Saved since 2026-08-27:
115 hours all-time, 115 in 2026; current rate 16.4/day, est. 5,986/yr (~$32 all-time, $32 in 2026, est.
$1,676/yr, estimated at $0.28 per hour)". The whole figure is one DAG, `fangorn_score_monitor`, with
n=1 before-observation and n=2 after. Three mechanisms compound:
- `daily_h` is last-write-wins over file order. 44 of 409 dag-days carry more than one distinct
  `exec_h`, because a retried sweep leaves a smaller partial day-sum. For fangorn on 2026-08-27 the file
  holds both 687.7 and 954.3, and which one the series takes is an artifact of line order.
- `mark_applied()` clones `exec_h` from the previous sweep's row into the applied row. Those synthetic
  points enter the series indistinguishably from measured ones. Fangorn's four applied rows carry 687.7
  copied from 08-26, and that copy won the last-write race.
- The estimator has no variance test at all: mean before minus mean after, times after-days.

**3. `delta()` drops a whole state.** It is an if/elif over new/chronic/resolved/fix_not_working/STICKY.
`classify()` can also set `recurring`, which matches no branch, so those entries reach no digest
renderer. On the 2026-09-04 sweep that dropped **51 of 186 spark entries (27%), 12 DAGs and 3,231
executor-hours**. Separately, a finding whose fix shipped hours earlier renders as untouched chronic
backlog with no mention of the PR: `advertiser_mid shuffle_fetch_wait:8/:9/:19`, streak 8, fixed by
PR #1281 on 2026-09-03.

**4. Ledger integrity, latent.** `_history` preserves file order, and `classify`, `latest`, `set_state`
and `mark_applied` all take `past[-1]`, so a row appended out of date order becomes the key's "last".
`render_shipped`'s DCU/h before and after columns can never populate: no ledger row carries a non-null
`dcu_h`, and the register is never emitted by the sweep at all, only by the `__main__` CLI.

## 3. Plan of Action
1. Verification pass, five probes, every claim independently refuted. Done 2026-09-04.
2. Design each fix against the live ledger, three adversarial reviews per design. Done: 4 designs, 12
   reviews, 0 refuted, ~50 required corrections folded back.
3. Apply in dependency order on `audi-1326-ledger-savings-correctness`: integrity, then grace window,
   then savings, then digest.
4. Gate: full suite, ruff, comment-rule audit, real-ledger replay.
5. `/pr_gauntlet`, then PR.

## 4. Investigation & Findings

Fidelity anchor for every replay below: reconstructing the 2026-09-04 sweep against the real ledger
reproduces prod exactly, 28 resolved keys against 28 rows in the live file.

| Defect | On origin/main | After |
|---|---|---|
| Grace window | 28 keys resolved on 2026-09-03, all after two quiet sweeps | 0 |
| Retry idempotence | 139 resolutions on try 1, 0 on the retry | identical both tries |
| `delta()` buckets | 135 of 186 bucketed, 51 dropped; firing set 107 findings / 40 DAGs / 9,331 exec-h | 186 of 186; 158 / 52 / 12,562 |
| Savings headline | "115 hours all-time ... ~$32 all-time" | "No measured savings to report: ..." with the reason |

Open questions carried into the PR description, not resolved here:
- The 28 false `resolved` rows dated 2026-09-03 stay in the ledger. The patch stops new ones; it does
  not correct the record. Unstamping them is a separate, deliberate data edit.
- `prior_sweep_dates` is computed across all surfaces, so a bq/dbx/pod key can be resolved from sweep
  dates on which only the spark surface recorded. Pre-existing, out of scope.
- `RESOLVE_SWEEPS` counts sweeps, not days, and the ledger has a three-day gap (2026-08-22 to 08-24).
  After an outage, "quiet for three sweeps" can span a week.
- Whether the applied date itself should count as a before-day. Today `d < applied_date` excludes it.

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
