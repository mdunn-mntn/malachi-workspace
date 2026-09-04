# Debugger + optimizer — priority order (2026-09-04)

Ordering the work Malachi set out: harden the two tools first, then spend the following sprint
implementing fixes across every DAG that needs them.

The verification pass run this morning (5 probes, every claim adversarially refuted by an
independent agent against live prod) changed the order. Two of the three headline features are
currently wrong in production, so correctness comes before coverage, and coverage before AI.

## What the verification pass found

Verdicts: sweep log PASS, #1285 deployed PASS, ledger reconciliation FAIL, savings FAIL, #1285
behavior FAIL.

Confirmed good:
- The 09:00 UTC sweep succeeded in 1121.9s with zero `manifest row skipped`, zero `ledger step
  failed`, zero tracebacks. All 60 manifest rows registered, zero drift against `shipped.jsonl`.
- #1285 is genuinely live: the running image `deploy-2026-09-04T15-09-52` was built from `63d4c4b`,
  which has both `277e5fd` (#1285) and `b1b741e` (#1284) as ancestors. The superseded-build gap did
  not bite this time.

Confirmed broken:
1. **#1285 is a no-op on its own acceptance case.** 53/53 tests pass, but run against the real
   2026-09-03 `conversion_signal_backfill_workflow/submit_batch_dsid_21` evidence it produces none of
   the three expected strings and emits the same pasted traceback region the PR was written to
   remove. Two independent defects: `dataproc_rca` fetches Cloud Logging with `--order desc`, so
   `error_region`'s `rfind("Traceback")` anchor lands on the outermost frame and the 2000-char window
   captures post-failure startup noise; and Cloud Logging tab-collapses each Java stack onto one
   line, so `exception_chain`'s MULTILINE `^\s*Caused by:` never matches. The new `db_unreachable`
   signature also does not fire on the real failure. Root cause of all three: the fixtures in
   `include/airflow_debugger/tests/test_parse.py` are hand-written, newline-separated and
   ascending-order, a shape prod never emits.
2. **The savings figure is live, leadership-facing, and unsound.** The 09:19 UTC sweep published
   "115 hours all-time, ~$32 all-time" to `optimizer_savings.md` and the 09-03 digest, six hours
   before the handoff said savings would stay $0. None of the 60 merge-train fixes contributed;
   all 60 are `watching` with 0 days observed. The estimator is a per-DAG mean difference against a
   one-day baseline multiplied by the number of after-days, resting on a single DAG with n=1
   (687.7 exec-h on 2026-08-26) whose own daily range is 592-954 exec-h. It grows superlinearly on
   noise and will swing by multiples day over day.
3. **A sweep retry resolves findings a sweep early.** `seen_dates` (`ledger.py:121`) is built from
   the whole prior ledger with no filter to dates before the current one, so on a retry the grace
   window slides from {D-2, D-1} to {D-1, D}. The DAG runs `retries: 1` and the ledger is uploaded
   to GCS before the digest render, the second publish and the Slack post, so try 1 can persist and
   then fail. Simulated against the real ledger: try 1 resolves 0 keys, the retry of the same sweep
   resolves 209.
4. **The resolution rule counts distinct ledger dates, not sweeps.** A quiet fleet, a crawl that
   returns nothing, or a run of `complete=False` sweeps advances the window not at all and stalls
   every pending resolution indefinitely. The ledger already shows the gap pattern: 2026-08-22,
   08-23 and 08-24 are missing entirely despite digests publishing on those days.
5. **The digest mis-buckets 10 of the 60 fixed findings.** `record()` runs after `apply_manifest()`
   and rewrites the same-day row, keeping `fix_pr` (intended, commit e00bd45) but taking the
   detector's state. 7 land as `chronic` and are read out with no indication a PR shipped hours
   earlier; 3 land as `recurring`, which `delta()` does not bucket, so they vanish from the digest.
   Attribution and resolution are intact, the register carries the truth. The digest is what people
   read first. (Mechanism caveat: a second probe attributed the 50/60 split instead to
   `apply_manifest`'s already-recorded dedup at `ledger.py:406-408`. Its refuter called that
   mechanism false. The same-day rewrite above is the reading that survived refutation.)
6. **`mark_applied` fabricates an executor-hours datapoint** at the applied date by copying `exec_h`
   from the previous entry. 50 of the 60 keys did not fire on the 09-03 crawl at all, so for six
   DAGs every 09-03 row is synthetic. Harmless today only because the savings window excludes the
   applied date itself; the second fix shipped for one of those DAGs pulls the fabricated point into
   its `before` baseline.
7. Two smaller ones, real but disputed on severity by the refuting agent: `classify()`, `latest()`
   and `_mark_resolved()` take `past[-1]` in FILE order rather than by max date, so a backdated
   manifest line lands at EOF and becomes the "last" row for its key; and `optimizer_savings.md`
   prints the same 114.8 hours on two rows under a 115-hour headline. `shipped()`'s DCU/h before and
   after columns are structurally dead: no ledger row ever carries a non-null `dcu_h`, and the
   register is never emitted by the sweep, only by the `__main__` CLI.

## The order

### P0 — stop publishing wrong numbers. Days, not weeks.

Findings 2, 3 and 5 are live on a surface leadership reads. Nothing else on this list matters while
the tool is confidently reporting a dollar figure it cannot support.

- Suppress or heavily caveat the all-time dollar figure until the estimator has a real baseline.
  A number that swings by multiples day over day costs more credibility than showing nothing.
- Filter `seen_dates` to dates strictly before the current sweep date. One-line fix, prevents a
  209-key mass-resolve on any retry.
- Give the estimator a defensible baseline: n>=5 observations per DAG before it contributes, and
  report an interval rather than a point.
- Surface the shipped PR in the digest for any finding carrying `fix_pr`, and bucket `recurring`
  so the 3 stop vanishing.

### P1 — the corpus, then #1285's acceptance case.

Finding 1 is the debugger's headline feature failing on the case it was built for, and the green
test suite is why it shipped. Fix the corpus first, then fix the parser underneath it, so the same
class of defect cannot ship again. This is IMP-108, promoted.

- Capture real prod logs per signature class, in the shape Cloud Logging actually returns them:
  descending order, tab-collapsed stacks. Pin the rendered reply, not just the parse.
- Then fix the two parse defects and `db_unreachable` under that corpus.
- Every reply defect to date was caught by a person seeing a bad Slack post. This is the change
  that ends that.

### P2 — validate why / fix against ground truth.

Malachi's "validate the suggestions where there are no gaps" is answerable empirically, and the
labelled set already exists for free: 60 applied fixes across 8 PRs, resolution landing around
09-07. Per key, measure whether the finding went quiet and whether executor-hours actually moved.
That is the honest answer to "are the recommendations right". It depends on P0 because an
untrustworthy estimator cannot score it.

### P3 — coverage: does it catch every issue?

Two distinct axes, both currently unmeasured:
- **Detector coverage.** Which failure and inefficiency classes have no detector at all.
- **Fleet coverage.** Which runs are never scanned. The 09-03 sweep reads "newest 200 of 200", and
  flagged Spark event logs vanish from `gs://mntn-data-archive-prod/spark-events` within hours, so
  the ceiling is unknown. IMP-104 is the standing proof that a real defect can pass through both
  tools clean: `site_network_hourly` loses whole hours to FetchFailed storms and still reports
  SUCCEEDED.

### P4 — the AI layer (IMP-109). Last, deliberately.

It needs P1's corpus as its evaluation harness and P2's ground truth as its scoring function.
Adding it before those exist means no way to tell whether it made the recommendations better.
Keep the deterministic answer as the labelled floor; `why()` already reserves a WHY_LLM slot below
every evidence source.

**Unblock it now, though, because it is an ask and not work:** the backlog row says no key can exist
on the deployment. That is false. `OPENAI_API_KEY` is already set on the prod Astro deployment and
read by `include/dbx/kube_operators.py`. The open question is whether reusing it for the debugger is
acceptable and who owns it.

### P5 — IMP-107 setup path. Drops out.

Under this plan it does not get done, and that should be a deliberate choice rather than a silent
one. It is the only item on the list that unlocks anyone outside this squad.

## Next sprint — implement the fixes (IMP-110)

Start with what is already scoped and pre-verified rather than with new findings. Five tickets
merged a subset of their own scope, leaving 39 DAG changes already analysed and agreed:

| Ticket | Scoped | Shipped | Remaining |
|---|---|---|---|
| AUDI-1270 | 15 | 1 | 14 |
| AUDI-1275 | 13 | 1 | 12 |
| AUDI-1272 | 10 | 2 | 8 |
| AUDI-1269 | 10 | 6 | 4 |
| AUDI-1273 | 3 | 2 | 1 |

AUDI-1274 (2 of 2) and AUDI-1276 (4 of 4) are complete and only need transitioning in Jira.

Gate: AUDI-1275's remaining 12 wait on the speculation canary verdict on `site_network_hourly`
(#1271). Ryan Kleck's kill criterion, recorded on the PR: kill it if executor-hours rise while
wall-clock stays flat. Speculation was proposed and refuted twice before for GCS-writer corruption
risk, so the other 12 should not get it until the canary is read.

Then the open fleet backlog: the 09-03 sweep carries 336 findings, 162 high-impact, across 344 jobs.
Each one fixed, `wont_fix`ed with a reason, or reassigned, with the #1284 manifest attributing it.

## Scope honesty

"Test on all our runs" and "cover every issue" are unbounded as written. The bounded version is a
pinned corpus plus a coverage number against it, so there is something to be done against.
