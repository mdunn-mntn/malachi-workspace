# Debugger + optimizer — execution plan, P0 through P5 (2026-09-04)

Companion to `audi_1325_priority_plan_2026_09_04.md`, which argues the order. This one says what
gets built, in what sequence, and what closes each item.

Both parent tickets (AUDI-1191, AUDI-1194) are closed Done/Done. Nothing below extends them; each
unit gets its own ticket. AUDI-1325 as filed scopes IMP-107, which is P5 here, so it is not the
home for P0 through P4.

## Sequence

P0 and P1 are independent and can run in parallel. P2 is gated on P0's estimator. P4 is gated on
P1's corpus and P2's ground truth. P3 is independent of all of them.

```
P0 savings + resolution correctness  ──┬──> P2 validate why/fix ──┐
P1 corpus + #1285 acceptance case    ──┴────────────────────────  ┴──> P4 AI layer
P3 coverage measurement              ─────────────────────────────
P5 IMP-107 setup path (deferred)
```

---

## P0 — stop publishing wrong numbers

**Type:** Task (defect fix). **Size:** S-M, days. **Surface:** `include/spark_optimizer/`.

Three defects are live on the Slack digest, a surface leadership reads.

1. **Gate the dollar figure.** `optimizer_savings.md` and the digest published "115 hours all-time,
   ~$32 all-time" at 09:19 UTC on 09-04 while every one of the 60 merge-train fixes is `watching`
   with 0 days observed. Require n>=5 before-observations per DAG before that DAG contributes, and
   render "not enough data yet" rather than a number when the bar is unmet.
2. **Report an interval, not a point.** The estimator is a per-DAG mean difference against a
   one-day baseline multiplied by the after-day count. The one DAG carrying the current figure has
   n=1 (687.7 exec-h, 2026-08-26) against a 592-954 exec-h daily range. Publish a range.
3. **Fix the retry off-by-one.** `seen_dates` at `include/spark_optimizer/ledger.py:121` is built
   from the whole prior ledger with no filter to dates before the current sweep date. The DAG runs
   `retries: 1` and uploads the ledger before the digest render, the second publish and the Slack
   post, so try 1 can persist and then fail. Filter to `d < date`.
4. **Fix the digest buckets.** Surface `fix_pr` on any finding that carries one, so a finding fixed
   hours earlier stops reading as untouched chronic backlog. Bucket `recurring` in `delta()` so the
   3 that vanish from the digest reappear.

**Done when:** a simulated retry of a sweep resolves zero keys beyond the first try; the digest
shows the shipped PR against every `fix_pr` finding; the savings surface either shows an interval
or states it has insufficient data. Each of the four has a test.

**Also fix here if cheap, else log:** `mark_applied` fabricates an `exec_h` datapoint by copying the
previous entry (poisons the `before` baseline on a DAG's second fix); `classify()`, `latest()` and
`_mark_resolved()` take `past[-1]` in file order rather than by max date (a backdated manifest line
lands at EOF and becomes the key's "last" row); `optimizer_savings.md` prints 114.8 on two rows
under a 115-hour headline; `shipped()`'s DCU/h columns are structurally dead and the register is
never emitted by the sweep at all, only by the `__main__` CLI.

---

## P1 — the corpus, then the #1285 acceptance case

**Type:** Task. **Size:** M-L. **Surface:** `include/airflow_debugger/`. This is IMP-108 promoted.

The order inside this item matters. Build the corpus first, then fix the parser under it, so the
same class of defect cannot ship green again.

1. **Capture a real-log corpus, one case per signature class.** Sources: `on-call/airflow_logs/`
   and Cloud Logging directly. Store them in the shape prod actually returns: descending order,
   tab-collapsed Java stacks. The current fixtures in
   `include/airflow_debugger/tests/test_parse.py` (`_JDBC_DRIVER_LOG`) are hand-written,
   newline-separated and ascending, which is why 53/53 passed on a broken parser.
2. **Pin the rendered reply, not just the parse.** Golden file per signature class, asserted on the
   Slack block output, so a wording or structure regression fails CI.
3. **Fix the log ordering.** `dataproc_rca` fetches Cloud Logging with `--order desc`, so
   `error_region`'s `rfind("Traceback")` anchor lands on the outermost frame and the 2000-char
   window runs forward into post-failure startup noise. Fetch ascending, or reverse before slicing.
4. **Fix `exception_chain` for collapsed stacks.** Cloud Logging joins each Java stack onto one
   tab-separated line, so the MULTILINE `^\s*Caused by:` anchor never matches. This is a second,
   independent defect: correcting the ordering alone does not satisfy the acceptance case.
5. **Make `db_unreachable` fire.** It was added by #1285 and returns None on the actual
   reachability failure, so the reader gets the generic "pull the downstream job's own log" remedy.

**Done when:** `conversion_signal_backfill_workflow/submit_batch_dsid_21` (2026-09-03) renders
`java.net.SocketTimeoutException: Connect timed out`, through `org.postgresql.util.PSQLException`,
at `spark_read_host.py:27`, from the real captured log, with that case pinned in CI; and every
signature class has a real-log fixture and a golden reply.

**Watch today:** the 17:00 UTC digest is predicted to render this failure as the unparsed traceback
paste. That prediction is itself a check on the diagnosis.

---

## P2 — validate the why and the fix against ground truth

**Type:** Spike (deliverable is a measured answer, not code). **Size:** M. **Gated on P0.**

The labelled set already exists and costs nothing to collect: 60 applied fixes across 8 PRs, with
resolution landing around 09-07 under `RESOLVE_SWEEPS = 3`.

1. Per applied key, measure whether the finding went quiet, and whether executor-hours actually
   moved, with an interval rather than a point estimate.
2. Split the result by detector and by signature class, so "which recommendations are right" is
   answered per class rather than in aggregate.
3. Report the false-positive rate: findings whose fix shipped and which kept firing.

**Caveat that shapes the method:** resolution counts distinct ledger dates, not sweep executions.
A quiet fleet, an empty crawl, or a run of `complete=False` sweeps advances the window not at all.
The ledger already shows this: 2026-08-22, 08-23 and 08-24 are missing entirely despite digests
publishing on those days. So 09-07 is an estimate, not a date to plan against.

**Done when:** every one of the 60 has a verdict (worked / did not work / not enough data), and the
per-class precision numbers are written to `knowledge/`.

---

## P3 — coverage measurement

**Type:** Spike. **Size:** M. **Independent.**

Two axes, both currently unmeasured, and "covers every issue" is not answerable without them.

1. **Detector coverage.** Build a taxonomy of failure and inefficiency classes actually observed in
   the fleet, then map each to the detector that catches it. The output is the list with no
   detector.
2. **Fleet coverage.** What fraction of runs are ever scanned. The 09-03 sweep reads "newest 200 of
   200" and flagged Spark event logs vanish from `gs://mntn-data-archive-prod/spark-events` within
   hours, so the ceiling is unknown. Establish it.

**Standing proof this is real:** IMP-104. `site_network_hourly` loses whole hours to FetchFailed
storms and still reports SUCCEEDED, passing both tools clean.

**Done when:** a coverage number exists for each axis and the uncovered classes are listed.

---

## P4 — the AI layer

**Type:** Task. **Size:** L. **Gated on P1 (eval harness) and P2 (scoring).**

Adding it before those exist means no way to tell whether it made the recommendations better.

**Unblock it now, because the first step is an ask and not work.** The IMP-109 backlog row says no
key can exist on the deployment. That is false: `OPENAI_API_KEY` is already set on the prod Astro
deployment and read by `include/dbx/kube_operators.py`. The open question is whether reusing it for
the debugger is acceptable and who owns it. That question can be asked this week regardless of
where P1 and P2 stand.

Design constraints already fixed: keep the deterministic answer as the labelled floor, mark model
output unverified, and slot it below every evidence source in the existing `why()` precedence,
which already reserves WHY_LLM.

---

## P5 — IMP-107 setup path

**Type:** Task. **Size:** L. **Deferred under this plan.** This is AUDI-1325 as currently filed.

It is the only item that unlocks any team outside this squad. Deferring it is a reasonable call
given the state P0 and P1 just exposed, but it should be an explicit one, and AUDI-1325 should be
reframed or moved out of sprint 8649 rather than left looking active.

---

## Next sprint — implementation (IMP-110)

Start with the 39 DAG changes already scoped, analysed and agreed but never shipped, before
touching new findings.

| Ticket | Scoped | Shipped | Remaining |
|---|---|---|---|
| AUDI-1270 | 15 | 1 | 14 |
| AUDI-1275 | 13 | 1 | 12 |
| AUDI-1272 | 10 | 2 | 8 |
| AUDI-1269 | 10 | 6 | 4 |
| AUDI-1273 | 3 | 2 | 1 |

AUDI-1274 (2 of 2) and AUDI-1276 (4 of 4) are complete and need only a Jira transition.

**Gate on AUDI-1275's remaining 12:** the speculation canary on `site_network_hourly` (#1271) has to
be read first. Kill criterion recorded on the PR by Ryan Kleck: kill it if executor-hours rise while
wall-clock stays flat. Speculation was proposed and refuted twice before over GCS-writer corruption
risk, so the other 12 do not get it until the canary says it is safe.

Then the open fleet backlog: 336 findings, 162 high-impact, across 344 jobs on the 09-03 sweep. Each
fixed, `wont_fix`ed with a reason, or reassigned, with the #1284 manifest attributing every one.

## Ticket routing decision still open

P0 through P3 are four distinct units of work with no Jira home. AUDI-1191 and AUDI-1194 are closed
and should not be reopened to carry them. AUDI-1325 scopes IMP-107, which this plan defers. So this
needs either four new tickets under the AUDI-1290 epic, or a reframe of AUDI-1325 to cover
hardening with IMP-107 split back out. Not filed pending that call.
