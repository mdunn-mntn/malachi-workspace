# Jira drafts — P0 through P3 (2026-09-04)

Not filed. Awaiting confirmation. All four parent to epic AUDI-1290, label `q3_2026`.
Tasks carry PMO Rep Bryce Wagg (`customfield_15612` = `17863`) and story points
(`customfield_10012`); Release Type omitted (internal tooling, per the standing rule).
Spikes file under AUDI by org routing and need only project, issuetype `11467`, summary.

| # | Type | SP | Summary |
|---|---|---|---|
| P0 | Task | 3 | Fix the optimizer's savings figure and the retry that mass-resolves findings |
| P1 | Task | 5 | Pin the debugger's replies to real logs, then fix the downstream-cause parser |
| P2 | Spike | 3 | Measure whether the optimizer's recommendations actually work |
| P3 | Spike | 3 | Measure detector and fleet coverage for the debugger and optimizer |

---

## P0 — Task, 3 SP

**Summary:** Fix the optimizer's savings figure and the retry that mass-resolves findings

```
The daily optimizer is publishing a savings figure to Slack that no completed fix supports, and a retried sweep would mark hundreds of findings fixed when they are not.

*Why:* The 2026-09-04 sweep published "115 hours all-time, ~$32 all-time" to the digest. All 60 fixes shipped 09-03 are still watching with zero days of evidence. The number comes from one job with a single before-day measurement (687.7 executor-hours) against a normal daily range of 592-954, multiplied by days elapsed, so it swings by multiples day to day. Separately the sweep has one retry and writes its record to storage before posting to Slack, so a later failure makes the retry read today's own rows as history. Simulated on the live record: first attempt marks 0 findings fixed, the retry marks 209.

*Task:*
* Gate the hours and dollar figures on a minimum of measured days per job; say why they are withheld instead of printing a number ([savings|https://github.com/SteelHouse/airflow-ti/blob/main/include/spark_optimizer/ledger.py#L497])
* Report a range, not a single value
* Restrict the grace window to dates before the sweep's own date ([classify|https://github.com/SteelHouse/airflow-ti/blob/main/include/spark_optimizer/ledger.py#L121], [_mark_resolved|https://github.com/SteelHouse/airflow-ti/blob/main/include/spark_optimizer/ledger.py#L174])
* Name the shipped pull request on any digest row that has one ([sweep|https://github.com/SteelHouse/airflow-ti/blob/main/include/spark_optimizer/sweep.py#L246])
* Bucket the recurring state so those findings stop vanishing from the digest ([delta|https://github.com/SteelHouse/airflow-ti/blob/main/include/spark_optimizer/ledger.py#L645])

*Done-when:* a simulated retry marks nothing fixed beyond the first attempt; the digest names the pull request against every finding that has one; the savings surface shows a range or states it lacks evidence; each has a test that fails before the change.
```

---

## P1 — Task, 5 SP

**Summary:** Pin the debugger's replies to real logs, then fix the downstream-cause parser

```
The debugger's root-cause feature does nothing on the production failure it was built for, and a fully green test suite says otherwise. Build a corpus of real logs first, then fix the parser under it.

*Why:* Pull request 1285 shipped 09-03 to make a wrapper failure name the error its downstream job ended on. Run against the real 09-03 conversion_signal_backfill_workflow failure it produces none of the three expected strings and emits the raw log paste it was written to remove. Three defects: the log is fetched newest-first so the error-region search anchors on the wrong end and captures start-up noise; the logging service joins each Java stack onto one tab-separated line so the line-by-line search for the underlying cause never matches; and the new unreachable-database signature does not fire on an unreachable database. All 53 tests pass because the fixtures are hand-written, oldest-first and newline-separated, a shape production never emits.

*Task:*
* Capture one real production log per signature class, in the shape the logging service returns ([test_parse|https://github.com/SteelHouse/airflow-ti/blob/main/include/airflow_debugger/tests/test_parse.py])
* Pin the rendered Slack reply per class, not only the parse ([slack_block|https://github.com/SteelHouse/airflow-ti/blob/main/include/airflow_debugger/slack_block.py])
* Fetch oldest-first, or reverse before locating the error region ([dataproc_rca|https://github.com/SteelHouse/airflow-ti/blob/main/include/airflow_debugger/dataproc_rca.py])
* Handle tab-joined stacks in the underlying-cause parser ([parse|https://github.com/SteelHouse/airflow-ti/blob/main/include/airflow_debugger/parse.py])
* Make the unreachable-database signature fire on the real failure ([signatures|https://github.com/SteelHouse/airflow-ti/blob/main/include/airflow_debugger/signatures.py])

*Done-when:* that failure renders as a connection timeout, raised through the Postgres driver, at spark_read_host.py line 27, from the captured real log, pinned in continuous integration; every signature class has a real-log fixture and a pinned reply.
```

---

## P2 — Spike, 3 SP

**Summary:** Measure whether the optimizer's recommendations actually work

```
Measure whether the optimizer's recommendations are actually right, using the 60 fixes shipped on 2026-09-03 as a labelled set.

*Why:* Nobody has checked. The tool says a job is spilling and names a knob; whether the knob helped has never been measured per recommendation. The 09-03 merge train gives 60 fixes across 8 pull requests whose findings resolve around 09-07, so the evidence collects itself. Resolution counts distinct dates present in the record rather than sweep runs, so a quiet fleet or an empty crawl stalls it; 09-07 is an estimate.

*Task:*
* Per fix, measure whether the finding went quiet and whether executor-hours moved, with an interval not a point
* Split the result by detector and signature class so precision is known per class, not in aggregate
* Report the rate at which a shipped fix kept firing
* Write the per-class numbers to the knowledge base

*Done-when:* all 60 carry a verdict of worked, did not work, or not enough data, and the per-class precision is recorded. Depends on the savings estimator being trustworthy first.
```

---

## P3 — Spike, 3 SP

**Summary:** Measure detector and fleet coverage for the debugger and optimizer

```
Establish what fraction of our failures and our fleet the two tools actually see, so "it covers everything" becomes a number.

*Why:* Neither axis is measured. site_network_hourly loses whole hours to shuffle-fetch storms and reports success, and both tools pass it clean, so we know the detector set has holes. The sweep reads the newest 200 event logs of 200 available, and flagged Spark event logs disappear from the archive bucket within hours, so the ceiling on how much of the fleet is ever scanned is unknown.

*Task:*
* Build a taxonomy of failure and inefficiency classes actually observed in the fleet and map each to the detector that catches it; output the classes with no detector
* Measure what fraction of runs are ever scanned, and what the event-log retention sets as the ceiling
* Quantify what the disappearing event logs cost in coverage

*Done-when:* a coverage number exists for detectors and for the fleet, and the uncovered classes are listed.
```

