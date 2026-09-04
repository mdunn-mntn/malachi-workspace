---
doc_type: ticket
title: "AUDI-1329: Measure detector and fleet coverage"
status: in_progress
date: 2026-09-04
summary: "Detector taxonomy gaps plus what fraction of the fleet is ever scanned"
result: "debugger 88% of reported failures, 0% of the silent class; optimizer 28% mean, 40% best"
question: "What share of our failures does the debugger catch, and what share of our fleet does the optimizer scan, with the uncovered classes and jobs named?"
framing_state: locked
---

# AUDI-1329: Measure detector and fleet coverage

**Jira:** https://mntn.atlassian.net/browse/AUDI-1329
**Status:** backlog
**Date Started:** 2026-09-04
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** What share of our failures does the debugger actually catch, and what
  share of our fleet does the optimizer actually scan, expressed as two numbers with the uncovered
  classes and the uncovered jobs named?
- **Goal (why / the decision):** "It covers everything" is currently an assertion. IMP-104 is the
  standing counterexample: `site_network_hourly` loses whole hours to FetchFailed storms, reports
  SUCCEEDED, and passes both tools clean. Until coverage is a number, nobody can say whether the
  next silent failure is an outlier or the norm, and AUDI-1325's adoption case cannot be made to
  another team.
- **Objective (done-when):** A coverage number exists for each axis, with the uncovered classes and
  the uncovered jobs listed by name, and the ceiling that retention imposes stated separately from
  the ceiling our own code imposes.
- **Approach (how):** Detector axis: build a taxonomy of failure and inefficiency classes actually
  observed, then map each to the detector that catches it; the residue is the gap. Fleet axis: count
  what fraction of runs are ever scanned and establish what the event-log retention caps it at.
  Sources: the 3,627 real task logs under `on-call/airflow_logs/`, the optimizer's published sweeps
  in `gs://mntn-data-archive-prod/optimizer/`, the ledger, and the Airflow REST API for the
  denominator. Assumptions to resolve empirically first: whether a denominator of all runs is even
  obtainable, and whether "scanned" means the crawl saw the job or the detectors ran on it.
- **What would change the answer:** If the uncovered residue is dominated by classes that cannot be
  detected from the artifacts we retain, the deliverable is a retention argument, not a detector
  backlog. If the fleet denominator turns out unobtainable, the fleet axis becomes a bound rather
  than a number and must say so.

## 1. Introduction
Neither tool's coverage had ever been measured. "It covers everything" was an assertion, and IMP-104
was the standing counterexample. This spike puts a number on each axis and names what is uncovered.

Full record: [audi_1329_coverage_2026_09_04.md](outputs/audi_1329_coverage_2026_09_04.md), 306 lines.

## 2. The Problem
Without a coverage number nobody can say whether the next silent failure is an outlier or the norm,
and AUDI-1325's adoption case cannot be made to another team. The going-in hypothesis, carried from
AUDI-1327, was that retention is the binding ceiling. That hypothesis is refuted below.

## 3. Plan of Action
Four axes measured in parallel, each adversarially reviewed twice, then synthesized with the
refutations folded in. 4 of 8 reviews refuted, all four against the optimizer half; the debugger half
reproduced verbatim under independent re-execution.

## 4. Investigation & Findings

**The two numbers.**

| Axis | Coverage | Basis |
|---|---|---|
| Debugger, failures Airflow reports as failed | **88%** (215 of 245) | measured; 90.5% on prod terminal failures, agreeing once split by outcome |
| Debugger, all failure events including retry-recovered | 46.1% (59 of 128) | measured |
| Debugger, the silent class | **0%, by construction** | 33 lost output partitions in 30 days sit behind GREEN runs |
| Optimizer, Spark task instances per day | **28% mean** (215.4 of 763) | measured across 14 published sweeps |
| Optimizer, best sweep de-duplicated | 40% (305 of 763) | 39 of the 09-03 sweep's sources were re-scans of 09-02 |
| Optimizer, worst sweep | 20% (154 of 763), on **6 of 14 sweeps** | measured |

**Retention is not the ceiling. This reverses the going-in assumption.** Retention removes nothing
either tool could otherwise read. The optimizer's gap is our own code plus engine opacity; the
debugger's gap is a detector backlog, not expired evidence. The one place retention genuinely binds
is the debugger's retrospective Dataproc deep-dive, which is not how the tool runs in production.

**A live defect, not a design limit: 6 of 14 sweeps ran at 154 jobs instead of ~344 because 194 of
200 archive downloads failed, and the sweep published a confident backlog anyway.** Their headers all
read `(newest 6 of 200, 194 failed)` and nothing surfaced an error. `phs.list_batches` returns `[]`
on any `gcloud` error without raising (`phs.py:35-38`), and the DAG catches the exception into
`phs_n = 0` with only a warning. Restoring those six sweeps moves the mean from 215 to ~297/day,
**28% to 39%, with no design change**.

**The biggest single block is Databricks opacity.** 195 Spark task instances a day (26% of the fleet)
run on an engine that writes nothing readable: `DbxDbtOperator` 144.7, `ModelPysparkDbxJobOperator`
48, `ModelPysparkWorkflowOperator` 2. No cap or retention change touches it; it needs
`cluster_log_conf` on the job clusters and `DATABRICKS_PROFILE` set on the prod deployment.

**Caps are coupled, so raising one alone is inert.** `MAX_BATCHES=150` re-binds at 195-201 eligible
inside the 500-slice, so it and `phs.list_batches(limit=500)` have to move together; then
`MAX_BYTES=4 GiB` (`phs.py:83`) binds, because 460 PHS dirs at a 4.4 MB mean is ~2.0 GiB against an
archive half already charging 1.81 GiB to the same tmpdir. Moving all three reaches roughly
**600 of 763 = 79%**.

**Detector inventory, measured for the first time.** The optimizer has 21 detectors; **8 have never
fired** in 11 days and 1,692 ledger rows, and `analyze_plan` — the entire plan-text half of
`optimizations.py` — has **zero production firings**, because the sweep feeds it event logs and never
plan text. The debugger exercises **27 of its 48 keys**; 20 have never matched a real log, and all 44
`signatures.py` entries carry `engine="any"`, so `classify()`'s engine filter is dead code.

**Recall is not accuracy.** 132 of 3,068 SUCCESS logs also carry a signature (4.3%).
`path_not_found_late_data` fires 72 times on success against 4 on failure; `spot_preemption` 50
against 2. At least 10 of the 215 matches rest on a key that does not discriminate — a lower bound,
since the other 205 were not hand-verified.

**Uncovered waste classes with no detector, each measured on real event logs:**
- **Driver prologue idle.** 754 idle executor-hours in one day from two jobs, 73% of their combined
  wall clock. 8 of 36 runs were over 90% prologue; the worst held 63 registered executors for 7,035
  seconds with zero tasks launched. This is IMP-106 generalised, and `idle_reserved_executors` fires
  on these apps with a tail remedy for jobs that have no tail.
- **Small-file read amplification.** `site_network_hourly` reads 92.1% of its 35,609 tasks at under
  8 MiB each against `maxPartitionBytes=256 MiB`. No detector reads Input or Output Metrics at all.
- **Sub-second task swarm.** Four jobs run 32,828 to 154,018 tasks with 97.9-99.8% finishing under a
  second. `shuffle_partition_sizing` only fires when partitions are too big.

**Structural invisibility.** 966 of 5,246 archive objects (18%) can never be read: 453 permanently
`.zstd.inprogress`, accreting 13-18/day since 2026-08-21 and disproportionately the crashed, killed
and longest-running apps, plus 385 `_GHFS_SYNC_TMP_FILE`. The sweep window also spans 28-40 hours
against a 24-hour schedule, and is hour-of-day biased: 255 of 340 scanned apps ran 00:00-08:59 UTC.

**PHS admission defaults foreign work in.** `(labels or {}).get('team','ti') == 'ti'` (`phs.py:23`)
admits any unlabeled batch as ours: 89 of 175 admitted had no team label, and 22,133 unlabeled
batches were admitted across a 100k pull.

**The silent class belongs to neither tool.** The debugger fires only on FAILED; the optimizer sweeps
only SUCCEEDED and looks for inefficiency, not correctness. An output-contract check — list the
`hh=`/`dt=` directory two hours after the run and assert `_SUCCESS` — measured **18 of 18 recall at
100% precision**, the only one of five candidate sources that worked. Three models swallow the
exception in a per-partition loop (`site_network_hourly.py:246`, `aug_log_ip_hourly.py:202`,
`aug_log_ip_vertical_id_hourly.py:288`), and neither
`utils_model/gcs_path_utils.py:filter_existing_gcs_paths` nor `writer.py:_filter_relpath` gates on
`_SUCCESS`, so a partial partition reads as good downstream. Until that is settled the marker is
decorative.

## 5. Solution
Measurement only; no code changed. The ranked leverage list is §9 of the output document. The top
three, in order: fix the archive-half download failure and make a degraded sweep refuse to publish
clean (28% to 39%, no design change); raise `MAX_BATCHES`, the PHS list limit and `MAX_BYTES`
together (to ~79%); enable `cluster_log_conf` and `DATABRICKS_PROFILE` for the Databricks 26%.

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
