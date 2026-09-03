---
doc_type: ticket
title: "AUDI-1317: Publish pipeline regressions to the cost dashboard"
status: in_progress
date: 2026-09-03
summary: "Write the AUDI-1281 guard's regressions to the finding ledger and render them on Mode"
result: "Publisher built and demonstrated end to end: 3 regression keys written by a real sweep, digest line carried, all three resolved on schedule; fleet noise 0 in 278 gated judgements so it ships ungated; PR/Jira left to the dispatcher"
question: "Can the daily sweep write the regression guard's verdicts to the finding ledger so a doubling of spill or fetch wait renders on the dashboard and in the digest?"
framing_state: locked
---

# AUDI-1317: Publish pipeline regressions to the cost dashboard

**Jira:** https://mntn.atlassian.net/browse/AUDI-1317
**Status:** in_progress (code complete in the worktree, PR not opened)
**Date Started:** 2026-09-03
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-03 by the dispatcher from the ticket description and the AUDI-1278/1281 records it follows.
- **Question (the unknown):** Can the daily sweep write the regression guard's verdicts to the finding ledger so a doubling of spill or fetch wait renders on the dashboard and in the digest?
- **Goal (why / the decision):** AUDI-1281 built the guard and shipped no publisher, so a real regression is still invisible the morning it happens. Reliability lever under epic AUDI-1290, and the sweep-side option needs no new credentials.
- **Objective (done-when):** Merged airflow-ti change: the sweep runs the guard over every profiled DAG, writes each regression as its own ledger key, and the digest carries a regression line; a seeded regression appears in both and clears when the job recovers.
- **Approach (how):** Branch stacks on audi-1281-perf-regression-guard (PR #1279, unmerged) and the PR targets that branch, not main, until it lands; reuse the guard's own evaluate() rather than reimplementing thresholds; the new key follows the ledger's existing new/recurring/chronic/resolved replay so a recovered job resolves itself; test with the repo's own suite and a seeded row.
- **What would change the answer:** If the guard's verdicts prove noisy across the fleet (many DAGs flagged on a normal day), the publisher gates to chronic-only or the ticket reports the noise floor instead of shipping a firehose.

## 1. Introduction
Follow-on to AUDI-1281, whose regression guard flags a doubling of disk spill or shuffle-fetch wait against a job's own 30-day median but publishes nowhere. Its plan recorded two options for a live gate and shipped neither; this ticket takes the sweep-side one, which needs no new credentials.

## 2. The Problem
AUDI-1281 built a guard that judges a run's disk spill and shuffle-fetch wait against that job's own 30-day median, and shipped it as a CLI with no publisher. Nothing calls it on a schedule, so a real doubling is invisible on the morning it happens: the daily sweep's digest says nothing, and the Mode cost dashboard (`e81786de8403`), which reads the finding ledger, has no row to render. The optimizer can already say a job is expensive; it cannot say a job got worse.

Affects whoever reads the daily digest and the cost dashboard, which is the hackathon epic's own audience. The impact is missed detection time, not wrong data: a regression that nobody sees is paid for every day until someone notices the cost line.

## 3. Plan of Action
Written 2026-09-03 from §0, then executed in the same run. Branch `audi-1317-publish-regressions`, **stacked on `audi-1281-perf-regression-guard`** (PR #1279, open), so the guard, `stage_metrics.py` and the crawl/sweep/DAG changes AUDI-1281 added are already in the worktree and the diff below is only what this ticket adds. The PR targets `audi-1281-perf-regression-guard`, not `main`.

1. **Read the publish path end to end before writing anything.** `ledger.record` / `classify` / `_mark_resolved` (how a key is minted, replayed and resolved), `bq_profile.reports` (the Report/Finding shapes a non-crawl surface hands the ledger), `sweep.run` (where the stage-metrics file is written and where `record` is called), `digest.render`/`blocks`, and the live Mode queries on report `e81786de8403`.
2. **Confirm the Mode contract empirically**, not from memory: pull the six query bodies over the Mode API and read the regex the BigQuery card uses, so the regression title carries a figure the same shape.
3. **Mint the findings inside `regression_guard.py`**, reusing `evaluate()` unchanged. Add `Finding`/`Report` dataclasses shaped like `bq_profile`'s, a `finding_for(verdict, result)` that writes the title, and `reports(rows, dags, as_of)` that evaluates each DAG and returns one Report per DAG with a finding per regressing (stage, metric). Detector name `regression_<metric>`; `ledger.finding_key` appends the stage number from the title, so each regression is its own key (`regression_disk_spill:3`).
4. **Publish through the existing `record` call, not a second one.** A second `ledger.record(..., surface="spark")` on the same date would run `_mark_resolved` over the Spark surface again; folding the regression Reports into the one call the sweep already makes keeps replay, dedup and resolution exactly as they are. The regression Report carries `exec_h=0.0` so it cannot double-count the DAG's day total.
5. **Digest line.** `digest.render` and `digest.blocks` take the sweep's regression entries and print one line naming the count and the DAGs, plus a stat chip, so a reader sees a regression without opening the ledger.
6. **Tests in the package's own suite** (`include/spark_optimizer/tests/`): title/key shape, one key per (stage, metric), the Mode regex actually matching the title, `exec_h` not double-counted, the sweep writing a regression row, the digest line, and the full new -> quiet -> resolved replay through `ledger.record`.
7. **Validate**: `ruff check --config include/spark_optimizer/ruff.toml include/spark_optimizer/`, `python -m pytest include/spark_optimizer/tests/ -q`, `python -m compileall`.
8. **Seeded regression end to end through the real sweep** (not the guard CLI, which AUDI-1281 already demonstrated): restore a stage-metrics file whose baseline window is scaled so the real 2026-09-02 `intent_score_map` run reads above 2x, run `sweep.run` over the two real event-log dirs kept from AUDI-1281, and show the ledger row, the digest line and the Mode-regex parse. Then re-run the sweep on later dates with the true window and show the key going quiet and resolving on the third sweep after it last fired.
9. **Fleet noise floor, measured not asserted.** Download one full fleet day of flat event logs from `gs://mntn-data-archive-prod/spark-events/` in batches under 200 MB, parse to stage-metric rows, delete the logs, then replay the guard for every DAG with at least `min_runs` runs and count how many (stage, metric) pairs fire. If the rate is high, gate the publisher to chronic-only and say so in §5. *(Executed with one deviation: `gsutil -m cp` wedged with every part at zero bytes, so the download went through the package's own `fetch.download` (gcloud token + GCS JSON API). Same batching, same deletes. Detail in §4.3.)*
10. **Deliverables**: `artifacts/audi_1317_pr_body.md` (lint `--kind pr`), `artifacts/audi_1317_result_comment.txt` (lint `--kind completion`), guard/sweep/digest outputs under `outputs/`, findings written into §4-§7 as they land.

## 4. Investigation & Findings

### 4.1 The publish path, read before it was changed
- **The dashboard's source is the ledger, not a new table.** Mode report `e81786de8403` reads the BigQuery external table `mntn-prj-prod-00.optimizer.optimization_ledger`, which is an external table over `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`. So "publish to the cost dashboard" means "write a ledger row"; no new credential, no new sink, which is why the sweep-side option in AUDI-1281 §3.5 was the cheap one.
- **The Mode contract, pulled live rather than recalled** (`GET /api/mntn/reports/e81786de8403/queries`, keychain basic pair, 2026-09-03; six queries: `3ead7301daa8` BigQuery cost by task, `5a66e5fad18c` Savings headline, `6fdc8ae9ccf7` Applied fixes, `513a4a7a4a71` Savings by surface, `183d18f86de6` Top DAGs by findings, `a167f6ad0146` Findings over time). The BigQuery card gets its cost number by **regex over the finding title**:
  `CAST(REPLACE(REGEXP_EXTRACT(title, r'used ([0-9,]+) slot-hours'), ',', '') AS INT64)`.
  That is the shape a regression title has to match, so its title ends `; the run used 145 executor-hours`.
- **`ledger.finding_key` mints the key from the detector plus the stage number it finds in the title** (`"stage" <digits>`), deliberately ignoring every other digit so byte totals and task counts do not re-key a finding every sweep. Setting the detector to `regression_<metric>` therefore yields `regression_disk_spill:3` with no change to the ledger at all, and each (stage, metric) is its own key with its own replay.
- **Two ledger traps the design had to avoid.**
  1. `ledger.record` ends in `classify`, which ends in `_mark_resolved`; that function resolves every key of the given surface that this sweep did not report. A **second** `record(..., surface="spark")` call on the same date would therefore re-run resolution over the Spark surface. The regression Reports are folded into the one `record` call the sweep already makes.
  2. `record` sums `exec_h` per DAG across the reports it is given. A regression Report carrying the run's hours would double the DAG's day total, which is the input to the savings math. It carries `exec_h=0.0`, and the run's own hours live in the title instead. Test: `test_the_publisher_adds_nothing_to_the_dag_day_total`.
- **Grouping matters for cost, not just tidiness.** `evaluate` filters the whole rows list by DAG, and the sweep's metrics file holds the entire fleet for the 45-day retention window. Calling it once per DAG over the unsorted list is quadratic, so `reports()` groups rows by `dag_id` once and hands each DAG only its own.

### 4.2 Seeded regression, end to end through the real sweep
Driver `artifacts/audi_1317_demo_sweep.py`, output `outputs/audi_1317_seeded_sweep_demo.txt`. Four `sweep.run` calls over the two real `intent_score_map` event-log dirs kept from AUDI-1281 (`eventlog_v2_batch-8f1a450a`, 2026-09-01, and `eventlog_v2_batch-42e88a22`, 2026-09-02). Real: crawl, stage metrics, guard, ledger, digest. Stubbed: BigQuery, pod, Databricks, billing, Slack, so nothing prod is read or posted. The seed is the **baseline window** scaled to 0.4x on the first sweep, which is the same arithmetic as multiplying the run under test by 2.5 and leaves the measured run untouched.

| sweep | window | regressions | ledger rows | state |
|---|---|---|---|---|
| 2026-09-02 | 84 rows, spill x0.4 | 3 | 8 | `new`, streak 1 |
| 2026-09-03 | 84 rows, true | 0 | 5 | quiet (inside the grace window) |
| 2026-09-04 | 84 rows, true | 0 | 5 | quiet |
| 2026-09-05 | 84 rows, true | 0 | 8 | `resolved`, streak 0 |

Keys written, one per stage, all under `intent_score_map`:
```
regression_disk_spill:2  stage 2 disk spill 1319.9 GiB is 2.4x its 30-day median of 541.1 GiB; the run used 145 executor-hours
regression_disk_spill:3  stage 3 disk spill 4262.0 GiB is 3.2x its 30-day median of 1347.4 GiB; the run used 145 executor-hours
regression_disk_spill:6  stage 6 disk spill 3988.2 GiB is 2.2x its 30-day median of 1780.1 GiB; the run used 145 executor-hours
```
The digest carried the line on the first sweep and dropped it on the second, with no hand edit anywhere:
```
Regressed vs its own 30-day baseline
- HIGH intent_score_map - stage 3 disk spill 4262.0 GiB is 3.2x its 30-day median of 1347.4 GiB; the run used 145 executor-hours
```
**Resolution takes three sweeps after the last firing, not one** (`RESOLVE_SWEEPS = 3`, and `_mark_resolved` skips any key seen on the last two sweep dates). The demo shows it landing on 2026-09-05 exactly as the ledger's existing replay dictates.

### 4.3 Noise floor, measured across the fleet
The framing's kill criterion was "if the guard's verdicts prove noisy across the fleet, gate the publisher to chronic-only". Three independent measurements, all on real runs, all with the shipped thresholds (2x, `disk_spill >= 2 GiB`, `fetch_wait_ratio >= 0.05`, `min_runs = 5`, 30-day window):

| corpus | run-days judged | gated (stage, metric) pairs | regressions filed |
|---|---|---|---|
| whole fleet, 2026-09-02 (80 jobs) | 80 | 120 | 0 |
| `intent_score_map`, 2026-08-16..09-02 | 18 | 130 | 0 |
| `site_network_hourly`, 2026-09-01..09-02 | 2 | 28 | 0 |
| **total** | **100** | **278** | **0** |

- **Fleet day** (`outputs/audi_1317_fleet_noise_2026_09_02.txt`, corpus `outputs/audi_1317_fleet_stage_metrics_2026_09_02.jsonl`): all 157 flat event logs written on 2026-09-02, 1,433 MiB, downloaded in 9 batches of at most 180 MiB and deleted after each batch. 139 parsed, 18 unparsed (`no jobs, no stages, no ApplicationEnd`, the crawl's existing marker for a killed or empty run), 1,961 stage rows, 80 distinct jobs.
- **Only 3 of the 80 jobs could be judged at all** on one day of data: `aug_log_ip_hourly` (23 window runs, 20 gated pairs), `aug_log_ip_vertical_id_hourly` (23, 48) and `site_network_hourly` (9, 52). The other 77 are daily jobs with a single run that day, and the guard reports them as "no baseline", never as a regression. That is the correct behaviour and it is also the honest limit of this measurement: a same-day window is not a 30-day window.
- **The 30-day replay closes that gap for the one job that has 30 days on file.** `intent_score_map`, judged as of each of its 18 complete run-days with the real window that preceded it, fired zero regressions across 130 gated pairs, including 4 days on which stages 2 and 3 swapped ids (the swap AUDI-1281 found; the (name, task-count) match absorbs it).
- **Verdict: not noisy, so the publisher ships ungated.** Nothing was added to gate it to chronic-only. If the rate ever rises, the cheapest gate is in the digest, not the ledger: a key cannot become chronic without being recorded first, so the ledger must keep every firing and only the digest line would filter on `state == "chronic"`.
- **Deviation from the brief, forced by the tool.** The download was told to use `gsutil -o "GSUtil:check_hashes=never" cp -r`. `gsutil -m cp` wedged on this archive: 14 `.gstmp` parts created, all zero bytes, no progress in 6 minutes, exactly the failure mode that made the sweep abandon gsutil (memory `project_airflow_optimizer`, 2026-09-02, "gsutil BANNED on Astro pods"). It reproduces on this Mac, so it is not a pod-CPU artifact. The backfill uses the package's own `fetch.download` (gcloud token + GCS JSON API `alt=media`) instead, which moved all 1,433 MiB with zero failures.
- **Second footgun, same script:** `nohup python3 ...` does not get the shell's `python3` alias and resolves to `/usr/bin/python3` (3.9), where `datetime.UTC` does not exist and `stage_metrics.run_date` raises. Use `/opt/homebrew/opt/python@3.11/bin/python3.11` explicitly in any backgrounded command.

### 4.4 The dashboard query
`queries/audi_1317_mode_regressions.sql`, validated against the live table through `bq_run.sh` (`--dry_run` then a real run, `--location=us-central1`, `LIMIT 100`): 1,439 rows read, 0.001 GB processed, empty result, which is correct because no regression row exists in prod until this merges. It parses the ratio out of `is ([0-9.]+)x its`, the hours out of `used ([0-9,]+) executor-hours`, and the stage out of the key, exactly mirroring the BigQuery card.
- **Gotcha for anyone re-running it:** a `.sql` file whose FIRST line is a `--` comment cannot be passed to `bq_run.sh` as an argument. The bq CLI parses the leading `--` as a flag and fails with `Unknown command line flag`. Use a `/* */` header instead. The truncation point in the error message moves with the query text, which makes it look like a quoting bug in the shell; it is not.

## 5. Solution
**PR:** https://github.com/SteelHouse/airflow-ti/pull/1282 (opened 2026-09-03 PT; medium tier, 2 rounds: 3 findings confirmed and fixed (regressions were rendered and counted twice in the digest, two new docstrings trimmed); the fixer's reformatting of regression_guard.py was reverted; 197 tests green; base is audi-1281-perf-regression-guard until #1279 merges)

Branch `audi-1317-publish-regressions` in `SteelHouse/airflow-ti`, stacked on `audi-1281-perf-regression-guard`; the PR targets that branch. Five files, +256 / -4 against the stack base, no DAG change and no new credential.

**`include/spark_optimizer/regression_guard.py`** (+79) — the publish half, on top of `evaluate()` unchanged:
- `Finding` and `Report`, shaped exactly like `bq_profile`'s so `ledger.record` consumes them with no ledger change.
- `title_for(verdict, result)` writes the one line the ledger and the dashboard both read: `stage 3 disk spill 4262.0 GiB is 3.2x its 30-day median of 1347.4 GiB; the run used 145 executor-hours`. The stage number is what `ledger.finding_key` keys on; the trailing hours are what the Mode regex extracts.
- `finding_for` sets the detector to `regression_<metric>`, so a key is `regression_disk_spill:3` and each (stage, metric) resolves independently.
- `reports(rows, dags, as_of)` groups the fleet's rows by `dag_id` once, evaluates each profiled DAG, and returns a Report only for DAGs that actually regressed. `exec_h` is 0.0 on every Report.
- `firing(entries)` picks this sweep's regression rows that are not resolved, for the digest.

**`include/spark_optimizer/sweep.py`** (+21) — `_regression_reports()` reads the stage-metrics file the sweep just wrote and judges the DAGs this sweep profiled; its output is folded into the **existing** `ledger.record` call rather than a second one, so replay, dedup and resolution are untouched. A guard fault is caught and printed; it cannot lose the ledger. `run()` returns `regressions`.

**`include/spark_optimizer/digest.py`** (+17) — `render()` and `blocks()` take the regression entries. The file digest gains a `Regressed vs its own 30-day baseline` section above the per-DAG blocks; the Slack parent gains an `N regressions` stat chip and a line naming the jobs.

**Tests** (+143, `tests/test_regression_guard.py` and `tests/test_sweep.py`): one key per regressing (stage, metric); the title parsed by the dashboard's own regex; a quiet run publishing nothing; the publisher adding nothing to the DAG day total; the full `new` -> quiet -> `resolved` replay; the sweep writing the row and the digest line; a stable stage writing neither; the Slack parent counting them.

**Not shipped, deliberately:** no chronic-only gate (§4.3 measured 0 regressions in 278 gated judgements, so there is nothing to gate), no new ledger surface, no Mode PR. The Mode card is a query, not code: `queries/audi_1317_mode_regressions.sql`, validated against the live table, to be pasted into report `e81786de8403` alongside the existing six.

**Validation run in the worktree:** `ruff check --config include/spark_optimizer/ruff.toml include/spark_optimizer/` clean on ruff 0.16.1 (CI pins >=0.16,<0.17); `pytest include/spark_optimizer/tests/ -q` 197 passed (189 on the AUDI-1281 base; this branch adds 8 tests); `python3 -m compileall` clean on the package and `dags/spark_optimizer_daily.py`; `lint_comments.py` clean on the three changed modules.

## 6. Questions Answered
- **Q:** Can the daily sweep write the regression guard's verdicts to the finding ledger so a doubling of spill or fetch wait renders on the dashboard and in the digest?
  **A:** Yes, and with no ledger change. The dashboard's source IS the ledger (`optimizer.optimization_ledger`, external over the sweep's JSONL), so the whole job is minting a well-shaped finding. Four real sweeps over real event logs wrote three regression keys, carried them in the digest, and resolved all three three sweeps after they stopped firing, without a hand edit.
- **Q:** Does each regression get its own key, and does a recovered job resolve itself?
  **A:** Yes. `ledger.finding_key` already extracts the stage number from a title, so a detector named `regression_disk_spill` yields `regression_disk_spill:3`. The existing new/recurring/chronic/resolved replay then runs unmodified: `resolved` lands on the third sweep after the last firing (`RESOLVE_SWEEPS = 3`, and `_mark_resolved` skips any key seen on the last two sweep dates).
- **Q:** Is the guard too noisy to publish, so the publisher should gate to chronic-only?
  **A:** No. 278 gated (stage, metric) judgements over 100 real run-days produced 0 regressions: a whole fleet day (80 jobs), 18 run-days of `intent_score_map` against its real 30-day window, and 2 of `site_network_hourly`. The absolute floors do most of that work, and 77 of the 80 fleet jobs run once a day and simply have no baseline. Shipped ungated.
- **Q:** What does the dashboard need in the title?
  **A:** A figure a regex can read. The live BigQuery card uses `REGEXP_EXTRACT(title, r'used ([0-9,]+) slot-hours')`; the regression title ends `; the run used 145 executor-hours` and the new card reads it with `used ([0-9,]+) executor-hours`, plus `is ([0-9.]+)x its` for the ratio.

## 7. Data Documentation Updates
Handed back to the dispatcher for `knowledge/` routing:
- **The Mode cost dashboard parses numbers out of ledger `title` text with a regex** (`used ([0-9,]+) slot-hours` in query `3ead7301daa8`). Any new detector that wants a number on the dashboard must put it in the title in that shape. Report `e81786de8403` holds six queries.
- **`ledger.record()` may be called at most once per (date, surface).** Its `classify` → `_mark_resolved` path resolves every key of that surface the call did not report, so a second same-surface call is a resolution pass over the first call's output. New Spark-surface findings fold into the sweep's existing call.
- **A Report handed to `ledger.record()` contributes its `exec_h` to the DAG's day total**, which is the input to the savings math; a report that is not a measurement must carry `exec_h=0.0`.
- **`gsutil -m cp` wedges on `gs://mntn-data-archive-prod/spark-events` from a Mac too**, not just Astro pods: zero-byte `.gstmp` parts, no progress, no error. Use `include/spark_optimizer/fetch.py::download` (gcloud token + GCS JSON API `alt=media`).
- **`bq_run.sh` cannot take a `.sql` file whose first line is a `--` comment**: the bq CLI parses the leading `--` as a flag (`Unknown command line flag`). Use a `/* */` header.
- **`nohup python3` resolves to `/usr/bin/python3` (3.9)** on this Mac because `python3` is a shell alias; `datetime.UTC` then raises. Use `/opt/homebrew/opt/python@3.11/bin/python3.11`.
- **Regression guard mechanism:** the daily sweep runs the guard over every profiled DAG, mints findings with detector `regression_<metric>` and stage-scoped keys (e.g., `regression_disk_spill:3`), and folds them into the existing one `ledger.record()` call, so replay (new→recurring→chronic→resolved at RESOLVE_SWEEPS=3) and dedup are untouched. Regression titles end `; the run used 145 executor-hours` for the Mode card to regex-extract. Fleet noise floor: 278 gated (stage, metric) judgements over 100 real run-days = 0 regressions fired, ships ungated.
- [[project_airflow_optimizer]], [[reference_mode_api]].

## 7. Data Documentation Updates
Handed back to the dispatcher for `knowledge/` (this agent may not write the masters):
- **The Mode cost dashboard parses numbers out of ledger `title` text with a regex** (`used ([0-9,]+) slot-hours` in query `3ead7301daa8`). Any new detector that wants a number on the dashboard must put it in the title in that shape. Report `e81786de8403` holds six queries: `3ead7301daa8`, `5a66e5fad18c`, `6fdc8ae9ccf7`, `513a4a7a4a71`, `183d18f86de6`, `a167f6ad0146`.
- **`ledger.record` may be called at most once per (date, surface).** Its `classify` -> `_mark_resolved` path resolves every key of that surface the call did not report, so a second same-surface call is a resolution pass over the first call's output. New Spark-surface findings fold into the sweep's existing call.
- **A Report handed to `ledger.record` contributes its `exec_h` to the DAG's day total**, which is the input to the savings math; a report that is not a measurement must carry `exec_h=0.0`.
- **`gsutil -m cp` wedges on `gs://mntn-data-archive-prod/spark-events` from a Mac too**, not just Astro pods: zero-byte `.gstmp` parts, no progress, no error. Use `include/spark_optimizer/fetch.py::download` (gcloud token + GCS JSON API `alt=media`).
- **`bq_run.sh` cannot take a `.sql` file whose first line is a `--` comment**: the bq CLI parses the leading `--` as a flag (`Unknown command line flag`). Use a `/* */` header.
- **`nohup python3` resolves to `/usr/bin/python3` (3.9)** on this Mac because `python3` is a shell alias; `datetime.UTC` then raises. Use `/opt/homebrew/opt/python@3.11/bin/python3.11`.

## 8. Open Items / Follow-ups
- **PR not opened.** This agent may not create PRs or write to Jira. Branch `audi-1317-publish-regressions` is committed nowhere yet (no git writes were made); the worktree holds the change. Body drafted at `artifacts/audi_1317_pr_body.md`, Jira comment at `artifacts/audi_1317_result_comment.txt`. The PR base must be `audi-1281-perf-regression-guard`, not `main`, until PR #1279 lands.
- **The Mode card is a paste, not a merge.** `queries/audi_1317_mode_regressions.sql` is validated but not yet added to report `e81786de8403`. Adding a query plus a layout section over the API is a proven path (memory `reference_mode_api`, verified 2026-09-01 on this same report), so it can ship the day the code merges. It renders empty until then.
- **First prod evidence is the sweep after merge.** No DAG was triggered. Check the 09:00 UTC sweep's digest and `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` for `regression_` keys the following morning. Expect zero on a healthy day; §4.3 says that is the normal state, not a broken publisher.
- **The noise floor covers 3 jobs at 30-day depth, not 80.** Only `intent_score_map` has a real 30-day window on file today, because the sweep only starts persisting stage metrics with AUDI-1281. Thirty days after that merges the same replay can be re-run over the whole fleet from the sweep's own file, with no downloads.
- **Self-review entry not written** (`self_review/` is outside this ticket's write scope). Speed / Craft: publisher, tests, fleet noise measurement and dashboard query in one session on top of an unmerged branch.
- **One plan deviation** (§3 step 9): the fleet download uses `fetch.download`, not `gsutil`, because `gsutil -m cp` wedges on this archive. §3 and §4.3 both record it.
- `knowledge/bq_perf_log.jsonl` gained one row from this ticket's `bq_run.sh` validation call (the file's other new rows and every `_UNDOCUMENTED.queue` line belong to AUDI-1316, running concurrently); that is the wrapper's own log, not a knowledge edit.

## Verification

Adversarial pass against §0/§3/§5, worktree diff read-only, no git/Jira/Slack writes. Core claim holds; two numbers in §5/§8 do not match source.

**Confirmed correct** (diff-checked, re-derived, or reproduced from source):
- Diff is exactly the 5 files claimed, +256/-4 against merge-base `afa6f3f` (= tip of `audi-1281-perf-regression-guard`) — base branch claim is right.
- `finding_for` keys on `f"{KEY_PREFIX}{v.metric}"` (no stage suffix); `ledger.finding_key` (unmodified, pre-existing) appends the stage number it parses out of `title_for`'s `"stage {v.stage_id} ..."` — reproduced `regression_disk_spill:3` shape from source, not the summary's assertion.
- `sweep.py` computes `regressed` before the ledger call and folds it into the **one** existing `ledger.record(reports + regressed, ...)` — no second `record()` call, confirmed by reading the diff.
- `exec_h=0.0` on every `Report`; `RESOLVE_SWEEPS = 3` and `_mark_resolved` are untouched (`git diff --stat -- ledger.py` = empty).
- Reran `pytest include/spark_optimizer/tests/ -q`: **197 passed**, ruff clean, `compileall` clean — matches §5.
- Reran `artifacts/audi_1317_demo_sweep.py` against the real event-log dirs kept from AUDI-1281: output is byte-for-byte identical to `outputs/audi_1317_seeded_sweep_demo.txt` (3 keys new → quiet → quiet → resolved on 09-05, same GiB/ratio/executor-hour figures). Demo is real, not authored.
- Re-summed `outputs/audi_1317_fleet_noise_2026_09_02.txt`: 80 job rows, 120 gated pairs, 0 regressions, matching the "1961 rows, 80 jobs" header. Re-summed `outputs/audi_1317_thirty_day_replay.txt`: 130 gated pairs (18 run-days) + 28 gated pairs (2 run-days) = 158; 120+158 = **278**, 0 regressions throughout — the fleet-noise table in §4.3 reproduces exactly from its own output files.
- Mode SQL regexes (`is ([0-9.]+)x its`, `used ([0-9,]+) executor-hours`) match `title_for`'s actual output string, confirmed by reading both. `bq_perf_log.jsonl` has one `AUDI-1317` row (2026-09-03T17:21:12Z) with `records_read: 1439`, `gb_processed: 0.001`, 0 rows written — matches "1,439 rows read, 0.001 GB processed, empty result" exactly.
- `queries/`, `artifacts/`, `outputs/` writes are confined to the ticket folder; worktree writes are confined to the 5 diffed files. No writes outside the two allowed directories. `audi_1317_pr_body.md` and `audi_1317_result_comment.txt` both lint clean under their stated `--kind`.
- Query token `3ead7301daa8` = "BigQuery cost by task" cross-checks against `knowledge/memory/reference_mode_api.md` (independently recorded 2026-09-01, AUDI-1278/1194 work) — the live-pull claim is credible.

**Defects found** (both in §5/§8, neither in the core mechanism):
1. **§5 "197 passed (172 before this branch)" is wrong.** `git stash` to the merge-base and rerunning pytest gives **189 passed**, not 172 — reran twice to confirm. This branch added 8 tests (5 in `test_regression_guard.py`, 3 in `test_sweep.py`, counted directly off the diff), so 189→197 is right; 172 is not this branch's baseline. 172 looks like the test count from *before* AUDI-1281 (which itself added tests), so the figure appears to attribute AUDI-1281's own test additions to this ticket as well, overstating this ticket's net-new coverage by about 2x (8 actual vs. an implied 25).
2. **§8 "gained two rows" is wrong.** `bq_perf_log.jsonl` has exactly **one** row tagged `AUDI-1317` today, not two (grepped and confirmed). The other 4 new rows in the file's diff are tagged `AUDI-1316` — a sibling ticket's concurrent, uncommitted work in this shared worktree, not this ticket's. Low stakes on its own, but worth flagging given this exact failure mode (another session's edits riding along in a shared file) is called out twice already in global CLAUDE.md §2. Whoever lands this must stage only the `AUDI-1317` line out of `bq_perf_log.jsonl`, and should not touch `knowledge/bq/_UNDOCUMENTED.queue` at all — its 5 new lines are all `camperbid_prod__*` tables, unrelated to this ticket's query.

**On the Objective's done-when ("Merged airflow-ti change"):** not met, but the gap is fully disclosed in the agent's own open_items (no PR, no commit, no Jira — outside this agent's write scope) rather than hidden. Not treated as a defect here.

**Verdict:** downgrade to `partial`. The headline technical claim — its own ledger key per (stage, metric), single fold-in `record()` call, `exec_h=0.0`, fleet noise 0/278, ships ungated — survives every check, including two independent reproductions (pytest, demo script) and three re-summed source files. The two wrong numbers are both in self-reported validation metadata, not in the mechanism, but they are exactly the kind of unverified figure this pass exists to catch, so `jira_comment` (which cites neither number) is copied through unchanged.

