---
doc_type: ticket
title: "AUDI-1281: Perf-regression guard POC from optimizer metrics"
status: backlog
date: 2026-09-02
summary: "CI check that fails when a model's spill or fetch-wait doubles vs its 30-day optimizer baseline"
result: "not started"
question: "Can a CI check compare a model's latest spill and shuffle-fetch-wait against its own 30-day baseline from optimizer metrics and fail on a 2x regression?"
framing_state: locked
---

# AUDI-1281: Perf-regression guard POC from optimizer metrics

**Jira:** https://mntn.atlassian.net/browse/AUDI-1281
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Can a CI check compare a model's latest spill and shuffle-fetch-wait against its own 30-day baseline from optimizer metrics and fail on a 2x regression?
- **Goal (why / the decision):** POC for Bryce's pipeline-testing-framework track. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A POC script plus test in airflow-ti (branch AUDI-1281) that reads the optimizer ledger or GCS outputs, computes a 30-day per-stage baseline for one critical pipeline, and flags a seeded 2x regression; run demonstrated against that pipeline.
- **Approach (how):** Metric source is include/spark_optimizer ledger rows and per-sweep outputs in gs://mntn-data-archive-prod/optimizer/; baseline = 30-day median per (dag, stage, metric); pipeline chosen from chronic ledger findings (intent_score_map or fangorn_score_monitor unless the user says otherwise); seeded regression = synthetic row at 2x.
- **What would change the answer:** Run-to-run noise above 50% CV on the chosen metric, in which case a fixed 2x threshold is wrong and the POC reports the adaptive threshold it needs instead.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Catch pipeline slowdowns in CI before they reach prod, using measurements the optimizer already collects. POC for the testing-framework track.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the [optimizer](https://github.com/SteelHouse/airflow-ti/blob/main/include/spark_optimizer/) records disk spill and network wait per stage for every run. A job that doubles against its own 30-day baseline is a real regression nobody sees today.

**Task:** build a check that fails CI when a model's spill or fetch-wait doubles vs its 30-day baseline; run it against one critical pipeline.

**Done-when:** POC runs against one critical pipeline and flags a seeded regression.

## 3. Plan of Action
Planning wave, written 2026-09-02. Nothing below has been executed; every "verified" fact was checked read-only today (commands and numbers in §3.6). The execute agent works in the dispatcher's worktree of `SteelHouse/airflow-ti` on branch `AUDI-1281`; the dispatcher commits, runs the gauntlet, opens the PR. Repo owner for every file touched: Malachi (the `include/spark_optimizer/` package and `dags/spark_optimizer_daily.py` are AUDI-1194 deliverables, PMO rep Bryce Wagg). No hand-off is needed; no file outside that package and that DAG is edited.

### 3.1 Design decisions the plan rests on
- **Pipeline: `intent_score_map`** (task `intent_score_map` in `dags/audience_intent/audience_intent.py`, daily `8 0 * * *` UTC, model `models/audience_intent/intent_score_map.py`, `spark.sql.shuffle.partitions=4915`). Chosen over `fangorn_score_monitor` because (a) it writes rolling `eventlog_v2_batch-<uuid>/` dirs to `gs://mntn-data-archive-prod/spark-events/`, and there are exactly 31 such dirs in the whole archive, one per day 2026-08-05..2026-09-02 (two parsed today both carry `app_name = Populate intent_score_map.IntentScoreMap`), so the 30-day history is enumerable with one `gsutil ls`, ~21 MiB per run (~650 MB total); (b) it spills to disk chronically on stages 2, 3 and 6 with stable stage ids across runs (ledger keys `disk_spill:2/3/6` on all 9 ledger runs; two runs parsed today agree: stage 2 ~1315 GiB, stage 3 4109/4262 GiB, stage 6 3970/3988 GiB); (c) fangorn's flat `app-*.zstd` logs cannot be told apart from ~150 other logs a day without parsing them all. Caveat: `fetch_wait_ms` is 0 on every intent_score_map stage, so the fetch-wait half of the guard is exercised only by the seeded fixture (see §3.5 for the live fetch-wait option).
- **Metric source is the event log, not the ledger.** The ledger (`optimization_ledger.jsonl`, 1,352 rows, 10 sweep dates 2026-08-21..09-02) carries spill and fetch-wait only inside `title` text, only when a detector fired (`disk_spill >= 2 GiB or mem_spill >= 32 GiB`; `fetch_wait_ms/run_time_ms >= 0.3 and run_time_ms >= 300s`), and its `exec_h` is the DAG's per-sweep-day sum across all logs seen (09-02 row 283.7 = 138.3 + 145.4, the two runs that sweep read). It is censored and not per-run, so a baseline built from it is wrong by construction. `eventlog.parse_eventlog` already yields the uncensored per-stage grain: `StageMetrics.disk_spill`, `mem_spill`, `fetch_wait_ms`, `run_time_ms`, `num_tasks`, `shuffle_read_bytes`, `shuffle_write_bytes` (from `SparkListenerTaskEnd` task metrics); `crawl.executor_hours(run)` gives per-run executor-hours.
- **Two inputs for one guard.** `--from-logs <dir>` parses event logs directly (the demo and the backfill path; no prod change needed). `--metrics <jsonl>` reads a per-stage metrics file the daily sweep starts persisting (so history accumulates without re-downloading logs). The POC's done-when is met by the first path; the second is what makes the guard usable after the hackathon.
- **Baseline and rule.** Per (dag_id, stage_id, metric), window = runs whose date is within 30 days before the run under test, deduplicated by `app_id` (the same rolling dir was read by two consecutive sweeps twice in the ledger: `06dfd454` on 08-26 and 08-27, `66cd15cd` on 08-30 and 08-31). Baseline = median. Regression = latest >= 2.0 x median AND latest above an absolute floor (disk_spill >= 2 GiB, fetch_wait_ratio >= 0.05) AND window has >= 5 runs. Metrics gated: `disk_spill` (bytes) and `fetch_wait_ratio` (= fetch_wait_ms / run_time_ms); `mem_spill`, `run_time_ms` and `exec_h` are printed for context, never gated. Framing kill criterion: if the window's coefficient of variation (stdev / mean) on a gated metric is > 0.5, the fixed 2x is wrong; the guard then prints the CV and gates on `median + 3 x MAD` instead, labelling the line "adaptive threshold". Ledger-title CVs today (censored, indicative only): stage 2 disk spill 0.47, stage 3 0.22, stage 6 0.05; the backfill computes the real ones on 29 runs.
- **Where "fails CI" lives.** `.github/workflows/pr_spark_optimizer.yaml` already runs `ruff` + `pytest include/spark_optimizer/tests/` on every PR touching `include/spark_optimizer/**` or `dags/spark_optimizer_daily.py`, with no GCP credentials. So the CI-time gate is the hermetic test (seeded 2x fixture must fail the guard, 1.5x must pass); the live gate against real runs executes where the data is, the daily sweep (`spark_optimizer_daily`, 09:00 UTC). Wiring a credentialed PR check is a post-POC decision (§3.5).

### 3.2 Steps (execute in order)
1. **Environment.** In the worktree: `python3 -m pip install zstandard` (the Mac's python3 lacks it; the parser falls back to `/opt/homebrew/bin/zstd` v1.5.7, which works but is slower). Confirm the baseline suite is green before touching anything: `ruff check --config include/spark_optimizer/ruff.toml include/spark_optimizer/` and `python -m pytest include/spark_optimizer/tests/ -q` (164 tests collected today). Ruff rules to write against: `D100-D104` one-line docstrings on every module/class/function, `ANN` annotations, line length 100, no block comments (workspace rule §9b).
2. **`include/spark_optimizer/stage_metrics.py` (new).** `rows_for(run, source, dag_id, date) -> list[dict]`: one dict per `run.stages` entry with keys `date, dag_id, app_id (= source basename), stage_id, num_tasks, run_time_ms, disk_spill, mem_spill, fetch_wait_ms, shuffle_read_bytes, shuffle_write_bytes, input_bytes, exec_h` (exec_h = `crawl.executor_hours(run)`, repeated per row). `append(rows, path)` and `read(path)` as JSONL, tolerant of a torn last line exactly like `ledger.read`. File name constant `STAGE_METRICS = "optimizer_stage_metrics.jsonl"`. Test: `tests/test_stage_metrics.py` builds a `SparkRun` with two `StageMetrics` (pattern in `tests/test_eventlog.py::test_infra_and_failure_recommendation_types`) and asserts the row values and the round-trip.
3. **`include/spark_optimizer/crawl.py`.** `JobReport` gains `stages: list = field(default_factory=list)`; in `crawl()` populate it from `run.stages` on the success path (the `JobReport(source=base, findings=..., app_name=..., exec_h=...)` line). Nothing else in crawl changes. Test: extend `tests/test_pipeline.py` with a log whose `SparkListenerTaskEnd` carries `Task Metrics` (`Disk Bytes Spilled`, `Shuffle Read Metrics.Fetch Wait Time`, `Executor Run Time`) and assert `reports[0].stages[0].disk_spill` matches.
4. **`include/spark_optimizer/sweep.py`.** In `run()`, right after `scored` is computed: for each scored report, `dag = ledger_mod._dag_id(r, known)` (same resolver the ledger uses, so the metrics file and the ledger agree on names; `known` is available only after the coverage block, so place the write after `known` is set and before `ledger_mod.record`), `stage_metrics.append(rows_for(...), path)`. Path = `os.path.join(outdir, STAGE_METRICS)`, add it to the first `publish([...])` list next to `ledger_path`, add `"stage_metrics": path` to the returned dict. Test: in `tests/test_sweep.py` give `_report()` a `stages=[StageMetrics(...)]` and assert `out["stage_metrics"]` exists with one row per stage and that the publish call includes it (the existing upload monkeypatch pattern `_Upload`).
5. **`dags/spark_optimizer_daily.py`.** Mirror the ledger restore: after `had_ledger = ...`, `fetch.fetch_optional(f"{REPORT_PREFIX}/optimizer_stage_metrics.jsonl", outdir)` guarded by `if gcs_prefix`, absent object = start fresh (same semantics as the ledger comment block explains). This is the only DAG line. Validation: `python -m compileall -q dags/spark_optimizer_daily.py include/spark_optimizer/` (the CI step) plus the sweep test in step 4. Prod safety: no manual trigger; the first prod execution is the next scheduled 09:00 UTC sweep after merge, checked afterwards by `gsutil ls gs://mntn-data-archive-prod/optimizer/optimizer_stage_metrics.jsonl`.
6. **`include/spark_optimizer/regression_guard.py` (new).** CLI `python -m include.spark_optimizer.regression_guard --dag intent_score_map (--from-logs <dir> | --metrics <jsonl>) [--window-days 30] [--factor 2.0] [--min-runs 5] [--seed stage=3,metric=disk_spill,factor=2.0]`. Functions: `baseline(rows, dag, as_of, window_days) -> dict[(stage_id, metric)] -> {median, mad, cv, n}`; `check(latest_rows, baseline, factor) -> list[Verdict]` with fields `stage_id, metric, latest, median, ratio, threshold, adaptive (bool), n`; `render(verdicts)` prints a table ranked by ratio descending, one line per gated metric with the CV, then context metrics. `--seed` appends one synthetic latest row = median x factor for the named stage/metric (the "seeded regression" in the done-when). Exit code 1 when any verdict is a regression, 0 otherwise, 2 when the window has fewer than `--min-runs` runs (no baseline is not a failure). `--from-logs` calls `parse_eventlog` on every log `crawl._event_logs` finds under the dir, resolves the DAG with `ledger._dag_id`, keeps only `--dag`, and takes the run date from `app_start_ts`.
7. **`include/spark_optimizer/tests/test_regression_guard.py`.** Synthetic metrics rows (10 runs, stage 3 disk_spill around 3,400 GiB with ~10% jitter, fetch_wait_ratio 0.0): (a) seeded 2.0x on stage 3 disk_spill -> one regression, exit 1; (b) 1.5x -> exit 0; (c) a seeded fetch_wait_ratio 0.02 -> 0.4 on a stage whose baseline is 0.0 -> flagged only via the absolute floor rule, so assert the floor works in both directions; (d) window with CV 0.8 -> `adaptive` is True and the printed line says "adaptive threshold"; (e) 4 runs -> exit 2 and no verdicts; (f) duplicate `app_id` rows count once; (g) a run older than 30 days is excluded. Plus one end-to-end test through `--from-logs` on a tmp dir with two JSON event logs (pattern `tests/test_pipeline.py::_write_log` extended with `SparkListenerTaskEnd` metrics), asserting the CLI's exit code via `main()`.
8. **Backfill and demo (local, outputs in this ticket).** `gsutil ls -d gs://mntn-data-archive-prod/spark-events/eventlog_v2_batch-*/` (31 dirs today), then in batches of 8: `gsutil -o "GSUtil:check_hashes=never" cp -r <dir> outputs/backfill/`, run the guard with `--from-logs outputs/backfill --dag intent_score_map --metrics-out outputs/audi_1281_stage_metrics_intent_score_map.jsonl` (add `--metrics-out` to step 6 so the parsed rows are persisted once), delete the downloaded dirs (200 MB cap). Then run the real check as of 2026-09-02 and save stdout to `outputs/audi_1281_guard_real_2026_09_02.txt`; run it again with `--seed stage=3,metric=disk_spill,factor=2.0` and save to `outputs/audi_1281_guard_seeded.txt` (must exit 1). Record in §4: the per-stage CV table for stages 2/3/6, whether any real regression fired (stage 3 was 4,262 GiB on 09-02 vs 4,109 on 09-01; the 30-day median is unknown until the backfill), and the exec_h series (per-run today 138-145 h).
9. **Validation before the PR (all must pass in the worktree):** `ruff check --config include/spark_optimizer/ruff.toml include/spark_optimizer/`; `python -m pytest include/spark_optimizer/tests/ -q`; `python -m compileall -q dags/spark_optimizer_daily.py include/spark_optimizer/`; a hermetic local sweep on the two most recent rolling dirs, `python -m include.spark_optimizer.sweep outputs/backfill/eventlog_v2_batch-42e88a22-6f13-4282-9910-34d2e097ea4e outputs/backfill/eventlog_v2_batch-8f1a450a-2ebc-44de-a375-ef5408d27b2f --date 2026-09-02 --outdir /tmp/audi_1281_sweep --ledger /tmp/audi_1281_sweep/l.jsonl` (no `--gcs-prefix`, no `--airflow-base`, no Slack token in the env -> nothing published, nothing posted) and confirm `/tmp/audi_1281_sweep/optimizer_stage_metrics.jsonl` holds 10 rows (5 stages x 2 runs) with `dag_id = intent_score_map`. `python3 .claude/scripts/lint_comments.py --staged` for the comment rule.
10. **PR (dispatcher).** Title `AUDI-1281: perf-regression guard POC on optimizer stage metrics`. Body per the PR cap: answer line, What (steps 2-6), Why (censored ledger, per-run grain), Validation (step 9 commands and the demo exit codes). Release Type omitted (library + one optional fetch in our own DAG; no model code ships). After merge: no manual trigger; check the 09:00 UTC sweep's `optimizer_stage_metrics.jsonl` lands in `gs://mntn-data-archive-prod/optimizer/` the next day.
11. **Close-out in this ticket:** §4 findings (CV table, real verdicts), §5 files changed, §6 the framing question answered yes/no with the measured CVs, `/capture` for the two durable facts (ledger `exec_h` is a per-sweep-day sum; the ledger is censored at the detector gates), self-review entry.

### 3.3 Sources (all verified read-only 2026-09-02)
- Spec: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` item 29; `audi_1194_hackathon_optimizations_2026_08_27.md` row 7 (intent_score_map disk spill mechanism and the AUDI-1269 fix: shuffle.partitions 4915 -> 40960).
- Code (airflow-ti main at `825b07e`, read-only checkout `/Users/malachi/Developer/work/mntn/airflow-ti-main`): `include/spark_optimizer/eventlog.py` (`StageMetrics`, `_task_end`), `optimizations.py` lines 266-311 (detector gates), `crawl.py` (`JobReport`, `executor_hours`, `_event_logs`), `sweep.py::run` (publish list, ledger gating), `ledger.py` (`Entry`, `read`, `record`, `_dag_id`, `_dedup`), `coverage.py::resolve`, `fetch.py::fetch_optional`, `dags/spark_optimizer_daily.py`, `.github/workflows/pr_spark_optimizer.yaml`, `tests/` (164 tests; fixtures `eventlog.zstd`, `gen_eventlog.py`).
- Data: `gs://mntn-data-archive-prod/spark-events/` (5,324 objects, flat logs ~150/day from 2026-08-04, 31 `eventlog_v2_batch-*` dirs 08-05..09-02, no lifecycle rule); `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` (copy in `outputs/`, 1,352 rows); BQ external table `mntn-prj-prod-00:optimizer.optimization_ledger` (16 columns, same censoring, not used by the guard); `outputs/optimizer_backlog_2026-09-02.md`, `outputs/optimizer_coverage_2026-09-02.md`.
- Memory: `knowledge/memory/project_airflow_optimizer.md` (ledger design, `exec_h` per-DAG-day semantics, the "archive holds 23 days, not 30" note from 08-26, the open "flagged apps' flat logs vanish within hours" question), `feedback_airflow_prod_safety.md`.

### 3.4 Assumptions to resolve empirically first (in step order)
1. All 31 rolling dirs are intent_score_map runs (2 of 31 parsed today; 08-11 and 08-19 each have two dirs, one marked `.inprogress`). Step 8 filters by resolved `dag_id`, so a stray dir is dropped, not miscounted.
2. Stage ids stay 2/3/6 across the whole 29-day window (true on the 9 ledger runs and the 2 parsed runs; a code change in the model would renumber them, and the guard reports an unmatched stage as "no baseline", never as a regression).
3. The real 30-day CVs on stages 2/3/6 disk spill are <= 0.5 (ledger-title estimates 0.47 / 0.22 / 0.05). If stage 2 lands above 0.5 the guard's adaptive branch is the reported answer for that stage, per the framing.
4. `zstandard` installs cleanly in the worktree's Python (3.11.12); otherwise the `zstd` CLI fallback is used and step 8 takes longer.
5. Dataproc batch metadata is NOT part of this plan: `gcloud dataproc batches list --filter=labels.task_id=...` returned zero rows for both candidate tasks over 6,000 batches and `batches describe <rolling-dir uuid>` is NOT_FOUND (the dir suffix is the batch uuid, not the batch id). Enumeration is by the rolling-dir listing only.

### 3.5 Risks and decisions
- **AUDI-1269 changes this pipeline's shuffle.partitions in the same sprint.** Spill will drop sharply after that merge, which the guard reads as an improvement (ratio < 1, exit 0), but the baseline window then straddles two regimes for 30 days. Mitigation for the POC: report only. Follow-up: reset the window at the ledger's `applied_date` for that DAG (the stamp already exists in `ledger.py::mark_applied`).
- **Flat-log pipelines.** Memory records flagged `app-*.zstd` objects 404ing within hours while neighbours persist; intent_score_map's rolling dirs are all present back to 08-05, so this POC is unaffected, but a flat-log pipeline's backfill must run from the sweep-persisted metrics (step 4), not from GCS after the fact.
- **Fetch-wait has no live signal on intent_score_map** (0% on every stage). If a live fetch-wait demonstration is wanted, `site_network_hourly` stage 9 (52% median, 79% max fetch-wait over 53 runs in the 08-27 corpus, hourly, flat logs) is the candidate: one day of logs (~150 downloads, ~1.2 GB in batches) gives ~24 runs. Decision for the user, not required by the done-when.
- **Where the live gate runs after the POC**: (a) the daily sweep adds a "Regression: <dag> stage N <metric> 2.3x its 30-day median" note to the digest (ours, no new credentials), or (b) a credentialed PR check via the existing GitHub workload-identity provider (`deploy_gcs.yaml` uses `projects/411678625229/locations/global/workloadIdentityPools/github-actions/providers/github`) that reads the metrics file on PRs touching `models/<pipeline>.py` (needs devops to allow the spark-optimizer SA for PR-time OIDC). User decision; the POC ships neither.
- Download budget: ~650 MB total for the backfill, handled in batches under the 200 MB rule; the sweep's own daily cost is unchanged (the metrics file adds ~5 rows per log).

### 3.6 Verification log (commands run 2026-09-02, read-only)
- `curl .../rest/api/2/issue/AUDI-1281`: Task, Backlog, parent AUDI-1290, labels hackathon + q3_2026, no story points set, no comments.
- Ledger profile (`outputs/optimization_ledger.jsonl`): 1,352 rows; detectors shuffle_fetch_wait 574, disk_spill 323, straggler 189, shuffle_partition_sizing 110, skew 59, idle_reserved_executors 44, bq_heavy_task 18, pod_cpu_overprovisioned 17, gc_pressure 13; intent_score_map 45 rows / 9 distinct app_ids / keys disk_spill:2,3,6 + shuffle_partition_sizing:2,3; fangorn_score_monitor 45 rows / 7 app_ids.
- `gsutil ls -l .../eventlog_v2_batch-*/appstatus_*`: 31 dirs, 2026-08-05T06:56Z .. 2026-09-02T08:20Z, one per day (08-11 and 08-19 have two).
- Parsed `eventlog_v2_batch-42e88a22` (09-02) and `eventlog_v2_batch-8f1a450a` (09-01) with `parse_eventlog`: 5 stages, 43-45 min, exec_h 145.4 / 138.3, shuffle.partitions 4915; stage 2 (14,000 tasks) disk 1,319.9 / 1,314.1 GiB; stage 3 (30,000 tasks) 4,262.0 / 4,108.9 GiB; stage 6 (4,915 tasks) 3,988.2 / 3,969.6 GiB; fetch_wait 0.0% on all. Downloads deleted after parsing.
- `bq show mntn-prj-prod-00:optimizer.optimization_ledger`: EXTERNAL over the ledger JSONL, columns date, dag_id, app_id, key, impact, title, owner, dcu_h, exec_h, fix, state, streak, note, fix_pr, applied_date, surface.
- Local tooling: Python 3.11.12, pytest 8.3.5, ruff 0.16.1 (CI pins `>=0.16,<0.17`), `zstandard` module absent, `zstd` CLI 1.5.7 present; `pytest --co` collects 164 tests.

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
