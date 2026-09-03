---
doc_type: ticket
title: "AUDI-1271: Raise initialExecutors on 2 pre-verified fetch-wait DAGs"
status: backlog
date: 2026-09-02
summary: "Raise dynamicAllocation.initialExecutors to 200 on two hourly DAGs stalled on shuffle-fetch wait"
result: "not started"
question: "Does raising spark.dynamicAllocation.initialExecutors to 200 on aug_log_ip_vertical_id_hourly and site_network_hourly remove the shuffle-fetch wait on stage 11 and stage 9 without raising DCU-hours per run?"
framing_state: locked
---

# AUDI-1271: Raise initialExecutors on 2 pre-verified fetch-wait DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1271
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does raising spark.dynamicAllocation.initialExecutors to 200 on aug_log_ip_vertical_id_hourly and site_network_hourly remove the shuffle-fetch wait on stage 11 and stage 9 without raising DCU-hours per run?
- **Goal (why / the decision):** Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** One PR (branch AUDI-1271) merged with both values and a regenerated dags/model_task_config.json; the ledger marks both shuffle_fetch_wait findings resolved and per-run DCU-h does not rise.
- **Approach (how):** Edit the two decorators, confirm maxExecutors is at or above 200 on both, regenerate the config with model_upload.py --dryrun; after merge read DCU-h per run from gcloud dataproc batches describe and compare to the 2026-08-20 baseline (site_network_hourly mean 510 DCU-h/run).
- **What would change the answer:** Fetch wait unchanged after the change, or DCU-h per run up more than the wait saved, in which case revert and record it in the ledger as fix_not_working.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Two hourly jobs spend a third to half their runtime waiting to copy data between machines. Starting them with more machines removes the wait. Values verified.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** these jobs start small and scale up. The first phase's output lands on the few starting machines, then every later machine queues to fetch from those few (shuffle-fetch wait).

**Task:** raise `spark.dynamicAllocation.initialExecutors`:
- [aug_log_ip_vertical_id_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py#L72) 100 -> 200
- [site_network_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/bidstream_hourly/site_network_hourly.py#L31) 50 -> 200

Then regenerate `dags/model_task_config.json`.

**Done-when:** PR merged; optimizer ledger shows the finding resolved (savings auto-measure).

## 3. Plan of Action
Planning wave, written 2026-09-02 (read-only verification; nothing edited in airflow-ti, nothing posted). Every path below is absolute or relative to `/Users/malachi/Developer/work/mntn/workspace`. The execute agent works in the per-ticket worktree of airflow-ti on branch `AUDI-1271` that the dispatcher creates; the dispatcher commits, runs the gauntlet, opens the PR, stamps the ledger.

### 3.0 What the plan rests on (verified 2026-09-02)

**Source of truth for each DAG's config = the `runtime_properties` dict on the `@compute.dataproc_batch` decorator in the model file.** `model_upload.py --dryrun` compiles every decorator into `dags/model_task_config.json` (`utils_model/base_model/compute.py:59`, `utils_model/model_core/context.py:258`), and the batch operator submits that JSON as `runtime_config.properties`. Confirmed end to end: today's live event logs carry exactly the decorator values.

| DAG | Model file (airflow-ti main 825b07e) | Decorator line | Today | Config line in `dags/model_task_config.json` | Live 2026-09-03 log confirms |
|---|---|---|---|---|---|
| aug_log_ip_vertical_id_hourly | `models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py` | L72 `"spark.dynamicAllocation.initialExecutors": "100"` (L71 min 50, L73 max 200, L74 executor.cores 8) | 100 | L357 (block starts L343) | initial 100 / min 50 / max 200 / cores 8 / shuffleTracking on / executorAllocationRatio 0.3 |
| site_network_hourly | `models/bidstream_hourly/site_network_hourly.py` | L31 `"spark.dynamicAllocation.initialExecutors": "50"` (L32 max 500, L33 shuffleTracking.timeout 300s) | 50 | L3416 (block starts L3401) | initial 50 / min 2 (Dataproc default) / max 500 / cores 4 |

Neither `SparkSession.builder` block sets a `dynamicAllocation` key (aug_log builder: maxPartitionBytes, openCostInBytes, parquet.block.size; site_network builder: maxPartitionBytes, shuffle.partitions 5000), so the decorator is the only surface to change. DAG files (`dags/models/feature_store_hourly.py` L37-38, schedule `15 * * * *`; `dags/models/bidstream_hourly/site_network_hourly.py` L20-38, schedule `50 * * * *`) reference the model by `model_id` and need no edit.

**CI gate that the PR must pass:** `.github/workflows/pr_model.yaml` L48-57 runs `MNTN_SDLC_ENV=dev python model_upload.py --dryrun` and fails if `git diff --quiet dags/model_task_config.json dags/ipdsc_third_party_audience_builders.json` shows drift. Prior art PR #1231 (commit ddf55a9, 2026-08-27) is the exact shape: decorator line + one regenerated config line, 2 files, 3 insertions, 3 deletions.

**Event logs exist at the grain the ticket needs, and the prior-art tools parse them.** Both DAGs are batch-fleet jobs; their logs land in `gs://mntn-data-archive-prod/spark-events/` (5,324 objects; 4,020 from August still present, so the 2026-08-04 logs the 08-27 pre-verification cites, `app-20260804231623606-0572` and `app-20260804225137739-0366`, are still readable; the 08-24..08-26 ledger-cited logs are gone). No PHS or PAM grant is needed for this ticket. Match a batch to its log by time: `gcloud dataproc batches list` createTime is 1-2 minutes before the `app-<yyyymmddHHMMSS...>` stamp. Downloaded and parsed today with `airflow_optimizer.eventlog._read_events` (workspace package, `PYTHONPATH=.`):
- `app-20260903021630713-0138.zstd` (11.6 MiB) = `Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly`, batch `aug-log-ip-ver-id-hou-uu6-20260903-011500-1`.
- `app-20260903005130592-0517.zstd` (10.3 MiB) = `Populate site_network_hourly.SiteNetworkHourly`, batch `sit-net-hou-htg-20260902-235000-1` (2,226,093,600 milliDcuSeconds = 618 DCU-h).
- `app-20260903001650651-0762.zstd`, `app-20260903025146698-0161.zstd` (40-100 KiB) = skipped-hour runs of the same two models; useless for profiling, filter these out by size (< 1 MiB).
Raw logs were deleted after parsing; the tool outputs are kept in `outputs/` (`audi_1271_concentration_2026_09_03.txt`, `audi_1271_aug_log_0138_timeline_{report.md,timeline.csv}`, `audi_1271_site_network_0517_timeline_{report.md,timeline.csv}`).

**The mechanism today, from those two logs.** `artifacts/audi_1194_shuffle_concentration.py` (in the AUDI-1194 folder) reproduces the finding: aug_log stage 11 at 36% fetch wait fed by map stage 7 whose 11.1 GiB sits on 50 executors (90% on 45); stage 35 at 29% fed by stage 33 on 74 executors. site_network stage 9 at 80% fetch wait fed by stage 5 whose 14.6 GiB sits on 185 executors with 90% on 58 and the hottest holding 25.6%. So the "few shuffle servers" mechanism is still live.

**But `audi_1194_executor_timeline.py` shows the initial executors do not survive to the map stage, in both runs:**
- aug_log: 100 executors registered by 02:16:48, cut to 50 (= minExecutors) at 02:17:48, exactly 60 s of idle later (Spark default `spark.dynamicAllocation.executorIdleTimeout` = 60s; neither decorator overrides it). First task runs at 02:18:50 (driver-side prologue: the runtime `pip install tldextract` + zip in `_ensure_tldextract_on_executors`, ~2.4 min). Map stage 7 (9,280 tasks, 1.4 min) starts 02:19:06 on 50 executors; the fleet only reaches 124 ever. Peak registered = 100. Run window 6.0 min, 42% utilization.
- site_network: 50 executors registered at 00:51:47, cut to 2 (= minExecutors) at 00:52:47, again 60 s later. Stage 0 (1 task) at 00:51:39, then nothing runs until stage 1 at 01:08:59: a 17-minute driver-side prologue with 2 executors held. Map stage 5 (7,802 tasks) launches ~01:14:30 on 2-4 executors; the fleet ramps 4 -> 324 inside its first 60 s, so the earliest-added executors take the most map output (hottest 25.6%). Peak registered 325, 18% utilization, 52.9-min window.
Consequence: `initialExecutors` is a dead knob on both jobs as written. Raising it to 200 buys 200 executors for the first 60 s, then the idle timeout trims the fleet to `minExecutors` (50 / 2) before the map stage starts, exactly as 100 and 50 are trimmed today. The 2026-08-27 pre-verification counted "executors live at stage start" with the concentration tool, which sums `SparkListenerExecutorAdded` and never subtracts removals, so it over-read the live fleet; its run-0572 natural experiment (second-pass map on ~200 executors -> 1% wait) still supports "more shuffle servers = less wait", it just does not show that `initialExecutors` gets you there. This is two runs; §3.1 step 2 measures it across the last 3 days before anyone decides.

**Prior art that already settled things:**
- `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_optimizations_2026_08_27.md` rows 4-5 and "Pre-verified (2026-08-27)": the spec, the two values, the evidence logs.
- `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_slack_ryan_site_network_hourly.md` (WITHDRAWN header, 2026-08-26) and `tickets/audi_1194_optimizer_efficiency_crawler/summary.md` L268-320: stage 9's fetch wait is a median 0.28% of site_network's executor-hours across the 25 heaviest runs (max 18.6%); 86% of the job's executor-hours are idle-reserved. Both claims stand (contradictions are appended, not overwritten): the 08-27 doc confirms the mechanism, the 08-26 memo says the mechanism is worth ~0.3% of the job's cost. The ticket's own kill criterion (DCU-h per run must not rise) is the arbiter.
- Ledger keys with history in the prod ledger (`gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`, 1,352 rows on 2026-09-02): `aug_log_ip_vertical_id_hourly` / `shuffle_fetch_wait:11` (6 rows, 2026-08-21 -> 2026-09-02, chronic, streak 7, exec_h 525.5) and `site_network_hourly` / `shuffle_fetch_wait:9` (6 rows, chronic, streak 8, exec_h 3,653.1). `ledger.mark_applied` needs this history and has it. Stamping recipe = the PR #1231 one (AUDI-1194 summary §8, 2026-08-27): download the prod ledger, `OPTIMIZER_LEDGER=<local copy> python3 -m airflow_optimizer.ledger applied <dag> <key> <pr> <YYYY-MM-DD> <note>`, upload, verify the rows land. Dispatcher only.
- Cost read: `gcloud dataproc batches list --project=mntn-prj-prod-00 --region=us-central1 --filter='labels.job_type=<label> AND state=SUCCEEDED AND create_time>"<ISO>"' --format="csv[no-heading](name.basename(),createTime,runtimeInfo.approximateUsage.milliDcuSeconds)"`; DCU-h = milliDcuSeconds / 3.6e6. `createTime` is not a filter field, `create_time` is. site_network label `job_type=site_network_hourly` (51 succeeded runs since 2026-09-01). aug_log label `job_type=aug_log_rollup` is SHARED with `aug_log_ip_hourly`: keep only batch names starting `aug-log-ip-ver-id-hou-`. Runs are hour-sized (site_network 3.5 to 618 DCU-h today), so compare 7-day windows on mean and median, never single runs.
- Prod safety (memory `feedback_airflow_prod_safety`): feature branch, `model_upload.py --dryrun` before pushing, commit the regenerated config, never push main, never trigger a prod run; the first prod execution is the next scheduled hourly firing after deploy.

### 3.1 Steps

1. **Baseline cost, before any change (execute agent, read-only, 10 min).** For each DAG pull the 7 days ending the day before the PR merges with the `gcloud dataproc batches list` command in §3.0 (use `--limit=500`), write `outputs/audi_1271_dcu_baseline_<dag>.csv` (batch, createTime, milliDcuSeconds, DCU-h) and record n, mean, median DCU-h per run in §4. Reference point: site_network mean 510 DCU-h/run on 2026-08-20 (17 runs). Drop the shared-label rows for aug_log as described above.

2. **Settle the idle-trim question across runs, before any edit (execute agent, read-only, ~30 min).** Download the last 3 days of full-size logs for both DAGs (match batch createTime to `app-<stamp>` prefix, `gsutil -o "GSUtil:check_hashes=never" cp`, skip objects < 1 MiB and any `.inprogress`, into `outputs/`, delete after parsing). Run `PYTHONPATH=. python3 tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_executor_timeline.py <log> outputs/audi_1271_<dag>_<appid>_timeline` on each and read the CSV, plus `audi_1194_shuffle_concentration.py` for the fetch-wait share. Tabulate per run into `outputs/audi_1271_idle_trim_<dag>.csv`: registered executors at +60 s, registered executors at the first map stage's submit (stage 7 for aug_log, stage 5 for site_network), prologue length (app start -> first stage with > 1 task), fetch-wait share on stage 11 / stage 9, executors holding 90% of the map output. Aim for >= 10 full-size runs per DAG. Decision rule: if the initial fleet is trimmed to minExecutors before the map stage in the majority of runs (today: 2 of 2), the spec'd change cannot work on its own; STOP and hand the table to the dispatcher for the user's call (§3.3 decision 1). If the trim is rare and the map stage normally starts on the initial fleet, continue with the spec as written.

3. **Edit the decorators in the AUDI-1271 worktree (execute agent).** Per the user's decision from step 2, either the spec alone or the spec plus an idle-timeout line:
   - `models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py` L72: `"spark.dynamicAllocation.initialExecutors": "100",` -> `"200",` (equals L73 maxExecutors 200; Spark rejects initial > max, so 200 is the ceiling). If decision 1 = option B, add after L73: `"spark.dynamicAllocation.executorIdleTimeout": "300s",` (covers the ~2.4-min tldextract prologue).
   - `models/bidstream_hourly/site_network_hourly.py` L31: `"spark.dynamicAllocation.initialExecutors": "50",` -> `"200",` (L32 maxExecutors 500 already >= 200). If decision 1 = option B, the 17-min prologue makes a 300s timeout useless here; the executor records the prologue length from step 2 and the user picks the value (§3.3 decision 2) or drops site_network from this PR.
   No other line changes. No comments added (the why goes in the PR description).

4. **Regenerate the generated config (execute agent, in the worktree).** `uv sync --group models`, then the extra deps `documentation/docs/airflow_ti_workflow.md` L236-246 lists (`uv pip install pretty_html_table matplotlib seaborn scipy scikit-learn statsmodels`), then `MNTN_SDLC_ENV=dev uv run python model_upload.py --dryrun`. Expected diff: `dags/model_task_config.json` L357 `"100"` -> `"200"` and L3416 `"50"` -> `"200"` (plus one inserted `executorIdleTimeout` line per DAG under option B, keys are sorted alphabetically so it lands between `enabled`/`executorAllocationRatio` and `initialExecutors`); `dags/ipdsc_third_party_audience_builders.json` unchanged. If the dryrun cannot complete locally for a missing exotic import, hand-edit those config lines to match and say so in the PR; CI's `model-upload-dryrun` job re-derives the file and will fail on any drift.

5. **Validate before handing to the dispatcher (execute agent).**
   - `git diff --stat` in the worktree shows exactly 2 files (3 with the builders JSON untouched): the two model files and `dags/model_task_config.json`; `git diff` shows only the lines in steps 3-4.
   - `uv run pytest tests/dags/` (DAG import validation) passes.
   - `uv run python -c "import json; c=json.load(open('dags/model_task_config.json')); print(c['aug_log_ip_vertical_id_hourly']['batch']['runtime_config']['properties']['spark.dynamicAllocation.initialExecutors'], c['site_network_hourly']['batch']['runtime_config']['properties']['spark.dynamicAllocation.initialExecutors'])"` prints `200 200`.
   - No `model_run.py` against prod, no DAG file edits, no Astro trigger.

6. **PR content (execute agent drafts `artifacts/audi_1271_pr_description.md`, dispatcher opens the PR).** Answer line, then What (the 2 decorator lines + regenerated config), Why (few shuffle servers at the first map stage: today's numbers from step 2, the 0572 natural experiment), Validation (dryrun clean, CI gate, the step-2 table, the idle-trim caveat and what the ledger will show if it does not work), Rollback (revert the PR; the ledger marks `fix_not_working` after the grace window). Lint with `python3 .claude/scripts/lint_comms.py --kind pr --file artifacts/audi_1271_pr_description.md` (900 chars, 130 words, <= 10 bullets). Dispatcher runs `/pr_gauntlet`, opens the PR from branch `AUDI-1271`, requests review from rkleck-mntn (last model-file author on aug_log, 2026-04-16) and merges per the sprint flow.

7. **Ledger stamp on merge day (dispatcher).** `gsutil cp gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl /tmp/ledger.jsonl` (scratchpad), `OPTIMIZER_LEDGER=/tmp/ledger.jsonl python3 -m airflow_optimizer.ledger applied aug_log_ip_vertical_id_hourly shuffle_fetch_wait:11 <PR#> <merge date> "initialExecutors 100 to 200"` and the same for `site_network_hourly shuffle_fetch_wait:9 <PR#> <merge date> "initialExecutors 50 to 200"`, upload, `gsutil cat ... | grep '"state": "applied"'` shows both rows. Stamp only those two keys: stage 35, the idle-reserved and straggler findings are not claimed.

8. **After merge: measure, do not trigger (execute agent, read-only, day 1 and day 7).** The next scheduled runs (aug_log :15, site_network :50 hourly) are the first prod executions. Day 1: pull 3 full-size logs per DAG from spark-events (they can vanish within days; fetch the same day) and rerun the step-2 tools; the expected signature of a working fix is >= 150 executors registered at the map stage's submit and stage 11 / stage 9 fetch wait under 10%. Day 7: rerun step 1 on the 7 days after merge and put before/after n, mean, median DCU-h per run side by side in §4; read `python3 -m airflow_optimizer.ledger shipped` on the prod ledger copy for the two keys' state (`resolved` = quiet 3 sweeps, `fix_not_working` = still firing). Kill criterion from §0: fetch wait unchanged, or DCU-h per run up by more than the wait removed -> revert PR, `ledger set <dag> <key> fix_not_working`, record in §5.

9. **Close (execute agent writes, dispatcher commits).** §4 findings, §5 the PR and ledger state, §6 the answer to the framing question, `/capture` for the durable facts in §3.4 (the idle-timeout trim, the shared aug_log label, the `create_time` filter, the concentration tool's adds-only live count), self-review entry.

### 3.2 Assumptions to resolve empirically first
- A1. The 60-s idle trim before the map stage is the norm, not a quirk of the two 2026-09-03 runs. Step 2 measures it on >= 10 runs per DAG; 2 of 2 today.
- A2. `spark.dynamicAllocation.executorIdleTimeout` is at its 60s default on both jobs (no override in decorator, builder, or the live `SparkListenerEnvironmentUpdate`; the Dataproc Serverless 2.3 image default is not documented anywhere I read, the observed trims fit 60 s exactly). Confirm from the step-2 CSVs: trim time minus register time.
- A3. Dataproc Serverless accepts `initialExecutors` = `maxExecutors` = 200 with `executor.cores` 8 for aug_log (no quota rejection). The 2026-08-04 run 0572 already held 200 executors on this job, so the quota exists.
- A4. The local `model_upload.py --dryrun` completes on this Mac after the documented extra pip installs; if not, the hand-edit fallback in step 4 applies and CI is the check.
- A5. The spark-events logs for post-merge runs are still in GCS on the day the executor looks (some August logs persist, the 08-24..08-26 ledger-cited ones are gone). Fetch same day; the daily optimizer sweep (17:00 UTC) is the backup record.
- A6. The site_network 17-min prologue is driver-side and repeatable (only stage 0's single task ran before 01:08:59 while 2 executors idled). Step 2 reads the prologue length per run; if it varies from run to run, any timeout value is a guess and option B is off the table for site_network.

### 3.3 Decisions for the user (do not guess)
1. **If step 2 confirms the idle trim (expected on today's evidence):** ship the spec as written and let the ledger record `fix_not_working` (cheap, honours §0, likely a no-op), or add `spark.dynamicAllocation.executorIdleTimeout` to the same decorator so the initial fleet survives the prologue (aug_log: `300s` covers ~2.4 min; holds up to 200 x 8-core executors idle for that window, roughly 8 executor-hours per run against a job that today registers 6 executor-hours per 6-min run), or raise `minExecutors` (holds the fleet for the whole run; contradicts the idle-reserved finding on both jobs; not recommended). Option B changes the spec and the ledger note.
2. **site_network specifically:** its prologue is ~17 min at 2 executors, so no sane idle timeout keeps 200 executors alive to stage 5 without paying 200 x 17 min = ~57 executor-hours of idle per run on a job whose median run holds 241. Either drop site_network from this PR (aug_log-only) pending a look at what the driver does for 17 minutes, or accept the spec'd no-op for the ledger record. The 08-26 memo already puts stage 9's wait at ~0.3% of the job's executor-hours.
3. **Reviewer:** rkleck-mntn authored both models; site_network is ours since PR #1232. Whether Ryan reviews or the PR merges on the hackathon flow is the dispatcher's call.

### 3.4 Risks
- R1. The spec'd change is a no-op on both DAGs if A1 holds; the PR then costs a review cycle and the ledger shows `fix_not_working` after the grace window. Bounded, reversible, but the hackathon savings line for this ticket would be zero.
- R2. DCU-h can rise: 200 executors from t0 instead of 100 / 50, held for the 60-s idle window at minimum (+ the whole prologue under option B). Both jobs already run at 18-42% utilization today (2-8% on heavy runs per the ledger). §0's kill criterion covers it; step 8 measures it.
- R3. Run-size variance: site_network 3.5 to 618 DCU-h per run within one day; aug_log 89 to 708. Single-run before/after comparisons mislead; use 7-day windows, mean and median, and the step-2 per-stage signature.
- R4. The `aug_log_rollup` label is shared by two DAGs; a cost read that forgets the batch-name filter double counts.
- R5. Spark-events logs are not retained uniformly; the "after" evidence must be captured the same day.
- R6. The concentration tool's "executors live at start" is adds-only; use the timeline CSV for live counts or the 08-27 style claim repeats.
- R7. The site_network stage 9 wait is 0.28% of executor-hours (08-26 memo): even a working fix saves little on that job; the money there is the idle fleet (AUDI-1194 QUEUE #1, PR #1232 shuffleTracking.timeout 300s already merged).

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
