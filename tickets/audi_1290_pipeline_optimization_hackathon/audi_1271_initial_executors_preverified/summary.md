---
doc_type: ticket
title: "AUDI-1271: Raise initialExecutors on 2 pre-verified fetch-wait DAGs"
status: in_progress
date: 2026-09-02
summary: "Raise dynamicAllocation.initialExecutors to 200 on two hourly DAGs stalled on shuffle-fetch wait"
result: "Diff ready on aug_log only (initialExecutors 200, config regenerated); 20-run profile says it adds ~1.7 executor-hours (~17 DCU-h, +12%) per run for 0.03-0.13 executor-hours of stage 11 wait; merge awaits the user's call"
question: "Does raising spark.dynamicAllocation.initialExecutors to 200 on aug_log_ip_vertical_id_hourly and site_network_hourly remove the shuffle-fetch wait on stage 11 and stage 9 without raising DCU-hours per run?"
framing_state: locked
---

# AUDI-1271: Raise initialExecutors on 2 pre-verified fetch-wait DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1271
**Status:** in_progress (diff ready in the AUDI-1271 worktree, PR not opened; user decides merge vs no-change close, see §5)
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

### 3.5 As executed (2026-09-02 execute wave, two agents; the second resumed after a session cut)
The plan above is kept as written; these are the deltas between it and what actually happened.
- Step 1 ran as planned (§4.1).
- Step 2 ran on aug_log only, 20 runs (§4.2). site_network_hourly was DROPPED from the PR by the user (decision 2) on the planning evidence (17-min driver prologue at 2 executors; stage 9 wait ~0.3% of the job's executor-hours), so no site_network logs beyond the planning run 0517 were profiled. Download deviated: `gsutil -o "GSUtil:check_hashes=never" -m cp -I` stalled at 0-byte `.gstmp` files twice (both agents), replaced by one GCS JSON API `alt=media` request per object (`artifacts/audi_1271_fetch_log.sh`, `xargs -P 6`), 19 objects in ~40 s. The per-run table came from a purpose-built profiler (`artifacts/audi_1271_run_profile.py`) rather than the two AUDI-1194 tools, because the timeline tool's live count and the concentration tool's fetch-wait share had to land in one CSV row per run; the AUDI-1194 tools were still run for the record (`outputs/audi_1271_concentration_20_runs.txt`, the three `_timeline_` pairs).
- Decision rule outcome: trim confirmed on 12 of 20 runs (11 to minExecutors, 1 to 11 executors), so the user's pre-answered decision 1 applied: (b) stage 11 fetch-wait executor-hours < (a) idle cost of holding 200 through the prologue in every run (§4.3), therefore the spec alone shipped, no `executorIdleTimeout` line.
- Step 3 edited one decorator line (aug_log L72), step 4 regenerated the config (one line), step 5 passed except that `tests/dags/` has 8 pre-existing Java-gateway failures on this Mac (§4.4). No `model_run.py`, no DAG edit, no trigger.
- Step 6: PR body written to `artifacts/audi_1271_pr_body.md` (lint `--kind pr` clean), reviewer Ryan Kleck (decision 3). NOT handed to the dispatcher as a plain "open it": §4.3 shows the change raises DCU-h per run by construction, which is §0's kill criterion, so the merge is returned to the user as a decision (§5, §8).
- Steps 7-9 not started (they follow a merge).

### 3.4 Risks
- R1. The spec'd change is a no-op on both DAGs if A1 holds; the PR then costs a review cycle and the ledger shows `fix_not_working` after the grace window. Bounded, reversible, but the hackathon savings line for this ticket would be zero. **Realised 2026-09-02 (§4.3): not a no-op, a net cost of ~1.7 executor-hours per run.**
- R2. DCU-h can rise: 200 executors from t0 instead of 100 / 50, held for the 60-s idle window at minimum (+ the whole prologue under option B). Both jobs already run at 18-42% utilization today (2-8% on heavy runs per the ledger). §0's kill criterion covers it; step 8 measures it.
- R3. Run-size variance: site_network 3.5 to 618 DCU-h per run within one day; aug_log 89 to 708. Single-run before/after comparisons mislead; use 7-day windows, mean and median, and the step-2 per-stage signature.
- R4. The `aug_log_rollup` label is shared by two DAGs; a cost read that forgets the batch-name filter double counts.
- R5. Spark-events logs are not retained uniformly; the "after" evidence must be captured the same day.
- R6. The concentration tool's "executors live at start" is adds-only; use the timeline CSV for live counts or the 08-27 style claim repeats.
- R7. The site_network stage 9 wait is 0.28% of executor-hours (08-26 memo): even a working fix saves little on that job; the money there is the idle fleet (AUDI-1194 QUEUE #1, PR #1232 shuffleTracking.timeout 300s already merged).

## 4. Investigation & Findings
Execute wave, 2026-09-02 (PDT evening; the event logs and batches below are stamped in UTC, so "today" spans 2026-09-02 and the first hours of 2026-09-03 UTC). User decisions applied: Decision 2 dropped site_network_hourly from the PR (§8 carries the driver-prologue question); Decision 3 named Ryan Kleck as reviewer.

### 4.1 Step 1: DCU baseline, 7 full UTC days 2026-08-26 to 2026-09-01 (before any change)
Source: `gcloud dataproc batches list --project=mntn-prj-prod-00 --region=us-central1 --filter='labels.job_type=<label> AND state=SUCCEEDED AND create_time>"2026-08-26T00:00:00Z"' --limit=500`, raw pulls in `outputs/audi_1271_batches_{aug_log,site_network}_raw.csv` (195 rows each: 8 days x 24 + 3 hours of 2026-09-03), window cut in `outputs/audi_1271_dcu_baseline_{aug_log,site_network}.csv`. aug_log rows kept only if the batch name starts `aug-log-ip-ver-id-hou-` (the `aug_log_rollup` label is shared with aug_log_ip_hourly; §3.0). DCU-h = milliDcuSeconds / 3.6e6.

| DAG | n runs | mean DCU-h | median DCU-h | min | max | 7-day sum |
|---|---|---|---|---|---|---|
| aug_log_ip_vertical_id_hourly | 168 | 162.5 | 108.6 | 38.1 | 674.4 | 27,307 |
| site_network_hourly (dropped from PR, recorded for the epic) | 168 | 552.7 | 282.1 | 99.9 | 4,130.3 | 92,846 |

Every hour ran (168 = 7 x 24) and no aug_log run in the window was under 20 DCU-h, so the "skipped hour" runs seen in the 40-100 KiB event logs still bill at least 38 DCU-h (the fleet is provisioned before the model decides there is nothing to do). Post-merge comparison (§3.1 step 8) uses this table: same 7-day shape, mean and median, never single runs.

`gcloud dataproc batches list` does not return rows in time order; sort on createTime before windowing.

### 4.2 Step 2: idle-trim profile, 20 aug_log_ip_vertical_id_hourly runs (2026-08-31 00:16 to 2026-09-03 02:16 UTC)

**Selection.** `gsutil ls -l gs://mntn-data-archive-prod/spark-events/` (`outputs/audi_1271_spark_events_listing_raw.csv`) matched to the `aug_log_rollup` batches by a 6-minute createTime window (`artifacts/audi_1271_match_logs.py` -> `outputs/audi_1271_log_manifest_aug_log.csv`, 162 rows). The shared label means every hour has two candidate logs (aug_log_ip_hourly at ~5 MiB, aug_log_ip_vertical_id_hourly at 6-15 MiB); the App Name in the first 256 KiB settles it (`artifacts/audi_1271_identify_log.sh` -> `outputs/audi_1271_log_identity_aug_log.csv`, 160 rows; the full list of 74 vertical_id logs is `outputs/audi_1271_vertical_id_logs_all.txt`). Sampled every fourth hour plus the two most recent: 20 runs (`outputs/audi_1271_vertical_id_logs_selected.txt`), all `Populate aug_log_ip_vertical_id_hourly.AugLogIpVerticalIdHourly`, 6.3-15.3 MiB each, 246 MiB total, deleted after parsing.

**Download.** `gsutil -m cp -I` allocated 0-byte `.gstmp` files and sat at 0 B/s (twice; the first agent's session ended on it). `artifacts/audi_1271_fetch_log.sh` (one `curl ... storage/v1/b/<bucket>/o/<object>?alt=media` per object with a `gcloud auth print-access-token` bearer, `xargs -P 6`) fetched 19 objects in ~40 s; `zstd -t` clean on all 20. Same failure class as memory `reference_gcloud_storage_over_gsutil`.

**Profiler.** `artifacts/audi_1271_run_profile.py` (one row per log; `airflow_optimizer.eventlog._read_events`): fleet registered at fixed offsets (live = added and not yet removed, so removals count), first removal, map stage = first stage with >= 1,000 tasks (stage 7 in all 20), reduce stage = first later stage with shuffle read and >= 100 tasks (stage 11 in all 20), prologue = app start to map-stage submit, fetch wait as `Fetch Wait Time` summed over the stage's tasks (ms) converted to core-hours and to executor-hours at 8 cores, map-output concentration from `Shuffle Bytes Written` by executor, registered and busy executor-hours. Output `outputs/audi_1271_idle_trim_aug_log.csv`. Batch DCU-h joined from the manifest.

| log (UTC start) | prologue s | reg. +60 s | reg. at map submit | first removal s | map output on N executors (90% on) | stage 11 wait % | stage 11 wait exec-h | registered exec-h | busy exec-h | batch DCU-h |
|---|---|---|---|---|---|---|---|---|---|---|
| 08-31 00:16 (0024) | 60.7 | 100 | 100 | none | 100 (83) | 32.2 | 0.066 | 10.45 | 3.18 | 124.7 |
| 08-31 04:16 (0428) | 223.0 | 100 | 50 | 71.8 | 79 (67) | 32.8 | 0.051 | 10.90 | 3.77 | 105.7 |
| 08-31 08:16 (0033) | 72.4 | 100 | 100 | 74.0 | 100 (80) | 26.5 | 0.058 | 8.65 | 3.21 | 75.5 |
| 08-31 12:17 (0293) | 63.6 | 97 | 97 | none | 100 (82) | 31.2 | 0.073 | 5.27 | 2.83 | 50.7 |
| 08-31 16:16 (0062) | 76.1 | 100 | 100 | 77.2 | 100 (82) | 33.8 | 0.073 | 9.12 | 4.85 | 87.1 |
| 08-31 20:17 (0001) | 98.6 | 100 | 50 | 72.4 | 100 (87) | 36.8 | 0.085 | 7.91 | 3.50 | 123.7 |
| 09-01 00:17 (0211) | 61.4 | 100 | 100 | none | 100 (88) | 41.1 | 0.105 | 8.34 | 4.84 | 74.7 |
| 09-01 04:16 (0829) | 68.9 | 100 | 100 | none | 100 (85) | 32.8 | 0.087 | 7.44 | 4.21 | 66.7 |
| 09-01 08:16 (0638) | 155.1 | 100 | 50 | 72.0 | 50 (42) | 31.8 | 0.029 | 7.14 | 2.21 | 82.1 |
| 09-01 12:16 (0437) | 39.4 | 100 | 11 | none | 100 (79) | 33.0 | 0.064 | 8.41 | 2.75 | 68.3 |
| 09-01 16:18 (0133) | 225.6 | 95 | 50 | 72.6 | 71 (53) | 31.6 | 0.028 | 6.82 | 2.13 | 76.6 |
| 09-01 20:16 (0599) | 449.0 | 100 | 50 | 73.2 | 77 (47) | 43.1 | 0.087 | 10.92 | 3.14 | 137.8 |
| 09-02 00:16 (0186) | 166.8 | 98 | 50 | 73.2 | 100 (87) | 42.6 | 0.102 | 9.56 | 4.33 | 96.6 |
| 09-02 04:16 (0744) | 92.4 | 100 | 50 | 73.1 | 100 (85) | 31.8 | 0.075 | 11.83 | 3.19 | 139.0 |
| 09-02 08:16 (0732) | 63.7 | 100 | 100 | none | 100 (85) | 29.0 | 0.065 | 7.15 | 3.62 | 65.1 |
| 09-02 12:16 (0053) | 925.6 | 99 | 50 | 71.7 | 65 (56) | 31.1 | 0.039 | 17.28 | 2.35 | 199.6 |
| 09-02 16:16 (0710) | 58.0 | 100 | 100 | 260.0 | 100 (85) | 34.9 | 0.089 | 16.76 | 3.64 | 191.4 |
| 09-02 20:16 (0790) | 2305.6 | 100 | 50 | 72.1 | 72 (62) | 46.8 | 0.100 | 55.26 | 1.85 | 545.5 |
| 09-03 01:16 (0707) | 1959.2 | 100 | 50 | 72.2 | 75 (65) | 53.3 | 0.126 | 29.86 | 1.73 | 360.5 |
| 09-03 02:16 (0138) | 157.6 | 100 | 50 | 71.8 | 50 (45) | 35.7 | 0.037 | 6.12 | 2.58 | 88.9 |

**What the 20 runs say (A1, A2, A3, A6 from §3.2 resolved):**
- **A2 confirmed, the idle timeout is the 60 s default.** `spark.dynamicAllocation.executorIdleTimeout` is unset in the live `SparkListenerEnvironmentUpdate` (also unset: `cachedExecutorIdleTimeout`, `schedulerBacklogTimeout`, `shuffleTracking.timeout`; set: `executorAllocationRatio` 0.3, `shuffleTracking.enabled` true, `spark.executor.instances` 50). The initial fleet of 100 registers 11-73 s after app start (95-100 live at +60 s in all 20) and the first removal lands 71.7-77.2 s after app start in 13 of the 14 runs that removed before the map stage: 60 s after the fleet came up, to the second. Removal reason on every one: `Command exited with code 0`, the idle-timeout path.
- **A1 confirmed, the trim beats the map stage in 12 of 20 runs.** Map stage 7 was submitted with 50 executors live in 11 runs (prologue 92-2,306 s), with 11 live in 1 run (0437: prologue 39 s, the fleet was still registering), and with the intact 97-100 in 8 runs (prologue 58-76 s). The boundary is the prologue length: under ~77 s the fleet survives, over it the fleet is at minExecutors when the map runs. With initialExecutors=200 the same 60 s clock applies, so the 200 would survive to the map stage in the same 8 of 20.
- **A3 confirmed, 200 executors are attainable on this job:** peak registered hit 200 in runs 0062 and 0211, 195-198 in 0186, 0428, 0732. No quota question.
- **Stage 11 waits regardless of how many executors held the map output.** Fetch wait 26.5-53.3% of stage 11 run time (median 32.9%) in all 20 runs. When the map ran on the intact fleet (8 runs, output over 100 executors, 90% on 79-88 of them) the wait averaged 32.7%; when cut to 50 (12 runs, output over 50-100 executors) 37.5%. Doubling the shuffle servers from 50 to 100 moved the wait by ~5 points, so 100 -> 200 buys a few points at most. Stage 35 (the second-pass reduce of stage 33) waits 0-46% across the same runs (`outputs/audi_1271_concentration_20_runs.txt`).
- **In executor-hours, stage 11's wait is 0.028-0.126 per run (mean 0.072, median 0.073).** The stage is 8-15 s long on 50-100 executors; even removing it entirely is worth under 0.2 executor-hours per run.
- **Utilization:** registered 5.3-55.3 executor-hours per run (mean 12.76, median 8.88), busy 1.7-4.9 (mean 3.20), so 75% of registered executor-hours are idle; the prologue alone holds 113.3 of the 255.2 registered executor-hours across the 20 runs (44%).

**Cost model, from the same 20 runs.** Batch `milliDcuSeconds` against registered executor-hours fits `DCU-h = 7.3 + 10.24 x executor-hours` (R^2 0.970, n = 20); the simple ratio runs 8.1-15.6 DCU-h per executor-hour (median 10.7). Executor shape in the environment: 8 cores, `spark.executor.memory` 19200m + `memoryOverhead` 7680m; driver 4 cores, 9600m + 3840m. Batch DCU-h in the sample: mean 138.0, median 92.7 (the 7-day baseline in §4.1 is mean 162.5, median 108.6).

### 4.3 Decision 1 arithmetic (user's rule: ship 200 plus an idle timeout only if (b) exceeds (a))
- **(a) idle cost of holding 200 executors through the prologue:** gross `200 x prologue / 3600` = 2.2-128.1 executor-hours per run (median 5.3, mean 20.3); incremental over the fleet already held today (100 for 60 s, then 50) = 1.9-95.3 (median 3.3, mean 14.7).
- **(b) executor-hours lost to fetch wait on stage 11:** 0.028-0.126 per run (mean 0.072).
- (b) is below (a) in every one of the 20 runs, by 45x at the medians on the incremental basis and 200x at the means. **Rule outcome: ship initialExecutors=200 alone, no `executorIdleTimeout`.** An idle timeout sized to the prologue would have to be 77 s to cover 8 runs, 5 min to cover 16, 39 min to cover all 20; every one of those holds 200 idle executors longer than the whole stage 11 wait is worth.
- **What the spec alone is expected to do, from the same numbers:** every run holds 100 more executors for the ~60 s until the idle trim: +1.67 executor-hours, +17.1 DCU-h per run at the fitted marginal rate, +12.4% of the sample mean run and +18.4% of its median (+10.5% / +15.7% against the §4.1 7-day baseline), about +2,870 DCU-h over a 168-run week. In the 8 short-prologue runs the extra 100 also run the map stage (9,280 tasks over 1,600 slots instead of 800), which conserves the map work and spreads the shuffle output over 200 executors instead of 100; the only expected effect on stage 11 is a few points off a wait worth 0.03-0.13 executor-hours. In the 12 long-prologue runs the extra 100 are trimmed before any task runs: pure cost. **So the change breaches §0's kill criterion (DCU-h per run must not rise) by construction, before the day-7 measurement.** This is the fact returned to the user with the diff.

### 4.4 The prologue is the cost, and it is the driver's runtime pip install (new finding, handed to §8)
`model()` (model file L119-124) calls `_ensure_tldextract_on_executors` (L21-40) before any Spark job: `subprocess.check_call([python, "-m", "pip", "install", "--quiet", "--target", tmpdir, "tldextract"])`, zip, `sc.addPyFile`. First task launch ranges 23 s to 2,286 s after app start across the 20 runs. The three longest prologues are the three heaviest batches in the sample: run 0790 (57.2-min app, first task at 2,286 s, all Spark work done in the last 2.5 min, 53.4 of 55.3 registered executor-hours idle, 545.5 DCU-h), run 0707 (34.2 min, first task at 1,939 s, 28.1 of 29.9 idle, 360.5 DCU-h), run 0053 (18.9 min, first task at 908 s, 14.9 of 17.3 idle, 199.6 DCU-h). Timeline reports: `outputs/audi_1271_aug_log_{0790,0707,0053}_timeline_report.md`. The event log cannot show what the driver waits on (no task is running); the candidates are PyPI reachability and pip retries from the serverless driver, and SparkSession creation. The batch's driver output (`gcloud storage cat` of the batch `driveroutput` URI, memory `reference_dataproc_eventlog_profiling`) settles it. Fix shape if confirmed: ship tldextract inside `utils_model.zip` or as a wheel on `ti_resources`, which removes the runtime install and the 50-executor idle floor during it. site_network_hourly's 17-min prologue (§3.0) is the same shape on a different model and is likewise open.

### 4.5 Steps 3-5: the code change and its checks
- Worktree `wt/audi_1271` (branch `audi-1271-initial-executors-preverified` off airflow-ti `825b07e`): `models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py` L72 `"100"` -> `"200"`. site_network_hourly untouched (decision 2). No comment added.
- `MNTN_SDLC_ENV=dev uv run python model_upload.py --dryrun` completed in the worktree venv (models group plus the extras from `documentation/docs/airflow_ti_workflow.md`, all already installed): `dags/model_task_config.json` L357 `"100"` -> `"200"`, nothing else; `dags/ipdsc_third_party_audience_builders.json` unchanged. `git diff --stat`: 2 files, 2 insertions, 2 deletions. Config read-back: aug_log initial 200 / max 200 / min 50, site_network initial 50.
- `uv run pytest tests/dags/`: 39 passed, 2 skipped, 2 failed, 6 errors; all 8 are `PySparkRuntimeError: [JAVA_GATEWAY_EXITED]` in `test_crm_match_rate_ds63.py`, `test_crm_match_rate_ip.py`, `test_tpa_ipdsc_export.py` (no Java runtime on this Mac: `java -version` -> "Unable to locate a Java Runtime"); none import the changed model. CI's `model-unit-test` job runs `tests/models`, not these.
- `ruff check` (0.16.1) on the model file: 7 findings (F401, I001, DTZ007 x2, BLE001 x3), byte-identical on the HEAD version of the file, none on L72.

## 5. Solution
- **Code:** one decorator line in `aug_log_ip_vertical_id_hourly.py` (initialExecutors 100 -> 200) plus the regenerated `dags/model_task_config.json`, in worktree `wt/audi_1271`, uncommitted. PR body ready at `artifacts/audi_1271_pr_body.md` (lint `--kind pr` clean), reviewer Ryan Kleck.
- **Not shipped:** site_network_hourly (user decision 2). No `executorIdleTimeout` (decision 1 rule, §4.3).
- **User decision (2026-09-03, received):** close without merging. The finding is measured and rejected: the spec raises DCU-h by ~1.7 executor-hours (~17 DCU-h, +12% of a mean run) per run while removing 0.03-0.13 executor-hours of stage 11 wait, which violates §0's kill criterion by 12-56x at the medians. The real cost driver (§4.4, §8) is the driver-side `pip install tldextract` prologue, which is a durable fix candidate but outside this ticket's scope.
- **Ledger:** not stamped (no merge).

## 6. Questions Answered
- **Q (§0):** Does raising initialExecutors to 200 on aug_log_ip_vertical_id_hourly remove the shuffle-fetch wait on stage 11 without raising DCU-h per run?
  **A:** No. Evidence on 20 profiled runs: (1) The initial fleet of 100 registers in 11-73 s, then is trimmed to minExecutors (50) at +60 s (executorIdleTimeout default) in 12 of 20 runs before the map stage. (2) With initialExecutors=200 the same 60s trim applies, costing +1.67 executor-hours per run. (3) Stage 11 waits only 0.03-0.13 executor-hours per run. (4) The change fails §0's kill criterion by construction, before any merge. Closed without action on recommendation to pursue the driver prologue (§4.4) instead.
- **Q:** Where does aug_log's executor cost go? **A:** 75% of registered executor-hours are idle; the driver prologue (runtime `pip install tldextract`) holds 44% of them, and in the three heaviest runs it is 90%+ of the run. At 58 DCU-hours per run average (IMP-106), it is the largest lever.
- **Q:** Is `executorIdleTimeout` at the 60 s default on aug_log? **A:** Yes, unset in the live environment; removals land 71.7-77.2 s after app start, exactly 60 s after the fleet registers.

## 7. Data Documentation Updates
Findings captured in `/capture` wave (2026-09-03): the 60 s idle trim on task-free prologues (12 of 20 aug_log runs, 0 of 21 AUDI-1272 runs where executors are busy from the first task), the DCU-h per executor-hour fit (10.24 marginal, 8.1-15.6 ratio for 8-core Serverless executors), the shared `aug_log_rollup` label requiring a batch-name filter in cost queries, the JSON API fetch as the gsutil -m workaround for GCS stalls, the AUDI-1194 concentration tool's adds-only live count defect, and the runtime pip-install prologue pattern on both aug_log and site_network_hourly (IMP-106 logged).

## 8. Open Items / Follow-ups
1. ✓ **User decision (2026-09-03):** close without merging.
2. **aug_log driver prologue (the real lever, IMP-106):** `pip install tldextract` at runtime idles 50-100 executors for 23s to 38min per run (§4.4), costing ~58 DCU-h per run average. Bundle tldextract in `utils_model.zip` or as a wheel on `ti_resources` to remove the cost. Also check site_network_hourly's 17-min prologue (what does the driver do?).
3. **Stage 11 mechanism open:** the wait persists at ~33% even with the map output spread over 100 executors; the mechanism is not fully settled (cold-first-read from AUDI-1194 may explain part of it). Not worth pursuing for 0.03-0.13 executor-hours per run.
