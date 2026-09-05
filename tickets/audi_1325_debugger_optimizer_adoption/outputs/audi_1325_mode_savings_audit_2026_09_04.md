# Spark Optimizer Savings dashboard audit (Mode report `e81786de8403`) — 2026-09-04

**Accurate: no. Up to date: partly.**

The dashboard is not accurate: it publishes 3,455.2 executor-hours / $960.55 saved (and $173,289/yr) when the correct answer on the same ledger is zero measured savings, because Mode recomputes savings in its own SQL (query 5a66e5fad18c "Savings headline", duplicated in 513a4a7a4a71 "Savings by surface") that the PR #1286 Python fix never touched.

## The numbers

SHOWS NOW (report run 80e77428916a, 2026-09-04T23:10:58Z): 3,455.2 executor-hours saved all-time · $960.55 saved all-time · 1,707.8 executor-hours/day run rate · $173,289 estimated annual savings · 15 DAGs fixed. "Savings by surface" republishes the same 3,455.2 / 1,707.8 / 15 lower on the page.

ON A REFRESH RIGHT NOW: 5,163.0 executor-hours / $1,435.31, verified by re-running the identical SQL through bq_run.sh at 2026-09-05T01:5xZ against the unchanged ledger. The all-time figure rises +1,707.8 hours (+$474.76) every calendar day with zero new data, because the multiplier is DATE_DIFF(CURRENT_DATE(), applied_date, DAY).

SHOULD SHOW: no measured savings. Running the shipped post-#1286 savings() against the same live ledger (930,490 bytes, written 2026-09-04T09:19:23Z, 1,692 rows) returns measured_jobs 0, total_exec_h_saved 0.0, CI [0.0, 0.0], evidence "No measured savings to report: 1 cleared job (1 of 3 sweep-days before the fix, 2 of 3 after)".

INTERMEDIATE CHECK: correcting only the aggregation (MAX per dag-day instead of SUM, exclude state='applied' rows, exclusive after-window, observed after-days instead of calendar days, no zero floor) drops the all-time figure from 5,163.0 to 114.8 executor-hours (~$32) across exactly 1 scoreable job, and zero of the 15 DAGs have 3 observations on each side of their fix. So the published number is 30-45x too high on aggregation alone, and the residue fails the evidence bar entirely.

SIGN CHECK: with the GREATEST(...,0) floor removed, the same sum is -125,734.2 executor-hours. The headline's sign is inverted relative to the net measured change across the DAGs it scores.

## Defects

### 1. Mode recomputes savings in its own SQL; the PR #1286 Python fix cannot reach it. Queries 5a66e5fad18c ("Savings headline") and 513a4a7a4a71 ("Savings by surface") implement a third, independent savings method over the raw ledger columns. savings() in include/spark_optimizer/ledger.py only writes gs://mntn-data-archive-prod/optimizer/optimizer_savings.md and the Slack digest note.

**Why it matters.** Deploying or waiting on #1286 changes nothing on the dashboard. The Python digest will say "No measured savings to report" while the dashboard keeps publishing $960.55-$1,435.31 to leadership. Two MNTN surfaces will disagree by 30-45x with no reconciliation anywhere.

**Fix.** Edit the two Mode queries. Nothing else fixes this number. Treat 5a66e5fad18c as the source and make 513a4a7a4a71 reuse the identical corrected CTE block (its daily/applied/rates block is byte-identical today, so the defect is rendered twice on one page).

**Evidence.** GET /api/mntn/reports/e81786de8403/queries returns 7 queries; 'savings' appears 0 times across all 7 raw_query strings, exec_h 12 times, applied_date 6. bq show mntn-prj-prod-00:optimizer.optimization_ledger returns 16 columns with no savings/dollar/delta field. PR #1286 touched only digest.py/ledger.py/sweep.py plus tests.

### 2. The daily CTE does SUM(exec_h) GROUP BY dag_id, surface, DATE(date), but exec_h is a DAG-level daily total stamped identically on every finding row for that DAG that day. SUM multiplies true executor-hours by the finding count.

**Why it matters.** This is the single largest error. It inflates both the before-rate and after-rate by an arbitrary integer that changes day to day as finding counts move, so the before/after delta partly measures the number of findings rather than compute.

**Fix.** Replace SUM(exec_h) with MAX(exec_h) in the daily CTE (or SUM over distinct app_id if you want multi-app days summed correctly), and add AND state != 'applied' to the WHERE clause so the 54 cloned applied rows carrying 42,172.9 hours stop entering the averages.

**Evidence.** ledger.py:280 docstring: "Every entry's exec_h is its dag's total for the sweep-day." BQ: 2026-09-03 hh_vertical_mid 13 rows, 1 distinct exec_h 106.6, SUM 1,385.8. 2026-09-02 site_network_hourly 27 rows, MAX 3,653.1, SUM 91,590.6. fangorn's "before" of 6,189.3 is 9 x 687.7. Swapping SUM for MAX takes the headline from 5,163.0 to 174.2.

### 3. The all-time figure is multiplied by DATE_DIFF(CURRENT_DATE(), a.ad, DAY) - elapsed calendar days since the fix, not days observed.

**Why it matters.** The headline is a clock, not a measurement. It grows +1,707.8 executor-hours / +$474.76 every day forever, with no new sweeps, no new fixes, and no new data. Anyone screenshotting it on two different days gets two different "all-time" totals.

**Fix.** Replace `days` with the observed after-day count: COUNTIF(d.d > a.ad) computed in the rates CTE, and use that as the multiplier in all four aggregates.

**Evidence.** Same SQL, same unchanged ledger: 1,747.4 h as of 2026-09-03, 3,455.2 as of 09-04 (the published run), 5,163.0 as of 09-05 (my run today), 56,396.5 as of 10-05. Exactly linear at +1,707.8/day.

### 4. GREATEST(before_rate - after_rate, 0) floors every regression at zero inside all four headline aggregates.

**Why it matters.** The headline is a one-sided selected sum that can only go up. A DAG that got 4x worse after its "fix" contributes 0 instead of a negative, so the dashboard structurally cannot show that the optimizer program is net-negative even when it is.

**Fix.** Delete GREATEST and sum the signed delta (before_rate - after_rate) * days. If a floor is wanted for a per-row display, apply it in the table only, never in the total.

**Evidence.** 6 of the 15 scored DAGs are regressions. site_network_hourly: before_rate 23,062.2, after_rate 88,476.1, contributes 0.0 instead of -130,827.9. Floored sum today +5,163.0; unfloored -125,734.2. The published number has the wrong sign.

### 5. The after-window is inclusive of the applied date: AVG(IF(d.d >= a.ad, d.exec_h, NULL)) after_rate.

**Why it matters.** The fix day itself usually still contains pre-fix hours, and on this ledger it also contains the cloned state='applied' rows written by mark_applied. Counting it as "after" drags the after-rate toward the before-rate on some DAGs and inflates it on others; either way it is not a post-fix observation.

**Fix.** Change to AVG(IF(d.d > a.ad, d.exec_h, NULL)), matching the Python (ledger.py:661-662 use strict < and strict >).

**Evidence.** Live SQL text pulled from the Mode API today shows `IF(d.d >= a.ad, ...)`. Python post-#1286 uses `d > r["applied_date"]`. fangorn's 2026-08-27 reading of 954.3 sits on the applied date.

### 6. No evidence gate of any kind: no outcome='resolved' requirement, no minimum observations, no confidence interval.

**Why it matters.** 14 of the 15 DAGs in the headline were marked applied on 2026-09-03 and have ZERO observed after-days. They are still 'watching' - nobody has confirmed the finding stopped firing - yet they contribute the bulk of the number. This is exactly the unsoundness AUDI-1326 was opened to kill, reproduced in a surface AUDI-1326 never looked at (its 166-line summary.md contains zero occurrences of 'mode').

**Fix.** Port the shipped Python gate into the SQL: require the DAG's finding to have reached state='resolved', require COUNTIF(d.d < a.ad) >= 3 AND COUNTIF(d.d > a.ad) >= 3, and require a 90% Welch interval clear of zero (or, if a Welch interval in SQL is too much, gate on the observation counts and render every ungated DAG in a separate "not yet measurable" table with its reason). Applying the count gate alone today yields 0 qualifying jobs.

**Evidence.** Ledger: 60 rows carry applied_date 2026-09-03 across 14 dag/surface pairs, all with zero after-days; only 2 of 64 shipped findings have ever reached outcome 'resolved' (both fangorn_score_monitor, PR #1231). My corrected-aggregation query returns jobs_with_3_each_side = 0, scored = 1.

### 7. 58% of the published all-time figure (1,997.1 of 3,455.2 hours) comes from one DAG, fangorn_score_monitor, whose true per-finding rate barely moved (687.7 before vs ~685 after, ~0.3%) but whose count-scaled delta is multiplied by 8 calendar days off a single before-day.

**Why it matters.** The headline is not a portfolio measurement, it is one thin, mis-aggregated comparison amplified by the clock. Fixing items 2, 3 and 5 removes it entirely.

**Fix.** No separate fix; this DAG's contribution disappears once MAX replaces SUM, the after-window becomes exclusive, and the 3-observations-per-side gate is applied. Verify after the SQL edit that fangorn contributes 0 and is listed as "1 of 3 sweep-days before the fix".

**Evidence.** Decomposition of the exact Mode SQL pinned to DATE '2026-09-04': fangorn_score_monitor applied 2026-08-27, before_rate 6,189.3, after_rate 5,939.7, before_days 1, after_days 3, days 8, contrib_all_time 1,997.1. Exact share 57.80%.

### 8. The report's only schedule ("Findings by DAG", token d30b701e413d) fires daily at 06:00 UTC; the ledger is rewritten by the spark_optimizer_daily sweep at ~09:19 UTC.

**Why it matters.** Every unattended render reads a ledger written by the PREVIOUS day's sweep. The dashboard is structurally ~21 hours stale on any day nobody clicks refresh. The 3,455.2 currently on screen only exists because someone ran it manually at 23:10 UTC.

**Fix.** Edit the schedule to cron_hour 10 UTC (the sweep's artifacts land 09:08-09:24 UTC across 14 observed days, so 10:00 clears it with margin). Also rename the schedule - it is named after a chart, not the report.

**Evidence.** GET /schedules: 1 schedule, cron_hour 6, time_zone UTC, last_scheduled_run 2026-09-04T06:00:00Z, next 2026-09-05T06:00:00Z. dags/spark_optimizer_daily.py:37 schedule="0 9 * * *". GCS: every daily artifact written 09:08-09:24Z.

### 9. The BigQuery external table's schema is frozen at its 2026-08-28T22:14:35Z metadata update with ignoreUnknownValues=true, despite autodetect=true. Fields the ledger has gained since are silently dropped.

**Why it matters.** prev_exec_h already exists on 781 of 1,692 rows and is invisible to SQL ("Unrecognized name"). `partial`, which PR #1286 adds to every row from the 2026-09-05 sweep onward, will be dropped the same way - so the Mode SQL can never implement the partial-sweep exclusion the Python now has, and nobody will get an error telling them why.

**Fix.** Recreate the external table with an explicit schema (or re-run autodetect after the first post-#1286 sweep) covering all 18 Entry fields including prev_exec_h and partial. Then add AND NOT COALESCE(partial, FALSE) to the daily CTE.

**Evidence.** bq show: lastModifiedTime 1787955275315 = 2026-08-28T22:14:35Z, 16 fields ending at `surface`. An inline autodetect external table over the identical GCS URI resolves prev_exec_h (1692 rows, 360 non-null); the registered table errors.

### 10. Dollar conversion rests on frozen literals in the SQL - 0.278 per executor-hour in 5a66e5fad18c, 0.04 per slot-hour in 3ead7301daa8 and f513b6ed7755 - while the report's hero paragraph tells the reader "Dollars use the blended rate from actual Dataproc spend, $0.278 per executor-hour".

**Why it matters.** The hero makes a live-derivation claim the SQL does not honour; the number will not track spend. The $0.04 slot-hour rate is stated in a column header but sourced nowhere in either repo. Upstream, billing.py:23 pins DCU_PER_EXEC_H = 5.44, documented as "the conservative end of the 5.4-9.9 range" - at the top of that range every dollar figure is ~82% higher, and nothing re-measures it.

**Fix.** Two edits: (a) change the hero text to state the rate is a fixed assumption with its measurement date, or publish the sweep's live blended rate into the ledger (it is already computed - the 2026-09-04 task log shows "usd/exec-h 0.277 blended from 30d of actual spend") and join it in the SQL; (b) add a one-line footnote under both BigQuery tables giving the source and date of the $0.04 slot-hour rate, or replace it with a rate derived from INFORMATION_SCHEMA.

**Evidence.** Live query text: "* 0.278, 0), 2) AS dollars_saved_all_time" and "* 365 * 0.278"; "* 0.04, 0) AS usd_at_004_per_slot_h". Report layout offset 3263 carries the "blended rate from actual Dataproc spend" sentence. billing.py:23 and :73. 9.9/5.44 = +81.99%.

### 11. PR #1286 is deployed but has never executed. The prod Astro image is deploy-2026-09-04T23-19-30 (DEPLOYED 23:19:30.328Z, description "Merge pull request #1286", HEALTHY, updated_at 23:27:46.125Z), while every savings artifact live in prod was written by the pre-fix code at 09:19 UTC that morning, ~14 hours before the merge.

**Why it matters.** Anyone reading gs://.../optimizer_savings.md or the digest today still sees "115 hours all-time / ~$32" and may conclude the fix failed to deploy. It did deploy; it just has not run. The first post-fix sweep is 2026-09-05 09:00 UTC. None of this changes the Mode number.

**Fix.** After the 2026-09-05 09:00 UTC sweep, confirm optimizer_savings.md carries the 11-column header with a "90% CI" column and the Welch/MIN_OBSERVATIONS parenthetical, and that new ledger rows carry a `partial` field. If it still prints the 8-column pre-fix table, the image is wrong and the deploy needs re-checking.

**Evidence.** astro deployment inspect cmd6bd10c0gl901rfuokgryiq -> current_tag deploy-2026-09-04T23-19-30, HEALTHY. Platform API deploys: that tag DEPLOYED at 23:19:30.328Z after a FAILED 23:17:20 attempt on the same PR. gsutil stat optimizer_savings.md -> 2026-09-04T09:19:31Z, headline "115 hours all-time ... ~$32". Newest DAG run scheduled__2026-09-03T09:00:00+00:00 ended 2026-09-04T09:19:36.660Z; nothing since. The GitHub "Deploy to Prod" workflow has only two jobs, both GCS copies, and never touches Astro.

### 12. Remaining Python-side defects that survive PR #1286 and will corrupt the ledger the dashboard reads: (a) the all-time total is a sum over only the jobs that individually passed the 90% gate, with no multiplicity correction; (b) shipped() never resets `outcome` once set to 'resolved', so a job later marked owner_notified/wont_fix is frozen as a permanent win even while running at full pre-fix hours; (c) a fix's before-window has no lower bound, so on a job with two successive fixes the later fix's baseline averages in the era before the first.

**Why it matters.** (a) On data where every job truly saved zero, a 200-rep null simulation driving the real savings() reported a positive total with a CI excluding zero in 124-131 of 200 reps, mean +136.5 h against a truth of 0, and the total can never come out negative. (b) and (c) are latent today but will silently inflate future numbers.

**Fix.** (a) Either apply a Bonferroni/Holm adjustment to the per-job alpha before admitting a job to the total, or stop publishing a summed all-time headline and publish only the per-job table with its intervals. (b) In shipped(), reset row["outcome"] to "watching" when a later entry carries a STICKY state (owner_notified/wont_fix) after the resolve. (c) Lower-bound before_days at the previous applied_date for that (dag_id, surface).

**Evidence.** (a) Reproduced against ledger.py at 016e161: proven = saved - half > 0 at :676 gating tot["total"] += saved at :682; per-job false-positive rate 4.8-5.0% as designed, aggregate CI excludes zero 62-64% of the time under the null, and it is forced: total > sum(half_i) >= sqrt(sum(half_i^2)) = half. (b) Driven through real record()/mark_applied()/set_state(): a wont_fix after a resolve leaves shipped() outcome 'resolved', measured_jobs 1, 903.9 h counted while the job re-fires at full hours. (c) ledger.py:661 before_days = sorted(d for d in series if d < applied_date), unchanged by #1286; controlled two-fix case reported 486.4 h vs 250 h true. Zero DAGs in the live ledger have two applied_dates yet, so (c) is not firing today.

## Not verified

- The exact git SHA baked into the running Astro image. The deploy row's description reads "Merge pull request #1286 from SteelHouse/audi-1326-ledger-savings-correctness" and gitCommitSha is null in the Platform API, so the tag-to-SHA binding rests on a commit message plus a 23:19:30Z build timestamp two minutes after the 23:17:16Z merge. No Airflow deployment API token, so the running container's include/spark_optimizer/ledger.py could not be read in place.
- Whether the 2026-09-05 09:00 UTC sweep actually emits post-fix output. No sweep has run since the deploy, and this lane was read-only (no manual DAG trigger). The behavioural test - optimizer_savings.md switching to the 11-column / 90% CI format and rows carrying `partial` - has not been executed.
- The source of the $0.04 per slot-hour constant. It appears nowhere in either repo, in the report layout, or in any ticket; only in the SQL literals and column aliases.
- Whether the Mode run at 2026-09-04T23:10:53Z was a UI click or a scripted POST /runs. executed_by is /api/malachi_dunn on both that run and the 06:06 scheduled run, so the API cannot distinguish them. All that is verified is that it was not the 06:00 UTC schedule.
- Whether the 2026-09-03 collapse of the BigQuery surface (6 rows / ~4,000 slot-hours on 09-02 down to 3 rows / 15.4 slot-hours) is a genuine load drop or a profiler under-collection. It broke a run of exactly 3 bq findings on every day from 08-28 to 09-02 and left the newest sweep-day with zero bq rows. Not settled by any evidence gathered.
- Airflow task logs for the 2026-08-22..08-24 sweeps. The 3-day ledger hole is explained by the coverage artifacts ("Could not enumerate DAGs: airflow session use is forbidden", all 333 bytes) and the digests' "No change tracking this run" note plus the sweep.py guard, but the per-day task logs were not read.
- The /schedules/d30b701e413d/runs endpoint returned HTTP 504 on repeated attempts, so schedule-triggered run tokens could not be enumerated directly; the 06:00 UTC cadence rests on the schedule object's own last_run_at / next_scheduled_run fields.

## Method

100 agents, five independent probe lanes (deployed image / ledger data / Mode report definition / BigQuery external table / savings arithmetic). Every material claim went through an independent default-refute verifier before entering this document; refuted claims were dropped or replaced by the verifier's corrected version.

