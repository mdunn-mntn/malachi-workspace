---
doc_type: ticket
title: "AUDI-1328: Measure whether the optimizer recommendations actually work"
status: backlog
date: 2026-09-05
summary: "Score the 60 fixes shipped 2026-09-03 by detector and signature class"
result: "harness built, two undercounting defects repaired 2026-09-05; blocked on data until 2026-09-07 (reading A) or 2026-09-08 (reading B), and the attributable sample is 55 findings across 15 units under reading A but 3 across 1 under reading B"
question: "For the fixes the optimizer recommended, did the recommended change match the change that shipped, and did the finding go quiet for that reason?"
framing_state: draft
---

# AUDI-1328: Measure whether the optimizer recommendations actually work

**Jira:** https://mntn.atlassian.net/browse/AUDI-1328
**Status:** backlog
**Date Started:** 2026-09-04
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** For each shipped optimizer fix, (a) did the change that shipped match the change the optimizer recommended, and (b) did the finding go quiet *because of that change* rather than for an unrelated reason?
- **Goal (why / the decision):** Whether to keep trusting the optimizer's recommendation text as an actionable fix, or to demote it to a "here is a signal, go look" pointer. AUDI-1194's fangorn #1231 fix was reported as holding and then was not, so "the recommendations are right" is currently an assertion.
- **Objective (done-when):** A scoring table over every eligible finding with a verdict in {matched, partially matched, different fix worked, quiet unrelated, fix not working}, plus a stated precision per detector, plus an explicit statement of how many findings the number rests on.
- **Approach (how):** `audi_1328_score_recommendations.py` against the prod ledger + the airflow-ti git history. Two gates before any scoring: eligibility (enough quiet sweep-dates) then attributability (the quiet has no competing explanation).
- **What would change the answer:** If the attributable sample is under ~10 independent (DAG, PR) units, no per-detector precision can be stated and the ticket reports a sample-size finding instead of a precision number. **Whether that is the case now depends entirely on the deploy date: 15 units under reading A, 1 under reading B - see 4c and 4d.**

## 1. Introduction
The optimizer names a knob for each finding. Whether the knob helped has never been measured per
recommendation. The 60 fixes shipped 2026-09-03 across 8 PRs are a labelled set that collects its
own evidence, so this ticket scores them by detector and by signature class.

## 2. The Problem
"The recommendations are right" is currently an assertion. AUDI-1194's own history shows why that is
not safe: the fangorn #1231 fix was reported as holding, then did not - the finding went chronic
again and the ~$900 cumulative saving turned out to be a blind-window artifact. Nobody knows the rate
at which a shipped fix keeps firing, and nobody knows whether some detectors are reliably right while
others are reliably wrong.

## 3. Plan of Action - harness BUILT 2026-09-05, still blocked on data

The harness is written and runs today. It is `audi_1328_score_recommendations.py` in this folder.
Running it now is the intended use: it reports the forecast, scores whatever is eligible, and exits
non-zero when there is nothing to conclude.

```
python3 audi_1328_score_recommendations.py                      # prod ledger over GCS
python3 audi_1328_score_recommendations.py --ledger <snapshot>  # a local ledger copy
python3 audi_1328_score_recommendations.py --forecast-only      # sample size only, no scoring
python3 audi_1328_score_recommendations.py --effective-from 2026-09-05
python3 -m unittest audi_1328_test_score_recommendations -v     # 10 tests, 3 need the checkout
```

`audi_1328_test_score_recommendations.py` is the regression suite for 4g. Three of its tests read the
real `airflow-ti` checkout at `--repo` and skip when it is absent; the rest run on synthetic ledgers.

Exit codes are the blocking signal, so this can be run from cron without reading the table:
`0` something attributable was scored, `2` nothing has reached the quiet-date threshold,
`3` findings are eligible but every one of them has a competing explanation for its quiet.

**The unblock date.** Derived independently from `include/spark_optimizer/ledger.py` at `016e161`,
not taken on trust: `_mark_resolved` skips any key with a row dated in
`prior_sweep_dates[-(RESOLVE_SWEEPS - 1):]`, i.e. the last **2** sweep-dates before the one being
written. The 60 fixes carry `state='applied'` rows dated `2026-09-03` (`mark_applied` stamps
`date = applied_date`). So ledger date `09-04` sees `{09-02, 09-03}` and is blocked, `09-05` sees
`{09-03, 09-04}` and is blocked, and **ledger date `09-06` is the first that can resolve them**. A
ledger date is written by the following day's run, so that is the **2026-09-07 09:00 UTC** run. The
script computes this itself and prints it (`earliest scoreable ledger date`).

**Blocked on, not resolved:**
- Three more complete sweeps must land (the `09-05`, `09-06`, `09-07` runs). Resolution counts
  distinct ledger DATES, not executions: a partial sweep, an empty crawl, or a skipped day does not
  advance the window. The ledger already has a three-day hole (`2026-08-22`..`08-24`). Confirm the
  dates exist before trusting the run.
- `partial` is now a ledger field. PR #1286 shipped and the `2026-09-04` ledger date is the first
  to carry it: 183 rows, all `partial: false`. Every date before `09-04` still lacks the field, so
  `partial_window` remains inert over the pre-`09-04` window and can only catch a torn sweep from
  `09-04` onward.

## 4. Investigation & Findings

### 4a. Method

**Unit of analysis.** One `(dag_id, finding_key)` pair carrying a non-empty `fix_pr` and
`applied_date`. State is not the filter: 10 of the 60 shipped keys carry a detector state
(`chronic`/`recurring`) rather than `applied`, because `append()` replaces same-`(date, dag, key)`
rows and the `09-04` sweep overwrote their `applied` row. Attribution survives that overwrite
(`append()` carries `fix_pr`/`applied_date` forward via `to_preserve`), so all 64 are scorable.

**Two gates, in order.** Nothing is scored for alignment until it has passed both.

1. **Eligibility** - is there enough evidence to say anything?
   - `E1` the finding carries a `fix_pr` and an `applied_date`
   - `E2` at least `RESOLVE_SWEEPS`=3 sweep-dates on the finding's **own surface** lie in the watch
     window. Surface matters: `bq` last swept `09-02`, so a `bq` key absent on `09-03` was not
     looked at, it did not go quiet.
   - `E3` those dates form an unbroken *trailing* quiet run - the same thing `_mark_resolved` keys
     on. A finding that re-fires and then goes quiet again starts its run over.

   Quiet means **measured and silent**, never merely absent. A sweep-date on which the DAG produced no
   measurement is not evidence of anything, in either direction. `unobserved_window` applies that to
   the post-fix window and `pre_fix_quiet` (4b) applies the same test to the pre-fix window; before
   2026-09-05 the pre-fix rule did not, which is defect 2 in 4g.

2. **Attributability** - does the quiet mean anything? Every rule in 4b is a competing explanation
   for the silence. Any hit makes the verdict `quiet_unrelated`, naming the rule. This is the gate
   that matters; without it the ticket measures the ledger's optimism rather than the optimizer.

**Verdicts.** `matched`, `partially_matched`, `different_fix_worked`, `no_shipped_change`,
`fix_not_working`, `quiet_unrelated`, `not_eligible`, `unscoreable`.

### 4b. The rubric

**Did the shipped fix match the recommendation?** The recommendation text is the ledger's `fix`
field; the shipped change is the diff of the `fix_pr`'s merge commit **on `origin/main`** against its
first parent, read two ways: the DAG's entry in `dags/model_task_config.json` (Spark properties,
recursively collected) and any `.config("k", "v")` line in the DAG's `main_python_file_uri` model.

| Verdict | Rule |
|---|---|
| `matched` | Every Spark key the recommendation asks for was changed, and each hit its stated target - a numeric target within a factor of 2, a boolean equal to the requested value, or a "2x the current" ask that at least doubled. |
| `partially_matched` | Some recommended keys landed and others did not; or none landed but an equivalent lever did (`spark.sql.shuffle.partitions` vs `spark.sql.adaptive.advisoryPartitionSizeInBytes`); or only a secondary parameter moved. |
| `different_fix_worked` | The PR changed this DAG's config, none of the recommended keys are among the changes, and the finding still went quiet. The optimizer found a real problem and named the wrong knob. |
| `no_shipped_change` | The PR touched nothing attributable to this DAG. Nothing to score. |
| `fix_not_working` | The finding fired again on a sweep-date inside the watch window. |
| `unscoreable` | The `fix_pr` merge commit is not on `origin/main` in the checkout. |

A recommendation that explicitly warns *against* a key ("do not raise `spark.sql.shuffle.partitions`")
does not count that key as a hit, and the diff records `violates_warning` when the PR changed it anyway.

**Was the quiet real?** - the `quiet_unrelated` rules, in reporting priority:

| Rule | Detection |
|---|---|
| `pre_fix_quiet` | The finding was already silent on a sweep-date **before** the fix went live **on which its DAG was actually measured**. It stopped on its own; the fix cannot be credited. The rule takes the trailing quiet run of pre-fix sweep-dates, keeps only the dates carrying a measured `exec_h` for that DAG, and fires only if at least one survives. An `applied` row's `exec_h` does not count as a measurement: `mark_applied` copies the previous date's value forward, so it is a carried figure, not a look. |
| `unobserved_window` | Fewer than 3 of the quiet dates carry a measured `exec_h` for that DAG. Absence of a finding is absence of a look, not absence of a problem. |
| `partial_window` | Quiet dates fall on sweeps flagged `partial`. Inert until PR #1286's field lands. |
| `dag_went_dark` | Median `exec_h` across the quiet dates is under 20% of the median across the 5 sweep-dates before the fix. A finding cannot fire on work the job stopped doing. |
| `detector_went_silent` | That detector fired on **no** DAG anywhere in the fleet across the quiet window, having fired before it. The silence belongs to the detector, not to this fix. |
| `confounded_by_other_change` | A commit other than the fix, reachable from `origin/main`, changed this DAG's own config entry inside the window. |

Three implementation notes that are load-bearing, because all three were bugs caught in this harness:
- The git scan is scoped to `--main-ref` (default `origin/main`), **not** `--all`. Scanning `--all`
  charged `fangorn_score_monitor` with `e59f3858`, a commit on an abandoned branch that never
  shipped. Three of the four commits it flagged were real; one was not.
- `pre_fix_quiet` compares the last firing date against the last **observed sweep-date** before the
  fix, not against the `applied_date`. The applied date is itself a mixed day (see 4d), so comparing
  to it silently reclassifies the whole cohort.
- `CONFIG_CALL` must carry `re.MULTILINE`. It is anchored with `^` and run over a whole multi-line
  `git diff` blob, so without the flag it can only match at byte 0 of the diff, which is the
  `diff --git` header line. See 4g.

### 4c. Sample size - the finding that matters most, and it still turns on the deploy date

Run against the live ledger pulled 2026-09-05T15:57Z (1,875 rows, 1,053,572 bytes, newest sweep-date
`2026-09-04`), with the 4g repairs in place:

| | Reading A: fix live on merge (`09-03`) | Reading B: fix live `09-05` |
|---|---|---|
| attributed findings | 64 | 64 |
| measured and silent before the fix went live | 0 | **52** |
| still firing after the fix | 9 | 9 |
| **best case attributable** | **55** | **3** |
| **independent (DAG, PR) units** | **15** | **1** |
| eligible right now | 2 | 0 |
| earliest scoreable ledger date | `2026-09-06` (09-07 run) | `2026-09-07` (09-08 run) |
| exit code | 3 | 2 |

The two findings already eligible under reading A are `fangorn_score_monitor/shuffle_partition_sizing:17`
and `:19` from #1231 (applied `2026-08-27`, 8 quiet dates). Both score `quiet_unrelated` on
`confounded_by_other_change`: commits `3ec2e138`, `234cb491` and `17df2fe9` also changed that DAG's
config inside the window. So nothing is attributable yet under either reading.

**The 4g repairs moved this number, and the direction depends on which ledger you run against.**
Against the `2026-09-04T09:19:23Z` snapshot the previous revision of this section used (1,692 rows,
930,490 bytes), the repaired harness reports **23 findings across 12 independent units** where the
defective one reported 10 across 5. Thirteen of those thirteen recovered findings were written off
because they were silent on `2026-09-03` - a date on which the fleet swept but their own DAG produced
no measurement. That is the regression check for defect 2 and it reproduces exactly.

**Against the live ledger reading B has since collapsed to 3 across 1.** The `2026-09-04` sweep-date
landed between the two runs, and under reading B `09-04` is a *pre-deploy* day. 52 of the 64 findings
were measured on it - their DAG was crawled and carries an `exec_h` - and were silent. That is
exactly the evidence `pre_fix_quiet` is meant to catch: they had stopped before the config could be
live. The single surviving unit is `vertical_size_monitor`/#1275 (`disk_spill:11`, `:13`, `:17`),
whose DAG was not crawled on `09-04` at all.

**Reading A is unaffected by that**, because under it `09-04` is the first post-fix sweep-date rather
than the last pre-fix one: 55 findings across 15 units are quiet on it and stay in the pool.

**The gap between 55 and 3 is entirely the deploy date (4d).** It is the single number this ticket
needs and it is still not established.

**One residual defect, not repaired, that inflates reading B's write-off (see 8).** `fired_after_fix`
counts firings after `applied_date`, not after `watch_from`. Under reading B the fix is not live
until `09-05`, so a firing on `09-04` is a pre-deploy event, yet 7 of the 9 "still firing" findings
fired on `09-04` and would be labelled `fix_not_working` off it. Measured from `watch_from` instead,
reading B's best case is **10 findings across 6 units** - `ipdsc_ds_2`/#1273, `ipdsc_ds_49`/#1272,
`ipdsc_third_party_audience_builder`/#1273, `site_network_hourly`/#1271,
`site_visit_signal_advertiser_id_dsc_id`/#1273 and `vertical_size_monitor`/#1275 - not 3 across 1.
Either way findings on the same DAG under the same PR are not independent trials: they share one
config change and one workload, so the effective n is the unit count, not the finding count.

### 4d. Why the deploy date decides the reading, and it is not established

`dags/model_task_config.json` is **not** in the prod GCS deploy path.
`.github/workflows/deploy_prod.yaml` calls only `deploy_gcs.yaml` (uploads `spark/` to
`mntn-data-archive-prod/ti_resources`) and `deploy_model_to_gcs.yaml`. The task config ships inside
the Airflow image, so **merging a config change does not make it live** - an Astro deploy does. Per
the AUDI-1325 audit the prod image is `deploy-2026-09-04T23-19-30`, and the GitHub deploy workflow
never touches Astro.

If that 23:19 UTC deploy on `09-04` is the one that carried #1270-#1281, then ledger date `09-04` is
still a pre-fix day, `09-05` is the first fully post-fix day, and the `09-07` run will be counting a
pre-fix day as post-fix evidence. **The earliest honest scoring run is then `2026-09-08`, not
`2026-09-07`.** Settling this needs the Astro deploy history for the deploys before
`deploy-2026-09-04T23-19-30`, which this lane could not read (no Airflow deployment API token, and
the AUDI-1325 audit lists the image-to-SHA binding as unverified). Until it is settled, run both
readings; they are one flag apart.

### 4e. Threats to validity

1. **Effective n is the unit count, not the finding count - 15 under reading A, 1 under reading B.**
   Findings cluster by DAG and PR. Any per-detector precision from this cohort is a statement about
   one or two DAGs wearing a detector's name.
2. **The deploy date is unverified (4d).** It moves the attributable sample between 55 and 3 and the
   scoring date between `09-07` and `09-08`. This is the single largest source of error.
3. **A resolution is not a measurement.** The ledger resolves on absence. Absence is produced by a
   working fix, an unobserved DAG, a torn sweep, a dead job, and a silent detector alike. The
   attributability gate is the whole defence and each of its rules is a heuristic with a threshold -
   `dag_went_dark` at 20% of a 5-date median baseline is a choice, not a derived constant.
4. **`partial` is recorded only from `2026-09-04` onward** (183 rows, all `false`), so
   `partial_window` cannot fire on any earlier date. A torn sweep inside the pre-`09-04` window would
   be caught only if it also made the DAG unobserved.
5. **Alignment scoring reads text.** `parse_recommendation` extracts Spark keys and targets by regex
   from prose. A recommendation phrased unusually will under-report its primary keys and can push a
   genuine `matched` toward `different_fix_worked`. Spot-check the `recommended_keys` column against
   the `fix` text before reporting any `different_fix_worked` count.
6. **Same-day mixing.** A ledger date is a whole calendar day of runs, but a fix merges mid-day. The
   applied date is neither clean pre nor clean post, and the harness therefore excludes it from both
   windows rather than assigning it.
7. **No multiplicity control.** With at most 15 effective units there is no power for one; equally
   there is no basis for reporting a rate with a confidence interval. Report counts, not percentages.
9. **The attributability rules are asymmetric by design.** Crediting a fix needs 3 measured quiet
   sweep-dates; discrediting it via `pre_fix_quiet` needs only 1 measured quiet pre-fix date. The
   burden sits on crediting the fix deliberately, but it means a single flap-quiet day before the
   deploy removes a finding from the sample.
8. **`unscoreable` is silent on squash-merges.** #1231 has no `Merge pull request #1231` commit and
   is found only by the `(#1231)` fallback. A PR squashed with neither pattern in its subject would
   score `unscoreable` rather than erroring.

### 4f. What runs on the day

```
python3 -m unittest audi_1328_test_score_recommendations
python3 audi_1328_score_recommendations.py --forecast-only
python3 audi_1328_score_recommendations.py --out outputs
python3 audi_1328_score_recommendations.py --effective-from <deploy date> --out outputs
```

Run the tests first: three of them read the real merge commits out of the checkout, so a failure
there means the `--repo` checkout is stale or the PR is not on `origin/main`, not that a fix regressed.
Then check that the ledger actually holds sweep-dates `09-04`, `09-05` and `09-06` and that none is
`partial`; the header prints both. Then fill 5 and 6 with the verdict tally and the per-detector
split. Everything above this line is independent of the results.

### 4g. Two defects that undercounted the sample, repaired 2026-09-05

Both were found by adversarial review of the built harness and both are now covered by
`audi_1328_test_score_recommendations.py`. Reverting either fix fails the suite: 13 failures and 1
error against the pre-repair code, 10 passes after.

**Defect 1 - `CONFIG_CALL` was missing `re.MULTILINE`.** The pattern is anchored with `^` and is run
via `findall` over a whole multi-line `git diff` blob, so without the flag it could only ever match at
byte 0, which is the `diff --git` header. The `.config("k", "v")` half of the shipped-change read was
therefore dead on every unit. Measured over all 16 attributed (DAG, PR) units against the real merge
commits on `origin/main`: 8 units read differently with the flag, and **7 of the 16 went from an empty
`changed` dict to a real one** - `conv_log_derived_ip`/#1272, `conversion_log_advertiser_id_dsc_id`/#1273,
`guid_conv_log_pivot_ip_vertical_id`/#1270, `guid_log_advertiser_id_dsc_id`/#1273,
`guid_log_pivot_ip_vertical_id`/#1270, `ipdsc_ds_49`/#1272 and
`site_visit_signal_advertiser_id_dsc_id`/#1273. Every one of those would have scored
`no_shipped_change` - "the PR changed nothing attributable to this dag" - against a PR that plainly
did change it. The 8th, `fangorn_score_monitor`/#1231, read its before-value as `512` (the
`model_task_config.json` figure) instead of `256` (the value the model file's own `.config` line
actually carried).

**Defect 2 - `pre_fix_quiet` did not require the DAG to have been measured.** The rule fired whenever
`last_fired < last_pre_fix_date`, where `last_pre_fix_date` is the newest sweep-date on the finding's
*surface* before the fix. A surface sweep-date does not mean this DAG was crawled - which is precisely
the doctrine `unobserved_window` encodes for the post-fix window. Against the `2026-09-04T09:19:23Z`
snapshot, **13 findings were written off on `2026-09-03`, a date on which their own DAG produced no
measurement at all.** The `applied` row those keys carry on `09-03` looks like a measurement because
it has an `exec_h`, but `mark_applied` copies the previous date's value forward
(`ipdsc_third_party_audience_builder/disk_spill:105`: `36.2` on `09-02`, `36.2` on the `09-03`
`applied` row), so `observed_hours` correctly excludes it. The repaired rule takes the trailing quiet
run of pre-fix sweep-dates, keeps only dates carrying a measured `exec_h` for that DAG, and fires only
if at least one survives. Against that same snapshot the sample goes from 10 findings across 5 units
to **23 across 12**. The same gate is used by `forecast()`, which previously duplicated the ungated
expression inline; there is now one definition, the unit field `pre_fix_quiet_dates`.

## 5. Solution
_Pending the 2026-09-07 (or 2026-09-08) run. Fill with: the verdict tally, the per-detector split,
and the recommendation-precision statement._

## 6. Questions Answered
- **Q:** When is the earliest run that can score these fixes?
  **A:** The `2026-09-07 09:00 UTC` run, which writes ledger date `2026-09-06` - derived from
  `_mark_resolved`'s 2-date grace window and `mark_applied` stamping `date = applied_date`. If the
  config only reached prod with the `2026-09-04T23:19:30Z` Astro deploy, it is `2026-09-08` instead.
- **Q:** Is the sample big enough to conclude anything about recommendation quality?
  **A:** It depends entirely on the deploy date, and that is the answer worth carrying. Under reading
  A (live on merge, `09-03`) the ceiling is 55 findings across 15 independent (DAG, PR) units, which
  would support a per-detector split. Under reading B (live `09-05`) it is 3 findings across 1 unit -
  10 across 6 once the residual `fired_after_fix` defect in 8 is corrected - which supports a per-fix
  narrative and nothing more. Settle 4d before promising anyone a precision number.
- **Q:** How many findings will actually be scoreable on the day?
  **A:** Reading A, the `2026-09-07 09:00 UTC` run (ledger date `09-06`): **at most 55, across 15
  units**, and only if `09-05` and `09-06` both land as complete sweeps and none of the 55 re-fires.
  The 9 findings that fired on `09-04` cannot reach 3 quiet dates until ledger date `09-07`, so they
  are not scoreable until the `2026-09-08` run. Reading B, the `2026-09-08 09:00 UTC` run (ledger date
  `09-07`): up to 64 findings become *eligible*, but only **3, across 1 unit** survive attributability
  as the harness stands (10 across 6 with the `fired_after_fix` correction). Both are ceilings; every
  `unobserved_window`, `dag_went_dark`, `detector_went_silent` or `confounded_by_other_change` hit on
  the day subtracts from them, as it already does for the two `fangorn_score_monitor` findings.
- **Q:** Does merging a config fix make it live in prod?
  **A:** No. `dags/model_task_config.json` ships in the Airflow image; the prod GitHub workflow only
  copies `spark/` and the model to GCS and never touches Astro.

## 7. Data Documentation Updates
_Pending /capture at close. Candidates: the ledger resolve-window arithmetic (`_mark_resolved` grace
= last `RESOLVE_SWEEPS - 1` sweep-dates, a ledger date is written by the following day's run), and
that a merged `model_task_config.json` change is not live until an Astro deploy._

## 8. Open Items / Follow-ups
- **Settle the deploy date (4d).** Needs the Astro deploy list before `deploy-2026-09-04T23-19-30`.
  It decides both the sample size (55 units-worth vs 3) and the scoring date (`09-07` vs `09-08`).
  Nothing else on this list matters as much.
- **`fired_after_fix` is measured from `applied_date`, not `watch_from`.** Under `--effective-from`
  the two differ, so a firing between the merge and the deploy is scored as the fix not working when
  it is a pre-deploy event. It mislabels 7 findings under reading B and understates that reading's
  best case by 7 findings across 5 units (4c). Left unrepaired because it was outside the scope of
  the 2026-09-05 repair pass; fix it before the `09-08` run if reading B is the one that stands.
- **The forecast line "still firing after their fix ... (scoreable now as fix_not_working)" is not
  true today.** `fix_not_working` is only reachable after the eligibility gate passes, and a finding
  that fired inside the watch window has 0 trailing quiet dates, so it scores `not_eligible` instead.
  Either drop the parenthetical or let a re-fire short-circuit eligibility.
- `partial` now lands (183 rows on `2026-09-04`, all `false`). Confirm it keeps landing on `09-05`
  and `09-06`; `partial_window` stays inert for every date before `09-04`.
- The `dag_went_dark` threshold (20% of a 5-date median) is unvalidated against a known-dead job.
- If the attributable sample really comes in at 1 unit, decide whether AUDI-1328 reports a sample-size
  finding and closes, rather than publishing a precision number nobody should act on.
