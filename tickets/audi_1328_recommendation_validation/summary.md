---
doc_type: ticket
title: "AUDI-1328: Measure whether the optimizer recommendations actually work"
status: backlog
date: 2026-09-05
summary: "Score the 60 fixes shipped 2026-09-03 by detector and signature class"
result: "harness built, five scoring defects repaired 2026-09-05; blocked on data until 2026-09-07 (reading A) or 2026-09-08 (reading B), and the attributable sample is 55 findings across 15 units under reading A but 12 across 7 under reading B"
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
- **What would change the answer:** If the attributable sample is under ~10 independent (DAG, PR) units, no per-detector precision can be stated and the ticket reports a sample-size finding instead of a precision number. **Whether that is the case now depends entirely on the deploy date: 15 units under reading A, 7 under reading B - see 4c and 4d.**

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
python3 -m unittest audi_1328_test_score_recommendations -v     # 18 tests, 5 need the checkout
```

`audi_1328_test_score_recommendations.py` is the regression suite for 4g and 4h. Five of its tests
read the real `airflow-ti` checkout at `--repo` and skip when it is absent; the rest run on synthetic
ledgers or on `score_alignment` directly.

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
| `partially_matched` | Some recommended keys landed and others did not; or a recommended key landed but missed its stated target (including a "2x the current" ask on a key the PR set for the first time, where there is no before-value to double); or none landed but an equivalent lever did (`spark.sql.shuffle.partitions` vs `spark.sql.adaptive.advisoryPartitionSizeInBytes`); or only a secondary parameter moved. |
| `different_fix_worked` | The PR changed this DAG's config and **not one** of the recommended keys is among the changes, yet the finding still went quiet. The optimizer found a real problem and named the wrong knob. A recommended key that shipped but missed its target is `partially_matched`, never this - see 4h defect 3. |
| `no_shipped_change` | The PR touched nothing attributable to this DAG: an empty config diff **and** an untouched model file. **Unreachable for all 16 units in this cohort** - every fix PR edits its DAG's model file, so `code_changed` is always true. Do not report a count for it; see 4g. |
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

Run against the live ledger (1,875 rows, newest sweep-date `2026-09-04`), with the 4g **and 4h**
repairs in place:

| | Reading A: fix live on merge (`09-03`) | Reading B: fix live `09-05` |
|---|---|---|
| attributed findings | 64 | 64 |
| measured and silent before the fix went live | 0 | **52** |
| still firing after the fix | 9 | 0 |
| **best case attributable** | **55** | **12** |
| **independent (DAG, PR) units** | **15** | **7** |
| eligible right now | 2 | 0 |
| earliest scoreable ledger date | `2026-09-06` (09-07 run) | `2026-09-07` (09-08 run) |
| exit code | 3 | 2 |

Reading B's 7 units are `fangorn_score_monitor`/#1231 (2 findings), `ipdsc_ds_2`/#1273 (1),
`ipdsc_ds_49`/#1272 (1), `ipdsc_third_party_audience_builder`/#1273 (2),
`site_network_hourly`/#1271 (1), `site_visit_signal_advertiser_id_dsc_id`/#1273 (2) and
`vertical_size_monitor`/#1275 (3).

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

**Against the live ledger reading B has since collapsed to 12 across 7.** The `2026-09-04` sweep-date
landed between the two runs, and under reading B `09-04` is a *pre-deploy* day. 52 of the 64 findings
were measured on it - their DAG was crawled and carries an `exec_h` - and were silent. That is
exactly the evidence `pre_fix_quiet` is meant to catch: they had stopped before the config could be
live. Of the 12 that survive, `vertical_size_monitor`/#1275 (`disk_spill:11`, `:13`, `:17`) is the
clearest case: its DAG was not crawled on `09-04` at all.

**Reading A is unaffected by that**, because under it `09-04` is the first post-fix sweep-date rather
than the last pre-fix one: 55 findings across 15 units are quiet on it and stay in the pool.

**The gap between 55 and 12 is entirely the deploy date (4d).** It is the single number this ticket
needs and it is still not established.

The 12/7 figure already includes the `fired_after_fix` repair (4h defect 4). Before it, reading B
counted all 9 firings on `09-04` as post-fix and reported 3 findings across 1 unit; `09-04` is a
pre-deploy day under reading B, so those firings are pre-deploy events and the correct figure is 12
across 7. Findings on the same DAG under the same PR are not independent trials either way: they
share one config change and one workload, so the effective n is the unit count, not the finding
count.

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

1. **Effective n is the unit count, not the finding count - 15 under reading A, 7 under reading B.**
   Findings cluster by DAG and PR. Any per-detector precision from this cohort is a statement about
   one or two DAGs wearing a detector's name.
2. **The deploy date is unverified (4d).** It moves the attributable sample between 55 and 12 and the
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
`audi_1328_test_score_recommendations.py`.

**What the suite actually proves, stated precisely.** Run the current 18-test suite against the
pre-repair module at `b53cfe9a` and it reports **23 failures and 3 errors**; against the repaired
module all 18 pass. The 3 errors are not behavioural detections: all three are
`KeyError: 'pre_fix_quiet_dates'`, raised because the pre-repair `analyse()` never wrote that unit
field at all. They detect the API shape of the repair, not the misbehaviour it fixed. The 23
failures are behavioural, and unittest counts each `subTest` case separately, so they cover 10
distinct test methods. An earlier revision of this section reported "13 failures and 1 error" for
the 10-test suite as it then stood; the failure count was right for that suite but the error count
was wrong - it was 3, and they were the same three `KeyError`s.

**Defect 1 - `CONFIG_CALL` was missing `re.MULTILINE`.** The pattern is anchored with `^` and is run
via `findall` over a whole multi-line `git diff` blob, so without the flag it could only ever match at
byte 0, which is the `diff --git` header. The `.config("k", "v")` half of the shipped-change read was
therefore dead on every unit. Measured over all 16 attributed (DAG, PR) units against the real merge
commits on `origin/main`: 8 units read differently with the flag, and **7 of the 16 went from an empty
`changed` dict to a real one** - `conv_log_derived_ip`/#1272, `conversion_log_advertiser_id_dsc_id`/#1273,
`guid_conv_log_pivot_ip_vertical_id`/#1270, `guid_log_advertiser_id_dsc_id`/#1273,
`guid_log_pivot_ip_vertical_id`/#1270, `ipdsc_ds_49`/#1272 and
`site_visit_signal_advertiser_id_dsc_id`/#1273. The 8th, `fangorn_score_monitor`/#1231, read its
before-value as `512` (the `model_task_config.json` figure) instead of `256` (the value the model
file's own `.config` line actually carried).

**What those 7 actually scored, corrected.** An earlier revision of this section said they "would
have scored `no_shipped_change`". They did not. Running the pre-repair module at `b53cfe9a` over the
live ledger scores all 16 findings on those 7 units `different_fix_worked` - "the optimizer found a
real problem and named the wrong knob" - which is a worse failure than `no_shipped_change`, because
it charges the optimizer with a wrong recommendation instead of recording that the harness could not
read the diff.

**`no_shipped_change` is dead code for this cohort, and here is why.** It fires only when the config
diff is empty **and** `code_changed` is false, and `code_changed` is `bool(diff)` over the DAG's
whole `main_python_file_uri` model file. All 16 attributed (DAG, PR) units have `code_changed=True`,
so none of them can reach the branch under any repair state of `CONFIG_CALL`. That is not an
accident of the flag: every one of these 8 PRs edits the model file it targets. The one unit whose
config diff is genuinely empty, `ipdsc_42_monitor`/#1276, is a true negative for the branch rather
than a miss - #1276 shipped `/*+ BROADCAST(dd) */` join hints and a rewritten comparison query in
`models/monitoring/ipdsc_42_monitor.py` and changed no Spark property, so "the PR changed nothing
attributable to this dag" would be false of it. The branch stays in the code because it is reachable
in principle, for a `fix_pr` that touches neither the DAG's config entry nor its model file, and
`test_pr_1276_changes_no_config_key_and_avoids_no_shipped_change_only_via_the_code_diff` pins the
reason it does not fire here. Treat the verdict as unavailable for this cohort: nothing in sections
5 or 6 may report a `no_shipped_change` count.

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

### 4h. Three further defects, repaired 2026-09-05 (second pass)

**Defect 3 - a recommended key that shipped was scored `different_fix_worked`.** `score_alignment`
appended a key to `misses` in two different situations: the key was not in the diff at all, and the
key *was* in the diff but did not meet its stated target. When every recommended key fell into the
second situation, `hits` was empty and the function fell through past `partially_matched` to
`different_fix_worked`, whose stated rule (4b) is "none of the recommended keys are among the
changes". Six of the 64 findings hit this and are now `partially_matched`:
`conversion_log_advertiser_id_dsc_id/disk_spill:13` and `:24`,
`guid_log_advertiser_id_dsc_id/disk_spill:13` and `:24`, and
`site_visit_signal_advertiser_id_dsc_id/disk_spill:7` and `:12`.

All six carry the same recommendation - "Raise `spark.sql.shuffle.partitions` so each task holds less
data (start at 2x the current count); if it still spills, raise `spark.executor.memory`" - and #1273
shipped exactly that key on all three DAGs: `3508`, `3400` and `3392` respectively. The target check
failed because each was a *newly added* `.config` line with no `-` counterpart in the diff, so the
before-value is `None`, and the `2x the current` relative test needs a before-value to divide. The
recommendation is nonetheless the knob that shipped. `spark.executor.memory` is a genuine miss (the
recommendation asks for it only conditionally), which is why the corrected verdict is
`partially_matched` rather than `matched`. The loop now separates "not changed" from "changed but
off target" and only falls through to `different_fix_worked` when no recommended key appears in the
diff at all. `RecommendedKeyThatShipped` covers the six by name and fails on the pre-repair code.

The five remaining `different_fix_worked` findings are unaffected and are real:
`conv_log_derived_ip/disk_spill:1` and `ipdsc_ds_49/disk_spill:1` (recommendation asked for
`spark.sql.shuffle.partitions`, #1272 shipped `spark.sql.files.maxPartitionBytes`) and
`ipdsc_42_monitor/skew:18`, `:22`, `:26` (recommendation asked for
`spark.sql.adaptive.skewJoin.enabled`, #1276 shipped broadcast join hints).

**Defect 4 - `fired_after_fix` counted from `applied_date`, not `watch_from`.** Under
`--effective-from` the two differ, so a firing between the merge and the deploy was scored as the fix
not working when it is a pre-deploy event. `analyse()` now builds one `in_watch` predicate and uses
it for both the post-fix sweep-date list and `fired_after_fix`, so the two can no longer disagree.
The effect on reading B is large: `still_firing` drops from 9 to 0 and the best case rises from 3
findings across 1 unit to **12 across 7** (4c). `FiredAfterFixWindow` covers both directions.

**Defect 5 - the forecast claimed re-firing findings were scoreable now.** It printed "still firing
after their fix N (scoreable now as `fix_not_working`)". `fix_not_working` is only reachable after
the eligibility gate passes, and a finding that fired inside the watch window has zero trailing quiet
sweep-dates, so it is `not_eligible`, not `fix_not_working`. The line now reads "each firing restarts
the quiet run; not scoreable until 3 quiet sweep-dates follow the last one", which is what the code
produces.

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
  would support a per-detector split. Under reading B (live `09-05`) it is 12 findings across 7 units,
  which supports a per-fix narrative and, at the margin of the ~10-unit bar in 0, no per-detector
  split. Settle 4d before promising anyone a precision number.
- **Q:** How many findings will actually be scoreable on the day?
  **A:** Reading A, the `2026-09-07 09:00 UTC` run (ledger date `09-06`): **at most 55, across 15
  units**, and only if `09-05` and `09-06` both land as complete sweeps and none of the 55 re-fires.
  The 9 findings that fired on `09-04` cannot reach 3 quiet dates until ledger date `09-07`, so they
  are not scoreable until the `2026-09-08` run. Reading B, the `2026-09-08 09:00 UTC` run (ledger date
  `09-07`): up to 64 findings become *eligible*, but only **12, across 7 units** survive
  attributability as the harness stands. The `2026-09-07` run scores nothing at all under reading B:
  its newest ledger date is `09-06`, which is only 2 quiet dates into a watch window opening `09-05`.
  Both are ceilings; every
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
  It decides both the sample size (55 findings across 15 units vs 12 across 7) and the scoring date
  (`09-07` vs `09-08`). Nothing else on this list matters as much.
- ~~`fired_after_fix` is measured from `applied_date`, not `watch_from`~~ - repaired 2026-09-05, 4h
  defect 4.
- ~~The forecast line "still firing after their fix ... (scoreable now as `fix_not_working`)" is not
  true today~~ - repaired 2026-09-05, 4h defect 5.
- **`no_shipped_change` cannot fire on this cohort** (4g). If sections 5 and 6 need that verdict to
  mean anything, `code_changed` would have to narrow from "the model file changed at all" to "the
  model file's `.config` calls changed". Not done: `ipdsc_42_monitor`/#1276 shows the current wide
  reading is the correct one for scoring, since a PR can ship a real fix with no Spark property.
- `partial` now lands (183 rows on `2026-09-04`, all `false`). Confirm it keeps landing on `09-05`
  and `09-06`; `partial_window` stays inert for every date before `09-04`.
- The `dag_went_dark` threshold (20% of a 5-date median) is unvalidated against a known-dead job.
- If the attributable sample really comes in at 1 unit, decide whether AUDI-1328 reports a sample-size
  finding and closes, rather than publishing a precision number nobody should act on.
