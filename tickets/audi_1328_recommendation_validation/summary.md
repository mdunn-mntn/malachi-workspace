---
doc_type: ticket
title: "AUDI-1328: Measure whether the optimizer recommendations actually work"
status: backlog
date: 2026-09-05
summary: "Score the 60 fixes shipped 2026-09-03 by detector and signature class"
result: "harness built and runnable; blocked on data until 2026-09-07 at the earliest, and the attributable sample is 10 findings across 5 independent units, not 60"
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
- **What would change the answer:** If the attributable sample is under ~10 independent (DAG, PR) units, no per-detector precision can be stated and the ticket reports a sample-size finding instead of a precision number. **This is the case as of 2026-09-05 - see 4c.**

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
```

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
- `partial` is not yet a ledger field. PR #1286 adds it from the `2026-09-05` sweep onward. Until
  rows carry it, the `partial_window` rule below is inert and cannot catch a torn sweep.

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
| `pre_fix_quiet` | The finding was already silent on the last sweep-date **before** the fix went live. It stopped on its own; the fix cannot be credited. |
| `unobserved_window` | Fewer than 3 of the quiet dates carry a measured `exec_h` for that DAG. Absence of a finding is absence of a look, not absence of a problem. |
| `partial_window` | Quiet dates fall on sweeps flagged `partial`. Inert until PR #1286's field lands. |
| `dag_went_dark` | Median `exec_h` across the quiet dates is under 20% of the median across the 5 sweep-dates before the fix. A finding cannot fire on work the job stopped doing. |
| `detector_went_silent` | That detector fired on **no** DAG anywhere in the fleet across the quiet window, having fired before it. The silence belongs to the detector, not to this fix. |
| `confounded_by_other_change` | A commit other than the fix, reachable from `origin/main`, changed this DAG's own config entry inside the window. |

Two implementation notes that are load-bearing, because both were bugs caught while building this:
- The git scan is scoped to `--main-ref` (default `origin/main`), **not** `--all`. Scanning `--all`
  charged `fangorn_score_monitor` with `e59f3858`, a commit on an abandoned branch that never
  shipped. Three of the four commits it flagged were real; one was not.
- `pre_fix_quiet` compares the last firing date against the last **observed sweep-date** before the
  fix, not against the `applied_date`. The applied date is itself a mixed day (see 4d), so comparing
  to it silently reclassifies the whole cohort.

### 4c. Sample size - the finding that matters most, and it is bad

Run against the live ledger snapshot of 2026-09-04T09:19:23Z (1,692 rows, 930,490 bytes):

| | Reading A: fix live on merge (`09-03`) | Reading B: fix live `09-05` |
|---|---|---|
| attributed findings | 64 | 64 |
| already silent on the last pre-fix sweep | 0 | **52** |
| still firing after the fix | 2 | 2 |
| **best case attributable** | 62 | **10** |
| **independent (DAG, PR) units** | 16 | **5** |
| earliest scoreable ledger date | `2026-09-06` (09-07 run) | `2026-09-07` (09-08 run) |

**Reading B is the defensible one, and it says the answer rests on 10 findings across 5 independent
units.** Under it the sample is: `advertiser_mid`/#1281 x3, `site_network_hourly`/#1271 x3,
`site_visit_signal_advertiser_id_dsc_id`/#1273 x2, `ipdsc_ds_2`/#1273 x1,
`advertiser_score_distribution_monitor`/#1273 x1. Findings on the same DAG under the same PR are not
independent trials - they share one config change and one workload - so the effective n is **5**,
not 60 and not even 10. Per-detector precision cannot be stated from that: `shuffle_partition_sizing`
would rest on a single DAG.

**Why 52 of 64 fall out.** They last fired on ledger date `2026-09-02`, were silent on `09-03`, and
their fix merged the evening of `09-03` (12:44-13:20 PDT = 19:44-20:20 UTC). They had already stopped
before the change could reach prod. When the `09-07` run marks them `resolved`, that resolution is an
artifact of the grace window, not evidence about the recommendation. This is verified, not inferred:
the 50 surviving `state='applied'` rows sit at file positions 1439-1488 and the `09-04` sweep's
detector rows at 1489-1691, so the sweep wrote *after* `mark_applied` and simply did not emit those
keys. The DAGs were profiled that day - `ipdsc_third_party_audience_builder` carries 14 surviving
`applied` rows alongside 2 `chronic` and 1 `recurring` - so this is real silence, not a missed crawl.

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

1. **Effective n is 5, not 60.** Findings cluster by DAG and PR. Any per-detector precision from
   this cohort is a statement about one or two DAGs wearing a detector's name.
2. **The deploy date is unverified (4d).** It moves the attributable sample between 62 and 10 and the
   scoring date between `09-07` and `09-08`. This is the single largest source of error.
3. **A resolution is not a measurement.** The ledger resolves on absence. Absence is produced by a
   working fix, an unobserved DAG, a torn sweep, a dead job, and a silent detector alike. The
   attributability gate is the whole defence and each of its rules is a heuristic with a threshold -
   `dag_went_dark` at 20% of a 5-date median baseline is a choice, not a derived constant.
4. **`partial` is not yet recorded**, so `partial_window` cannot fire on any date before `09-05`. A
   torn sweep inside the window would currently be caught only if it also made the DAG unobserved.
5. **Alignment scoring reads text.** `parse_recommendation` extracts Spark keys and targets by regex
   from prose. A recommendation phrased unusually will under-report its primary keys and can push a
   genuine `matched` toward `different_fix_worked`. Spot-check the `recommended_keys` column against
   the `fix` text before reporting any `different_fix_worked` count.
6. **Same-day mixing.** A ledger date is a whole calendar day of runs, but a fix merges mid-day. The
   applied date is neither clean pre nor clean post, and the harness therefore excludes it from both
   windows rather than assigning it.
7. **No multiplicity control.** With 5 effective units there is no power for one; equally there is
   no basis for reporting a rate with a confidence interval. Report counts, not percentages.
8. **`unscoreable` is silent on squash-merges.** #1231 has no `Merge pull request #1231` commit and
   is found only by the `(#1231)` fallback. A PR squashed with neither pattern in its subject would
   score `unscoreable` rather than erroring.

### 4f. What runs on the day

```
python3 audi_1328_score_recommendations.py --forecast-only
python3 audi_1328_score_recommendations.py --out outputs
python3 audi_1328_score_recommendations.py --effective-from <deploy date> --out outputs
```

Check first that the ledger actually holds sweep-dates `09-04`, `09-05` and `09-06` and that none is
`partial`; the header prints both. Then fill 5 and 6 with the verdict tally and the per-detector
split. Everything above this line is independent of the results.

## 5. Solution
_Pending the 2026-09-07 (or 2026-09-08) run. Fill with: the verdict tally, the per-detector split,
and the recommendation-precision statement._

## 6. Questions Answered
- **Q:** When is the earliest run that can score these fixes?
  **A:** The `2026-09-07 09:00 UTC` run, which writes ledger date `2026-09-06` - derived from
  `_mark_resolved`'s 2-date grace window and `mark_applied` stamping `date = applied_date`. If the
  config only reached prod with the `2026-09-04T23:19:30Z` Astro deploy, it is `2026-09-08` instead.
- **Q:** Is the sample big enough to conclude anything about recommendation quality?
  **A:** No. 52 of the 64 attributed findings were already silent on the last sweep before their fix
  could go live, leaving 10 findings across 5 independent (DAG, PR) units. That supports a per-fix
  narrative, not a per-detector precision rate.
- **Q:** Does merging a config fix make it live in prod?
  **A:** No. `dags/model_task_config.json` ships in the Airflow image; the prod GitHub workflow only
  copies `spark/` and the model to GCS and never touches Astro.

## 7. Data Documentation Updates
_Pending /capture at close. Candidates: the ledger resolve-window arithmetic (`_mark_resolved` grace
= last `RESOLVE_SWEEPS - 1` sweep-dates, a ledger date is written by the following day's run), and
that a merged `model_task_config.json` change is not live until an Astro deploy._

## 8. Open Items / Follow-ups
- **Settle the deploy date (4d).** Needs the Astro deploy list before `deploy-2026-09-04T23-19-30`.
  It decides both the sample size and the scoring date.
- Confirm the `09-05` sweep emits rows carrying `partial`; until then rule `partial_window` is inert.
- The `dag_went_dark` threshold (20% of a 5-date median) is unvalidated against a known-dead job.
- If the attributable sample really is 5 units, decide whether AUDI-1328 reports a sample-size
  finding and closes, rather than publishing a precision number nobody should act on.
