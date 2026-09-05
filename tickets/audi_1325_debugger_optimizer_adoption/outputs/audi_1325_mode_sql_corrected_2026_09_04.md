# Corrected Mode savings SQL (report `e81786de8403`)

**Rewritten 2026-09-05 16:05 UTC against the ledger the 09:00 UTC sweep wrote.** The 2026-09-04 draft
of this file is superseded; four defects in it are fixed below and each fix is re-run and pasted.

**The corrected headline returns zero measured savings. The published one returns 13,061.3
executor-hours / $3,631.03 / $314,382 a year.** Both were run through `bq_run.sh` against the live
external table within four minutes of each other; the results are side by side in §4.

Ledger as of this rewrite: GCS object `optimization_ledger.jsonl` written 2026-09-05T09:18:16Z,
1,875 rows, newest ledger date **2026-09-04**. `optimizer_savings.md` written 2026-09-05T09:18:28Z.
The shipped Python's own headline on that ledger is
*"No measured savings to report: 1 cleared job (1 of 3 sweep-days before the fix, 3 of 3 after)"*.
The corrected SQL reproduces that job, those counts and that sentence.

---

## 1. What changed in this rewrite

| # | Defect in the 09-04 draft | Repair |
|---|---|---|
| 1 | Run rate divided by `DATE_DIFF(CURRENT_DATE(), applied_date, DAY)`, so the number drifted away from a static ledger | divisor is now `DATE_DIFF((SELECT MAX(DATE(date)) FROM …optimization_ledger), applied_date, DAY)` in both queries |
| 2 | By-surface empty state asserted Spark savings exist | replacement text in §6 step 5 |
| 3 | 14 of 15 rows of "Fixes not yet measurable" said "the finding is still firing", which the ledger does not support | reason re-derived from ledger counts; every row now states a checkable fact |
| 4 | `dags_fixed` silently changed meaning | renamed `dags_measured`, header "DAGs measured", and the "DAGs fixed so far" card copy is addressed in §6 step 8 |

### Defect 1 in detail

`sweep.py:306` calls `ledger_mod.savings(ledger_path, today=date, …)` where `date` is the sweep's own
`--date`. `ledger.py:687-693` pins the run-rate divisor to that value:

```python
elapsed = max(
    (
        datetime.date.fromisoformat(today)
        - datetime.date.fromisoformat(r["applied_date"])
    ).days,
    1,
)
tot["rate"] += saved / elapsed
```

So the Python's divisor is *ledger date minus applied date*, frozen the moment the sweep runs. A
`CURRENT_DATE()` divisor in Mode grows every day the ledger does not, so the two surfaces disagree by
the staleness gap. The gap is not hypothetical: a sweep-day's ledger rows are written by the
*following* morning's run, so Mode is always at least one day ahead of the ledger.

`(SELECT MAX(DATE(date)) FROM …optimization_ledger)` is exactly the Python's `today`: `savings()`
defaults `today` to `max(e["date"] for e in entries)` when the caller passes nothing, and the sweep
passes its own `date`, which is the date it stamps on every row it writes.

### Defect 3 in detail

`shipped()` labels a fix `resolved`, `fix_not_working`, or `watching`. `savings()` gives a
non-`resolved` row `caveat = ""` — the Python prints **no** reason for it, and the digest's evidence
line counts only `resolved` jobs. So there was no Python wording to copy, and the draft invented one.

Re-derived from the live ledger: of the 60 `watching` findings across 14 DAGs, **53 have not fired on
any sweep-day after their applied date**; 7 have. So "still firing" is false for 53 of 60 findings
and unproven as a per-DAG statement for all 14. The 53 are quiet but inside the grace window
`_mark_resolved` enforces at `ledger.py:195` (a key that appears in either of the last
`RESOLVE_SWEEPS - 1` = 2 sweep dates cannot be resolved yet), so no sweep may call them cleared. The
repaired reason states only what the ledger holds: how many of the fix's findings are marked cleared,
how many kept firing, and how many are still awaiting a verdict.

---

## 2. The published SQL (what is live in Mode right now)

### `5a66e5fad18c` "Savings headline"

```sql
WITH daily AS (
  SELECT dag_id, COALESCE(surface, 'spark') surface, DATE(date) d, SUM(exec_h) exec_h
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger` WHERE exec_h IS NOT NULL GROUP BY 1, 2, 3
), applied AS (
  SELECT dag_id, COALESCE(surface, 'spark') surface,
    MIN(SAFE_CAST(NULLIF(applied_date, '') AS DATE)) ad
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE NULLIF(applied_date, '') IS NOT NULL GROUP BY 1, 2
), rates AS (
  SELECT a.dag_id, a.surface,
    AVG(IF(d.d < a.ad, d.exec_h, NULL)) before_rate,
    AVG(IF(d.d >= a.ad, d.exec_h, NULL)) after_rate,
    DATE_DIFF(CURRENT_DATE(), a.ad, DAY) days
  FROM applied a JOIN daily d USING (dag_id, surface) GROUP BY a.dag_id, a.surface, a.ad
)
SELECT
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0) * days, 0)), 0), 1) AS exec_hours_saved_all_time,
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0) * days, 0)) * 0.278, 0), 2) AS dollars_saved_all_time,
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0), 0)), 0), 1) AS exec_hours_saved_per_day,
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0)), 0) * 365 * 0.278, 0), 0) AS est_annual_dollars,
  (SELECT COUNT(DISTINCT dag_id) FROM applied) AS dags_with_applied_fixes
FROM rates WHERE after_rate IS NOT NULL
```

### `513a4a7a4a71` "Savings by surface"

Identical CTE block, then:

```sql
SELECT surface,
  CASE surface WHEN 'spark' THEN 'executor-hours' WHEN 'bq' THEN 'slot-hours'
       WHEN 'dbx' THEN 'DBU' ELSE 'units' END AS unit,
  COUNT(DISTINCT dag_id) AS dags_fixed,
  ROUND(SUM(GREATEST(before_rate - after_rate, 0) * days), 1) AS saved_all_time,
  ROUND(SUM(GREATEST(before_rate - after_rate, 0)), 1) AS saved_per_day
FROM rates WHERE after_rate IS NOT NULL
GROUP BY 1 ORDER BY saved_per_day DESC
```

Local copies: `queries/audi_1325_mode_savings_headline_published.sql`,
`queries/audi_1325_mode_savings_by_surface_published.sql`.

---

## 3. The corrected SQL

Both replacements open with one shared CTE block, 2,929 bytes, verified byte-identical between the
two local files. Paste it identically into both queries so the page cannot render two different
answers again.

### Shared CTE block

```sql
WITH daily AS (
  SELECT
    dag_id,
    COALESCE(surface, 'spark') AS surface,
    DATE(date) AS d,
    MAX(exec_h) AS exec_h
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE exec_h IS NOT NULL
    AND COALESCE(state, '') != 'applied'
  GROUP BY 1, 2, 3
), fix_rows AS (
  SELECT
    dag_id,
    key,
    DATE(date) AS d,
    COALESCE(surface, 'spark') AS surface,
    SAFE_CAST(NULLIF(applied_date, '') AS DATE) AS applied_date,
    state
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE NULLIF(fix_pr, '') IS NOT NULL
), shipped AS (
  SELECT
    dag_id,
    key,
    ARRAY_AGG(surface ORDER BY d LIMIT 1)[OFFSET(0)] AS surface,
    ARRAY_AGG(applied_date IGNORE NULLS ORDER BY d DESC LIMIT 1)[SAFE_OFFSET(0)] AS applied_date,
    COALESCE(
      ARRAY_AGG(IF(state IN ('resolved', 'fix_not_working'), state, NULL)
                IGNORE NULLS ORDER BY d DESC LIMIT 1)[SAFE_OFFSET(0)],
      'watching') AS outcome
  FROM fix_rows
  GROUP BY 1, 2
), jobs AS (
  SELECT dag_id, surface, MAX(applied_date) AS applied_date
  FROM shipped
  WHERE outcome = 'resolved' AND applied_date IS NOT NULL
  GROUP BY 1, 2
), rates AS (
  SELECT
    j.dag_id,
    j.surface,
    j.applied_date,
    COUNTIF(d.d < j.applied_date) AS before_days,
    COUNTIF(d.d > j.applied_date) AS after_days,
    AVG(IF(d.d < j.applied_date, d.exec_h, NULL)) AS before_rate,
    AVG(IF(d.d > j.applied_date, d.exec_h, NULL)) AS after_rate,
    VAR_SAMP(IF(d.d < j.applied_date, d.exec_h, NULL)) AS var_before,
    VAR_SAMP(IF(d.d > j.applied_date, d.exec_h, NULL)) AS var_after
  FROM jobs j
  JOIN daily d USING (dag_id, surface)
  GROUP BY 1, 2, 3
), gated AS (
  SELECT
    *,
    (before_rate - after_rate) * after_days AS exec_h_saved,
    SAFE.SQRT(var_before / before_days + var_after / after_days) AS se,
    SAFE_DIVIDE(
      POW(var_before / before_days + var_after / after_days, 2),
      POW(var_before / before_days, 2) / (before_days - 1)
      + POW(var_after / after_days, 2) / (after_days - 1)) AS df
  FROM rates
  WHERE before_days >= 3 AND after_days >= 3
), scored AS (
  SELECT
    g.*,
    COALESCE(
      (SELECT t.lo_v + (g.df - t.lo) / (t.hi - t.lo) * (t.hi_v - t.lo_v)
       FROM UNNEST([
         STRUCT(1 AS lo, 2 AS hi, 6.314 AS lo_v, 2.920 AS hi_v),
         STRUCT(2, 3, 2.920, 2.353), STRUCT(3, 4, 2.353, 2.132),
         STRUCT(4, 5, 2.132, 2.015), STRUCT(5, 6, 2.015, 1.943),
         STRUCT(6, 7, 1.943, 1.895), STRUCT(7, 8, 1.895, 1.860),
         STRUCT(8, 9, 1.860, 1.833), STRUCT(9, 10, 1.833, 1.812),
         STRUCT(10, 12, 1.812, 1.782), STRUCT(12, 15, 1.782, 1.753),
         STRUCT(15, 20, 1.753, 1.725), STRUCT(20, 30, 1.725, 1.697),
         STRUCT(30, 60, 1.697, 1.671)]) t
       WHERE g.df BETWEEN t.lo AND t.hi
       ORDER BY t.lo
       LIMIT 1),
      IF(g.df <= 1, 6.314, 1.645)) * g.se * g.after_days AS half
  FROM gated g
  WHERE var_before > 0 AND var_after > 0
)
```

### `5a66e5fad18c` "Savings headline" — corrected tail

```sql
SELECT
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved, 0)), 0), 1) AS exec_hours_saved_all_time,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved, 0))
               - SQRT(IFNULL(SUM(IF(surface = 'spark', POW(half, 2), 0)), 0)), 0), 1)
    AS exec_hours_saved_ci_low,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved, 0))
               + SQRT(IFNULL(SUM(IF(surface = 'spark', POW(half, 2), 0)), 0)), 0), 1)
    AS exec_hours_saved_ci_high,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved, 0)), 0) * 0.278, 2) AS dollars_saved_all_time,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved / GREATEST(DATE_DIFF(
    (SELECT MAX(DATE(date)) FROM `mntn-prj-prod-00.optimizer.optimization_ledger`),
    applied_date, DAY), 1), 0)), 0), 1)
    AS exec_hours_saved_per_day,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved / GREATEST(DATE_DIFF(
    (SELECT MAX(DATE(date)) FROM `mntn-prj-prod-00.optimizer.optimization_ledger`),
    applied_date, DAY), 1), 0)), 0)
    * 365 * 0.278, 0) AS est_annual_dollars,
  COUNTIF(surface = 'spark') AS dags_measured,
  COUNTIF(surface = 'spark' AND exec_h_saved - half > 0) AS dags_measured_interval_clear_of_zero,
  (SELECT COUNT(DISTINCT dag_id) FROM shipped WHERE applied_date IS NOT NULL)
    AS dags_with_applied_fixes,
  (SELECT COUNT(DISTINCT FORMAT('%s|%s', dag_id, surface)) FROM shipped
   WHERE applied_date IS NOT NULL) - (SELECT COUNT(*) FROM scored)
    AS dags_not_yet_measurable,
  (SELECT MAX(DATE(date)) FROM `mntn-prj-prod-00.optimizer.optimization_ledger`)
    AS ledger_through,
  0.278 AS usd_per_exec_h,
  'fixed assumption, not derived live' AS usd_rate_basis,
  DATE '2026-08-27' AS usd_rate_measured_on
FROM scored
```

`ledger_through` is new: it is the date every number on the page is computed through, and the layout
should print it so a stale render is visible rather than silent.

### `513a4a7a4a71` "Savings by surface" — corrected tail

```sql
SELECT
  surface,
  CASE surface WHEN 'spark' THEN 'executor-hours' WHEN 'bq' THEN 'slot-hours'
       WHEN 'dbx' THEN 'DBU' ELSE 'units' END AS unit,
  COUNT(DISTINCT dag_id) AS dags_measured,
  ROUND(SUM(exec_h_saved), 1) AS saved_all_time,
  ROUND(SUM(exec_h_saved) - SQRT(SUM(POW(half, 2))), 1) AS saved_all_time_ci_low,
  ROUND(SUM(exec_h_saved) + SQRT(SUM(POW(half, 2))), 1) AS saved_all_time_ci_high,
  ROUND(SUM(exec_h_saved / GREATEST(DATE_DIFF(
    (SELECT MAX(DATE(date)) FROM `mntn-prj-prod-00.optimizer.optimization_ledger`),
    applied_date, DAY), 1)), 1) AS saved_per_day
FROM scored
GROUP BY 1
ORDER BY saved_per_day DESC
```

`dags_fixed` is gone. `scored` holds only jobs that cleared the evidence gate, so the count is DAGs
**measured**, not DAGs fixed, and it must not keep the old name or the old header. It is the same
quantity the headline returns as `dags_measured`; the two now agree by name.

### "Fixes not yet measurable" — repaired reasons

Standalone, does not share the block above (it must keep the jobs the block filters out). Full text:
`queries/audi_1325_mode_not_yet_measurable.sql`. The reason ladder:

```sql
  CASE
    WHEN cleared = 0 AND still_firing = findings
      THEN 'every finding this fix covers kept firing afterwards, so there is nothing cleared to measure'
    WHEN cleared = 0
      THEN FORMAT(
        'no finding this fix covers is marked cleared yet: %d of %d %s still awaiting a verdict',
        undecided, findings, IF(undecided = 1, 'is', 'are'))
    WHEN before_days < 3 OR after_days < 3
      THEN FORMAT('%d of 3 sweep-days before the fix, %d of 3 after', before_days, after_days)
    WHEN COALESCE(var_before, 0) = 0 OR COALESCE(var_after, 0) = 0
      THEN "the job's hours never varied, so the difference carries no interval"
    ELSE 'measured'
  END AS why_not_measurable
```

`cleared`, `still_firing` and `undecided` are the per-DAG counts of `shipped.outcome` values
`resolved`, `fix_not_working` and `watching`, and they are returned as columns so the sentence can be
checked against the numbers beside it. The last two branches are the shipped Python's own strings:
`_too_thin` at `ledger.py:578-585` and its zero-variance return, quoted character for character
including the apostrophe in "job's".

---

## 4. Run today, 2026-09-05 16:02-16:05 UTC

All runs: `bash .claude/scripts/bq_run.sh --project_id=dw-main-bronze --location=us-central1
--use_legacy_sql=false --nouse_cache --format=csv "$(cat <file>)"`, against the live external table
(1,875 rows read, unchanged across every run below).

### Headline

```
exec_hours_saved_all_time,dollars_saved_all_time,exec_hours_saved_per_day,est_annual_dollars,dags_with_applied_fixes
13061.3,3631.03,3098.3,314382.0,15
```
published `5a66e5fad18c`

```
exec_hours_saved_all_time,exec_hours_saved_ci_low,exec_hours_saved_ci_high,dollars_saved_all_time,exec_hours_saved_per_day,est_annual_dollars,dags_measured,dags_measured_interval_clear_of_zero,dags_with_applied_fixes,dags_not_yet_measurable,ledger_through,usd_per_exec_h,usd_rate_basis,usd_rate_measured_on
0.0,0.0,0.0,0.0,0.0,0.0,0,0,15,15,2026-09-04,0.278,"fixed assumption, not derived live",2026-08-27
```
corrected

| Column | Published | Corrected |
|---|---:|---:|
| `exec_hours_saved_all_time` | 13,061.3 | **0.0** |
| `dollars_saved_all_time` | 3,631.03 | **0.00** |
| `exec_hours_saved_per_day` | 3,098.3 | **0.0** |
| `est_annual_dollars` | 314,382 | **0** |
| `dags_with_applied_fixes` | 15 | 15 |
| `dags_measured` | — | 0 |
| `ledger_through` | — | 2026-09-04 |

### By surface

Two consecutive uncached runs, 16:03:07Z and 16:03:12Z: **0 rows** both times. Published
`513a4a7a4a71` on the same ledger returns one row: `spark,executor-hours,15,13061.3,3098.3`.

### Defect 1 proof — the corrected queries do not drift

The ledger did not change between these runs; neither did the answer.

```
== HEADLINE nocache run 1 16:04:01Z ==
0.0,0.0,0.0,0.0,0.0,0.0,0,0,15,15,2026-09-04,0.278,"fixed assumption, not derived live",2026-08-27
== HEADLINE nocache run 2 16:04:05Z ==
0.0,0.0,0.0,0.0,0.0,0.0,0,0,15,15,2026-09-04,0.278,"fixed assumption, not derived live",2026-08-27
```

Two identical zeros do not on their own exercise the divisor, so
`queries/audi_1325_run_rate_divisor_check.sql` prints both divisors against every applied date in
the ledger, and what each does to a run rate on a nominal 100 saved hours:

```
applied_date,ledger_through,query_run_date,elapsed_days_from_ledger,elapsed_days_from_current_date,per_day_from_ledger_on_100h,per_day_from_current_date_on_100h
2026-08-27,2026-09-04,2026-09-05,8,9,12.5,11.1111
2026-09-03,2026-09-04,2026-09-05,1,2,100.0,50.0
```

`elapsed_days_from_ledger` is the repaired divisor; `elapsed_days_from_current_date` is the one the
09-04 draft shipped. The shipped Python, run against this same ledger file, computes:

```
python elapsed today=2026-09-04 applied=2026-08-27 -> 8
python elapsed today=2026-09-04 applied=2026-09-03 -> 1
```

The repaired divisor matches the Python exactly. The old one is already wrong by a factor of two on
the 2026-09-03 cohort, and the error grows every day the sweep does not land.

### Fixes not yet measurable — 15 rows, every reason re-derived

```
dag_id,surface,applied_date,findings_the_fix_covers,findings_the_ledger_marks_cleared,findings_that_kept_firing_after_the_fix,findings_with_no_post_fix_verdict_yet,sweep_days_before_the_fix,sweep_days_after_the_fix,mean_hours_per_day_before,mean_hours_per_day_after,why_not_measurable
advertiser_mid,spark,2026-09-03,4,0,0,4,4,1,16.5,15.7,no finding this fix covers is marked cleared yet: 4 of 4 are still awaiting a verdict
advertiser_score_distribution_monitor,spark,2026-09-03,1,0,0,1,3,1,33.6,42.9,no finding this fix covers is marked cleared yet: 1 of 1 is still awaiting a verdict
conv_log_derived_ip,spark,2026-09-03,1,0,0,1,3,1,0.5,0.4,no finding this fix covers is marked cleared yet: 1 of 1 is still awaiting a verdict
conversion_log_advertiser_id_dsc_id,spark,2026-09-03,4,0,0,4,3,1,80.2,37.2,no finding this fix covers is marked cleared yet: 4 of 4 are still awaiting a verdict
guid_conv_log_pivot_ip_vertical_id,spark,2026-09-03,1,0,0,1,4,1,7.3,10.3,no finding this fix covers is marked cleared yet: 1 of 1 is still awaiting a verdict
guid_log_advertiser_id_dsc_id,spark,2026-09-03,4,0,0,4,3,1,92.3,43.8,no finding this fix covers is marked cleared yet: 4 of 4 are still awaiting a verdict
guid_log_pivot_ip_vertical_id,spark,2026-09-03,1,0,0,1,3,1,7.6,10.0,no finding this fix covers is marked cleared yet: 1 of 1 is still awaiting a verdict
ipdsc_42_monitor,spark,2026-09-03,6,0,0,6,4,1,1.5,0.2,no finding this fix covers is marked cleared yet: 6 of 6 are still awaiting a verdict
ipdsc_ds_2,spark,2026-09-03,2,0,0,2,3,1,30.8,30.4,no finding this fix covers is marked cleared yet: 2 of 2 are still awaiting a verdict
ipdsc_ds_49,spark,2026-09-03,1,0,0,1,3,1,2.9,5.3,no finding this fix covers is marked cleared yet: 1 of 1 is still awaiting a verdict
ipdsc_third_party_audience_builder,spark,2026-09-03,14,0,0,14,2,1,44.7,41.0,no finding this fix covers is marked cleared yet: 14 of 14 are still awaiting a verdict
site_network_hourly,spark,2026-09-03,14,0,0,14,4,1,970.9,6581.7,no finding this fix covers is marked cleared yet: 14 of 14 are still awaiting a verdict
site_visit_signal_advertiser_id_dsc_id,spark,2026-09-03,4,0,0,4,3,1,126.6,55.0,no finding this fix covers is marked cleared yet: 4 of 4 are still awaiting a verdict
vertical_size_monitor,spark,2026-09-03,3,0,0,3,3,0,4.4,,no finding this fix covers is marked cleared yet: 3 of 3 are still awaiting a verdict
fangorn_score_monitor,spark,2026-08-27,4,2,2,0,1,3,687.7,621.2,"1 of 3 sweep-days before the fix, 3 of 3 after"
```

Cross-checked against `savings()` at commit `016e161` run over the same ledger file: the 14 DAGs
applied 2026-09-03 carry outcome `watching` on all 60 findings and `caveat = ""`, and
`fangorn_score_monitor` carries 2 `resolved` and 2 `fix_not_working` findings with the caveat
"1 of 3 sweep-days before the fix, 3 of 3 after". The last row's sentence is byte-identical to the
evidence line in this morning's `optimizer_savings.md`.

`site_network_hourly` shows 970.9 hours/day before the fix and 6,581.7 after. That is one sweep-day
of after-data, and the row says so; it is not a measured regression.

### The gate machinery computes, it does not just return empty

`queries/audi_1325_welch_parity_check.sql` swaps `daily` and `jobs` for an inline synthetic series
(before 100, 110, 90, 105; after 60, 55, 70; applied 2026-08-09) and leaves `gated`/`scored`
untouched. Re-run today:

```
dag_id,before_days,after_days,before_rate,after_rate,df,se,exec_h_saved,half,proven
synth,4,3,101.25,61.6667,4.73399,6.137883,118.75,37.6766,true
```

| | SQL | `ledger._delta_ci` at 016e161 |
|---|---:|---:|
| `se` | 6.137883 | 6.137883 |
| `df` | 4.73399 | 4.73399 |
| `exec_h_saved` | 118.75 | 118.75 |
| `half` | 37.6766 | 37.6766 |
| `proven` | true | true |

So the zero on the live ledger is the evidence gate firing, not a broken query.

### Decomposition ladder, re-run on today's ledger

`queries/audi_1325_savings_ladder_decomposition.sql`:

```
step0_published_method,step1_max_only,step2_signed_only,step3_all_aggregation_fixes_ungated,step3_alt_sum_over_app_id,jobs_with_any_after_day
13061.3,255.3,-95123.0,-5259.2,-33029.9,14
```

`MAX` instead of `SUM` alone takes the published 13,061.3 down to 255.3. Signing the delta instead
of flooring it at zero gives -95,123.0. All aggregation fixes with no evidence gate: -5,259.2. The
2026-09-04 figures (5,163.0 / 174.2 / -125,734.2 / 114.8) were correct for the 1,692-row ledger and
are superseded, not contradicted.

---

## 5. Why the other choices stand

**Why `MAX` and not `SUM` over distinct `app_id`.** `record()` computes `exec_h_by_dag` as the DAG's
total across every run the sweep saw, then stamps that one value on every row it writes for that DAG
that day. When a sweep sees several apps, all its rows carry the *same* number, so summing over
`app_id` double-counts. On today's ledger that alternative gives -33,029.9 against -5,259.2. `MAX` is
also what the shipped Python does (`day[d] = max(day.get(d, 0.0), e["exec_h"])`), which is the point:
the two surfaces must agree.

**Why `DATE_DIFF` survives in the run rate.** As a *multiplier* it inflates a total with elapsed
time, which is the audit's defect 3 and is gone. As a *divisor* it is the Python's own definition
and decays with time. What was wrong in the 09-04 draft was the endpoint, not the operation, and §1
fixes the endpoint.

**Why the dollar rate stays a literal.** `mntn-prj-prod-00:optimizer` contains exactly one table, the
external ledger, and no row carries a rate, dollar or cost field: the `Entry` dataclass at `016e161`
has 18 fields, none monetary. The only live rate lives in `include/spark_optimizer/billing.py`, which
reads `mntn-billing-00.gcp_cloud_billing_standard.gcp_billing_export_v1_01E62F_CDF2FC_8AC7A4` in
another project behind a dataset-scoped grant (mntn-devops#5121) the Mode service account does not
hold. So the literal stays and the query states it: `usd_per_exec_h` 0.278, `usd_rate_basis` "fixed
assumption, not derived live", `usd_rate_measured_on` 2026-08-27 (billing.py's own verification date,
with `DCU_PER_EXEC_H = 5.44`).

**Where this still differs from the Python.** The Python admits a job to the total only when its 90%
interval is clear of zero, so a regression contributes nothing. This SQL admits every job that clears
the evidence gate and sums the signed delta, so a regression counts against the total, as the audit
requires. Both are zero today. They can diverge on a job with 3 sweep-days each side whose interval
spans zero: it counts in Mode's total but not in the digest's.
`dags_measured_interval_clear_of_zero` and the CI columns are on the result so the difference is
visible rather than silent.

---

## 6. Exactly what a human must paste, where

1. Open Mode report `e81786de8403` → **Query editor → "Savings headline"** (`5a66e5fad18c`). Select
   all, delete, paste `queries/audi_1325_mode_savings_headline_corrected.sql` whole. Save.
2. Open **"Savings by surface"** (`513a4a7a4a71`). Select all, delete, paste
   `queries/audi_1325_mode_savings_by_surface_corrected.sql` whole. Save. Its first 2,929 bytes are
   byte-identical to step 1's; if you edit one CTE block, edit both.
3. **Add a new query** named exactly `Fixes not yet measurable`, pasting
   `queries/audi_1325_mode_not_yet_measurable.sql`. Save.
4. **Run the report** and confirm: $0 saved all-time, $0 est. annual, 0 executor-hours, 0.0/day,
   `dags_measured` 0, `dags_with_applied_fixes` 15, `ledger_through` 2026-09-04, and "Savings by
   surface" showing its empty state rather than a row.
5. **Replace the "Savings by surface" empty-state paragraph** (layout offset ~9489). It currently
   reads *"No measured savings outside Spark yet; BigQuery and Databricks rows start with the next
   daily sweep."* That sentence asserts Spark savings exist, and it now renders beside $0 cards.
   Replace with:

   > No surface has a measured saving yet. A fix reaches this table once the ledger marks its finding
   > cleared and holds three sweep-days on each side of the applied date; "Fixes not yet measurable"
   > below lists every shipped fix and what each one is still short of.

6. **Edit the layout to render the new query.** `window.datasets` exposes it as
   `ds('Fixes not yet measurable')`; the columns are `dag_id`, `surface`, `applied_date`,
   `findings_the_fix_covers`, `findings_the_ledger_marks_cleared`,
   `findings_that_kept_firing_after_the_fix`, `findings_with_no_post_fix_verdict_yet`,
   `sweep_days_before_the_fix`, `sweep_days_after_the_fix`, `mean_hours_per_day_before`,
   `mean_hours_per_day_after`, `why_not_measurable`. Without this step the corrected report shows
   zeros with no explanation of why.
7. **Edit the hero paragraph** (layout offset ~3263), which reads "Dollars use the blended rate from
   actual Dataproc spend, $0.278 per executor-hour". Replace with the fixed-assumption wording and
   read `usd_per_exec_h`, `usd_rate_basis` and `usd_rate_measured_on` off the headline query instead
   of hard-coding the sentence.
8. **Retitle the "DAGs fixed so far" card.** It reads 15, subtitle "of 33 Spark DAGs profiled", and
   it now sits beside a $0 headline, so it reads as a contradiction. 15 is `dags_with_applied_fixes`
   and is still true; it counts fixes shipped, not savings measured. Retitle it "DAGs with a fix
   shipped" and put `dags_measured` (0) beside it, so the page states both numbers rather than
   letting one imply the other.
9. **Print `ledger_through`** somewhere on the page. Every figure is computed through that date, and
   a render whose ledger is days stale is otherwise invisible.
10. **Recreate the external table** with the §7 command, from a shell that can write to
    `mntn-prj-prod-00`. Then run the §7 verification.
11. **Only after step 10**, add `AND NOT COALESCE(partial, FALSE)` to the `daily` CTE `WHERE` clause
    in both queries, matching `ledger.py:643`'s partial-sweep exclusion. Do not add it before step
    10: `partial` is not in the current table schema and the queries fail with "Unrecognized name".
12. **Fix the schedule** (audit defect 8, token `d30b701e413d`): move `cron_hour` from 6 to 10 UTC so
    the unattended render reads the same day's sweep. Schedules are UI-only; the API rejects its own
    documented payloads.

Steps 1-9 are Mode edits and need no BigQuery permissions. Step 10 needs write access to the
`mntn-prj-prod-00:optimizer` dataset. Steps 1-3 are the ones that change the published number.

---

## 7. Recreate the external table

**Do not run this blind — it rebuilds a table six live Mode queries read.** The live schema, read
today with `bq show --schema`, still holds 16 fields ending at `surface`.

**`partial` is now confirmed live in the data and confirmed invisible to SQL.** The 2026-09-05 sweep
wrote it on all 183 rows it appended (ledger date 2026-09-04), every one `false`. It is absent from
the table schema, so BigQuery drops it silently under `ignoreUnknownValues: true` — the same failure
`prev_exec_h` has had all along: the key is on 964 of the 1,875 GCS rows, non-null on 510, and
readable in none of them.

Coverage check, run today against the live GCS object and the `Entry` dataclass at `016e161`:

```
Entry fields (016e161): 18 ['date', 'dag_id', 'app_id', 'key', 'impact', 'title', 'fix', 'owner', 'dcu_h', 'exec_h', 'prev_exec_h', 'state', 'streak', 'note', 'fix_pr', 'applied_date', 'surface', 'partial']
definition fields     : 18 ['date', 'dag_id', 'app_id', 'key', 'impact', 'title', 'fix', 'owner', 'dcu_h', 'exec_h', 'prev_exec_h', 'state', 'streak', 'note', 'fix_pr', 'applied_date', 'surface', 'partial']
order identical       : True
GCS keys not in defn  : []
defn fields never in GCS: []
partial rows in GCS   : 183
```

The definition at `artifacts/audi_1325_optimization_ledger_external_def.json` needs no change:

```json
{
  "sourceFormat": "NEWLINE_DELIMITED_JSON",
  "sourceUris": ["gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl"],
  "autodetect": false,
  "ignoreUnknownValues": true,
  "jsonOptions": {"encoding": "UTF-8"},
  "schema": {
    "fields": [
      {"name": "date",         "type": "DATE",    "mode": "NULLABLE"},
      {"name": "dag_id",       "type": "STRING",  "mode": "NULLABLE"},
      {"name": "app_id",       "type": "STRING",  "mode": "NULLABLE"},
      {"name": "key",          "type": "STRING",  "mode": "NULLABLE"},
      {"name": "impact",       "type": "STRING",  "mode": "NULLABLE"},
      {"name": "title",        "type": "STRING",  "mode": "NULLABLE"},
      {"name": "fix",          "type": "STRING",  "mode": "NULLABLE"},
      {"name": "owner",        "type": "STRING",  "mode": "NULLABLE"},
      {"name": "dcu_h",        "type": "FLOAT",   "mode": "NULLABLE"},
      {"name": "exec_h",       "type": "FLOAT",   "mode": "NULLABLE"},
      {"name": "prev_exec_h",  "type": "FLOAT",   "mode": "NULLABLE"},
      {"name": "state",        "type": "STRING",  "mode": "NULLABLE"},
      {"name": "streak",       "type": "INTEGER", "mode": "NULLABLE"},
      {"name": "note",         "type": "STRING",  "mode": "NULLABLE"},
      {"name": "fix_pr",       "type": "STRING",  "mode": "NULLABLE"},
      {"name": "applied_date", "type": "STRING",  "mode": "NULLABLE"},
      {"name": "surface",      "type": "STRING",  "mode": "NULLABLE"},
      {"name": "partial",      "type": "BOOLEAN", "mode": "NULLABLE"}
    ]
  }
}
```

Command — update in place, no drop window:

```bash
bq --location=us-central1 update \
  --external_table_definition=/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1325_debugger_optimizer_adoption/artifacts/audi_1325_optimization_ledger_external_def.json \
  mntn-prj-prod-00:optimizer.optimization_ledger
```

If `update` refuses the definition, drop and recreate instead:

```bash
bq --location=us-central1 rm -f -t mntn-prj-prod-00:optimizer.optimization_ledger
bq --location=us-central1 mk --table \
  --external_table_definition=/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1325_debugger_optimizer_adoption/artifacts/audi_1325_optimization_ledger_external_def.json \
  mntn-prj-prod-00:optimizer.optimization_ledger
```

Verify:

```bash
bq show --schema --format=prettyjson mntn-prj-prod-00:optimizer.optimization_ledger
bash .claude/scripts/bq_run.sh --project_id=dw-main-bronze --location=us-central1 \
  "SELECT COUNT(*) rows, COUNTIF(prev_exec_h IS NOT NULL) prev, COUNTIF(partial IS NOT NULL) part
   FROM \`mntn-prj-prod-00.optimizer.optimization_ledger\` LIMIT 1"
```

Expect 1,875 rows, 510 non-null `prev_exec_h` and 183 non-null `partial` on today's object; all
three grow with each sweep.

Three notes on the choices:

- `applied_date` stays `STRING`. The ledger writes `""` for "not applied", which is not a valid
  `DATE`; typing it `DATE` would fail every row. The corrected SQL casts it with
  `SAFE_CAST(NULLIF(applied_date, '') AS DATE)`.
- `dcu_h` changes `STRING` → `FLOAT`. Autodetect typed it `STRING` only because every row is null
  (still 0 of 1,875 non-null today). No Mode query on the report references it.
- `ignoreUnknownValues` stays `true`. With a full explicit schema the silent-drop failure mode
  returns only when the Python gains a *new* field, and failing open beats hard-failing every
  dashboard query. The cost is that this file must be updated whenever `Entry` gains a field.
  Setting it `false` makes that omission loud instead; it is a real option if someone owns the
  breakage. The AUDI-1330 branch (`909ce80`) adds no `Entry` field, so this definition covers it too.
