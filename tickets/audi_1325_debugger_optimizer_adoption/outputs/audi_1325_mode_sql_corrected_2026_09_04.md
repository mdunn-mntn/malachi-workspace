# Corrected Mode savings SQL (report `e81786de8403`) — 2026-09-04

**The corrected headline returns zero measured savings on today's ledger. The published one returns 5,163.0 executor-hours / $1,435.31.** Both were run through `bq_run.sh` against the live external table within four minutes of each other; the results are side by side below.

Every defect the audit lists is fixed. The audit's arithmetic was re-derived independently here and reproduces exactly at every step (5,163.0 → 174.2 → -125,734.2 → 114.8, 1 scoreable job). Nothing in the audit was found wrong.

Query text was pulled live from the Mode API this session (`GET /api/mntn/reports/e81786de8403/queries`, both queries `updated_at` 2026-09-04T23:10:55Z). The daily/applied/rates CTE block of `5a66e5fad18c` and `513a4a7a4a71` is byte-identical today — verified, the first 727 characters of both match exactly.

---

## 1. The published SQL

### `5a66e5fad18c` "Savings headline" (as it stands now)

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
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0), 0)) * 365 * 0.278, 0), 0) AS est_annual_dollars,
  (SELECT COUNT(DISTINCT dag_id) FROM applied) AS dags_with_applied_fixes
FROM rates WHERE after_rate IS NOT NULL
```

### `513a4a7a4a71` "Savings by surface" (as it stands now)

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

Local copies: `queries/audi_1325_mode_savings_headline_published.sql`, `queries/audi_1325_mode_savings_by_surface_published.sql`.

---

## 2. The corrected SQL

Both replacements open with one shared CTE block. Paste it identically into both queries so the page cannot render two different answers again.

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
  ROUND(IFNULL(SUM(IF(surface = 'spark',
    exec_h_saved / GREATEST(DATE_DIFF(CURRENT_DATE(), applied_date, DAY), 1), 0)), 0), 1)
    AS exec_hours_saved_per_day,
  ROUND(IFNULL(SUM(IF(surface = 'spark',
    exec_h_saved / GREATEST(DATE_DIFF(CURRENT_DATE(), applied_date, DAY), 1), 0)), 0)
    * 365 * 0.278, 0) AS est_annual_dollars,
  COUNTIF(surface = 'spark') AS dags_measured,
  COUNTIF(surface = 'spark' AND exec_h_saved - half > 0) AS dags_measured_interval_clear_of_zero,
  (SELECT COUNT(DISTINCT dag_id) FROM shipped WHERE applied_date IS NOT NULL)
    AS dags_with_applied_fixes,
  (SELECT COUNT(DISTINCT FORMAT('%s|%s', dag_id, surface)) FROM shipped
   WHERE applied_date IS NOT NULL) - (SELECT COUNT(*) FROM scored)
    AS dags_not_yet_measurable,
  0.278 AS usd_per_exec_h,
  'fixed assumption, not derived live' AS usd_rate_basis,
  DATE '2026-08-27' AS usd_rate_measured_on
FROM scored
```

### `513a4a7a4a71` "Savings by surface" — corrected tail

```sql
SELECT
  surface,
  CASE surface WHEN 'spark' THEN 'executor-hours' WHEN 'bq' THEN 'slot-hours'
       WHEN 'dbx' THEN 'DBU' ELSE 'units' END AS unit,
  COUNT(DISTINCT dag_id) AS dags_fixed,
  ROUND(SUM(exec_h_saved), 1) AS saved_all_time,
  ROUND(SUM(exec_h_saved) - SQRT(SUM(POW(half, 2))), 1) AS saved_all_time_ci_low,
  ROUND(SUM(exec_h_saved) + SQRT(SUM(POW(half, 2))), 1) AS saved_all_time_ci_high,
  ROUND(SUM(exec_h_saved / GREATEST(DATE_DIFF(CURRENT_DATE(), applied_date, DAY), 1)), 1)
    AS saved_per_day
FROM scored
GROUP BY 1
ORDER BY saved_per_day DESC
```

### New query: "Fixes not yet measurable"

Standalone, does not share the block above (it must keep the jobs the block filters out). Full text: `queries/audi_1325_mode_not_yet_measurable.sql`. It returns one row per shipped fix with the reason it carries no number, in the same words the Python digest prints.

---

## 3. What each fix does, and why

| # | Defect | Correction | Verified |
|---|---|---|---|
| 1 | `SUM(exec_h)` per DAG-day | `MAX(exec_h)` | `MAX` alone takes the headline 5,163.0 → 174.2 |
| 2 | `state='applied'` clone rows enter the rates | `AND COALESCE(state,'') != 'applied'` | drops 54 rows carrying 42,172.9 hours; `daily` goes 409 → 403 rows |
| 3 | `DATE_DIFF(CURRENT_DATE(), ad, DAY)` multiplier | `COUNTIF(d.d > applied_date)` as `after_days` | headline no longer moves on a day with no new data |
| 4 | `GREATEST(..., 0)` floor | signed `(before_rate - after_rate) * after_days` | signed sum on the published aggregation is -125,734.2 |
| 5 | after-window `d >= ad` | `d > applied_date` | matches `ledger.py` at 016e161 (`d > r["applied_date"]`) |
| 6 | no evidence gate | `outcome='resolved'` + `before_days >= 3` + `after_days >= 3` + non-zero variance both sides, plus a 90% Welch interval | 0 jobs qualify today |
| 7 | frozen `0.278` presented as live | literal kept, returned as `usd_per_exec_h` / `usd_rate_basis` / `usd_rate_measured_on` | the ledger carries no rate column; see below |

**Why `MAX` and not `SUM` over distinct `app_id`.** Multi-app DAG-days do occur, but summing them double-counts, badly. `record()` computes `exec_h_by_dag` as the DAG's total across every run the sweep saw, then stamps that one value on every row it writes for that DAG that day. So when a sweep sees several apps, all its rows carry the *same* number. Live proof: `site_network_hourly` on 2026-09-02 has four app_ids all carrying 3,653.1 and five more all carrying 3,238.8 (two sweeps' totals, each stamped across its apps). Summing over distinct app_id gives 30,806.4 executor-hours for a DAG-day whose true total is at most 3,653.1. Across the whole ledger the alternative flips the ungated figure from +114.8 to -1,805.0. 22 DAG-days show two or more app_ids sharing one `exec_h` value. `MAX` is also what the shipped Python does (`day[d] = max(day.get(d, 0.0), e["exec_h"])`), which is the point: the two surfaces must agree.

**Why `DATE_DIFF` survives in the run-rate.** Defect 3 is about `DATE_DIFF` as a *multiplier*, where elapsed time inflates the total. In the run rate it is a *divisor* (`saved / elapsed`), which is the shipped Python's own definition and decays with time rather than growing. Removing it would make the rate a per-sweep-day delta instead of a run rate and would disagree with the digest.

**Why the dollar rate stays a literal.** `mntn-prj-prod-00:optimizer` contains exactly one table, the external ledger. The ledger row carries no rate, dollar or cost field on either side of PR #1286 (the `Entry` dataclass at 016e161 has 18 fields, none monetary). The only live rate lives in `include/spark_optimizer/billing.py`, which reads `mntn-billing-00.gcp_cloud_billing_standard.gcp_billing_export_v1_01E62F_CDF2FC_8AC7A4` in another project behind a dataset-scoped grant (mntn-devops#5121) that the Mode service account does not hold. So the literal stays and the query now states it: `usd_per_exec_h` 0.278, `usd_rate_basis` "fixed assumption, not derived live", `usd_rate_measured_on` 2026-08-27 (billing.py's own verification date, with `DCU_PER_EXEC_H = 5.44`). Item (a) of audit defect 10 — the hero text claiming a live derivation — is a layout edit, not a SQL edit, and is not addressed here.

**Where this still differs from the Python.** The Python admits a job to the total only when its 90% interval is clear of zero, so a regression contributes nothing. This SQL admits every job that clears the evidence gate and sums the signed delta, so a regression counts against the total, as the audit requires. Both are zero today. When they can diverge: a job with 3 sweep-days each side whose interval spans zero counts in Mode's total but not in the digest's. `dags_measured_interval_clear_of_zero` and the CI columns are on the result so the difference is visible rather than silent.

---

## 4. Side by side, run today

Both run through `.claude/scripts/bq_run.sh --project_id=dw-main-bronze --location=us-central1` against `mntn-prj-prod-00.optimizer.optimization_ledger` (1,692 rows, GCS object written 2026-09-04T09:19:23Z, unchanged between runs). Wall clock 2026-09-05T02:56Z; `CURRENT_DATE()` = 2026-09-05.

### Headline

| Column | Published `5a66e5fad18c` | Corrected |
|---|---:|---:|
| `exec_hours_saved_all_time` | 5,163.0 | **0.0** |
| `dollars_saved_all_time` | 1,435.31 | **0.00** |
| `exec_hours_saved_per_day` | 1,707.8 | **0.0** |
| `est_annual_dollars` | 173,289 | **0** |
| `dags_with_applied_fixes` | 15 | 15 |
| `exec_hours_saved_ci_low` / `_ci_high` | — | 0.0 / 0.0 |
| `dags_measured` | — | 0 |
| `dags_measured_interval_clear_of_zero` | — | 0 |
| `dags_not_yet_measurable` | — | 15 |
| `usd_per_exec_h` | — | 0.278 |
| `usd_rate_basis` | — | fixed assumption, not derived live |
| `usd_rate_measured_on` | — | 2026-08-27 |

The published run reproduces the audit's refresh figure (5,163.0 / $1,435.31) exactly. Note it is not the 3,455.2 on screen: the screen figure is one calendar day older, and the gap is the audit's defect 3 accruing at +1,707.8 hours per day.

### By surface

| | Published `513a4a7a4a71` | Corrected |
|---|---|---|
| rows | 1 (`spark`, executor-hours, 15 DAGs, 5,163.0 all-time, 1,707.8/day) | **0 rows** |

Zero rows is the correct render: the layout already handles it, falling through to its "No measur…" empty-state paragraph.

### Fixes not yet measurable

15 rows — the same 15 DAGs the published headline was scoring.

```
dag_id,surface,applied_date,findings,before_days,after_days,before_rate,after_rate,why_not_measurable
advertiser_mid,spark,2026-09-03,4,4,0,16.5,,the finding is still firing, so nothing is confirmed cleared
advertiser_score_distribution_monitor,spark,2026-09-03,1,3,0,33.6,,the finding is still firing, so nothing is confirmed cleared
conv_log_derived_ip,spark,2026-09-03,1,3,0,0.5,,the finding is still firing, so nothing is confirmed cleared
conversion_log_advertiser_id_dsc_id,spark,2026-09-03,4,3,0,80.2,,the finding is still firing, so nothing is confirmed cleared
guid_conv_log_pivot_ip_vertical_id,spark,2026-09-03,1,4,0,7.3,,the finding is still firing, so nothing is confirmed cleared
guid_log_advertiser_id_dsc_id,spark,2026-09-03,4,3,0,92.3,,the finding is still firing, so nothing is confirmed cleared
guid_log_pivot_ip_vertical_id,spark,2026-09-03,1,3,0,7.6,,the finding is still firing, so nothing is confirmed cleared
ipdsc_42_monitor,spark,2026-09-03,6,4,0,1.5,,the finding is still firing, so nothing is confirmed cleared
ipdsc_ds_2,spark,2026-09-03,2,3,0,30.8,,the finding is still firing, so nothing is confirmed cleared
ipdsc_ds_49,spark,2026-09-03,1,3,0,2.9,,the finding is still firing, so nothing is confirmed cleared
ipdsc_third_party_audience_builder,spark,2026-09-03,14,2,0,44.7,,the finding is still firing, so nothing is confirmed cleared
site_network_hourly,spark,2026-09-03,14,4,0,970.9,,the finding is still firing, so nothing is confirmed cleared
site_visit_signal_advertiser_id_dsc_id,spark,2026-09-03,4,3,0,126.6,,the finding is still firing, so nothing is confirmed cleared
vertical_size_monitor,spark,2026-09-03,3,3,0,4.4,,the finding is still firing, so nothing is confirmed cleared
fangorn_score_monitor,spark,2026-08-27,4,1,2,687.7,630.3,"1 of 3 sweep-days before the fix, 2 of 3 after"
```

The last row is the parity check. The shipped Python's evidence string for this ledger is "No measured savings to report: 1 cleared job (1 of 3 sweep-days before the fix, 2 of 3 after)". The SQL produces the same job, the same counts, and the same sentence.

### The gate machinery computes, it does not just return empty

A query that returns zero because of a typo looks identical to one that returns zero because the evidence is thin. `queries/audi_1325_welch_parity_check.sql` swaps the `daily` and `jobs` CTEs for an inline synthetic series (before 100, 110, 90, 105; after 60, 55, 70; applied 2026-08-09) and leaves the `gated`/`scored` block untouched. BigQuery and the shipped Python agree to every digit printed:

| | SQL | `ledger._delta_ci` at 016e161 |
|---|---:|---:|
| `before_days` / `after_days` | 4 / 3 | 4 / 3 |
| `se` | 6.137883 | 6.137883 |
| `df` | 4.73399 | 4.73399 |
| t (90%, interpolated) | 2.046123 | 2.046123 |
| `exec_h_saved` | 118.75 | 118.75 |
| `half` | 37.6766 | 37.6766 |
| `proven` | true | true |

So the zero on the live ledger is the evidence gate firing, not a broken query.

### Decomposition ladder (`queries/audi_1325_savings_ladder_decomposition.sql`)

Every audit arithmetic claim, re-derived independently in one query:

| Step | Result | Audit claimed |
|---|---:|---:|
| published method | 5,163.0 | 5,163.0 ✓ |
| `MAX` instead of `SUM`, nothing else | 174.2 | 174.2 ✓ |
| signed instead of floored, nothing else | -125,734.2 | -125,734.2 ✓ |
| all aggregation fixes, no evidence gate | 114.8 | 114.8 ✓ |
| jobs with any observed after-day | 1 | 1 ✓ |
| alternative: `SUM` over distinct `app_id` | -1,805.0 | (not in audit) |

---

## 5. Recreate the external table

**Do not run this blind — it drops and rebuilds a table six live Mode queries read.** The schema is frozen at its 2026-08-28T22:14:35Z metadata update with `autodetect: true` and `ignoreUnknownValues: true`, holding 16 fields ending at `surface`. `prev_exec_h` is present on 781 of 1,692 GCS rows (360 non-null) and invisible to SQL; `partial`, which PR #1286 writes on every row from the 2026-09-05 sweep, will be dropped the same silent way.

The 18-field list below was extracted from the `Entry` dataclass in `include/spark_optimizer/ledger.py` at commit 016e161 and compared field-for-field, in order, against this definition — exact match.

Definition file, already written to `artifacts/audi_1325_optimization_ledger_external_def.json`:

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
  "SELECT COUNT(*) rows, COUNTIF(prev_exec_h IS NOT NULL) prev, COUNTIF(partial) part
   FROM \`mntn-prj-prod-00.optimizer.optimization_ledger\` LIMIT 1"
```

Expect 1,692 rows and 360 non-null `prev_exec_h` today; `partial` stays 0 until the 2026-09-05 09:00 UTC sweep.

Three notes on the choices:

- `applied_date` stays `STRING`. The ledger writes `""` for "not applied", which is not a valid `DATE`; typing it `DATE` would fail every row. The corrected SQL casts it with `SAFE_CAST(NULLIF(applied_date, '') AS DATE)`.
- `dcu_h` changes `STRING` → `FLOAT`. Autodetect typed it `STRING` only because all 1,692 rows are null. No Mode query on the report references it.
- `ignoreUnknownValues` stays `true`. With a full explicit schema the silent-drop failure mode returns only when the Python gains a *new* field, and failing open beats hard-failing every dashboard query. The cost is that this file must be updated whenever `Entry` gains a field. Setting it `false` makes that omission loud instead; it is a real option if someone owns the breakage.

---

## 6. Exactly what a human must paste, where

1. Open Mode report `e81786de8403` → **Query editor → "Savings headline"** (`5a66e5fad18c`). Select all, delete, paste the shared CTE block from §2 followed by the "Savings headline — corrected tail". Full file: `queries/audi_1325_mode_savings_headline_corrected.sql`. Save.
2. Open **"Savings by surface"** (`513a4a7a4a71`). Select all, delete, paste the same shared CTE block followed by the "Savings by surface — corrected tail". Full file: `queries/audi_1325_mode_savings_by_surface_corrected.sql`. Save. The CTE block must be byte-identical to step 1's.
3. **Add a new query** named exactly `Fixes not yet measurable`, pasting `queries/audi_1325_mode_not_yet_measurable.sql`. Save.
4. **Run the report** and confirm the KPI cards read $0 saved all-time, $0 est. annual, 0 executor-hours, 0.0/day, 15 DAGs fixed, and that the "Savings by surface" table shows its empty-state line.
5. **Edit the layout** to render the new query. `window.datasets` exposes it as `ds('Fixes not yet measurable')`; the columns are `dag_id`, `surface`, `applied_date`, `findings_the_fix_covers`, `sweep_days_before_the_fix`, `sweep_days_after_the_fix`, `mean_hours_per_day_before`, `mean_hours_per_day_after`, `why_not_measurable`. Without this step the corrected report shows zeros with no explanation of why.
6. **Edit the hero paragraph** (layout offset ~3263) which currently reads "Dollars use the blended rate from actual Dataproc spend, $0.278 per executor-hour". Replace with the fixed-assumption wording and cite the query's own `usd_rate_measured_on` (2026-08-27). The SQL now returns `usd_per_exec_h`, `usd_rate_basis` and `usd_rate_measured_on` for the layout to read instead of hard-coding the sentence.
7. **Recheck the "DAGs fixed so far" card copy.** The number (15) is unchanged and still true, but its subtitle "of 33 Spark DAGs profiled" sitting next to $0 reads as a contradiction. It counts fixes shipped, not fixes measured; say so, or point it at `dags_measured` instead.
8. **Recreate the external table** with the §5 command, from a shell that can write to `mntn-prj-prod-00`. Then run the §5 verification.
9. **Only after step 8 and after the 2026-09-05 09:00 UTC sweep has run**, add `AND NOT COALESCE(partial, FALSE)` to the `daily` CTE `WHERE` clause in both queries, matching `ledger.py`'s partial-sweep exclusion. Do not add it before step 8 — `partial` does not exist in the current table schema and the queries will fail with "Unrecognized name".
10. **Fix the schedule** (audit defect 8, token `d30b701e413d`): move `cron_hour` from 6 to 10 UTC so the unattended render reads the same day's sweep. Schedules are UI-only; the API rejects its own documented payloads.

Steps 1-7 are Mode edits and need no BigQuery permissions. Step 8 needs write access to the `mntn-prj-prod-00:optimizer` dataset. Steps 1-3 are the ones that change the published number.
