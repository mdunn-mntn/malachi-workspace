# AUDI-1194 — Optimizer production-history diagnosis, 2026-09-01

Full production history of the spark optimizer's ledger and report surfaces. The ask was "30 days"; **the system's entire production history is 12 days (2026-08-21..2026-09-01)** — this report covers all of it, completely.

## 0. Data and method

- **Primary:** `mntn-prj-prod-00.optimizer.optimization_ledger` (936 rows, queried via `bq_run.sh`). Cross-checked against `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` (936 lines — exact mirror; the heavy per-key analysis ran locally on the mirror after the row counts matched, scripts `analyze_ledger.py`/`analyze2.py`/`analyze3.py` in session scratchpad).
- **Secondary (GCS `gs://mntn-data-archive-prod/optimizer/`):** all 12 digests, all 12 coverage reports, all 12 backlog headers, all 5 BQ cost reports (2026-08-28..2026-09-01), `optimizer_savings.md`.
- Field notes established empirically below: `streak` is an INTEGER (BQ schema and all 936 mirror rows; corrected 09-01, originally misrecorded as STRING); `dcu_h` is a STRING column and never populated; `exec_h` is a per-app worst-run figure duplicated onto every finding row of that app (see §8).
- Dead ends: `gsutil -m` parallel copy hung indefinitely on this Mac (all `.gstmp` at 0 bytes, exit 144); sequential `gsutil cp` with `parallel_process_count=1` worked. First BQ query failed on reserved word `rows` as an alias.

## 1. Timeline of the system itself (the single most important context)

| Date | Archive logs downloaded | PHS logs | Jobs scanned | Digest findings (high) | Ledger rows written | Coverage enumeration |
|---|---|---:|---:|---|---:|---|
| 08-21 | 200 of 200, 0 failed | 19 | 212 | 303 (201) | 130 | FAILED (airflow session forbidden) |
| 08-22 | 200 of 200, 0 failed | 19 | 210 | 273 (184) | **0** | FAILED |
| 08-23 | 200 of 200, 0 failed | 19 | 212 | 281 (190) | **0** | FAILED |
| 08-24 | 200 of 200, 0 failed | 19 | 211 | 301 (194) | **0** | FAILED |
| 08-25 | 200 of 200, 0 failed | 22 | 214 | 284 (185) | 136 | OK (72 DAGs) |
| 08-26 | 200 of 200, 0 failed | **150** | **344** | 368 (172) | 240 | OK |
| 08-27 | **6 of 200, 194 failed** | 150 | 154 | 75 (47) | 180 | OK |
| 08-28 | 6 of 200, 194 failed | 150 | 154 | 61 (34) | 71 | OK |
| 08-29 | 6 of 200, 194 failed | 150 | 154 | 55 (31) | 49 | OK |
| 08-30 | 6 of 200, 194 failed | 150 | 154 | 61 (35) | 46 | OK |
| 08-31 | 6 of 200, 194 failed | 150 | 154 | 59 (31) | 51 | OK |
| 09-01 | 6 of 200, 194 failed | 150 | 154 | 27 (21) | 33 | OK |

Three regime changes, all visible in every downstream number:

1. **08-21..24: coverage enumeration failure.** All four sweeps errored with "could not access attribute query because airflow session use is forbidden in this context". Digests for all four days say "this sweep was not recorded" — yet **the ledger contains 130 rows dated 08-21** (all `state=new`, `streak=1`) and zero for 08-22..24. Reconciling hypothesis: 08-21 was written retroactively as the baseline when the ledger went live (the 08-25 rows are `recurring`/`streak=2`, which requires a recorded 08-21), and the three sweeps in between (273–301 findings each, backlogs exist in GCS) were dropped permanently. Check that settles it: the ledger-writer commit history in `airflow-ti` around 08-25.
2. **08-26: the PHS grant landed.** PHS logs jumped 19 → 150 (the ipdsc/tpa standing GCS read from the framing's blocker list). Biggest sweep ever: 344 jobs, 240 ledger rows, 65 dags, 86 new findings.
3. **08-27 onward: the archive surface died.** `spark-events` downloads collapsed to 6 of 200 with 194 failures, and have stayed there for **6 consecutive sweeps**. Every digest since carries "_Partial sweep: some event logs could not be downloaded, so nothing is reported as resolved this run._" The fleet the optimizer actually sees since 08-27 is ~150 PHS jobs + 6 archive logs — dags/day fell 65 → 20–22, and 30 dags seen on 08-26 have never appeared again (list in §7).

## 2. Findings volume and mix

**Rows/day by impact** (only `high` and `medium` exist; no `low` was ever emitted):

| Date | high | medium | high share |
|---|---:|---:|---:|
| 08-26 | 134 | 106 | 56% |
| 08-25 | 106 | 30 | 78% |
| 08-27 | 105 | 75 | 58% |
| 08-21 | 97 | 33 | 75% |
| 08-28 | 39 | 32 | 55% |
| 08-31 | 27 | 24 | 53% |
| 08-29 | 27 | 22 | 55% |
| 08-30 | 25 | 21 | 54% |
| 09-01 | 20 | 13 | 61% |

**By state per day:** new 130 → (gap) → 19/86/26 (08-25/26/27) → 22/11/15/22/2 (08-28..09-01). Chronic dominates from 08-26 on (126, 114, 39, 34, 29, 28, 27). Recurring collapses after 08-27 (27 → 10/4/2/1/4) — with 97% of the archive missing, findings can't recur.

**By key type, all 936 rows (high, medium):**

| Key type | rows | high | medium |
|---|---:|---:|---:|
| shuffle_fetch_wait | 381 | 205 | 176 |
| disk_spill | 249 | 157 | 92 |
| straggler | 117 | 54 | 63 |
| shuffle_partition_sizing | 91 | 91 | 0 |
| skew | 40 | 21 | 19 |
| idle_reserved_executors | 32 | 28 | 4 |
| bq_heavy_task | 15 | 15 | 0 |
| gc_pressure | 11 | 9 | 2 |

**By surface:** surface is NULL on all 701 rows before 08-28 (the column shipped mid-history, never backfilled — plus 15 NULL rows on 08-28 itself). Since 08-28: spark 220 rows (18–22 dags/day), bq 15 rows (always exactly 3/day, always the same 2 dags), **dbx 0 rows ever** (blocked: prod lacks `DATABRICKS_WAREHOUSE`; root-caused in PR #1250, per ticket summary).

## 3. Chronic offenders (fleet view the system produces)

Top (dag, key) pairs by longest streak (streak = consecutive sweeps firing; states observed: new=1, recurring=1–2, chronic=3–13):

| dag_id | key | max streak | times seen | last seen | worst-run exec_h | last state |
|---|---|---:|---:|---|---:|---|
| intent_score_map | disk_spill:2, disk_spill:3, shuffle_partition_sizing:2, shuffle_partition_sizing:3 | 13 | 9 | 09-01 | 159.5 | chronic |
| materialize_mntn_select | shuffle_fetch_wait:6 | 11 | 7 | 09-01 | 320.1 | chronic |
| AugmentorLogDsid30Processing | straggler:1 | 10 | 7 | 09-01 | 317.7 | chronic |
| segment-updates-to-parquet | shuffle_fetch_wait:2 | 8 | 8 | 09-01 | 287.6 | chronic |
| site_network_hourly | idle_reserved_executors + shuffle_fetch_wait:9 | 7 | 5 | 08-29 | 101.7 | chronic |
| materialize_mntn_select_15/16/17 | shuffle_fetch_wait:6 | 7 | 7 | 09-01 | 46.9–68.4 | chronic |
| bos__spend | bq_heavy_task:campaign_summary_hourly-create, flight_metrics_per2388-create | 6 | 5 | 09-01 | 2,252.1 slot-h | chronic |
| intent_score_threshold_v4 | bq_heavy_task:population_histogram | 6 | 5 | 09-01 | 1,231.6 slot-h | chronic |
| ETL Audience Intent - Prospecting Keywords | disk_spill:18 | 6 | 6 | 08-31 | 843.6 | chronic |

Top open findings by exec_h: bos__spend 2,252 slot-h/day across 288+96 BQ jobs; intent_score_threshold_v4 1,231 slot-h from **4 jobs** (98.8 TiB billed, 08-28); fangorn_score_monitor 954.3 exec-h (last seen 08-27, then went dark with the archive); ETL Audience Intent - Prospecting Keywords 843.6; Run Single-Day TPA Export 583.4.

Floor on currently-open waste (latest row per dag, dags still observed since 08-28, one exec_h per dag to avoid the duplication in §8): **3,109.8 spark executor-hours (worst-run) + 2,570.3 BQ slot-hours/day across 51 dags**. At the savings meter's own $0.28/exec-h rate the spark side alone is ~$871 per worst-run cycle. This is a floor: 30 dags are currently invisible (§7).

## 4. Resolution flow

- 343 distinct (dag, key) pairs all-time. Last-state distribution: **new 148, chronic 143, recurring 28, resolved 20, applied 4**.
- **22 resolved rows total, all in a 2-day window (13 on 08-26, 9 on 08-27), zero since.** Every one is an auto-resolve by disappearance ("stopped firing after YYYY-MM-DD"); none is a verified fix.
- Days first-seen → resolved: **median 5, min 1, max 6, mean 3.8** — but the sample is only those 22, and the 08-27 batch's 6-day figure is inflated by the unrecorded 08-22..24 sweeps.
- 2 resolved pairs later reappeared (`materialize_mntn_select_8`/`_9` `shuffle_fetch_wait:6`) — resolution by absence is noisy.
- The dominant exit from the ledger is not resolution: **286 of 343 pairs (83%) silently vanished** — last row exists, never marked resolved or applied (69 last seen 08-26, 114 on 08-27, 27/17/20/39 across 08-28..31). The partial-sweep guard ("nothing reported as resolved") has frozen resolution since 08-27, so nothing can ever close while the archive surface is down.
- Contradiction to investigate: digest 08-27 says nothing was resolved that run, yet 9 ledger rows dated 08-27 are `state=resolved`.

## 5. Applied fixes and measured savings

- **Exactly one fix has ever been applied:** PR [#1231](https://github.com/SteelHouse/airflow-ti/pull/1231), 2026-08-27, `fangorn_score_monitor`, "shuffle partitions 256 to 2048 in decorator and builder" — 4 ledger rows (disk_spill:17/19, shuffle_partition_sizing:17/19, exec_h 687.7, streak 5).
- **Measured savings: $0.** `optimizer_savings.md`: "Saved since 2026-08-27: 0 hours all-time... current rate 0.0/day", all four rows "watching", **Days observed 0**. Cause: `fangorn_score_monitor` logs live in the archive path that has failed 194/200 downloads since the very day the fix merged. The before/after join is empty — the dag has zero ledger rows after 08-27. The savings loop has never completed a single measurement.
- Method per the report: before-rate minus after-rate × days observed, only fixes whose finding stopped firing count, $0.28/exec-h.

## 6. Coverage trend

- 08-21..24: coverage unknown (enumeration failure), completeness explicitly "unknown".
- Since 08-25 the denominator is stable: **72 active DAGs; 33 have a Spark task; 38 invisible (no Spark task); 1 cost-profiled via BQ/Databricks** (tag shipped 08-28).
- "Profiled this sweep" (Spark DAGs actually tied to a profiled log): **2 (08-25), 13 (08-26), 4 (08-27), 2, 3, 4, 4, 2 (09-01)** — i.e. 6–12% of Spark DAGs on every sweep except the one full-fleet day (08-26: 39%). "No log this sweep": 29–32.
- Jobs scanned but **not tiable to any DAG**: 33 (08-26) → 26 (08-28..31) → 12 (09-01). These carry findings with no Airflow link and no owner.
- "Paused DAGs could not be excluded" errors on **every** coverage report since 08-25 — the same forbidden-airflow-session bug in a second location; denominator counts paused DAGs as active.
- The 48 "Spark we cannot read" task rows (Databricks job clusters with no `cluster_log_conf`, 2 managed clusters with no `spark.eventLog.dir`) are unchanged across all 8 enumerable days — no progress on the dbx estate.

## 7. Surfaces health

| Surface | Ledger rows (all-time) | Days active | Dags/day | Status |
|---|---:|---:|---|---|
| (pre-surface, NULL) | 701 | 08-21..08-28 | 48–65 | column not backfilled |
| spark | 220 | 08-28..09-01 | 18–22 | degraded: 6/200 archive + 150 PHS since 08-27 |
| bq | 15 | 08-28..09-01 | 2 | live but shallow: only `bos__spend` + `intent_score_threshold_v4` ever emitted; `unattributed` bucket is 551–611 jobs and 793–1,184 slot-h **every day** in the BQ cost reports |
| dbx | 0 | never | 0 | blocked: prod lacks `DATABRICKS_WAREHOUSE` (PR #1250 root-caused); entire Databricks estate invisible |

30 dags seen on 08-26 and never again (the archive-blackout casualties): advertiser_join, advertiser_score_distribution_monitor, audience_intent_scoring_staging_ds46, conv_log_derived_ip, conversion_log_advertiser_id_dsc_id, fangorn_household_predictions_vertical, fangorn_predictions_vertical, **fangorn_score_monitor** (the one dag with an applied fix), guid_log_advertiser_id_dsc_id, guid_log_pivot_ip_vertical_id, intent_score_household_map, ipdsc_46_monitor, ipdsc_49_monitor, ipdsc_ds_{2,17,35,42,46,47,49,67}, ipdsc_third_party_audience_builder, prospecting_join, site_visit_signal_advertiser_id_dsc_id, tpa_export_enrich, tpa_mntn_id_export, vertical_size_monitor, plus 3 ephemeral date-stamped names.

## 8. Data-quality oddities

1. **exec_h is a per-app constant, duplicated across findings.** 258 (date, app_id) groups with exec_h populated (363 counting exec_h-NULL rows); exec_h varies within a group in **0** of them. Any `SUM(exec_h)` over rows multi-counts. Worse on bq: each of the two `bos__spend` task rows carries the **DAG total** (08-28: 2,225.1 = 1,245.8 + 979.3 from the same day's BQ report), so the per-task field is wrong by construction.
2. **`dcu_h` NULL in 936/936 rows. `owner` empty in 936/936.** Dead columns. `fix_pr` filled 4/936, `note` 26/936, `fix` empty 292/936.
3. **`surface` NULL for 701/936 rows** (everything before 08-28 plus 15 rows on 08-28).
4. **288 NULL exec_h rows**, concentrated where the parser had no metrics: all of 08-21 (130) and 08-25 (136), plus 13/9 on 08-26/27 (the resolved rows — all 22 also have empty app_id — and some carried rows).
5. **app_id leaks the raw log filename:** 859/936 end in `.zstd` (e.g. `app-20260821064036193-0621.zstd`); PHS jobs use `eventlog_v2_batch-<uuid>`; 22 rows empty. Three ID formats in one column.
6. **Ephemeral date-stamped dag_ids break streak tracking:** `Run Single-Day TPA Export for 2026-08-{26..31}` (6 ids), `geo_data in prod for ...` (6), `data_source_id 4 for ...` (3) — 15 dag_ids for 3 real jobs, 88 rows, **every one state=new streak=1 forever**. A 583 exec-h/day job (TPA export) can never become chronic.
7. **dag_id is not a DAG id.** 85 distinct values vs 33 Spark DAGs in coverage; the column mixes DAG names, task names (`materialize_mntn_select_15`), Spark app names (`AugmentorLogDsid30Processing`, `ETL Audience Intent - ...`), and raw filenames — the 26–33/day "could not be tied to a DAG" coverage entries land here unresolved.
8. **State machine glitches:** 3 `recurring` rows with streak=1 (1 on 08-26, 2 on 08-28) where recurring implies 2; `resolved` rows get streak=0; `applied` rows froze at streak=5.
9. **No duplicate (date, dag_id, key) rows** — the one clean check (0 groups with count > 1).
10. **Digest/ledger counts never match** (digest 08-26: 368 findings vs 240 ledger rows; digest 08-27: 75 vs 167 active ledger rows) — digest counts per-app findings, ledger dedups to (dag, key) and on 08-27 apparently carried forward unseen rows; neither doc states this.

## 9. What to fix about the SYSTEM (ranked)

1. **Fix the spark-events archive downloader — 194/200 failures on 6 consecutive sweeps since 08-27.** This is the root of nearly everything else: dags/day 65→20, 30 dags invisible, resolution frozen ("nothing reported as resolved" 6 sweeps running), savings unmeasurable. Evidence: backlog Source lines 08-27..09-01; §1 table.
2. **Give findings a terminal state that isn't silence.** 286/343 pairs (83%) left the ledger with no resolved/applied marker; only 22 ever resolved, all by absence, 2 of which reappeared. Add `log_lost`/`expired` states and re-emit or expire carried rows so the ledger's open set is trustworthy. Evidence: §4.
3. **Make the savings loop able to complete a measurement.** One applied fix in 12 days and its measurement is stuck at "Days observed 0" because measurement depends on the same fragile log path as detection. Fall back to Dataproc batch runtime (wall-clock × executors from the Batches API) when the event log is missing. Evidence: §5, `optimizer_savings.md`.
4. **Normalize job identity.** Strip dates from ephemeral app names (3 real jobs currently produce 15 dag_ids and 88 permanently-"new" rows, one of them 583 exec-h/day); resolve app→DAG for the 12–33 unlinked jobs per sweep (fuzzy-match against bundle task names or read the batch's airflow labels); stop leaking `.zstd` filenames as app_ids. Evidence: §8.5–8.7, coverage reports.
5. **Fix the two airflow-session bugs in coverage.** The enumeration variant cost the ledger its first 4 days (3 sweeps of ~280 findings each permanently unrecorded); the paused-DAG variant still fails every day and inflates the denominator. Evidence: §1, §6, every coverage file.
6. **Unblock the dbx surface.** 0 rows ever; 48 Databricks task rows "cannot read" unchanged across the whole history; prod lacks `DATABRICKS_WAREHOUSE` (PR #1250). Until then "fleet coverage" claims should say Dataproc-only. Evidence: §7.
7. **Fix exec_h semantics or document them.** Per-app worst-run value duplicated per finding row, and DAG-total on bq task rows — every naive aggregate over the ledger is wrong. Either attribute per finding or add an `app_exec_h` column and lint the reports that sum it. Evidence: §8.1.
8. **Drop or populate the dead columns.** `dcu_h` and `owner` are 100% empty 12 days in; owner was the whole point of a routable backlog (framing: "tells owners which jobs to fix first"). Populate owner from DAG metadata at write time. Evidence: §8.2.
9. **Backfill `surface`** for the 701 pre-08-28 rows (derivable from key prefix + app_id format) so surface-level trends don't start mid-history. Evidence: §2, §8.3.
10. **Chase the `unattributed` BQ bucket.** ~600 jobs and 793–1,184 slot-h per day (comparable to the #1 attributed offender) invisible to per-DAG accounting because jobs run unlabeled. Enforce airflow labels on the fleet service account. Evidence: BQ cost reports 08-28..09-01.
11. **Calibrate impact.** Two-level scale where 53–78% of findings are "high" ranks nothing; digests already lead with only 3 items, confirming the scale is decorative. Add thresholds tied to exec_h so "high" is scarce. Evidence: §2.
12. **Reconcile digest vs ledger accounting** (368 vs 240 on 08-26; "not recorded" vs 130 baseline rows on 08-21; "nothing resolved" vs 9 resolved rows on 08-27) — one counting rule, stated in both artifacts. Evidence: §1, §4, §8.10.

## 10. Cross-system: debugger (AUDI-1191) overlap

Compared against the debugger's full prod corpus (10 rca files, 08-20..08-31: 13 DAGs, 27 dag/task pairs). The ledger's dag_id is often the Spark app name, so mapping is by task/app identity.

**Jobs appearing in both systems' records:**

| family | optimizer side | debugger side |
|---|---|---|
| tpa_ipdsc_export | ipdsc_ds_{2,13,17,35,42,46,47,49,67} apps, 89 rows, chronic disk_spill/shuffle; most last seen 08-27, ipdsc_ds_13 live to 09-01 | ipdsc_ds_49: 2 unclassified retry-recovered flakes (08-21, 08-24) |
| ipdsc monitors | ipdsc_{14,42,46,49}_monitor apps, 71 rows; 14/42 live to 09-01 | ipdsc_monitor/monitor_ipdsc_42: 1 unclassified recovered row (08-28) |
| audience_intent | "ETL Audience Intent - *" apps, 5 ids, 61 rows; Prospecting Keywords a top-10 chronic at 843.6 exec_h | audience_intent DAG: 2 unclassified recovered rows 08-24. Name-level family match, app-to-DAG link unproven |
| fangorn (weak mapping) | fangorn_{predictions_vertical, household_predictions_vertical, prospecting_scoring, score_monitor}, 61 rows incl. the one applied fix | fangorn_hhid_inference_pipeline_run + fangorn_inference_pipeline_run, 4 recovered rows (model-alias, quota, stockout, 1 unclassified). App-to-DAG link unproven |

**Do the debugger's failures and this ledger's findings point at the same jobs? Mostly no — near-disjoint fleets.**
- This ledger's top offenders (intent_score_map, materialize_mntn_select, AugmentorLogDsid30Processing, segment-updates-to-parquet, bos__spend, intent_score_threshold_v4) have zero debugger rows: chronically inefficient but not failing (caveat: the debugger's pre-#1248 2-tag scope could also hide their failures).
- The debugger's top 3 (vertical_classification_api 34 rows, mntn_match_verticals_precache_v1_1 17, mntn_match_incrementals_submit 14 — 72% of its diagnosis rows) have zero ledger rows ever: Databricks-API/dbt/pod/OpenAI-batch jobs, exactly the dbx surface (0 rows, §7) and the unattributed BQ bucket this system cannot see.
- Where the fleets meet (ipdsc family, audience_intent), signals are complementary: the debugger records retry-recovered infra flakes on jobs this ledger flags as chronic spill/shuffle waste. No job in either record has a terminal failure and an efficiency finding tracing to the same defect.

## 11. Corrections (2026-09-01 spot-check)

Adversarial re-check against BQ (`optimization_ledger` via bq_run.sh) and the GCS mirror; two edits made in place:
- §0 field note corrected: `streak` was recorded as a STRING; it is INTEGER in the BQ schema and int in all 936 JSONL mirror rows. The STRING-typed always-empty column is `dcu_h`.
- §8.1 clarified: 258 counts (date, app_id) groups with exec_h populated; 363 groups exist counting exec_h-NULL rows. The claim itself (0 groups with varying exec_h) holds in both populations.
- Confirmed exact, no change: rows/day and high/medium split for all 9 write days (936 total); key-type distribution (all 8 types and impact splits); last-state over 343 pairs (new 148 / chronic 143 / recurring 28 / resolved 20 / applied 4); resolved rows 13 on 08-26 + 9 on 08-27 and zero elsewhere; dcu_h/owner empty 936/936, surface NULL 701, `.zstd` app_ids 859, exec_h NULL 288; §3 top-four max streaks 13/11/10/8 with days-seen 9/7/7/8; ephemeral ids 15 dag_ids / 88 rows; 85 distinct dag_id values; mirror line count 936.
