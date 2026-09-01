# AUDI-1191 — Debugger production-history diagnosis, 2026-09-01

**Answer line: 12 days of production history (2026-08-20..2026-08-31, the system's entire life — the "30 days" ask is longer than the system has existed; all of it is covered here) show 128 failure candidates, 90 diagnosed, 52 root-caused high-confidence; every terminal failure inside the corpus got exactly one Slack reply since delivery went live 2026-08-25, and all 23 daily-sweep skips reconcile against rapid-sweep markers with zero orphans; the two known missed replies (08-28 `audience_intent_scoring_household_14day_lookback`, 08-29 `fetch_common_crawl`) are the only delivery gaps found and both were tag-filter blind spots fixed in #1248 — but the corpus also has two whole missing days (08-22, 08-26) that were never swept and never backfilled.**

Generated 2026-09-01 (analysis pulled ~22:15-22:30 UTC). This is the analytical record; terse rules do not apply.

---

## 1. Scope and coverage

- The ask was a 30-day diagnosis. **Production history is shorter: the first prod RCA report is `rca_2026-08-20.json`; the newest is `rca_2026-08-31.json` (published 2026-09-01 17:00:59Z). Full history = 12 calendar days, 10 report days.** All of it is analyzed here — coverage of the production record is complete.
- A 30-day *laptop replay* covering 2026-07-28..2026-08-26 exists separately (`outputs/audi_1191_backfill_30d_2026_08_27.md`: 173 fleet-wide failures, 95.3% of diagnosable deterministically root-caused). It ran fleet-wide with **no tag filter**, so its numbers are not directly comparable to the tag-scoped prod corpus; it is used below only as a fleet baseline for the blind-spot check.
- **Days with no RCA report: 2026-08-22 and 2026-08-26.** Neither was ever produced or backfilled. The laptop fleet-wide backfill shows real failures on both days (08-22: 2 failed TIs; 08-26: 5), so these are coverage holes, not zero-failure days — caveat: the backfill is untagged, so the *tag-scoped* candidate count on those days could in principle have been 0; unverifiable now (Airflow TI history for those dates has aged out of easy reach).
- Publish cadence was ad hoc (laptop-run) before 08-28 and out of order: rca_08-23 published 08-25 00:37Z, rca_08-21 on 08-26 00:04Z, rca_08-20 on 08-26 16:19Z, rca_08-24 on 08-27 02:40Z, rca_08-25 on 08-27 21:40Z. From 08-28 the prod daily DAG runs on schedule `0 17 * * *` (`dags/airflow_debugger_daily.py:45`), publishing day D-1 at ~17:01Z with no misses since (5/5 on-time publishes 08-28..09-01). Rapid DAG: `*/15 * * * *` (`dags/airflow_debugger_rapid.py:30`); `cycle_watermark.json` end=2026-09-01T22:15:13Z at pull time, i.e. the watermark loop is live and current to the most recent 15-min cycle.

## 2. Data and method

- Pulled `gs://mntn-data-archive-prod/debugger/rca_*.json` (10 files, 20 rca objects counting .md twins), `cycle_watermark.json`, the full `delivered/` marker listing (31 markers), and the `unclassified/` log listing (26 logs, 5 days) to `scratchpad/debugger_rca/`. gsutil note: `-m cp` hung on this Mac (LibreSSL, known from `reference_airflow_ti`); sequential `cp` with `check_hashes=never` worked.
- Cross-checked both incident logs: `~/Developer/work/mntn/airflow-ti/include/airflow_debugger/incident_log.jsonl` (24 rows, ends INC-024 2026-08-20 — this copy is vendored into the prod image as the similar-incident corpus) and `workspace/on-call/incident_log.jsonl` (25 rows, adds INC-025 2026-08-23).
- Read the debugger source at the airflow-ti checkout (branch `audi-1191-1194-improvements`) for field semantics: `candidates` = failed-at-any-point task instances on ds within PAGING_TAGS (`daily.py:59`); `diagnosed` = candidates with non-empty logs; `empty_logs` = candidates skipped for empty log text (`daily.py:79-81` — identity not recorded, count only); `resolved` = confidence high; `unclassified` = no signature matched.
- RCA JSON schema drifted twice, consistent with the ship history: 08-20..08-24 files have no Slack fields (pre-delivery), 08-25 adds `slack_posted`/`slack_threaded` (delivery live), 08-27+ adds `tickets`/`unclassified`/`unclassified_logs` (triage filer #1240, unclassified publishing #1239).

## 3. Failures per day

Ranked by candidates (primary metric). `sent`/`skip` are Slack: skip = "answered by the rapid sweep" marker hit.

| ds | candidates | diagnosed | empty_logs | high-conf | unclassified | terminal-failed rows | sent | skip |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-29 | 30 | 19 | 11 | 9 | 10 | 6 | 13 | 6 |
| 2026-08-30 | 28 | 17 | 11 | 9 | 8 | 7 | 10 | 7 |
| 2026-08-31 | 20 | 9 | 11 | 7 | 2 | 7 | 2 | 7 |
| 2026-08-24 | 10 | 10 | 0 | 4 | 6 | 2 | 0 | 0 |
| 2026-08-27 | 10 | 8 | 2 | 5 | 3 | 5 | 8 | 0 |
| 2026-08-28 | 10 | 10 | 0 | 7 | 3 | 6 | 7 | 3 |
| 2026-08-20 | 7 | 7 | 0 | 4 | 3 | 2 | 0 | 0 |
| 2026-08-21 | 5 | 4 | 1 | 2 | 2 | 1 | 0 | 0 |
| 2026-08-25 | 5 | 3 | 2 | 3 | 0 | 2 | 3 | 0 |
| 2026-08-23 | 3 | 3 | 0 | 2 | 1 | 2 | 0 | 0 |
| **Total** | **128** | **90** | **38** | **52** | **38** | **40** | **43** | **23** |

- The 08-29..08-31 spike (78 of 128 candidates, 61%) is one incident: the OpenAI batch outage (org-side, cohorts 08-27..08-30 dead) plus its `upstream_failed` fan-out through `mntn_match_incrementals_submit`/`fetch` and `keyword_ddp_reporting`. The 33 empty-log candidates on those three days are that fan-out (upstream_failed tasks emit nothing).
- Baseline load outside the incident: 3-10 candidates/day.
- 50 of 90 diagnosed rows have `final_state: success` — the failed try was retried into success. The debugger diagnoses per failed try, so retry-recovered flakes are a majority of its workload.

## 4. Top failing DAGs and tasks

Diagnosis rows, ranked. Per-try counting inflates chronic retry loops — that is itself a finding (§9 item 2).

| rows | dag/task | days seen | story |
|---:|---|---:|---|
| 30 | vertical_classification_api/response_tests | 9 of 10 | 26x task_execution_timeout (killed at the 45m limit; successes ran 39-42m), 3x dbt_test_failure, 1x pod_evicted_404. Chronic single defect. #1248 raised execution_timeout to 55m (`dags/machine_learning/ddp_vertical_classification_api.py:23`), live only since the 2026-09-01 19:06Z deploy — 2 more failures on 09-01 pre-deploy (markers); post-deploy effect not yet judgeable. |
| 15 | mntn_match_verticals_precache_v1_1/pre_cache_verticals | 9 of 10 | 11 unclassified (pod never reached Running in its 120s budget, empty-ish log), 2x pod_evicted_404, 2x dbt_model_runtime_error. All retry-recovered successes. Cluster-capacity flake class. |
| 14 | mntn_match_incrementals_submit/* (7 tasks) | 2 | OpenAI-outage recovery churn 08-29/08-30; 13 retry-recovered, 1 externally-killed. |
| 6 | mntn_match_incrementals_fetch/* | 3 | 3x batch_post.openai_batch_joined terminal (late-data/missing-partition x2, then openai_results_cohort_missing on 08-31 — the #1249 signature fired on its first live day), plus 3 recovered. |
| 4 | databricks_guid_geos/run_databricks_job | 4 | 3 unclassified recovered + 1 pod-evicted. |
| 4 | vertical_classification_api/ddp_vertical_classification_api | 3 | 3x timeout, 1x dbt runtime. |
| 3 | keyword_ddp_reporting/wait_for_product_categorization | 3 | Sensor fast-fail, upstream/external-task-failed — downstream symptom of the OpenAI outage every time. |
| 2 each | fangorn_hhid.../challenger, fangorn.../challenger+inference, tpa_ipdsc_export/ipdsc_ds_49, auto_assign_verticals | 2 | One-offs: model-alias bug (INC-024), quota (INC-025 chain), zonal stockout, spot preemption. |

13 distinct DAGs, 27 distinct dag/task pairs. By DAG: vertical_classification_api 34, mntn_match_verticals_precache_v1_1 17, mntn_match_incrementals_submit 14, mntn_match_incrementals_fetch 6, keyword_ddp_reporting 4, databricks_guid_geos 4, everything else ≤2.

## 5. Root-cause signature distribution

52 of 90 diagnosed rows matched a signature (57.8%); all 52 are confidence=high. Ranked:

| n | signature | sig_class |
|---:|---|---|
| 29 | task_execution_timeout | timeout/execution |
| 4 | pod_evicted_404 | orchestration/pod-evicted |
| 3 | dbt_test_failure | dbt-test/data-quality |
| 3 | dbt_model_runtime_error | dbt/model-runtime-error |
| 3 | external_task_target_failed | upstream/external-task-failed |
| 2 | spot_preemption | infra/spot-preemption |
| 2 | path_not_found_late_data | late-data/missing-partition |
| 1 each | analysis_exception, model_alias_not_found, quota_exhaustion, cluster_create_stockout, task_externally_terminated, openai_results_cohort_missing | one each |

**Classified vs routed vs unresolved:**
- Classified (signature, high confidence): 52 (57.8% of diagnosed, 40.6% of candidates).
- Routed (cause placed in another system): the 3 `external_task_target_failed` rows route to the upstream DAG; no `downstream_job_no_local_cause` fired in this window (it fired 13x in the 30-day laptop replay — that class simply didn't occur, or the engines' handles resolved locally).
- Unresolved (no signature, confidence low): 38 (42.2% of diagnosed).

**The sharpest fact in the corpus: the split is perfectly aligned with outcome. All 40 terminal failures are classified high-confidence (100%); all 38 unclassified rows are retry-recovered successes.** The signature debt is entirely in the "pod/cluster flake that healed itself" class — dominated by pre_cache_verticals (11) and one-off empty-ish pod-start logs. Nothing that stayed dead went unexplained in 12 days of production.

Similar-incident matching fired on 75 of 90 rows; the timeout rows overwhelmingly match (INC-009, INC-010, INC-011). Corpus note stayed None all 12 days (threshold `_STALE_DAYS=45`, `incident_match.py:31`).

## 6. Repeat failures (same dag+task across days)

16 of 27 pairs repeated across days. The top repeats ARE the system's residual noise:

- `response_tests`: 9 of 10 report days, 30 rows, one defect (45m limit). A fix existed from day one of the record (raise the limit — the debugger's own recommendation) and shipped only 08-31/09-01 via #1248. Twelve days of a known one-line fix generating ~30 diagnoses and ~19 rapid replies (19 of the 31 markers are this DAG).
- `pre_cache_verticals`: 9 of 10 days, no signature — a recurring shape (pod not Running within 120s) that has never been promoted to a signature despite `unclassified/` publishing existing exactly for that purpose since 08-27 (its logs sit there on 08-27, 08-28, 08-29, 08-31).
- Triage-filer confirmation of chronicity: `AUDI-1231` (response_tests) appears in `already_ticketed` on all 5 triage-era days; `AUDI-1238` (pre_cache) on 4. The dedup works; nothing escalates a ticket that keeps re-firing.

## 7. Reply delivery — markers vs diagnoses

Delivery eras, from the records: **pre-Slack** 08-20..08-24 (24 diagnosis rows, no slack fields — 7 of them terminal failures that never got a reply; expected, delivery didn't exist); **daily-only** 08-25..08-27 (11 posts to alert channel C08CURMGNMQ; 1 threaded — the only threaded daily post ever); **rapid+daily** 08-28.. (rapid answers terminal failures in ≤15min threaded under the alert; daily posts the remainder loose to fallback C0BT9TKRMKM `#airflow-debugger`).

Reconciliation results:
- **All 23 daily rows marked "answered by the rapid sweep" have a matching `delivered/` marker (23/23, zero orphan skips).** A skip claimed without a marker would be a silent drop; none exist.
- **All 31 markers reconcile**: 23 match daily skip rows; 8 are rapid-only with an explanation each — 4 are markers written 09-01 for runs the 09-01 daily sweep hasn't covered yet (ds=09-01 publishes 09-02 17:01Z), and 4 are *intermediate* failed tries of runs whose later try the daily sweep re-diagnosed (keyword_ddp try2→daily carried try4; openai_batch_joined try5/try10→daily carried try15; batch_submit try5 answered 08-29 19:45Z, task then cleared and succeeded so daily's row shows the recovered TI). Rapid replies per terminal try; daily reports per TI final try. Overlap is absorbed exactly as designed.
- **Zero rows anywhere in the delivery era with sent=false and no skip marker, zero slack errors recorded.** Every diagnosis that should have produced a reply produced one.
- **The known 08-28 missed reply** (referred to as fangorn_household; the actual DAG is `audience_intent_scoring_household_14day_lookback`, tags `audience_intent_scoring`/`ipdsc`/`ml`/`fangorn` — alert 08-28 14:40 PT in monitor-tpa): confirmed a *candidacy* gap, not a delivery gap — the DAG appears in **zero** rca files because `ml` != `Machine Learning` in PAGING_TAGS. Same for the second known miss, `fetch_common_crawl` (08-29 07:03 PT, tags `common_crawl_content`/`vertical_categorization`): zero rows in the corpus. Full record: `outputs/audi_1191_missed_replies_2026_08_29.md`.
- **Check for OTHER misses:** within the corpus, none (above). Outside the corpus the tag filter is unfalsifiable from the corpus itself — a DAG it never saw leaves no record. The fleet-wide laptop backfill (07-28..08-26) shows what the filter excluded: `set_gaclid_enabled_flag` (31 failures), `ga4` (29), `url_pattern_identification`, `blocked_ip_addresses_export`, `conversion_signal_backfill_workflow` — none of these DAGs appear in any prod rca. Those are non-paging DAGs by design, but the two proven misses show the boundary was wrong at least twice. #1248 (merged, live 09-01 19:06Z) broadened PAGING_TAGS from 2 tags to 10 (`daily.py:35-46`); a fleet-vs-corpus reconciliation has not been run since.

Marker volume by DAG: vertical_classification_api 19, mntn_match_incrementals_fetch 6, keyword_ddp_reporting 5, mntn_match_incrementals_submit 1.

## 8. Cross-check against incident_log.jsonl

Overlap window (incident log ends at INC-025, 2026-08-23): 3 incidents, all represented in the corpus.

| incident | corpus row | agreement |
|---|---|---|
| INC-023 08-20 keyword_ddp/write_targeted_signal_ds_13 | rca_08-20: analysis_exception TABLE_OR_VIEW_NOT_FOUND prod.ml.ddp_url_verticals, high | Surface matches; the incident-log verdict (`resource_contention`) carries the deeper cause the debugger's log-local view cannot see. |
| INC-024 08-20 fangorn_hhid/challenger | rca_08-21: model_alias_not_found, high (try2, recovered) | Match; one-day offset from try windowing. |
| INC-025 08-23 fangorn/challenger (quota, settled by audit-log arithmetic) | **absent from rca_08-23** (3 candidates only); appears rca_08-24 as quota_exhaustion, high | Debugger's eventual verdict agrees with the settled human verdict (quota, not stockout). The day-of miss is consistent with either try start_date windowing or the pre-#1248 tag scope. |

Corpus decay direction: the human log stops at 08-23 while the debugger carries 27 dag/task pairs since — the log was superseded, as intended. But the **vendored** corpus in the prod image still ends at INC-024 (08-20), nothing syncs it (by its own docstring, `incident_match.py:74-76`), and the 45-day staleness alarm will stay silent until early October on a corpus whose whole life is ~5 weeks.

## 9. What to fix about the SYSTEM — ranked

1. **Coverage is still allowlist-shaped; run a fleet-vs-corpus reconciliation daily.** Evidence: both real missed replies were tag blind spots invisible to every internal check (zero corpus rows, healthy rapid runs, zero slack errors); the fleet backfill names 5+ DAGs the corpus has never seen. #1248 widened the list but a list it remains — the next new/renamed/untagged DAG fails silently. Cheap fix: daily sweep also pulls `failed_task_instances(ds, tags=None)` and reports (not diagnoses) the fleet-minus-scope remainder, so an uncovered failing DAG is a visible line, not a void.
2. **Chronic repeats need escalation, not re-answering.** Evidence: one known one-line defect (response_tests 45m limit) produced 30 diagnoses, ~19 rapid replies, and 5 days of `already_ticketed` dedup with zero escalation pressure; the joined run got 3 separate replies at try5/try10/try15. #1251's digest collapse (merged 09-01) fixes the *rendering*; nothing yet raises "same signature ≥N days, ticket already filed" to a priority bump or owner ping. Add an age/repeat escalation rule on the triage side.
3. **`empty_logs` candidates are counted but not identified — 38 of 128 candidates (30%) left no per-task record**, spiking to 11/day exactly during the biggest incident (upstream_failed fan-out). Nobody can audit today whether an empty-log candidate deserved a reply. Record dag/task/run/state for empties (a one-line list in the JSON), tag `upstream_failed` explicitly, and fold the count into the digest.
4. **Two missing days and no backfill path.** rca_08-22 and rca_08-26 don't exist; nothing noticed except a gap-check script that (a) checks for *same-day* rca_D although the daily DAG publishes D-1 at 17:01Z — so it cries wolf on every healthy day (08-27/08-28 entries were this false alarm) and can't distinguish the real miss — and (b) crashed on `gsutil` not in launchd PATH (traceback preserved in `gaps_2026-08-28.md`). Fix: gap-check tests rca_(D-1) after 17:15Z; daily DAG gets catchup or a manual-backfill runbook line; backfill 08-22/08-26 if their logs still exist (10-90d TTL, likely yes).
5. **Promote the pre_cache_verticals shape to a signature.** Evidence: 11 unclassified rows over 9 days, identical narrative ("pod did not reach Running inside its 120s budget"), logs already published to `unclassified/` four separate days for exactly this purpose, and it is 29% of all unclassified debt. The report text literally states the condition; it should be a signature (pod-start-timeout family) with the eviction/capacity routing, closing the biggest unclassified class and improving the digest's grouping.
6. **Cross-DAG root-cause walk is still one layer short where it matters most.** Evidence: all 3 keyword_ddp sensor rows and the joined late-data rows during the OpenAI outage are correct but symptom-level; the org-side OpenAI cause was established by hand over 3 days. #1249 shipped the sensor-target extraction (partial IMP-096); the walk to the *producing* DAG's failure (and a vendor-status probe for the OpenAI path) is the remaining depth. The 08-31 `openai_results_cohort_missing` first-day fire shows the signature half is landing.
7. **Sync the vendored incident corpus.** Evidence: prod image corpus ends INC-024 (08-20); INC-025's settled quota verdict — which the 08-24 debugger row independently reproduced — is not in the match corpus; `_STALE_DAYS=45` will not warn until October. Read the corpus from GCS at sweep time (same pattern as markers) or sync it in deploy CI; drop the threshold to ~14d.
8. **Self-report cosmetics:** `published` is serialized before it is set (`daily.py:296-311` writes the JSON, then the caller assigns `out["published"]`), so every rca file in GCS claims `published: []` — harmless but makes the file lie about itself; and `slack_threaded=0` for every daily sweep since 08-27 is by design (rapid owns threading) but reads like a defect in the JSON. One-line fixes.

## 10. Dead ends and caveats

- `gsutil -m cp` hung twice on the parallel path (0-byte `.gstmp` after 2+ min); killed and re-copied sequentially. Known Mac/LibreSSL behavior.
- Could not verify tag-scoped failure counts for the missing days 08-22/08-26 (would need Airflow TI history by date; fleet counts from the backfill used instead).
- The marker `try` numbers vs daily `try_number` for `batch_submit` (marker try5, daily row try4 final-state success) were left as recorded; consistent with a clear bumping max_tries mid-recovery on 08-29, not re-derived.
- Daily-vs-rapid double-post check is corpus-side only (markers + skip flags); Slack channel history itself was not re-read this session. The 08-29 channel audit in `audi_1191_missed_replies_2026_08_29.md` found zero un-replied alerts in alerts-tpa-pipeline, matching this reconciliation.
- summary.md §7i says #1248 raised the response_tests timeout to 68m; the merged value is 55m (`ddp_vertical_classification_api.py:23`, and next_actions item 5 agrees). The 55m figure is used here.

## 11. Inputs

- `gs://mntn-data-archive-prod/debugger/rca_{2026-08-20..2026-08-31}.json` (10; local copies in session scratchpad `debugger_rca/`)
- `gs://mntn-data-archive-prod/debugger/delivered/` (31 markers), `unclassified/` (26 logs), `cycle_watermark.json`
- `~/Developer/work/mntn/airflow-ti/include/airflow_debugger/{daily,rapid,markers,notify,incident_match,pull}.py`, `dags/airflow_debugger_{daily,rapid}.py`, `dags/machine_learning/ddp_vertical_classification_api.py`
- `~/Developer/work/mntn/airflow-ti/include/airflow_debugger/incident_log.jsonl` (24 rows) and `workspace/on-call/incident_log.jsonl` (25 rows)
- `workspace/on-call/gap_checks/gaps_2026-08-{26,27,28}.md`
- Ticket records: `outputs/audi_1191_missed_replies_2026_08_29.md`, `audi_1191_backfill_30d_2026_08_27.md`, `audi_1191_every_failure_2026_08_27.md`, `audi_1191_next_actions_2026_08_31.md`, `summary.md` §7g-7m

## 12. Cross-system: optimizer (AUDI-1194) overlap

Compared against the optimizer ledger's 85 dag_id values (full history, `mntn-prj-prod-00.optimizer.optimization_ledger`; the optimizer's dag_id is often the Spark app name, so mapping is by task/app identity).

**Jobs appearing in both systems' records:**

| family | debugger side | optimizer side |
|---|---|---|
| tpa_ipdsc_export | ipdsc_ds_49: 2 unclassified retry-recovered flakes (08-21, 08-24) | ipdsc_ds_{2,13,17,35,42,46,47,49,67} apps, 89 rows, chronic disk_spill/shuffle; most last seen 08-27 (archive blackout), ipdsc_ds_13 live to 09-01 |
| ipdsc_monitor | monitor_ipdsc_42: 1 unclassified recovered row (08-28) | ipdsc_{14,42,46,49}_monitor apps, 71 rows; 14/42 live to 09-01 |
| audience_intent | 2 unclassified recovered rows 08-24 (data_aggregation.prospecting_active_campaign_categories, household_score_distribution_monitor) | "ETL Audience Intent - *" apps, 5 ids, 61 rows; Prospecting Keywords is a top-10 chronic at 843.6 exec_h. Name-level family match, app-to-DAG link unproven |
| fangorn (weak mapping) | fangorn_hhid_inference_pipeline_run + fangorn_inference_pipeline_run, 4 recovered rows (model-alias, quota, stockout, 1 unclassified) | fangorn_{predictions_vertical, household_predictions_vertical, prospecting_scoring, score_monitor}, 61 rows incl. the one applied fix. Same product family, app-to-DAG link unproven |

**Do failures and findings point at the same jobs? Mostly no — the two systems see nearly disjoint fleets.**
- The debugger's top 3 offenders (vertical_classification_api 34 rows, mntn_match_verticals_precache_v1_1 17, mntn_match_incrementals_submit 14 — 72% of all diagnosis rows) have zero optimizer rows ever: they are Databricks-API/dbt/pod/OpenAI-batch jobs, i.e. exactly the optimizer's blind spots (dbx surface 0 rows, DATABRICKS_WAREHOUSE blocked; BQ surface only ever emitted 2 dags).
- The optimizer's top offenders (intent_score_map, materialize_mntn_select, AugmentorLogDsid30Processing, segment-updates-to-parquet, bos__spend, intent_score_threshold_v4) have zero debugger rows: chronically inefficient but not failing. Caveat: pre-#1248 the 2-tag PAGING_TAGS scope could also hide their failures, so absence from this corpus is not proof they never fail.
- Where the fleets do meet (ipdsc family, audience_intent), the signals are complementary, not duplicative: the debugger records retry-recovered infra flakes on the same jobs the optimizer flags as chronic spill/shuffle waste. No job in either record has a terminal failure and an efficiency finding tracing to the same defect.

## 13. Spot-check verification (2026-09-01 adversarial pass)

Re-derived from the 10 `rca_*.json` files and GCS listings; **no corrections needed.** Confirmed exact: the §3 per-day table (all 10 rows; totals 128/90/38/52/38/40/43/23; skip 3/6/7/7 on 08-28..31 via per-row `slack.skipped` fields, and zero delivery-era rows with sent=false and no skip); §5 signature distribution (29 task_execution_timeout down to the 6 singletons, 52 total; similar-incidents 75/90); the terminal/classified alignment (all 40 terminal rows confidence=high, all 38 unclassified rows retry-recovered, 50/90 final_state=success); §4 DAG totals (34/17/14/6/4/4; 13 DAGs, 27 pairs; response_tests 30 rows = 26 timeout + 3 dbt_test + 1 pod_evicted over 9 of 10 days; spot_preemption both on auto_assign_verticals); §7 marker inventory (31 = 19 vertical_classification_api + 6 fetch + 5 keyword_ddp_reporting + 1 submit) and `unclassified/` (26 logs, 5 days).
