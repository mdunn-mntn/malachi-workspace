---
name: project_airflow_debugger
description: AUDI-1191 airflow_debugger/ — key-free deterministic RCA for FAILED Airflow tasks (Dataproc + Databricks); Phase 1 complete, live-fires through INC-014 (2026-08-08); hardened 2026-08-06 by full-corpus adversarial review (40 confirmed defects → 37 fixed); IMP-030 troubleshooting pack shipped + hardened 2026-08-08; leftovers closed 2026-08-20 (Vertex signature, committed corpus sweep 55%->85%, DNS fallback verified live; Phase 3 held); SHIPPED as a prod DAG and verified end to end 2026-08-24, with masks.py closing the 'deepest error is not the cause' failure mode; Slack delivery wire (PR #1230) merged and verified live 2026-08-27 (3 posted, threaded); 30-day backfill validated 2026-08-27 (173 failures, 94.7% root-caused), cassandra_invalid_request signature on PR #1233, 14 triage tickets AUDI-1227..1240; rapid 15-min replies (PR #1239) + in-DAG Jira triage filer (PR #1240) merged 2026-08-27/28; Confluence content now on TI On Call Playbook 2908061697 (3769991216 is a redirect stub); 2026-08-28 #1242 fallback channel + #1243 GCS-JSON-API markers merged (gsutil unauthenticated in Astro pods), #1244 open, rapid DAG live and verified, AUDI-1249 auto-filed in prod; round-2 PRs #1248+#1249 MERGED and deploy-verified 2026-08-31 (deploy_prod CI 17:37/18:22 UTC; cycle_watermark.json rewritten by prod, IMP-095 closed); digest PR #1251 open (one parent per sweep, threaded RCAs, duplicate collapse), demo live in #airflow-debugger; 2026-09-01 #1251 MERGED + LIVE on prod image deploy-2026-09-01T19-06-22 (retrigger PR #1254 after the Astro superseded-build gap), post-deploy rapid cycle clean on the new code; 2026-09-01 (later) trigger PR #1256 open — failure-triggered rapid sweep via an on_task_instance_failed listener plugin (fires in the task-runner process for FAILED/UP_FOR_RETRY; Airflow wraps listeners in try/except so they cannot fail the task); 2026-09-01 (evening) #1256 merged via COMBINED PR #1258 (with #1255+#1257) LIVE on deploy-2026-09-01T22-22-40 — failure-trigger plugin REGISTERED in prod (GET /plugins lists airflow_debugger_trigger with its listener); 12-day full-history diagnosis written (outputs/audi_1191_diagnosis_2026_09_01.md: 128 candidates, 90 diagnosed, 52 high-conf; days 08-22/08-26 never swept; debugger+optimizer fleets near-disjoint, top-3 offenders = 72% of diagnosis rows have zero optimizer rows ever); 2026-09-02 parse-rate canary canary.py built + folded into PR 1260 (retitled twice, final: 'downloader loses the batch; parse canary; digest numbered list', 3 commits) — key-free detector for silent log-format drift (today's empty+unclassified fraction vs 7-day published norm, fires past max(2x, +25pts), notify.post_note, rca json canary key, 275 tests); 15-min rapid schedule KEPT as backstop for listener-invisible failures, hourly stretch after ~a week; LLM recommendation layer = open security question. Optimizer half split to AUDI-1194 / airflow_optimizer/ on 2026-08-05
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [airflow debugger, AUDI-1191, PR 1230 merged, slack delivery wire, spark failure rca, dataproc rca, databricks rca, cloud logging dataproc, dbx run_id correlation, operator engine map, oncall automation, ttl_exceeded, orchestration-only, signatures taxonomy, bluf star report, adversarial code review, order-integrity test, full-corpus sweep, code review findings archive, INC-013 live-fire, pihole dns block, logging.googleapis.com blocked, curl resolve pin, cloud logging dns blocked mac, IMP-030, troubleshooting pack, fix_pr, fix_files, code_links, --troubleshoot, build_troubleshooting, basename collision, duplicated basenames, framework frame filter, known-fix identity gate, vertex code 9 unclassified, vertex_pipeline_task_failed, INC-014 live-fire, corpus sweep tool, airflow_debugger sweep, 991 logs, batch_id_attach_trap, impersonation_unavailable, slack_notify_failed, task_execution_timeout, dbt_model_runtime_error, downstream_job_no_local_cause, None-1 batch id, test_perf_profile no main block, pinned curl verified, LAN sinkhole rejected, IMP-051, IMP-052, IMP-053, phase 3 held, include-recovered, ti_state, empty log worker death, batch_cancelled, batch_id_missing, dag_not_found_at_startup, task_externally_terminated, never open a PR, read-only github, 14-tab workbook, INC-024 live fire, 30-day backfill, cassandra_invalid_request, InvalidRequest code 2200, PR 1233, debugger_triage, AUDI-1227, triage tickets, TPA Pipeline On-Call Reference, confluence 3769991216, SLACK_ALERT_CHANNEL, PR 1239, PR 1240, airflow_debugger_rapid, rapid replies 15 min, exactly-once gcs markers, debugger delivered markers, debugger unclassified, triage.py, in-dag jira filer, JIRA_API_TOKEN astro var, AUDI-1054 tech debt epic, TI On Call Playbook 2908061697, nextPageToken paging, api 2 search removed 410, bug two put conversion, TRIAGE summary dedup, ReauthUnattendedError, PR 1242, PR 1243, PR 1244, SLACK_FALLBACK_CHANNEL, C0BT9TKRMKM, gsutil unauthenticated astro pods, gcs json api markers, rapid duplicate spam purge, PATCH is_paused, AIRFLOW_BEARER, deploy rollout race, priority rubric P1 P2 P3, AUDI-1249, jira service account robin fox, IMP-087 cursor pagination, SLACK_ALERT_CHANNEL comma list, monitor-tpa threading, fangorn_household_14day_lookback, PR 1249, audi-1191-debugger-round2, openai_results_cohort_missing, openai_batch_state_guard, dataproc_await_died_no_payload, vertex_await_died_no_payload, await died no payload, fast-fail sensor rca, external_task_rca on_date fallback, _run_holding day-scan, cycle_watermark.json, rapid lookback watermark, IMP-095, IMP-096, recognized a known failure pattern, monitor_ipdsc_42, challenger_inference, PR 1251, digest parent threaded replies, duplicate collapse counted line, defer_fallback, post_digest, rapid task id reply, watermark verified prod 1930 utc, deploy_prod ci runs, PR 1251 merged, PR 1254 retrigger, deploy-2026-09-01T19-06-22, astro superseded build, PR 1256, on_task_instance_failed, listener plugin, trigger.py, airflow_debugger_trigger_plugin, task-runner process listener, AIRFLOW_API_BASE, airflow 3.0.3, astro runtime 3.1-9, failure-triggered rapid sweep, plugins ships with image, parse-rate canary, canary.py, PR 1260, post_note, silent log format change, format drift, digest numbered list rich_text ordered, PR 1260 three commits]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-09-02
---

## SHIPPED AS A DAG 2026-08-21 — airflow-ti PR #1214, off the laptop cron

**Status 2026-08-24: PR #1214 is MERGED** (`504fe947`, 18:59Z). **LIVE and REGISTERED** on bundle
`2026-08-24T19:00:21Z`, schedule `0 17 * * *`, zero import errors — but **arrived PAUSED**, as new
DAGs do, so it will not run until someone unpauses it. The bundle was stamped ~1.5 min after the
merge yet only adopted ~25-40 min later, which is the lag to expect; see [[reference_airflow_ti]].
With `catchup=False`, `next_dagrun_logical_date` was already `2026-08-23T17:00`, so unpausing
fires a run immediately for the last closed day rather than waiting.

**Sean Yang pushed back on the PR's size** (28 files / 5,226 lines) and was half right:
`context_parse.py` is the Phase-3 in-callback tier, Phase 3 is held, and nothing in the bundle
imported it. Removed with its test (240 lines). The rest is 2,779 lines of vendored engine (moved,
not written), 2,125 of tests, 322 of DAG + CI + docs — genuinely new surface is 152 lines. Kept the
tests: they are what pin the five defects self-review found. **Vendoring a package is a good moment
to ask which modules the new entrypoint actually reaches** — held or deferred tiers travel along
silently otherwise.

**Prior status (2026-08-24, superseded above): out of draft and ready to merge.** Its identity blocker cleared —
`airflow-debugger@mntn-prj-prod-00` is live via Crossplane ([mntn-devops#4990](https://github.com/SteelHouse/mntn-devops/pull/4990),
merged + synced; my Terragrunt #4985 closed as superseded). Verified against live IAM, not the diff:
`dataproc.viewer` + `logging.viewer` on `mntn-prj-prod-00`, and `aiplatform.viewer` +
`dataproc.viewer` + `logging.viewer` on `mntn-targeting-prj-prod`. Bucket grants and the `debugger/`
prefix condition are manifest-verified only (`storage.buckets.getIamPolicy` is denied to
`malachi@mountain.com`). New IAM goes in Crossplane now, not Terragrunt: [[reference_mntn_devops_permissions]].

**Self-review before asking for a human found 5 blocking defects I had introduced**, all of them
environment assumptions that ruff + 106 tests + compileall could not see. Detail and the
generalizable lesson: [[feedback_review_own_pr_before_asking]] and
`tickets/audi_1191_.../outputs/audi_1191_pr_1214_self_review.md`.

**First live run (2026-08-21), with a real Astro deployment token: 7 failed tasks on 2026-08-20,
7 diagnosed, 4 root-caused deterministically.** The run immediately exposed a defect no test could:
the taskInstances POST takes `page_limit`/`page_offset`, so `pull.failed_task_instances` had
**never worked**. One real invocation beat 106 mocked tests.

**The vendored incident corpus is a PROJECTION, not a copy.** Wiz flagged colleagues' names
(`resolved_by`, `note`, `action`) in the first push. Fixed by projecting onto `CORPUS_FIELDS` — the
nine fields `incident_match` actually reads — with a test enforcing the allowlist, rather than an
ignore rule.

`airflow_debugger_daily` (`dags/airflow_debugger_daily.py`, package vendored at
`include/airflow_debugger/`), 17:00 UTC = 10:00 PT. Deliberately mirrors `spark_optimizer_daily`:
same identity, same vendoring, its own `pr_airflow_debugger.yaml` (the existing workflows filter to
`models/**`, so an `include/` library is otherwise never tested). The laptop `oncall_daily_rca.sh`
still works and stays the local entrypoint. All CI green on first push.

**Identity is a straight copy of AUDI-1194's, no new Terragrunt unit:** GSA
`spark-optimizer@mntn-prj-prod-00` impersonated from the deployment ADC `airflow-ti-prod@` via
`serviceAccountTokenCreator` + `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT`. See
[[reference_gcs_iam_creator_vs_user]].

**The generalizable finding: moving a workload into a DAG removes an API token only when the
workload's input is DAG metadata.** The optimizer's was, so its Astro token disappeared. The
debugger's input is another task's **log**, which on Astro Hosted lives in Astronomer's store; a
task's Task-Execution JWT is scoped to itself, and the deployment sets no
`AIRFLOW__LOGGING__REMOTE_*` that would put logs in a bucket the SA could read. So `AIRFLOW_BEARER`
is genuinely required (IMP-065). Check what the workload READS before promising the move frees it.

**A required credential can still be optional at runtime.** `pull.NoTokenError` is caught in
`daily.run`, which logs a skip naming the exact mint command and returns cleanly. The DAG therefore
merges and runs green before the token exists, and starts working the day it lands. That is the
pattern to reuse for anything gated on someone else's permission.

**What changed in the vendored copy** (everything else byte-identical): `synth.py` NOT vendored
(an `ANTHROPIC_API_KEY` on a prod worker is the decommissioned pattern; `orchestrate` catches the
`ImportError` and returns a low-confidence deterministic report); `sweep.py` NOT vendored (offline
tool); new `pull.py` + `daily.py`; `incident_match._CORPUS` searches beside the package first (a DAG
bundle has no `on-call/` tree, so the corpus travels with the package); `report._AIRFLOW_TI_LOCAL`
resolves to the bundle itself; `perf_profile` falls back to `include.spark_optimizer`.

**Open (neither blocking):** IMP-065 `AIRFLOW_BEARER` + `AIRFLOW_API_BASE`, minting needs
`WORKSPACE_OWNER` (Ryan Kleck). IMP-066 the SA's `objectUser` condition is scoped to `optimizer/`,
so publishing to `debugger/` 403s until widened; a publish failure warns rather than failing.
**SCOPE (as of 2026-08-05 ticket + package split):** this memory is the **DEBUGGER** — the failure-triggered RCA workflow, ticket **AUDI-1191**, package **`airflow_debugger/`** (parse, context_parse, signatures, dataproc_rca, databricks_rca, incident_match, report, synth, orchestrate). AUDI-1191 was retitled "Automated Airflow/Spark failure debugger (key-free RCA, Dataproc + Databricks)". The **OPTIMIZER** (success-triggered efficiency crawler) split out to ticket **AUDI-1194** / package **`airflow_optimizer/`** → see **[[project_airflow_optimizer]]** for the go-forward source of truth (eventlog parser, detectors, crawl, weekly cron, PHS access). The two packages are fully decoupled and share ONLY `eventlog.py`, which now lives in `airflow_optimizer/`. Paragraphs below that discuss the eventlog parser / optimizations / crawl / weekly cron are the **historical build record** from when both engines lived under AUDI-1191 — current optimizer facts belong in the optimizer memory.

Building an automated Airflow/Spark failure-triage agent under **AUDI-1191** (the build ticket AUDI-1190 §8 deferred; origin IMP-021). Code lives in the workspace at **`airflow_debugger/`** (key-free, no bot/tokens, no changes to SteelHouse work repos). Harvest source cloned read-only to `~/Developer/work/mntn/mntn-data-eng-assistant`. Approved plan: `~/.claude/plans/we-may-have-already-logical-ladybug.md`. See [[reference_data_eng_mcp]], [[reference_airflow_ti]], [[reference_databricks]], [[reference_oncall_runbook]].

**Why:** cut on-call MTTR + remove the Victor-shaped bus factor on Spark/Databricks debugging. Deterministic-first: code does log-fetch + signature-match; an LLM only synthesizes unknown cases.

**How to apply (Phase 1 done — works today, no LLM for known signatures):**
- Full chain: `python3 -m airflow_debugger.report <airflow_log_file>` → parse → route → diagnose → ≤500-char BLUF/STAR report. Modules: `signatures.py` (regex taxonomy), `databricks_rca.py`, `dataproc_rca.py`, `parse.py` (router+synthesis), `report.py`. Offline tests in `airflow_debugger/tests/`. Package is ruff-clean.
- **Engine routing** from the log's `op_classpath`: `DbxDbtOperator`/`DatabricksSubmitRun`/`ModelPysparkDbxJob` → databricks; `ModelPysparkBatch`/`DataprocCreateBatch`/`TiPysparkBatch` → dataproc; else `other` (sensor/python — not Spark).
- **Job-id correlation:** Dataproc `Batch job <batch_id>`; Databricks run_id from the dbt-databricks adapter line `Job submission response={"run_id":N}` (NOT the Airflow run_id).
- **Cross-layer synthesis (`diagnose()`):** if the Spark job SUCCEEDED but Airflow failed → **orchestration-only** (use the Airflow-log signature, e.g. pod-404). This auto-reproduced INC-009's hard-won reconciliation.
- **Databricks access (Phase-0 resolved):** CLI profile `malachi@mountain.com` (U2M OAuth); `databricks jobs get-run <run_id>` → state, `get-run-output <TASK run_id>` → root error. Detail in [[reference_databricks]].
- **Dataproc access:** `gcloud` user creds; driver text via **Cloud Logging** first, then (IMP-028, shipped 2026-08-06) the staging `driveroutput.*` glob named in the batch `stateMessage` when Logging yields no error text (`driveroutput_uri` + `_driveroutput_text` in `dataproc_rca.py`; head+tail cap keeps the MCP breadcrumb AND the failure). The staging bucket (`dataproc-staging-us-central1-995798185124-d8mf0cme`, distinct from the PHS temp bucket) denies EVERYTHING without the `dataproc-debug` PAM grant — even a direct object `get` reports a `storage.objects.list` 403 — so the fallback degrades to an actionable note naming the PAM unblock. A standing-grant ask must cover the STAGING bucket too, not just the temp bucket. On the real INC-012 driver text the fallback chain returns the full `gcs_list_timeout` verdict (fixture test `test_dataproc_rca.py`). TTL kills detected structurally (state CANCELLED + runtime≈ttl). Deep spill/skew profile needs the `.zstd` event log (often absent — `eventLog.dir` unset) + read-only `storage.objectViewer` on the prod dataproc-staging/-temp buckets (requested, not a blocker).

**Validated:** INC-005 (Dataproc) → `ttl_exceeded`; INC-009 (Databricks) → `orchestration/pod-evicted` (run 65237255325756 SUCCEEDED). Both match the runbook verdicts.

**Real-prod RCA validation (2026-08-04):** ran the classifier on 3 live #airflow-ti-alerts failures (2026-08-02/03, all tied to new `data_source_id=67`). Found + closed 2 gaps → taxonomy 22→24: **`invalid_output_path_config`** (`Invalid GCS bucket name|<bound method|bucket name must contain only`, code-error, fix=yes) = the DS67 model bug (bound method `write_location` passed instead of `write_location()`); extended `path_not_found_late_data` for "Missing required ... partition". Result: system named `ipdsc_ds_67` as ROOT (code fix) + the other two as downstream missing-partition symptoms — the **`programmatic_fix=yes` flag separates fixable root from symptoms**. Matches Sean's ground truth (skip ds67 quick fix; real fix = DS67 code bug). Note: live `analyze_batch` (Dataproc describe) needs fresh `gcloud auth login` (SSO expired); classifier/report path unaffected.

**Live-fire armed + first catch (2026-08-03):** daily retrospective RCA cron (`.claude/scripts/oncall_daily_rca.sh [date]`) over paging tags `tpa`+`Machine Learning` (all 6 corpus DAGs), `failed`/`upstream_failed`, writes `<log>.rca.md`. Chose a daily cron over `--watch` because watch is day-pinned + needs babysitting; added `--diagnose` to day-dump `list` mode (was watch-only) to enable the batch pass. First run (2026-08-02) caught a real `vertical_classification_api.response_tests` failure → surfaced a taxonomy gap (dbt data-quality test) → added `dbt_test_failure` signature (now 22). Astronomer metadata retains ~5+ days but re-run-to-success failures drop from live `--state failed` (INC-005 no longer present), so retrospective validation on OLD incidents is limited — validate on fresh failures. Small known gap: identity="unknown task" for KubernetesPodOperator dbt logs (parse from filename as fallback).

**Taxonomy covers the full §2 corpus (2026-08-03):** signatures went 14→22 after auditing against all 9 runbook §2 incidents + the first live catch — the original 14 only matched the 2 clean-Spark cases, but most on-call alerts are orchestration/late-data/capacity. Added `path_not_found_late_data` (INC-004), `vertex_param_contract` (INC-003), `cluster_create_stockout`+`quota_exhaustion` (INC-002/008), `openai_file_quota` (INC-007), `sensor_timeout` (INC-001/006), `external_task_failed` (INC-007); each has a test case + ordering guards (PATH_NOT_FOUND before generic AnalysisException; openai quota before generic quota). No router change — `classify()` runs on the whole log body and `diagnose()` falls back to the Airflow-log signature on the `engine=other` path.

**Use-case #2 (optimization) STARTED (2026-08-03).** The two asks are (1) automate failure triage→PR AND (2) find query-optimization wins from logs/plans — the latter applies to SUCCEEDED jobs too. Honest gap: current analyzers pull failure state+signature, NOT the Spark query plan/per-node metrics/optimizer-stats. Built the detector brain `airflow_debugger/optimizations.py` (plan-text parser + detectors: `missing_statistics`, `broadcast_candidate`, `shuffle_partition_sizing`, `window_full_sort`, `repeated_scan`; impact+evidence+fix, ranked), validated on the REAL `write_targeted_signal` plan (INC-009's Databricks job) — surfaced missing stats on product_categorization (13.5B rows → ANALYZE TABLE) + a 182 GiB shuffle to repartition. **Acquisition PROBED + BLOCKED (2026-08-03):** the rich Spark plan/metrics are NOT reachable key-free for a COMPLETED Databricks job cluster — `get-run-output` has only error/notebook_output (no plan); driver-proxy REST = `400 Terminated`; `/sparkui/.../api/v1` = `500 TEMPORARILY_UNAVAILABLE`; `cluster_log_conf=None` (no event log persisted); dbt python model so not in SQL query history. So automated collection needs a **prod-config enablement** (the user greenlit this). Concrete levers (spec: `artifacts/audi_1191_optimization_data_enablement.md`, backlog IMP-023): **Dataproc** = `spark.eventLog.enabled=true`+dir+compress in batch `runtime_properties` (`spark_job_monitor.py:144` already warns it's off) → `.zstd` → `eventlog_profiler.py`; + persistent Spark History Server (`spark_history_server_config`, `ipdsc_emr_cluster.py:68`) for history. **Databricks** = `EXPLAIN COST`/`df.explain(mode="cost")` in the dbt model (→ stdout → get-run-output, lightest) OR `cluster_log_conf` on `new_cluster` (`operators.py:481`) OR enforce via cluster policy `001D160AE4052091` (org-wide). Historical pre-change runs uncollectable (going-forward only). Next = crawl once logs flow.

**Complete Spark data inventory (2026-08-03, `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_spark_data_inventory.md`):** the analyzer consumed 1 of 7 Spark data surfaces (plan text). Full surface = 7 REST endpoints (jobs/stages/stage-task-summary-percentiles/executors/environment/storage/SQL-per-node), and the **event log is ONE artifact holding all 7** → IMP-023 "enable event log" = 100% capture (not partial). Highest-value signal is NOT in the plan: stage spill+shuffle bytes, **task-level skew percentiles (max≫median)**, executor failedTasks+GC+peakMem, full config (spot availability, shuffle.partitions). Saved Spark-UI HTML = SPA shell only (no data) → UI-scraping is a dead end, REST/event-log only. Databricks + Dataproc identical (both Apache Spark; a persistent Spark History Server serves `/api/v1/applications/...` for terminated runs). New detectors to add: skew, disk_spill, spot_preemption_cost, gc_pressure, cache_ineffective, shuffle_fetch_instability, default_shuffle_partitions.

**Full event-log parser BUILT + validated on a REAL event log (2026-08-03):** to prove systematic extraction, generated a real Spark 4.0 event log locally (installed **openjdk@17** for pyspark; `JAVA_HOME=/opt/homebrew/opt/openjdk@17`; generator `airflow_debugger/tests/fixtures/gen_eventlog.py`; fixture `eventlog.zstd`). `airflow_debugger/eventlog.py::parse_eventlog()` → structured `SparkRun` across all 7 surfaces (env props, stage input/output/shuffle/spill/GC + per-task **skew ratio**, executors failed/GC/removal-reason, SQL plan + **per-node metrics via sparkPlanInfo accumulatorId↔Accumulables/DriverAccumUpdates join**). Handles `.zstd` (dir/file). Field names verified against the real log. `optimizations.py::analyze_run()` emits 3 rec types — **code** (skew/spill/shuffle-partitions), **infra** (gc_pressure, spot_preemption_cost), **failure** (fetch instability) — each with real numbers + fix. Validated: skew 12.2x caught on the skewed stage (invisible in plan text); demo on real targeted_signal (`tickets/audi_1194_.../artifacts/targeted_signal_demo.py`) yields ANALYZE-TABLE + repartition (code) + spot on-demand (infra) + FetchFailed (failure). Tests `tests/test_eventlog.py`; 6 test modules pass, ruff-clean.

**Storage/cache surface completed → 7 of 7 (2026-08-04):** cached RDD bytes/blocks + evictions parsed from `SparkListenerBlockUpdated` (`Block Updated Info`→`Block ID` rdd_*, Storage Level, Memory/Disk Size). **Requires `spark.eventLog.logBlockUpdates.enabled=true`** — the event log OMITS block updates by default (verified: RDD Info `Number of Cached Partitions`=0 without it). **SUPERSEDED 2026-08-04: Dataproc Serverless REJECTS `spark.eventLog.logBlockUpdates.enabled` (`INVALID_ARGUMENT: unsupported properties`) → this storage/cache surface is UNCAPTURABLE on Dataproc Serverless; `cache_ineffective` has no data source there. It IS valid on a managed cluster.** `cache_ineffective` detector (rec_type infra) fires on evictions. Validated on a real cache fixture (`tests/fixtures/eventlog_cache.zstd`, 4 cached blocks). All 7 Spark surfaces now parsed + tested on real event logs.

**Optimization UNBLOCKED — real prod event logs already exist (2026-08-04):** Ryan pointed to **`gs://mntn-data-archive-prod/spark-events/`** (49 `.zstd`, Nov 2025 window; he wants a TTL/cleanup). **Download gotcha: `gcloud storage cp` corrupts `.zstd` (crc32c/decompress gatekeeper → 0 bytes); use `gsutil -o "GSUtil:check_hashes=never" cp`** (gcloud `-m` bulk crashes here). Crawl validated on 13 real prod jobs, labeled by `spark.app.name` (event log self-identifies). Real finding: **`Update Vertical Categorization` chronic Stage-0 skew up to 242x** (every run 10-242x) = #1 fleet target; `Prepare HTML Content` 18.4x. `crawl.py` now labels by app_name (`optimize.analyze_eventlog()` returns (run, findings)). Ryan's 2 enhancements to `spark/utils/spark_job_monitor.py` (Victor's `SparkJobMonitor`): models should call `log_script_content(__file__)` (→ `MCP_SCRIPT_BASE64` Cloud-Logging breadcrumb, maps app-id→exact `.py`) + `log_execution_plan(df)` (Physical/Optimized/Analyzed plans + missing-stats advisory). See [[reference_airflow_ti]].

**Ryan enablement meeting (2026-08-04, transcript in ticket `meetings/`):** event logging is **OFF** (Ryan turned it off after Nov-2025 test). **PHS ⊕ eventLog mutual exclusion:** can't run `spark.eventLog.enabled=true` AND a persistent history cluster on the same Dataproc batch. Turn-on path (`artifacts/audi_1191_enablement_steps.md`): set `spark.eventLog.enabled=true`+dir→`spark-events`+`logBlockUpdates.enabled`, turn OFF the PHS on the ~2 jobs using it (`ipdsc_emr_cluster.py:67`, only Victor used it → low-risk). GCS write perm already set for airflow-ti. **TTL age 30 on `spark-events/` + delete-all APPROVED** — I lack `storage.buckets.update`, Ryan/admin applies (rule staged `scratchpad/lifecycle_proposed.json`). **Databricks:** DBX user can't write GCS → enable setting, see error, Cursor→mountain-devops PR → Cristina approves (skip DevOps ticket). **2 extras:** `log_execution_plan` + `log_script_content` (version tracking) in a shared BaseModel. **Adoption low-key** (no big ticket, share when polished); future **Scala Spark** (identity team, event logs engine-agnostic). Scope: `tickets/audi_1194_.../artifacts/audi_1194_optimization_analyzer_scope.md`. **Daily RCA cron INSTALLED:** launchd `com.mntn.oncall-daily-rca` (10:00 PT), plist in the ticket artifacts.

**Dev end-to-end validation of the eventLog PR (2026-08-04) — PASSED + 2 prod-breaking bugs caught in dev:** deployed PR #1169 to dev (cherry-picked onto `dev` branch), ran `feature_store_hourly` (an hourly Dataproc feature-store DAG, Ryan/Sean's suggested small test job) → **the dev SA `airflow-ti-dev@mntn-prj-dev-00` writes real event logs to `gs://mntn-data-archive-dev/spark-events`** (`app-*.zstd.inprogress`, multi-MB, both hourly tasks). **SA-write CONFIRMED** (prod SA→prod bucket already proven historically). Two bugs found ONLY because we tested in dev (would have broken prod): **(1) `airflow.sdk.Variable.get(key, default=..., deserialize_json=...)` takes `default=`, NOT `default_var=`** — the classic `airflow.models.Variable.get` used `default_var`; Airflow 3 SDK renamed it. Wrong kwarg → `TypeError` in `execute()` BEFORE batch submit → fails EVERY Dataproc DAG (central operator). **(2) Dataproc Serverless REJECTS `spark.eventLog.logBlockUpdates.enabled`** (`INVALID_ARGUMENT: Attempted to set unsupported properties`) — only `spark.eventLog.enabled`/`dir`/`compress` are on its property allowlist. **So the RDD block-update / cache-storage surface (surface 7) is NOT capturable via Dataproc Serverless event logs** — supersedes the earlier IMP-023 note to add logBlockUpdates for cache stats; `cache_ineffective` detector has no Dataproc data source. Both fixed in PR #1169 (4 commits). **Dev deploy mechanism learned:** dev Airflow AUTO-picks-up on push to the `dev` branch (Astro git-integration bumps the DAG-bundle version v1→v2→…; the GH "Deploy to Dev" workflow only does the GCS code copy, NOT the bundle). Code root resolved by `dags/current_branch.json` (gitignored, local-only) → defaults to `dev`; a stale bundle can pin an old branch (`ds63`) whose GCS root was cleaned up → "file not found". No manual `astro deploy`. See [[reference_airflow_ti]].

**Post-meeting execution (2026-08-04) — the 8 numbered steps started:** **#7 optimizer cron LIVE (mine) — [SUPERSEDED 2026-08-20: now DAILY, `.claude/scripts/oncall_daily_optimizer.sh` + launchd `com.mntn.daily-spark-optimizer`, cap 200, PHS half wired in; see [[project_airflow_optimizer]]]:** originally `oncall_weekly_optimizer.sh` + launchd `com.mntn.weekly-spark-optimizer` (Mon 11:00 PT). Pulled newest ≤40 logs from `spark-events` (key-free gsutil `check_hashes=never`, one-at-a-time), runs `airflow_debugger.crawl`, writes `outputs/optimizer_backlog_<date>.md`. **Idles with NO git noise until enablement lands** (empty/denied prefix → exit 0); local-dir arg for testing. Verified on the 13 real logs (13 jobs/34 findings/242x top). **#1+#2 SHIPPED AS ONE PR — [airflow-ti#1169](https://github.com/SteelHouse/airflow-ti/pull/1169)** (open, base main, reviewer rkleck-mntn). **#1 eventLog:** central injection in `ModelPysparkBatchOperator.execute` (`include/models/operators.py`) so all **72** `@compute.dataproc_batch` models inherit `spark.eventLog.enabled/dir/compress` (logBlockUpdates dropped 2026-08-04: Serverless rejects it) — **env-aware dir** `gs://mntn-data-archive-{env}/spark-events`, **kill switch** Variable `SPARK_EVENT_LOG_ENABLED=false`. Two Dataproc submit paths exist: the 72 decorator models (no PHS) + the **ipdsc/tpa raw path** (`ipdsc_emr_cluster.py`, built via `ds_id/geo/tpa_export_spark_batch` → plain `DataprocCreateBatchOperator`) which had the **only live PHS** (`peripherals_config.spark_history_server_config`); the PR *as opened* added eventLog to `get_config` + removed the PHS — **[SUPERSEDED at merge: this ipdsc change was REVERTED; the PHS is KEPT. See the "PR #1169 MERGED" paragraph below.]** audience_intent PHS already commented + has an `include_peripherals_config` flag. **#2 observe:** guarded `BaseModel._observe_output(df)` invoking the existing `SparkJobMonitor` (methods already exist) from 4 concrete `df_write`s (`FileStorageBaseModel`/iceberg/feature/ipdsc; `signal_model` raises, read-only skipped); `MNTN_SPARK_OBSERVE=0` off-switch, deferred import (compile-safe). Built on a **git worktree off origin/main** (never touched the active `TI-956` checkout; `git worktree add/remove/prune`). `py_compile` clean; **airflow-ti has NO lint/ruff CI** (only deploy + trufflehog-scan). Ryan GitHub handle = **rkleck-mntn**. **3 distinct Dataproc submit paths (each injects properties separately):** (1) the Airflow operator `ModelPysparkBatchOperator.execute` (my #1), (2) the LOCAL runner `utils_runner/dataproc.py DataprocBatch.__init__` (hardcoded dev: `mntn-prj-dev-00`, SA `airflow-ti-dev@...`), (3) the ipdsc/tpa raw path. A plain `model_run.py` local run goes through path 2, NOT the operator — so I added the eventLog block to path 2 too (env-gated `SPARK_EVENT_LOG_ENABLED`) so local runs smoke-test the write path. **Rollout (default stays ON):** only breaking vector = Spark failing at SparkContext init if it can't write the eventLog dir (pipeline logic untouched); dev Airflow is a separate deploy = the natural canary (test one DAG there → promote to prod). Dev bucket `mntn-data-archive-dev` exists + models write output there (perm very likely fine). **Testing gotcha (2026-08-04):** cannot test the runtime eventLog write from the CLI OR local `model_run.py` — `malachi@mountain.com` lacks `iam.serviceAccounts.actAs` on `airflow-ti-dev@mntn-prj-dev-00` (and the default compute SA), and every Dataproc batch submit impersonates a job SA. So the impersonation-free runtime test = **dev Airflow** (Astro runner already has actAs; deploy branch → run one DAG → check for a `.zstd` in `mntn-data-archive-dev/spark-events`). To unlock the local `model_run.py` loop instead, grant `roles/iam.serviceAccountUser` on the dev SA (mountain-devops PR). Bucket confirmed writable by my user (uploaded+deleted a probe). Follow-ups: audience_intent raw batch, 3 `dataproc_workflow` templates, Databricks (mountain-devops GCS-write PR). Design: `artifacts/audi_1191_basemodel_observe_patch.md` + `audi_1191_enablement_steps.md`.

**Phase 3 trigger design STARTED (2026-08-03, no prod change).** Design spec: `tickets/audi_1191.../artifacts/audi_1191_indag_callback_design.md`. **Attach point (verified in airflow-ti source):** `include/job_config/job_config.py` centralizes all failure callbacks — `make_default_args` builds the task-level `on_failure_callback` list (line 145), `make_dag_args` the DAG-level one (line 199); contract = `Callable[[Context], None]`. Append ONE callback behind opt-in `Variable` flag `DEBUGGER_AUTOFIRE` (mirrors `pagerduty_send_enabled` in `job_env.py`). **Two-tier (forced by in-worker constraints):** in-worker first-look = identity + operator→engine + `classify(context["exception"])`, all zero-network + key-free (no ANTHROPIC_API_KEY in prod), never raises, emits a structured event; off-worker deep RCA (on-call box w/ creds) consumes it → full `orchestrate`. **Sanctioned-Slack unlock:** airflow-ti ALREADY posts to Slack from these callbacks via `SlackNotifier` over an org-blessed connection (`slack_messages.py:68`) — that's the sanctioned Slack path the separate Phase-3 Slack item was blocked on (integration lives in airflow, not our killed local bot). **Companion built (ours, offline-tested):** `airflow_debugger/context_parse.py::parse_context(ctx)` — Airflow-free pure fn proving the in-callback contract; `tests/test_context_parse.py` (5 cases). Gated on Ryan review; never push airflow-ti main.

**PR #1169 MERGED to prod (2026-08-04, merge cef446a3).** A 6-hypothesis adversarial prod-break audit narrowed scope before merge: the managed-cluster **workflow-operator** eventLog commit (`a140807`) was **DROPPED** (deferred to its own PR; touches the live scheduled `adv_score_live_cg_monitor`) and the **ipdsc/tpa** raw-batch path was **fully REVERTED** to main (untested + not kill-switch-covered) — **the PHS is KEPT** there (revert commit `d8b535c`). What shipped: batch-operator eventLog (`ModelPysparkBatchOperator.execute`, env-aware dir, kill switch `SPARK_EVENT_LOG_ENABLED` default `"true"`) + the local runner (`utils_runner/dataproc.py`) + guarded `BaseModel._observe_output` (`SparkJobMonitor` query-plan + script log, off-switch `MNTN_SPARK_OBSERVE=0`). Merge to `main` auto-triggered the "Deploy to Prod" GH workflow (prod bundle picks up main). **Audit verdicts:** config deep-merge SAFE (recursive `ConfigJsonHelper.update`/`spark_cfg_dict.update`, proven by a pre-existing fleet-wide prod call site), batch-path PHS collision SAFE (0/88 models attach a PHS), `_observe_output` SAFE (try/except-guarded, can't abort `df_write`), `Variable.get(default=)` correct for `airflow.sdk` 1.0.3. **PHS ⊕ eventLog reusable finding:** ipdsc/tpa batches ALREADY write event logs to `gs://{temp_bucket}/*/spark-job-history` via the attached PHS (`persistent_history_cluster.get(env)`; temp_bucket prod=`dataproc-temp-us-central1-995798185124-svhwvc6j`, dev=`...-411678625229-rfctkpug`) → the crawler reads them there (NO code needed); you can't set `spark.eventLog.*` alongside a PHS (Dataproc rejects, 400); the 88 batch-operator models attach no PHS so they get the archive-bucket eventLog. **Open follow-ups:** (a) workflow-op eventLog as its own PR after a dev `data_set_iceberg` run proves the managed-cluster write path (covers `data_set_a`/`data_set_iceberg`/`adv_score_live_cg_monitor`); (b) point the crawler at the ipdsc/tpa PHS temp-bucket `.../spark-job-history` prefix (prod+dev); (c) `spark-events` TTL age-30 + delete old (admin, `storage.buckets.update`); (d) Databricks GCS-write via a mountain-devops PR → Cristina; (e) crawl prod `spark-events` once logs flow + send the 242x vertical-categorization skew finding → Sean/DDP. ds67 `write_location()` bug FIXED by owner 2026-08-04 (main commit `a008b2e`); the debugger named it ROOT and the owner fixed exactly that, so it is dropped from the send list. Also: `#2 crawl ipdsc/tpa PHS logs` is RESHAPED, not a 1-line prefix scan (validated end-to-end 2026-08-05 under a 1h `dataproc-debug` PAM grant). The PHS event logs live per-batch at `gs://{temp_bucket}/<dataproc-batch-uuid>/spark-job-history/app-<appid>.zstd` — SPARSE (only PHS-attached ipdsc/tpa batches write them; the 88 batch-operator models write to `mntn-data-archive` instead) and scattered across thousands of unsorted per-uuid temp dirs, most empty. So a flat `**` scan is infeasible: the crawler must ENUMERATE ipdsc/tpa batches via `gcloud dataproc batches list/describe` (→ uuid) then read that uuid's `spark-job-history`. Format = `.zstd`, SAME as the batch-operator logs; `eventlog.py` parses them (verified on `Populate ipdsc_ds_67.DS67`, shuffle.partitions=1000). Download needs `gsutil -o "GSUtil:check_hashes=never"` (the .zstd gets decompressive-transcoding otherwise; `gcloud storage cp` fails the hash check). **Standing grant needed (Slack/mountain-devops → Cristina): `roles/dataproc.viewer` on `mntn-prj-prod-00` (enumerate batches) + `roles/storage.objectViewer` on `dataproc-temp-us-central1-995798185124-svhwvc6j` (read logs)** — the 1h PAM grant can't run the weekly cron. PAM propagation ~30s. Sean's INC-010 ds17 day-1 fallback shipped as PR #1172 (`e8010bf`). See [[reference_airflow_ti]].

**Phases 0-2 done (2026-08-03).** Phase 2 added `orchestrate.py` (deterministic-first entrypoint), `incident_match.py` (lightweight lexical matcher over `incident_log.jsonl` — chose it over `all-MiniLM-L6-v2`/torch for a 9-row corpus), and `synth.py` (LLM fallback for unknown signatures only). **Orchestration uses the Anthropic Messages API directly (`claude-opus-4-8`), NOT the full Claude Agent SDK** — the deterministic pre-processor does the extraction, so the LLM's job is one bounded synthesis call; the `claude` binary + agent SDK aren't installed and aren't needed. `ANTHROPIC_API_KEY` is the LLM-orchestration credential, separate from the key-free data layer; swappable behind `synth.synthesize` (e.g. ChatGPT later). **Wired 2026-08-03:** `airflow_pull.sh --watch --tag <tag> --diagnose` runs `orchestrate` on each dropped failure log and writes `<log>.rca.md` for `/oncall` (loose coupling: `airflow_api._run_diagnosis` subprocesses the orchestrator, no import; default `--no-llm` = key-free + zero API cost in the unattended loop; the 3-surface write-back stays `/oncall`'s single-writer job). **Remaining = Phase 3 (deferred/gated, backlog IMP-022):** in-DAG auto-fire callback (airflow-ti `JobConfig.make_dag_args`, prod, Ryan's review), sanctioned Slack thread-reply (no-bot policy), propose-only PR (claude-code-action) + adversarial reviewer. Hold until the read-only RCA is trusted in real use.

**Adversarial hardening (2026-08-06): 40 execution-confirmed defects → 37 fixed same day.** An 18-agent adversarial review of all 9 modules against the FULL real-log corpus (64 prod logs, `on-call/airflow_logs/2026-08-05` + `2026-08-06`) confirmed 40 of 41 claimed defects by execution; a 7-fixer wave fixed 37 with a regression test each (2 skips: one cross-owned, one refuted). **Parse rebuilt:** failed-run Dataproc batch-id wording is `Starting batch <id>` / lowercase `batch job <id>` — capital `Batch job` appears on SUCCESS only; real run_id preferred over the k8s pod-label mangle; `manual__`/`backfill__` run-id prefixes accepted; filename-convention identity fallback; operator detected from Airflow-3 log shapes because `op_classpath` appears in ZERO prod logs; `map_index` added. **Signatures tightened, count still 24:** `executor_lost` excludes 'spark scale down' decommissions, SocketTimeout must be GCS-bound, `dbt_test_failure` requires a real test marker, DEADLINE_EXCEEDED must be batch/ttl-bound, `driver_oom` span no DOTALL, quota/stockout engine=any — plus an ORDER-INTEGRITY test (`executor_lost` had stolen `gcs_list_timeout` on the real INC-012 driver blob, i.e. the tool would have repeated the exact human misdiagnosis it exists to prevent; see [[feedback_validated_is_not_correct]]). **orchestrate/synth:** an LLM error stub can never replace a deterministic report; structure-aware evidence truncation; raw log tail included in LLM evidence. **incident_match:** per-line JSONL guard; dag boost requires text overlap. **report:** URL-safe truncation. **databricks_rca:** TIMEDOUT/CANCELED count as failed, task pagination, missing result_state ≠ failed. **dataproc_rca:** gcloud stderr surfaced (the INC-012 replay now honestly reports the acquisition failure). Acceptance: 5 test modules green (`test_context_parse` + `test_synth_orchestrate` NEW), 27 classifier cases, ruff clean; real-log sweep identity 64/64, Spark job id 33/33, mangled run_ids 0. Findings archive: `tickets/audi_1191_airflow_spark_debugger/outputs/code_review_findings_2026_08_06.json` (needed `git add -f` — blanket `*.json` ignore, see [[reference_gitignore_json_rule]]).

**INC-012 CLOSED, prod-verified end to end (2026-08-06 evening):** the live-fire #5 catch ran the full arc. Fix v1 ([airflow-ti#1176](https://github.com/SteelHouse/airflow-ti/pull/1176), literal region paths) merged but proved INCOMPLETE in prod — the 16:45 PT run failed identically on the new code because the read's `basePath` option statted the same ~17M-object root (`getFileInfoInternal` list timeout). The second driver trace produced fix v2 ([#1177](https://github.com/SteelHouse/airflow-ti/pull/1177), drop `basePath`) the same evening; merged + prod-verified: the hh=23 re-run SUCCEEDED in 7.4 min (vs ~11.5 min historical healthy runs) and closed the dt=2026-08-06 data hole. Mechanism + generalized call-site-sweep rule: [[reference_airflow_ti]]; process lesson: [[feedback_validated_is_not_correct]].

**IMP-030 troubleshooting pack SHIPPED (2026-08-08) + hardened by a 6-agent adversarial workflow (2026-08-09).** What shipped: `fix_pr`/`fix_files` fields on resolved incident records; `incident_match` passes them + dag/task through; `report.py build_troubleshooting` + `code_links` map traceback frames to GitHub `#L` links via `git -C ~/Developer/work/mntn/airflow-ti ls-files`; `--troubleshoot` CLI. The review confirmed 3 high defects, all fixed with regression tests: (1) **basename-collision wrong-file link** — airflow-ti has 11 duplicated `.py` basenames (incl. `materialize_mntn_select.py` in BOTH `dags/tpa_export/` and `spark/data_source/`); the resolver now prefers `spark/` for `/var/dataproc/`/`/databricks` driver frames and SKIPS ambiguous basenames rather than guess; (2) **framework-frame leak** — the frame filter now blocks `/databricks/`, `/opt/`, `/pyspark/`, `/py4j/`, dist-packages, `__init__.py`; (3) **unrelated-PR known-fix claim** — top-match-only + a dag/task identity gate, because a 2-token query can score 1.0 on overlap. Example sheets removed from the how-it-works xlsx per user (5 tabs now). **Live-fire (INC-014, 2026-08-08): the `--troubleshoot` chain ran end-to-end on a real page the day it shipped** — classified `[high]` late-data/missing-partition, surfaced INC-010 as top similar, lifecycle root found via one bucket describe. **Known taxonomy gap:** Vertex code-9 boilerplate logs classify as UNCLASSIFIED (no `vertex_pipeline_task_failed` signature; INC-015's drift logs hit this). Test-fixture lesson from the collision defect: [[feedback_validated_is_not_correct]].

**INC-013 CLOSED — live-fire #7 (2026-08-07):** the debugger pipeline (parse → batch describe → driver log via pinned curl → signature) root-caused INC-012's class recurring in a sibling reader (`fpa_site_visit_batch_serverless/dsid30_augmentor_log_processing`) in ~30 min. Repo-wide call-site sweep found 2 more unfixed readers, one already silently degrading (green run, zero augmentor rows — [[feedback_validated_is_not_correct]]); one PR ([airflow-ti#1179](https://github.com/SteelHouse/airflow-ti/pull/1179)) fixed all 3 (literal region paths, drop basePath, existence guards), merged 16:22Z, deployed ~40s later, prod-verified same morning (15Z dsid30 retry ~6 min vs ~19-min deaths). Gap surfaced: on the augmentor_daily map13 Airflow log the orchestrator returned UNCLASSIFIED because the Cloud Logging fetch is DNS-blocked on this Mac — the signature only fired once driver text was fetched manually via the pinned curl. **Cloud Logging on this Mac is blocked by the user's Pi-hole DNS**: `logging.googleapis.com` resolves to 0.0.0.0 (IPv4 AND IPv6 both dead; `dataproc.googleapis.com` unaffected) — this CORRECTS the INC-012 read of a transient VPN/gcloud-IPv6 egress flake. Proven workaround: `curl --resolve logging.googleapis.com:443:142.250.73.106` with a `gcloud auth print-access-token` bearer on `POST /v2/entries:list` → HTTP 200. Consequence: `dataproc_rca`'s `gcloud logging read` stays broken locally until the user allowlists the domain in Pi-hole (aware, not yet done); a pinned-curl fallback in `dataproc_rca` is a candidate improvement, not yet built.

**Leftovers closed + full-corpus re-sweep (2026-08-20). Taxonomy 24 -> 31; classification on real failed logs 55% -> 85%.**
- **Vertex gap CLOSED.** `vertex_pipeline_task_failed` matches BOTH log shapes: real newlines in the older incident `.txt` captures (INC-002/008) and **literal two-character `\n` escapes** in current Airflow-3 `.log` files, where the `[error] task Task failed with exception` line is EMPTY and the payload survives only inside the `include.job_config.slack_messages` dict repr. A pattern anchored on a real newline matches the incidents and silently misses every prod log. Verdict points at the bracketed step + `job_id` (steps vary: `create-dataproc-cluster`, `submit-parallel-inference-jobs`, `submit-daily-drift-job`) instead of inventing a cause.
- **`airflow_debugger/sweep.py` — committed sweep tooling** (`python3 -m airflow_debugger.sweep [<glob>] [--out <path>]`). The 2026-08-06 "64 logs" sweep was ad-hoc and unrecoverable; this one is reproducible. Offline (reuses `parse_log_file` + `classify`; never `orchestrate.investigate`, which would hit live GCP 991 times). Corpus is now **991 raw `.log` files** (831 success / 84 failed / 59 upstream_failed / 14 skipped). Report: `tickets/audi_1191_airflow_spark_debugger/outputs/audi_1191_corpus_sweep_2026_08_20.md`.
- **Result:** classified 46/83 -> **71/83 (85%)** of diagnosable failures, +8 routable via job id = 79/83 (95%) resolved. Identity **991/991** via `parse_log_file` with 0 body/filename contradictions — but body-only extraction fires on 72/84 failed and **0/831 success** logs (`dag_id=` appears only in the failure-callback dump), so the **filename fallback is load-bearing**, not a nicety.
- **New signatures:** `batch_id_attach_trap` (INC-016/017/018 — retry reattaches to the already-failed batch and inherits its error), `impersonation_unavailable` (INC-020 IAM 503 before submission), `slack_notify_failed`, `task_execution_timeout`, `dbt_model_runtime_error`, `downstream_job_no_local_cause` (LAST by design).
- **Two self-inflicted defects caught by measuring, not reading** — both passed all 36 cases AND order-integrity: a loose alternative fired on **325 green runs**; Slack callback noise **stole ga4's real cause**. Law + the three corpus checks: [[feedback_validated_is_not_correct]].
- **Parser:** `Starting batch None-1` was extracted as the literal batch id `"None-1"` and would have been queried against GCP. `_BOGUS_BATCH_ID` rejects it; the upstream id-minting task returning nothing IS the finding.
- **Test-gate defect:** `test_perf_profile.py` was the only test module with no `if __name__ == "__main__"` block, so `python3 -m airflow_debugger.tests.test_perf_profile` imported it and **ran nothing** — 12 real assertions silently skipped by the stated gate. Added.
- **DNS pinned-IP fallback VERIFIED LIVE.** Pi-hole is back ON with `logging.googleapis.com` allowlisted, so the sinkhole no longer self-reproduces; both branches were run against live FAILED batch `f73fa983-67a7-4f35-8e5d-37919e30b43d` — normal `gcloud logging read` 20993 chars vs forced `_logging_via_curl` 20991 chars (one trailing blank line apart), and full `analyze_batch` returns `state=FAILED` + `signature=driver_oom` + `application_id`, not an empty classification. Fixed: `_public_ip` accepted any digit-leading answer that was not `0.0.0.0`, so an **IP-blocking-mode** blocker answering with its own LAN address (`192.168.10.177`) was pinned; it now requires a globally routable address. See [[reference_pihole_dns_contaminates_fetch]].
- **Acquisition gap (IMP-053):** `oncall_daily_rca.sh` pulls only the `tpa` + `Machine Learning` tags, so `audience_intent`, `mntn_match_incrementals_fetch` and `keyword_ddp_reporting` never land on disk — **INC-021/022/023 have NO raw logs** and cannot be swept or replayed. Every sweep and regression fixture is bounded by what the daily pull captured. Also logged: IMP-051 (token fetch not pinned), IMP-052 (`curl -s` without `--fail` discards the real API error).
- **Phase 3 (in-DAG auto-fire) HELD by user decision 2026-08-20** — not started. Design spec still at `artifacts/audi_1191_indag_callback_design.md`.

**Same-day continuation (2026-08-20, after the leftovers): taxonomy 25 -> 33, classification 55% -> 92%.**
- **Acquisition was the bigger defect.** `--state failed` is state AT PULL TIME, so every failure that recovered was invisible; `--include-recovered` fixed it and the corpus went 991 -> 1031 logs. Detail + the wrong-first-diagnosis correction: [[reference_oncall_runbook]].
- **Signatures added from the widened corpus:** `batch_id_attach_trap`, `impersonation_unavailable`, `slack_notify_failed`, `task_execution_timeout`, `dbt_model_runtime_error`, `downstream_job_no_local_cause`, `task_externally_terminated`, `batch_id_missing`, `batch_cancelled`, `dag_not_found_at_startup`. Sweep now reports 100/108 classified + 6 routable = **106 of 108 diagnosable failures resolved**.
- **An empty log means different things by terminal state.** `upstream_failed` + empty = the task never ran, diagnose upstream. `failed` + empty = the WORKER died before the task could raise (INC-021); check whether it already retried before touching anything. `parse_log_file` now carries `ti_state` from the filename so the report can branch.
- **The last 2 cannot be closed with a regex and should not be** — a Vertex submission line and a sensor's `Poking for tasks` both appear in SUCCESSFUL runs. They need engine routing (IMP-055), the same shape as Dataproc.
- **`airflow_debugger/sweep.py`** is the standing measurement: rates per outcome, routable-vs-gap, and **how many signatures fire on a green run** (the check that caught 325 false positives). Run it after ANY signature change.
- **Explainer workbook is now 14 tabs** (`AUDI-1191 Failure-Debugger How It Works.xlsx`): INC-013 end to end with all 28 Airflow and 99 driver lines verbatim, each tagged with the step that consumed it, plus the chain, the verbatim report, the proposed Slack reply, and a 7-step live demo. Evidence is captured to `artifacts/audi_1191_worked_example.json` by `audi_1191_capture_worked_example.py` so the workbook regenerates without a live PAM grant.
- **HARD CONSTRAINT (user, 2026-08-20): the tool must NEVER open a PR, push a branch, commit code, or change a prod resource, in any phase, behind any flag.** The old "propose-only PR" item is DROPPED, not deferred. Read-only GitHub is fine and is what the code-link resolver already uses. Phase 3 remains HELD.
- **Live-fire (INC-024, 2026-08-20):** the `vertex_pipeline_task_failed` signature written that morning fired `[high]` on a real PagerDuty page hours later. It correctly named the class and pointed one layer down; the actual cause was five layers down in the Vertex Model Registry ([[reference_fangorn_inference_dataproc]]).

## Shipped to prod and verified end to end (2026-08-24)

**`airflow_debugger_daily` runs in airflow-ti**, unpaused, identity `airflow-debugger@mntn-prj-prod-00` (Crossplane, mntn-devops#4990). Two post-merge PRs closed the last gaps: [airflow-ti#1214](https://github.com/SteelHouse/airflow-ti/pull/1214) shipped it, [#1215](https://github.com/SteelHouse/airflow-ti/pull/1215) fixed publishing and added the mask registry. Verified live, not assumed: `rca_2026-08-23.json` + `.md` landed under `gs://mntn-data-archive-prod/debugger/` at 00:37Z.

**`masks.py` — a masking error can never be the verdict.** INC-025's deepest exception was the fangorn component's own cleanup `NotFound: 404`, raised because a quota-refused create left no cluster to delete. Real, reproducible, and completely wrong as a root cause: it sends the reader after a deletion race while the actual `Insufficient 'N2_CPUS' quota` text exists only in the ClusterController admin audit log. **The generalizable failure mode is that the deepest error in a log is not always the cause** — a cleanup handler, a failure callback, or a retry that reattaches to a prior attempt each raise something real that stands *in front of* the fault, and a classifier that always trusts the deepest frame is confidently wrong. Registry entries, each declaring `hides` + `next_hop` + an optional resolver:

| Mask | Hides | Next hop |
|---|---|---|
| `dataproc_cleanup_delete_404` | the CreateCluster refusal that left nothing to delete | the ClusterController audit log (resolver: `vertex_rca._cluster_create_error`) |
| `slack_notifier_failed` | the task failure the on-failure callback was announcing | the task's own error, above the callback frames |
| `dataproc_batch_reattach` | the earlier attempt's failure, inherited not caused | the first attempt's batch driver output |

**The invariant: a mask never silently ends a chain.** Either a resolver reaches the next hop, or `report.build_report` prints `This is not the cause: it hides <X>. Read <Y>.` so "one hop short" surfaces as a known gap instead of a plausible answer. Pinned tests assert every entry declares both fields **and** that a genuine error (`OutOfMemoryError`, the quota text itself) is NOT matched — the registry must stay narrow or it starts refusing real verdicts. Net effect on INC-025's log: `[high] infra/quota`, `similar: INC-025(0.851)`, no LLM, where before it stopped at `vertex/pipeline-task-failed` (a pointer, not an answer).

**Vertex chain is now 6 layers**, `error_layer` ∈ {pipeline, component, replica, dataproc-driver, **dataproc-create**}. The sixth reads `protoPayload.status.message` off the CreateCluster audit entry — `logging_messages()` took a `field` parameter for this, since audit logs carry the text there rather than in `jsonPayload.message`.

**Manual-run trap: `logical_date` must sit inside `[the DAG's start_date, now]`.** Outside it Airflow reports the run **success with zero task instances** — no error, no log, indistinguishable from a clean run unless you read `total_entries` on `/taskInstances`. Cost three verification attempts: `2026-08-26` queued forever (future), `2026-08-19` instant success with no tasks (before `start_date` 2026-08-21). Never read a green manual run as evidence without checking the task count.

See [[reference_fangorn_inference_dataproc]], [[reference_gcs_iam_creator_vs_user]], [[reference_oncall_runbook]].

## The 30-day replay and the five gaps it closed (2026-08-25)

**211 failed-state logs collapse to 67 distinct failures**, keyed `(dag_id, task_id, signature)`. Each replayed through the real `orchestrate.investigate(use_llm=False)`. After the fixes: **47 root-caused, 0 bare `unclassified`** — every low-confidence result is now a named condition (stub naming its culprit, pod never started, process killed mid-poke, sensor gave up in another try, evidence expired) rather than a residue. Harness: `tickets/audi_1191_airflow_spark_debugger/artifacts/audi_1191_replay30.py`.

**Two of the five backlog rows were WRONG about their own cause, and reading the log changed the fix both times.**
- **IMP-077** was filed as "the traceback did not serialise, and the Databricks run-id extractor is missing". It is neither: a `KubernetesPodOperator` waited 120s for its pod to reach Running, deleted it, and raised with an EMPTY message. The pod name was the handle all along. Detection is structural — budget announced + `Deleting pod:` + no root error — because there is no text to match.
- **IMP-075** was filed as a signature for the heartbeat `500` / `psycopg2.OperationalError` / PgBouncer strings. **Those never appear in the task's own log** (verified: zero matches on the task that died inside Astronomer's maintenance window); they live in Astronomer's API-server logs. What ships instead: 22 pokes with **zero** reschedules means the sensor was polling in-process, so a log that stops mid-poke means the PROCESS was killed, not that the sensor timed out.

**Writing a backlog row from the symptom and implementing it without re-reading the log would have shipped two wrong fixes.**

**A stub's run must be matched on the stub's OWN state.** An `upstream_failed` log's filename carries its day, never its run id. `vertical_classification_api` had **21 runs on 2026-08-21 and one failure**, so "the first run containing this task" matched a SUCCESS run and answered confidently wrong. Rank failed runs first, then require the task's state to equal the stub's.

**The replay harness had the same class of bug it was built to find.** It picked each group's most recent log as the representative; for a group whose newest member is an empty 69-byte stub that reports the whole group as "no error text" and hides a real gap. Selecting the member with the longest error text changed the gap analysis completely. **A sampling harness that silently picks the wrong member looks exactly like a clean result.**

**`db_credential_rejected` came from a live failure, not the corpus** (IMP-080): `PSQLException: FATAL: password authentication failed` matched nothing, so a Vertex code-9 stopped at "read the step's logs". A database rejecting a credential is not a missing IAM grant, so it is ordered BEFORE `auth_error`.

**Still open:** seven stubs whose culprit does not resolve, and IMP-081 — acquisition pulls only the `tpa` and `Machine Learning` tags, so a control-plane outage that killed four tasks across three DAGs is visible as ONE log and the cross-log co-occurrence detector cannot be built.

## Shipped and verified end to end (2026-08-25, airflow-ti#1217)

**All five gap fixes are live in prod.** Bundle `2026-08-25T23:15:06` (44s after merge). Verified on
a real failure, not asserted: `mntn_match_verticals_precache_v1_1/pre_cache_verticals`, whose log
says only `Task failed with exception`, now reports *"The pod pre-cache-verticals-a79kvr7k did not
reach Running inside its 120s budget... check node capacity and image-pull time for that pod, not
the task's code."*

**The gauntlet caught a real defect in the fix (THRASH, then a clean second pass).** `_run_holding`
scanned only the first 12 candidate runs while claiming an ambiguity guard, so a second holder past
the cut left one hit that looked unambiguous and named the wrong run's culprit as fact. **An
ambiguity guard over a truncated list is not a guard.** The scan is exhaustive over FAILED runs
only, which is sound because an `upstream_failed` task cannot exist in a successful dag_run, and
that filter is what keeps it affordable.

**Its fixer over-reached and had to be rejected.** It deleted four of the five gap fixes plus
`slack_block.py` — working, tested code, not defects. **Take a gauntlet's findings; do not take its
fixer's diff on faith.** The correct move was restoring the tested state and re-applying only the
confirmed finding.

## Slack delivery, MERGED and inert (AUDI-1221, airflow-ti#1219, live 2026-08-26)

**`notify.py` is inert until a token exists**, and the gate is the TOKEN, not a boolean: a flag can
be switched on by someone who has not decided which channel the bot may write to; a missing token
cannot. Unset renders the body and returns it unsent, so the shape is reviewable in a log first.

**Threading matches the RUN ID, and the first version did not — a blocker the gauntlet caught.**
Matching an alert by `dag_id` + `task_id` attaches the reply to the wrong message, and the wrong
matches are the COMMON ones: the daily sweep diagnoses a day that already closed, so a task failing
again today has a NEWER alert carrying the same two names, and an engineer typing "looking at
<dag>/<task> now" carries them too. The alert's own link contains `dag_run_id=<run_id>`
(`include/job_config/message_utils.py` `dag_grid_task_url`), so the match is exact or absent. No run
id in the diagnosis means no thread: a loose channel post reads worse and is never attached to
someone else's incident.

## Answer-shaped replies shipped 2026-08-26 — airflow-ti PR #1224
Every alert reply is now five sections ending in a numbered fix (What failed / Why / Where / How it
failed / Fix), and eight resolvers in `resolvers.py` settle the fork a signature leaves open by
reading run history or the task's declared config. `root_cause_walk.py` follows `downstream_task_ids`
to the task that actually raised. Replay over 216 failed-state logs / 25 days: 67 distinct failures,
47 root-caused, **0 bare categories and 0 replies without a fix** (was 139 ending on a category and
77 on "diagnose the upstream task"). Browsable record: artifact `ada3322c-046c-4a23-bd6b-dfea9bad2e8f`,
regenerated by `artifacts/audi_1191_replay30.py` then `scratchpad/build_page.py`.

**Six gauntlet rounds caught six confident WRONG answers, all shipped green under a full test suite.**
The pattern every time: a guard or a statistic that is true of the fixture and false of the fleet.
With exactly 3 successful runs `third = max(len(ok)//3, 3)` made both trend windows the SAME slice, so
growth was 0.0 by construction and a task that had doubled printed "a steady 30m" and "do not raise the
time limit". A bare `401` anywhere in a 24 KB log window turned a missing IAM grant into "the credential
expired, refresh it". A stale sensor state prescribed the remedy its own cause had just ruled out. See
[[feedback_validated_is_not_correct]] — short hand-written fixtures are what hide this class.

**Slack is live** (app, scopes, private-channel trap): [[reference_slack_debugger_app]]. Delivery
shipped as #1230 (the #1225 wire was folded into #1229); the Astro env vars are deployed and prod
delivery is verified (2026-08-27, see below). Run-origin gotcha: [[reference_airflow_run_origin]].

## 2026-08-26 night — the delivery was never wired, found live, shipped as #1230

The Astro env vars landed (`SLACK_BOT_TOKEN` + `SLACK_ALERT_CHANNEL`) and a manual verify run on
the post-#1229 bundle posted NOTHING: `notify.deliver` shipped complete and tested and no caller
existed — `daily.run` published to GCS and returned. **The token gate masked the missing wire for
two PRs: unwired and untokened are indistinguishable until a token exists.** Wire = airflow-ti
**#1230** (**MERGED 2026-08-27**, squash-and-merge; also carries the optimizer's cumulative savings
log, see [[project_airflow_optimizer]]): per-diagnosis `notify.deliver`, outcomes
on each result row plus `slack_posted`/`slack_threaded`, `conversations.history` bounded to the
sweep's day, and the gauntlet's addition — delivery runs after the sweep and is skipped when
`rca_<ds>.json` already exists in GCS, so an Airflow retry or old-date re-run never re-posts.
Merged before 17:00 UTC, so the scheduled run delivers ds-yesterday threaded replies with no
further action. IMP-087: the alert search reads one Slack page (100 messages).

**Two Airflow-3 manual-run traps (cost ~30 min):** a manual run's data interval SNAPS onto the
schedule slot, so a second trigger inside the same interval 409s on the unique constraint — and a
CLEARED run re-executes pinned to its ORIGINAL `bundle_version`, so verifying new code needs
DELETE + fresh trigger, never clear. `POST /dagRuns` requires the `logical_date` KEY even when
null (null = "now", works only when the DAG tolerates a missing data interval, #1223-style).

## 2026-08-27 — 30-day backfill validated; delivery verified live; triage tickets

**30-day backfill: 173 failures, 100% logs available, 94.7% deterministically root-caused**
(`tickets/audi_1191_.../outputs/audi_1191_backfill_30d_2026_08_27.md`). The 9-failure gap = a
Cassandra `InvalidRequest` code=2200 masked by `channel_not_found` Slack-callback noise; new
**`cassandra_invalid_request` signature placed BEFORE `slack_notify_failed`** (order-integrity
matters) ships on **airflow-ti PR #1233** with the severity glyph + section spacing.

**Prod Slack delivery verified end-to-end 2026-08-27: 3 diagnoses posted, threaded.** The blocker
was the missing `SLACK_ALERT_CHANNEL` Astro env var; `OPTIMIZER_SLACK_CHANNEL` is the optimizer's
separate var — do not confuse the two.

**14 triage tickets AUDI-1227..AUDI-1240** from the backfill's root causes, label `debugger_triage`
(7 closed Done, 7 open). **Confluence "TPA Pipeline On-Call Reference"** (space TAR, page id
`3769991216`) remote-linked from AUDI-1191 and AUDI-1194. **SUPERSEDED 2026-08-28: its content was
merged into the team's existing "TI On Call Playbook" page `2908061697`; `3769991216` is now a
redirect stub. `triage.py` appends known-issues rows to `2908061697`.**

## 2026-08-28 — rapid replies (#1239) and the in-DAG triage filer (#1240), both MERGED

**airflow-ti #1239 (merged 2026-08-27) — `airflow_debugger_rapid`**, every 15 min, answers
terminal failures within minutes instead of next-day. Exactly-once via GCS markers under
`gs://mntn-data-archive-prod/debugger/delivered/` — the marker is written only AFTER a successful
Slack post, so a crash between diagnose and post retries rather than drops. The daily sweep skips
marker-answered rows but still publishes its artifacts. Unmatched failures publish raw logs to
`debugger/unclassified/<ds>/` for later signature mining.

**airflow-ti #1240 (merged 2026-08-28) — the daily sweep files its own Jira Bugs**
(`include/airflow_debugger/triage.py`): one AUDI Bug per NEW `dag/task` pair, gated on
`JIRA_API_TOKEN` + `JIRA_USER_EMAIL` Astro env vars (user's personal token for now; IT service
account requested via Robin Fox — the SA needs AUDI Browse/Create/Link Issues/Add Comments + TAR
Confluence view/edit). Ticket spec from Bryce Wagg (2026-08-27) and the two-PUT Bug conversion:
[[reference_jira_conventions]]. Dedup is by the `[TRIAGE] dag/task - class` summary prefix, so the
laptop backstop filer (`workspace/airflow_debugger/triage.py`, run by `daily_gap_check.sh`'s noon
launchd job) cannot double-file.

**Jira REST paging trap (2026-08-28):** `/rest/api/2/search` is REMOVED (HTTP 410); use
`/rest/api/3/search/jql`, which pages ONLY by `nextPageToken` and SILENTLY IGNORES `startAt` — a
gauntlet fixer introduced startAt paging, which would infinite-loop past 100 tickets; reverted.

**gcloud/gsutil on this Mac hit `ReauthUnattendedError` 2026-08-28** — blocks savings-log
verification until the user runs `gcloud auth login`.

## 2026-08-28 (later) — #1242 fallback channel, #1243 marker auth fix, rapid DAG verified

- **#1242 MERGED:** unmatched diagnoses post to `SLACK_FALLBACK_CHANNEL=C0BT9TKRMKM`
  (#airflow-debugger); alert channels stay threaded-only.
- **#1243 MERGED:** exactly-once markers now written with a gcloud token via the GCS JSON API.
  **`gsutil` is UNAUTHENTICATED in Astro task pods** — marker writes silently failed, so the rapid
  sweep spammed duplicates; two purges of `C08CURMGNMQ` were run.
- **#1244 OPEN** (per-ticket priority rationale line + IMP-087 alert-search cursor pagination,
  3 pages); Ryan to merge.
- **Rapid DAG live and verified.** Unpause/pause via `PATCH is_paused` with `AIRFLOW_BEARER`.
  **Deploy-rollout race repeats** — unpaused too early twice; wait ~10 min after merge.
- **Priority rubric published** in playbook `2908061697` (v6+): P1 infra/upstream, P2 classified
  app, P3 unclassified; each filed ticket carries a reason line (in #1244). **AUDI-1249 auto-filed
  in prod** — the filer works on Astro with the user's token. Jira SA request sent to IT
  (Robin Fox): AUDI Browse/Create/Link/Comment + TAR view/edit; swap Astro
  `JIRA_USER_EMAIL`/`JIRA_API_TOKEN` when it lands.

## 2026-08-28 (later) — monitor-tpa threading live; fangorn failure unrelated to #1231

- **`SLACK_ALERT_CHANNEL` on Astro prod is now `"C08CURMGNMQ,C067ZM2EC5S"`** — the debugger
  searches and threads in `#monitor-tpa` too (comma-list support was already in the #1244-era
  `notify.py`). See [[reference_slack_debugger_app]].
- **`fangorn_household_14day_lookback` failed try 1 on 2026-08-28, UNRELATED to #1231** — it is a
  different model file with its own `spark.sql.shuffle.partitions=32768`. A terminal failure would
  be answered by the rapid sweep; the live end-to-end test is still pending.

## 2026-08-31 — round-2 PR #1249 (gauntlet PASS, 254 tests; MERGED — see the evening section)

**[airflow-ti#1249](https://github.com/SteelHouse/airflow-ti/pull/1249)** (branch
`audi-1191-debugger-round2`), the packaged fixes from the 08-29 missed-replies/incrementals work:

- **4 new signatures:** `openai_results_cohort_missing` (ordered BEFORE generic `path_not_found`
  on `openai_batch_results/dt=` paths), `openai_batch_state_guard` (the "Inconsistent state"
  double-submission guard), and the pair `dataproc_await_died_no_payload` +
  `vertex_await_died_no_payload` (an await/Run-URL line paired with "Task failed with exception"
  within 300 chars; FAILURE-ONLY so green runs never match — the 2026-08-25 lesson that a
  distinguishing line appearing on successful runs too repeats the defect).
- **Fast-fail ExternalTaskSensor RCA (IMP-096 partial):** `parse` extracts the target dag/tasks
  from the `ExternalTaskFailedError` message when no poke line exists;
  `external_task_rca.analyze_external_task` gains an `on_date` fallback via a `_run_holding`
  day-scan. Fast-fail sensors now reach the API-state verdict + the upstream walk; the
  producer-chain walk (to the artifact two DAGs up) is still open.
- **Rapid lookback watermark:** `markers.read_watermark`/`write_watermark` at the debugger GCS
  prefix `cycle_watermark.json`; rapid extends its lookback to the last completed cycle, capped
  at 6h, so paused cycles (deploy rollouts, env-var restarts) no longer drop alerts.
  **IMP-095 CLOSED once merged.**
- **Reply clarity:** matched-on raw strings replaced with "recognized a known failure pattern
  (<key>)"; the `external_task_failed` cause carries no INC ids; unclassified replies include the
  log's own error tail.
- **The two 08-29 white-circle unclassified failures are classified** (verified on the real
  pulled logs): `ipdsc_monitor/monitor_ipdsc_42` = dataproc await died with no payload;
  `fangorn_hhid` `challenger_inference` = vertex await died with no payload.
- Lint gotchas from this round (ruff pin, `per-file-ignores` resolution, no `ruff format` in CI,
  zsh word-split): [[reference_airflow_ti]].

## 2026-08-31 (evening) — #1248 + #1249 MERGED and verified; digest PR #1251 open

- **#1248 + #1249 MERGED, deploy verified end-to-end:** `deploy_prod` CI runs 17:37 and 18:22 UTC;
  `cycle_watermark.json` seeded manually 19:08 UTC and REWRITTEN BY PROD 19:30 UTC — the rapid
  lookback watermark is live (**IMP-095 closed**).
- **PR #1251 OPEN — fallback-channel digest** (user feedback: per-event posts in
  #airflow-debugger were too spammy — [[feedback_slack_digest_not_per_event]]): ONE digest parent
  per sweep; unmatched RCAs are threaded replies under it; `(dag, task, signature)` duplicates
  collapse to a counted line + a single reply; exactly-once markers are written only AFTER the
  group reply lands. Code shape: `notify.deliver` gained `defer_fallback`; `notify.post_digest`
  does the grouping. Demo live in `#airflow-debugger` (`C0BT9TKRMKM` = `SLACK_FALLBACK_CHANNEL`).
- **`SLACK_ALERT_CHANNEL` on prod is now `C08CURMGNMQ` ONLY** (monitor-tpa removed;
  [[reference_slack_debugger_app]] corrected).
- **The rapid DAG's task id is `reply`, not `rapid`** — use `reply` for log fetches.

## 2026-09-01 — #1251 LIVE in prod (image deploy-2026-09-01T19-06-22)

- All four airflow-ti PRs (#1250-#1253) merged 2026-09-01, but prod silently stayed on the OLD
  image: Astro cancels superseded builds on back-to-back merges and never enqueued the final
  SHA. Two verification sweeps ran the old image and their verdicts were VOID. Retrigger PR
  #1254 got the build; ALWAYS check `current_tag` against the merge before trusting a prod
  verification run. Mechanics: [[reference_astro_deploy_mechanics]].
- Post-deploy rapid debugger cycle ran clean on the new code (digest threading live).

## 2026-09-01 (later) — event-driven trigger PR #1256; Airflow 3 listener facts

**PR #1256 OPEN — failure-triggered rapid sweep** (`include/airflow_debugger/trigger.py` +
`plugins/airflow_debugger_trigger_plugin.py`): an `on_task_instance_failed` listener fires the
rapid sweep the moment a task fails, instead of waiting for the 15-min schedule.

**Airflow 3 listener facts (verified locally on Astro Runtime 3.1-9 / Airflow 3.0.3 — what
airflow-ti runs):**
- The `on_task_instance_failed` hookspec exists and fires in the TASK-RUNNER process, for both
  FAILED and UP_FOR_RETRY. Airflow wraps listener calls in try/except, so a listener can never
  fail the task it observes.
- `plugins/` ships with the deployed image (it is not in `.dockerignore`).
- `AIRFLOW_BEARER` + `AIRFLOW_API_BASE` are deployment env vars, available to listeners.

Relay close-out the same session (counter-read rule, descriptor delete via PAM):
[[reference_astro_metrics_relay]]. Review queue at close: airflow-ti #1255/#1256/#1257 +
mntn-devops #5224 ([[feedback_gauntlet_findings_not_fixes]] — the #5224 fixer's nonexistent
IAM role). *(Superseded the same night: all three combined into PR #1258 and merged — next
section.)*

## 2026-09-01 (evening) — #1256 live via combined PR #1258; trigger plugin registered; 12-day diagnosis

- **PR #1256 merged via COMBINED PR #1258** (#1255+#1256+#1257 closed as superseded, branches
  kept; octopus merge, 430 tests green) — **LIVE on image `deploy-2026-09-01T22-22-40`**; one
  merge = one Astro deploy, so the superseded-build gap could not recur. mntn-devops #5224
  merged. **The failure-trigger plugin is REGISTERED in prod:** `GET /plugins` lists
  `airflow_debugger_trigger` with its listener. Next natural task failure (or a canary) proves
  the instant trigger end to end.
- **Full-production-history diagnosis written:** `outputs/audi_1191_diagnosis_2026_09_01.md` —
  production history is 12 days (2026-08-20..08-31), not the 30 asked; all of it covered. 128
  failure candidates, 90 diagnosed, 52 root-caused high-confidence; every terminal failure
  inside the corpus got exactly one Slack reply since delivery went live 08-25; the only two
  delivery gaps are the known tag-filter blind spots fixed in #1248. Two whole days (08-22,
  08-26) were never swept and never backfilled. 50 of 90 diagnosed rows are retry-recovered
  flakes (per-try counting inflates chronic retry loops).
- **Cross-system finding: the debugger and the optimizer see NEAR-DISJOINT fleets.** The
  debugger's top 3 offenders (`vertical_classification_api` 34 rows,
  `mntn_match_verticals_precache_v1_1` 17, `mntn_match_incrementals_submit` 14 — 72% of all
  diagnosis rows) have zero optimizer ledger rows ever: Databricks-API/dbt/pod/OpenAI-batch
  jobs, exactly the optimizer's dbx blind spot ([[project_airflow_optimizer]]).
- Optimizer half of the evening (pod first light, v3 point-order fix PR #1259, downloader fix
  PR #1260): [[project_airflow_optimizer]].

## 2026-09-02 (morning) — parse-rate canary built + folded into PR #1260; rapid schedule stays

- **Parse-rate canary BUILT and folded into PR #1260 at the user's request** (PR retitled
  "AUDI-1191/1194: downloader loses the batch; canary for silent parse breaks").
  `include/airflow_debugger/canary.py`: today's `(empty_logs + unclassified) / candidates`
  vs the mean of the last 7 published `rca_<ds>.json` (key-free GCS JSON API read); fires ONE
  loud warning only past `max(2 * norm, norm + 25pts)` AND >= 5 failures today AND >= 3
  history days (`LOOKBACK_DAYS=7`, `MIN_HISTORY_DAYS=3`, `MIN_CANDIDATES=5`). Posts via the
  new `notify.post_note`; `daily.py` records the warning in the rca json under `"canary"`.
  275 debugger tests on the branch.
- **This is the ACCEPTED answer to "what if the log format changes":** extraction never errors
  on a format change, it just classifies less — the canary makes that visible with no AI key.
  **The LLM recommendation layer stays an OPEN security question for Malachi to raise.**
- **DECISION: the 15-min rapid schedule is KEPT as the backstop** for failures the
  `on_task_instance_failed` listener cannot see — hard worker death, Airflow API blips, deploy
  windows, DagRun-level failures. Plan: stretch to hourly after the trigger proves out ~a week.
- **User verification pass (screenshots): digests confirmed working as designed**; new ask —
  ranked rows read ragged (emoji + number misalign), reformat queued for the post-merge digest
  pass ([[feedback_slack_digest_not_per_event]]).
- **Review queue: #1259 + #1260** (both verified OPEN 2026-09-02). *(Overnight: #1260 grew to
  3 commits — canary `99ba84f`, digest ordered-list `dd53939`, wildcard overrides `3d87c6f` —
  retitled "AUDI-1191/1194: downloader loses the batch; parse canary; digest numbered list";
  Jira AUDI-1194 comment 614410. Detail: [[project_airflow_optimizer]].)*
