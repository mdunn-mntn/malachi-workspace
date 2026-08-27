---
name: project_airflow_optimizer
description: AUDI-1194 airflow_optimizer/ — key-free Spark efficiency crawler, live as the spark_optimizer_daily DAG in airflow-ti; 2026-08-26 gained an executor-hour cost unit, Databricks dollar costing from system.billing, and Block Kit Slack delivery to #spark-optimizer.
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [airflow optimizer, AUDI-1194, spark_optimizer_daily, airflow-ti 1212, spark-optimizer service account, serviceAccountTokenCreator, CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT, airflow session use is forbidden, spark optimization crawler, efficiency sweep, eventlog parser, 7-surface spark, optimization detectors, skew spill shuffle, fleet crawl backlog, daily optimizer cron, oncall_daily_optimizer, com.mntn.daily-spark-optimizer, phs event logs, phs.fetch_logs, dataproc-debug pam, audi-storage-object-view, 242x skew, dataproc databricks optimization, straggler detector, idle_reserved_executors, shuffle_fetch_wait, map-side concentration, site_network_hourly stage 9, optimization ledger, optimizer coverage gap, optimizer digest, sweep.py, ledger.py, coverage.py, digest.py, workload identity runner, EXPLAIN COST statement execution api, jobs get-run-output empty, IMP-029 rolling dirs]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-08-26
---
**AUDI-1194 = the OPTIMIZER** (success-triggered efficiency sweep), **split from the AUDI-1191 debugger 2026-08-05** (both AUDI, type Task, Backlog). The two are separate workflows with distinct triggers/schedules/deliverables: the debugger fires only on a **failure**, the optimizer sweeps every DAG that **succeeds**. They can chain but are distinct. AUDI-1194: 5 story points, PMO rep Bryce Wagg, label q3_2026, folder `tickets/audi_1194_optimizer_efficiency_crawler/`, framing **LOCKED** (§0 in its summary). See [[project_airflow_debugger]] (the RCA half), [[reference_airflow_ti]], [[reference_oncall_runbook]].

**Question (framed):** Can a scheduled, key-free crawler read every succeeded Spark job across both engines (Dataproc event logs + Databricks EXPLAIN COST plans/metrics) and emit a ranked, actionable optimization backlog with no manual step? **Goal:** cut Spark compute cost + wall-clock fleet-wide, replacing the departed framework author's tribal knowledge (cost-reduction lever, Medium tier + bus-factor win). **Done-when:** a scheduled crawler scans every succeeded Spark job **including the ipdsc/tpa PHS logs** and emits a ranked cross-job backlog (worst-first, per-finding fix grouped CODE/INFRA/FAILURE) with no manual step.

**Package `airflow_optimizer/` (split from `airflow_debugger/`, commits a8ebad2d + b153266d).** Modules: `eventlog` (the 7-surface Spark event-log parser), `optimizations` (detectors), `optimize` (single-job BLUF), `crawl` (fleet backlog) + `tests`, `fixtures`, own `README`. **Coupling (updated 2026-08-07):** `eventlog.py` lives in `airflow_optimizer/`; since IMP-032 the debugger ALSO lazy-imports `airflow_optimizer.optimize` from `airflow_debugger/perf_profile.py` — escalation-only (ttl_exceeded/driver_oom/fetch_failed + event log), degrades to a note if the optimizer is absent. Entrypoints: `python3 -m airflow_optimizer.{optimize,crawl}` (debugger side = `python3 -m airflow_debugger.{orchestrate,report}`). All 6 tests pass, ruff-clean, git-tracked as renames.

**The engine (all built + validated on real event logs):**
- `eventlog.py::parse_eventlog()` → structured `SparkRun` across all **7 Spark surfaces** (jobs/stages/tasks/executors/environment/storage/SQL per-node), recovering per-operator metrics by joining `sparkPlanInfo` accumulatorId↔Accumulables/DriverAccumUpdates. Handles `.zstd` (dir/file). Surface 7 (storage/cache, `SparkListenerBlockUpdated`) needs `spark.eventLog.logBlockUpdates.enabled` — **UNCAPTURABLE on Dataproc Serverless** (rejects that prop), valid on managed clusters only.
- `optimizations.py::analyze_run()` → 3 rec types — **code** (skew/spill/shuffle-partitions), **infra** (gc_pressure, spot_preemption_cost, cache_ineffective), **failure** (fetch instability) — each with real numbers + fix, impact-ranked.
- `optimize.py` = one event log in → engineer-ready single-job BLUF backlog (plan-text `analyze_plan` + metric `analyze_run`). `crawl.py` = optimize every job in a dir/glob, rank a cross-job backlog worst-first.

**Acquisition state per engine:**
- **Batch-operator Dataproc fleet (88 models, no PHS):** event logs land in `gs://mntn-data-archive-{env}/spark-events` (PR #1169 turned this on fleet-wide, merged prod 2026-08-04) — accessible, download with `gsutil -o "GSUtil:check_hashes=never" cp` (`gcloud storage cp` corrupts `.zstd`).
- **ipdsc/tpa (PHS-attached):** logs are per-batch at `gs://{temp_bucket}/<dataproc-batch-uuid>/spark-job-history/app-<id>.zstd` — SPARSE + scattered across thousands of unsorted per-uuid temp dirs, most empty. A flat prefix scan is infeasible → the crawler must **ENUMERATE ipdsc/tpa batches via `gcloud dataproc batches list/describe` (→ uuid), then read that uuid's `spark-job-history`**. Validated end-to-end 2026-08-05 (parsed `Populate ipdsc_ds_67.DS67`, shuffle.partitions=1000). This reshapes the earlier "point the crawler at the PHS prefix = 1-line change" note, which was WRONG.
- **Databricks:** `EXPLAIN COST` plan + Spark job metrics via `jobs get-run-output` (live acquisition path still to validate).

**PHS event-log access (2026-08-05):** `malachi@mountain.com` has **NO standing `storage.objects.list`** on `gs://dataproc-temp-us-central1-995798185124-svhwvc6j`. Interim read = the **`dataproc-debug` PAM bundle** (Compute Viewer + Dataproc Viewer + Storage Object Viewer; self-service ~1h, 18h max with L1 devops-squad approval; access propagates ~30s after the grant activates). The 1h PAM grant can't run the weekly cron → **standing grant needed (Slack/mountain-devops → Cristina): `roles/dataproc.viewer` on `mntn-prj-prod-00` (enumerate batches) + `roles/storage.objectViewer` on `dataproc-temp-us-central1-995798185124-svhwvc6j` (read logs)**.

**DAILY cron LIVE (superseded the weekly one 2026-08-20):** `.claude/scripts/oncall_daily_optimizer.sh` + launchd `com.mntn.daily-spark-optimizer` (11:00 PT **daily**), `CAP` default **200** (`OPTIMIZER_LOG_CAP`), `OPTIMIZER_PHS=0` skips the PHS half. Writes `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimizer_backlog_<date>.md`; idles with no git noise when a source is unreachable.

**Why daily, the number that settled it:** the fleet emits **~160 event logs/day** (157-164/day over 2026-08-14..08-19; 7.85 MB average object). The old weekly `cap=40` sweep therefore read ~6 hours of ONE day out of 168 — **~4% of the fleet**, a coverage hole, not a freshness preference. A full-day sweep costs ~1.3 GB download, ~3 min CPU, ~50 MB RSS. First daily run (2026-08-20): **214 jobs / 278 findings / 197 high-impact** vs 37/59/42 weekly.

**Proof it works — real fleet finding (IMP-024):** crawl of 13 real prod jobs → **`Update Vertical Categorization` chronic Stage-0 skew up to 242x** (every run 10-242x) = #1 fleet target; `Prepare HTML Content` 18.4x; 6 jobs clean. Labeled by `spark.app.name` (event log self-identifies).

**Hardening pass (2026-08-07 PM, commit 629660a1):** 41 corpus-confirmed defects fixed (5 finders on a 48-log/611MB real corpus → execution-required skeptics → fix wave, AUDI-1191 pattern). Worst: **multi-frame zstd — with the `zstandard` package installed only frame 1 decompressed, every real log parsed EMPTY and reported "clean"** (masked here by the CLI fallback); parser now streams (98MB log: 18GB→49MB RSS, corrupt→error). Phantom skew killed (zero-median guard + 60s floor `SKEW_MIN_TASK_MS`); stage retries attempt-keyed; failed tasks excluded from skew; chimera-dir guard; cron rebuilds rolling dirs (`--selftest`). **New detectors: `straggler`, `shuffle_fetch_wait` (6 prod jobs at 53-72% fetch-wait), zero-task idle fleets.** Plan detectors remain Databricks-format-only (dead on Dataproc OSS text) → IMP-033. **Systemic fleet finding: hourly `aug_log_ip*`/`site_network_hourly` run at 2-8% executor utilization, 20-61 idle exec-h/run.** Sweep cost measured: 48 logs = 58s + 49MB RSS + ~600MB download → **daily cadence is cheap** (switch after next green live cron). PHS: `phs.py` enumerates PHS-attached SUCCEEDED batches key-free (22 live); standing bucket read = draft PR mntn-devops#4724 (bucket-scoped objectViewer to `audience-intelligence@`, mirrors the mntn-marketo pattern; `dataproc.viewer` already standing via DEV-8182).

**Validation run 1 (2026-08-07) — first external ask (Ryan Kleck, `aud-int-int-map` = `intent_score_map`):** ticket now **in_progress**. Found + adversarially verified: a 67-min IO-stalled straggler (13.4x duration on 1.0x data, 5% CPU, speculation off) pinned 240 executors at 32% utilization (~$175 of the ~$260 list run idle); 88.8 TiB spill/run. En route: **IMP-029 FIXED** (v2 rolling dirs `eventlog_v2_batch-<uuid>/events_N_*` parsed in numeric order — the old cand[0] bug would have missed the entire tail), **2 new detectors** (`straggler` = duration-skew × uniform-data cross-check; `idle_reserved_executors` = exec-hours held vs slot-busy). **Verify-pass lesson: 4-agent adversarial workflow refuted my shuffleTracking.timeout rec against Spark source BEFORE it reached the job owner — keep verify-before-send mandatory for owner-facing tuning recs** (mechanism detail in [[reference_dataproc_eventlog_profiling]]). Recs delivered to Ryan: speculation=true + shuffle.partitions 4915→~30k (set in BOTH decorator and builder line ~89 — builder wins).

**2026-08-20 session — cadence CLOSED, PHS proven, Databricks route CORRECTED.**

**PHS half works end-to-end; only the standing grant is missing.** Proven under a 1h `audi-storage-object-view` PAM grant: **22/22** PHS-attached SUCCEEDED batches enumerated, fetched (568 MB) and parsed, producing 21 findings on jobs the archive sweep had never seen (`materialize_mntn_select_*` Stage 6 at 40-78% fetch wait, `segment-updates-to-parquet-*` Stage 2 at 36-67%). PAM detail: on `mntn-prj-prod-00` both `audi-storage-object-view` (objectViewer only, the least-privilege choice) and `dataproc-debug` (compute.viewer + dataproc.viewer + storage.objectViewer) are **64800s max** and need 1 approval from `devops-squad@` / `gcp-audi-admins@` / `pam-slack-bot` — it auto-approved in minutes. `gcloud pam entitlements search --caller-access-type=grant-requester` (NOT `--caller-access`) lists what you can request.

**Two `phs.fetch_logs` defects fixed (tests fail against the old code):** (1) `gsutil cp` had **no `-r`**, so any batch that wrote an `eventlog_v2_*` rolling dir downloaded empty and was silently dropped; (2) no `dest_for()` equivalent — a top-level `appstatus_*` marker in a uuid dir makes `crawl._event_logs` read the WHOLE dir as one merged log. Top-level markers are now stripped; the marker *inside* a rolling dir is load-bearing and kept.

**`site_network_hourly` Stage 9 — the verify pass refuted TWO hypotheses before anything reached the owner.** (a) The detector's OWN stock fix ("raise `spark.sql.shuffle.partitions`") is wrong here: in the same app, stages 29/35 fetch **23.4M blocks at 1,607 B with 1s of fetch wait** while stage 9 stalls on 4.2M blocks of the same size — block count/size is not the cause, and raising partitions multiplies it. Fix text corrected in `optimizations.py`. (b) The source-read guess was wrong: the builder's `shuffle.partitions=5000` with a `// 33` coalesce predicts ~151 reducers; the log shows 74-622. **What the evidence supports is map-side output spread** — shuffle blocks are served by the executor that WROTE them, so the reduce stage is rate-limited by how many map-side executors hold the output. On all four logs profiled the feeding map stage starts with **exactly 50 executors** (`initialExecutors=50`) and lands 90% of its output on 48-105 of them (hottest up to 24.6%); the later map stages start with 306-500, spread across ~480 (hottest 0.3%), and their reducers wait ~0%. **The one fact that does not fit:** stage 15 reads the SAME map output at comparable block count/size and waits ~0% (likely cold-vs-warm first read, unprovable from the event log) — so the ask to Ryan is a one-hour `initialExecutors` experiment, not a config prescription. Tooling: `artifacts/audi_1194_shuffle_concentration.py`. Backlog IMP-047.

**Cost is self-serve and metered:** `gcloud dataproc batches describe --format='value(runtimeInfo.approximateUsage.milliDcuSeconds)'` on `mntn-prj-prod-00`. `site_network_hourly` = **8,663 DCU-h over 17 runs on 2026-08-20** (mean 510, range 164-1,547) vs 99-208/run for `aug_log_ip_hourly` — which is why it outranked the aug family. DCU attributable to the stall is NOT established; the CUD caveat still applies ([[feedback_dataproc_cost_awareness]]).

**Ranking blind spot (IMP-046):** `JobReport.score` is per-LOG `(high, medium, total)`, and `shuffle_fetch_wait` is `high` only at ratio ≥0.50 — so `aug_log_ip_vertical_id_hourly` firing at 31-45% on 11 of 11 runs never reaches the top of the backlog. Chronic-across-every-run is invisible to per-log severity.

**`spark-events` has NO lifecycle rule (IMP-048).** `gs://mntn-data-archive-prod` has Delete rules for 14 prefixes, none for `spark-events/`; the age-30 TTL recorded as approved 2026-08-04 was never applied. 17.1 GiB / 2,237 `.zstd` today, growing ~1.3 GiB/day. Needs `storage.buckets.update`.

See [[reference_databricks]] for the corrected Databricks acquisition route, [[reference_mntn_devops_permissions]] for why #4724 sat unreviewed, [[feedback_verify_before_volunteering]] for the verify-before-send rule this session exercised again.

**2026-08-20 (later) — the sweep became a reporting pipeline, not just a crawl.** `sweep.py` is the post-download entry point (crawl → coverage → ledger → digest); the cron's `--selftest` asserts through it. 40 tests, ruff clean.
- **`ledger.py`** appends every finding to `outputs/optimization_ledger.jsonl` keyed **job + detector + stage**, replayed to derive `new` → `recurring` → `chronic` (3+ sweeps) → `resolved` (3 quiet sweeps); `owner_notified` / `wont_fix` are set by hand and **sticky**. Dedup matters: 26 findings across 25 job-logs collapse to **4 keys**.
- **Two identity traps, both silent.** (1) The key first swallowed any digit in the title, so task counts and byte totals minted a new key every sweep — now the stage number only. (2) `spark.app.name` carries per-run stamps, but **`_<n>` is ambiguous**: a run index in `materialize_mntn_select_16`, a data-source id in `ipdsc_ds_67`. Blind stripping merges the whole ipdsc family. Dates/timestamps/`[n]` always strip; `_<n>` only when the stripped form is a DAG **coverage actually saw** — which is why coverage runs BEFORE the ledger.
- **`coverage.py`** enumerates unpaused DAGs via `.claude/scripts/airflow_api.py` (astro bearer, shelled out so auth lives in one place). Live 2026-08-20: **62 active DAGs, 24 with a Spark task, 38 structurally invisible** (BQ operators, sensors, Python), plus 4 `create_ip_verticals` Databricks tasks. All named in `outputs/optimizer_coverage_<date>.md`.
- **`digest.py`** leads with the delta, says "No change since the last sweep" when true, and links a DAG **only when coverage saw it** (Spark app names are not always dag_ids). Delivery deliberately unbuilt — route is the `compass-slack` automation in mntn-devops, never a local credential.
- **Deliverables:** workbook rebuilt to 9 tabs (adds Ex — site_network_hourly, Ledger + digest, O5/O6 steps, 14 detectors); exhaustive walkthrough page `https://claude.ai/code/artifact/878ac222-4ed6-4376-aea5-cd1772308cca` (real DAG, raw `SparkListenerTaskEnd` annotated, detector verbatim, both refutations, demo commands).
- **Runner scoped, not built** (`artifacts/audi_1194_runner_and_identities.md`, its own ticket when approved): the blocker is **expiry, not identity** — astro bearer ~1h, Databricks OAuth refresh needs an interactive renewal, gcloud is personal SSO. Recommendation: **GH Actions + GCP Workload Identity Federation** (no SA key exists at all), Astro deployment token + Databricks service-principal secret in Secret Manager (no keyless path for those two), GitHub App scoped **`contents:read` + `metadata:read`** so "never opens a PR" is structural. Artifacts to GCS, so the GitHub identity never needs write. Two of four GCP grants already exist because DEV-8182 and #4724 were written against `group:audience-intelligence@`, not a person.

**Shipped-optimization register (IMP-054, 2026-08-20).** The finding ledger gained an **`applied`** state: `python3 -m airflow_optimizer.ledger applied <dag> <key> <pr> <date>` records the PR and applied date and carries both onto every later sweep, so an outcome stays attributable. **`applied` is deliberately NOT sticky** — a merged fix is not a verified fix, so the detectors decide what happened next: the finding going quiet becomes `resolved` (noted "cleared by <PR>"), the finding still firing after the grace window becomes **`fix_not_working`**. `ledger shipped` renders the register (applied date, DAG, finding, PR, outcome, DCU/h before vs after). **Cost is read per-DAG, not per-finding** — a fix that works stops the finding firing, so a per-finding "after" would always be empty. Validated both directions: a working fix reads 100.0 -> 40.0 DCU/h `resolved`, a non-working one 80.0 -> 79.0 `fix_not_working`. Answers "what has this tool actually saved", which the backlog alone never could.

---

## SHIPPED TO PROD 2026-08-21 — it is an airflow-ti DAG now, not a laptop cron

**`spark_optimizer_daily`** in `SteelHouse/airflow-ti` (`dags/spark_optimizer_daily.py`, package
vendored at `include/spark_optimizer/`), 09:00 UTC daily, `retries=1`, `execution_timeout=1h`,
`dagrun_timeout=2h`. **First prod run 2026-08-21: 215 jobs, 290 findings, 196 high**, all four
artifacts published to `gs://mntn-data-archive-prod/optimizer/`. The laptop launchd job and
`oncall_daily_optimizer.sh` still work and are still the local entrypoint.

**Why it moved off the runner design entirely:** Cristina Szumilo asked why a job owned by AUDI
was living in `mntn-devops` and being attributed to the platform team. She was right, and the
move was cheaper than the design it replaced: it deleted the container image, the GAR push, the
ArgoCD manifests, the `mntn-helm` chart change, **and the Astro API token** (a DAG can enumerate
DAGs locally). **Before designing a store for a credential, check whether moving the workload
removes the need for it.** Prior design iterations, all superseded, are in
`tickets/audi_1194_.../artifacts/audi_1194_runner_and_identities.md` §3-§13.

**Identity, verified in prod:** GSA `spark-optimizer@mntn-prj-prod-00` (mntn-devops#4971,
merged), impersonated from the deployment's own ADC `airflow-ti-prod@` via
`roles/iam.serviceAccountTokenCreator` and `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT`. Grants:
`dataproc.viewer` on the project, `storage.objectViewer` on `mntn-data-archive-prod` and the PHS
temp bucket, `storage.objectUser` conditioned to the `optimizer/` prefix. Mechanism detail:
[[reference_gcs_iam_creator_vs_user]].

### Known broken after run 1

**`coverage.collect_local` is dead on Airflow 3.** It reads the paused set from the metadata DB,
and a task gets `airflow session use is forbidden in this context`. The DAG-bundle parse is fine;
the ORM is not reachable from task code. The sweep degrades correctly (`DAG coverage unknown`,
and the ledger declines to write rather than rekey), so this is a missing feature, not a
corruption. **Fix: go back through the REST API** with a deployment token, or find a Task-SDK
call that exposes paused state.

**The download is 200 serial `gsutil` invocations.** `fetch.download()` shells out once per
object, each paying Python interpreter startup. On the Astro default pod (**0.25 CPU, 0.5 Gi** —
the DAG sets no `executor_config`) run 1 took **~19 minutes**, and process spawn dominated, not
the parse. **Fix: one `gsutil -m cp -I` fed the whole object list.** Raising CPU is the smaller
lever.

**The digest cites the container path.** `Full backlog: /tmp/spark_events_*/out/...` instead of
the GCS URL, because `run()` passes the local path through. Cosmetic but useless to a reader.

### Not yet built
Slack delivery (the digest already renders Slack markup; `compass-slack` in mntn-devops is the
transport), the Databricks `EXPLAIN COST` bridge, and `USE SCHEMA` on `system.lakeflow` — **reopened as an internal ask 2026-08-24.** The 2026-08-21 note here ("unobtainable, Databricks-side only") misread the enable error; David Qiu (Databricks) confirmed lakeflow is enabled automatically and that error is expected. The real gate is **metastore admin**, which MNTN has never assigned, and it blocks every `system.*` schema. Workspace admin is not enough and neither is account admin on its own (`grants update` → `User is not an account admin`); Ryan Kleck's assumption that workspace admin would do it is wrong. See [[reference_databricks]].

## 2026-08-26 — the sweep produces a ranked, linked, readable digest

Four merges since the prod ship (airflow-ti #1216, #1218, #1221, plus #1213's CI fix), all
gauntleted. What each one settled:

**Coverage no longer blinds the sweep (#1216).** Airflow 3 forbids ORM access from task code, so the
paused-DAG query raises `airflow session use is forbidden in this context`. That raise used to take the
whole DAG enumeration with it, `known` came back empty, and `sweep` correctly declined to write ledger
rows it could not key — so **change tracking sat frozen at 130 rows / 2026-08-21 for four days**. Only
the paused exclusion needs the DB; the bundle parses off disk. Split, and a lost paused set now costs
only the paused set. Verified: the 08-25 manual run wrote 136 new rows and enumerated 72 DAGs.

**A Spark app name is an Airflow TASK id, never a dag_id (#1218).** `aug_log_ip_hourly` is the task
`feature_group_1_source.aug_log_ip_hourly` inside DAG `feature_store_hourly`; `fangorn_score_monitor`
is a task in `audience_intent`. Matching job names against dag_ids resolved **0 of 57** on the 08-25
sweep, which is why every digest line since launch carried a bare name instead of a link. Coverage now
builds a task-name → dag_id index (DAG ids indexed too, ambiguous names dropped rather than guessed).
The coverage page also lists every unresolved job with its reason, so the gap is stated, not silent.
Three genuinely-unfixable classes remain: Spark set no app name (`app-2026...`, `segment-updates-...-[11]`),
the name is shared by two DAGs, or no DAG defines a task by that name.

**Findings rank by cost, not count (#1221).** The parser had been computing each run's executor-hours
and discarding it. `crawl` carries it as `exec_h`, the ledger records it, and the digest is now four
fixed blocks per DAG — What / Where / Why / How — capped at three DAGs, ranked on impact then hours.
`dcu_h` stays a separate, still-unpopulated field: measured DCU and executor-hours are different units
and conflating them would be a lie in a published artifact. Two costing bugs found adversarially:
a killed or still-rolling app writes no `ApplicationEnd` so the fleet's biggest runaway costed **0.0
and sorted last** (now measured to the log's last event), and an executor whose `added_ts` was 0 was
dropped by a falsy check.

**Databricks enumeration is live.** `airflow_optimizer/databricks.py` reads
`system.lakeflow.job_run_timeline` (1,531 SUBMIT_RUNs / 7 days). Ranked by hours:
`prod-ml-ddp_vertical_classification_api` 251 runs / 85.0h / 24 failures,
`prod-ml-verticals_pre_cache` 381 / 65.2h, `prod-tpa-guid_geos_raw` 188 / 24.5h.
`prod-mntn_matched-mntn_matched_taxonomy_vector` fails 16 of 25 runs. Gotchas in
[[reference_databricks]]. **The remaining gap is model → SQL**: enumeration says a model ran, not what
it ran, so `EXPLAIN COST` still needs the model's source.

**Still not built:** Slack delivery (blocked on the app — Robin Fox reviews scopes, see
[[reference_pi5_server]]), GitHub line permalinks in the *Where* block, batching the 200-invocation
`gsutil` download, and the `EXPLAIN COST` bridge.

**Two prod defects found and NOT yet fixed:** the DAG cannot be manually triggered without an explicit
`logical_date` (Airflow 3 gives such a run no data interval, so the task raises `KeyError('ds')` in
seconds — the 9am schedule is unaffected), and the failure callback's Slack post returns
`channel_not_found`, so a failed sweep notifies nobody.

## 2026-08-26 (later) — the full-corpus validation, and four defects only a full crawl could show

Malachi's ask was "prove there are no gaps before we send this to anyone". Every `.zstd` in
`gs://mntn-data-archive-prod/spark-events` written inside the last 30 days was downloaded and
parsed: **3,022 objects / 25 GB → 2,954 event logs** (92 objects belong to 24 `eventlog_v2_*`
rolling dirs), **all 2,954 parsed**, **80 distinct Spark jobs**, **3,620 findings**,
**85,655 executor-hours**. Scripts: `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_validation_*.py`.
Deliverable: `My Drive/Tickets/AUDI-1194 Airflow Spark Optimization Crawler/AUDI-1194 Spark Optimizer Validation.xlsx`.

**The archive holds 23 days, not 30.** Span is 2026-08-04..2026-08-26, median 133 logs/day.
Archiving to that prefix began 2026-08-04; the bucket also holds an unrelated 2025-10-07..11-12
block. `gsutil lifecycle get` still shows **no rule matching `spark-events/`** (IMP-048 open), so
nothing expires it and the window grows a day per day. Do not promise a 30-day lookback.

**A Spark app name puts the job BEFORE the dot; an Airflow task id puts it AFTER.** This corrects
the #1218 note above. `Populate site_network_hourly.SiteNetworkHourly` → the task is
`site_network_hourly`; `feature_group_1_source.aug_log_ip_hourly` → the task is
`aug_log_ip_hourly`. `normalise_job` took the segment after the dot for both, so it returned the
CLASS name. One segment cannot serve both sides. `job_keys` now offers first segment, last
segment, full name, and a digit-infix form (`ipdsc_14_monitor` → `ipdsc_monitor`, one datasource's
run of the `ipdsc_monitor` DAG); `Coverage.resolve` takes the first candidate naming exactly one
DAG. **Measured on the real fleet: the shipped digest linked 3 of 80 jobs, the fix links 77.**
The coverage report's own `profiled this sweep` count went **2 → 13** — that was the visible
symptom, and it read as "the fleet barely ran". `normalise_job`, `Coverage.task_owner` and
`digest._resolver` all lost their last caller and were deleted.
**Correction on the record:** the "0 of 62" figure in commit `b80d3047` describes the coverage
INDEX, not the digest; a gauntlet refuter caught the overstatement. 3 → 77 of 80 is the evidenced
number. Two names stay unresolvable: `guid_log_ip_advertiser_id` (its task is
`feature_group_1_source.guid_log_ip_advertiser_id_rollup`, so the app drops a `_rollup` the task
carries) and `ipdsc_third_party_audience_builder` (no task in the bundle defines it).

**175 of 200 recent Dataproc batches write their event log where the sweep never looked.** Of 200
listed batches: 13 set `spark.eventLog.dir = gs://mntn-data-archive-prod/spark-events`, 10 set an
explicit temp path, and **175 set nothing at all** — and Dataproc still writes their log to
`gs://<temp-bucket>/<uuid>/spark-job-history/`, history server attached or not. `phs_succeeded`
filtered on `sparkHistoryServerConfig`, which Dataproc returns as an **empty dict** (falsy) for all
but 10, so it kept 10 of 200. Sampled 12 of the 175 at random: **12 of 12 had a readable log
there.** New rule keeps any SUCCEEDED batch not writing to the archive → **185 of 200**;
`MAX_BATCHES` 60 → 150 (~585 MiB at the measured 3.9 MiB/batch). The temp bucket
`dataproc-temp-us-central1-995798185124-svhwvc6j` **is readable as of 2026-08-26** (403 on
2026-08-20; mntn-devops#4724 is out of draft with DevOps requested). `spark_optimizer_daily.py`
already calls `phs.fetch_logs`, so no new wiring was needed — the selector was the whole gap.

**16 of 30 Spark DAGs ran successfully in the window and produced no readable log.** Not "they did
not run": task-instance states show `materialize_mntn_select.materialize` 24 successes,
`fpa_site_visit_batch_serverless.dsid23_guid_log_processing` 24, `hashed_email_guid_log_signals.populate_hem_data_ds_23`
24. Cause confirmed by `batches describe`. Proven end-to-end: fetched 14 temp-bucket batches,
crawled 14/14 clean, and `materialize_mntn_select_16` — a DAG the sweep had never seen — resolved
with a finding at 31.7 executor-hours.

**A no-op run is a finding, and `ApplicationEnd` is the WRONG discriminator.** `crawl` skipped any
log with no jobs and no stages as a truncated download. 39 such logs in the corpus were all real
named apps that started and ended without submitting a job, costing **546 executor-hours** across
15 discarded high-impact findings; the worst held **100 executors for 64.4 executor-hours with zero
tasks run** — a finding `idle_reserved_executors` already knew how to raise and never got to.
The first fix keyed on `ApplicationEnd`; a gauntlet refuter proved that throws away the killed apps
the guard exists to catch (a TTL kill, cancel, or driver OOM writes no `ApplicationEnd` either, so
a 64-hour run read as a failed download at 0.0 cost, contradicting `executor_hours`, which costs
that exact case from `last_event_ts`). **Holding an executor is the discriminator**: skip only when
there are no jobs, no stages AND no executors.

**Detector status, measured on a random 300-log sample — 10 of 14 work.** Firing: `shuffle_fetch_wait`
1,496 · `disk_spill` 884 · `idle_reserved_executors` 549 · `shuffle_partition_sizing` 324 ·
`straggler` 242 · `skew` 125. Correctly quiet, with the input confirmed present: `gc_pressure`
(GC time recorded on 295/300, max share **4.3%** vs a 10% threshold), `spot_preemption_cost`
(removal reasons recorded, **no** `preempt`/`spot` string — serverless, no spot),
`shuffle_fetch_instability` (**0** FetchFailed tasks). Unproven either way: `cache_ineffective`
(`cached_rdd_bytes == 0` fleet-wide, nothing caches, so it has never had a case to judge).

**The four plan detectors are dead on Spark event logs, now measured, and the unblock is named.**
295 of 300 sampled runs DO carry SQL plan text — **4,734,637 chars** — and `parse_plan_text`
extracts **0 leaf scan nodes** from all of it. OSS Spark writes `Relation [cols...] parquet`; the
detectors need Databricks's `Scan parquet <table> ... Statistics(sizeInBytes=...)`. This widens
IMP-033 from "the scan/stats regexes are Databricks-only" to "all five plan detectors are dead on
OSS text". **`system.query.history` is the missing input** — it carries the statement text that
`system.lakeflow.job_run_timeline` does not, which is the model→SQL gap noted above. It exists and
is listed by `system.information_schema.schemata`, and returns
`INSUFFICIENT_PERMISSIONS: User does not have USE SCHEMA on Schema 'system.query'`. Same account-admin
ladder Alyson Lefkowitz ran for `system.lakeflow`; draft at
`artifacts/audi_1194_slack_alyson_query_schema.md`. See [[reference_databricks]].

**`site_network_hourly` is the fleet's #1 target, and now it is not a 4-log claim.** 302 runs in the
window, **21,200 executor-hours** (top-10 jobs hold 79% of the fleet total), per-run median 51.3h /
max 371.2h. A fetch-wait finding fires on **254 of 302 runs**, **252 of them on stage 9**, min 30% /
median 56% / max 90% of task time; `idle_reserved_executors` fires on 236. The Ryan Kleck draft now
rests on this rather than on the four profiled logs.


## 2026-08-26 (evening) — merged, verified in prod, and the Databricks half fully granted

**#1222 is live and the fix is real.** The 2026-08-26 prod sweep reports `profiled this sweep: 12`
(was 2) and names only **7 of 217** scanned jobs as untied to a DAG. The digest's *Where* block now
carries a working Astro link. **#1223 open** for the manual-trigger `KeyError('ds')`; gauntlet PASS
in 2 rounds, the first clean verdict of the day.

**Every Databricks grant is done**, verified by reading rows: `system.lakeflow`, `system.query`,
`system.billing` (168,853 rows), `system.compute` (114,899), `system.access` (612,075),
`system.storage`, for both `malachi@mountain.com` and the `spark_optimizer` SP; plus `SELECT` +
`USE CATALOG` on `CATALOG prod` for the SP, which `EXPLAIN COST` needs because it plans against
the real tables. Ladder, tiers and the stale-CLI trap: [[reference_databricks_system_schema_grants]].

**Billing can now be joined to the optimizer's own rankings.** `system.billing.usage` carries
`usage_quantity`/`usage_unit` and a `usage_metadata` struct with `job_id`, `job_run_id`,
`run_name`, `job_name`, `warehouse_id`, `cluster_id`. `job_run_id` joins
`system.lakeflow.job_run_timeline.run_id` (per-dbt-submission DBUs); `warehouse_id` joins the
warehouse a `system.query.history` row ran on (per-statement DBUs); `sku_name` +
`usage_start_time` join `system.billing.list_prices` for dollars. **Dataproc is NOT in
`system.billing`** - the Spark half stays on `milliDcuSeconds`.

**The EXPLAIN COST bridge works and has a structural limit worth knowing before relying on it.**
`databricks.heavy_queries` reads `system.query.history` (statement text is populated on every row;
2,820 SELECTs / 132.6 query-hours in 2 days) and `analyze_queries` plans each. But **0 of 20
heaviest statements are plannable**: all reference `prod.ml.ddp_vertical_classification_api`, which
is **dropped and recreated ~21x/day** (149 DROPs in 7 days). EXPLAIN COST replays historical SQL,
so any query touching a transient table can never be planned after the fact. Run it on recent
queries against durable tables, or capture the plan at run time.

**A false positive I shipped and had to retract.** `EXPLAIN COST` SUCCEEDS as a statement and
returns the planner's error as its RESULT TEXT, so catching an exception is not enough. That text
parses: an unresolved plan carries no statistics, so `missing_statistics` fired, and
`repeated_scan` fired on the two unresolved references. I reported both as real findings on
`ddp_vertical_classification_api` before checking whether the table existed. `explain_cost` now
rejects any plan containing a planner-error marker or `unresolvedalias`. **The lesson is the
order:** confirm the object exists before believing a finding about it.

**What survived that retraction, because it came from query history rather than a plan:** four dbt
TESTS on `ddp_vertical_classification_api` consumed **131 of the top-12 nodes' 135 warehouse-hours
over 2 days**, 57 runs each, **1,178 TiB read**; the models themselves are ~1 hour. Every test
filters `WHERE load_ts = (SELECT max(load_ts) FROM <same table>)`.

**Open disagreement to chase:** the prod digest renders `fangorn_score_monitor` and `ipdsc_ds_35`
unlinked while the coverage report does NOT list them as unresolved. Same `Coverage` object feeds
both, so suspect the `_owner_index` cache or the difference between what `unresolved()` is passed
and what `resolve()` is passed. Ordered plan: `artifacts/audi_1194_next_steps.md`.

## 2026-08-26 — cost has a unit, Databricks has a price, Slack has the digest

- **#1222 and #1223 merged.** Job-to-DAG resolution live in prod (`profiled this sweep` 2 -> 12,
  7 of 217 jobs untied); manual trigger no longer needs an explicit `logical_date`.
- **Cost was 29% short.** `ExecutorAdded` covered 359 of the 497 executors that ran tasks in one
  prod log; the rest were billed at zero. Seeding `added_ts` from the first task's launch moves
  that run 276.1 -> 356.6 executor-hours. [[reference_spark_eventlog_cost_units]]
- **IMP-084 closed.** `cost_h` on every finding; `high` needs 10 executor-hours OR 10% of the run.
  `_cores` returns 0 when unknown rather than guessing 1, and an underivable cost never demotes.
- **Peak concurrency settled** and an earlier conclusion reversed: it IS measurable, with a 100ms
  handoff tolerance. `site_network_hourly` saturates 1,988 slots at peak and averages 2.2%, so
  `maxExecutors` is not the lever; the tail is.
- **Databricks costed.** `job_costs` / `query_costs` price jobs and dbt nodes from
  `system.billing`. [[reference_databricks_billing_cost]]
- **Biggest single finding:** four `ddp_vertical_classification_api` dbt tests each scan 5.13 TB
  / 2.15M files ~20x/day to return one row, and are 98.6% of a warehouse costing $850/week.
- **Slack delivery live.** `notify.py` + `digest.blocks()` post Block Kit to `#spark-optimizer`
  (`C0BSTH6E84T`), reusing the `airflow-debugger` app. [[reference_slack_debugger_app]]
- **Open:** prod's `collect_local` leaves `fangorn_score_monitor` and `ipdsc_ds_35` unlinked while
  the REST path resolves both (`audience_intent`, `tpa_ipdsc_export`). Astro needs
  `OPTIMIZER_SLACK_CHANNEL` set before the DAG posts.
- **All work consolidated into airflow-ti #1229** on 2026-08-26 at the user's ask: the two
  AUDI-1194 branches plus AUDI-1191's `audi-1191/two-channels` (another session's), merged into
  `audi-1194-1191-combined`. Zero file overlap, no rebase, 336 tests. #1225/#1227/#1228 closed
  pointing at it. The debugger files were left untouched, including three 2-line comment blocks
  our `lint_comments.py` would fail but which airflow-ti does not lint.
- **#1229 MERGED same evening** (squash `03706e8`); the three comment blocks were tightened to
  one line before the squash (`f48fea9`). The delivery slice shipped with one completed
  adversarial pass (4 confirmed/fixed); a second-round certification was attempted twice and
  died both times (usage limit, then IMP-086: gauntlet resume replays a cached fixer's report
  but not its edits and self-declares THRASH), and a third run was cancelled at merge.
- **Two real defects the delivery gauntlet caught in the Block Kit renderer:** the parent
  collected only `new` + `chronic`, so a `fix_not_working` DAG never reached Slack; and the
  partial-sweep and no-change-tracking caveats reached only the text digest, so a Slack reader
  saw a confident post with the warning missing. Both fixed.
