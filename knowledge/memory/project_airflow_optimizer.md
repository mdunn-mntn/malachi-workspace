---
name: project_airflow_optimizer
description: AUDI-1194 airflow_optimizer/ — key-free efficiency crawler over SUCCEEDED Spark jobs; SHIPPED TO PROD 2026-08-21 as the spark_optimizer_daily DAG in airflow-ti (215 jobs/290 findings on run 1); coverage pass is dead on Airflow 3 (ORM forbidden in tasks)
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [airflow optimizer, AUDI-1194, spark_optimizer_daily, airflow-ti 1212, spark-optimizer service account, serviceAccountTokenCreator, CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT, airflow session use is forbidden, spark optimization crawler, efficiency sweep, eventlog parser, 7-surface spark, optimization detectors, skew spill shuffle, fleet crawl backlog, daily optimizer cron, oncall_daily_optimizer, com.mntn.daily-spark-optimizer, phs event logs, phs.fetch_logs, dataproc-debug pam, audi-storage-object-view, 242x skew, dataproc databricks optimization, straggler detector, idle_reserved_executors, shuffle_fetch_wait, map-side concentration, site_network_hourly stage 9, optimization ledger, optimizer coverage gap, optimizer digest, sweep.py, ledger.py, coverage.py, digest.py, workload identity runner, EXPLAIN COST statement execution api, jobs get-run-output empty, IMP-029 rolling dirs]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-08-21
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
