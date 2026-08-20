---
doc_type: runbook
title: On-Call Runbook — Master
summary: "Read FIRST on any Airflow/pager/pipeline alert. Triage protocol, alert catalog (signature→verdict→protocol), incident log, producer→consumer maps. Every resolution appends back here."
last_verified: 2026-08-10
keywords: [on-call, oncall, on call, incident, pager, pagerduty, alert triage, airflow failure, airflow alert, pipeline failure, dag failure, task failed, sensor timeout, AirflowSensorTimeout, precondition_bombora, ipdsc_monitor, tpa_ipdsc_export, ipdsc, bombora, DS51, optional partner skip, fangorn_inference_pipeline, inference_pipeline, create-dataproc-cluster, dataproc, dataproc saturation, resource contention, champion challenger, 94% cap, vertex pipeline, INC-008, zonal stockout, us-central1-a stockout, code 14 UNAVAILABLE, COMPUTE_ENGINE stockout, zone does not have enough resources, does not have enough resources, dataproc create failed, cluster create failed, challenger upstream of inference, overlap test, discriminating test dataproc, IMP-015, multi-zone dataproc, fallback zone, N2_CPUS quota, DISKS_TOTAL_GB quota, insufficient quota, insufficient N2_CPUS quota, compute quota ceiling, quota exhaustion, 94% cap, 93% quota, workerpool0-0, self-block retry, lingering ERROR cluster, failed cluster not cleaned, gcloud compute regions describe, quota vs usage, 290 workers, benign expected, late data, batch-id trap, force_export, prod safety, escalation, runbook, daily_drift_pipeline, feature drift, fangorn_daily_feature_drift_pipeline, reference_date, run_date, parameter not found, input definitions, ValueError, param mismatch, param contract, TiVertexPipelineOperator, PipelineJob, latest bundled version, audience_intent, fangorn_score_monitor, ipdsc_geo, ModelPysparkBatchOperator, dataproc serverless, dataproc batch, batches wait, driver output, AnalysisException, PATH_NOT_FOUND, path does not exist, producer consumer race, PAM, privileged access manager, storage.objects.get, INC-001, INC-002, INC-003, INC-004, enriched_impressions, analytics_curated, bombora skip downstream, ds51 zero, ds51 disappeared, tpa_mntn_id_export, mntn id export, mntn_id_data, tpa export, ttl exceeded, batch cancelled, batch was cancelled, cancelling batch as ttl exceeded, dataproc serverless ttl, ModelPysparkBatchOperator ttl, FetchFailedException, FetchFailed storm, shuffle fetch, shuffle fetch failure, auth bootstrap timeout, doSparkAuth, SettableFuture timeout, maxExecutors, zero ttl headroom, sh-dw-external-tables, INC-005, recomputation spiral, uncached lineage, shuffle spill, memory bytes spilled, disk spill, spark.sql.shuffle.partitions, shuffle partitions too few, spark event log, eventlog profiler, cache mntn_df, persist dataframe, dataproc temp bucket, spark-job-history, zstd event log, gcloud-crc32c gatekeeper, storage api download, keyword_ddp_reporting, wait_for_product_categorization, product_categorization, mntn_match_incrementals_fetch, ExternalTaskSensor, external task sensor timeout, execution_delta, allowed_states, openai batch, openai batch runner, batch_fetch, batch_transition, shopper_graph, mntn matched, DS19 keyword pipeline, reschedule sensor, INC-006, INC-007, mntn_match_incrementals_submit, batch_submit, openai file storage quota, 2.5TB file quota, file storage quota exceeded, client.files.create, openai batch quota, exceeded your file storage quota, ExternalTaskFailedError, sensor fast-fail, upstream_failed, batch_submitter, openai_batch_submissions, submit dag failed, product categorization missing, batch_transition, FileNotFoundError, batch_transitioner, batch_cleanup, openai file cleanup, AUDI-1042, openai storage quota ticket, Victor Savitskiy departed, 30-day IPDSC lookback, Ryan Kleck, mntn matched keyword pipeline owner, deploy_openai_dockerhub_gcp, deploy_middleware_dockerhub, openai_batch_runner image, DbtImageName, OPEN_AI_BATCH, image_pull_policy Always, shopper_graph deploy, middleware deploy wrong image, workflow_dispatch, merge is not shipping, mntn-argocd, which image which workflow, auto_paging_iter, SyncCursorPage, openai sdk pagination, batch_cleanup crash, delete_all_storage_files, shopper_graph#297 regression, shopper_graph#298, shopper_graph#299, cleanup regression, files.list pagination, dt=2026-07-28 backfill, write_targeted_signal_ds_19, write_targeted_signal_ds_13, write_targeted_signal_ds_19_domain, targeted_signal, targeted_signal_ds_19, mntn_matched_reporting, DbxDbtOperator, KubernetesPodOperator eviction, pod evicted mid-run, pod not found, not found during istio check, ApiException 404 pods not found, Could not read served logs, served logs timeout, connect timeout 39091, No exception message found, sources empty, reusing existing pod, reattach pod, generic_dbt_runner_ml, prod_warehouse_2xs, dbt python model, dbt python table model, databricks jobs run, run_id 65237255325756, cluster autoscaler scale-down, node preemption, safe-to-evict, do-not-disrupt, await_pod_completion, keyword_ddp downstream dbt task, INC-009, IMP-018, ExecutorLostFailure, spot instance preemption, spot instance kill, spot preemption, executor lost, spill to disk, spark executor memory, bump executor ram, on-demand fallback, PREEMPTIBLE_WITH_FALLBACK_GCP, first_on_demand, gcp autoscaling change, long-running dag killed too long, brian gcp fix, vendor payments ddp, give money to vendors, databricks job compute, all-purpose cluster, sql warehouse dbt, spark history server retention, data-ing-ai, agentic oncall triage, IMP-021, targeted_signal databricks spot, INC-010, wait_ds17_src, ds17, DS17, sharethis, ShareThis, mandatory partner sensor, mandatory data source, wait_ds_src, GCSObjectsWithPrefixExistenceSensor, mntn-data-partners, partner feed missing, missing source files, backfilled copy passes sensor, existence sensor stale data, day-1 fallback, resolve to day-1, copied previous day data, Sean Yang, tpa_ipdsc mandatory sensor, partner delivery late, INC-011, hashed_email_ds_26_signals, wait_fpa, fpa_site_visit_batch_serverless, dsid26_predactiv_processing, predactiv, DS26, source_available_dsid26, ShortCircuitOperator, short circuit skip, skipped treated as failure, skip as failure, ExternalTaskSensor skipped, external task skipped, populate_hem_data_ds_26, hashed email signals, mntn-data-partners predactiv, no source data for dsid, skipped_states, sensor false alarm, benign partner data gap, DataprocCreateBatchOperator skipped, external_task_skipped signature, cross-DAG contract mismatch, IMP-026, INC-012, materialize_mntn_select, mntn-select batch, ipdsc_mntn_select, augmentor_log listing, Error listing gs, SocketTimeoutException, read timed out, gcs list timeout, flat glob, fs.gs.glob.flat.enable, globStatus, get_paths, spark_utils get_paths, lost executor red herring, executor decommission spark scale down, driveroutput, staging bucket driver output, constant runtime death, agent reports job failure constant interval, hh partition hole, materialize backfill dt hhs, IMP-027, spark.app.name attribution, event log attribution, INC-013, dsid30, DS30, dsid30_augmentor_log_processing, fpa-dsid30, fpa_vendor_log, site_visit_signal dsid30, augmentor_log sibling reader, auction_log_augmentor_process_gcs, create_mntn_global_data, silent try except degrade, pihole dns block, logging.googleapis.com blocked, 0.0.0.0 resolve, curl resolve pin, IMP-031, INC-014, sharethis categories, static reference file deleted, lifecycle age 365, bucket lifecycle delete, PATH_NOT_FOUND static path, mntn-data-partners lifecycle, soft delete policy null, ipdsc_ds_17, INC-015, feature_store_setup_model, feature store paused, missing feature store day, wait_for_challenger_features, challenger sensor timeout, daily_drift_pipeline PATH_NOT_FOUND, guid_log_pivot_ip_vertical_id, LOOKBACK_DAYS, drift lookback window, run_daily_feature_drift, fangorn drift self-heal, unpause checklist, vertex pipelineJobs list, ml_job replica exit, green run not data landed, task duration proxy, anomalously fast task, silent degrade signature, retired model directory, false missing partition, cross-check dag task list, GCP_INSUFFICIENT_CAPACITY, ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS, VM_MIN_COUNT_NOT_REACHED, databricks stockout, flexible node types, worker_node_type_flexibility, taxonomy_vector, mntn_matched_taxonomy_vector, quota vs stockout, LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY, mntn-databricks, us-central1-f, c2-standard-8, alert names the wrong task, walk to the producer, INC-022, No exception message found, no exception message, worker loss burst, multiple dags failing same window, try 0 of 3, task did not raise, pod died no traceback, self-recovered retry, already retrying do not clear, ipdsc_ds_35, wait_for_ipdsc_geo, intent_score_map, audience_intent failure, blamed the wrong merge, INC-021, site_network_hourly, unable to acquire impersonated credentials, impersonated credentials 503, 503 getting metadata from plugin, AuthMetadataPluginCallback, grpc plugin wrapping, google.auth.default, UNAVAILABLE iam credentials, batch never created, failure before submission, model heals last 2 hours, 2-hour overlap self heal, ipdsc_site_network, site_network_hourly subdirectory, location_root subdirectory mismatch, matched no objects false alarm, retries 0 default_args empty, job always succeeds skip hours, Skip dt= silent skip, INC-020, IMP-044, AirflowSensorTimeout, sensor has timed out, run duration exceeds the specified timeout, hashed_email_guid_log_signals, wait_fpa timeout, external task sensor timeout 900, producer slow not broken, late data sensor impatience, sensor timeout does not retry, retries skipped on sensor timeout, try 0 of 2, reschedule mode sensor, dsid23_guid_log_processing, dsid26_predactiv_processing, hashed_email_signal partition, signals/hashed_email_signal, subtract hours 1 vs 2, different hh offsets per consumer, mode append duplicate partition, uncaught hourly hole, INC-019, IMP-043, get_dt_hh_by_dsid scheduler gap, producer run duration distribution]
tags: [on-call, airflow, incident-response]
---

# On-Call Runbook — Master

**Read this FIRST on any on-call alert. Append an incident entry after every resolution.**
The more incidents we log, the faster the next one closes. If an alert matches a row in
§2 Known-Alert Catalog, jump straight to its protocol.

- **Entry point:** run **`/oncall`** (or `/oncall <alert-log-file>`) — it triages, matches the catalog,
  and **enforces the write-back** (§3 + §2 + the JSONL log) so nothing leaks. This runbook is what
  `/oncall` reads and writes.
- **Home:** `on-call/`. A **new** raw alert log lands at the top level (named as downloaded) so the
  triage-reminder Stop hook (`find -maxdepth 1`) flags it as un-triaged. **On resolution, file its logs
  under `on-call/incidents/INC-NNN/`** (one folder per incident), renamed
  `<dag-or-task>_<rundate>_try<N>_<outcome>.txt` — this clears the loose-log signal and keeps evidence with
  its incident. Top level stays just the runbook, `incident_log.jsonl`, and the `incidents/` tree.
- **Indexed:** this file carries `doc_type: runbook` front-matter, so `.claude/scripts/build_index.sh`
  folds its `keywords:` into `knowledge/_ROUTING.md` and lists it in `knowledge/runbooks/INDEX.md`.
  Grep `_ROUTING.md` for an alert symptom (`sensor timeout`, `dataproc`, `bombora`) and it points here.
  **After editing §2/§3 keywords, rebuild:** `.claude/scripts/build_index.sh`.
- **Update rule (3 surfaces, every resolution):**
  1. **§3 Incident log** — full incident (the human narrative + diagnosis + decision tree).
  2. **§2 Known-Alert Catalog** — one-line signature row (so the next match is instant).
  3. **`on-call/incident_log.jsonl`** — one machine-readable record (the queryable index; see §5).

  Never delete rows — a "benign, expected" verdict is as valuable as a fix.
- **Prod safety (non-negotiable):** never modify prod DAGs or push to `main` in `airflow-ti` /
  `sqlmesh` to "fix" an alert. Diagnose → clear/re-run or route to the owner. Widening a timeout or
  soft-failing a sensor is a code change owned by the producing team, not an on-call action.

---

## 0. Is this on-call? — classify FIRST, then pick the surface to write to

On-call work and ticket work look similar (both "something's wrong, investigate") but they are
**different workflows with different homes.** Decide before you start — the write-back surface differs.

**It's ON-CALL (→ this runbook + an INC entry) when the trigger is an operational alert:**
- An Airflow/Astronomer task **FAILURE** or **retry-exhausted** email/Slack (`🔴 [prod] Airflow <Team>
  FAILURE [dag/task] at <ts>`), a **PagerDuty** page, a sensor **timeout**, a pipeline that **broke**.
- The job is to **restore/clear/route** and **explain the alert**, not to answer an open question.
- Output = a resolved alert + an incident record that makes the next identical alert instant.

**It's a TICKET (→ `tickets/`, `/frame`, `summary.md`) when the trigger is a question or a change:**
- "Did X move a KPI?", "size/evaluate Y", "build Z", "why does the system do W?" — analysis, design,
  or a deliverable. No pager fired; nobody is waiting on a pipeline.
- The job is to **answer a falsifiable question** and produce a deliverable.

**Decision rule:** _Did an alert/pager fire and is a pipeline currently degraded?_
→ **yes = on-call**, use `/oncall`, write to this runbook.
→ **no = ticket**, use `/frame`, write to `tickets/`.

**Where each artifact goes:**

| Artifact | On-call | Ticket |
|---|---|---|
| Entry-point skill | `/oncall` | `/frame` → work → `/capture` |
| Working record | §3 incident (`INC-NNN`) | `tickets/<key>/summary.md` |
| Fast-match index | §2 catalog row + JSONL | `tickets/INDEX.md` + `_ROUTING.md` keywords |
| Raw evidence | `on-call/<downloaded log>` | `tickets/<key>/outputs/` |
| Durable code fix | **route to owning team** (never hot-patch) | the ticket / a PR |

**The crossover:** an alert that reveals a real, recurring defect (not a one-off) **spawns a ticket for
the durable fix** — but the incident still gets logged here first. Example: INC-001's durable fix
(`soft_fail=True` on optional-partner preconditions) is an `airflow-ti` change → propose it as a ticket,
don't hot-patch. The INC entry records "routed to ticket TI-XXX"; it doesn't do the code change.

---

## 1. General triage protocol (any Airflow alert)

1. **Identify** DAG + task + logical date from the alert (`[prod] Airflow <Team> FAILURE [dag/task] at <ts>`).
2. **Pull the task log** — `bash .claude/scripts/airflow_pull.sh --date <D> [--dag <dag>]` dumps every
   task's log + a `_manifest.jsonl` pass/fail grid to `on-call/airflow_logs/<D>/` (needs `astro login`;
   `--watch --tag <tag>` auto-drops live failures into `on-call/`). Then find what the task is *actually
   doing* — not that it failed:
   - **Sensor** → the poke target (`Sensor checks existence of : <bucket>, <object>`).
   - **Producer/Spark/BQ** → the output path / query / the real exception (search the log tail for `ERROR`/`Exception`/`Traceback`, skip the boilerplate).
3. **Check empirical state** — did the thing it waited on / was supposed to write actually land?
   `gcloud storage ls -l "gs://<bucket>/<path>/"` (reauth with `gcloud auth login` if you get
   `Reauthentication required`; do it as ONE call — parallel gsutil calls trip the reauth quota).
   - **A GREEN run is not proof the data landed.** Airflow task success is orchestration-level only; a
     swallowed read can ship an empty partition with every task green (INC-013 shipped `mntn_global_data`
     with zero augmentor rows on a green run). **GCS/BQ output listing is the only proof.**
   - **When GCS is unreachable (expired gcloud SSO, PAM gap), per-task DURATION vs a known-good scheduled
     run is the cheap proxy** — an anomalously FAST task is the silent-degrade signature. INC-015's
     backfill checked out because durations tracked the healthy run (pivot 5.9m vs 6.5m, derived 7.0m vs
     8.0m, L1 3.7m vs 4.1m). It is a smell test, not proof — still list the output when access returns.
   - **A recovery action can MANUFACTURE a false green.** Clearing a task whose backing Dataproc batch is
     still RUNNING cancels that batch, and Airflow records the new try as SUCCESS with no output (INC-018:
     try 3 green in 2:28 against a ~7 min healthy run, batch `CANCELLED`, partition still empty). Never
     clear a task that is still doing real work, and verify a re-run by its output partition.
   - **Not every empty directory is a hole: cross-check against the DAG's actual task list first.**
     Retired models leave live-looking GCS prefixes frozen at their last write (INC-015: two
     `*_derived_advertiser_id_dsc_id` dirs dead since 2026-02-08, superseded by a merged model), so a
     directory sweep over-reports missing partitions. Confirm the producer task still exists before
     calling a gap.
4. **Classify** (see verdict taxonomy below).
5. **Act** per class. **Log** the incident on all 3 surfaces (§3 + §2 + JSONL).

**Verdict taxonomy**

| Class | Signature | Action |
|---|---|---|
| **Benign / expected** | Alert is a known side-effect of intended behavior (e.g. optional-partner skip). Main pipeline succeeded. | Ack. Reply in thread "expected, <reason>". No re-run. Log it. |
| **Late data** | The awaited object exists *now*, arrived after the sensor's window. | Clear the failed task → it passes immediately. Not an outage. |
| **Transient infra** | A downstream cloud resource failed to provision/transiently errored (e.g. Dataproc cluster create, quota/stockout, 5xx). Config + inputs are fine. | Re-run the task once. If it recurs, check quota/region capacity, then route to the owning team. |
| **Resource contention** | Our OWN concurrent job holds the resource (e.g. a challenger/QA run saturating Dataproc → `create-dataproc-cluster` code 9). Not stockout, not config. | Do NOT blind-re-run (it re-fails while the other job holds it). Confirm no concurrent job, let it FINISH, then re-trigger. Recurs → durable fix (stagger/quota) → `improvements_backlog.md`. |
| **Real upstream failure** | Object genuinely absent AND was required; or producer task threw a real error. | Find + re-run the producer task (mind batch-id traps), or route to the feed/vendor owner. |
| **DAG/logic bug** | Wrong path, bad param, code regression. | Route to the owning team with the evidence. Do NOT hot-patch prod. |

---

## 2. Known-Alert Catalog (signature → verdict → protocol)

Grep the **DAG/task key** to match fast. If your alert's key is here, jump to its protocol.

| DAG / task key | Alert signature | Root cause | Verdict | Protocol |
|---|---|---|---|---|
| `ipdsc_monitor / precondition_<partner>` | GCS sensor **18h timeout** (e.g. `precondition_bombora`, DS51) `AirflowSensorTimeout` | Optional 3P partner didn't deliver source files that day → producer skips it silently → monitor pages on the absent `ipdsc/dt=.../data_source_id=<id>/` partition | **Benign / expected** on partner-skip days (verify source absence first) | INC-001 |
| `fangorn_inference_pipeline_run / inference_pipeline` | `RuntimeError: Job failed with: code: 9 … failed tasks are: [create-dataproc-cluster]` (PagerDuty, retries exhausted). The 290-worker cluster needs **~4,672 N2_CPUS (~93% of the 5,000 us-central1 quota)** + ~145,500 GB disk, so it fails on the CAPACITY CEILING via one of 3 surfaces: a Dataproc-op **zonal `code 14 UNAVAILABLE`** stockout, a Vertex/worker-pool **`Insufficient N2_CPUS/DISKS_TOTAL_GB quota`** error, or (INC-002) starvation by a truly-concurrent job. | Usually an **external transient GCP stockout** (GCP out of large-instance machines in the zone; owner-confirmed for INC-008) that SELF-RECOVERS in ~1-2h. Because the cluster is ~93% of N2 quota it has no headroom, so it can ALSO fail on: quota exhaustion — often a SELF-BLOCK from its OWN prior stockout-failed cluster's VMs lingering (INC-008); or real concurrent-job contention (INC-002). ⚠ `inference_pipeline` is UPSTREAM of `challenger_inference_pipeline` (sequential) — the challenger is NEVER the contender. | **Pull ALL 3 surfaces + reconcile** (don't name a cause from one): `dataproc operations describe <failed-create>` (zonal stockout), the Vertex `service`/worker-pool log (quota), and `gcloud compute regions describe us-central1` quota-vs-usage. Delete any prior ERROR cluster still holding VMs (it self-blocks the retry). Default: **re-run — stockouts self-clear**; autozone re-picks. Persistent across zones → owner+infra (raise quota / auto-delete failed clusters / multi-zone / smaller cluster, IMP-015). | INC-002 (concurrent-job contention), INC-008 (transient stockout + quota self-block, 07-30) |
| `fangorn_inference_pipeline_run / daily_drift_pipeline` | `ValueError: The pipeline parameter reference_date is not found in the pipeline job input definitions` (retries exhausted → PagerDuty). **Different task + signature from INC-002 — not resource contention.** | `TiVertexPipelineOperator` ALWAYS injects `reference_date` into the Vertex `parameter_values`, but the drift template declares `run_date` (its KFP source `fangorn_daily_feature_drift_pipeline.py:393` uses `run_date`) → `PipelineJob.__init__` rejects the unknown param before submission. Param-contract mismatch. | **DAG bug** — route to owner (Brian/ML). **PR #1158 (airflow-ti) does NOT fix it** (confirmed: re-run on the fixed bundle re-failed identically); the operator-injected `reference_date` is the failing param. Real fix = rename the KFP pipeline param `run_date`→`reference_date` in **`targeting-infra-ml`** + recompile/redeploy the template. Do NOT blind-re-run until that ships. **RESOLVED 2026-07-28** (Brian redeployed template, green on try 5). | INC-003 |
| `audience_intent / fangorn_score_monitor` | Airflow log = boilerplate `AirflowException: … Dataproc Agent reports job failure`; **batch driver output** = `AnalysisException [PATH_NOT_FOUND]: gs://mntn-data-archive-prod/ipdsc_geo/dt=<run_date>`. PagerDuty, retries exhausted. | Consumer `ModelPysparkBatchOperator` reads `ipdsc_geo/dt=<run_date>`, which lands on D+1 with ~3.5h-variable timing (tpa_export `run_geo`); monitor has only `retries=2×10min` + no cross-DAG sensor → races the producer, pages when it slips past ~07:45Z. | **Late data** (this case) — pull the driver output for the real error (Airflow log is boilerplate), confirm `ipdsc_geo/dt=<run_date>/_SUCCESS` is present, then clear+re-run the monitor. If partition still absent → real upstream failure, re-run tpa_export `run_geo`. **RESOLVED 2026-07-29.** | INC-004 |
| `tpa_mntn_id_export / tpa_mntn_id_export` | Airflow log = boilerplate `AirflowException: Batch job <id> was cancelled`; **batch `stateHistory`** = `Cancelling batch as ttl exceeded` (ran the full `ttl=10800s`=3h); **event log** = the final `.write.json()` shuffle stage recomputed 7-9× (same `json at ...` call site, ~1900GB each), 29TB memory + 14TB disk spill, `shuffle.partitions=1000`, 150 executors 0 removed. All retries exhausted. | **DAG_BUG (Spark perf), verified from the event log.** The `mntn_df` lineage is never cached, so the ~1.9TB `mntn_id` shuffle is recomputed 7-9× per action + FetchFailed resubmit; `shuffle.partitions=1000` → ~1.9GB partitions → 29TB spill → I/O-bound tasks (70-97% fetch-wait, 8-10% CPU) → past 3h. NOT infra (0 executor loss), NOT data volume (inputs identical to last good day), NOT contention. | **dag_bug (perf)** — get the TTL reason from `batches describe` stateHistory; then download+parse the Spark **event log** (`eventlog_profiler.py`) for the real profile (driver output alone is not enough). Fix is owner-side: **cache `mntn_df`** + raise `shuffle.partitions` 1000→~6000 + collapse the 14 crossJoins, then a modest TTL bump. A re-run may pass (spiral is timing-dependent) but does NOT fix it. Do NOT hot-patch. **RESOLVED 2026-07-29 — PR #1161 merged by owner Nivas Nalla; confirm on next run, 07-28 backfill optional.** | INC-005 |
| `keyword_ddp_reporting / wait_for_product_categorization` | Two variants of the SAME sensor: **(a)** 6h `AirflowSensorTimeout` (307 pokes) = upstream still running/not-ready (INC-006); **(b)** fast-fail `ExternalTaskFailedError` in ~9s (single poke) = upstream `product_categorization` in `failed`/`skipped`/**`upstream_failed`** (post-PR #1162, INC-007). Both waiting for `mntn_match_incrementals_fetch.batch_post.product_categorization` = success at logical `−6h`. | Downstream **symptom** — real cause is upstream in the DS13/DS19 MNTN Matched OpenAI pipeline. GCS `dt=` partition **absent**. INC-006 = `batch_fetch` loop-abort (fix shopper_graph#296). INC-007 = `batch_submit` failed 3 levels up on the OpenAI file-quota (see next row). | **real_upstream_failure** — audit the GCS chain `openai_batch_submissions→results→results_joined→product_categorization` for the missing `dt` to find WHICH stage broke (a missing `submissions/dt` means it failed at *submit*, not fetch). Do NOT clear the sensor until `product_categorization/dt` lands (clear just re-fails fast / re-waits). PR #1162 makes it fail fast; #296 fixes the fetch bug. **#296 ships ONLY via `deploy_openai_dockerhub_gcp.yml` (openai_batch_runner image, context `openai/`) — NOT the middleware deploy; deployed 2026-07-30, first exercised 07-31 09:00Z. Deploy-workflow map → memory `reference_shopper_graph_deploy`.** | INC-006, INC-007 |
| `mntn_match_incrementals_submit / batch_submit` | `batch_submit` (MntnKubePodOperator, `openai_batch_runner`) fails ALL retries; pod traceback `submit_batch.py → batch_submitter.create_batch → client.files.create` → **OpenAI 400 `invalid_request_error`: "You have exceeded your file storage quota. Projects are limited to 2.5TB of files."** | OpenAI project ≥ 2.5TB file-storage quota → batch-input upload rejected → NO `openai_batch_submissions/dt=<D>` written → next-day fetch DAG has no batch → `product_categorization/dt=<D>` upstream_failed → keyword_ddp sensor pages a day later. Deterministic 400 (retries can't fix a quota wall). Likely aggravated by INC-006 leaked/undeleted OpenAI files. | **real_upstream_failure (OpenAI resource/quota exhaustion)** — free OpenAI file storage (purge old files); quota can self-clear via old-file expiry (07-29 submit succeeded on its own). Re-run `batch_submit` for the missed cycle only if that day is needed (new batch = cost + ~24h). Do NOT rebuild the image / re-run the FETCH DAG (no batch to fetch; #296 irrelevant). Durable cleanup fix **shopper_graph#298 DEPLOYED 2026-07-30** (⚠ the first attempt #297 REGRESSED on a nonexistent `auto_paging_iter` and crashed every `batch_cleanup`; #298 reverted to `for file in client.files.list():` + per-file delete + 72h→48h, holds ~150 GiB, `batch_cleanup` verified green); **AUDI-1042 In Progress (P1)**, validation = storage ~2.4TB→~150GB on next cleanup cycles. | INC-007 |
| `keyword_ddp_reporting / write_targeted_signal_ds_{19,13,19_domain}` | Empty log / **"No exception message found"** / `sources=[]`; grep reveals `Could not read served logs … timed out` (try 1) then `not found during istio check` + `ApiException: (404) … pods … not found` in `await_pod_completion` (try 2). ~40-52 min then fail; both `retries=1` attempts exhausted → terminal FAILED (downstream ds_13/ds_19_domain never run). **Different task from INC-006/007** (those = the `wait_for_product_categorization` sensor upstream; here the sensor is GREEN). | `DbxDbtOperator` (`KubernetesPodOperator`, image `generic_dbt_runner_ml`) runs dbt **python** model `mntn_matched_reporting.targeted_signal`, which submits a Databricks **Jobs** run (`run_id` in the log) + polls it ~50 min. **CONFIRMED (Ryan, Spark UI):** the job's Spark executors are killed by **`ExecutorLostFailure … Reason: spot instance preemption, spot instance kill`** (+ ~5 GiB/task spill) → the job thrashes and never completes in the pod's window; the same spot-reclaim wave also kills the KPO pod (log-server loss / 404). NOT a dbt/data error; input `product_categorization/dt` healthy. | **transient_infra** (Databricks job cluster on **spot** instances, no on-demand fallback + heavy spill — chronic config) | INC-009 |
| `tpa_ipdsc_export / wait_<ds>_src` (mandatory partner, e.g. `wait_ds17_src` = ShareThis/DS17) | 1h `AirflowSensorTimeout` (`run duration … exceeds 3600.0`, `mode=reschedule`, ~6 reschedules) on a `GCSObjectsWithPrefixExistenceSensor` polling `gs://mntn-data-partners/partners/<vendor>/segments/date=<data_interval_start−1d>/`. **Hard-fails + pages** — distinct from the OPTIONAL `precondition_<partner>`/`wait_bombora_src` (DS51, `soft_fail=True` → SKIPPED, INC-001). | Mandatory 3P partner missed its daily source-file delivery → the prefix is empty through the 1h window → the mandatory sensor hard-fails (docstring: "mandatory data sources DS4, DS17, … are never tolerated"; only registry-`optional` partners soft-skip). | **real_upstream_failure (late/missing partner data)** — verify absence in GCS, route to the feed owner; NEVER widen the timeout / soft-fail a mandatory sensor (prod DAG change). ⚠ existence-only sensor: a backfilled COPY (even stale/wrong-dated) greenlights a retry silently. Forward fix shipped (Sean): auto-resolve to **day-1** data on late source. | INC-010 |
| `hashed_email_ds_26_signals / wait_fpa` | `ExternalTaskFailedError: Some of the external tasks ['dsid26_predactiv_processing'] in DAG fpa_site_visit_batch_serverless failed.` — **fast-fail in ~5s** (single poke), NOT a timeout. Sensor polls the SAME logical date (no `execution_delta`). | **False alarm — benign partner-data gap.** The producer DAG SUCCEEDED; `dsid26_predactiv_processing` was **`skipped`** (not failed) because `source_available_dsid26` short-circuited on a missing Predactiv/DS26 hourly file (`No source data for dsid=26 …`). `wait_fpa`'s `failed_states` treats a `skipped` external task as a failure → pages. Distinct from INC-006/007 (upstream truly `failed`/`upstream_failed`) and INC-010 (mandatory existence-sensor timeout). | **benign_expected** — verify the external task's final state is `skipped` + the `source_available_<ds>` log says no source data; no-op the hour (self-heals next run), no backfill. Durable fix (owner): `skipped_states=[SKIPPED]` on `wait_fpa` or short-circuit-gate the consumer DAG (IMP-026). NEVER hot-patch. | INC-011 |
| `materialize_mntn_select / materialize` | `Google Cloud Dataproc Agent reports job failure` (boilerplate), retries dying at a **~constant ~19 min**; driver log shows `ERROR ... Lost executor ... spark scale down` (red herring) then `Error listing gs://mntn-data-archive-prod/augmentor_log/region=` → `SocketTimeoutException: Read timed out`. | Driver-side GCS LIST timeout during input discovery: `get_paths` globs `region={east,west}/dt=/hh=`, and the GCS connector flat-lists EVERYTHING under `augmentor_log/region=` then filters — O(entire prefix), latency-fragile. Executor "loss" = benign idle decommissions while the driver lists. | **transient_infra** (re-run passes; list latency variable) + durable fix SHIPPED (IMP-027): v1 #1176 literal region paths, v2 #1177 drop `basePath` (root stat was a second timeout surface). Answer lives in staging `driveroutput.*` (PAM), NOT the Airflow log, NOT spark-events (PHS-path DAG). | INC-012 |
| `fpa_site_visit_batch_serverless / dsid30_augmentor_log_processing` | `Dataproc Agent reports job failure` boilerplate; intermittent, some hours fail BOTH tries, others pass on retry. Driver traceback: `dsid30_augmentor_log_processing.py:30` → `Error listing gs://mntn-data-archive-prod/augmentor_log/region=` → `SocketTimeoutException`. | INC-012's class in a sibling reader: `{east,west}` glob flat-lists the whole prefix + `basePath` stats the root. Two more unfixed readers found in the sweep (`auction_log_augmentor_process_gcs.py`, `create_mntn_global_data_pyspark.py` — the latter fails SILENTLY via try/except). | **transient_infra** recurring + durable fix SHIPPED (IMP-031: #1179 merged+prod-verified 2026-08-07, all 3 scripts — literal region paths, no basePath, existence guards). Check output holes at hh=H-1 per failed logical run H, both `site_visit_signal` and `fpa_vendor_log` dsid30 outputs. | INC-013 |
| `tpa_ipdsc_export / ipdsc_ds_17` | `AnalysisException: [PATH_NOT_FOUND] Path does not exist: gs://mntn-data-partners/partners/sharethis/categories` (a STATIC path, not a dated partition). | The bucket's unrestricted age-365 Delete lifecycle rule removed the one-time static mapping file on its 365-day birthday; no versioning, no soft delete. Not a code change, not a late delivery. | **real_upstream_failure** — re-obtain from the partner; exempt static reference files from the age rule. Every static file in `mntn-data-partners` has the same expiry. | INC-014 |
| `fangorn_inference_pipeline_run / daily_drift_pipeline` (+ challenger sensor) | Sensor timeout on `feature_store/feature_group_3_pivoted/.../dt=<ds>/_SUCCESS`, then drift code 9 daily; driver: `[PATH_NOT_FOUND] .../dt=<missing day>`. | One missing FS day (producer DAG paused, catchup=False skipped it) sits inside drift's LOOKBACK_DAYS=3 literal-path window; each daily run fails until the window clears the hole. | **real_upstream_failure** — compute the self-heal date from the window; single-date FS trigger only if it can't wait; durable = existence-guard the drift read (owner). | INC-015 |
| `tpa_ipdsc_export / tpa_export` | Alert quotes `exited with 137 code ... potentially signifies a memory pressure` on **try 2+**, but try 1 ran ~44 min and tries 2+ die in 6-18s. Tries 2+ logs say verbatim `Batch with given id already exists.` / `Attaching to the job ... if it is still running.` | TWO stacked failures. (1) Driver SIGKILL 137 on try 1, which here landed AFTER the write completed. (2) The batch id is minted by upstream `create_batch_id__2` and cached in XCom; that task does NOT re-run on a downstream retry, so every retry reattaches to the failed batch and inherits its error. The 137 in the try-2 alert is INHERITED text, not a fresh OOM. | **transient_infra + dag_bug** — check GCS for `_SUCCESS` + object count BEFORE re-running (the work may already be done). To genuinely re-run, clear `create_batch_id__2` WITH downstream so a fresh id is minted (deleting the Dataproc batch also frees the id). Never read the retry's 137 as a second OOM. | INC-016 |
| `materialize_mntn_first_party / materialize` (hourly `50 * * * *`) | Alert on **try 2 of 3**, `Batch job mntn-first-party-<dt>-<epoch> failed with error: Google Cloud Dataproc Agent reports job failure` (boilerplate). try 1 ~1.8m real, tries 2-3 die in 6-12s with `Batch with given id already exists.` / `Attaching to the job ...`. | Same shared-helper defect as INC-016: `create_batch_id` is an `@task` (`include/util/dag_vars.py:31`) that runs ONCE and caches the id in XCom, so every retry reattaches to the failed batch. try 1's real cause is PAM-gated in the staging bucket's `driveroutput.*`. | **transient_infra (cause UNCONFIRMED) + dag_bug.** 1 failure in 100 runs, next hour green, prior 7 days all 24/24 hours. **This DAG does NOT self-heal** (each run owns exactly one `hh`), so the failed hour stays missing until re-run. Re-run = clear `create_batch_id` **WITH downstream**. | INC-017 |
| `materialize_mntn_select / materialize` (hourly `45 * * * *`) | Repeated `Dataproc Agent reports job failure` on try 1, batches dying at a **constant ~12.0-12.6 min** while healthy hours finish in ~7 min. Airflow log is boilerplate; driver output shows the GCS reads SUCCEEDING, then `java.lang.OutOfMemoryError: Java heap space` in `map-output-dispatcher` threads. | Driver-side MapOutputTracker OOM: `spark.driver.memory=9600m` against `spark.sql.shuffle.partitions=5000`. Map-status memory scales with map tasks x reduce partitions, so the driver sat at its ceiling and tips over on heavier hours (intermittent, not every hour). NOT a GCS listing timeout (INC-012) and NOT the batch-id defect. | **transient_infra trending to dag_bug (capacity).** Constant death interval = resource ceiling, not a data bug. Pull `driveroutput` (needs `dataproc-debug` PAM) before theorising. Fix = raise driver memory for the one DAG ([#1198](https://github.com/SteelHouse/airflow-ti/pull/1198), 16g + 4g). Re-run missing `hh=` only AFTER the bundle refreshes, else they OOM again. | INC-018 |

| `hashed_email_{guid_log,ds_26}_signals / wait_fpa` | `AirflowSensorTimeout: Sensor has timed out; run duration of 958.x seconds exceeds the specified timeout of 900.0.` on **both** consumer DAGs in the same hour. NOT the INC-011 fast-fail (that one dies in ~5s on `ExternalTaskFailedError`). | Producer `fpa_site_visit_batch_serverless` **SUCCEEDED** but ran longer than the sensor's 15-min budget (19.5 min on 2026-08-16T01:00Z; median run is 10.0 min, 6/99 runs exceed 15 min). Sensors quit 40s-3min before the external task went green. | **late_data** — clear `wait_fpa` with downstream on the affected run; it passes on the first poke. **A sensor timeout does NOT retry** (`AirflowSensorTimeout` fails the task outright), so `retries: 1` never fires and every occurrence is a manual clear + an hourly hole that stays open until someone notices. Durable fix = IMP-043 / [airflow-ti#1199](https://github.com/SteelHouse/airflow-ti/pull/1199). | INC-019 |

| `site_network_hourly / site_network_hourly` (hourly `50 * * * *`) | `503 Getting metadata from plugin failed with error: ('Unable to acquire impersonated credentials', ... "status": "UNAVAILABLE")`, **Try 0 of 1**, task dead in ~16s. Log stops right after `Starting batch sit-net-hou-fv2-<ts>` at the first `google.auth.default()` call. | GCP IAM credential-minting service returned 503 while the operator was impersonating the service account to submit the Dataproc batch. **No batch was ever created** (the failure precedes submission), so there is nothing to delete and no partial write. | **transient_infra, benign — verify, then no-op.** The model processes the **last 2 hours** every run, so each hour is written twice by consecutive runs; one lost run is covered by its neighbours. The DAG has `default_args={}` = **retries 0**, so a 16s blip pages a human for something one retry absorbs (IMP-044 / [airflow-ti#1202](https://github.com/SteelHouse/airflow-ti/pull/1202)). | INC-020 |

| Any Targeting DAG, **`No exception message found`** + `Try 0 of N` | A burst of these across unrelated DAGs in one window (2026-08-19: `site_network_hourly`, `audience_intent/wait_for_ipdsc_geo`, `audience_intent/intent_score_map`, `tpa_ipdsc_export/ipdsc_ds_35`). Airflow has no exception because the worker died rather than the task raising. | One cluster-level event: on 2026-08-19 all four ended within 40s of each other on 3+ distinct workers after 22min-2h32m of runtime, with empty try-1 logs. Not a code fault, not a deploy (nearest was 15h earlier). Trigger unverified, see INC-021. | **transient_infra.** Check whether the task already retried before touching anything: `GET /dags/<id>/dagRuns/<run>/taskInstances` and look for `try_number > 1` or `failures=none`. All four on 2026-08-19 recovered on their own. Only act if a task has retries=0 or exhausted them. Consequence now guarded: [airflow-ti#1206](https://github.com/SteelHouse/airflow-ti/pull/1206) (merged 2026-08-20) makes each `ModelPysparkBatchOperator` try cancel any live batch left by a previous try. The recycle itself is still unguarded — deferrable is broken on Airflow 3.1.5, see IMP-049. | INC-021 |

| `keyword_ddp_reporting / write_targeted_signal_ds_13` | dbt `Runtime Error` → `AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view prod.ml.ddp_url_verticals cannot be found` | The source is a dbt table-materialized model that is dropped and recreated on every rebuild. `create_ip_verticals / ddp_url_classification` (daily 00:05 UTC, 25 min to 2h32m) was mid-rebuild. Only collides when the 15:00 reporting run is delayed into the nightly window. | **resource_contention.** Check the producer task's state before touching anything; if it is running, wait for green, then clear the consumer. Do not read `_filtered` as a rename. | INC-023 |
| `fangorn_hhid_inference_pipeline_run / challenger_inference_pipeline` | Vertex `code: 9` boilerplate, `The failed tasks are: [submit-parallel-inference-jobs]`. Every try dies identically. The Airflow log carries NO cause. | The hhid model's challenger alias is gone. `run_challenger_inference.py` resolves the model by alias pattern `challenger-v*`; `fangorn-hhid-xgboost` was re-registered 2026-08-18 and its v1 carries only `default` + `champion`. Sibling `fangorn-xgboost` still has `challenger-v2`, which is why the non-hhid challenger is green. | **dag_bug (registry regression).** Deterministic, so do NOT re-run. Check `models?filter=display_name="<model>"` for a `challenger-v*` alias before anything else; if it is missing, route to the model owner to re-add it. `create-dataproc-cluster` SUCCEEDED here, so this is NOT the INC-002/008 stockout class. | INC-024 |
| `mntn_match_incrementals_fetch / batch_post.taxonomy_vector` (and its consumer `keyword_ddp_reporting / wait_for_product_categorization`) | dbt `Runtime Error in model mntn_matched_taxonomy_vector` → `Cluster '<id>' was terminated. Reason: GCP_INSUFFICIENT_CAPACITY`, detail `VM_MIN_COUNT_NOT_REACHED|ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS`. Every try fails identically for hours. | GCP is out of the requested Intel node type (`c2-standard-8`) in the Databricks project's zone. **Stockout, NOT quota** — a quota fault reads `Quota '<NAME>' exceeded`. `zone_id` was already `auto`, so zone is not the lever, and retry backoff cannot outlast an 8h shortage. | **transient_infra (external).** Do not re-run blindly; confirm the detail string first. Durable fix = flexible node types ([shopper_graph#300](https://github.com/SteelHouse/shopper_graph/pull/300), merged 2026-08-19) plus the workspace toggle *Compute → Enable auto flexible node types*. Full detail + the cost answer: memory `reference_databricks_stockout_flexible_nodes`. | INC-022 |

---

## 3. Incident log

### INC-001 — `ipdsc_monitor` `precondition_bombora` sensor timeout (DS51 Bombora)
**Date:** 2026-07-28 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [ipdsc_monitor/precondition_bombora] at 2026-07-26 17:05 PT` · `AirflowSensorTimeout: run duration 64836s exceeds timeout 64800.0` (18h).

**STATUS: RESOLVED — confirmed benign by owners (no action).** Sean Yang, Brian McAdams (Sr MLE), and
Jordan Piepkow (Staff SWE, author of the skip step) confirmed in #alerts-tpa-pipeline that a missing
Bombora drop "is not an error" — it's skipped by design. Standing practice: **let it slide, drop a note in
the #alerts-tpa-pipeline thread** so the next on-call doesn't re-investigate. Escalate only if Bombora
misses go chronic (feed → vendor, not a re-run).

**Open design question (Jordan, 2026-07-28): remove the sensor, or not?** Recommendation = don't remove;
make it not *page*. Set `soft_fail=_partner.optional` on the registry-driven preconditions in
`ipdsc_monitor` (mirrors `wait_{name}_src` in `tpa_ipdsc_export`): absent partition → SKIPPED not FAILED →
no alert, while drop days stay monitored (removing the sensor would also drop that QA coverage). Optionally
`mode="reschedule"` so it doesn't hold a worker slot for 18h. One-line change, owned by TPA_EXPORT team.

**Provenance (traced 2026-07-28 — the Bombora drop IS the external top of what we own):** No MNTN code
we control fetches Bombora. In airflow-ti nothing writes `partners/bombora/` (only wait/read); our S3→GCS
transfer DAG `storage_transfer.py` does NOT include Bombora; grepping all 8 local MNTN repos found zero
Bombora references outside airflow-ti's read path. The one MNTN transform (`ipdsc_bombora` builder) is
*downstream* of the drop and correctly skips when source is absent. Delivery is automated (good drops land
~20:1x UTC daily, e.g. 07-25 file created 07-26 20:16Z). The only hop I couldn't inspect is whether that
automation is Bombora pushing straight to GCS vs a managed GCP Storage Transfer job in `mntn-prj-prod-00`
(Transfer API disabled on my project) — but owners treat it as a vendor drop, so external.

**Verdict: BENIGN / EXPECTED.** Bombora (DS51) is an `optional: true` partner. It didn't deliver its
source files, so the producer skipped it and no `data_source_id=51` partition was written; the separate
`ipdsc_monitor` DAG doesn't know about the skip and pages on the absent partition. The producer's own
docstring documents this exact alert as expected on partner-skip days. **No action needed** — the main
`tpa_export` pipeline completed fine (exports `{"data_source_id": 51, "cats": []}`).

**The two DAGs (both team TPA_EXPORT):**
- **Producer** `tpa_ipdsc_export` — `dags/tpa_export/tpa_ipdsc_export.py`, schedule `35 2 * * *` (02:35 UTC), severity 0. Writes `gs://mntn-data-archive-prod/ipdsc/dt=<ds>/data_source_id=<id>/`. Bombora build = task **`ipdsc_bombora`** (model `ipdsc_third_party_audience_builder --partner bombora`), gated by source sensor **`wait_bombora_src`** (1h timeout, `mode=reschedule`, `soft_fail=True` because optional → SKIPPED when source absent).
- **Consumer/monitor** `ipdsc_monitor` — `dags/monitoring/ipdsc_monitor.py`, schedule `5 0 * * *` (00:05 UTC), severity 1. Registry-driven loop builds one `precondition_<partner>` `GCSObjectExistenceSensor` per entry in `THIRD_PARTY_AUDIENCE_BUILDERS`, each **18h timeout / 60s poke / `soft_fail=False`** → this is what pages.

**Registry** `dags/ipdsc_third_party_audience_builders.json` — Bombora entry:
`data_source_id: 51`, `optional: true`, `source_date_offset_days: 1`,
`input_glob_template: gs://mntn-data-partners/partners/bombora/segments/{yyyymmdd}/hem_segments_*.csv.gz`.
So `ipdsc/dt=D` sources Bombora files dated **D−1**.

**Diagnosis run (copy-paste for next time):**
```bash
# 1. Confirm the missing ipdsc partition + that ONLY the partner is missing (pipeline else healthy)
gcloud storage ls "gs://mntn-data-archive-prod/ipdsc/dt=2026-07-27/data_source_id=51/"   # -> no objects
gcloud storage ls "gs://mntn-data-archive-prod/ipdsc/dt=2026-07-27/"                       # -> 16 other DS present
# 2. Root cause = check the PARTNER SOURCE for date D-1 (offset 1)
gcloud storage ls "gs://mntn-data-partners/partners/bombora/segments/20260726/"            # -> no objects = Bombora didn't deliver
gcloud storage ls "gs://mntn-data-partners/partners/bombora/segments/20260725/"            # -> delivered fine (contrast)
```
**07-27 evidence:** DS51 partition absent; 16 other sources present (2,4,8,13,14,16,17,18,19,35,42,43,46,47,49,63); Bombora source `20260726/` absent (07-25 present, 07-24/07-27 also absent → Bombora feed is intermittent).

**Decision tree for this alert next time:**
1. Source dir for D−1 **absent** → benign optional-partner skip. Ack, reply, done. (this case)
2. Source dir **present** but ipdsc partition absent → the `ipdsc_bombora` builder failed with a real error. Check the `tpa_ipdsc_export` run's `ipdsc_bombora` task log; re-run it (or mark-success to ship without it — export tolerates `cats: []`). **Batch-id trap:** to re-run `tpa_export`/`ipdsc_geo`, clear the paired `create_batch_id*` task WITH its downstream, else it silently reattaches to the old batch. `ipdsc_<partner>` tasks are immune (try_number in batch id).
3. Partition present *now* → it landed late; clear `precondition_bombora` to pass.

**Late-arriving recovery (partner file shows up after export shipped):** trigger a NEW manual
`tpa_ipdsc_export` run with params `{"dt":"<YYYY-MM-DD>","force_export":true}`. Do NOT task-clear the
original run (params can't change on an existing run; `force_export` stays false → no-op).

**⚠ A re-run does NOT clear an absent-source day — verified.** Bombora source ↔ DS51 partition is a
clean 1:1 all month (offset 1: `ipdsc dt=D` needs source `D−1`). Re-running the monitor just restarts an
18h wait for a file that isn't coming; re-running the producer skips again (no source to build). The only
re-run that ever helps is the **late-arrival** path (`force_export:true`) AND only if the source file
actually shows up. Absent-source days do NOT backfill (e.g. 07-24 source still missing 4 days later).

**History proves self-heal (no manual action needed):**

| ipdsc dt | source (D−1) | source present | DS51 partition | note |
|---|---|---|---|---|
| 07-24 | 07-23 | ✓ | ✓ 49 files | |
| 07-25 | 07-24 | ✗ | ✗ skipped | **same failure — self-healed next day, no rerun** |
| 07-26 | 07-25 | ✓ | ✓ 49 files | recovered automatically |
| 07-27 | 07-26 | ✗ | ✗ skipped | INC-001 alert |

DS51 just has no data on skip days (dt=07-19, 07-25, 07-27, …); the next delivery day self-recovers. This
matches the "we leave it failed and it picks up the following day" prior practice — that is correct.

**Reconciliation (2026-07-28, Brian McAdams, Sr MLE said "needs to be re-run"):** for an absent-source day
there is nothing for a re-run to act on (evidence above). Do NOT restart the monitor on a source-absent
day. The one legitimate action is upstream: **the Bombora feed is degrading** — daily through ~07-09, now
every other day, missed 07-24/26/27 (DS51 empty ~half of recent days). If it needs chasing, that's the
Bombora vendor/feed, not a DAG re-run. Only re-run when the D−1 source is actually present (real builder
crash) or arrives late (`force_export:true` manual run).

**If this pages too often:** the durable fix is to make `ipdsc_monitor`'s DS51 precondition tolerate
skips (e.g. `soft_fail=True` on optional partners' preconditions) so it stops paging on expected skips.
That's a `airflow-ti` code change owned by the TPA_EXPORT / AUDI team — propose it, don't hot-patch.
Tracked as **IMP-001** in `improvements_backlog.md`.

**Update 2026-07-29 (07-28 self-heal confirmed for the IPDSC partition).** Re-verified live in GCS: Bombora
source `partners/bombora/segments/20260726/` **empty** → `ipdsc/dt=2026-07-27/data_source_id=51/` **absent**
(07-26 ✓, 07-28 ✓; sources 20260725 ✓ / 20260727 ✓ → **07-28 recovered automatically**). DS51 same-day
partition calendar (delivery began ~07-06): **ABSENT** 07-13/15/17/19/25/27; **PRESENT** 07-06→12, 14, 16,
18, 20→24, 26, 28. All correct/expected for the intermittent Bombora feed.

**Downstream symptom — `enriched_impressions` DS51=0 for 07-27. RESOLVED + PROVEN (verified end-to-end + time-travel
2026-07-29): a `cost_impression_log` CAMPAIGN-ID RESOLUTION regression — the real impressions were re-stamped
`campaign_id = -3` (the unresolved-campaign sentinel), NOT dropped.** Jordan Piepkow (Staff SWE) flagged
`enriched_impressions` DS51 for `dt=2026-07-27` reading ~110,798 one day and **0** the next. Proof chain (6+ verified
queries) for the 6 Bombora campaigns (CG 131563 / adv 30506 "MNTN - No ENG Testing"; campaigns 648318-648323):
1. **Real, billed:** spend_log = **110,792** wins on 07-27, **$903.83 billed, 100% production (test=0), 100% rendered**,
   partner_id=8 (Beeswax); win_logs = **110,862**.
2. **Present in CIL, mis-stamped:** CIL 07-27 for these campaigns = **0 under 648318-648323**, but **110,750 under
   `campaign_id = -3`**. NOT missing rows — the rows are there, the campaign attribution is wrong.
3. **`-3` = the Bombora bucket (the swap):** `-3` is **0** on every day the campaigns resolve (07-24/25/26/29 =
   17.5k/104k/108k/146k resolved, 0 as -3) and **spikes to exactly 110,750** on 07-27 (0 resolved). 07-28 is a PARTIAL
   hit: 141,002 resolved + 102,456 as -3 = 243,458 ≈ spend_log 243,961 (this also explains the 07-28 spend_log>CIL gap).
4. **Regression PROVEN via CIL physical time-travel:** 47h ago 07-27 Bombora = **109,530 correctly attributed, 0 as -3**;
   now = **0 attributed, 110,750 as -3**. A CIL reprocess of the 07-27 partition (between ~47h and ~26h ago) re-stamped
   resolved campaign_ids → `-3`. That IS Jordan's "110,798 yesterday → 0 today".
5. **Cascade:** `enriched_impressions` resolves `data_source_id` via campaign → `v_campaign_group_segment_history`;
   `campaign_id=-3` matches no campaign/segment → no DS51 tag → enriched DS51 = 0.

**Route to the CIL SQLMesh-model owner (BER/data-platform):** the campaign-id resolution step re-stamps resolved
impressions to `-3` on reprocess (regression), for very new campaigns (created 07-24). Tracked as IMP-012.

**⚠ RETRACTED (ALL prior framings were wrong, in order):** (1) "DS51 ipdsc skip → 0" — dead (07-25 was also a skip,
served 104K). (2) "serving gap / bidder-side" — dead (won 110,792, $904 billed). (3) "CIL dropped/lost the impressions"
— dead (they're IN CIL as `-3`, not missing). The true bug is campaign_id RESOLUTION. **Method lesson (the big one):
for a DERIVED table's anomalous 0, (a) check the SOURCE OF TRUTH (spend_log) before theorizing why 0 is "correct", and
(b) check WHERE the data actually is (group by the id, time-travel the partition) before saying it was "dropped".** I
spent four rounds rationalizing why 0 was right, then a fifth claiming "data loss" — the rows were present all along
under `-3`. Confirm the number is real, then find the rows, before naming the mechanism.

**⚠ Process lesson (the real takeaway — a reasoning trap, logged so we don't repeat it):** the original
GCS-evidenced call (0 correct, benign skip) was right. When Jordan raised a smart architectural objection
(*"IPDSC = targetable IPs, 35-day lookback should preserve DS51"*) hedged with *"who knows,"* I treated a
plausible-but-unconfirmed hypothesis as a refutation and **fully flipped** to a "suspected build bug / not
benign" reframe — abandoning a well-evidenced conclusion instead of holding it and running the discriminating
test. Correct move: **acknowledge the objection, keep the evidenced verdict, and settle it with the test** —
not concede. A domain owner's plausible pushback is a hypothesis to check, not an authority to fold to.

**Update 2026-07-30 (owner handoff + backfill query; recorded as spike AUDI-1181, Done).** Sonali (CIL / category-facts owner, BER) is manually
backfilling the 07-27 partition; her working hypothesis is the category-facts job's **2-day lookback** missed
07-27 (compatible with the regression: a reprocess un-resolved the rows and a 2-day window won't self-heal them).
Shared the isolation query + example rows. **Key for the backfill:** the `-3` re-stamp blanks `campaign_id`,
`group_id`, AND `creative_id` together, so `-3` rows are identifiable ONLY by `advertiser_id` (Bombora = adv 30506,
which normally sits at **0** unresolved) — not by campaign or campaign-group:
```sql
SELECT impression_id, time, advertiser_id, campaign_id, group_id, creative_id, ip, partner_id, media_cost
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE time >= TIMESTAMP('2026-07-27') AND time < TIMESTAMP('2026-07-28')
  AND advertiser_id = 30506 AND campaign_id = -3;   -- 110,750 rows
```
**07-28 also needs backfilling** (still 102,456 under `-3` alongside 141,002 resolved). Spend-breakdown columns are
NULL on `-3` rows (`media_cost` populated). Re-confirmed the daily swap holds stable 2026-07-30
(07-24/25/26 = 0 `-3`; 07-27 = 110,750 `-3`/0 resolved; 07-28 = 102,456 `-3`/141,002 resolved; 07-29 = 0 `-3`).

**How we PROVE the `-3` rows are Bombora (row-level, since the advertiser filter alone isn't proof — Sonali's Q):**
join `CIL.impression_id = spend_log.auction_id` (the `<micros>.<rand>.<n>.steelhouse` id; **NOT** `spend_log.impression_id`,
which is a separate UUID → 0 matches). Result: **110,735 of 110,750 `-3` rows matched spend_log, 110,732 carried
CG 131563 / campaign 648323 (Bombora)**. spend_log (CIL's input) had the correct campaign, so the resolution break is
inside the CIL build, not the input.
```sql
SELECT s.campaign_group_id, s.campaign_id, COUNT(*) AS n
FROM `dw-main-silver.logdata.cost_impression_log` c
JOIN `dw-main-silver.logdata.spend_log` s ON s.auction_id = c.impression_id
WHERE c.time >= TIMESTAMP('2026-07-27') AND c.time < TIMESTAMP('2026-07-28')
  AND c.advertiser_id = 30506 AND c.campaign_id = -3
  AND s.auction_timestamp >= TIMESTAMP('2026-07-26') AND s.auction_timestamp < TIMESTAMP('2026-07-29')
GROUP BY 1,2 ORDER BY n DESC;   -- 110,732 -> CG 131563 / campaign 648323
```

---

### INC-002 — `fangorn_inference_pipeline_run` `inference_pipeline` — Dataproc cluster-create failure
**Date:** 2026-07-27 · **Alert:** PagerDuty page, `fangorn_inference_pipeline_run/inference_pipeline`,
run `scheduled__2026-07-26T18:00:00+00:00`, `try_number=2` (final retry, `max_tries=1` → exhausted → paged).
**Error (log tail):**
```
RuntimeError: Job failed with:
code: 9
message: "The DAG failed because some tasks failed. The failed tasks are: [create-dataproc-cluster].;
Job (project_id = mntn-targeting-prj-prod, job_id = 951702149350293504) is failed due to the above error."
```
Vertex AI pipeline `fangorn_inference_dataproc_pipeline` (template
`gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_inference_dataproc_pipeline.json`,
project `mntn-targeting-prj-prod`, region `us-central1`).

**STATUS: RESOLVED — owner root-caused + fixed (Brian McAdams, 2026-07-28, #alerts-tpa-pipeline).**

**Verdict: RESOURCE CONTENTION (Dataproc saturation) — NOT stockout / quota / config.** The Fangorn
inference pipeline — and any Fangorn-like inference pipeline — caps out MNTN's Dataproc usage at **~94%**.
So if ANY other Dataproc job is running concurrently (even a **QA / challenger** run), `create-dataproc-cluster`
can't get capacity to provision and fails with gRPC `code: 9` (FAILED_PRECONDITION). The blocker is another
job holding the compute — not a regional stockout and not a template regression (the pipeline submitted
cleanly: template resolved, params rendered, run URL emitted). Brian caused this run's failure (a challenger
was running) and fixed it by **letting the challenger finish, then manually re-triggering the champion**.

**Action next time (decision tree):**
1. **Do NOT blind-re-run.** First check whether another Dataproc job is running (a challenger pipeline, a QA
   job, another Fangorn-like inference run) in project `mntn-targeting-prj-prod` / region `us-central1`.
2. **Another job running** → **wait for it to finish**, THEN manually re-trigger the champion
   `inference_pipeline`. Re-running while the other job holds Dataproc just re-fails with code 9.
3. **Nothing else running yet it still fails** → inspect the Dataproc job via the Vertex Run URL from the
   log (`console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/fangorn-inference-dataproc-pipeline-<ts>`),
   drill into `create-dataproc-cluster` for the real GCP error (now genuine quota/stockout/config is in play).
4. **Recurring collisions** (champion + challenger routinely overlap) → durable fix is scheduling/quota
   (stagger runs, raise the Dataproc ceiling, or a concurrency guard), owned by the Fangorn/ML + infra team
   (template lives in `targeting-infra`, not `airflow-ti`). Spawn a ticket; do NOT hot-patch.

**Durable-fix note:** the ~94% Dataproc ceiling makes champion/challenger collisions a standing risk. If
this pages repeatedly, the durable fix is run-staggering or a higher Dataproc quota rather than
hand-re-triggering each time. Tracked as **IMP-002** in `improvements_backlog.md`.

---

### INC-003 — `fangorn_inference_pipeline_run` `daily_drift_pipeline` — Vertex param `reference_date` not in template
**Date:** 2026-07-28 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [fangorn_inference_pipeline_run/daily_drift_pipeline] at 2026-07-27 11:00 PT`, run `scheduled__2026-07-27T18:00:00+00:00`, `try_number=2` (`max_tries=1` → exhausted → PagerDuty).
**Error (log tail):**
```
ValueError: The pipeline parameter reference_date is not found in the pipeline job input definitions.
  .../google/cloud/aiplatform/utils/pipeline_utils.py, line 241 in _get_vertex_value
  .../include/vertex/operators.py, line 145 in _run_pipeline
```
Vertex pipeline `fangorn_daily_feature_drift_pipeline` (template
`gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_daily_feature_drift_pipeline.json`,
project `mntn-targeting-prj-prod`, region `us-central1`).

**STATUS: RESOLVED — owner redeployed the template; re-run went green (2026-07-28).** Two-part fix, both
landed: (1) **PR #1158** (airflow-ti) dropped the DAG's redundant `run_date`; (2) **Brian McAdams
redeployed the Vertex template** — renamed the KFP param `run_date`→`reference_date` in `targeting-infra-ml`,
recompiled, redeployed `fangorn_daily_feature_drift_pipeline.json` (GCS object updated 23:31Z; verified it now
declares `reference_date`, no `run_date`). A plain **Clear Task Instance** on the same v6 run then went green
(Try #5, `Pipeline completed with state: 4` = SUCCEEDED). No new DAG version was needed — the template is read
live from GCS at task runtime, so the redeploy applied without a bundle change.

**Key lesson — the DAG fix alone was a NO-OP; the empirical re-run caught it.** The param mismatch is
invisible at DAG-parse, so PR #1158 looked like the fix but the re-run on the fixed bundle re-failed
identically. Only checking the compiled template's declared params (diagnosis command 2) revealed the real
half. **When a Vertex `parameter … not found` fix is proposed, verify the *template* param list changed —
don't trust a DAG-side PR alone.**

**Logs:** `on-call/incidents/INC-003/` (try2/try3 failed = `reference_date` mismatch; try5 succeeded).

**Re-run proof (2026-07-28 23:16Z, attempt 3):** re-ran WITH "Run with latest bundled version" — the bundle
loaded was `2026-07-28T21:55:33Z`, i.e. AFTER PR #1158 merged (21:54:58Z), so the DAG fix WAS active — and
it failed with the **identical** `ValueError: … parameter reference_date … not found`. This proves the
DAG-side change is insufficient (see mechanism below). Do NOT keep re-running; every retry reproduces it
until the drift template is redeployed.

**The actual fix (owner = Brian, `targeting-infra-ml`):** the top-level KFP pipeline
`fangorn_daily_feature_drift_pipeline(...)` declares its date param as **`run_date: str = "2026-07-25"`**
(`vertex/fangorn/pipelines/fangorn_daily_feature_drift_pipeline.py:393`, threaded to
`submit_daily_drift_job`'s `run_date`). The airflow-ti operator ALWAYS injects `reference_date`. Rename the
pipeline param `run_date` → `reference_date`, recompile, and redeploy
`gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_daily_feature_drift_pipeline.json` — then the
operator-injected `reference_date` is accepted and PR #1158 (dropping the DAG's redundant `run_date`) is
correct/complete. PR #1158 alone is a **no-op** for the failing param. (The drift task was NEW —
`43f11915 "Add daily roll-up"`, 2026-07-27 — so it was broken from inception, never green.)

**Verdict: DAG BUG (param-contract mismatch) — NOT resource contention.** Same DAG as INC-002 but a
different task (`daily_drift_pipeline`, the last task in the chain) and an unrelated cause.
`TiVertexPipelineOperator._run_pipeline` (`include/vertex/operators.py`) ALWAYS builds
`parameter_values = {google_cloud_project, google_cloud_region, bucket_name, branch, reference_date, **additional_params}`,
so it injects `reference_date` for every pipeline it submits. `PipelineJob.__init__` validates each key
against the compiled template's `inputDefinitions` (`_get_vertex_value`); the drift template doesn't
declare `reference_date` → hard ValueError before submission. The task ALSO passed a redundant
`parameter_values={"run_date": run_date}` (same value as `reference_date`) — the "extra passed data"
PR #1158 removes.

**Empirical ground truth (confirmed 2026-07-28):** the deployed drift template declares params
`[branch, bucket_name, google_cloud_project, google_cloud_region, run_date, service_account]` — it uses
**`run_date`, NOT `reference_date`.** It is the odd one out: the DAG's other Fangorn pipelines
(`inference_pipeline` → `fangorn_inference_dataproc_pipeline`, `challenger_inference_pipeline`) also pass
`reference_date` and succeed, so their templates declare `reference_date`. The proper fix therefore
standardizes the drift template on `reference_date` (a **targeting-infra** recompile) AND drops the DAG's
`run_date` workaround (PR #1158, `airflow-ti`).
**⚠ DAG-only bundle update is CONFIRMED insufficient** (attempt 3 on the fixed bundle failed identically —
see STATUS above). The compiled template's param name comes straight from the KFP pipeline source:
the deployed JSON declares `run_date` because `fangorn_daily_feature_drift_pipeline.py:393` declares
`run_date: str`. Until that source is renamed to `reference_date` and the template recompiled/redeployed
(owner-side, `targeting-infra-ml`), the operator-injected `reference_date` is rejected every run.

**Diagnosis run (copy-paste for next time):**
```bash
# 1. Which param does the failing task send that the template rejects? (log tail)
#    -> "ValueError: The pipeline parameter <X> is not found in the pipeline job input definitions."  (here X=reference_date)
# 2. Ground truth: what params does the compiled Vertex template actually declare?
gcloud storage cat "gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_daily_feature_drift_pipeline.json" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(sorted(d['root']['inputDefinitions']['parameters'].keys()))"
#    -> ['branch','bucket_name','google_cloud_project','google_cloud_region','run_date','service_account']  (no reference_date)
# 3. What does the operator send? include/vertex/operators.py _run_pipeline always adds "reference_date": reference_date  + **additional_params
```

**Decision tree next time (Vertex `ValueError: parameter <X> not found in input definitions`):**
1. This is a DAG/template param-contract mismatch, NOT infra — do NOT blind-re-run against the same
   bundle+template (it reproduces the exact error).
2. Diff operator-sent params vs template-declared params (commands above): the submitter is sending a key
   the compiled template doesn't declare.
3. Fix is owner-side (align the DAG param, or recompile the template). Route to the Fangorn/ML owner —
   template lives in **targeting-infra**, the DAG in **airflow-ti**. Do NOT hot-patch prod.
4. After the owner's fix is merged AND the bundle + template version propagate (~30 min): clear+re-run WITH
   "Run with latest bundled version" checked, or let the next scheduled run pick it up. Verify green.

**Severity: LOW.** `daily_drift_pipeline` is Fangorn feature-DRIFT monitoring, downstream of
`inference_pipeline`/`challenger_inference_pipeline` (`trigger_rule="all_done"`, last in the chain); it does
not score or serve. Impact = a one-day gap in drift telemetry, no scoring/serving impact.

**Durable-fix note:** the operator silently injects `reference_date` into every pipeline it submits, so any
template whose date-param name drifts from the operator convention fails only at task-exec (runtime), not at
DAG-parse. Tracked as **IMP-003** in `improvements_backlog.md` (standardize Fangorn template param names on
`reference_date`, or validate the operator↔template param contract earlier).

---

### INC-004 — `audience_intent` `fangorn_score_monitor` — Dataproc batch AnalysisException, missing `ipdsc_geo/dt=<run_date>` (LATE DATA)
**Date:** 2026-07-29 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [audience_intent/fangorn_score_monitor] at 2026-07-27 17:08 PT`, run `scheduled__2026-07-28T00:08:00+00:00`, try 3/3 (`max_tries=2` → exhausted → PagerDuty).
**Error — the Airflow task log is boilerplate** (`AirflowException: Batch job … Google Cloud Dataproc Agent reports job failure`); the real Spark traceback is only in the **Dataproc Serverless batch driver output**:
```
File ".../fangorn_score_monitor.py", line 216, in model
    self.spark.read.parquet(self._ipdsc_geo_path(run_date))
pyspark.errors.exceptions.captured.AnalysisException: [PATH_NOT_FOUND] Path does not exist: gs://mntn-data-archive-prod/ipdsc_geo/dt=2026-07-28.
```

**STATUS: RESOLVED — late data; clear+re-run the monitor.** The monitor reads `ipdsc_geo/dt=<run_date>`;
that partition was absent when the batch ran (last retry 2026-07-29 07:43Z) and landed **complete 34 min
later at 08:17:34Z** (18 part-files ~5.2GB + `_SUCCESS`, same shape as 07-27). Action: **Clear Task
Instance on `fangorn_score_monitor`** → it re-reads the now-present partition and passes. Each clear mints a
fresh Dataproc batch_id (timestamped) so there's **no batch-id trap**. No producer re-run and no code change
— the producer completed, just late.

**Confirmed green (2026-07-29):** cleared+re-ran (try 4), batch SUCCEEDED 11:38:11Z, output
`fangorn_score_monitor/dt=2026-07-28/_SUCCESS` written 11:37:15Z. **Runtime ~66 min against a 90-min TTL
(`ttl: 5400s`) — only ~24 min headroom** (reads the 20K-file `prospecting_intent` partition + the ~5.2GB
`ipdsc_geo` join). A heavier-data day could hit the TTL and hard-fail. Tracked as IMP-005.

**Root cause = a producer/consumer RACE, not a bug.** `ipdsc_geo/dt=D` (tpa_export `run_geo`,
`gs://mntn-data-archive-prod/ipdsc_geo/`) lands on D+1 with a **~3.5h-variable arrival**: 07-25→04:56Z,
07-26→06:26Z, 07-27→05:00Z, **07-28→08:17Z (late)**. `audience_intent` (`8 0 * * *`) runs
`fangorn_score_monitor` with only `retries=2 × 10-min` (~30-40 min of slack) and **no cross-DAG sensor** on
`ipdsc_geo`, so when the producer slips past ~07:45Z the monitor exhausts retries and pages. Same
tpa_export/ipdsc chain as INC-001. Monitor code + DAG both unchanged (07-02 / 07-24) — not a regression.

**Upstream cause corroborated (mission control, 2026-07-29):** Zach reported the **DS data flow was behind
for many data sources** last night (self-correcting the next day); Scotty separately reported a data-pipeline
delay that left mission-control system-signals un-updated for the prior day. That systemic DS-flow delay is
why `ipdsc_geo/dt=2026-07-28` (built at the tail of the ipdsc chain) landed ~3h late. So the "why was it
late" is a system-wide DS-flow lag, not an isolated builder bug. (Same-night `aud22` Geo Includes/Excludes
audit: **RULED OUT as related to this DS-flow lag / INC-004** — Compass confirmed aud22 reads CIL +
`geo.network_locations` + audience config from BigQuery, never `ipdsc_geo`/the DS chain, and is
geo_version-pinned. Its 07-28 fires are low-volume (6 CGs, 11 IPs, 12 imps, $0.12) but **are a real geo
data-sync artifact, not noise** — traced to **AUDI-1072, which is still OPEN**: `location_data` has rows
where `metro_id` and the `hierarchy` chain disagree on the DMA (e.g. ZIP 43221 → hierarchy 638/Toledo vs
metro_id 535/Columbus); served vs targeted geo diverge → audit fires. Fix = PR SteelHouse/sqlmesh#1147
("keep location_data.metro_id and hierarchy in sync", a GENERAL type-6/7 COALESCE) is **still an OPEN DRAFT,
never deployed** — blocked on forward-only backfill + a plan-permission error (DevOps DEV-8264). AUDI-1072
was marked Done only because DM suppressed the audit (ignore template-55, filter World Cup), not because the
root cause shipped. Not per-CGID; deploy #1147 forward-only once DEV-8264 clears. Full detail + diagnostic +
open network_locations caveat in `knowledge/data_catalog.md` (geo location mapping discrepancy note).)

**Compass RCA (2026-07-29) — deeper findings (evidence: Cloud Audit Logs + Spark logs + git):**
- **Late START, not slow compute.** `run_geo` CreateBatch was accepted at **08:07:23Z**; the geo Spark job
  then ran ~10 min (`_SUCCESS` 08:17:34Z). The miss is 100% "run_geo launched 3h late," not slow geo compute.
  The delay is upstream of run_geo's launch (a builder/gate finishing late), not run_geo itself.
- **Chronically flaky producer, NOT a new regression.** `ipdsc_geo` launch crept from a tight ~03:45Z band
  (mid-June) to ~04:45–06:53Z (late July), with historical outliers of **+14h (06-23→24) and +16h (06-28→29)**
  that dwarf 07-28's +3h. **07-28 was the first time this recurring lateness collided with an unguarded
  consumer deadline** (the then-new `fangorn_score_monitor`). Design for multi-hour, even mid-day, delays.
- **Leading (unconfirmed) suspect: `DS9` was added to `tpa_ipdsc_export` the day before** (commits 07-27
  "Add DS 9" / 07-28 "Fix DS number and add DS 9", Alyson) — one more `ipdsc_ds_*` builder upstream of
  `run_geo`. **Open gap:** the specific long-pole builder couldn't be pinned from audit logs (batch-ID naming
  mismatch); to close it, pull the `ipdsc_ds_*` task-instance start/end times from the **Astronomer/Airflow
  metadata DB**, not GCP audit logs.
- **Consumer scope confirmed complete.** Only two `ipdsc_geo` consumers exist: `tpa_mntn_id_export`
  (triggered synchronously downstream inside the producer DAG → no race) and `fangorn_score_monitor` (the one
  that raced). PR #1160's sensor covers the only racing consumer. 18h sensor timeout > worst-ever +16h slip.
- **Separate anomaly to route (not causal here):** run_geo's Spark workers hit `Failed to connect to master …
  Connection refused` + `SIGNAL TERM` at 08:20:17–18Z — AFTER `_SUCCESS` (08:17:34Z), so it reads as normal
  decommission of a finished batch. Flag to whoever owns the Dataproc Serverless subnet/SA config.
- **Producer-side fix recommended (Compass):** the consumer sensor stops the page but doesn't touch the
  producer's recurring tail latency. Add wall-clock alerting (repo pattern `dag_run_duration_watchdog`) on
  `tpa_ipdsc_export` so a 3h+ slip pages **TPA_EXPORT** proactively. Tracked as **IMP-006**.

**Diagnosis path (copy-paste — the Airflow log is NOT enough):**
```bash
# 1. Airflow log only says "Dataproc Agent reports job failure" — get the REAL error from the batch driver output:
gcloud dataproc batches wait '<batch-id>' --region us-central1 --project mntn-prj-prod-00 2>&1 | tail -80
#    (needs storage.objects.get on the mntn-prj-prod-00 dataproc-staging bucket — request PAM 'audi-storage-object-view' if 403)
# 2. The traceback names the missing path. Does it exist NOW, and when did it land?
gcloud storage ls -l "gs://mntn-data-archive-prod/ipdsc_geo/dt=<run_date>/_SUCCESS"
# 3. Present + landed AFTER the task's last try = LATE DATA -> clear+re-run the consumer task.
```

**Decision tree — `ModelPysparkBatchOperator` "Dataproc Agent reports job failure":**
1. The Airflow log is boilerplate — always pull the batch **driver output** (command 1) for the Spark traceback.
2. `AnalysisException [PATH_NOT_FOUND]` → an input partition was missing at run time. Check if it exists now (command 2):
   - **Present, landed AFTER the last try** → **late_data → clear+re-run the task** (this case).
   - **Still absent** → real upstream failure → re-run the producer (`ipdsc_geo` = tpa_export `run_geo`; mind the INC-001 batch-id trap), or route to the feed owner.
3. Other Spark errors (OOM / skew / schema) → the driver output shows it; route to the owning team.

**Severity: LOW.** `fangorn_score_monitor` is a reporting leaf (emails/Slacks Fangorn intent-band counts,
writes a monitoring parquet). Its downstream siblings (`household_score_distribution`, `intent_score_map`,
`trigger_intent_score_household`) all went green in the same run → no scoring/serving impact, just a missed
monitoring email for 07-28.

**Durable fix → IMP-004 → PR #1160 (MERGED 2026-07-29).** Adds a `GCSObjectExistenceSensor`
`wait_for_ipdsc_geo` on `ipdsc_geo/dt={{ ds }}/_SUCCESS` gating `fangorn_score_monitor` (mirrors the DAG's
existing `wait_for_ipdsc_13/19` preconditions; `soft_fail` so a truly-absent day skips instead of paging).
Wired `scoring() >> wait_for_ipdsc_geo >> fangorn_score_monitor` so only the monitor waits on geo, not the
scoring path. Suggestion PR only (not merged) — Ryan owns the DAG.

**Logs:** `on-call/incidents/INC-004/`.

---

### INC-005 — `tpa_mntn_id_export` `tpa_mntn_id_export` — Dataproc batch cancelled at 3h TTL (uncached recomputation + 29TB shuffle spill; verdict corrected below)
**Date:** 2026-07-29 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [tpa_mntn_id_export/tpa_mntn_id_export] at 2026-07-29 01:19:35 PT`, run `manual__2026-07-29T08:19:35+00:00`, try 3/3 (`max_tries=2` → exhausted). Slack (#alerts-tpa-pipeline): Brian McAdams "Looks like it hit TTL", pinged Sean Yang.
**Error — the Airflow log is boilerplate** (`AirflowException: Batch job tpa-mntn-id-20260729-3 was cancelled`); the TTL reason is only in the **batch state history**:
```
stateHistory: PENDING 14:40:09Z → RUNNING 14:41:42Z → CANCELLING "Cancelling batch as ttl exceeded" 17:40:14Z   (ttl=10800s = 3h)
driveroutput tail:  95× FetchFailedException (shuffleId=45, stage 829), 33 Lost task, 58 stage Resubmitted,
                    Caused by TimeoutException: Waited 30000 ms for SettableFuture ... doSparkAuth / AuthClientBootstrap
```

**STATUS: RESOLVED — durable fix merged by the owner (Nivas Nalla), 2026-07-29.** Root cause VERIFIED from the Spark event log (see correction); PR #1161 (cache `mntn_df` + `shuffle.partitions` 1000→8000 + TTL margin, collapse crossJoins) merged to `airflow-ti` main (merge commit `2cb041f48`, 21:12Z). Two small follow-ups remain, neither on-call: (1) effectiveness is confirmed on the **next scheduled run** (model read live from `ti_resources_v2/main`, so the merge applies without a bundle bump); (2) the **07-28 export is still missing** (optional force re-run to backfill; next scheduled run covers 07-29 onward). Nivas may still re-home the job to Identity, which would supersede the fix.

**⚠ ROOT CAUSE — verified from the try-3 Spark event log (2026-07-29 deep-dive). This CORRECTS the first-pass transient_infra verdict below.** Downloaded + profiled the full event log (`app-20260729144156809-0952.zstd`, 326K events, 2h56m of the 3h captured; profiler + output in `incidents/INC-005/`). Real verdict: **DAG_BUG (Spark perf). The final `.write.json()` recomputes a ~1.9TB `mntn_id`-keyed shuffle 7-9 times because the `mntn_df` lineage is never cached, and `spark.sql.shuffle.partitions=1000` makes each partition ~1.9GB so it spills to disk.** Not infra, not data volume, not the auth-handshake storm.
- **150 executors up the entire run, 0 removed.** No executor loss. Rules out infra / INC-004's subnet anomaly as the cause.
- **The 7-9 dominant stages ALL share one call site (`json at NativeMethodAccessorImpl.java:0`)**, each ~1000 tasks / ~1900GB shuffle-read + ~1900GB shuffle-write. That is the SAME write recomputed, not distinct work. Per-job stage counts climb 17→19→21→23→25 across the tail jobs (the uncached-lineage recomputation signature). The last write attempts took 15, 19, 34, 34 min; killed mid-write (job 146 INCOMPLETE).
- **Spill, not memory or CPU.** Big stages are 70-97% shuffle fetch-wait, 8-10% CPU, ~0% GC. **App-wide: 17.3TB shuffle write, 29.0TB memory spill, 13.8TB disk spill** for a job whose output is a few GB. peakExecMem 7.9GB/task vs 24GB executors → no OOM, just spill.
- **FetchFailed = 33 tasks** (NOT the 95 I first cited — that was grep-of-stacktrace-lines; and the "re-ran 58×" was resubmit log-lines). FetchFailed is an aggravator, not the root: each one resubmits a ~1900GB map stage, feeding the recompute spiral.
- **Why 78 min some days, >3h others on identical input:** the recompute spiral is kicked off by timing-dependent fetch failures, so the design sits right on the cliff.

**Real fix (owner-side; supersedes "just raise the TTL"):** (1) **cache/persist `mntn_df`** (e.g. `DISK_ONLY`) before the `_apply_audience_filter` loop + write so the ~1.9TB shuffle computes ONCE, not 7-9×. Biggest win. (2) **raise `spark.sql.shuffle.partitions` 1000 → ~6000-8000** (~256MB/partition, kills the multi-TB spill). (3) **collapse the 14 sequential `crossJoin(F.broadcast(...))` in `_apply_audience_filter` into one broadcast intersect** (one allowlist map, one select) so the plan isn't 14 stacked exchanges. THEN a modest TTL bump as margin. A TTL bump ALONE just lets the 29TB-spill spiral run longer. Owner-side model-decorator + code change, do NOT hot-patch. → IMP-007.

**Event-log deep-dive method (reusable):** the batch describe/driver-output only gets you the symptom. For the real per-stage profile, download the Spark event log and parse it:
```bash
# 1. find the event log (zstd, .inprogress if the batch was killed):
gcloud dataproc batches describe <batch-id> --region us-central1 --project mntn-prj-prod-00 --format="value(uuid)"
gcloud storage ls -l "gs://dataproc-temp-us-central1-<PROJNUM>-<suffix>/<uuid>/spark-job-history/"
# 2. if gcloud-crc32c is Gatekeeper-blocked on macOS, download via the storage API instead (click "Done" on the popup, NOT "Move to Trash"):
curl -sS --fail -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://storage.googleapis.com/storage/v1/b/<bucket>/o/<url-encoded-object>?alt=media" -o eventlog.zstd
zstd -d eventlog.zstd -o eventlog.json
# 3. profile it (per-stage runtime/shuffle/spill/skew/FetchFailed/executor churn):
python3 incidents/INC-005/eventlog_profiler.py eventlog.json
```

**[SUPERSEDED first-pass read] Verdict: TRANSIENT_INFRA (Dataproc Serverless shuffle-fetch instability) aggravated by ZERO TTL headroom — NOT data volume, NOT config, NOT concurrent-job contention.** The batch ran the full 3h and Dataproc killed it at the TTL. Root of the slowness = a **shuffle FetchFailed retry storm**: executors could not complete the RPC **auth handshake** to fetch each other's shuffle blocks within its 30s timeout (`doSparkAuth` → `SettableFuture` TimeoutException), so Spark marked map output lost and **re-ran map stages 58×** → runtime blew past 3h. Same-day sibling of INC-004, same `targeting` prod Dataproc Serverless subnet (`mntn-prod-prj-snet-central1`) whose Compass RCA already flagged a "Failed to connect to master / Connection refused" networking anomaly — this is the same class of network/RPC instability, and it hurts far more here because this job does a ~54TB all-to-all shuffle.

**What was ruled out (the discriminators that matter):**
- **Data volume — RULED OUT.** Input partitions ~identical 07-27 (last good) vs 07-28 (failed): `ipdsc_geo` 4.94GB vs 4.93GB; `ipdsc/…ds=4` 11.21GB vs 11.23GB; `ds=19` 1.91GB vs 1.88GB. Runtime **doubled on identical input** (78 min on 07-28 → >180 min ×3 on 07-29).
- **Concurrent-job contention (INC-002 shape) — RULED OUT.** The batch provisioned fine (RUNNING 1.5 min after create); the surrounding window is healthy small-job churn (`site_network_hourly`, `aug_log_rollup`, … all SUCCEEDED in 2-6 min). No big concurrent job hogging Dataproc.
- **OOM / executor loss — RULED OUT.** 0 `OutOfMemory`, 0 `ExecutorLost`/decommission in the driver output. The failures are purely shuffle-fetch RPC-auth timeouts.
- **Code change / "Sean edited it mid-incident" — WEAKENED.** The model's 16:21Z mtime is a **full `ti_resources_v2/main` redeploy** (every model file stamped 16:21:55–58Z = routine CI sync on a main merge), not a targeted edit. All 3 tries ran the pre-16:21 bundle. (The main branch DID get a merge at 16:21Z; whether it touched this model needs the source repo git history — the current GCS copy still hardcodes `timeout=10_800`.)

**The job** (`gs://mntn-data-archive-prod/ti_resources_v2/main/models/tpa_export/tpa_mntn_id_export.py`, `ModelPysparkBatchOperator`, team `targeting`): "TPA export keyed by MNTN (household) id." Reads ~15 IPDSC `ipdsc/dt=D/data_source_id=*` partitions, does a **14-way cascade of FULL OUTER JOINs on `ip`**, joins `ipdsc_geo` + the identity graph (`identity-graph-prod/mntn-graph/household_graph_parquet`, pinned max(asOfDate)→max(GraphVersion)), maps ip→mntn_id, then `groupBy(mntn_id)` collect_list of fat category arrays **+ a `Window.partitionBy(mntn_id)` best-location sort** (the `sort_addToSorter` in the FetchFailed stack), intersects with a BQ campaign-group allowlist (CG 127075, broadcast), writes JSONL to `gs://sh-dw-external-tables-prod/mntn_id_data/YYYY/MM/DD/`. Inherently shuffle-heavy. `timeout=10_800` (3h) and `spark.dynamicAllocation.maxExecutors=150` are **hardcoded in the model decorator** (lines 48-75). Idempotent: `force=false` + `_already_exported()` skips a day whose output exists; `export_date = latest ipdsc dt ≤ run_date`.

**Binding-timeout insight for the owner:** the failing timeout is the **30s auth-bootstrap** (`Waited 30000 ms for SettableFuture` in `doSparkAuth`), NOT `spark.network.timeout` (already 600s). So widening `network.timeout` won't help. The storm-reducing levers are: **lower `maxExecutors`** (150 → e.g. 60-80, fewer all-to-all auth handshakes), raise the RPC/auth + `spark.shuffle.io.maxRetries`/`retryWait`, and **raise the 3h TTL for headroom** (precedent: sibling `fangorn_prospecting_scoring` got an emergency 6h TTL bump, PR #1147, 07-24 — IMP-005). All are owner-side model-decorator changes — do NOT hot-patch.

**Diagnosis run (copy-paste — Airflow log says only "was cancelled"):**
```bash
# 1. WHY cancelled — the state history gives the TTL reason the Airflow log omits:
gcloud dataproc batches describe <batch-id> --region us-central1 --project mntn-prj-prod-00 \
  --format="yaml(state,stateHistory,runtimeInfo.approximateUsage)"     # -> "Cancelling batch as ttl exceeded"
# 2. WHY it ran long — pull the driver output, count the storm:
OUT=$(gcloud dataproc batches describe <batch-id> --region us-central1 --project mntn-prj-prod-00 --format="value(runtimeInfo.outputUri)")
gcloud storage cat "${OUT}.*" | grep -c 'FetchFailed'      # >0 with 0 OOM / 0 ExecutorLost = shuffle-fetch storm
# 3. Rule out data volume — compare input sizes vs the last GOOD day (identical => not data):
for dt in 2026-07-27 2026-07-28; do gcloud storage du -s "gs://mntn-data-archive-prod/ipdsc_geo/dt=${dt}/"; done
# 4. Rule out contention — is a big concurrent batch running the same window?
gcloud dataproc batches list --region us-central1 --project mntn-prj-prod-00 --sort-by=~createTime --limit=60 --format="table(state,createTime,stateTime,labels.job_type)"
# 5. Is the output actually missing?
gcloud storage ls "gs://sh-dw-external-tables-prod/mntn_id_data/2026/07/28/"    # 0 entries = export missing
```

**Decision tree — `ModelPysparkBatchOperator` "Batch job … was cancelled" (Dataproc Serverless):**
1. Get the **state history** (cmd 1). `Cancelling batch as ttl exceeded` = it ran the full TTL, was NOT a code/input error → go to 2. (Any other cancel reason → treat as its own error via the driver output.)
2. Pull the **driver output** (cmd 2). `FetchFailed` storm + 0 OOM + 0 ExecutorLost = **shuffle-fetch/RPC-auth instability** (this case). OOM/skew/AnalysisException instead → route that specific error.
3. Rule out data volume (cmd 3) and contention (cmd 4). Normal input + healthy churn → **transient_infra on a zero-TTL-headroom job.**
4. **Action = one controlled re-run** (env may have recovered; a fresh batch picks up the live bundle) — clear the task in Astronomer, or the owner triggers a new `tpa_ipdsc_export.trigger_mntn_id_export`. Each re-run mints a fresh timestamped batch_id → **no batch-id trap** for this task. Verify `mntn_id_data/<date>/` lands.
5. **Re-storms again** → do NOT keep burning 3h runs (~2,400 DCU-hr/try, ~$100-150/try). Route to owner for the model-decorator fix (raise TTL / lower maxExecutors / raise auth+shuffle-retry timeouts — see binding-timeout insight). Never hot-patch prod.

**Severity: MEDIUM.** Production household-keyed TPA audience export for the default campaign group (CG 127075) → `mntn_id_data/` external-table bucket (activation/serving contract). 07-28's export is missing (last good = dt=07-27, written by the 07-28 17:41Z run). Self-heals on the next successful run; downstream consumer of `mntn_id_data/` not traced (owner can confirm activation impact). Not a monitoring leaf like INC-003/004 — this is serving data — hence MEDIUM not LOW.

**Durable fix → IMP-007.** Raise TTL / cap `maxExecutors` / raise auth+shuffle-retry timeouts on the `tpa_mntn_id_export` model decorator to survive a shuffle-fetch storm on a zero-headroom job; and (cross-ref IMP-006) the same Dataproc Serverless subnet networking anomaly (INC-004 Compass §, run_geo "connection refused to master") should be routed to whoever owns the `dataproc-prod` subnet/SA config.

**Fix PR: [SteelHouse/airflow-ti#1161](https://github.com/SteelHouse/airflow-ti/pull/1161)** (2026-07-29, CI green) — 3 commits: core (persist `mntn_df` DISK_ONLY + `shuffle.partitions` 1000→8000 + TTL 10800→14400), optional (collapse the 14 crossJoins), and the regenerated `dags/model_task_config.json`. Patch + linted PR body in `incidents/INC-005/`. **CI gotcha:** the first push failed the `model-upload-dryryn` job's "check generated model artifacts" step — any `@compute.dataproc_batch` decorator change must be followed by `python model_upload.py --dryrun` and committing the regenerated `dags/model_task_config.json` (the compiled batch config the operator reads at runtime), else `git diff --quiet` on that file fails CI. **Owner routing (Sean Yang, #alerts-tpa-pipeline):** real fix/redevelop routed to **Nivas Nalla** (with Ryan Kleck); Sean asked whether the job is business-critical and, if not, to re-route its alerts off #alerts-tpa-pipeline and let Nivas take it in a sprint. So the PR is a ready stopgap Nivas can take or supersede. A re-run to unblock the missing 07-28 export is still separate (may pass since the spiral is timing-dependent).

**HANDOFF COMPLETE (2026-07-29 14:05 PT):** Malachi handed the RCA + PR #1161 to Nivas Nalla in Slack; Nivas confirmed ownership ("Ryan sent me those, I am going to change that to identity"). PR #1161 is now in Nivas's court to review/approve/merge — his call. Nivas's stated plan to "change that to identity" suggests the job may be re-homed/re-routed to the Identity team/pipeline (exact meaning TBD), which could supersede this stopgap. Nothing left on our side except an optional re-run for the 07-28 gap. **Owner = Nivas Nalla; on-call action closed.**

**⚠ Deploy nuance (validating the fix) — split between live `.py` and the bundled config.** A task clear/re-run reads the model **`.py` LIVE** from `gs://…/ti_resources_v2/main/models/…` (synced on merge, so the persist + collapsed-crossJoin logic applied immediately, GCS mtime 21:13:09Z), BUT `ttl` and the spark `runtime_properties` (incl. `spark.sql.shuffle.partitions`) come from **`model_task_config.json` baked into the Astronomer DAG bundle**, which only refreshes on an `astro deploy`. Malachi's 21:15Z try-4 re-run (`tpa-mntn-id-20260729-4`) loaded the **pre-merge 18:08:11Z bundle** → **persist active, but ttl still 10800 and partitions still 1000** (confirmed in the batch config). So a decorator-only change (ttl / shuffle.partitions) does NOT take effect on a re-run until the bundle redeploys past the merge — this applies to the scheduled runs too. **To fully validate:** wait for the Astronomer bundle to deploy post-21:12 main, confirm the deployed `model_task_config.json` shows `ttl: 14400s` + `spark.sql.shuffle.partitions: 8000`, then re-run. The try-4 run tests the persist fix alone (likely completes since persist removes the 7-9× rebuild, even at 1000 partitions under 3h).

**✅ VALIDATED IN PROD (2026-07-29 21:25Z) — try-4 SUCCEEDED in ~9.4 min** (RUNNING 21:16:30Z → SUCCEEDED 21:25:56Z), vs the 3h TTL kills. Persist alone (config half NOT yet applied) did it. Decisive before/after: **compute 334,016 DCU-s vs 8,651,422 (26× less); shuffle 23M GB-s vs 596M (26× less); FetchFailed 0 vs 33; 0 TTL cancels.** 07-28 export **produced and backfilled**: `mntn_id_data/2026/07/28/385864/` = 8.16GB JSONL + audit pointer (written last, so `_save_dataframe` completed). So the persist fix is the decisive one; the config half (8000 partitions + 14400 TTL) is now just extra margin and applies to scheduled runs once the Astronomer bundle deploys post-merge. **07-28 gap CLOSED. Only remaining item: bundle redeploy for the config margin (non-urgent, persist already gets it to 9 min).**

**Logs:** `on-call/incidents/INC-005/`.

---

### INC-006 — `keyword_ddp_reporting` `wait_for_product_categorization` — ExternalTaskSensor 6h timeout (upstream OpenAI-batch product_categorization not ready)
**Date:** 2026-07-29 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [keyword_ddp_reporting/wait_for_product_categorization] at 2026-07-28 08:00 PT`, run `scheduled__2026-07-28T15:00:00+00:00`, try 1/1. `AirflowSensorTimeout: run duration 21682.7s exceeds timeout 21600.0` (6h; 307 reschedule pokes).

**STATUS: RESOLVED 2026-07-30 — root-cause fix `shopper_graph#296` MERGED + DEPLOYED; downstream fail-fast `airflow-ti#1162` MERGED.** Verdict was CONFIRMED real_upstream_failure (Sean Yang confirmed the `batch_fetch` OpenAI-batch step failed, 2026-07-29); the GCS evidence was right — upstream never produced. The stalled dt=2026-07-27 cycle **self-healed** to `product_categorization` `_SUCCESS` @07-30 02:35Z (see INC-007 table) — no data backfill owed. The #296 fix is deployed to the `openai_batch_runner` image but **NOT yet EXERCISED** — first run with the fix = **2026-07-31 09:00 UTC**. See the 2026-07-30 deploy update at the end of this entry. **⚠ Deploy-workflow lesson (below): the OBVIOUS deploy (middleware) was the WRONG one.**

**Verdict: real_upstream_failure OR late_data (upstream not ready) — NOT a sensor misconfig.** `keyword_ddp_reporting` (`0 15 * * *`) waits via `ExternalTaskSensor` for `mntn_match_incrementals_fetch.batch_post.product_categorization` at logical `07-28T09:00` (`execution_delta=6h`, `allowed_states=["success"]`, `mode=reschedule`, `timeout=21600`). Alignment is CORRECT (upstream schedule `0 9 * * *` + 6h delta → 09:00). It poked 307× over 6h; the upstream never reached success → timed out.

**Empirical evidence (GCS — `product_categorization` is date-partitioned `dt=`):**
- Latest partition = **`dt=2026-07-26`** (written **2026-07-28T13:34Z**); expected **`dt=2026-07-27` is ABSENT** (verified `ls` → no objects). 478 daily partitions, continuous through 07-26.
- Normal cadence: `dt` lags the run ~2 days and completes ~13:00-13:34Z (dt=07-25 → 07-27T12:50Z; dt=07-26 → 07-28T13:34Z), i.e. ~4.5h after the 09:00 start.
- The 07-28-logical run (executing 07-29T09:00) should have written `dt=2026-07-27` by ~07-29T13:34Z. By the sensor timeout (07-29T21:01Z) it was **~7.5h overdue** → the upstream failed or the OpenAI batch is running abnormally long.
- **Not a chronic race:** normally the upstream finishes ~13:34Z, before keyword_ddp even starts poking (15:00Z), so the sensor usually passes instantly. This was a bad upstream cycle.

**The pipeline (DS13/DS19 MNTN Matched OpenAI keyword pipeline; TI-1058/1060):** `mntn_match_incrementals_fetch` (`0 9 * * *`, migrated by Victor Savitskiy): `batch_cleanup_1 >> batch_transition >> batch_fetch >> batch_post >> [batch_test, batch_cleanup_2]`. `batch_transition`/`batch_fetch` = `MntnKubePodOperator` "openai_batch_runner" (submit/fetch an **OpenAI Batch API** job — hours, up to 24h SLA). `batch_post` (dbt on Databricks, `DbxDbtOperator`, image SHOPPER_GRAPH): openai_batch_joined → categorization_temp → mm_taxonomy_update → **product_categorization** (+ mm_taxonomy_update_bq). Output `gs://mntn-data-archive-prod/shopper_graph/product_categorization/dt=<D>/` (+ Databricks `shopper_graph.product_categorization`, BQ `external.tpa__mntn_matched_taxonomy__v2`). Likeliest failure point = the OpenAI batch step (API error / batch expiry / quota) or a dbt model.

**Diagnosis run (copy-paste — upstream state needs Astronomer/K8s, not reachable from this box):**
```bash
# 1. Did the upstream produce for this cycle? expected dt ≈ run's logical date − ~2d
gcloud storage ls -l "gs://mntn-data-archive-prod/shopper_graph/product_categorization/" | grep "dt=" | tail -4
gcloud storage ls "gs://mntn-data-archive-prod/shopper_graph/product_categorization/dt=<expected>/"   # absent = upstream not done
# 2. Upstream run state (Astronomer UI / astro CLI if authed): mntn_match_incrementals_fetch run for logical 07-28T09:00
#    -> which task failed? batch_transition / batch_fetch (OpenAI batch pod logs) or a batch_post dbt task.
```

**Decision tree — `wait_for_product_categorization` timeout:**
1. GCS check (cmd 1). Expected `dt=` partition **absent** = upstream not done → do NOT clear the sensor (a clear just restarts a 6h wait that re-fails).
2. Upstream state (cmd 2): **FAILED** → real_upstream_failure: re-run the failed task(s) (⚠ re-running `batch_transition`/`batch_fetch` re-submits an OpenAI batch = cost + up to 24h), or route to owner; once `product_categorization` succeeds → clear `wait_for_product_categorization` → passes. **STILL RUNNING / finished late** → late_data: wait for it, then clear the sensor.
3. Recurs → durable fix: alert on the UPSTREAM failure directly (not via the downstream sensor 6h later) and/or switch keyword_ddp to asset/dataset-triggered scheduling on `product_categorization` instead of a time-boxed sensor → improvements_backlog (IMP-009).

**Severity: LOW-MEDIUM.** `keyword_ddp_reporting` is a reporting DAG (no serving path). Impact = delayed DS19 keyword DDP reporting for the 07-28 cycle; `product_categorization` a day behind (dt=07-27 missing). Self-heals once the upstream cycle completes.

**Durable fix → IMP-009** (direct alerting on the OpenAI-batch upstream failure / data-aware scheduling for keyword_ddp).

**Downstream fix PR: [SteelHouse/airflow-ti#1162](https://github.com/SteelHouse/airflow-ti/pull/1162)** — the sensor's `failed_states` was `["failed","skipped"]`, but a failed `batch_fetch` puts `product_categorization` in **`upstream_failed`** (not `failed`), which fell through → it poked the full 6h then timed out. PR adds `"upstream_failed"` so it fails in seconds. Downstream hygiene only; does NOT fix the upstream `batch_fetch` failure. **Upstream root cause: routed to Compass** (prompt saved in `incidents/INC-006/compass_rootcause_prompt.txt`) — `batch_fetch` already retries 3× and still failed, so it's persistent (likely the OpenAI batch expired/never completed); the fix lives in the `OPEN_AI_BATCH` image, not airflow-ti.

**Compass RCA + GCS verification (2026-07-29):** Compass traced the likely root cause to a real source bug; I closed the GCS gap Compass was permission-blocked on (`PERMISSION_DENIED` on `mntn-data-archive-prod` — a different project; I have read access). Full prompt + answer in `incidents/INC-006/`.
- **Root cause (Compass, moderate confidence): `SteelHouse/shopper_graph/openai/openai_wrapper/batch_fetcher.py::download_file` (L27-45) never null-checks `output_file_id`.** When an OpenAI batch completes with all rows errored, `output_file_id` is None (only `error_file_id` set) and `client.files.content(None)` throws uncaught → kills the pod deterministically on all 3 retries. Verified in source (code_read); Compass could NOT pull the live pod traceback (the Airflow deploy — Composer or a cluster outside its monitored GKE/Loki fleet — isn't observable to it).
- **GCS verification (I closed this):** dt=07-27 chain: `openai_batch_submissions` ✓ (1101 files, 22:37Z) → `openai_batch_results` ✓ (732 files, 30GB, 22:37Z) → `openai_batch_results_joined` ✗ absent → `product_categorization` ✗ absent. Healthy dt=07-26: results 37GB → results_joined 7.8GB (+13min) → product_categorization (+~2h).
- **OOM (Compass's secondary) is UNLIKELY:** results are ~42MB/file (879 files / 37GB on 07-26), far under the pod's 1Gi limit — `download_file` handles one ~42MB sub-batch at a time. The null-`output_file_id` crash is the stronger trigger.
- **Errored-batch signal (suggestive, confounded):** submissions 1101 > results 732 for dt=07-27 fits some sub-batches completing all-errored (no output file), the exact null case — but Sean's re-run overwrote the partition, so not clean proof.
- **Recovery IN PROGRESS (not done):** Sean's re-run FETCHED the 30GB results at 07-29T22:37Z (past batch_fetch) but the dbt `batch_post` (results_joined → product_categorization) hasn't completed — `product_categorization/dt=2026-07-27` still absent, so keyword_ddp's sensor still can't pass. Watcher `bv1r79c1f` still waiting.
- **Still open (needs Composer/cluster access):** the exact original pod traceback + OpenAI batch id/status. Route to ML/data-eng to pull `batch_fetch` pod logs+events for logical 2026-07-28T09:00Z, and register that Airflow deploy in Compass's monitored fleet.
- **Durable code fix → IMP-010** (`batch_fetcher.download_file` null-check + terminal-status guards + try/except; gate `fetch_results.py` `update_source_file_s3()` on real success). Real source-confirmed bug — fix regardless of which trigger fired.

**Deeper read + fix PR (2026-07-29, later):** pulled the actual `batch_fetcher.py` / `fetch_results.py` source and re-checked GCS. Refines the mechanism; still not recovered.
- **Mechanism (source-confirmed):** `fetch_results.py` loops over all submitted-not-downloaded batches. `download_file` only acts when `status=="completed"` and calls `client.files.content(output_file_id)` with NO null-check and NO per-batch try/except, so a single completed-but-errored batch (`output_file_id=None`) throws and aborts the WHOLE loop; the remaining batches in that run never download. `update_source_file_s3` (sets `was_downloaded=True`) was called unconditionally after `download_file`, so a not-completed / skipped batch got marked done and stranded. Each retry resumes and grabs a few more before hitting the next bad batch, so the cycle makes partial progress but never finishes. This unifies Compass's null-crash finding with the GCS evidence.
- **GCS now (supersedes the 732 / 22:37Z line above):** dt=2026-07-27 results = 928 objects / 35.7GB and still trickling (newest write 07-29T22:55Z, up from 732), `results_joined` + `product_categorization` STILL absent; next cycle dt=2026-07-28 has nothing (pipeline backed up). 928 < 1101 submissions means ~173 batches never produced a result, matching the loop-abort mechanism, not a clean re-run.
- **Fix PR: [SteelHouse/shopper_graph#296](https://github.com/SteelHouse/shopper_graph/pull/296)** (IMP-010). `download_file` returns bool and skips (does not crash on) a completed-null or not-completed batch; `fetch_results` marks `was_downloaded` only on a real download. Lets the loop finish and leaves skipped batches eligible next run. Minimal and safe (kept the existing loud-fail on systemic upload errors); not run on the cluster; owner (Sean / Victor) reviews and merges.
- **STATUS: MERGED + DEPLOYED 2026-07-30** (see the deploy update below). `product_categorization/dt=2026-07-27` self-healed to `_SUCCESS` @07-30 02:35Z (INC-007 table), so the stalled cycle un-stuck itself; the PR prevents recurrence and is now live on the `openai_batch_runner` image (first exercised 07-31 09:00 UTC).

**Update (2026-07-30 ~00:2xZ — sensor now on try 2; bottleneck moved to dbt):** the sensor timed out on try 1 (6h/307 pokes) and was cleared → **try 2 re-poking** (`attempt=2.log`, 277 pokes since 21:49Z, still UP_FOR_RESCHEDULE; will re-timeout ~03:49Z = cosmetic re-alert, not a new failure). GCS re-check: `openai_batch_results/dt=2026-07-27` now **1097 objects / 42.2GiB** (up from 928; newest 22:55Z) — fetch is ~complete (1097/1101). BUT `openai_batch_results_joined` + `product_categorization` for dt=2026-07-27 **STILL absent**. So the blocker moved off `batch_fetch` (now essentially done) onto the **dbt `batch_post`** step (results → results_joined → product_categorization), which hasn't produced output. Action unchanged: **do not clear** (self-passes when the partition lands); nudge owner (Sean) that fetch completed and the remaining gap is the dbt join/post. Separate pipeline from the same-morning aud22/`ipdsc_geo` geo delay (INC-004) — do not conflate.

**Update (2026-07-30 — root-cause fix DEPLOYED; the deploy-workflow lesson):**
- **Both fixes MERGED.** `shopper_graph#296` (root cause): `batch_fetcher.py` now guards a `completed`
  OpenAI batch with a null `output_file_id` (all rows errored) AND a not-yet-completed batch, so one bad
  batch no longer crashes the whole download loop; `fetch_results.py` marks a batch consumed only on a real
  download. `airflow-ti#1162` (sensor `failed_states += "upstream_failed"` → fails fast instead of poking
  the full 6h; auto-deploys via GCS→Astronomer).
- **⚠ KEY LESSON — merging is not shipping, and the OBVIOUS deploy is the WRONG one.** Brian's
  **"Deploy Middleware to DockerHub"** run (prod/main, run #117) did **NOT** ship the fix. `batch_fetch`
  runs the **`openai_batch_runner`** image (`DbtImageName.OPEN_AI_BATCH`), built ONLY by **"Deploy OpenAI
  Batch Runner to Dockerhub for GCP"** (`deploy_openai_dockerhub_gcp.yml`, build context `openai/`). The
  middleware workflow builds a **different** image (`steelhousedev/shopper-graph`, context `middleware/k8s`)
  — the API-serving app, unrelated to the batch pipeline. Always map the changed file's **build context**
  to the deploy workflow you run. Full three-image map → MEMORY `reference_shopper_graph_deploy`.
- **Deploy action taken:** dispatched `deploy_openai_dockerhub_gcp.yml` (ref main, environment=prod,
  mntn_cloud=gcp) → run **30571986734 SUCCESS 2026-07-30 18:49 UTC**. Pushed
  `steelhousedev/openai_batch_runner:gcp-prod` (digest `sha256:ad94fe9c…`) + `:gcp-prod-c6c8eda` from commit
  `c6c8eda` — verified the deployed commit contains the `output_file_id` guard.
- **STATUS = fix deployed, NOT yet exercised.** `mntn_match_incrementals_fetch` = `0 9 * * *`
  (daily 09:00 UTC, `catchup=False`); today's run predated the deploy, so **first run with the fix =
  2026-07-31 09:00 UTC** unless manually triggered. `MntnKubePodOperator` uses `image_pull_policy=Always`,
  so the rebuilt `gcp-prod` tag is picked up on the next DAG run with no Astronomer bundle redeploy.
  **Session decision: wait for tomorrow's scheduled run to validate** — do not force-trigger early.
  dt=2026-07-27 already self-healed (INC-007), so no INC-006 data backfill is owed. IMP-010 → done.

**Logs:** `on-call/incidents/INC-006/`.

---

### INC-007 — `keyword_ddp_reporting` `wait_for_product_categorization` FAST-FAIL — upstream `batch_submit` hit the OpenAI 2.5TB file-storage quota (recurrence of the INC-006 symptom, NEW root cause 3 levels up)
**Date:** 2026-07-30 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [keyword_ddp_reporting/wait_for_product_categorization] at 2026-07-29 08:00 PT`, run `scheduled__2026-07-29T15:00:00+00:00`, try 2/2. **`ExternalTaskFailedError: Some of the external tasks ['batch_post.product_categorization'] in DAG mntn_match_incrementals_fetch failed`** — fast-fail in **8.9s** (single poke), NOT a 6h timeout.

**STATUS: RESOLVED + CLOSED — cleanup fix `shopper_graph#298` DEPLOYED + VERIFIED (`batch_cleanup` green); `dt=2026-07-28` backfill COMPLETE (`product_categorization/dt=2026-07-28/_SUCCESS` + full parquet set verified in GCS 2026-07-30; the one-day hole is closed, supersedes the earlier accept-gap).** Root cause CONFIRMED from the submit-DAG pod log AND corroborated by the fetch-DAG `batch_transition` `FileNotFoundError`. Durable fix ticketed as **AUDI-1042** (In Progress, P1, Malachi). Not a defect in the pipeline logic; not the INC-006 fetch bug. ⚠ The FIRST cleanup fix `#297` REGRESSED (called a nonexistent `auto_paging_iter` → every `batch_cleanup` crashed, deleted nothing) — `#298` is the real fix. See the 2026-07-30 updates at the end of this entry.

**Verdict: real_upstream_failure (OpenAI account resource/quota exhaustion) — three levels up from the alert.** The alert is a downstream symptom. The chain, confirmed end-to-end:
```
OpenAI project ≥ 2.5TB file-storage quota
  → mntn_match_incrementals_SUBMIT.batch_submit (logical 07-28, exec 07-29 10:43Z) 400 on client.files.create ×4 tries ❌
    → NO openai_batch_submissions/dt=2026-07-28  (0 objects at EVERY stage: submissions, results, joined, categorization, submissions_errored)
      → mntn_match_incrementals_FETCH (logical 07-29) has no batch to transition/fetch → product_categorization/dt=2026-07-28 = upstream_failed
        → keyword_ddp_reporting wait_for_product_categorization fast-fails (PR #1162 working as designed) → ALERT
```

**The real error (submit-DAG pod traceback — the fetch/sensor logs never show it):**
```
File "/app/submit_batch.py", line 9, in <module>            openai.create_batch(file)
File "/app/openai_wrapper/batch_submitter.py", line 23,      batch_input_file = self.client.files.create(
File ".../openai/resources/files.py", line 122, in create
Error code: 400 - {'error': {'message': 'You have exceeded your file storage quota.
  Projects are limited to 2.5TB of files. Please delete old files or attempt with a
  smaller file size.', 'type': 'invalid_request_error'}}
```
Deterministic 400 → all 4 `batch_submit` tries failed identically (retries can't clear a quota wall).

**Two DAGs, both team ML/`airflow-ti`, both `0 9 * * *`:**
- **Producer of the submission** `mntn_match_incrementals_submit` (severity 1, retries 3): `batch_cleanup_1 >> batch_prep{product_uniques >> openai_batch_input_raw >> openai_batch_input_formatted} >> batch_validate >> **batch_submit** >> batch_cleanup_2`. `batch_submit` = `MntnKubePodOperator` (`openai_batch_runner` image) → uploads the batch input file + creates the OpenAI batch → writes `openai_batch_submissions/dt=<logical>`. **This is where it broke.**
- **Consumer** `mntn_match_incrementals_fetch` (severity 5): `batch_transition >> batch_fetch >> batch_post{… >> product_categorization}` — fetches YESTERDAY's submitted batch and categorizes it. product_categorization dt = fetch-run logical − 1.

**Empirical GCS proof (2026-07-30, verified live):**

| dt | submissions | results | joined | product_categorization | note |
|---|---|---|---|---|---|
| 07-27 | 1101 | 1101 | 118 | 53 ✓ `_SUCCESS` @07-30 02:35Z | INC-006 cycle **self-healed** |
| **07-28** | **0** | **0** | **0** | **0** (+ `submissions_errored`=0) | **never submitted — this incident** |
| 07-29 | 1073 @07-30 10:44→12:45Z | 0 | 0 | 0 | submit **succeeded** → quota already cleared; in flight |

**Quota SELF-CLEARED between 07-29 10:43Z (fail) and 07-30 10:44Z (07-29 submit start)** — old-file expiry or a cleanup ran. So continuity is restored (07-29 onward flows); only the **dt=07-28 slice is a permanent one-day hole** (its submit failed and was never re-run after the quota freed). keyword_ddp's logical-07-29 report is the only broken cycle; logical-07-30 (waits on dt=07-29, coming) self-recovers.

**Likely aggravator (plausible, not proven):** INC-006's `batch_fetcher.download_file` bug (fix shopper_graph#296) left errored/undownloaded OpenAI files undeleted (`client.files.delete` only ran after a successful download+upload); multi-day INC-006 stalls compounded the accumulation → pushed the project over 2.5TB. #296 slows the leak but doesn't purge the backlog or add headroom.

**Decision tree — `wait_for_product_categorization` fast-fail (`ExternalTaskFailedError`, ~9s):**
1. It's a downstream symptom. **Audit the GCS chain for the missing `dt`** (below) to find WHICH stage broke — do not assume it's `batch_fetch`.
   ```bash
   B=gs://mntn-data-archive-prod/shopper_graph
   for s in openai_batch_submissions openai_batch_results openai_batch_results_joined product_categorization; do \
     echo "$s: $(gcloud storage ls "$B/$s/dt=<D>/" 2>/dev/null | grep -c gs://)"; done
   ```
2. **`submissions/dt=<D>` = 0** → it failed at **SUBMIT**, not fetch → pull `mntn_match_incrementals_submit` run logical `<D>` (exec `<D>+1` 09:00Z), task `batch_submit` pod log. `client.files.create` 400 "file storage quota" = OpenAI 2.5TB quota (this incident). Do NOT rebuild the image / re-run the fetch DAG — there's no batch to fetch, #296 is irrelevant.
3. **`submissions/dt` present but `results/dt` partial** → INC-006 `batch_fetch` loop-abort (fix #296).
4. **Recovery:** quota can self-clear (old-file expiry). To fill a specific missing day, re-run `mntn_match_incrementals_submit` `batch_submit` for that logical date after storage is freed (new OpenAI batch = cost + ~24h), then the fetch, then clear the sensor. keyword_ddp is a reporting DAG (no serving) → accepting the one-day gap is usually correct; confirm `product_categorization` consumers (`tpa_export`, `audience_sizes`, `mntn_matched_taxonomy_bq`) don't need that exact day.

**Severity: LOW-MEDIUM.** Reporting DAG, no serving path. Impact = one missed DS19 keyword-DDP report cycle (dt=07-28 categorization gap). Pipeline continuity already restored.

**PRs / durable fix:**
- **NOT covered by the merged pair.** PR #1162 (fail-fast, merged 07-29 22:37Z — working here) and PR #296 (batch_fetch hardening, merged 07-30 15:12Z, 7 min AFTER this alert) address the INC-006 fetch path, not the submit quota.
- **Durable fix — the ticket already exists: [AUDI-1042](https://mntn.atlassian.net/browse/AUDI-1042) "product_categorization: OpenAI storage quota issue"** (reporter Victor Savitskiy, opened 2026-06-18, **Backlog, P3, unassigned, 0 comments** — i.e. a known problem left unprioritized). Its description IS this exact 400 quota error. **IMP-013** (OpenAI file-storage hygiene) is the fix content; routed against AUDI-1042 (now Malachi's, **In Progress + P1-Critical**, Bryce Wagg bumped 2026-07-30). **Fix PR [shopper_graph#298](https://github.com/SteelHouse/shopper_graph/pull/298) — MERGED + DEPLOYED 2026-07-30** (merge `8b23620`, now main HEAD, branch `audi-1042/hotfix-cleanup-pagination`; deployed via `deploy_openai_dockerhub_gcp.yml` run 30586147014 SUCCESS 22:11Z → `openai_batch_runner:gcp-prod` `sha256:20d1cf25…` from `8b23620`). **⚠ CORRECTION:** the FIRST fix attempt [#297](https://github.com/SteelHouse/shopper_graph/pull/297) (merge `cf2c76e`, deploy run 30577185770) **REGRESSED** — its rewrite called `client.files.list().auto_paging_iter()`, a method the OpenAI Python SDK does **not** have (Stripe-SDK idiom), so **every** `batch_cleanup` (all 4×/day across submit+fetch) crashed `AttributeError: 'SyncCursorPage[FileObject]' object has no attribute 'auto_paging_iter'` and deleted **0** files; the 30577185770 deploy shipped a BROKEN cleanup. #298 reverts to `for file in client.files.list():` (the OpenAI SDK auto-fetches all pages — the original 2-year-proven pattern). [#299](https://github.com/SteelHouse/shopper_graph/pull/299) (Malachi's manual after-cursor loop, undocumented `has_more`) CLOSED as superseded (could stop after page 1). **VERIFIED:** `batch_cleanup_2` re-ran GREEN on the #298 image (deterministic crash gone). — the cleanup (`delete_all_storage_files.py`, run 4×/day by `batch_cleanup_1/2` in both DAGs) deletes nothing: one `try/except` wraps the whole delete loop, so a single undeletable file (one still attached to an in-flight batch) aborts every delete → files ride OpenAI's **30-day auto-expiry** (evidenced: file created 07-28 → expires 08-27) to ~2.4TB, tripping the 2.5TB cap. The fix = per-file delete + iterate `client.files.list()` directly (the SDK auto-pages all ~70k files) + 72h→48h, and **age-deletes only `part-` INPUT files**: outputs (`batch_*`) are removed by the fetcher on download, so age-deleting them would only ever hit un-fetched results (Ryan Kleck's weekend-failure concern); the OpenAI project is SHARED, so other teams' `fine-tune` files (neither prefix) are never matched. Root cause is the abort, not pagination: ~10k = ~5 days would keep up if deletes succeeded. Immediate mitigation (owner-agreed): ask OpenAI for more free storage via the account owner. Daily production ~75 GiB/day, so a working 48h cleanup holds ~150 GiB. **Deploy path (done)** — the fix lives in the `openai_batch_runner` image (NOT the dbt image `mntn_matched_data_pipeline`): shipped by the manual **`deploy_openai_dockerhub_gcp.yml`** (`workflow_dispatch`, environment=prod, cloud=gcp, run 30586147014 for #298) → pushed `steelhousedev/openai_batch_runner:gcp-prod` → the next scheduled `mntn_match_incrementals_{submit,fetch}` run pulls it (`image_pull_policy=Always`, no Astronomer bundle redeploy needed, unlike the airflow-ti model-config path). shopper_graph has 3 separate deploys: `deploy_dbt_dockerhub.yml` / `deploy_middleware_dockerhub.yml` / `deploy_openai_dockerhub_gcp.yml`.

**Update 2026-07-30 (fetch corroboration + Slack + owner meeting — RESOLVED as accept-gap):**
- **Fetch-side proof of the cascade.** `mntn_match_incrementals_fetch` (logical 07-29) `batch_transition` failed **8/8** tries with `FileNotFoundError: mntn-data-archive-prod/shopper_graph/openai_batch_submissions/dt=2026-07-28` (`transition_batch.py` → `batch_transitioner.py:19 transition_to_in_progress` → `pd.read_parquet`). The fetch DAG's FIRST task dies on the missing 07-28 submission → the whole DAG `upstream_failed` → sensor fast-fail. Direct confirmation of the submit→fetch→sensor chain.
- **Slack (#alerts-tpa-pipeline):** Malachi "Upstream failed"; **Sean Yang: "can you re-run the upstream as well as this job?"**; Malachi "in meeting with Brian"; then Malachi→Alyson framed it as the >2.5TB OpenAI quota (free tier per Ryan Kleck) + "only one of these DAGs signals us it fails, so we need to update this"; **Bryce Wagg** asked FinOps to find an OpenAI contact; **Alyson Lefkowitz** → "sounds like one of Victor's tickets would fix this" (= AUDI-1042).
- **Owner meeting 2026-07-30 (Malachi + Brian McAdams + Ryan Kleck) decided ACCEPT-GAP, NO re-run** (transcript: `incidents/INC-007/inc007_01_brian_meeting_2026_07_30.txt`). Rationale, in the owners' words: batch_submit is "a pretty long run" and the job is "brittle"; **"worst case … wouldn't have keywords for a day … but when we load it up into IPDSC it goes back 30 days"** — so a one-day DS19 keyword gap is absorbed by the 30-day IPDSC lookback → mark/ignore the red run, don't re-submit. **This supersedes Sean's earlier re-run ask** (made before the owner context). Malachi to close the loop with Sean in-thread.
- **Why the cleanup stopped keeping up (Ryan):** `batch_cleanup` is *supposed* to delete old OpenAI files daily but volume outgrew it ("we must be sending a lot more than what we used to"). **Victor Savitskiy has left** ("Victor destroyed things on his way out") → the pipeline is under-owned; Ryan Kleck is the nearest owner. Nobody in the meeting had OpenAI-account purge access → hence the FinOps contact hunt.
- **Immediate mitigation agreed:** ping Alyson to find the OpenAI account owner / **ask OpenAI for more free storage** (Ryan: "they've done that before"). Buys headroom while AUDI-1042/IMP-013 hardens the cleanup.
- **Related thread:** the `batch_fetch` failures "the last couple of days" (INC-006) are fixed by **shopper_graph#296**, MERGED + DEPLOYED 2026-07-30 (`deploy_openai_dockerhub_gcp.yml` run 30571986734) — both the #296 fetch fix and the #297 cleanup fix are now live on the `openai_batch_runner:gcp-prod` image.
- **Recovery action (COMPLETE 2026-07-30):** the earlier accept-gap was **SUPERSEDED** — with the quota fix landing, the team **BACKFILLED dt=2026-07-28** end-to-end: `mntn_match_incrementals_submit` logical 07-28 re-ran → `batch_submit` SUCCEEDED and wrote `openai_batch_submissions/dt=2026-07-28`; then `mntn_match_incrementals_fetch` logical 07-29 re-ran (`batch_transition` read `dt=07-28` cleanly, the earlier `FileNotFoundError` gone) → `batch_fetch`/`batch_post` produced **`product_categorization/dt=2026-07-28/_SUCCESS` + full parquet set (verified in GCS)**. The `keyword_ddp` sensor's dependency is satisfied; the one-day DS19 hole is closed. `#296` made `batch_fetch` skip-safe for still-processing OpenAI batches during the re-run.
- **Backfill wrinkle — the `batch_test.test_product_categorization` dbt test fails SPURIOUSLY on a backdated re-run (expected, not a data problem).** The test `product_categorization__max_dt` asserts a partition exists for `date_sub(current_date, 2)` (wall-clock, UTC). A manual re-run of an OLD logical date produces `dt=yesterday-of-logical`, not `current_date-2`, so once the re-run happens >1 day late the assertion points at a date the backfill never wrote → FAIL. Here: re-run executed 07-31 01:45Z → test looked for `dt=2026-07-29` but the backfill wrote `dt=2026-07-28` → 1-row fail. The substantive checks PASSED (`record_count`, `dsc_id__length/not_null/values`); `dt=2026-07-28/_SUCCESS` + full parquet verified in GCS. **Handling:** mark `test_product_categorization` SUCCESS (do NOT re-run — it fails every time now that UTC > 07-30); `batch_post.product_categorization` is already green so `keyword_ddp` clears independently. The trailing `OSError: [Errno 99] Cannot assign requested address` is a red herring (post-failure SMTP email send from the pod, not the cause). Durable fix = key the test off the run's `yesterday`/`run_date` var, not `current_date` → **IMP-016**.

**Update 2026-07-30 (⚠ #297 regressed → #298 is the real cleanup fix):**
- **#297 shipped a BROKEN cleanup.** Its rewrite of `delete_all_storage_files.py` called `client.files.list().auto_paging_iter()` — a method the OpenAI Python SDK does **not** expose (it is a Stripe-SDK idiom). Every `batch_cleanup` task (all 4×/day across submit+fetch) crashed `AttributeError: 'SyncCursorPage[FileObject]' object has no attribute 'auto_paging_iter'` and deleted nothing; the run-30577185770 deploy made cleanup a no-op, so AUDI-1042 was NOT fixed by #297.
- **#298 (`audi-1042/hotfix-cleanup-pagination`) is the real fix.** Reverted to `for file in client.files.list():`, which the OpenAI SDK auto-fetches all pages (the original 2-year-proven pattern; kept the per-file delete). MERGED 2026-07-30 22:09Z (merge `8b23620`, now main HEAD), DEPLOYED via `deploy_openai_dockerhub_gcp.yml` run 30586147014 SUCCESS 22:11Z → `openai_batch_runner:gcp-prod` `sha256:20d1cf25…` from `8b23620`.
- **#299 (Malachi's `audi-1042/fix-cleanup-pagination`) CLOSED** as superseded — a manual after-cursor loop using the undocumented `has_more`, over-engineered and could stop after page 1. #298 is simpler and correct.
- **VERIFIED:** `batch_cleanup_2` re-ran GREEN on the #298 image (the deterministic crash is gone). AUDI-1042 remains In Progress / P1 / Malachi; validation bar = OpenAI storage ~2.4TB→~150GB over the next cleanup cycles (pending). SDK gotcha captured in memory `reference_openai_sdk_pagination`.

**Logs:** `on-call/incidents/INC-007/` — `batch_submit` try4 (submit quota fail), `batch_transition` try8 (fetch FileNotFoundError), `wait_for_product_categorization` try2 (sensor fast-fail), + the 07-30 owner-meeting transcript.

---

### INC-008 — `fangorn_inference_pipeline_run` `inference_pipeline` — 290-worker Dataproc create failed on a TRANSIENT us-central1 GCP STOCKOUT (out of large-instance machines; owner-confirmed) — cascaded into a quota self-block on the middle retry; NOT champion/challenger contention
**Date:** 2026-07-30 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [fangorn_inference_pipeline_run/inference_pipeline] at 2026-07-29 11:00 PT`, run `scheduled__2026-07-29T18:00:00+00:00`, `try_number=2` (`max_tries=1` → exhausted → PagerDuty).
**Error (Airflow log tail):** `RuntimeError: Job failed with: code: 9 … failed tasks are: [create-dataproc-cluster]` — the generic wrapper. The REAL cause depends on the attempt (two surfaces, one root — see below).

**STATUS: RESOLVED — re-run provisioned green.** `fangorn-inference-01685d1a` created 21:45→**21:49:15Z DONE in us-central1-a** (the same zone that stocked out at 20:02Z — capacity had cleared), cluster RUNNING. **Root cause owner-CONFIRMED (Brian McAdams, 2026-07-30 ~21:56Z): "it was a stockout — GCP ran out of available machines in general. Wasn't an issue on our end."** Consistent with the evidence: attempts 1 (us-central1-a) and 3 (us-central1-c) were explicit external zonal stockouts; the quota error on the middle attempt was a downstream self-block (below), not a separate fault. Nothing to fix on our side beyond optional hardening (IMP-015).

**Root cause (RECONCILED from owner's log + the quota API — this supersedes my two earlier wrong calls):** the 290-worker cluster requests **~4,672 N2_CPUS (~93% of the project's 5,000 us-central1 N2_CPUS quota)** + **~145,500 GB DISKS_TOTAL_GB (~65% of 225,280)**. It barely fits, so it fails **two ways on the same ceiling**, on different attempts:
- **Attempt 1** `fangorn-inference-445fba60` (us-central1-a, 19:55→FAILED 20:02Z): partially provisioned **~250 of 290 workers** then hit a **zonal `code 14 UNAVAILABLE '(resource type:compute)'`** on the remainder (my first Dataproc-op read). The ERROR cluster's ~250 VMs (**~4,016 N2 / ~125,000 GB**) then **lingered until cleanup at 20:47Z.**
- **Attempt 2 (task try-2, 20:12→20:21Z; owner Brian McAdams' log @20:17Z):** upfront **quota check FAILED** — `Insufficient 'N2_CPUS' quota. Requested 4672, available 984` + `Insufficient 'DISKS_TOTAL_GB' quota. Requested 145500, available 100280`. The 984 free = 5000 − the ~4,016 that attempt-1's uncleaned ERROR cluster was still holding. **Self-blocking retry trap:** the failed attempt's VMs quota-block the retry until they're torn down. (Quota fails before any cluster object is created → no cluster in the Dataproc list for this attempt, which is why I missed it.)
- **Attempt 3** `fangorn-inference-c7115589` (manual re-run, us-central1-c, 21:07→FAILED 21:15Z): fresh **zonal `state:STOCKOUT`** in us-central1-c.

Math corroboration: 4,016 N2 ÷ 16 vCPU ≈ 251 nodes; 125,000 GB ÷ 500 GB ≈ 250 nodes = attempt-1's partial cluster. A smaller `fangorn-daily-drift` cluster created fine at 20:48Z (once attempt-1's VMs were freed) — confirming it's a large-request ceiling problem, not a blanket outage.

**Verdict: `transient_infra` — external, transient GCP large-instance STOCKOUT in us-central1 (owner-confirmed), which cascaded into a one-attempt quota self-block.** The root trigger was GCP being out of the machines to fill a 290-worker (~4,672 N2_CPUS) cluster; it cleared on its own within ~2h (green at 21:49Z, same zone). The one "our-side" wrinkle was a downstream consequence, not the cause: because the cluster is ~93% of the 5,000 N2_CPUS quota, attempt-1's stockout-failed cluster left ~250 workers (~4,016 N2) lingering until cleanup 45 min later, so the middle retry hit `Insufficient N2_CPUS quota` (984 free < 4,672). **Both my earlier calls were wrong:** (1) champion-vs-challenger contention — refuted (challenger deleted 18:53Z, 62 min before the champion's 19:55Z create; `inference_pipeline` is UPSTREAM of `challenger_inference_pipeline`, sequential/never concurrent); (2) "the binding ceiling is our quota" — over-reached: the quota fail was real but secondary to the external stockout, which is the actual root.

**Action:** re-run now — as of 21:2xZ N2 usage is 80 (4,920 free) and disk usage 500 GB, so quota headroom is ample; the only remaining risk is a fresh zonal stockout at create time (autozone re-picks). If a re-run's create fails, **first check the failed op / worker-pool log for quota-vs-stockout** and, if quota, **confirm no prior failed cluster is still holding VMs** (delete it), then retry. Keep owner (Brian/ML) informed while the daily inference output is delayed.

**Method lesson (mine — logged, two misses):** (1) inferred champion/challenger contention from a grid screenshot + two same-day 290-worker creates WITHOUT the discriminating test (challenger DELETE-time 18:53Z vs champion CREATE-time 19:55Z; plus the DAG makes them sequential). (2) then over-corrected to "external zonal stockout" from ONE surface (the Dataproc-op error) without pulling the **quota API** or the owner's worker-pool log — which showed the real ceiling is N2_CPUS/DISKS quota. **The general rule for "create-dataproc-cluster code 9": pull ALL surfaces before naming a cause — the failed op's error (zonal), the Vertex/worker-pool `service` log (quota), AND `gcloud compute regions describe <region>` quota-vs-usage — and reconcile them. One surface underdetermines the root cause.**

**Diagnosis run (copy-paste — pull all three surfaces, then reconcile):**
```bash
# A. Dataproc op error (zonal stockout surfaces here): describe the FAILED create
gcloud dataproc operations describe <FAILED_CREATE_OP> --project=mntn-targeting-prj-prod --region=us-central1 --format="value(error)"
# B. Compute quota vs usage NOW (quota exhaustion surfaces here): N2_CPUS, DISKS_TOTAL_GB
gcloud compute regions describe us-central1 --project=mntn-targeting-prj-prod --format=json \
  | python3 -c "import sys,json;d=json.load(sys.stdin);[print(q['metric'],q['limit'],q['usage']) for q in d['quotas'] if q['metric'] in ('N2_CPUS','DISKS_TOTAL_GB','CPUS')]"
#    290-worker cluster needs ~4672 N2 (~93% of 5000) + ~145500 GB disk. If usage high -> quota fail. If usage low -> zonal.
# C. Self-block check: is a prior FAILED (ERROR) cluster still holding VMs/quota? delete it before retry.
gcloud dataproc clusters list --project=mntn-targeting-prj-prod --region=us-central1 --format="table(clusterName,status.state)"
```

**Durable fix (IMP-015):** the 290-worker cluster at ~93% of N2 quota has no headroom — it fails on any concurrent N2 use, on its own uncleaned failed attempts, or on single-zone stockouts. Fixes (owner Fangorn/ML + infra, template in `targeting-infra`): (1) **raise us-central1 N2_CPUS + DISKS_TOTAL_GB quota** so the request isn't ~93% of ceiling; (2) **auto-delete a failed cluster-create immediately** so its VMs don't quota-block the retry (the 20:02→20:47Z lingering caused the 20:17Z quota fail); (3) multi-zone / fallback-zone or secondary-region placement; and/or (4) a smaller cluster / machine family. Distinct from INC-002/IMP-002 (real champion-vs-concurrent-job contention). Do NOT hot-patch.

**Logs:** `on-call/incidents/INC-008/`.

---

### INC-009 — `keyword_ddp_reporting` `write_targeted_signal_ds_19` — KubernetesPodOperator pod EVICTED mid-run (long dbt python model on Databricks); both retries exhausted
**Date:** 2026-07-31 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [keyword_ddp_reporting/write_targeted_signal_ds_19] at 2026-07-30 08:00:00 PT`, run `scheduled__2026-07-30T15:00:00+00:00`, "Try 0 of 2", alert body **"No exception message found."** (empty log, `sources=[]`). Task **terminally FAILED** after both attempts (`retries=1`). **NOTE:** the alert prefix says "Targeting" but the DAG's team is **ML** (`JobTeamConfig.ML`).

**STATUS: RESOLVED 2026-07-31 — Sean Yang manually marked `write_targeted_signal_ds_19` success + let ds_13 run; the Databricks job actually SUCCEEDED and the data is verified complete in GCS (1,200 parquet files at `signals/targeted_signal/data_source_id=19/dt=2026-07-30/`, matching the plan's 1,200-file / 1.8B-row write). The Airflow failure was ORCHESTRATION-only (pod 404). Recurrence fix = IMP-018.** (Interim hypotheses below evolved spot-preemption → table-already-exists → the reconciled cause; see the RESOLVED block at the end of this entry. The original two owner checks — did the Databricks job succeed + is the data there — are both YES.) **NOT a dbt/SQL error, NOT a data problem, NOT the OpenAI/shopper_graph image path.** This is a DIFFERENT task from INC-006/007 (those = the `wait_for_product_categorization` sensor upstream; here the sensor is GREEN and the downstream dbt task failed).

**Verdict: transient_infra — the orchestrating `KubernetesPodOperator` pod is evicted/deleted ~50 min into the run, before the long-running dbt PYTHON model finishes on Databricks.** The pager is an Airflow↔pod infra failure, not task logic.

**What happened (both tries share ONE cause — the pod doesn't survive the model's runtime):**
`write_targeted_signal_ds_19` = `DbxDbtOperator` (a `KubernetesPodOperator`, image `generic_dbt_runner_ml:gcp-prod`, `is_delete_operator_pod=True`) running `dbt run --select targeted_signal_ds_19 --target prod_warehouse_2xs`. The dbt model `mntn_matched_reporting.targeted_signal` is a **python table model** → it submits a Databricks **Jobs** run and polls it to completion (NOT a SQL-warehouse query; the `2xs` target only sets the connection). Pod `write-targeted-signal-ds-19-822bmvra` (ns `meteoric-conservation-7520`) started ~15:00Z, submitted Databricks `run_id=65237255325756` at 15:02:10Z, then sat polling with no further log for ~50 min:
- **Try 1** (15:00:21Z → failed 15:42:42Z, ~42 min): the Airflow worker serving it lost connection — `Could not read served logs: ... Connection to 100.64.4.174:39091 timed out (connect timeout=5)`. Worker/log-server unreachable → task marked failed. **This is the pager + the empty "No exception message found" UI** (the log endpoint was gone → `sources=[]`).
- **Try 2** (15:47:44Z → failed 15:52:03Z): rebuilt the pod spec, found the try-1 pod **still `phase=Running` and REUSED it** ("since it is not terminated or evicted"), resumed streaming the live dbt output — then at **15:52:03Z the pod was DELETED out from under it**: `Pod meteoric-conservation-7520/write-targeted-signal-ds-19-822bmvra not found during istio check` → `ApiException: (404) ... pods "…822bmvra" not found` in `await_pod_completion` → FAILED. `retries=1` → both attempts used → terminal FAILED; downstream `write_targeted_signal_ds_13` + `write_targeted_signal_ds_19_domain` never ran.

**Why the pod vanished (most likely):** GKE/Astronomer cluster-autoscaler **scale-down or node preemption** evicting a pod that is only *polling* a long Databricks job (near-zero CPU → looks idle → node reclaimed); "not found during istio check" is the pod-manager's sidecar liveness probe noticing it gone. Ruled out: try-1 `is_delete_operator_pod` cleanup (the pod was still Running at 15:47, 5 min after try-1 failed), and natural dbt completion (no "Completed"/"OK" line ever printed — the pod died mid-poll). Either way it's a K8s pod-lifecycle event, not the DAG or the data.

**Input is HEALTHY (rules out a data cause):** the sensor `wait_for_product_categorization` is GREEN — `product_categorization/dt=2026-07-29` present (53 objects, normal size; the fetch-logical-07-30 run writes `dt=07-29`). The model ran 42+ min doing real work, not fast-failing on absent input.

**Impact: LOW.** Sev-5 reporting DAG, no serving path (same class as INC-006/007). Worst case = one delayed DS19 keyword-DDP report cycle. The Databricks job (65237255325756) may have completed independently of the pod death and written the ds_19 table — owner to confirm.

**Diagnosis run (copy-paste for next time):**
```bash
# 1. Input health — is the categorization partition the sensor cleared actually present?
gcloud storage ls "gs://mntn-data-archive-prod/shopper_graph/product_categorization/dt=<run_date_minus_1>/" | grep -c gs://
# 2. Did the Databricks job the dbt python model submitted actually finish?
#    (grab the run_id from the log line: "Databricks adapter: Job submission response=b'{\"run_id\":<ID>}'")
databricks jobs get-run <RUN_ID> -o json -p malachi@mountain.com   # state.result_state; then get-run-output <TASK run_id> for the root error. Access resolved 2026-08-03 (U2M OAuth profile; DEFAULT profile invalid). See reference_databricks.
# 3. Grep the task log for the REAL signal — not the empty UI:
#    "Could not read served logs ... timed out"                 = worker/log-server lost (try marked failed, task itself fine)
#    "not found during istio check" + "(404) ... pods ... not found" = POD EVICTED mid-run
```

**Decision tree — `write_targeted_signal_ds_*` fails with empty log / "No exception message found" / (404) pod not found:**
1. Grep the log. `Could not read served logs ... timed out` and/or `(404) ... pods ... not found` → **transient_infra (pod/worker lost), NOT a dbt error.** Do not chase a data/SQL cause.
2. Confirm input present (cmd 1) + check the Databricks job (cmd 2). **Job SUCCESS** → the ds_19 table wrote; a **Clear Task Instance** re-run should finish fast (idempotent) and let ds_13 + ds_19_domain run. **Job FAILED/absent** → the model must fully re-run.
3. **Re-run once** (Clear the task) when cluster pressure has passed — the pod likely survives on a calmer cluster (INC-002/INC-008 transient-infra playbook). **Recurs** → the pod is reliably evicted before the ~50-min model completes → route to owner for the durable anti-eviction fix (IMP-018); or accept the one-cycle gap (reporting DAG, no serving). Never hot-patch.

**Owner + durable fix (IMP-018):** ML / MNTN Matched keyword pipeline (nearest owner **Ryan Kleck**; Victor Savitskiy departed). Keep the KPO pod from being evicted during a long dbt python model — `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` (or Karpenter/GKE do-not-disrupt) annotation, a non-preemptible node pool for these tasks, or a deferrable/async submit-and-poll so a pod eviction doesn't kill the run; and/or run the model on a bigger warehouse/job cluster so its runtime (and the pod's eviction-exposure window) shrinks below ~50 min.

**Update 2026-07-31 (re-run REPRODUCED the ~50-min hang → NOT a passing cluster blip; handed to owner Brian McAdams):** Malachi cleared + restarted the task; it **hung ~50 min then failed again**, with a new alert variant — `Pod write-targeted-signal-ds-19-822bmvra returned a failure. remote_pod details omitted; open the task log for pod metadata` (the pod reached a **Failed phase** this attempt, vs the try-2 404 eviction). Because it now reproduces across clear+restart, decision-tree step 3 ("re-run once, pod likely survives on a calmer cluster") is **tested and refuted** — not a one-off. Two live hypotheses, discriminated by the **Databricks job**, not the Airflow log:
- **(a) consistent pod/node eviction** ~50 min in (pod always on preemptible nodes / autoscaler reclaim), or
- **(b) the dbt python model's Databricks job itself reproducibly fails or times out ~50 min in** — the consistent ~50-min wall points at a fixed timeout or a genuine model/job failure (candidate: the caught-up INC-006/007 backlog inflated this cycle's input, or a job/cluster timeout). Reproducibility elevates (b) over the original transient read.
- **Discriminating test (owner has Databricks access):** inspect Databricks job **`run_id=65237255325756`** (+ the run from Malachi's re-run) — SUCCESS = the table wrote and only the pod/tracking died (a); FAILED/timeout ~50 min = the job is the problem (b). **Handed to Brian McAdams** (was looking at this recently) 2026-07-31.

**PARKED 2026-07-31 (Brian unavailable, no active owner):** Brian hasn't time to dig in now; it waits for Malachi's return or another owner. Fine to park — sev-5 reporting, no serving path, nothing owed beyond a possibly-delayed DS19 report cycle. **Alternate owners if it can't wait:** Ryan Kleck (nearest MNTN-Matched pipeline owner since Victor Savitskiy departed) or Sean Yang (fetch side, INC-006). **To CLOSE (needs Databricks access the on-call box lacks — CLI hangs on OAuth, API `connection refused`):** run the discriminating test on Databricks job `run_id=65237255325756` + Malachi's re-run. The dbt model source `mntn_matched_reporting.targeted_signal` lives in the **non-local `generic_dbt_runner_ml` dbt-ml project** (not in any cloned repo; writes only to Databricks, no GCS export), so reading the model needs that repo too.

**SUPERSEDED 2026-08-03 (AUDI-1191): Databricks access resolved + discriminating test run.** The on-call box now reads Databricks key-free via the U2M OAuth CLI profile `malachi@mountain.com` (see reference_databricks). The discriminating test was run: job `run_id=65237255325756` returned `result_state=SUCCESS` with 0 failed tasks, confirming outcome (a), the pod/tracking died and the table wrote, consistent with the already-recorded RESOLVED reconciliation. The [[project_airflow_debugger]] `airflow_debugger` package reproduces this verdict automatically from the raw log (`orchestration/pod-evicted`, downstream job SUCCEEDED). Durable fix remains IMP-018.

**Update 2026-07-31 (ROOT CAUSE CONFIRMED = SPOT INSTANCE PREEMPTION; Ryan Kleck picked it up + shared the Spark UI):** the discriminating test resolves to **the Databricks Spark job's executors being killed by spot preemption** — the fused (a)+(b): the *job* fails, and the mechanism is *infra reclaim*. Ryan's Spark task UI shows a wall of `FAILED … ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: spot instance preemption, spot instance kill` (executor 2 / host 10.52.0.158) with **heavy per-task spill (4.9 GiB memory / 1.2-1.3 GiB disk on ~75-108 MiB input)**. Mechanism: the job cluster runs on **spot/preemptible instances**; executors get reclaimed mid-stage → tasks fail → Spark re-runs them (spilling ~5 GiB each) → the job thrashes and never completes in the pod's ~50-min window → the reproducible hang-then-fail. The Airflow KPO pod's own try-1 log-server loss / try-2 404 are the SAME spot-reclaim wave hitting the pod's node (same spot pool). NOT a data/logic bug (input partition healthy). **Chronic config issue** (pure-spot cluster + no on-demand fallback + heavy spill), not a passing blip → verdict `transient_infra` (external spot reclaim) but the point is the durable fix. **Ownership (corrected, supersedes the PARKED note): WE own the fix (Malachi); Ryan Kleck is ADVISING, not on it.** Ryan's immediate call = "bump the RAM for now" (cuts the spill) — but RAM alone does NOT stop the spot kill, so pair it with getting the cluster off pure spot (see fix #1). Config lives in the python model's job-cluster spec (dbt-ml repo) or, if it targets an all-purpose cluster, that cluster's Databricks config.

**Durable fix (this SUPERSEDES the earlier pod-annotation-only framing of IMP-018 — the fix is Databricks-cluster-side):**
1. **Take the Databricks job cluster off pure spot** — driver + a floor of executors on **on-demand**, or Databricks **spot-with-fallback-to-on-demand** (`gcp_attributes` availability / cluster policy), so a preemption wave can't kill the whole job. Direct fix.
2. **Cut the spill** — 4.9 GiB memory-spill/task on ~90 MiB input = under-partitioned / memory-starved Spark (same class as INC-005's shuffle spill on `tpa_mntn_id_export`): raise `spark.sql.shuffle.partitions` / executor memory so each task is cheaper and each preemption costs less recompute.
3. **Observability (Ryan's 2nd issue):** the job surfaces no Spark logs by default; `spark.eventLog.enabled=true` → GCS gives them but disables the live Spark History Server. That tradeoff is fine for post-mortems — parse the GCS event log offline (the INC-005 `eventlog_profiler.py` pattern), which is the better tool for a spill/preemption profile anyway.

**Meeting 2026-07-31 (Malachi + Ryan Kleck, 30 min — transcript `incidents/INC-009/meetings/inc009_01_ryan_spot_preemption_2026_07_31.txt`) — added facts:**
- **A GCP autoscaling change (2026-07-30, Brian's team) was floated as the trigger — UNCONFIRMED, and Malachi doubts it's related.** Ryan's in-meeting guess ("they did some autoscaling change yesterday … 'nope, it's been too long'"). It does NOT matter for the fix: the cluster config runs all 6 workers as spot (below), so it's been exposed to `spot instance kill` regardless of any autoscaling change. Fix the config; don't gate on the GCP theory. (If wanted, the "why now" is worth a separate look — did the job's runtime creep up / input grow so it now reliably spans a preemption?)
- **Impact is HIGHER than the sev-5 label — per Malachi (owns the business context):** "this is the one where we give money to our vendors" (Ryan noted Victor normally owned it). DS19 keyword DDP feeds a **vendor payment / reconciliation** path, not just an internal report → a multi-day gap is NOT low-impact. This qualifies the runbook §4 "no serving path" note for keyword_ddp; confirm the exact consumer + $ cadence.
- **The FAILED spot-preemption rows are NORMAL Spark retry, not app bugs** (converges with Cursor's read + mine): don't debug them unless the SAME stage keeps failing after retries with a **non-preemption** error. Cheapest first attempt = raise Spark/Databricks task max-retries + bump RAM so preemptions don't exhaust the stage; on-demand is the reliable fix.
- **Access:** Malachi had fewer Databricks perms than Ryan/Brian ("producers/dev/users" vs Brian "admins/users"); Ryan granted **temp admin** so the Spark UI was reachable. **That grant persisted — `malachi@mountain.com` is in `admins` as of 2026-08-20.** Workspace admin still does NOT grant Unity Catalog `system` schema access (`system.lakeflow` denies `USE SCHEMA`). Permanent programmatic access (API token/MCP) is gone (vault/security policy — same class as the decommissioned Slack bot).
- **Owner reality:** Victor Savitskiy departed → pipeline under-owned; Sean Yang is a fallback. **We (Malachi) drive the fix; Ryan + Brian advise.**
- **Current run:** by end of meeting "looks like it's running" (Spark grinding through preemptions via retry). Watch to green.

**Exact config located (GitHub, read-only) — `SteelHouse/dbt` → `ml_squad/models/reporting/targeted_signal_ds_19.yml`** (+ `_ds_13`, `_ds_19_domain`, same pattern; `alias: "targeted_signal"` → why the relation is `mntn_matched_reporting.targeted_signal`; `submission_method: job_cluster`). Current `job_cluster_config`: `driver_node_type_id: c3d-standard-4` (16 GB), `node_type_id: c3d-standard-8` (32 GB), `autoscale min/max = 6/6`, `gcp_attributes.availability: PREEMPTIBLE_WITH_FALLBACK_GCP`, **`first_on_demand: 1`**, and **NO `spark_conf`** (executor memory = node default). The two knobs:
1. **RAM (Ryan's "+4 GB"):** no `spark_conf` today and no clean "+4 GB" node — the effective move is worker `node_type_id` `c3d-standard-8` (32 GB) → `c3d-highmem-8` (64 GB), and/or add a `spark_conf` (mirror `ml_squad/models/audience_intent/prospecting_intent.yml`: `spark.executor.memory` + `spark.sql.shuffle.partitions: auto`) to cut the 4.9 GB/task spill.
2. **Spot kill (the actual failure):** `first_on_demand: 1` = only the driver on-demand; all 6 workers spot → `ExecutorLostFailure spot instance kill`. `PREEMPTIBLE_WITH_FALLBACK_GCP` fallback applies only at LAUNCH, not mid-run reclaim, so workers still die. Fix = set `availability: ON_DEMAND_GCP` (all nodes on-demand, simplest), OR keep fallback and raise `first_on_demand` to cover the workers (1 driver + 6 workers = 7 nodes → `7` = fully on-demand; `4` = driver + 3 workers). **RAM alone won't stop this.**
**Do NOT edit from the on-call box** — `SteelHouse/dbt` is a prod repo we don't own (prod-safety); hand the diff to the owner or do it in Cursor. Deploy = a rebuild of the `generic_dbt_runner_ml` image (confirm the workflow before merging).

**⚠ MAJOR CORRECTION 2026-07-31 (Malachi let the Databricks job run to completion → the TRUE fatal error surfaced): the job fails on `[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table `prod`.`mntn_matched_reporting`.`targeted_signal` because it already exists` (SQLSTATE 42P07), NOT spot preemption.** Databricks Job run 459011294807453 (job 682599050756239) ran **1h 56m** (09:07→11:03) and failed here. **Spot preemption + spill are SECONDARY** — they slow the job to ~2h and kill the Airflow KPO pod at ~50 min *before* it reaches this write, which is why the Airflow-truncated runs only ever showed pod-eviction, masking the real error. **Model code is unchanged since 2026-06-16 → this is state/concurrency, not a code regression.**
- **LEADING HYPOTHESIS = orphaned concurrent runs.** Killing the Airflow KPO pod does NOT cancel the Databricks job it launched (the pod only polls). So a pod death at ~50 min leaves the ~2h Databricks run orphaned-but-alive; it finishes and CREATES `targeted_signal`. The Airflow retry / a manual clear then launches ANOTHER ~2h job → its final create collides with the orphan's table → `TABLE_OR_VIEW_ALREADY_EXISTS`. Reproduces on every clear+restart (each adds an orphan). `max_active_runs=1` on the DAG does NOT stop this — orphaned Databricks runs escape Airflow's concurrency guard.
- **This RE-CONNECTS the spot/RAM fixes:** they matter because a surviving pod → Airflow tracks the single job to completion → no orphaned duplicates → no collision. Fixing spot/spill (so the pod finishes in-window) or making `DbxDbtOperator` cancel its Databricks run on pod termination removes the DUPLICATION that causes the create error.
- **Model = dbt python `materialized="table"` + `partition_by=[data_source_id,dt,source_data_source_id]` + `partitionOverwriteMode=dynamic`, `spark.sql.shuffle.partitions=853` (set in-model), returns `final_df` (site_visit_signal ⋈ product_categorization).** Post-hooks VACUUM/OPTIMIZE.
- **Checks to CONFIRM (Databricks, Malachi has temp admin):** (1) does `targeted_signal` already have the current `run_date` `dt` partition? — if YES, an orphan already produced the data → the cycle is DONE, just reconcile Airflow (mark ds_19 success, run ds_13/ds_19_domain), no new run. (2) how many runs of job 682599050756239 ran/overlapped in the last ~3h, any still RUNNING now? — 2+ overlapping confirms the orphan mechanism. (3) last clean SUCCESS date.
- **Immediate remediation IF confirmed:** cancel all running/orphaned Databricks runs of this job → confirm/repair the table state (drop the stale `targeted_signal` + any `__dbt_tmp` ONLY if it lacks today's data; it's `materialized=table` so it rebuilds — but it feeds vendor payments, coordinate) → run EXACTLY ONE clean run, let it finish (~2h), do NOT clear/retry while one is running.

**✅ RESOLVED 2026-07-31 — reconciled from the Spark query plan + GCS + Sean Yang. The DATA is fine every cycle; the failures are purely orchestration (pod 404) + concurrent-run collision.**
- **The Databricks job SUCCEEDS and writes its data.** Spark plan node (44) `Execute InsertIntoHadoopFsRelationCommand … gs://mntn-data-archive-prod/signals/targeted_signal … Overwrite [dynamic partition]` completed: **1,811,620,941 rows, 1,200 files, 10 dynamic partitions.** Verified live in GCS: `signals/targeted_signal/data_source_id=19/dt=2026-07-30/` = **1,200 parquet files** (exact match). The `dt=2026-07-30` output for this cycle is COMPLETE.
- **The `TABLE_OR_VIEW_ALREADY_EXISTS` is a SEPARATE catalog step AFTER a successful data write** — dbt registering the UC table `prod.mntn_matched_reporting.targeted_signal`. It collides because a prior/concurrent run already created it. Confirms the orphan/duplicate-run mechanism: run A (pod died at ~50 min, but the detached Databricks job kept running, SUCCEEDED, wrote data + created the table) → a retry/clear launched run B → run B re-did the idempotent dynamic-overwrite write, then hard-failed on the catalog CREATE because run A's table exists.
- **Sean Yang (2026-07-31 11:11):** "The actual ds19 job was successful in databricks, but the pod in k8 disappeared [`404 … pods "write-targeted-signal-ds-19-822bmvra" not found`], causing airflow not able to find it. I manually marked this step successful and let DS13 run. If the same issue happens again, we'll need a long-term fix." Correct call — data was already complete.
- **Why the pod outlives its window then dies:** the model runs **~2h** because it re-dedupes the ENTIRE `prod.mntn_matched.product_categorization` history — Spark plan: scan `dt ≤ 2026-07-29`, **13.5 BILLION rows / 481 daily partitions / 954 GiB / ~2 TiB shuffle**, rank-latest-per-`composite_key`, then SortMergeJoin to the day's `site_visit_signal` (`dt=2026-07-29`). Multi-TiB sorts + 300+ GiB spills. Plan also flags **optimizer stats MISSING on `product_categorization`** ("consider ANALYZE TABLE … COMPUTE STATISTICS"). A ~2h job + a pod that dies at ~50 min = orphan + 404 + a colliding retry.
- **Long-term fix (Sean's "if it happens again") = IMP-018, broadened to 4 levers:** (1) **orchestration** — keep the KPO pod alive for the full ~2h (`safe-to-evict:false` / non-preemptible / longer deadline) OR make `DbxDbtOperator` **cancel its Databricks run on pod termination** so a dead pod neither orphans nor lets a retry spawn a colliding duplicate; (2) **perf** — cut the ~2h: `ANALYZE TABLE prod.mntn_matched.product_categorization COMPUTE STATISTICS` + bound/materialize the "latest categorization per composite_key" instead of re-scanning all 481 days each run (a faster job survives the pod window); (3) **idempotency** — the catalog CREATE should tolerate an existing table (CREATE OR REPLACE / handle concurrent create) so a duplicate run doesn't hard-fail; (4) **spot/RAM** (earlier thread) — secondary, reduces preemption/spill. **Data was never at risk** at any point.

**Logs:** `on-call/incidents/INC-009/` — try-2 full (`…try2_full_pod404-eviction.txt`: the Databricks `run_id` + the (404) eviction) + a try-2 reattach snapshot. (Try-1's 2-line `Could not read served logs` timeout was captured in-session; that file was overwritten with the try-2 full log before archival.)

---

### INC-010 — `tpa_ipdsc_export` `wait_ds17_src` — mandatory partner (ShareThis/DS17) missed source delivery → 1h sensor hard-timeout; existence sensor then PASSED on a backfilled copy
**Date:** 2026-08-05 (executed) · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [tpa_ipdsc_export/wait_ds17_src] at 2026-08-03 19:35 PT`, run `scheduled__2026-08-04T02:35:00`, `AirflowSensorTimeout: run duration 3774.38s exceeds timeout 3600.0` (1h, `mode=reschedule`, 6 reschedules). **STATUS: RESOLVED** (owner Sean Yang).

**Verdict: real_upstream_failure (late/missing MANDATORY partner data).** `wait_ds17_src` is a `GCSObjectsWithPrefixExistenceSensor` polling `gs://mntn-data-partners/partners/sharethis/segments/date={{ data_interval_start.subtract(days=1).format('YYYYMMDD') }}/` (1h timeout / 120s poke / `mode=reschedule`). Schedule `35 2 * * *`, so the alerted run (`data_interval_start=2026-08-04 02:35 UTC` = **2026-08-03 19:35 PT**, matching the alert; executed **2026-08-05 02:35 UTC**) resolves the prefix to `date=20260803/`. ShareThis (DS17) missed the 2026-08-03 delivery → the prefix was empty through the whole window (02:35–03:38 UTC) → timeout. **DS17 is MANDATORY** — unlike the OPTIONAL Bombora/DS51 `wait_bombora_src` (`soft_fail=True` → SKIPPED, INC-001), a mandatory `wait_<ds>_src` timeout hard-fails and pages (docstring: "mandatory data sources DS4, DS17, … are never tolerated").

**Resolution (Sean Yang, MNTN — Slack):** manually **copied the previous day's (Aug 2) data** into `date=20260803/` to unblock, then deployed a **forward fix: auto-resolve to day-1 data when source is not received on time.** GCS-confirmed: `date=20260803/` holds byte-identical Aug-2 dupes (files named `…ip-20260802-*`, 14.26 GiB total, landed **2026-08-05T05:02:49Z**, ~1.5h after the try-1 timeout). Because the sensor checks EXISTENCE only, retries try2/try3 then passed on that copy and the DAG completed green.

**⚠ Data-quality gap (the half the page doesn't catch):** `GCSObjectsWithPrefixExistenceSensor` greenlights on ANY object at the prefix — a backfilled copy (even stale/duplicate/wrong-dated data) passes the retry silently. The page catches the *delay*; nothing catches the *bad backfill*. Sean's day-1 fallback makes "use previous-day data on late source" the intended behavior for this feed.

**NOT related to eventLog PR #1169** (merged 2026-08-04): a GCS existence sensor has no Spark/Dataproc/eventLog surface, and the `tpa_ipdsc`/`ipdsc_emr_cluster` path was reverted to main before that merge (byte-identical to main).

**Diagnosis (copy-paste):**
```
bash .claude/scripts/airflow_pull.sh --date 2026-08-05 --dag tpa_ipdsc_export --all-tries
grep "Checking for existence" on-call/airflow_logs/2026-08-05/*wait_ds17_src*try1*failed.log   # -> date=20260803/
gcloud storage ls -l "gs://mntn-data-partners/partners/sharethis/segments/date=20260803/"        # empty in window; Aug-2 dupes @05:02Z
```

**Decision tree (next mandatory `wait_<ds>_src` timeout):**
1. Read the failed-try log → the poked prefix `partners/<vendor>/segments/date=<D>/`.
2. `gcloud storage ls .../date=<D>/` — **absent** → partner missed delivery → **real_upstream_failure**, route to the feed owner. Do NOT widen the timeout or soft-fail a mandatory sensor (that is a prod DAG change owned by TPA_EXPORT).
3. **present but WRONG** (filenames/date/bytes match a prior day) → a backfilled COPY; the pipeline will run on stale data. Flag the data-quality risk to the owner.
4. Optional partners (Bombora/DS51 `precondition_*`/`wait_bombora_src`) soft-skip instead → benign (INC-001).

**Logs:** `on-call/airflow_logs/2026-08-05/033816__tpa_ipdsc_export__wait_ds17_src__try{1_failed,2_success,3_success}.log`.

---

### INC-011 — `hashed_email_ds_26_signals` `wait_fpa` — ExternalTaskSensor fast-failed on an upstream SKIP (Predactiv/DS26 missed one hourly file); producer DAG SUCCEEDED
**Date:** 2026-08-05 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [hashed_email_ds_26_signals/wait_fpa] at 2026-08-05 15:00:00 PT`, run `scheduled__2026-08-05T22:00:00+00:00`, Try 1 of 2, `ExternalTaskFailedError: Some of the external tasks ['dsid26_predactiv_processing'] in DAG fpa_site_visit_batch_serverless failed.` **STATUS: RESOLVED (benign false alarm — self-heals next hour).**

**Verdict: false alarm — benign partner-data gap surfaced as skip-treated-as-failure.** NOT a pipeline break. The producer DAG `fpa_site_visit_batch_serverless` for logical `2026-08-05T22:00:00Z` **SUCCEEDED** (23:00:00→23:08:35Z). `wait_fpa` is an `ExternalTaskSensor` on the SAME logical date (no `execution_delta`) that poked `dsid26_predactiv_processing` and **fast-failed in 4.8s** (not a timeout). That external task's final state = **`skipped`** (try 0, `DataprocCreateBatchOperator`, instant @23:00:43Z) — the sensor's `failed_states` counts a `skipped` external task as a failure and raises `ExternalTaskFailedError`.

**Why the skip (root cause):** the producer's `source_available_dsid26` (`_ShortCircuitDecoratedOperator`) checked `gs://mntn-data-partners/partners/predactiv/dt=2026080520/` → logged `No source data for dsid=26, dt=2026-08-05, hh=20; skipping Dataproc batch` → returned `False` → downstream `dsid26_predactiv_processing` correctly **skipped**. **Predactiv (DS26) missed its hour-20 (UTC) source file.** The other 5 sources in the same run (DS25/23/28/30/36 `source_available_*` → `*_processing`) all ran and succeeded. This is intentional, data-driven producer behavior.

**Blast radius:** downstream `hashed_email_ds_26_signals` run = failed (`wait_fpa` failed → `populate_hem_data_ds_26` `upstream_failed`). **No data loss** — there was no DS26 source to process that hour. **Rarity:** `dsid26_predactiv_processing` 23/24 recent hourly runs = success (1 skipped); `wait_fpa` 23/24 = success (1 failed) — first occurrence in 24h.

**Distinct from lookalikes:** INC-010 (`tpa_ipdsc_export/wait_<ds>_src`) is a MANDATORY partner-feed **existence-sensor 1h timeout** — a different sensor family in a different consumer DAG. INC-006/007 (`keyword_ddp_reporting/wait_for_product_categorization`) is an ExternalTaskSensor fast-fail on upstream **`failed`/`upstream_failed`** = a real break. **Here the upstream is `skipped` (benign no-data), not failed** — same fast-fail shape, opposite meaning.

**Action:** none required to recover data (nothing to process); the DAG proceeds normally next hour. Optionally Clear `wait_fpa` to green the run, but do NOT expect it to pass on retry and do NOT backfill — the DS26 partition for hour-20 will never land.

**Durable fix (route to the Targeting/TI owner; never hot-patch a prod DAG):** align the cross-DAG contract with the producer's intentional skip — set `skipped_states=[State.SKIPPED]` on `wait_fpa` so a legitimate no-data upstream skip makes the consumer **SKIP** (not FAIL + page), OR gate `hashed_email_ds_26_signals` with the same `source_available_dsid26` short-circuit the producer uses. Logged IMP-026. **Fix MERGED 2026-08-05 ([airflow-ti#1175](https://github.com/SteelHouse/airflow-ti/pull/1175), 2 DAGs: this one + sibling `hashed_email_guid_log_signals`/DS23, same latent bug; `keyword_ddp_reporting` NOT changed, its skip is a real break). Tracked AUDI-1195 (Spike, Done). Moved `skipped` from `failed_states` to `skipped_states`.**

**Decision tree (next `wait_fpa` / ExternalTaskSensor fast-fail in seconds):**
1. Read the failed-try log → `Poking for tasks [X] in dag Y on <logical>`. Confirm it's a **fast-fail (secs), not a timeout**.
2. Query the external task's final state at that run: `skipped` vs `failed`/`upstream_failed`.
   - **`skipped`** → check the producer's `source_available_<ds>` short-circuit log for `No source data …` → benign partner-data gap → **no-op the hour**; durable fix = propagate skips.
   - **`failed`/`upstream_failed`** → real upstream break (INC-006/007 family) → audit the upstream chain.
3. Never clear-to-retry expecting success on a skip — the awaited partition will never land for that hour.

**Diagnosis (copy-paste):**
```
bash .claude/scripts/airflow_pull.sh --date 2026-08-05 --dag hashed_email_ds_26_signals --all-tries
grep "Poking for tasks" on-call/airflow_logs/2026-08-05/*wait_fpa*failed.log   # -> dsid26_predactiv_processing on 2026-08-05T22:00:00+00:00
# external task final state (REST v2): /dags/fpa_site_visit_batch_serverless/dagRuns/<run>/taskInstances/dsid26_predactiv_processing  -> state=skipped
# skip reason: source_available_dsid26 log -> "No source data for dsid=26, dt=2026-08-05, hh=20; skipping Dataproc batch"
gcloud storage ls "gs://mntn-data-partners/partners/predactiv/dt=2026080520/"   # empty
```

**Logs:** `on-call/airflow_logs/2026-08-05/230648__hashed_email_ds_26_signals__wait_fpa__try2__failed.log`.

---

### INC-012 — `materialize_mntn_select` `materialize` — driver-side GCS LIST of `augmentor_log/` timed out (flat-glob lists the whole prefix); "lost executors" was a red herring
**Date:** 2026-08-06 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [materialize_mntn_select/materialize] at 2026-08-06 12:45:00 PT`, `Try 0 of 1`, `Batch job mntn-select-2026-08-06-1786049114 failed with error: Google Cloud Dataproc Agent reports job failure` (boilerplate). **STATUS: CLOSED, VERIFIED IN PROD** (fix v2 #1177; hh=23 re-run succeeded in 7.4 min, data hole closed).

**Verdict: transient_infra (GCS list latency) hitting a fragile full-prefix listing (durable code hardening = IMP-027).** Two tries died with the same profile — try 1 batch `mntn-select-2026-08-06-1786049114` (20:45:43→21:04:24Z) and the manually-cleared try 2 batch `...1786051775` (21:29:46→21:48:17Z), both "agent reports job failure" ~19 min in, real DCU burned. Root cause from `driveroutput.000000000` (staging bucket, via `dataproc-debug` PAM): `java.io.IOException: Error listing gs://mntn-data-archive-prod/augmentor_log/region=` → `java.net.SocketTimeoutException: Read timed out`, retries exhausted, identical on both tries. Mechanism: `get_paths()` (`spark/spark_utils.py:15`) runs Hadoop `globStatus` on `region={east,west}/dt=<D>/hh=<HH>/`; the GCS connector resolves a glob by **flat-listing everything under the first wildcard** (`augmentor_log/region=` = all regions × all dates × all hours), then filtering client-side — O(entire prefix history), latency-fragile. The next-hour batch (`...1786052726`) succeeded in 11.5 min while try 2 was dying — list latency is variable, not a bad-infra window.

**The red herring:** the driver log is full of `ERROR TaskSchedulerImpl: Lost executor N ... Executor decommission finished: spark scale down` — those are **benign dynamic-allocation scale-DOWNS** (Spark logs decommission at ERROR) while the driver idled on the listing. First-glance verdicts of "lost executors" read exactly these lines.

**Impact:** `ipdsc_mntn_select/dt=2026-08-06/` missing `hh=19` (hh=12–18 + hh=20 present). Owner (Sean Yang) re-running with the DAG's `dt`/`hhs` params to close the hole; a re-run usually passes because the latency is variable.

**Durable fix v1 (IMP-027) MERGED 2026-08-06 as [airflow-ti#1176](https://github.com/SteelHouse/airflow-ti/pull/1176) — INCOMPLETE:** literal region paths for `get_paths` + the read (kills the flat-glob crawl). **The 16:45 PT run failed identically ON the new code** (script deployed 00:09Z, batch created 00:45Z, died at 1136s): driver trace shows the read's **`basePath` option** makes Spark STAT `gs://.../augmentor_log/`, and the connector resolves that stat by LISTING the root (`getFileInfoInternal` → `Error listing .../augmentor_log/` → SocketTimeout). Same ~19-min retry budget, different call site; `bidder_auction_events` survives only because its root lists fast enough (neither prefix has a directory marker). **Fix v2 MERGED + VERIFIED IN PROD 2026-08-06 ([airflow-ti#1177](https://github.com/SteelHouse/airflow-ti/pull/1177)): drops `basePath` entirely** - the hh=23 re-run on v2 SUCCEEDED in 7.4 min (vs ~11.5 min historical healthy runs; the root-list overhead is gone, not just the failure) and closed the data hole — provably unused (the parquet files carry `region` internally per the COLUMN_ALREADY_EXISTS warning; the job selects only ip/pmp/partner_id). **Lesson: on a ~17M-object prefix, EVERY path that hands the root to the GCS connector is a timeout surface — glob expansion AND basePath stat.** Owner TPA_EXPORT.

**Diagnosis lessons (hard-won):**
- The Airflow log + `batches describe` stateMessage are pure boilerplate for this class — the answer lives in the **staging-bucket `driveroutput.*`** (`dataproc-debug` PAM grants read; first ~2 min of "still 403" is propagation, retry).
- **This DAG is PHS-path** (`get_config` ← `ipdsc_emr_cluster.py`, attaches `spark_history_server_config`) — its event logs go to the PAM-gated temp bucket, NOT `spark-events`. Do not expect an archive-bucket event log.
- **Verify `spark.app.name` before trusting an event log:** `app-20260806205122216` in `spark-events` fit the failure window perfectly and was a completely different job (`site_network_hourly`). Time-window attribution alone WILL mislead.
- Cloud Logging failures from this Mac are the user's **Pi-hole DNS blackholing `logging.googleapis.com`** (resolves to 0.0.0.0; IPv4 AND IPv6 both dead) while `dataproc.googleapis.com` is unaffected — NOT a VPN/egress flake and not perms (that was this incident's mis-read; corrected during INC-013, 2026-08-07). Proven workaround: `curl --resolve logging.googleapis.com:443:142.250.73.106` with a `gcloud auth print-access-token` bearer on `POST /v2/entries:list` → HTTP 200. Permanent fix = Pi-hole allowlist (user aware, pending). driveroutput is still the better source for this class anyway.

**Decision tree (next `Dataproc Agent reports job failure` with retries dying at a ~constant elapsed):**
1. `batches describe` → runtime vs TTL (rules out the INC-005 TTL class), `approximateUsage` > 0 (Spark ran).
2. Pull `driveroutput.000000000` from the staging path in the alert (PAM if 403) → the real exception. `Error listing gs://…` + `SocketTimeoutException` → THIS incident's class.
3. Ignore `Lost executor ... spark scale down` lines — benign decommissions, not the cause.
4. Constant ~N-min death across tries = the same execution point hitting a timeout budget, not flaky infra.
5. Data hole check: `gcloud storage ls` the output partition list; re-run/backfill with the DAG's params closes it.
6. **Re-run trap:** clearing ONLY `materialize` reuses `create_batch_id`'s old XCom -> the operator finds the old FAILED batch, ATTACHES to it ('Batch with given id already exists'), and instantly inherits its failure. Clear `create_batch_id` WITH downstream so a fresh batch id is minted (same batch-id trap as `tpa_mntn_id_export`).

**Logs:** `on-call/airflow_logs/2026-08-06/210400__materialize_mntn_select__materialize__try1__failed.log`; driver outputs in scratchpad (try1/try2), key excerpts above.

### INC-013 — `fpa_site_visit_batch_serverless` `dsid30_augmentor_log_processing` — INC-012's failure class in a sibling `augmentor_log` reader (glob + basePath, both surfaces)

**Date:** 2026-08-07 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [fpa_site_visit_batch_serverless/dsid30_augmentor_log_processing] at 2026-08-07 06:00:00 PT`, Try 1 of 2, batch `fpa-dsid30-20260807-20260807t130000-7149`, "Dataproc Agent reports job failure" boilerplate. **STATUS: RESOLVED. Fix MERGED+VERIFIED 2026-08-07 (PR [airflow-ti#1179](https://github.com/SteelHouse/airflow-ti/pull/1179), all 3 scripts: literal region paths, drop basePath, existence guards; merged 16:22Z, deployed to GCS 16:23:09Z ~40s later; the 15Z dsid30 retry SUCCEEDED in ~6 min vs ~19-min deaths, hh=14 landed in both outputs). Re-runs of the 3 holes (dsid30 07/08/13Z) + augmentor_daily map13 + mntn_global_data dt=2026-08-06 DEFERRED by user — next on-call: check they happened.**

**Verdict: transient_infra (GCS list latency) hitting the SAME fragile full-prefix listing as INC-012, in a different script.** Driver traceback (Cloud Logging REST): `dsid30_augmentor_log_processing.py:30` → `spark.read.option("basePath", AUGMENTOR_LOG_BASE).parquet("…/region={east,west}/dt=<D>/hh=<H>")` → `Error listing gs://mntn-data-archive-prod/augmentor_log/region=` → `SocketTimeoutException`. Both INC-012 timeout surfaces present: the `{east,west}` glob flat-lists the whole ~17M-object prefix AND `basePath` stats the root. Script added 2026-05-07; retries masked it for ~3 months until prefix growth pushed list latency over the budget.

**Impact (2026-08-07):** failed-failed logical runs 07Z/08Z/13Z → holes at hh=06/07/12 in BOTH outputs (`signals/site_visit_signal/dt=2026-08-07/hh=<H>/data_source_id=30/` and `fpa_vendor_log/data_source_id=30/dt=2026-08-07/hh=<H>/`; logical run H processes hh=H-1). 09Z and yesterday's 23Z survived only via try 2. The 15Z run also failed try 1 at 16:19Z (4 min before deploy). **Both sibling readers were ALSO hit on dt=2026-08-06:** `augmentor_daily_gcs` `augment_hour_d0` map13 failed both tries on the identical traceback (~19-min death 03:19Z), blocking `merge_day_d0`; and `mntn_global_data`'s 00:24Z run went GREEN while its driver log said `⚠️ No data in augmentor_log` — the try/except swallowed the listing timeout, so `mntn_global_data/dt=2026-08-06` shipped with zero augmentor rows (the predicted silent-degrade, confirmed same day; prior days 08-02..08-05 were clean). Rebuild both by clearing the failed map13 + the green-but-degraded `mntn_global_data` task (both outputs overwrite-mode). DS14 `populate_data_source` consumes `mntn_global_data/dt=` — re-run it if it already read the bad day.

**Call-site sweep (the INC-012 v1 lesson, executed repo-wide this time):** remaining fragile `augmentor_log`/`bidder_auction_events` readers on origin/main: (1) `spark/fpa/dsid30_augmentor_log_processing.py` (this incident); (2) `spark/auction_log_augmentor_process_gcs.py` (`region=*` glob + basePath on BOTH prefixes, daily dt-level = larger listing; DAG `augmentor_daily_gcs`); (3) `spark/create_mntn_global_data_pyspark.py` (`region=????` + `hh=??` wildcards + basePath + recursiveFileLookup on BOTH prefixes, wrapped in try/except → a listing timeout SILENTLY degrades the output instead of failing; DAG `mntn_global_data`). `materialize_mntn_select` is fixed (#1176+#1177). **All three fixed by [airflow-ti#1179](https://github.com/SteelHouse/airflow-ti/pull/1179), merged + prod-verified 2026-08-07.**

**Decision tree (next time):** same as INC-012 steps 1-4. Additionally: local Cloud Logging failures were the user's Pi-hole blackholing `logging.googleapis.com` (resolves to 0.0.0.0) — allowlist it, or pin curl to a Google front end (`--resolve logging.googleapis.com:443:142.250.73.106`) with a `gcloud auth print-access-token` bearer on `POST /v2/entries:list`.

**Logs:** `on-call/airflow_logs/2026-08-07/142547__…dsid30_augmentor_log_processing__try2__failed.log`; driver entries in scratchpad `inc013_logging.json`.

### INC-014 — `tpa_ipdsc_export` `ipdsc_ds_17` — static ShareThis categories mapping deleted by the bucket's age-365 lifecycle rule

**Date:** 2026-08-08 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [tpa_ipdsc_export/ipdsc_ds_17] at 2026-08-06 19:35:00 PT`, tries 2+3 failed, batch `ipd-ds-17-97z-20260807-023500-3`, `AnalysisException: [PATH_NOT_FOUND] Path does not exist: gs://mntn-data-partners/partners/sharethis/categories`. **STATUS: RESOLVED (root-caused; recovery = partner re-delivery, routed to owner).**

**Verdict: real_upstream_failure (input reference file deleted by bucket lifecycle).** `populate_data_source.py:1019` reads the STATIC pipe-delimited mapping `partners/sharethis/categories` (segment id → category id); code unchanged since 2026-07-28, daily runs green through 2026-08-07 02:45Z, file gone by 2026-08-08 02:35Z. `mntn-data-partners` rule 1 = Delete at age 365 with NO prefix filter, so the one-time static delivery aged out overnight. No versioning, `softDeletePolicy: null` → unrecoverable from GCS; GCS data-access audit logs not enabled (0 entries), so lifecycle attribution is by mechanism, not a log line. Downstream `tpa_export_enrich`/`insert_file_audits`/`trigger_*` all upstream_failed; the day is blocked until the file is restored, then clear `ipdsc_ds_17` with downstream.

**Systemic exposure:** EVERY static reference file in `mntn-data-partners` dies at its 365-day birthday under the same rule. Durable fix (owner): re-obtain the mapping from ShareThis (or a mirror), and exempt static reference paths from the age rule or move them to an un-TTL'd home.

**Diagnosis commands:** debugger `--troubleshoot` on the failed log → `[high] late-data/missing-partition`, similar INC-010; `gcloud storage ls partners/sharethis/` (categories absent, segments/userprofile current); `gcloud storage buckets describe mntn-data-partners --format="json(lifecycle_config,softDeletePolicy)"`.

**Decision tree (next PATH_NOT_FOUND on a partner path):** 1. Dated partition path → INC-010/late-data class (check delivery). 2. STATIC path that "was always there" → check the bucket lifecycle rules FIRST (age vs the file's delivery date) before blaming the partner; check soft delete before declaring it unrecoverable. 3. Attribution needs data-access audit logs — usually off; say "mechanism-attributed", not "confirmed".

### INC-015 — `fangorn_inference_pipeline_run` — one missing feature-store day (dt=2026-08-07) cascaded into 4 days of alerts

**Date:** 2026-08-07..10 · **Alerts:** `wait_for_challenger_features` sensor timeout (65065s) + `challenger_inference_pipeline` code 9 (run started 08-06 18Z), then `daily_drift_pipeline` code 9 on three consecutive runs (ref 08-07/08/09). **STATUS: CLOSED 2026-08-10** (self-heal confirmed on the 18:00Z run AND the dt=2026-08-07 hole backfilled; see the CLOSED block below).

**Verdict: real_upstream_failure (producer paused) amplified by a fragile literal-path read.** `feature_store_setup_model` was paused during backfill cleanup; logical 08-04/05/06 never ran (catchup only ran the latest interval on unpause 2026-08-08 ~18:48Z). dt=08-05/06 were covered during the cleanup; **dt=2026-08-07 (logical 08-06) was never written** — the single hole. Airflow run records for Aug 4-7 do NOT survive (pause + the cleanup's run-history deletion are now indistinguishable in the UI; the grid jumps May 2 → Aug 8) — GCS dt listing is the only ground truth for what data exists. Unrelated grid artifact: `bae_ip` shows no new cells because Brian commented it out 2026-07-20 ('temporarily disabled while fixing upstream', commit a0359299), not because of the pause. Consequences: (1) run-08-06's sensor poked the missing dt for 18h → timeout → challenger failed → drift failed; (2) drift kept failing 3 more days because `run_daily_feature_drift.py:171` (targeting-infra-ml) reads `LOOKBACK_DAYS=3` literal dt paths with NO existence guard — driver output verbatim: `AnalysisException: [PATH_NOT_FOUND] ... guid_log_pivot_ip_vertical_id/dt=2026-08-07`. Same fragile-read class as INC-013/#1179.

**Window math (ref = ds(data_interval_end) of the 18:00Z run):** ref 08-07 → {07,06,05} ✗ · ref 08-08 → {08,07,06} ✗ · ref 08-09 → {09,08,07} ✗ · **ref 08-10 (Mon 11:00 PT) → {10,09,08} all present (dt=08-10 landed 08-10 ~01:0xZ) → PASSES.** Challenger green since 08-08 (its sensor needs only ref's dt).

**Diagnosis chain (all key-free):** airflow_pull per day (FS DAG: 0 tasks Aug 5/6/7 = the pause window) → GCS dt listing (the one hole) → Vertex REST `pipelineJobs?orderBy=create_time desc` (numeric job_id from the Airflow log is NOT the resource id, 404s; use the listed string names) → replica error names a Dataproc job in mntn-targeting-prj-prod → `jobs describe` driverOutputResourceUri → gsutil cat (this project's staging bucket IS user-readable, no PAM).

**CLOSED 2026-08-10.** (a) **Self-heal confirmed end-to-end:** the 18:00Z run went **fully green** — `wait_for_challenger_features` passed on the first poke (ref dt=2026-08-10 present), then `inference_pipeline` → `challenger_inference_pipeline` → `daily_drift_pipeline` all success, ending the 4-day cascade. Note the drift task ran AFTER [#85](https://github.com/SteelHouse/targeting-infra-ml/pull/85) merged (17:51Z) but its window {08-10,09,08} had no hole, so #85's slide behaviour was not exercised here. (b) **Hole filled:** manual dagRun `manual__inc015_backfill_dt_2026-08-07` on `feature_store_setup_model` (prod API, explicit `data_interval_start/end` 08-06T01:03→08-07T01:03 → `--run_date 2026-08-06` → `dt=2026-08-07`) ran **34/34 success, 0 failures, ~48 min**; all **9 live** models that lacked dt=2026-08-07 now present (`guid_log_pivot_ip_vertical_id` = 502 objects / 11.51 GiB, matching dt=08-06's 11.50 GiB). Two dirs still empty for that dt (`guid_log_derived_advertiser_id_dsc_id`, `conversion_log_derived_advertiser_id_dsc_id`) are **RETIRED models, dead since 2026-02-08**, superseded by `guid_and_conv_log_derived_advertiser_id_dsc_id` — not gaps; don't chase them. **Trigger recipe (works in prod, key-free):** `POST {astro airflow_api_url}/dags/<dag>/dagRuns` with bearer from `~/.astro/config.yaml`, passing `logical_date` **AND explicit `data_interval_start/end`** — a manual trigger does NOT infer the scheduled interval, and this DAG renders `--run_date {{ data_interval_start }}` (`dags/models/feature_store_setup_model.py:34`), so omitting the interval rebuilds the wrong day. Mechanism was pre-tested on the **dev** deployment (`cmcvcbd3j03vk01p91ksvm1vd`, DAG paused → run created, interval echoed verbatim, then deleted). Concurrency with fangorn's 18:00Z window is proven safe (the 2026-08-08 unpause run started 18:25Z and succeeded). Durable follow-up = **IMP-039** (dedicated paused prod backfill DAG, Brian's ask).

**Decision tree (next FS-related fangorn failure):** 1. Sensor timeout on `feature_store/...` dt → is `feature_store_setup_model` paused / which logical runs missed (`airflow_pull` per day)? 2. Drift code-9 → list the 3-day window's dt dirs; a hole = this incident. 3. Compute the first ref whose window clears the hole = self-heal date; backfill (single-date trigger of the missing LOGICAL date, dt = logical+1) only if you can't wait. 4. NEVER use the 90-day backfill UI on this DAG (that started this).

**Durable fixes (owner Brian/ML):** existence-guard the drift read (filter existing dt paths, warn on holes) = the #1179 pattern — **SHIPPED 2026-08-10 17:51Z: [targeting-infra-ml#85](https://github.com/SteelHouse/targeting-infra-ml/pull/85)** (`get_latest_paths()` lists existing `dt=` partitions ≤ run_date, takes latest LOOKBACK_DAYS — a hole slides the window instead of crashing; also default reference_date → 2026-08-09). **Post-#85 a missing FS day fails ONLY day 1** (ref-day sensor timeout + challenger), not the 3-day drift tail; drift silently computes on the latest existing days. Optional unpause checklist (verify next-run date + missed intervals after any pause) still open.

### INC-016 — `tpa_ipdsc_export` `tpa_export` — driver 137 after a COMPLETE write, then 4 retries wasted on the batch-id attach trap

**Date:** 2026-08-10/11 · **Alert:** `[prod] Airflow Targeting FAILURE [tpa_ipdsc_export/tpa_export] at 2026-08-09 19:35 PT`, `Try 2 of 3: Batch job tpa-export-2026-08-10-1786426594 failed with error: Driver received SIGTERM/SIGKILL signal and exited with 137 code, which potentially signifies a memory pressure.` **STATUS: RESOLVED, no data loss.**

**Verdict: `transient_infra` (driver OOM at teardown) + `dag_bug` (retry reattaches to the failed batch).** Verified timeline (logical date 2026-08-10, all times UTC):

| When | What | Evidence |
|---|---|---|
| 05:36:29 | `create_batch_id__2` runs ONCE, mints `tpa-export-2026-08-10-1786426594` into XCom | taskInstance try=1 |
| 06:42:51 | try 1 starts | |
| 07:21-07:22 | **all 5002 staging objects + `_SUCCESS` written** to `gs://sh-dw-external-tables-prod/ip_data_staging/2026/08/10/` | GCS object timestamps |
| ~07:26 | driver SIGKILL 137, batch marked FAILED **despite the export being complete** | try 1 dur 43.6m, alert text |
| 07:31 / 07:36 / 14:08 / 14:14 | tries 2-5 die in 6-18s each | logs: `Batch with given id already exists.` + `Attaching to the job ... if it is still running.` |
| 13:57:34 | [airflow-ti#1188](https://github.com/SteelHouse/airflow-ti/pull/1188) merged (driver 8g→16g, overhead 1g→4g) | PR merged_at |
| 14:23:36 | try 6 SUCCEEDS in 78s after the batch was deleted (id freed → fresh batch created) | log: `The batch ... was created` |

**Three things the alert text actively misleads you about:**
1. **The 137 on "Try 2" is INHERITED, not a second OOM.** The operator attached to try 1's failed batch and re-surfaced its terminal error. Only try 1 ever OOM'd.
2. **The OOM did NOT cost data.** try 1 wrote the complete output *and* `_SUCCESS` ~4 min before the driver died, so the 137 hit during teardown. Sizes: 08/06 224.68 · 08/07 491.33 · 08/08 334.78 · 08/09 310.99 · **08/10 416.98 GiB**, all exactly 5002 objects. 08/10 is mid-band.
3. **#1188 is NOT what fixed this and has never been exercised.** The successful batch reports `spark.driver.memory=8g` / `memoryOverhead=1g` (`gcloud dataproc batches describe ... --format="value(runtimeConfig.properties)"`), i.e. the OLD values, because the bundle had not redeployed past the 13:57 merge by 14:23. It is still a reasonable fix; it is just unproven.

**Why try 6 took 78s and that was CORRECT (not a silent no-op).** `spark/exporter/export_tpa.py:280` is `if self.force or (not self.force and not self.already_exported)`. The batch runs with `-f false`, and try 1 had already written the output, so it logged `TPA staging export already exists and force flag deselected` and skipped. It saved a ~30-min recompute of work already on disk.

**⚠ The latent landmine (benign here, dangerous next time):** `already_exported` (line 127) is `list_blobs(prefix=..., max_results=1)` then `next(blobs, None) is not None` — it returns True on **ANY ONE blob**, and never checks `_SUCCESS`. This time try 1 had finished, so skipping was right. Had try 1 died MID-write, try 6 would have skipped on a PARTIAL export and reported success, shipping an incomplete ~400 GiB dataset green. Same class as INC-013's silent degrade. Durable fix = **IMP-041**.

**Mechanism correction (matters for the fix):** `create_batch_id` (`include/util/dag_vars.py:32`) is `f"{_name}-{_dt}-{int(datetime.now().timestamp())}"`, which is **unique on every call** — so the id formula is NOT the bug and adding entropy to it changes nothing. The bug is that the id is minted by a *separate upstream task* whose XCom is re-read verbatim by each downstream retry. The correct pattern already exists in this repo: `ipdsc_ds_*` tasks fold `try_number` into the id at the point of use and are immune.

**RECURRENCE (the real cost): the retry mechanism on this task has NEVER worked.** Try history for `tpa_export`: **08/03** t1 fail 2.4m, t2 fail 0.1m, t3 fail 0.1m → **retries exhausted, dag run state = `failed`, never recovered**; **08/06** t1 fail 2.2m, t2/t3 fail in seconds, t4 success 8.9m (human); **08/10** t1 fail 43.6m, t2-t5 fail in seconds, t6 success (human). So in the last 9 days every single try-1 failure became either a page + manual recovery or an outright failed run. The three declared retries are decorative. Note try 1 fails for DIFFERENT reasons across days (2.2m vs 43.6m), so this is a retry defect, not one recurring OOM.

**⚠ FIX ORDER MATTERS — IMP-042 alone makes things WORSE.** Today a retry dies at the attach and never executes the export script, so `already_exported` is never consulted on a retry. Once batch ids are unique per try (IMP-042), every retry WILL run the script, hit `already_exported`, and on a try-1 that died MID-write it will skip on PARTIAL output and report success. The attach trap is currently acting as accidental protection against IMP-041's bug. **Ship IMP-041 (`_SUCCESS` gate) first, or both together.**

**Evidence-preservation lesson:** deleting the failed Dataproc batch (the standard way to free the id) also destroys its driver output, which is the only place the OOM's real cause lives. Capture `driverOutputResourceUri` BEFORE deleting, else the memory fix stays a guess.

**Decision tree (next `tpa_export` 137 / instant-fail):** 1. Read try 1's duration. A long try 1 + seconds-long later tries = this incident, NOT a repeated OOM. 2. **Check GCS first**: `_SUCCESS` present and object count matching a neighbouring day (5002) means the work is DONE, so just mark the task success rather than re-running 400 GiB. **Both `ip_data_staging/` and `ip_data/` retain only ~6 days**, so this check works for the current incident but CANNOT audit whether an older run shipped. 3. If it must re-run, clear `create_batch_id__2` WITH downstream (or delete the Dataproc batch to free the id). 4. Never conclude "not OOM" from the fast retries alone; and never conclude "OOM fixed" from a success unless you confirm the batch's actual `runtimeConfig.properties`.

---

### INC-017 — `materialize_mntn_first_party` `materialize` — the INC-016 retry defect on a THIRD DAG, leaving an hourly data hole

**Date:** 2026-08-15 (alert 2026-08-14 17:50 PT) · **Alert:** `[prod] Airflow Targeting FAILURE [materialize_mntn_first_party/materialize]`, `Try 2 of 3: Batch job mntn-first-party-2026-08-15-1786758616 failed`. **STATUS: RESOLVED 2026-08-15 — hole filled, fix PR open; try 1's root cause remains UNCONFIRMED (driver output is PAM-gated).**

**Recovery verified:** clearing `create_batch_id` WITH downstream minted a fresh id and try 4 succeeded. `dt=2026-08-15/hh=00` now holds **5002 objects / 25.10 GiB** (written 03:33Z), in band against hh=01 23.07 GiB and the prior day's 22.80 / 26.25 GiB. Day is complete. **Durable fix: [airflow-ti#1195](https://github.com/SteelHouse/airflow-ti/pull/1195) MERGED 2026-08-15 04:49:59Z (Ryan Kleck) and PROD-VERIFIED.** Appends `task_instance.try_number` to the existing xcom_pull batch id on both materialize DAGs, so a retry mints a new batch instead of reattaching. Live proof in the batch names: 04:45/04:50 ran `mntn-select-2026-08-15-1786769118` (no suffix, old bundle), 05:45 onward `...-1786772724-1` (suffix = try number). Six consecutive runs across both DAGs SUCCEEDED post-deploy; bundle refreshed between 04:50 and 05:45. Scoped to the two materialize DAGs (both write `.mode("overwrite")`, no skip path); the three `tpa_ipdsc_export` sites still need IMP-041's `_SUCCESS` gate first.

**Verdict: `transient_infra` (unconfirmed) amplified by `dag_bug` (shared batch-id helper).**

| Evidence | Value |
|---|---|
| try 1 / 2 / 3 | failed 1.8m · failed 0.1m · failed 0.2m → retries exhausted, dag run `failed` |
| try 2+ log, verbatim | `Batch with given id already exists.` + `Attaching to the job mntn-first-party-2026-08-15-1786758616 if it is still running.` |
| Batch lifetime | created 01:50:25Z, FAILED 01:52:02Z (97s) |
| Rarity | **1 failure in the last 100 runs**; next hour (01:50) green |
| Data | `ipdsc_mntn_first_party/dt=2026-08-15/` has ONLY `hh=01`. **`hh=00` is MISSING.** dt=08-08..08-14 all 24/24, so no accumulated debt |

**Why this one does NOT self-heal (unlike the daily DAGs).** `get_hhs` returns `[dag_run.data_interval_start.hour]`, so each hourly run owns exactly ONE `hh=`. The failed 00:50 run owned `hh=00`; the 01:50 run that succeeded only wrote `hh=01`. Nothing re-covers the lost hour. Contrast INC-015, where a daily window slid over the hole on its own.

**⚠ SYSTEMIC: this is ONE shared helper, not three separate bugs.** `create_batch_id` (`include/util/dag_vars.py:31`) is `@task`-decorated, so it runs once per dag-run and every downstream retry re-reads the same id from XCom. **Five prod call sites**, all with dead retries:
`dags/targeting/materialize_mntn_first_party_dag.py:71` (`mntn-first-party`) · `dags/tpa_export/materialize_mntn_select.py:76` (`mntn-select`) · `dags/tpa_export/tpa_ipdsc_export.py:309/458/502` (`ipdsc`, `ipdsc-geo`, `tpa-export`).
All three DAGs have now produced incidents: **INC-012** (mntn-select), **INC-016** (tpa-export), **INC-017** (mntn-first-party). One fix in the helper closes all five. Tracked as **IMP-042** (and read its ⚠ ordering note vs IMP-041 before shipping).

**Recovery (fills `hh=00`):** clear `create_batch_id` **with downstream** on run `scheduled__2026-08-15T00:50:00+00:00` so a fresh id is minted. Clearing ONLY `materialize` re-attaches to the failed batch and dies in seconds. Alternative: delete the Dataproc batch to free the id (but that destroys `driveroutput`, the only copy of the root cause — capture it first).

**Root cause still open.** The Airflow log is boilerplate; the real error is in `gs://dataproc-staging-us-central1-995798185124-d8mf0cme/.../driveroutput.*`, which returns `403 storage.objects.list denied` without the **`dataproc-debug` PAM grant** (same wall as INC-012). Given 1/100 rarity, a single re-run is the sanctioned action; request PAM only if it recurs.

**Decision tree (next `materialize_*` / `tpa_export` instant-fail):** 1. Read try 1's duration; long try 1 + seconds-long retries = this defect, and the alert's quoted error on try 2+ is INHERITED, not fresh. 2. Check the `hh=`/`dt=` output for a hole before assuming the work is lost or done. 3. Re-run by clearing `create_batch_id` WITH downstream. 4. Capture `driveroutput` BEFORE deleting any batch.

---

### INC-018 — `materialize_mntn_select` `materialize` — driver MapOutputTracker OOM, 5 failures + 5 hour holes

**Date:** 2026-08-15 · **Alerts:** 5 x `[prod] Airflow Targeting FAILURE [materialize_mntn_select/materialize]` (runs 11:45, 15:45, 16:45, 17:45, 18:45Z). **STATUS: CLOSED 2026-08-15. Fix merged 21:53:28Z, prod-verified, all 5 hour holes refilled.** Post-fix batch reported `driver.memory=16g`/`4g` and finished in 7.1 min (vs 12-min OOMs). dt=2026-08-15 now complete through hh=20: every hour 77 objects, 5.90-7.92 GiB on a smooth diurnal curve, with the five refilled hours (11, 15, 16, 17, 18) sitting exactly between their neighbours.

**Verdict: capacity `dag_bug`.** Driver output (PAM-gated staging bucket) is unambiguous: the GCS reads succeed (`Found data in bidder_auction_events` / `augmentor_log` for the hour), then `java.lang.OutOfMemoryError: Java heap space` repeatedly in `map-output-dispatcher-*` threads. That is the driver's MapOutputTracker serialising shuffle map statuses, with `spark.driver.memory=9600m` and `spark.sql.shuffle.partitions=5000`.

| Signal | Value |
|---|---|
| Failed batch durations | 12.0, 12.0, 12.2, 12.5, 12.6 min (**constant**) |
| Healthy batch durations | 6.6, 7.1, 7.4 min |
| Prior 8 days | 24/24 success every day, so this is a newly-crossed ceiling, not a regression |
| Data | `ipdsc_mntn_select/dt=2026-08-15` missing **hh=11, 15, 16, 17, 18** |

**A constant death interval means a resource ceiling, not a data bug.** A data- or content-dependent failure varies in runtime; a driver filling a fixed heap at a steady rate does not. Two hypotheses were wrong before the driver output was read: (a) a GCS listing timeout recurrence (INC-012) — refuted, the reads logged success; (b) caused by the batch-id change #1195 — refuted, ten runs passed on the new ids first, the ids render correctly, and the OOM is in shuffle machinery.

**Fix:** [airflow-ti#1198](https://github.com/SteelHouse/airflow-ti/pull/1198) merged 2026-08-15 21:53:28Z sets `spark.driver.memory=16g` + `memoryOverhead=4g` on this DAG only (NOT in shared `get_config`, which every ipdsc job uses). Matches the `driver.cores=4` shape shipped for `tpa_export` in #1188, and the commented-out DS47 precedent at `include/spark/data_source/ipdsc_emr_cluster.py:26`.

**⚠ Headroom, not a trend fix.** Map-status memory keeps growing with volume. If this recurs, the real lever is lowering `spark.sql.shuffle.partitions` for this job rather than climbing driver memory again.

**Interaction with #1195 worth knowing:** now that retries mint fresh batch ids, a retry runs a REAL 12-minute job instead of dying in 6 seconds. Correct behaviour, but a persistent failure now costs roughly 3x the Dataproc spend rather than failing cheap.

**⚠ THE RE-RUN TRAP (cost the most time on 2026-08-15, applies to EVERY DAG of this shape).** After merging a config fix, clearing the Dataproc task does NOT pick it up, and neither does "Run with latest bundle version". Three separate caches, only one of which the bundle version controls:

| Task | Caches in XCom | Clearing it gets you |
|---|---|---|
| `create_batch` | the **whole batch spec incl. `runtime_config`** (driver memory, TTL, spark props) | the new config |
| `create_batch_id` | just the batch id string | a new batch id (unnecessary post-#1195, try_number already does it) |
| `materialize` alone | nothing | the OLD spec, re-submitted |

On 2026-08-15 the dag run WAS correctly on the post-merge bundle (`bundle_version=21:54:01Z`, after the 21:53:28Z merge) and the task still submitted `driver.memory=9600m`, because `create_batch` last ran at 12:45 and its XCom had no `driver.memory` key at all, so Dataproc applied its 9600m default. **Fix = clear `create_batch` WITH downstream** (proven: `create_batch` try 2 at 22:16:32Z → batch reported `16g`/`4g` → 7.1 min → `hh=11` landed 77 objects / 6.15 GiB, matching neighbours). A fresh triggered run with `{"dt":..., "hhs":[...]}` works too, since it rebuilds everything.

**⚠ NEVER clear a task whose batch is still running.** It cancels the in-flight batch, and Airflow records the try as **SUCCESS with no output**. Observed here: batch `...-1786831127-3` went `CANCELLED` at 22:01:24Z while Airflow showed try 3 green in 2:28, and `hh=11` stayed empty. **A materialize success under ~3 min is a lie** (healthy ~7 min); always confirm the `hh=` partition in GCS, never the green tick.

**Decision tree (next `materialize_*` repeated failure):** 1. Compare failed vs healthy batch durations. Constant interval = resource ceiling; variable = data. 2. Get `driveroutput` before theorising (`gcloud pam grants create --entitlement=dataproc-debug --location=global --project=mntn-prj-prod-00`; approval is quick, grant lasts 4h). 3. Reads logged OK + OOM in `map-output-dispatcher` = driver heap vs shuffle partitions. 4. Raise memory on the ONE DAG, never in shared `get_config`. 5. Re-run missing `hh=` only after the bundle refresh (verify the batch reports the new `driver.memory`).

---

---

### INC-019 — `hashed_email_guid_log_signals` + `hashed_email_ds_26_signals` `wait_fpa` — sensor timed out while the producer was merely SLOW; producer succeeded 40s-3min later

**Date:** 2026-08-15/16 · **Alert:** two at 19:16 PT, `🔴 [prod] Airflow Targeting FAILURE [hashed_email_guid_log_signals/wait_fpa]` and `[hashed_email_ds_26_signals/wait_fpa] at 2026-08-15 18:00:00 PT`, `Try 0 of 2: Sensor has timed out; run duration of 958.863484 seconds exceeds the specified timeout of 900.0.` **STATUS: RESOLVED (verdict verified; 4 partitions awaiting a clear at write-up time).**

**Verdict: `late_data`.** Nothing broke. The producer `fpa_site_visit_batch_serverless` for logical `2026-08-16T01:00:00Z` finished **green**, it was just slower than the consumers' 15-minute patience. Timeline (UTC):

| Time | Event |
|---|---|
| 02:00:03 → 02:00:16 | producer `get_dt_hh_by_dsid` runs (12.8s) |
| 02:00:16 → **02:10:27** | **~10-minute gap with nothing scheduled** — this is what blew the budget |
| 02:00:28 | both `wait_fpa` sensors start poking (`mode="reschedule"`) |
| 02:10:27 → 02:11:00 | producer `source_available_dsid*` + `build_partitions` |
| 02:12:51 → 02:19:27 | the six `dsid*_processing` batches run, all success |
| **02:16:27** | **both sensors hit 900s and fail** |
| 02:17:07 | `dsid26_predactiv_processing` succeeds — **40s too late** |
| 02:19:27 | `dsid23_guid_log_processing` succeeds — **3 min too late** |
| 02:19:29 | producer dag run `end` — state **success**, 19.5 min total |

**This is NOT INC-011.** Same task name, opposite mechanism. INC-011 = `ExternalTaskFailedError` fast-fail in ~5s because the external task was `skipped`; fixed by #1175 (`skipped_states`), and that fix is working (one benign `skipped` observed on ds_26 since 08-12, handled correctly). INC-019 = a genuine `AirflowSensorTimeout` after the full 900s. **Read the alert text**: `Sensor has timed out` = this incident; `ExternalTaskFailedError` = INC-011.

**Why 900s is the wrong number.** Producer dag-run durations, 99 runs since 2026-08-12: **median 10.0 min, max 50.2 min, 6 runs (6%) over 15 min** (08-11T23Z 15.6 · 08-13T01Z 15.4 · 08-13T05Z 19.8 · 08-13T06Z 15.2 · 08-13T13Z 50.2 · 08-16T01Z 19.5). The sensor's budget sits ~5 min above the median, so a routine slow hour pages. Not every long run pages: the 50.2-min run on 08-13T13Z did **not** fail `wait_fpa`, because the two watched tasks finished early and a different `dsid*` batch dragged the run out. What matters is the time to *that one* external task, not the dag-run total.

**⚠ A sensor timeout does not retry.** Both DAGs carry `default_args={"retries": 1}`, but `AirflowSensorTimeout` fails the task outright — no try 2 was ever created (`max_tries=1`, try_number=1, `populate_hem_data_*` immediately `upstream_failed`). Airflow's own alert text says `Try 0 of 2`, which reads as "a retry is coming". It is not. Every occurrence needs a human. Same family as INC-016/017's dead retries, different root cause. A retry *would* have worked: it starts a fresh 900s window against an external task that is by then already `success`.

**Recurrence + the hole nobody caught.** `wait_fpa` failures since 2026-08-12, both consumers: **2026-08-13T05:00:00Z** and **2026-08-16T01:00:00Z** (97/99 and 96/99 success otherwise). The 08-13 pair was **never cleared** — its partitions were still empty three days later, found only by this triage. Nothing alerts on a missing hourly partition once the alert scrolls out of Slack.

**⚠ The two consumers write DIFFERENT hours from the same logical date.** `hashed_email_guid_log_signals` renders `data_interval_start.subtract(hours=1)`; `hashed_email_ds_26_signals` renders `subtract(hours=2)`. Both wait on the same producer run. So one failed logical date leaves holes in two different `hh=`, and computing them by eye gets it wrong. Output is `gs://mntn-data-archive-prod/signals/hashed_email_signal/dt=<D>/hh=<H>/data_source_id=<23|26>/`:

| Failed logical run | ds 23 partition (−1h) | ds 26 partition (−2h) |
|---|---|---|
| `2026-08-13T05:00:00Z` | `dt=2026-08-13/hh=04` — MISSING | `dt=2026-08-13/hh=03` — MISSING |
| `2026-08-16T01:00:00Z` | `dt=2026-08-16/hh=00` — MISSING | `dt=2026-08-15/hh=23` — MISSING |

All four sources still on disk at triage time (`fpa_vendor_log/data_source_id=<ds>/dt=<D>/hh=<H>/` = 75 / 40 / 111 / 37 objects), so all four are refillable.

**⚠ The spark job writes `mode="append"`.** Re-running over a partition that already holds data DUPLICATES it rather than replacing it. There is a guard (`Data source id <ds> already populated for <date>`) but do not lean on it — **confirm the partition is empty before clearing**, which for these four it was.

**Action:** clear `wait_fpa` with downstream on both DAGs for `scheduled__2026-08-16T01:00:00+00:00` and `scheduled__2026-08-13T05:00:00+00:00`. The external tasks are long since `success`, so the sensor passes on the first poke and `populate_hem_data_ds_*` fills the partition. Do **not** widen the timeout by hand — that is a code change owned by the producing team (IMP-043).

**Decision tree (next `wait_fpa` failure):**
1. **Read the exception text.** `Sensor has timed out` → INC-019 (this one). `ExternalTaskFailedError` → INC-011 (benign skip, no backfill).
2. **Check the producer's dag-run state** for the same logical date. Green → the sensor was merely impatient, this is `late_data`, nothing is broken.
3. **Both consumers alerting in the same hour** is the tell that it's producer-side slowness, not a per-partner data gap.
4. **Map BOTH holes** off the offsets above (−1h for ds 23, −2h for ds 26) — do not assume they share an `hh`.
5. **Verify each partition is empty**, then clear `wait_fpa` with downstream. Verify `_SUCCESS` lands after.
6. **Sweep backwards.** These do not self-heal and nothing re-alerts, so check the previous few days' `wait_fpa` failures for holes still open.

```bash
bash .claude/scripts/airflow_pull.sh --date <UTC-date> --dag hashed_email_guid_log_signals --all-tries
bash .claude/scripts/airflow_pull.sh --date <UTC-date> --dag fpa_site_visit_batch_serverless   # producer timeline
gcloud storage ls -l "gs://mntn-data-archive-prod/signals/hashed_email_signal/dt=<D>/hh=<H>/data_source_id=23/_SUCCESS"
```

**Durable fix:** IMP-043 — **PR [airflow-ti#1199](https://github.com/SteelHouse/airflow-ti/pull/1199) OPEN 2026-08-16**, raises `wait_fpa`'s `timeout` 900 → 1800 on both DAGs to cover the real producer distribution (a 50-min tail exists; 900s covers ~94% of runs), and/or move the two consumers to the producer's `all_batches_done` boundary. Raising `retries` alone does NOT help: `AirflowSensorTimeout` skips retries entirely. Worth a separate look: the recurring ~10-min gap between `get_dt_hh_by_dsid` and `source_available_*` is scheduling latency inside the producer, not task work.

**Logs:** `on-call/airflow_logs/2026-08-16/021621__hashed_email_guid_log_signals__wait_fpa__try1__failed.log` (+ the ds_26 sibling and the full producer timeline in the same folder).

---

### INC-020 — `site_network_hourly` — GCP 503 on impersonated credentials, task died before submitting anything; self-healed

**Date:** 2026-08-17 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [site_network_hourly/site_network_hourly] at 2026-08-16 23:50:00 PT` (= logical `2026-08-17T06:50:00Z`), `Try 0 of 1: 503 Getting metadata from plugin failed with error: ('Unable to acquire impersonated credentials', '{ "error": { "code": 503, "message": "Unable to extract the resource from the request.", "status": "UNAVAILABLE" }}')`. **STATUS: RESOLVED, no data loss, no action taken.**

**Verdict: `transient_infra`.** A one-off 503 from Google's IAM credentials service while the operator impersonated the job service account. Timeline from the task log, total life **15.9s**:

| Time (UTC) | Event |
|---|---|
| 07:50:06 | DagBag fills, task starts |
| 07:50:13.070 | `ModelPysparkBatchOperator` builds the batch spec |
| 07:50:13.071 | `Starting batch sit-net-hou-fv2-20260817-065000-1` |
| 07:50:13.152 | `Getting connection using google.auth.default()` |
| 07:50:13.945 | `grpc._plugin_wrapping AuthMetadataPluginCallback ... raised exception!` |
| 07:50:14.769 | task failed, Slack alert fired |

**The failure precedes submission, which is what makes it clean.** The batch id was minted and logged but the gRPC call that would have created it never authenticated, so **no Dataproc batch exists** for `sit-net-hou-fv2-20260817-065000-1`. Nothing to delete, no partial write, no batch-id attach trap on a re-run (contrast INC-016/017). Distinguish from a batch that failed *after* creation: check whether the log ever reaches a batch state (`RUNNING`/`Waiting for batch`). If it dies inside ~20s at `google.auth.default()`, nothing was submitted.

**Why no action was needed — the model heals 2 hours per run.** `models/bidstream_hourly/site_network_hourly.py` loops `for delta_h in (2, 1)` off `hour_start`, so a run at logical `H:50` overwrites `hh=H-2` **and** `hh=H-1`. Every hour is therefore written twice by consecutive runs. The lost run would have written `hh=04` and `hh=05` of `dt=2026-08-17`; both were covered by its neighbours:

| Partition | Written at | By logical run |
|---|---|---|
| `hh=04` | 2026-08-17T07:05:01Z | `05:50Z` (success) |
| `hh=05` | 2026-08-17T09:01:28Z | `07:50Z` (success) |
| `hh=06` | 2026-08-17T10:10:05Z | `08:50Z` (success) |

Verified full: `gs://mntn-data-archive-prod/ipdsc_site_network/site_network_hourly/dt=2026-08-17/hh=0{3..8}/` = 32 / 27 / 23 / 23 / 8 / 18 objects on a smooth 369 → 212 MiB curve. **One isolated failure on this DAG is always benign; two consecutive failures are not** (that leaves a real hole in the shared hour).

**⚠ Two things this DAG hides.**
1. **`default_args={}` means `retries` is 0** on a severity-1 hourly job. A 16-second transient pages a human for something a single retry would have swallowed. Logged IMP-044, fixed by **PR [airflow-ti#1202](https://github.com/SteelHouse/airflow-ti/pull/1202) MERGED 2026-08-17** (`retries=2`, `retry_delay=5min`). Safe because the failure precedes submission and `ModelPysparkBatchOperator` already suffixes `try_number` onto the batch id (`include/models/operators.py:213`), so a retry mints a fresh batch.
2. **A green run does not mean the hours landed.** The model wraps each hour in try/except and its last line is `# Job always succeeds so Airflow marks run success; missing hours are picked up next run.` A run whose inputs are missing prints `[site_network_hourly] Skip dt=… hh=…` and still exits 0. Same silent-degrade class as INC-013. Grep the driver output for `Skip dt=` before trusting a green run on this DAG.

**Note the output path.** `location_root` is `gs://mntn-data-archive-prod/ipdsc_site_network`, but the model writes under a `site_network_hourly/` subdirectory the config does not name: `…/ipdsc_site_network/site_network_hourly/dt=<D>/hh=<HH>/`. Listing the config path alone returns "matched no objects" and reads as total data loss.

**Decision tree (next `site_network_hourly` failure):**
1. **Read the task duration.** Under ~20s + `impersonated credentials` / `UNAVAILABLE` = this incident. Nothing was submitted.
2. **Check the neighbouring runs.** `H-1:50` and `H+1:50` green → the two affected hours are already covered, no action.
3. **Verify anyway** at `…/ipdsc_site_network/site_network_hourly/dt=<D>/hh=<HH>/` for the two hours `H-2` and `H-1`. Include the `site_network_hourly/` segment.
4. **Two consecutive failures** → clear the later run; it heals both hours.
5. Do **not** re-run for a single failure, and do not chase the 503 — one occurrence in 15 runs that day, all others green.

```bash
bash .claude/scripts/airflow_pull.sh --date <UTC-date> --dag site_network_hourly --all-tries
gcloud storage ls -l "gs://mntn-data-archive-prod/ipdsc_site_network/site_network_hourly/dt=<D>/hh=<HH>/"
```

**Cross-reference:** `site_network_hourly` is also the DAG whose `spark-events` application was mistaken for a `materialize_mntn_select` batch in INC-012. It is a busy neighbour on the same infrastructure; always confirm `spark.app.name` before attributing an event log to it.

**Logs:** `on-call/airflow_logs/2026-08-17/075001__site_network_hourly__site_network_hourly__try1__failed.log`.

---

### INC-021 — four DAGs, `No exception message found` — worker-loss burst, all self-recovered, and the first payoff from IMP-044

**Date:** 2026-08-19 · **Alerts:** four in one Slack batch at 11:40 PM PT, every one `Try 0 of N` / `No exception message found`: `site_network_hourly/site_network_hourly` (logical 2026-08-18 21:50 PT), `audience_intent/wait_for_ipdsc_geo` and `audience_intent/intent_score_map` (both logical 2026-08-17 17:08 PT), `tpa_ipdsc_export/ipdsc_ds_35` (logical 2026-08-17 19:35 PT). **STATUS: OBSERVED, not resolved — the trigger is platform-side and unverified. No action needed; all four recovered.**

**Verdict: `transient_infra` (mechanism confirmed, trigger NOT).** The four tasks started 2h32m apart and ran for wildly different durations, then **all died within 40 seconds of each other**, across at least three different workers:

| Task | try-1 start | try-1 end | worker | ran for |
|---|---|---|---|---|
| `ipdsc_ds_35` | 04:08:40 | **06:40:51** | 100.64.6.54 | 2h32m |
| `site_network_hourly` | 05:50:01 | **06:40:11** | 100.64.2.3 | 50m |
| `wait_for_ipdsc_geo` | 06:18:06 | **06:40:11** | 100.64.3.18 | 22m |
| `intent_score_map` | 06:18:06 | **06:40:41** | 100.64.2.3 | 22m |

Simultaneous death across **multiple distinct hosts** rules out a single pod eviction and rules out anything task-specific. This was one cluster-level event at **2026-08-19 06:40 UTC** (= 11:40 PM PT, exactly when the Slack batch fired). Three of the four try-1 logs are **completely empty** (2 lines, just the `::group::` markers), which is what you get when the worker dies without flushing — and is also why the alert says `No exception message found`: nothing raised, the process vanished.

**Mechanism CONFIRMED from the UI (2026-08-19, screenshots).** The try-1 logs are not merely empty, they are unreachable: `Could not read served logs: HTTPConnectionPool(host='100.64.6.54', port=39091): Max retries exceeded ... ConnectTimeoutError ... Connection to 100.64.6.54 timed out (connect timeout=5)`, and the same for `100.64.2.3` on `site_network_hourly`. Those hosts are exactly the `hostname` values on the failed tries. **The log server died with the worker**, which is why the API returns a 2-line log and the alert says `No exception message found`. This is the INC-009 signature verbatim. Note the API's log fetch hides this: it returns the empty body without surfacing the `sources=[...]` error that the UI shows, so **the UI log panel is the better surface for this failure class**.

**What is still NOT determined:** what caused the 06:40 termination. It was **not a code deploy** — the most recent `Deploy to Prod` was 2026-08-18T15:15Z, about 15 hours earlier. That leaves an Astronomer platform event (node-pool recycle, maintenance, scheduler restart), which is not visible from the Airflow API. **To close this:** check the Astro UI deployment/cluster events for 2026-08-19 06:40 UTC, or ask Astronomer support. Until then this stays OBSERVED.

**Every one had already recovered before triage finished:**

| DAG / task | Run state at triage | Detail |
|---|---|---|
| `site_network_hourly` | running, **try 2** | retry in flight, alert was try 1 |
| `audience_intent` (both tasks) | running, `failures=none` | recovered |
| `tpa_ipdsc_export/ipdsc_ds_35` | running, `failures=none` | recovered |

**⚠ Not related to [airflow-ti#1196](https://github.com/SteelHouse/airflow-ti/pull/1196), despite `ipdsc_ds_35` living in that DAG.** #1196 touches only `ipdsc`, `ipdsc_geo` and `tpa_export`; `ipdsc_ds_35` is a `ModelPysparkBatchOperator` it never modified. The previous run (`scheduled__2026-08-17T02:35:00+00:00`, the first after the merge) completed **success with zero failed tasks**. Reach for the recent-merge explanation only after checking whether the alerting task is one the PR actually touched.

**The `site_network_hourly` retry only exists because of [airflow-ti#1202](https://github.com/SteelHouse/airflow-ti/pull/1202) (IMP-044), merged 2026-08-17.** That DAG ran `default_args={}` = **retries 0**, so its alerts read `Try 0 of 1` (see INC-020) and any transient was a dead run needing a human. This alert reads **`Try 0 of 3`** and a try 2 was already running unattended. First real-world payoff, two days after merge.

**The obvious durable fix is unavailable, and that is now confirmed.** Deferrable operators would move the wait into the triggerer so a recycled worker could not take a running task with it, and that was the original [airflow-ti#1206](https://github.com/SteelHouse/airflow-ti/pull/1206). **Airflow 3.1.5 cannot resume a `DataprocBatchTrigger`** — the trigger returns the batch state as a proto enum, the triggerer's msgpack comms raises `NotImplementedError`, and the trigger runner dies ([apache/airflow#54836](https://github.com/apache/airflow/issues/54836)). A dev task sat `deferred` over an hour after its batch had already succeeded. Dev and prod both run `runtime_version: 3.1-9`. A clean prod triggerer log is not evidence of health: prod had **zero** deferred task instances, so its triggerer had never been exercised. Detail: memory `reference_airflow_deferrable_broken_3_1`; revisit tracked as IMP-049.

**What #1206 actually shipped instead** (merged 2026-08-20, prod-validated on `scheduled__2026-08-20T01:50:00+00:00`, success try 1 in 14.7 min on bundle `2026-08-20T02:00:01Z`): a cancel guardrail on `ModelPysparkBatchOperator`. Batches now carry `airflow-dag` / `airflow-task` / `airflow-run` labels; each try cancels any live batch matching those labels that it did not create and waits for terminal before submitting. That closes the *consequence* of a worker recycle (the stranded batch, and a retry double-writing the same output path, which we hand-cancelled twice on 2026-08-19), not the recycle itself. **Labels are the join key because the batch-id prefix carries a per-parse random segment** (`sit-net-hou-nld-…` vs `sit-net-hou-j9s-…`), so try 2 cannot derive try 1's id the way #1196's guardrail does.

**Decision tree (next `No exception message found` burst):**
1. **Count the DAGs.** Several unrelated ones in one window = infrastructure. One DAG alone = look at that DAG.
2. **Check state before acting.** `GET /dags/<id>/dagRuns/<run>/taskInstances`; `try_number > 1` or `failures=none` means it is already handling itself. Do not clear a task that is mid-retry.
3. **Only act on tasks that cannot retry** (`retries=0`) or have exhausted them.
4. **Do not blame a recent merge by DAG name alone** — confirm the failing task is one the diff actually touched.
5. Empty `--state failed` day-pulls are expected here: `airflow_pull.sh` filters on current state, so a recovered task no longer appears. Query the dag run directly instead.

**⚠ Method note — do not conclude "worker loss" from the alert text alone.** My first pass at this incident called it resolved on the strength of the `No exception message found` wording. That was an inference, not evidence. The empty logs prove only that nothing was written; the thing that actually established a single shared cause was **comparing try-1 `end_date` and `hostname` across all four tasks**, which is one API call per try:
```
GET /dags/<dag>/dagRuns/<run>/taskInstances/<task>/tries/<n>   -> state, start_date, end_date, hostname
```
Clustered end times + distinct hostnames = one platform event. Spread-out end times on one hostname = that worker. Same end time on one hostname = that pod.

**Logs:** `airflow_pull.sh --state failed` returns nothing for these because it filters on CURRENT state and all four had recovered; query the dag run directly instead.

---

### INC-022 — `mntn_match_incrementals_fetch` `batch_post.taxonomy_vector` — GCP stockout, not a code fault and not our quota

**Date:** 2026-08-19 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [keyword_ddp_reporting/wait_for_product_categorization] at 2026-08-18 08:00:00 PT`, `Some of the external tasks ['batch_post.product_categorization'] in DAG mntn_match_incrementals_fetch failed.` **STATUS: RESOLVED (cause confirmed, fix merged; the run still needs a clear once capacity returns).**

**The alert names the wrong task.** `wait_for_product_categorization` is the messenger. The real failure is two hops upstream: `batch_post.taxonomy_vector` exhausted its tries, so `product_categorization` never ran and went `upstream_failed`. Always walk to the producer before triaging an `ExternalTaskFailedError`.

**Verdict: `transient_infra`, external.** Every try died with `GCP_INSUFFICIENT_CAPACITY` / `ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS` for `c2-standard-8` in `projects/mntn-databricks/zones/us-central1-f`. 11+ tries across ~8 hours, all identical.

**⚠ Stockout vs quota — the team conflated these all morning.** Ours said `ZONE_RESOURCE_POOL_EXHAUSTED`; another team's job the same hour said `Quota 'LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY' exceeded`. Different faults, different owners: a stockout needs node-type fallback (us), a quota needs a raise (devops). Quota utilization on the Databricks project looked healthy, which is itself evidence it was not quota. Brian McAdams flagged that GCP sometimes reports `QUOTA_EXCEEDED` when it is really a stockout, so read the full detail string.

**Two dead ends, recorded so nobody re-walks them.** (a) **Zone** — `zone_id` was already `auto`, so "pin a different zone" was never available. (b) **Retry spacing** — [airflow-ti#1208](https://github.com/SteelHouse/airflow-ti/pull/1208) adds exponential backoff (merged as 4 retries at +10, +20, +40, +45 min) and is worth having, but the shortage outlasted any retry window. Do not expect it to rescue a run.

**Fix: flexible node types — but it took THREE PRs and a manual image deploy, and none of that is obvious.**

1. [shopper_graph#300](https://github.com/SteelHouse/shopper_graph/pull/300) added the two flexibility blocks. **It never ran.** The dbt project is baked into the `steelhousedev/mntn_matched_data_pipeline:gcp-prod` image, and *Deploy dbt to Dockerhub* is `workflow_dispatch`-only; it had last run 2026-06-17. Merging changed nothing until the image was rebuilt. See memory `reference_shopper_graph_deploy`.
2. Once deployed, Databricks rejected the spec in 2.98s: `INVALID_PARAMETER_VALUE: Node type c2d-standard-8 is not supported`. `c2d-*` is absent from this workspace's catalog. [shopper_graph#301](https://github.com/SteelHouse/shopper_graph/pull/301) swapped in supported ids.
3. Rejected again, on a rule the docs bury: alternates must match the preferred node's **local SSD count** (plus x86/ARM class, core count, memory within 90-100%, and HYPERDISK_BALANCED support). `c2-standard-8` carries 2x375 GB local SSD; `c3d`/`n4d-standard-8` carry none. [shopper_graph#302](https://github.com/SteelHouse/shopper_graph/pull/302) landed on worker `n2d-standard-8`, `n2-standard-8` and driver `c3d-standard-4`, `n4d-standard-4`.

**The run that finally succeeded (try 22, 15.9 min) used the PREFERRED `c2-standard-8`** — capacity returned on its own. The fallback list is validated but has never been exercised. Do not claim it fixed this incident.

**Authoritative node list:** `databricks clusters list-node-types -p malachi@mountain.com` (169 entries). The Job Compute policy `001D160AE4052091` leaves `node_type_id` unlimited, so the GCP catalog is the only gate. Field to compare: `node_instance_type.local_disks`.

The workspace-wide toggle (**Compute → Enable auto flexible node types**) was turned ON 2026-08-19 after Alyson Lefkowitz signed off on reliability over marginal cost and Brian McAdams agreed conditional on someone watching costs. It applies to NEW classic compute only. Cost monitoring is still unbuilt: `system.billing` is not readable by our account.

**Decision tree (next `GCP_INSUFFICIENT_CAPACITY`):**
1. **Read the detail string.** `ZONE_RESOURCE_POOL_EXHAUSTED` = stockout. `Quota '<NAME>' exceeded` = devops. They co-occur; do not assume.
2. **Check `zone_id`.** If already `auto`, zone is not the lever.
3. **Do not spam retries** — a stockout can last many hours.
4. **Add or confirm flexible node types** on the model's `job_cluster_config`, or turn on the workspace toggle.
5. **Clear the failed producer task** once capacity returns, then clear the downstream sensor.

**Where things live:** the cluster spec is in `SteelHouse/shopper_graph` → `dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy_vector.yml`. airflow-ti's `DbxDbtOperator` only launches the dbt pod and carries no cluster config. Full detail incl. the cost objection: memory `reference_databricks_stockout_flexible_nodes`.

### INC-023 — `keyword_ddp_reporting` `write_targeted_signal_ds_13` — read a source table while its producer was rebuilding it

**Date:** 2026-08-20 UTC (2026-08-19 PT) · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [keyword_ddp_reporting/write_targeted_signal_ds_13] at 2026-08-18 08:00:00 PT`, `Pod write-targeted-signal-ds-13-... returned a failure.` **STATUS: RESOLVED.**

**Verdict: `resource_contention` (producer/consumer race), not a code fault.** dbt died with `AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view prod.ml.ddp_url_verticals cannot be found`. The table is a dbt **table**-materialized python model, so each rebuild drops and recreates it. `create_ip_verticals / ddp_url_classification` (daily `5 0 * * *`, 25 min to 2h32m) was mid-rebuild; ds_13 read at 00:15 UTC and died at 00:22.

**Why it had never happened before.** `keyword_ddp_reporting` runs at 15:00 UTC, nine hours clear of the 00:05 rebuild. Today's stockout backlog (INC-022) pushed the 2026-08-18T15:00 run into the next night's rebuild window. **There is no cross-DAG dependency between them** — the separation is schedule luck.

**Do not diagnose this from the table name.** `prod.ml.ddp_url_verticals_filtered` exists and looks like a rename; it is a different model. Confirm with `SHOW TABLES IN prod.ml LIKE 'ddp*'` and check the producer's run state before assuming a rename or a dropped table.

**Decision tree (next `TABLE_OR_VIEW_NOT_FOUND` on a dbt source):**
1. `SHOW TABLES IN <catalog>.<schema>` — absent, or just renamed?
2. Find the producing model: `gh search code --owner SteelHouse "<table>"` → the `.yml` in `SteelHouse/dbt`, then the DAG task that runs it.
3. Check that task's state right now. Running = you raced it; wait, do not clear.
4. Clear the consumer only after the producer is green.

**Diagnosis commands:**
```bash
gh search code --owner SteelHouse "ddp_url_verticals" --limit 20 --json repository,path
# producer: create_ip_verticals / ddp_url_classification, dbt model in SteelHouse/dbt
#   ml_squad/models/vertical_categorization/ddp_url_verticals.yml
```

Durable fix logged as IMP-047 (a cross-DAG guard, or a non-destructive materialization).

## 4. System reference (producer → consumer maps as we learn them)

**IPDSC / TPA export chain (team TPA_EXPORT, `airflow-ti`)**
```
Bombora vendor drop ──▶ gs://mntn-data-partners/partners/bombora/segments/<D-1>/   (source, optional)
        │  wait_bombora_src (1h, reschedule, soft_fail)  [tpa_ipdsc_export @ 02:35 UTC]
        ▼
ipdsc_bombora builder ──▶ gs://mntn-data-archive-prod/ipdsc/dt=<D>/data_source_id=51/_SUCCESS
        │                                                   ▲
        ▼                                                   │ polls (18h, hard-fail)  [ipdsc_monitor @ 00:05 UTC]
run_geo ──▶ tpa_export ──▶ external table bucket           precondition_bombora  ← ALERTS here
```
- Mandatory data sources (DS4, DS17, …) are never tolerated — a missing mandatory partition
  hard-fails `tpa_export`. Only optional partners (currently just Bombora/DS51) skip silently.
- `ds17` sources ShareThis at `gs://mntn-data-partners/partners/sharethis/segments/date=<D-1>/`.
- Full DS id → vendor map + ipdsc query tips: `knowledge/data_catalog.md` (`bronze.external.ipdsc__v1`, DS-id legend).
- **`run_geo` also writes `gs://mntn-data-archive-prod/ipdsc_geo/dt=D` (lands D+1, ~05:00–08:17Z, variable).**
  Producer = task `run_geo` (Airflow task_id `ipdsc_geo`, a `DataprocCreateBatchOperator`) in
  `tpa_ipdsc_export.py`, TPA_EXPORT team, schedule `35 2 * * *` (02:35 UTC). It runs LAST — after all
  `ipdsc_ds_*` builders + audience-builder tasks (`trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS`) — and joins
  them into the IP→geo table `external.ipdsc_geo__v1` (`ip, geo_version, location_id[], lat, long,
  accuracy_radius`). Its finish time swings ~3.5h with the builder chain's runtime, so its landing time is
  not fixed. Writes `_SUCCESS` last (complete-partition marker).
  Downstream consumer (different team + schedule, no cross-DAG dep): `audience_intent` `fangorn_score_monitor`
  (targeting, `8 0 * * *`) reads `ipdsc_geo/dt=<run_date>`. Pre-INC-004 it had no sensor → **raced the
  producer** and paged (`AnalysisException [PATH_NOT_FOUND]`) when geo slipped past its ~30-40min retry slack.
  PR #1160 added `wait_for_ipdsc_geo` on the `_SUCCESS` marker so the monitor waits instead of racing.
- **`tpa_mntn_id_export` is the OTHER (synchronous, no-race) `ipdsc`/`ipdsc_geo` consumer** — triggered inside
  the producer DAG (`tpa_ipdsc_export.trigger_mntn_id_export`), so it never races. It reads ~15
  `ipdsc/dt=D/data_source_id=*` partitions + `ipdsc_geo/dt=D` + the identity graph, does a 14-way
  full-outer-join on `ip` + a `groupBy(mntn_id)` window (~54TB shuffle), and writes household-keyed JSONL to
  `gs://sh-dw-external-tables-prod/mntn_id_data/YYYY/MM/DD/` for CG 127075 (TPA activation).
  `ModelPysparkBatchOperator`, hardcoded **3h TTL** + `maxExecutors=150`, baseline ~78 min → **zero TTL
  headroom**; a shuffle-fetch storm tips it over the wall (INC-005).

**Fangorn inference chain (ML pipeline, `airflow-ti` → Vertex/Dataproc)**
```
fangorn_inference_pipeline_run  [Astronomer, PythonOperator: inference_pipeline]
        │  submits Vertex AI pipeline
        ▼
fangorn_inference_dataproc_pipeline  (template in gs://targeting-infra-vertex-pipelines-prod/fangorn/)
        │  step: create-dataproc-cluster  ← INC-002 failed HERE (code 9 = Dataproc saturated by a concurrent job)
        ▼
inference on Dataproc ──▶ Fangorn scores  (project mntn-targeting-prj-prod, region us-central1)
```
- Alerts route via PagerDuty (`pagerduty_events` connection), not just Slack.
- **Dataproc ~94% cap:** a Fangorn(-like) inference run saturates Dataproc; a concurrent Dataproc job (even
  QA / a challenger) starves `create-dataproc-cluster` → code 9. Fix = let the other job finish, then
  re-trigger the champion. Never two Fangorn-like inference runs on Dataproc at once. (INC-002)
- The Vertex pipeline template + Dataproc config live in **`targeting-infra`** (not `airflow-ti`); a
  config regression is routed there. `airflow-ti` only *submits* the pipeline.
- **DAG shape** (`dags/machine_learning/fangorn_inference_pipeline_run.py`, team TPA_EXPORT, `0 18 * * *`,
  severity 0, PagerDuty on failure): `wait_for_features >> inference_pipeline >> challenger_inference_pipeline >> daily_drift_pipeline`.
  All three pipeline tasks use `TiVertexPipelineOperator`, which ALWAYS injects `reference_date` into the
  Vertex `parameter_values`. Each submitted template (`fangorn_inference_dataproc_pipeline`,
  `fangorn_challenger_inference_pipeline`, `fangorn_daily_feature_drift_pipeline`) MUST declare
  `reference_date` in its `inputDefinitions` or the task hard-fails at exec with
  `ValueError: … parameter reference_date not found …` (INC-003 — drift template declared `run_date`).
- Fangorn context: see `[[fangorn_tier_assignment]]`, `[[fangorn_two_model_passes]]`, `[[fangorn_detection]]` in memory.

**MNTN Matched keyword pipeline (DS13/DS19, OpenAI Batch API, team ML, `airflow-ti`)** — hit by INC-006 + INC-007
```
mntn_match_incrementals_submit  [0 9 * * *, sev 1]
  batch_prep(dbt) >> batch_validate >> batch_submit(openai_batch_runner pod)
    └─ client.files.create(input) + create OpenAI batch ──▶ gs://…/shopper_graph/openai_batch_submissions/dt=<L>
        ⚠ INC-007 failed HERE: OpenAI 400 "file storage quota (2.5TB) exceeded"  → no submission written
            │ (OpenAI Batch API runs async, up to 24h SLA)
            ▼  next day
mntn_match_incrementals_fetch  [0 9 * * *, sev 5]
  batch_transition >> batch_fetch(openai_batch_runner: fetch_results.py) ──▶ openai_batch_results/dt=<L>
     ⚠ INC-006 failed HERE: download_file loop-abort on a null-output errored batch (fix shopper_graph#296)
  >> batch_post(dbt): openai_batch_joined ──▶ results_joined/dt >> categorization_temp >> mm_taxonomy_update
     >> product_categorization ──▶ product_categorization/dt=<L−1>  (+ mm_taxonomy_update_bq ──▶ BQ)
            │  (ExternalTaskSensor, execution_delta 6h, mode=reschedule, failed_states incl upstream_failed via #1162)
            ▼
keyword_ddp_reporting  [0 15 * * *, sev 5]  wait_for_product_categorization ← ALERTS here (INC-006 timeout / INC-007 fast-fail)
  >> write_targeted_signal_ds_19 >> ds_13 >> ds_19_domain  (DS19 keyword DDP report; no serving path)
downstream of product_categorization: tpa_export, audience_sizes, mntn_matched_taxonomy_bq
```
- **dt convention:** all stages share one `dt`; `product_categorization/dt=D` is fed by `submissions/dt=D`. A fetch-run logical `L` produces `product_categorization/dt=L−1` (and submits `dt=L`). **A missing `submissions/dt` ⇒ the SUBMIT failed, not the fetch** (INC-007 diagnostic).
- **Where each incident's failure lives:** submit-side OpenAI file quota = INC-007 (durable fix IMP-013); fetch-side download loop-abort = INC-006 (fix #296); downstream sensor hygiene = PR #1162 (fail-fast). The keyword_ddp sensor is only ever a *lagging* symptom of an upstream break.
- Alerts are Slack-only (sev 1/5, no PagerDuty); an upstream failure surfaces on-call ~a day later via keyword_ddp. Direct upstream alerting / data-aware scheduling = IMP-009.

**FPA site-visit batch → hashed-email/GUID signal consumers (team TGT/targeting, `airflow-ti`)** — hit by INC-011
```
partner drops ──▶ gs://mntn-data-partners/partners/<vendor>/dt=<YYYYMMDDHH>/   (hourly, per source)
fpa_site_visit_batch_serverless  [@hourly]
  source_available_dsid<N>  (_ShortCircuitDecoratedOperator)  ← checks the partner's hourly GCS prefix
     │  present → True                         │  absent → False → SKIP (producer DAG still SUCCEEDS)
     ▼                                         ▼
  dsid<N>_processing  (DataprocCreateBatchOperator) ──▶ site_visit_signal/…/data_source_id=<N>
     (sources seen live in one run: dsid 26/25/23/28/30/36)
            │  (ExternalTaskSensor wait_fpa, SAME logical date, no execution_delta,
            │   timeout=900, mode=reschedule, check_existence=True)
            ▼
  hashed_email_ds_26_signals   wait_fpa → dsid26_predactiv_processing  ← ALERTS here (INC-011, INC-019)
  hashed_email_guid_log_signals wait_fpa → dsid23_guid_log_processing  (sibling, same latent bug)
```
- Each source is gated: `source_available_dsid<N>` short-circuits the hour when the partner's hourly prefix is
  absent (`No source data for dsid=<N> …`) → its `dsid<N>_processing` is **SKIPPED** while the producer DAG
  still SUCCEEDS. A missing hourly partner file is a routine benign event, not a break.
- **Only 3 ExternalTaskSensors exist in `airflow-ti` `dags/`:** these two `wait_fpa` sensors + `keyword_ddp_reporting`'s
  `wait_for_product_categorization` (INC-006/007). The `wait_fpa` pair polls the SAME logical date (no
  `execution_delta`); `keyword_ddp` uses a 6h delta.
- **Skip-as-failure trap (INC-011):** `wait_fpa`'s `failed_states` counts a `skipped` external task as a failure →
  a benign partner-data gap pages prod. Durable fix MERGED (airflow-ti#1175, AUDI-1195): `skipped` moved to
  `skipped_states` on both `wait_fpa` DAGs; `keyword_ddp` deliberately excluded (its skip is a real break). See §3 INC-011.
- **Producer-slowness trap (INC-019):** `wait_fpa` carries `timeout=900` on both consumers, but the producer dag run
  has a median of 10.0 min and a 6% tail over 15 min (max 50.2 min observed). A slow-but-successful producer times the
  sensors out; `AirflowSensorTimeout` does **not** retry despite `retries: 1`, so it is always a manual clear. IMP-043.
- **The two consumers write DIFFERENT hours from one logical date:** ds 23 renders `data_interval_start.subtract(hours=1)`,
  ds 26 renders `subtract(hours=2)`. One failed run leaves two holes in two different `hh=`. Output
  `gs://mntn-data-archive-prod/signals/hashed_email_signal/dt=<D>/hh=<H>/data_source_id=<23|26>/`, written `mode="append"`
  (a re-run over a non-empty partition duplicates). See §3 INC-019.

---

## 5. Structured incident log (`on-call/incident_log.jsonl`)

Append-only JSONL, one record per incident — the machine-readable index over §3 (mirrors the perf/request
logs). Lets you answer "how often does `precondition_bombora` page?" or "which DAG pages most?" without
reading the prose. **Write one record every time you add an INC to §3.**

Record shape (one line per incident):
```json
{"inc":"INC-001","date":"2026-07-28","dag":"ipdsc_monitor","task":"precondition_bombora","team":"TPA_EXPORT","signature":"AirflowSensorTimeout 18h optional-partner skip","verdict":"benign_expected","action":"ack_no_rerun","resolved":true,"ticket":null,"ref":"§3 INC-001"}
```
Fields: `inc` · `date` (YYYY-MM-DD) · `dag` · `task` · `team` · `signature` (short) · `verdict`
(`benign_expected|late_data|transient_infra|resource_contention|real_upstream_failure|dag_bug`) · `action`
(`ack_no_rerun|clear_task|rerun|force_export|routed_owner|spawned_ticket`) · `resolved` (bool) ·
`ticket` (TI/AUDI key if a durable fix was spun out, else null) · `pagerduty` (PD incident # if it paged, else null) ·
`ref` (`§3 INC-NNN`). **When the user gives a PagerDuty incident #, record it in `pagerduty` and cite it in the §3 alert line** — it ties our INC-NNN to the PD record.

### INC-024 — `fangorn_hhid_inference_pipeline_run` `challenger_inference_pipeline` — the challenger model alias vanished when the hhid model was re-registered

**2026-08-20** · PagerDuty · `[prod] Airflow Targeting FAILURE [fangorn_hhid_inference_pipeline_run/challenger_inference_pipeline]`, logical date 2026-08-19 18:00Z, both tries failed (try 2 ran 22:34:44-22:53:33Z, 18m49s). **STATUS: RESOLVED (cause verified, fix is owner-side).**

**Verdict: `dag_bug` — a registry regression, not infra.** `run_challenger_inference.py:35` resolves the model by alias **pattern `challenger-v*`**. `fangorn-hhid-xgboost` has exactly one version (v1) whose aliases are `['default', 'champion']` — no challenger alias at all — so the resolve raises before any inference runs.

**Timeline that pins it:** hhid challenger SUCCEEDED 2026-08-12 and 2026-08-17. The model's `updateTime` is **2026-08-18**. The next challenger run (2026-08-20) failed, and every try since has failed identically. The sibling `fangorn-xgboost` still carries `challenger-v2`, which is why the **non-hhid** challenger succeeded at 21:25Z the same day. So the alias was dropped by the 8/18 re-registration; the code and the pattern are correct and unchanged.

**Why the alert is useless on its own.** The Airflow log is pure Vertex `code: 9` boilerplate naming only the failed Vertex STEP (`submit-parallel-inference-jobs`). The cause is four layers down. The chain:

```bash
# 1. Airflow log -> the debugger names the class, not the cause (this signature shipped 2026-08-20)
python3 -m airflow_debugger.orchestrate <log> --no-llm      # -> [high] vertex/pipeline-task-failed

# 2. the numeric job_id in the Airflow text is NOT the pipelineJobs id - list to get the real name
TOK=$(gcloud auth print-access-token)
curl -s -H "Authorization: Bearer $TOK" \
  "https://us-central1-aiplatform.googleapis.com/v1/projects/401977096985/locations/us-central1/pipelineJobs?pageSize=15&orderBy=create_time%20desc"

# 3. the pipeline job's taskDetails name the failing component + its ml_job id
curl -s -H "Authorization: Bearer $TOK" \
  ".../pipelineJobs/fangorn-hhid-challenger-inference-pipeline-20260820223456"
#    create-dataproc-cluster SUCCEEDED | submit-parallel-inference-jobs FAILED (replica exited 1)

# 4. the replica log names the Dataproc job that failed 3 times
gcloud logging read 'resource.type="ml_job" AND resource.labels.job_id="4870005426285969408" AND severity>=ERROR' \
  --project mntn-targeting-prj-prod --limit 25 --freshness=2d

# 5. that Dataproc job's driver output holds the actual exception
gcloud dataproc jobs describe ce5eb0d1-37e2-4bac-bf6e-756caf530dd4 --region us-central1 \
  --project mntn-targeting-prj-prod --format='value(driverOutputResourceUri)'
gsutil -o "GSUtil:check_hashes=never" cat '<uri>.*'
#    ValueError: No version found with alias pattern 'challenger-v*' for model 'fangorn-hhid-xgboost'

# 6. confirm against the registry, and against the sibling model
curl -s -H "Authorization: Bearer $TOK" \
  ".../models?filter=display_name%3D%22fangorn-hhid-xgboost%22"   # aliases ['default','champion']
#    fangorn-xgboost -> ['default','challenger-v2']   <- the working convention
```

**Decision tree for next time (any fangorn `code: 9`):**
1. Read the Vertex `taskDetails`. **`create-dataproc-cluster` FAILED** → capacity/stockout, INC-002 / INC-008. **SUCCEEDED** → the cluster was fine; keep going down.
2. `submit-parallel-inference-jobs` FAILED → the cause is in the ml_job replica log, then in the Dataproc job it names. Do not stop at the code-9 text.
3. `ValueError: No version found with alias pattern` → a Model Registry alias is missing. **Compare against the sibling model** (`fangorn-xgboost` vs `fangorn-hhid-xgboost`) and check the model's `updateTime` against the last green run. A re-registration that drops an alias looks exactly like this.
4. **Never re-run this class.** It is deterministic and burns a Dataproc cluster per attempt (cluster create + delete both ran on every try here).

**Action taken:** routed to the model owner; no re-run. Durable fix + the missing guardrail: **IMP-056**.

**Owner confirmed same day (2026-08-20 16:06 PT):** Brian McAdams took the fix and said "thought this might happen" — so dropping the challenger alias on a re-registration is a **known, anticipated failure mode**, not a surprise. That raises IMP-056 from a nice-to-have to the real ask: the guardrail (re-apply aliases on re-register, or check for `challenger-v*` before provisioning) is what stops it recurring, since the manual fix is already understood and will be needed again.

**Note on `fetch-advertisers` SKIPPED:** expected — the component is conditional and the challenger path skips it. Not part of this failure.
