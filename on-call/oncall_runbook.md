---
doc_type: runbook
title: On-Call Runbook — Master
summary: "Read FIRST on any Airflow/pager/pipeline alert. Triage protocol, alert catalog (signature→verdict→protocol), incident log, producer→consumer maps. Every resolution appends back here."
last_verified: 2026-07-30
keywords: [on-call, oncall, on call, incident, pager, pagerduty, alert triage, airflow failure, airflow alert, pipeline failure, dag failure, task failed, sensor timeout, AirflowSensorTimeout, precondition_bombora, ipdsc_monitor, tpa_ipdsc_export, ipdsc, bombora, DS51, optional partner skip, fangorn_inference_pipeline, inference_pipeline, create-dataproc-cluster, dataproc, dataproc saturation, resource contention, champion challenger, 94% cap, vertex pipeline, INC-008, zonal stockout, us-central1-a stockout, code 14 UNAVAILABLE, COMPUTE_ENGINE stockout, zone does not have enough resources, does not have enough resources, dataproc create failed, cluster create failed, challenger upstream of inference, overlap test, discriminating test dataproc, IMP-015, multi-zone dataproc, fallback zone, N2_CPUS quota, DISKS_TOTAL_GB quota, insufficient quota, insufficient N2_CPUS quota, compute quota ceiling, quota exhaustion, 94% cap, 93% quota, workerpool0-0, self-block retry, lingering ERROR cluster, failed cluster not cleaned, gcloud compute regions describe, quota vs usage, 290 workers, benign expected, late data, batch-id trap, force_export, prod safety, escalation, runbook, daily_drift_pipeline, feature drift, fangorn_daily_feature_drift_pipeline, reference_date, run_date, parameter not found, input definitions, ValueError, param mismatch, param contract, TiVertexPipelineOperator, PipelineJob, latest bundled version, audience_intent, fangorn_score_monitor, ipdsc_geo, ModelPysparkBatchOperator, dataproc serverless, dataproc batch, batches wait, driver output, AnalysisException, PATH_NOT_FOUND, path does not exist, producer consumer race, PAM, privileged access manager, storage.objects.get, INC-001, INC-002, INC-003, INC-004, enriched_impressions, analytics_curated, bombora skip downstream, ds51 zero, ds51 disappeared, tpa_mntn_id_export, mntn id export, mntn_id_data, tpa export, ttl exceeded, batch cancelled, batch was cancelled, cancelling batch as ttl exceeded, dataproc serverless ttl, ModelPysparkBatchOperator ttl, FetchFailedException, FetchFailed storm, shuffle fetch, shuffle fetch failure, auth bootstrap timeout, doSparkAuth, SettableFuture timeout, maxExecutors, zero ttl headroom, sh-dw-external-tables, INC-005, recomputation spiral, uncached lineage, shuffle spill, memory bytes spilled, disk spill, spark.sql.shuffle.partitions, shuffle partitions too few, spark event log, eventlog profiler, cache mntn_df, persist dataframe, dataproc temp bucket, spark-job-history, zstd event log, gcloud-crc32c gatekeeper, storage api download, keyword_ddp_reporting, wait_for_product_categorization, product_categorization, mntn_match_incrementals_fetch, ExternalTaskSensor, external task sensor timeout, execution_delta, allowed_states, openai batch, openai batch runner, batch_fetch, batch_transition, shopper_graph, mntn matched, DS19 keyword pipeline, reschedule sensor, INC-006, INC-007, mntn_match_incrementals_submit, batch_submit, openai file storage quota, 2.5TB file quota, file storage quota exceeded, client.files.create, openai batch quota, exceeded your file storage quota, ExternalTaskFailedError, sensor fast-fail, upstream_failed, batch_submitter, openai_batch_submissions, submit dag failed, product categorization missing, batch_transition, FileNotFoundError, batch_transitioner, batch_cleanup, openai file cleanup, AUDI-1042, openai storage quota ticket, Victor Savitskiy departed, 30-day IPDSC lookback, Ryan Kleck, mntn matched keyword pipeline owner, deploy_openai_dockerhub_gcp, deploy_middleware_dockerhub, openai_batch_runner image, DbtImageName, OPEN_AI_BATCH, image_pull_policy Always, shopper_graph deploy, middleware deploy wrong image, workflow_dispatch, merge is not shipping, mntn-argocd, which image which workflow, auto_paging_iter, SyncCursorPage, openai sdk pagination, batch_cleanup crash, delete_all_storage_files, shopper_graph#297 regression, shopper_graph#298, shopper_graph#299, cleanup regression, files.list pagination, dt=2026-07-28 backfill, write_targeted_signal_ds_19, write_targeted_signal_ds_13, write_targeted_signal_ds_19_domain, targeted_signal, targeted_signal_ds_19, mntn_matched_reporting, DbxDbtOperator, KubernetesPodOperator eviction, pod evicted mid-run, pod not found, not found during istio check, ApiException 404 pods not found, Could not read served logs, served logs timeout, connect timeout 39091, No exception message found, sources empty, reusing existing pod, reattach pod, generic_dbt_runner_ml, prod_warehouse_2xs, dbt python model, dbt python table model, databricks jobs run, run_id 65237255325756, cluster autoscaler scale-down, node preemption, safe-to-evict, do-not-disrupt, await_pod_completion, keyword_ddp downstream dbt task, INC-009, IMP-018, ExecutorLostFailure, spot instance preemption, spot instance kill, spot preemption, executor lost, spill to disk, spark executor memory, bump executor ram, on-demand fallback, PREEMPTIBLE_WITH_FALLBACK_GCP, first_on_demand, gcp autoscaling change, long-running dag killed too long, brian gcp fix, vendor payments ddp, give money to vendors, databricks job compute, all-purpose cluster, sql warehouse dbt, spark history server retention, data-ing-ai, agentic oncall triage, IMP-021, targeted_signal databricks spot]
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

**STATUS: OBSERVED — root cause CONFIRMED (K8s pod evicted mid-run), resolution PENDING two owner checks:** (1) did Databricks job `run_id=65237255325756` complete + write `mntn_matched_reporting.targeted_signal` (ds_19)?, (2) a protected re-run (or accept the one-cycle gap). **NOT a dbt/SQL error, NOT a data problem, NOT the OpenAI/shopper_graph image path.** This is a DIFFERENT task from INC-006/007 (those = the `wait_for_product_categorization` sensor upstream; here the sensor is GREEN and the downstream dbt task failed).

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
databricks jobs get-run <RUN_ID> -o json   # state.result_state = SUCCESS/FAILED (needs databricks OAuth)
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

**Update 2026-07-31 (ROOT CAUSE CONFIRMED = SPOT INSTANCE PREEMPTION; Ryan Kleck picked it up + shared the Spark UI):** the discriminating test resolves to **the Databricks Spark job's executors being killed by spot preemption** — the fused (a)+(b): the *job* fails, and the mechanism is *infra reclaim*. Ryan's Spark task UI shows a wall of `FAILED … ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: spot instance preemption, spot instance kill` (executor 2 / host 10.52.0.158) with **heavy per-task spill (4.9 GiB memory / 1.2-1.3 GiB disk on ~75-108 MiB input)**. Mechanism: the job cluster runs on **spot/preemptible instances**; executors get reclaimed mid-stage → tasks fail → Spark re-runs them (spilling ~5 GiB each) → the job thrashes and never completes in the pod's ~50-min window → the reproducible hang-then-fail. The Airflow KPO pod's own try-1 log-server loss / try-2 404 are the SAME spot-reclaim wave hitting the pod's node (same spot pool). NOT a data/logic bug (input partition healthy). **Chronic config issue** (pure-spot cluster + no on-demand fallback + heavy spill), not a passing blip → verdict `transient_infra` (external spot reclaim) but the point is the durable fix. **Ownership (corrected, supersedes the PARKED note): WE own the fix (Malachi); Ryan Kleck is ADVISING, not on it.** Ryan's immediate call = "bump the RAM for now" (cuts the spill) — but RAM alone does NOT stop the spot kill, so pair it with getting the cluster off pure spot (see fix #1). Config lives in the python model's job-cluster spec (dbt-ml repo) or, if it targets an all-purpose cluster, that cluster's Databricks config.

**Durable fix (this SUPERSEDES the earlier pod-annotation-only framing of IMP-018 — the fix is Databricks-cluster-side):**
1. **Take the Databricks job cluster off pure spot** — driver + a floor of executors on **on-demand**, or Databricks **spot-with-fallback-to-on-demand** (`gcp_attributes` availability / cluster policy), so a preemption wave can't kill the whole job. Direct fix.
2. **Cut the spill** — 4.9 GiB memory-spill/task on ~90 MiB input = under-partitioned / memory-starved Spark (same class as INC-005's shuffle spill on `tpa_mntn_id_export`): raise `spark.sql.shuffle.partitions` / executor memory so each task is cheaper and each preemption costs less recompute.
3. **Observability (Ryan's 2nd issue):** the job surfaces no Spark logs by default; `spark.eventLog.enabled=true` → GCS gives them but disables the live Spark History Server. That tradeoff is fine for post-mortems — parse the GCS event log offline (the INC-005 `eventlog_profiler.py` pattern), which is the better tool for a spill/preemption profile anyway.

**Meeting 2026-07-31 (Malachi + Ryan Kleck, 30 min — transcript `incidents/INC-009/meetings/inc009_01_ryan_spot_preemption_2026_07_31.txt`) — added facts:**
- **Real TRIGGER = a GCP autoscaling change made 2026-07-30 (Brian's team), ALREADY being fixed by Brian.** Ryan: "they did some change yesterday … causing certain long-running DAGs to fail … 'nope, it's been too long'." The ~50-min-then-fail is NEW behavior from that infra change, not a standing config. **Confirm with Brian whether his GCP fix covers THIS job before investing in a job-cluster code change** — it may resolve the preemption/timeout at the source, making our IMP-018 change belt-and-suspenders.
- **Impact is HIGHER than the sev-5 label — per Malachi (owns the business context):** "this is the one where we give money to our vendors" (Ryan noted Victor normally owned it). DS19 keyword DDP feeds a **vendor payment / reconciliation** path, not just an internal report → a multi-day gap is NOT low-impact. This qualifies the runbook §4 "no serving path" note for keyword_ddp; confirm the exact consumer + $ cadence.
- **The FAILED spot-preemption rows are NORMAL Spark retry, not app bugs** (converges with Cursor's read + mine): don't debug them unless the SAME stage keeps failing after retries with a **non-preemption** error. Cheapest first attempt = raise Spark/Databricks task max-retries + bump RAM so preemptions don't exhaust the stage; on-demand is the reliable fix.
- **Access is the standing blocker:** Malachi had fewer Databricks perms than Ryan/Brian ("producers/dev/users" vs Brian "admins/users"); Ryan granted **temp admin** so the Spark UI was reachable. Permanent programmatic access (API token/MCP) is gone (vault/security policy — same class as the decommissioned Slack bot).
- **Owner reality:** Victor Savitskiy departed → pipeline under-owned; Sean Yang is a fallback. **We (Malachi) drive the fix; Ryan + Brian advise.**
- **Current run:** by end of meeting "looks like it's running" (Spark grinding through preemptions via retry). Watch to green.

**Exact config located (GitHub, read-only) — `SteelHouse/dbt` → `ml_squad/models/reporting/targeted_signal_ds_19.yml`** (+ `_ds_13`, `_ds_19_domain`, same pattern; `alias: "targeted_signal"` → why the relation is `mntn_matched_reporting.targeted_signal`; `submission_method: job_cluster`). Current `job_cluster_config`: `driver_node_type_id: c3d-standard-4` (16 GB), `node_type_id: c3d-standard-8` (32 GB), `autoscale min/max = 6/6`, `gcp_attributes.availability: PREEMPTIBLE_WITH_FALLBACK_GCP`, **`first_on_demand: 1`**, and **NO `spark_conf`** (executor memory = node default). The two knobs:
1. **RAM (Ryan's "+4 GB"):** no `spark_conf` today and no clean "+4 GB" node — the effective move is worker `node_type_id` `c3d-standard-8` (32 GB) → `c3d-highmem-8` (64 GB), and/or add a `spark_conf` (mirror `ml_squad/models/audience_intent/prospecting_intent.yml`: `spark.executor.memory` + `spark.sql.shuffle.partitions: auto`) to cut the 4.9 GB/task spill.
2. **Spot kill (the actual failure):** `first_on_demand: 1` = only the driver on-demand; all 6 workers spot → `ExecutorLostFailure spot instance kill`. `PREEMPTIBLE_WITH_FALLBACK_GCP` fallback applies only at LAUNCH, not mid-run reclaim, so workers still die. Raise `first_on_demand` to 3 (half) or 6 (all on-demand, most reliable) so a preemption wave can't kill the job. **RAM alone won't stop this.**
**Do NOT edit from the on-call box** — `SteelHouse/dbt` is a prod repo we don't own (prod-safety); hand the diff to the owner or do it in Cursor. Deploy = a rebuild of the `generic_dbt_runner_ml` image (confirm the workflow before merging).

**Logs:** `on-call/incidents/INC-009/` — try-2 full (`…try2_full_pod404-eviction.txt`: the Databricks `run_id` + the (404) eviction) + a try-2 reattach snapshot. (Try-1's 2-line `Could not read served logs` timeout was captured in-session; that file was overwritten with the try-2 full log before archival.)

---

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
