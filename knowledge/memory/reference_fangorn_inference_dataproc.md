---
name: reference_fangorn_inference_dataproc
description: "Fangorn inference DAG + Dataproc sizing: champion inference_pipeline is UPSTREAM of challenger (sequential, never concurrent); each cluster is 290 workers ≈ 93% of the us-central1 N2_CPUS quota, so it fails on zonal stockouts and quota self-blocks. Plus (INC-015): sensor/drift wiring (LOOKBACK_DAYS=3, no existence guard) and the Vertex→Dataproc driver-output debugging path"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [fangorn_inference_pipeline_run, inference_pipeline, challenger_inference_pipeline, daily_drift_pipeline, wait_for_features, wait_for_challenger_features, champion upstream of challenger, fangorn dataproc cluster, 290 workers, N2_CPUS quota, DISKS_TOTAL_GB, us-central1 stockout, mntn-targeting-prj-prod, create-dataproc-cluster, vertex pipeline, fangorn_inference_dataproc_pipeline, INC-008, IMP-015, 94% cap, 93% quota, INC-015, run_daily_feature_drift, LOOKBACK_DAYS, drift lookback window, feature drift pipeline, guid_log_pivot_ip_vertical_id sensor, challenger sensor 18h timeout, vertex job_id not resource id, pipelineJobs list orderBy create_time, driverOutputResourceUri, dataproc driver output gsutil, targeting-infra-ml drift, fangorn-daily-feature-drift-pipeline]
domain: [infra]
lifecycle: active
last_verified: 2026-08-09
---
How the Fangorn inference pipeline is wired + why its Dataproc creates fail (from on-call INC-008, 2026-07-30).

**DAG `fangorn_inference_pipeline_run`** (team Fangorn/ML, project `mntn-targeting-prj-prod`, region `us-central1`; owner Brian McAdams + ML/infra). Task graph — the champion and challenger are **sequential, NOT parallel**:
```
wait_for_features           → inference_pipeline (CHAMPION) ─┐
wait_for_challenger_features → ───────────────────────────── challenger_inference_pipeline → daily_drift_pipeline
```
**`inference_pipeline` (champion) is UPSTREAM of `challenger_inference_pipeline`** — they run one-after-the-other and **never hold Dataproc clusters at the same time.** So a champion create-failure is NEVER caused by challenger contention (this refuted my first INC-008 verdict). Each task submits a Vertex AI pipeline (`fangorn_inference_dataproc_pipeline`, template in `gs://targeting-infra-vertex-pipelines-prod/fangorn/...`) whose `create-dataproc-cluster` step spins up a real Dataproc **cluster** (not serverless batch): `fangorn-inference-*`, `fangorn-challenger-*`, `fangorn-daily-drift-*`. Template lives in `targeting-infra`, not `airflow-ti`.

**Cluster sizing (the key number):** inference/challenger clusters are **290 workers ≈ 4,672 N2_CPUS (~93% of the project's 5,000 us-central1 N2_CPUS quota) + ~145,500 GB disk** (n2-standard-16, ~500 GB/node). `daily-drift` is small (~4 workers). The us-central1 quota for `mntn-targeting-prj-prod`: **N2_CPUS 5,000 · CPUS 5,000 · DISKS_TOTAL_GB 225,280 · SSD 81,920.** That ~93% ratio IS the "~94% Dataproc cap" referenced since INC-002.

**Why it fails (`create-dataproc-cluster` → RuntimeError code 9):** at ~93% of quota the cluster has no headroom, so it fails on: (1) **transient external GCP zonal STOCKOUT** — GCP out of large instances in the autozone-picked zone (`code 14 UNAVAILABLE, COMPUTE_ENGINE`); self-recovers in ~1–2h (INC-008 root cause, owner-confirmed); (2) **quota SELF-BLOCK** — a stockout-failed create leaves ~250 partial workers (~4,016 N2) running until teardown ~45 min later, so the retry hits `Insufficient N2_CPUS quota`; (3) real concurrent-job contention (INC-002, a manual/QA job — NOT the challenger).

**DAG wiring detail (INC-015, 2026-08-09):** runs **18:00Z daily**; `run_date`/`reference_date` = `ds(data_interval_end)`. `wait_for_challenger_features` pokes `feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=<that ds>/_SUCCESS` with an **18h timeout**; `wait_for_features` pokes matt.brorby's xgboost training data in `mntn-data-archive-dev`. The producer of that FS partition is airflow-ti `feature_store_setup_model` (schedule/`dt` offset/catchup gotchas: [[reference_airflow_ti]]). **Drift job:** `targeting-infra-ml` `vertex/fangorn/dataproc/run_daily_feature_drift.py` — (superseded 2026-08-10: pre-#85 it read `LOOKBACK_DAYS=3` literal `dt=` paths with NO existence guard, so one missing FS day failed every daily run until the window cleared the hole). **Fixed by [targeting-infra-ml#85](https://github.com/SteelHouse/targeting-infra-ml/pull/85)** (merged 2026-08-10, Brian): `get_latest_paths()` GCS-lists existing `dt=` partitions ≤ run_date and takes the latest 3, so a hole slides the window (drift silently computes on older days). **A missing FS day now fails only day 1** (ref-day `wait_for_challenger_features` 18h sensor timeout + challenger), not the drift tail. Champion baseline comes from `targeting-infra-vertex-pipelines-prod`. Incident detail: runbook §3 INC-015.

**Vertex → Dataproc debugging path (proven end-to-end on INC-015):** the numeric job_id in Airflow's Vertex error text is **NOT** the `pipelineJobs` resource id (GET on it 404s) — list `pipelineJobs?orderBy=create_time desc` for the real string names (`fangorn-daily-feature-drift-pipeline-<ts>`). The failing component's replica error names a **Dataproc job in `mntn-targeting-prj-prod`**; `gcloud dataproc jobs describe <id>` → `driverOutputResourceUri` → `gsutil cat` works with plain user creds — that project's staging bucket is NOT PAM-gated (unlike `mntn-prj-prod-00`'s).

**Diagnose by pulling ALL 3 surfaces + reconciling** (one underdetermines it): `gcloud dataproc operations describe <failed-create> --format="value(error)"` (zonal), the Vertex `service`/worker-pool log (quota), and `gcloud compute regions describe us-central1` quota-vs-usage. Delete any lingering ERROR cluster before retrying. Durable fix = **IMP-015** (raise N2/DISK quota, auto-delete failed clusters, multi-zone, or a smaller cluster). Full incident: on-call runbook §3 INC-008.

Related: [[reference_oncall_runbook]], [[reference_dataproc_eventlog_profiling]], [[feedback_hold_evidenced_verdict]], [[feedback_dataproc_cost_awareness]].
