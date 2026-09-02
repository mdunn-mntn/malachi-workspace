---
name: reference_fangorn_inference_dataproc
description: "Fangorn inference DAG + Dataproc sizing: champion inference_pipeline is UPSTREAM of challenger (sequential, never concurrent); each cluster is 290 workers ≈ 93% of the us-central1 N2_CPUS quota, so it fails on zonal stockouts and quota self-blocks. Plus (INC-015): sensor/drift wiring (LOOKBACK_DAYS=3, no existence guard) and the Vertex→Dataproc driver-output debugging path"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [fangorn_hhid_inference_pipeline_run, challenger model alias, challenger-v alias missing, resolve_model_uri, vertex model registry, versionAliases, No version found with alias pattern, fangorn-hhid-xgboost, re-registration drops aliases, submit-parallel-inference-jobs, ml_job replica exited, INC-024, fangorn_inference_pipeline_run, inference_pipeline, challenger_inference_pipeline, daily_drift_pipeline, wait_for_features, wait_for_challenger_features, champion upstream of challenger, fangorn dataproc cluster, 290 workers, N2_CPUS quota, DISKS_TOTAL_GB, us-central1 stockout, mntn-targeting-prj-prod, create-dataproc-cluster, vertex pipeline, fangorn_inference_dataproc_pipeline, INC-008, IMP-015, 94% cap, 93% quota, instance_flexibility_policy, instance_selection_list, e2-standard-16, google-cloud-dataproc 5.10.1, proto field absent in 5.4.0, machine family stockout rate, targeting-infra-ml 94, INC-025, QA starves prod, vertex-ai-qa, shared regional quota, N2D_CPUS different metric, principalEmail cluster owner, quota refusal base rate, hackathon week deferral, IMP-070, delete_cluster_before_retry, cleanup masks quota error, INC-015, run_daily_feature_drift, LOOKBACK_DAYS, drift lookback window, feature drift pipeline, guid_log_pivot_ip_vertical_id sensor, challenger sensor 18h timeout, vertex job_id not resource id, pipelineJobs list orderBy create_time, driverOutputResourceUri, dataproc driver output gsutil, targeting-infra-ml drift, fangorn-daily-feature-drift-pipeline, AUDI-1217, wont do, fe4e3d6, 0b19f29, 7247996, fangorn_conversions_training_pipeline, n2-highmem-16, DISKS_TOTAL_GB residual, 247808 disk limit, 145500 GB per cluster, three metric spreading, N2D_CPUS separate pool, CPUS generic metric, quota adjuster not N2_CPUS, cloudquotas permission denied, IMP-098, IMP-099, zero refusals post fix, 27 stockouts before fix]
domain: [infra]
lifecycle: active
last_verified: 2026-09-02
---
How the Fangorn inference pipeline is wired + why its Dataproc creates fail (from on-call INC-008, 2026-07-30).

**DAG `fangorn_inference_pipeline_run`** (team Fangorn/ML, project `mntn-targeting-prj-prod`, region `us-central1`; owner Brian McAdams + ML/infra). Task graph — the champion and challenger are **sequential, NOT parallel**:
```
wait_for_features           → inference_pipeline (CHAMPION) ─┐
wait_for_challenger_features → ───────────────────────────── challenger_inference_pipeline → daily_drift_pipeline
```
**`inference_pipeline` (champion) is UPSTREAM of `challenger_inference_pipeline`** — they run one-after-the-other and **never hold Dataproc clusters at the same time.** So a champion create-failure is NEVER caused by challenger contention (this refuted my first INC-008 verdict). Each task submits a Vertex AI pipeline (`fangorn_inference_dataproc_pipeline`, template in `gs://targeting-infra-vertex-pipelines-prod/fangorn/...`) whose `create-dataproc-cluster` step spins up a real Dataproc **cluster** (not serverless batch): `fangorn-inference-*`, `fangorn-challenger-*`, `fangorn-daily-drift-*`. Template lives in `targeting-infra`, not `airflow-ti`.

**Cluster sizing (the key number):** inference/challenger clusters are **290 workers ≈ 4,672 N2_CPUS (~93% of the project's 5,000 us-central1 N2_CPUS quota) + ~145,500 GB disk** (n2-standard-16, ~500 GB/node). `daily-drift` is small (~4 workers). The us-central1 quota for `mntn-targeting-prj-prod`: **N2_CPUS 5,000 · CPUS 5,000 · DISKS_TOTAL_GB 225,280 · SSD 81,920** (as of 2026-08-25; `DISKS_TOTAL_GB` read **247,808** on 2026-09-02, `N2_CPUS` unchanged — see the AUDI-1217 close-out below). That ~93% ratio IS the "~94% Dataproc cap" referenced since INC-002.

**Why it fails (`create-dataproc-cluster` → RuntimeError code 9):** at ~93% of quota the cluster has no headroom, so it fails on: (1) **transient external GCP zonal STOCKOUT** — GCP out of large instances in the autozone-picked zone (`code 14 UNAVAILABLE, COMPUTE_ENGINE`); self-recovers in ~1–2h (INC-008 root cause, owner-confirmed); (2) **quota SELF-BLOCK** — a stockout-failed create leaves ~250 partial workers (~4,016 N2) running until teardown ~45 min later, so the retry hits `Insufficient N2_CPUS quota`; (3) real concurrent-job contention (INC-002, a manual/QA job — NOT the challenger).

**DAG wiring detail (INC-015, 2026-08-09):** runs **18:00Z daily**; `run_date`/`reference_date` = `ds(data_interval_end)`. `wait_for_challenger_features` pokes `feature_store/feature_group_3_pivoted/guid_log_pivot_ip_vertical_id/dt=<that ds>/_SUCCESS` with an **18h timeout**; `wait_for_features` pokes matt.brorby's xgboost training data in `mntn-data-archive-dev`. The producer of that FS partition is airflow-ti `feature_store_setup_model` (schedule/`dt` offset/catchup gotchas: [[reference_airflow_ti]]). **Drift job:** `targeting-infra-ml` `vertex/fangorn/dataproc/run_daily_feature_drift.py` — (superseded 2026-08-10: pre-#85 it read `LOOKBACK_DAYS=3` literal `dt=` paths with NO existence guard, so one missing FS day failed every daily run until the window cleared the hole). **Fixed by [targeting-infra-ml#85](https://github.com/SteelHouse/targeting-infra-ml/pull/85)** (merged 2026-08-10, Brian): `get_latest_paths()` GCS-lists existing `dt=` partitions ≤ run_date and takes the latest 3, so a hole slides the window (drift silently computes on older days). **A missing FS day now fails only day 1** (ref-day `wait_for_challenger_features` 18h sensor timeout + challenger), not the drift tail. Champion baseline comes from `targeting-infra-vertex-pipelines-prod`. Incident detail: runbook §3 INC-015.

**Vertex → Dataproc debugging path (proven end-to-end on INC-015):** the numeric job_id in Airflow's Vertex error text is **NOT** the `pipelineJobs` resource id (GET on it 404s) — list `pipelineJobs?orderBy=create_time desc` for the real string names (`fangorn-daily-feature-drift-pipeline-<ts>`). The failing component's replica error names a **Dataproc job in `mntn-targeting-prj-prod`**; `gcloud dataproc jobs describe <id>` → `driverOutputResourceUri` → `gsutil cat` works with plain user creds — that project's staging bucket is NOT PAM-gated (unlike `mntn-prj-prod-00`'s).

**The chain has a FIFTH layer: the Vertex Model Registry (INC-024, 2026-08-20).** A `code: 9` whose failed step is `submit-parallel-inference-jobs` (not `create-dataproc-cluster`) is NOT the capacity class. Walk: Airflow log -> `pipelineJobs` list (the numeric job_id is not the resource id) -> that job's `jobDetail.taskDetails` names the failing component and its **ml_job** id -> `gcloud logging read 'resource.type="ml_job" AND resource.labels.job_id="<id>"' --severity>=ERROR` names the **Dataproc job** that failed 3 times -> `gcloud dataproc jobs describe <id> --format='value(driverOutputResourceUri)'` + `gsutil cat '<uri>.*'` holds the real exception. On INC-024 that was `ValueError: No version found with alias pattern 'challenger-v*' for model 'fangorn-hhid-xgboost'`. **`run_challenger_inference.py:35 resolve_model_uri` resolves the model by ALIAS PATTERN `<alias>-v*`**, so a missing alias fails the whole pipeline after a full cluster create. **Check the registry directly and against the sibling model:** `GET .../models?filter=display_name%3D%22<name>%22` -> `versionAliases`. `fangorn-xgboost` carries `['default','challenger-v2']` (the working convention); `fangorn-hhid-xgboost` was re-registered 2026-08-18 and its v1 carries only `['default','champion']`. **A re-registration silently drops aliases** — compare the model's `updateTime` against the last green run and that alone pins it. Owner (Brian McAdams) confirmed same-day and had anticipated it, so treat it as recurring, not one-off; guardrail = IMP-056. Never re-run this class: it is deterministic and each try builds and tears down a Dataproc cluster.

**A SIXTH failure surface, and the only one where prod is starved from OUTSIDE prod (INC-025, 2026-08-24).** `vertex-ai-qa@mntn-targeting-prj-prod` runs the **identical** 4,672-vCPU shape (`1x n2-standard-32 + 290x n2-standard-16`), in the **same GCP project**, against the **same regional `N2_CPUS` pool** — targeting-infra-ml `CLAUDE.md`: "Both environments run in the same GCP project — separation is by bucket and registry suffix." Two of these cannot coexist under a 5,000 ceiling, so **any QA inference run locks prod out for its full lifetime.** Settled by arithmetic, not inference: `fangorn-inference-26f05d0f` (QA, 22:44Z) = 4,672, and 5,000 − 4,672 = **328**, exactly the `available 328.0` in prod's refusal at 22:46Z. **Cluster NAMES are identical across environments** (`fangorn-inference-*` in both) — only `protoPayload.authenticationInfo.principalEmail` on the CreateCluster audit entry says whose it is. Two things that looked like contenders and were not: `fangorn-hhid-inference-*` is **`n2d`**-standard-16, which bills to `N2D_CPUS`, a different metric, so it holds **zero** N2; and the sibling `inference_pipeline` is sequentially upstream (`fangorn_inference_pipeline_run.py:92`) with its cluster gone before the challenger starts.

**Base rate, measured: 7 refusals in 30 days, every one served to prod.** `CreateCluster` with `protoPayload.status.code=3`, 30-day window: 2026-07-27 (1), 2026-07-30 (4), 2026-08-24 (2); **zero** to `vertex-ai-qa@`. July's all followed a `code 14` stockout minutes earlier (the self-block above); 08-24 had no `code 14` at all. Different triggers, one condition underneath.

**The cleanup masks the cause, and kills the retry (fixed by [targeting-infra-ml#93](https://github.com/SteelHouse/targeting-infra-ml/pull/93)).** `_delete_cluster_before_retry` runs inside the create's `except Exception as e`, so on a refused create it calls `delete_cluster` on a name that was never created, raises `NotFound: 404` from inside the handler, and escapes. Two effects: the surfaced error is a missing cluster rather than the quota text, and `MAX_CREATE_RETRIES = 3` / `RETRY_WAIT_SECONDS = 300` **never runs a second attempt** — the backoff written for exactly this has never executed. On INC-025 it would still have missed by ~4 minutes (attempts ~22:46/22:51/22:56, QA released 22:58:46), so it is resilience, not the fix.

**DEFERRED by owner decision, 2026-08-24: Sean Yang moved the durable fix to hackathon week when Brian McAdams is back.** Recorded reasoning was "no clear pattern"; the 7-in-30-days base rate above was measured after that call and was not in front of him. The two fixes on the table: raise `N2_CPUS` in us-central1 from 5,000 to ~15,000 (a Google quota request, no code), and cap the QA cluster so it stops requesting the full prod shape. Tracked as **IMP-070**; Malachi set a reminder for **Tuesday 2026-08-25**, when Brian is back; Sean confirmed the two fixes. Do not let it lapse.

**A stockout can last a WORKING DAY, not the "1-2h" the runbook promises (2026-08-25).** `code 14 UNAVAILABLE` on `CreateCluster` ran from **13:18 to 22:35 UTC — over nine hours** — across both `us-central1-a` and `-b`, and took `inference_pipeline` (failed 20:59) and then `challenger_inference_pipeline` (failed 22:41) in the same DAG run. The "self-recovers in ~1-2h, just re-run" guidance in §2's catalog row comes from INC-008 and is a floor, not a rule: on a long shortage, re-running burns a cluster-create cycle per try and changes nothing. Check how far back `status.code=14` goes before deciding to wait: `gcloud logging read 'protoPayload.methodName="...CreateCluster" AND protoPayload.status.code=14' --freshness=1d --format="value(timestamp)"`.

**The stockout half got a real fix on 2026-08-25, and it is an INSTANCE FLEXIBILITY POLICY, not a machine swap.** [targeting-infra-ml#94](https://github.com/SteelHouse/targeting-infra-ml/pull/94) (Sean Yang) replaces the fixed `machine_type_uri` on `worker_config` with:

```python
"instance_flexibility_policy": {
    "instance_selection_list": [
        {"machine_types": ["n2-standard-16", "n2d-standard-16", "e2-standard-16"], "rank": 0},
    ],
},
```

**The library pin is load-bearing, not incidental.** It also bumps `google-cloud-dataproc` 5.4.0 to 5.10.1. Verified by installing both: `instance_flexibility_policy` is **absent** from `InstanceGroupConfig` in 5.4.0 and present in 5.10.1, so on the old pin the field would have been silently dropped and the cluster would still have been single-family. Check a proto field before trusting it: `python -c "from google.cloud.dataproc_v1.types import InstanceGroupConfig; print([f.name for f in InstanceGroupConfig.pb(InstanceGroupConfig()).DESCRIPTOR.fields])"`.

**The policy is valid on PRIMARY `worker_config`**, not only on secondary workers — confirmed against the 5.10.1 descriptor. All three types are 16 vCPU / 64 GB so there is no memory skew, but **`e2-standard-16` has no local SSD and is a different performance class**: a cluster that lands mostly on E2 may start and then run long. Read what was actually acquired with `gcloud dataproc clusters describe`, never assume the preferred type won.

**Machine-family stockout rates, 30 days to 2026-08-25** (`CreateCluster` `status.code=14`): hhid `n2d` 8/94 (8.5%), `fangorn-inference` `n2` 21/104 (20%), `fangorn-challenger` `n2` 21/113 (19%). **Do not read that as "AMD is 2x better"** — hhid requests 11 nodes and the other two request 290, so family and size are confounded and the comparison isolates neither. A superseded PR of mine swapped both files to `n2d` on that reasoning; #94 is strictly better and mine was closed.

**Coverage, re-read from `origin/main` 2026-09-02 (this line was stale — it said only `fangorn_inference_dataproc` was covered).** The challenger got the identical change the same day: `fe4e3d6` (PR #94) on `fangorn_inference_dataproc_pipeline.py` and `0b19f29` on `fangorn_challenger_inference_pipeline.py`, both 2026-08-25 by Sean Yang. Alex Knorr carried it to `fangorn_conversions_inference_pipeline.py` in `7247996` on 2026-09-01. **Still NOT covered: `fangorn_conversions_training_pipeline.py`** — hardcoded `n2-highmem-16` on 1 master + 16 workers, and its `create_dataproc_cluster` component pins `google-cloud-dataproc==5.4.0`, so adding the policy there without also bumping the pin drops the field silently (IMP-098). **Check the pin and the file, not the PR number** — all three fixed `create_dataproc_cluster` components carry 5.10.1 while their sibling `submit_*`/`delete_*` components still pin 5.4.0, which is correct, since those build no cluster config.

**Diagnose by pulling ALL 3 surfaces + reconciling** (one underdetermines it): `gcloud dataproc operations describe <failed-create> --format="value(error)"` (zonal), the Vertex `service`/worker-pool log (quota), and `gcloud compute regions describe us-central1` quota-vs-usage. Delete any lingering ERROR cluster before retrying. Durable fix = **IMP-015** (raise N2/DISK quota, auto-delete failed clusters, multi-zone, or a smaller cluster). Full incident: on-call runbook §3 INC-008.

**The fix worked, measured 8 days later (2026-09-02, AUDI-1217 close-out).** From the `ClusterController` admin audit log:
**zero quota refusals** (`status.code=3`) since the INC-025 pair at 2026-08-24 22:46/22:58Z, and **zero stockouts**
(`code=14`) on `fangorn-inference-*` / `fangorn-challenger-*` since 2026-08-25 22:48Z — against **27** in the two days
immediately before the fix. The only `code=14` in the 9-day window since is `fangorn-conversions-training-8012099b`
(2026-09-01 21:07:56Z), the one pipeline the policy does not cover. This supersedes the "7 refusals in 30 days" base
rate above, which was measured before the fix existed.

**Why the flexibility policy also relieved the QUOTA half, which is not obvious.** The three machine types bill to
**three separate regional metrics** — `n2-standard-16` to `N2_CPUS`, `n2d-standard-16` to `N2D_CPUS`, `e2-standard-16`
to the generic `CPUS` — each 5,000 in us-central1. So the same 4,672-vCPU request that used to concentrate on one
5,000 pool now spreads across ~15,000 vCPU of headroom. **That is the "raise N2_CPUS to ~15,000" ask, delivered without
a quota request.** `N2_CPUS` itself is unchanged at 5,000 (read 2026-09-02); devops refused to raise it (Brian McAdams,
office hours with Alyson) and the cluster cannot shrink, which is why AUDI-1217 closed Won't Do.

**The residual is DISK, and no machine-family trick can touch it.** `DISKS_TOTAL_GB` is family-agnostic. One full-shape
cluster is 1x500 GB master + 290x500 GB `pd-standard` workers = **145,500 GB**, matching the `Requested 145500.0` in the
INC-025 refusal exactly. The regional limit rose 225,280 -> **247,808** between 2026-08-25 and 2026-09-02, so two
concurrent full-shape clusters (291,000 GB) are **still refused, by ~43,192 GB**. `vertex-ai-qa@` still creates
full-shape clusters in the same project (`fangorn-challenger-883bbd2b`, 2026-08-26 13:19Z), so the INC-025 overlap is
still reachable — it would now fail on disk, not CPU. **Watch signature: `status.code=3` naming `DISKS_TOTAL_GB` with
`N2_CPUS` absent** means the CPU half is working and only disk is binding (IMP-099).

**The 2026-07-29 Dataproc quota auto-scaling that Edris Mohsin enabled did NOT raise `N2_CPUS`** — still 5,000 on
2026-09-02, and 6 of the 7 pre-fix refusals post-date it. `DISKS_TOTAL_GB` did rise +22,528 over the same window, which
is consistent with an adjuster acting on disk only, but one before/after pair does not establish cause. Settling it
needs `cloudquotas.quotas.get` on `projects/mntn-targeting-prj-prod/locations/global/quotaAdjusterSettings`; my account
is `PERMISSION_DENIED`. **Do not credit the auto-scaler for the recovery** — the flexibility policy is what the dates fit.

**Both failure classes hit on 2026-08-24 and neither reading replaces the other.** The alert linked in Slack that day
(`inference_pipeline`, 11:00 PT) really was a stockout (`503 UNAVAILABLE`, `STOCKOUT`, `us-central1-a`), and INC-025's
`challenger_inference_pipeline` failure that evening really was a quota refusal (`status.code=3`, re-confirmed from the
audit log 2026-09-02). Conceding the stockout does not retract the quota finding; they are different tasks in the same
DAG run.

Related: [[reference_oncall_runbook]], [[reference_dataproc_eventlog_profiling]], [[feedback_hold_evidenced_verdict]], [[feedback_dataproc_cost_awareness]].
