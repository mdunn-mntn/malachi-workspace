---
name: reference_dataproc_eventlog_profiling
description: How to profile a Dataproc Serverless batch from its Spark event log (stage timing, shuffle, spill, skew, FetchFailed) when the Airflow log + driver output only show the symptom.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [dataproc serverless, spark event log, eventlog profiler, driveroutput, zstd, gcloud-crc32c gatekeeper, storage api download, shuffle spill, FetchFailed, uncached recomputation, milliDcuSeconds, ttl exceeded, batch profiling, tpa_mntn_id_export, INC-005, straggler, speculation, shuffleTracking, executor pinning, eventlog_v2 rolling dir, idle executors, dynamic allocation, intent_score_map, aud-int-int-map]
domain: [infra]
lifecycle: active
last_verified: 2026-08-07
---
Profile a Dataproc Serverless batch from its **Spark event log** when the Airflow task log + `dataproc batches wait` driver output only show the symptom (e.g. "batch was cancelled") not where the wall-clock went. Built for INC-005 (`tpa_mntn_id_export` hit its 3h TTL); the parser is saved at `on-call/incidents/INC-005/eventlog_profiler.py`.

**Steps:**
1. Find the event log: `gcloud dataproc batches describe <id> --region us-central1 --project mntn-prj-prod-00 --format="value(uuid,runtimeInfo.outputUri)"` → the event log is `gs://dataproc-temp-us-central1-<projnum>-<suffix>/<uuid>/spark-job-history/app-*.zstd` (`.inprogress` suffix if the batch was killed). It captures up to ~2 min before a TTL kill.
2. **Download** (heads-up on a macOS gotcha): `gcloud storage cp` can report throughput but write **no file** because Gatekeeper blocks gcloud's `gcloud-crc32c` checksum helper (popup "gcloud-crc32c Not Opened" → click **Done**, NOT Move to Trash, which deletes the helper). Route around it with the storage JSON API: `curl -sS --fail -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://storage.googleapis.com/storage/v1/b/<bucket>/o/<URL-ENCODED-object>?alt=media" -o eventlog.zstd` (encode `/` as `%2F`).
3. **Decompress:** `zstd -d eventlog.zstd -o eventlog.json` (event log is JSON-lines, one Spark event per line; a 55MB zstd → ~1.2GB / ~300K events).
4. **Parse:** `python3 on-call/incidents/INC-005/eventlog_profiler.py eventlog.json` — streams the file and prints per-stage cumulative runtime, shuffle read/write, memory+disk spill, GC%, fetchWait%, task-dur p50/p95/max (skew), FetchFailed count by (stage,shuffle), executor add/remove + removal reasons, job durations, SQL execs.

**Discriminators it surfaces (memory vs compute vs shuffle vs recompute vs infra):** cpu% ≈ runtime → compute-bound; high gc% → GC/memory pressure; high **fetchWait%** + big memory/disk **spill** → shuffle I/O bound from under-partitioning; the **same call-site stage repeated N×** (e.g. 7-9 `json at ...` stages) → uncached-lineage recompute spiral; `executors removed=0` → no executor loss (rules out infra); task max/p50 ratio → skew. **Skew needs a data cross-check (2026-08-07):** duration max/p50 high while per-task read bytes are ~uniform (data ratio ~1x) AND the slow task's CPU is a tiny fraction of its wall (fetchWait 0, GC 0) → an **IO-stalled STRAGGLER on one node, not data skew** — fix is `spark.speculation=true` (quantile ~0.9), NOT salting (AUDI-1194 `intent_score_map`: 13.4x duration, 1.0x data, 5% CPU, 67-min tail; `airflow_optimizer` now has `straggler` + `idle_reserved_executors` detectors for this). Cross-check the batch's `runtimeInfo.approximateUsage.milliDcuSeconds` + `shuffleStorageGbSeconds` good-run vs bad-run (INC-005 fix cut both ~26×). TTL-kill reason is in `stateHistory` → `"Cancelling batch as ttl exceeded"` (the Airflow log omits it).

**Dynamic-allocation pinning (Spark 4, serverless, verified vs apache/spark v4.0.0 source 2026-08-07):** with `shuffleTracking.enabled` (mandatory without an external shuffle service), `ExecutorMonitor.timedOutExecutors()` categorically excludes executors whose shuffle blocks are referenced by a **live job** (`hasActiveShuffle`) — AQE final jobs register upstream shuffles for their whole duration, so a 1-task tail pins the ENTIRE fleet (240/240 held, 0 removed, in the intent_score_map run). `spark.dynamicAllocation.shuffleTracking.timeout` only applies AFTER all referencing jobs end → **it is NOT a mid-query release lever; don't recommend it for tail-pinning**. The real lever is killing the tail (speculation / skew fix). `executorIdleTimeout` is similarly moot while shuffles are live.

**Event-log shapes in `gs://mntn-data-archive-{env}/spark-events`:** single `app-<id>.zstd` files AND **v2 rolling dirs** `eventlog_v2_batch-<batch-uuid>/events_{1..N}_batch-<uuid>.zstd` (+ `appstatus_*` marker). The batch-fleet logs use the batch UUID (from `batches describe .uuid`), NOT an app- name — find them via the uuid. `airflow_optimizer.eventlog` parses all parts in numeric order (IMP-029 fixed 2026-08-07). macOS download gotcha #2: `gsutil -m cp` can hang forever at 0-byte `.gstmp` files (sliced downloads + LibreSSL) — use sequential `gsutil -o "GSUtil:check_hashes=never" -o "GSUtil:sliced_object_download_threshold=0" cp`.

See [[reference_airflow_ti]] (the models that run these batches), [[reference_databricks]], [[reference_bq_location_reservation]]. INC-005 detail in `on-call/oncall_runbook.md`.
