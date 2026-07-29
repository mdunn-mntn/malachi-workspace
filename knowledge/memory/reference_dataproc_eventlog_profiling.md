---
name: reference_dataproc_eventlog_profiling
description: How to profile a Dataproc Serverless batch from its Spark event log (stage timing, shuffle, spill, skew, FetchFailed) when the Airflow log + driver output only show the symptom.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [dataproc serverless, spark event log, eventlog profiler, driveroutput, zstd, gcloud-crc32c gatekeeper, storage api download, shuffle spill, FetchFailed, uncached recomputation, milliDcuSeconds, ttl exceeded, batch profiling, tpa_mntn_id_export, INC-005]
domain: [infra]
lifecycle: active
last_verified: 2026-07-29
---
Profile a Dataproc Serverless batch from its **Spark event log** when the Airflow task log + `dataproc batches wait` driver output only show the symptom (e.g. "batch was cancelled") not where the wall-clock went. Built for INC-005 (`tpa_mntn_id_export` hit its 3h TTL); the parser is saved at `on-call/incidents/INC-005/eventlog_profiler.py`.

**Steps:**
1. Find the event log: `gcloud dataproc batches describe <id> --region us-central1 --project mntn-prj-prod-00 --format="value(uuid,runtimeInfo.outputUri)"` → the event log is `gs://dataproc-temp-us-central1-<projnum>-<suffix>/<uuid>/spark-job-history/app-*.zstd` (`.inprogress` suffix if the batch was killed). It captures up to ~2 min before a TTL kill.
2. **Download** (heads-up on a macOS gotcha): `gcloud storage cp` can report throughput but write **no file** because Gatekeeper blocks gcloud's `gcloud-crc32c` checksum helper (popup "gcloud-crc32c Not Opened" → click **Done**, NOT Move to Trash, which deletes the helper). Route around it with the storage JSON API: `curl -sS --fail -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://storage.googleapis.com/storage/v1/b/<bucket>/o/<URL-ENCODED-object>?alt=media" -o eventlog.zstd` (encode `/` as `%2F`).
3. **Decompress:** `zstd -d eventlog.zstd -o eventlog.json` (event log is JSON-lines, one Spark event per line; a 55MB zstd → ~1.2GB / ~300K events).
4. **Parse:** `python3 on-call/incidents/INC-005/eventlog_profiler.py eventlog.json` — streams the file and prints per-stage cumulative runtime, shuffle read/write, memory+disk spill, GC%, fetchWait%, task-dur p50/p95/max (skew), FetchFailed count by (stage,shuffle), executor add/remove + removal reasons, job durations, SQL execs.

**Discriminators it surfaces (memory vs compute vs shuffle vs recompute vs infra):** cpu% ≈ runtime → compute-bound; high gc% → GC/memory pressure; high **fetchWait%** + big memory/disk **spill** → shuffle I/O bound from under-partitioning; the **same call-site stage repeated N×** (e.g. 7-9 `json at ...` stages) → uncached-lineage recompute spiral; `executors removed=0` → no executor loss (rules out infra); task max/p50 ratio → skew. Cross-check the batch's `runtimeInfo.approximateUsage.milliDcuSeconds` + `shuffleStorageGbSeconds` good-run vs bad-run (INC-005 fix cut both ~26×). TTL-kill reason is in `stateHistory` → `"Cancelling batch as ttl exceeded"` (the Airflow log omits it).

See [[reference_airflow_ti]] (the models that run these batches), [[reference_databricks]], [[reference_bq_location_reservation]]. INC-005 detail in `on-call/oncall_runbook.md`.
