---
name: reference_gcloud_storage_over_gsutil
description: gsutil -m's process-FORKED workers die/hang silently — on this Mac AND in CPU-constrained pods (root-caused 2026-09-01, AUDI-1194 downloader freeze); use `gcloud storage cp`, plain `gsutil cp`, or threads-only `-m` (`GSUtil:parallel_process_count=1`).
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [gsutil, gcloud storage, GCS download, multi-file copy, hang, stall, LibreSSL, parallel copy, multiprocessing, python 3.9, parallel_process_count, threads-only gsutil, forked workers die, constrained pod cpu, fetch.py GSUTIL_OPTS, PR 1260, downloader freeze partial sweep, spark-events composite, GHFS composite no hashes, found no hashes to validate]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-09-01
---

`gsutil -m cp` (and `-m cp -r`) stalls indefinitely on multi-file transfers on this Mac (single-file and sequential copies work). `gcloud -q storage cp` moves the same file sets at ~10-30 MiB/s without issue.

Re-confirmed 2026-08-24 (AUDI-1142 shopper-graph pod-log pulls): `-m` hung again; a sequential `gsutil cp` loop worked. The two early hypotheses (macOS LibreSSL, AUDI-431 2026-08-10; gsutil's Python 3.9 `multiprocessing` on macOS, 2026-08-24) were SETTLED 2026-09-01: the discriminating variable is **process forking**, and it is not Mac-only — see below.

**Why:** cost 2 stalled background downloads + a 15-min stall-detector round-trip in AUDI-431 (2026-08-10), and another hang in AUDI-1142 (2026-08-24).

**How to apply:** default to `gcloud -q storage cp [-r]` for every GCS transfer involving wildcards or multiple objects; keep `gsutil ls/du` for listings only. Where gsutil is required (e.g. `check_hashes=never` for zstd event logs), use plain `cp` or threads-only `-m` (`-o "GSUtil:parallel_process_count=1"`). [[feedback_background_work_liveness]]

## Root cause settled 2026-09-01 — forked `-m` workers die quietly in constrained pods too (AUDI-1194)

**gsutil `-m`'s process-FORKED workers die silently under CPU constraint, and the parent still exits "Done".** Every optimizer sweep since 2026-08-28 landed ~2/192 spark-events logs (194/200 counted failed) on the 0.25-CPU Astro pod while exiting cleanly, which froze finding resolution for 6 consecutive sweeps — the sweep never errored, it just went partial forever (full impact: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_diagnosis_2026_09_01.md` §1).

Proven by isolation, not inference: forked `-m` hangs or silently loses files on the Mac AND the pod; plain `cp` and `-m` with `-o "GSUtil:parallel_process_count=1"` (threads-only parallelism) copy everything on both.

- **Fix: airflow-ti PR #1260** — `GSUTIL_OPTS` in `airflow_optimizer/fetch.py` forces threads-only `-m`.
- **Benign warning, do not chase:** spark-events objects are GHFS-synced COMPOSITE objects with NO stored hashes — gsutil's "Found no hashes to validate" warning under `check_hashes=never` is expected, not a failure.

[[project_airflow_optimizer]]
