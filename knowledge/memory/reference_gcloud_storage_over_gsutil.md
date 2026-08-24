---
name: reference_gcloud_storage_over_gsutil
description: gsutil -m cp hangs indefinitely on this Mac — use `gcloud storage cp` for any multi-file GCS transfer; sequential `gsutil cp` also works.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [gsutil, gcloud storage, GCS download, multi-file copy, hang, stall, LibreSSL, parallel copy, multiprocessing, python 3.9]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-08-24
---

`gsutil -m cp` (and `-m cp -r`) stalls indefinitely on multi-file transfers on this Mac (single-file and sequential copies work). `gcloud -q storage cp` moves the same file sets at ~10-30 MiB/s without issue.

Re-confirmed 2026-08-24 (AUDI-1142 shopper-graph pod-log pulls): `-m` hung again; a sequential `gsutil cp` loop worked. Cause has two recorded hypotheses, neither settled: the macOS LibreSSL build (2026-08-10 note, AUDI-431) vs gsutil's Python 3.9 `multiprocessing` on macOS (2026-08-24 observation). The rule holds either way.

**Why:** cost 2 stalled background downloads + a 15-min stall-detector round-trip in AUDI-431 (2026-08-10), and another hang in AUDI-1142 (2026-08-24).

**How to apply:** default to `gcloud -q storage cp [-r]` for every GCS transfer involving wildcards or multiple objects; keep `gsutil ls/du` for listings only. [[feedback_background_work_liveness]]
