---
name: reference_gcloud_storage_over_gsutil
description: gsutil -m cp hangs indefinitely on this Mac (LibreSSL) — use `gcloud storage cp` for any multi-file GCS transfer.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [gsutil, gcloud storage, GCS download, multi-file copy, hang, stall, LibreSSL, parallel copy]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-08-10
---

`gsutil -m cp` (and `-m cp -r`) stalls indefinitely on multi-file transfers on this Mac (macOS LibreSSL build; single-file copies work). `gcloud -q storage cp` moves the same file sets at ~10-30 MiB/s without issue.

**Why:** cost 2 stalled background downloads + a 15-min stall-detector round-trip in AUDI-431 (2026-08-10).

**How to apply:** default to `gcloud -q storage cp [-r]` for every GCS transfer involving wildcards or multiple objects; keep `gsutil ls/du` for listings only. [[feedback_background_work_liveness]]
