---
name: reference_openai_sdk_pagination
description: "Two OpenAI files.list traps: (1) iterating a list response auto-fetches ALL pages and there is NO .auto_paging_iter() (Stripe idiom → AttributeError on SyncCursorPage, shopper_graph#297); (2) GET /v1/files caps limit at 10,000 and defaults to created_at desc, so an age-based cleanup sees only the newest window and frees NOTHING under churn — fix is order='asc' paging (shopper_graph#306)."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [openai sdk pagination, openai python sdk, auto_paging_iter, SyncCursorPage, SyncCursorPage FileObject, client.files.list, files.list pagination, has_next_page, get_next_page, next_page_info, after cursor, AttributeError auto_paging_iter, list response auto fetch, cursor page, stripe idiom pagination, delete_all_storage_files, batch_cleanup crash, shopper_graph#297, shopper_graph#298, shopper_graph#299, shopper_graph#306, openai file cleanup pagination, files.list order, order asc, created_at desc default, limit 10000 cap, newest 10000 files window, oldest-first cleanup, age-based cleanup order, Total number of files to delete 0, file storage quota 2.5TB, exceeded your file storage quota, openai file cleanup order defect, deploy_openai_dockerhub_gcp]
domain: [repos, infra]
lifecycle: active
last_verified: 2026-09-03
---
**OpenAI Python SDK — how to page a list response (the gotcha that regressed `shopper_graph#297`).**

- **Iterating a list response directly auto-fetches ALL pages.** `for file in client.files.list():` (same for
  `.batches.list()` etc.) transparently pulls every page — OpenAI's docs: *"Automatically fetches more pages
  as needed."* This is the simplest correct pattern and the one the cleanup script ran for ~2 years.
- **There is NO `.auto_paging_iter()` method.** That is a **Stripe-SDK** idiom. Calling it on an OpenAI list
  result raises **`AttributeError: 'SyncCursorPage[FileObject]' object has no attribute 'auto_paging_iter'`**
  — a deterministic crash on the FIRST use, not a data or version issue. (`SyncCursorPage[T]` is the type the
  sync client returns for `*.list()`.)
- **Manual pagination API (if you must page by hand):** the returned page exposes `.has_next_page()`,
  `.get_next_page()`, `.next_page_info()`, plus `.data` (the rows) and an `.after` cursor. **Do NOT hand-roll
  a loop on a `has_more` field** — that field may be absent on the page object, silently stopping you after
  page 1. Prefer direct iteration (auto-paging) unless there's a reason not to.
- **Version note:** shopper_graph `openai/requirements.txt` pins `openai` **unversioned** (a fresh build gets
  latest). The #297 crash was a **nonexistent method**, not a version regression, so pinning isn't required
  to fix it (optional hygiene only).

**TRAP 2 (AUDI-1191, 2026-09-03) — ORDER, not pagination: `files.list` can only see the NEWEST 10,000 files.**
Distinct from trap 1 above and additive to it: after `#298` the paging was correct, and the sweep still froze the
pipeline for six days.

- **`GET /v1/files` caps `limit` at 10,000 (also the default) and returns `created_at desc` by default**, so any
  list walk sees a newest-first window at most 10,000 files wide.
- `delete_all_storage_files.py` deletes only files **older than 48h**. Once more than 10,000 files are younger than
  48h, every file on that page is ineligible and the sweep frees **NOTHING** — precisely when churn is highest and
  cleanup matters most.
- **Evidence (Airflow task logs):** normal-churn runs 2026-08-25..27 found 13 / 14 / 28 / 131 / 181 / 357 / 788 /
  1170 files; EVERY run 2026-08-29 → 2026-09-03 reported `Total number of files to delete: 0` while `batch_submit`
  died with `400 ... exceeded your file storage quota. Projects are limited to 2.5TB`. Trigger: the 2026-09-03
  dead-cohort backfill uploaded **3,429+ input files in ~2 hours** on top of the outage retry storm.
- **Fix = list `order="asc"` with explicit paging** — break at the first file inside the retention window, page only
  while a page comes back full (`shopper_graph#306`, MERGED; image deployed via `deploy_openai_dockerhub_gcp.yml`
  run `33775001798`). **Proven live the same hour:** the first sweep on the new image deleted **1,132 of 1,132**
  files with 0 skips, where every prior run found 0.
- **Ascending is also FASTER in steady state:** page one starts at the oldest file and the loop breaks at the cutoff
  (one API call); descending had to page past every young file to reach the old ones.
- **Rule:** for any age-based cleanup over a cursor-paged list API, sort **oldest-first** and stop at the cutoff.
  Newest-first plus a page cap degrades silently to a no-op exactly under load. Quota/storage context:
  [[reference_mntn_matched_batch_pipeline]].

**Provenance (INC-007 / AUDI-1042, 2026-07-30):** the OpenAI file-cleanup rewrite `#297` replaced the proven
`for file in client.files.list():` with `client.files.list().auto_paging_iter()` → every `batch_cleanup`
(4×/day across the submit + fetch DAGs) crashed the `AttributeError` before deleting anything, so the deploy
shipped a cleanup that deleted 0 files and did NOT fix the 2.5TB quota wall. **Real fix `#298`** reverted to
direct iteration; **`#299`** (a manual after-cursor `has_more` loop) was closed as over-engineered/possibly
page-1-only. See [[reference_shopper_graph_deploy]] (deploy path), [[reference_oncall_runbook]] (INC-007).
