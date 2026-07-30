---
name: reference_openai_sdk_pagination
description: "OpenAI Python SDK list pagination — iterating a list response directly (for x in client.files.list()) auto-fetches ALL pages; there is NO .auto_paging_iter() method (Stripe idiom → AttributeError on SyncCursorPage). The gotcha that regressed shopper_graph#297."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [openai sdk pagination, openai python sdk, auto_paging_iter, SyncCursorPage, SyncCursorPage FileObject, client.files.list, files.list pagination, has_next_page, get_next_page, next_page_info, after cursor, AttributeError auto_paging_iter, list response auto fetch, cursor page, stripe idiom pagination, delete_all_storage_files, batch_cleanup crash, shopper_graph#297, shopper_graph#298, shopper_graph#299, openai file cleanup pagination]
domain: [repos, infra]
lifecycle: active
last_verified: 2026-07-30
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

**Provenance (INC-007 / AUDI-1042, 2026-07-30):** the OpenAI file-cleanup rewrite `#297` replaced the proven
`for file in client.files.list():` with `client.files.list().auto_paging_iter()` → every `batch_cleanup`
(4×/day across the submit + fetch DAGs) crashed the `AttributeError` before deleting anything, so the deploy
shipped a cleanup that deleted 0 files and did NOT fix the 2.5TB quota wall. **Real fix `#298`** reverted to
direct iteration; **`#299`** (a manual after-cursor `has_more` loop) was closed as over-engineered/possibly
page-1-only. See [[reference_shopper_graph_deploy]] (deploy path), [[reference_oncall_runbook]] (INC-007).
