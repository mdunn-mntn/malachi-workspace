---
name: reference_openai_sdk_pagination
description: "Two OpenAI files.list traps: (1) iterating a list response auto-fetches ALL pages and there is NO .auto_paging_iter() (Stripe idiom → AttributeError on SyncCursorPage, shopper_graph#297); (2) GET /v1/files caps limit at 10,000 and defaults to created_at desc, so an age-based cleanup sees only the newest window and frees NOTHING under churn — fix is order='asc' paging (shopper_graph#306); (3) a page SHORTER than the requested limit is NOT the last page, so `len(files) < PAGE: break` truncates the walk and the sweep acts on a list it never retrieved (fixed #308, 2026-09-04); (4) the `after` cursor file can be DELETED mid-listing by the other DAG sweeping on the same cron, and the 404 aborts the run (fixed #309). With all four fixed the sweep measured 129 files / 4.2 GiB of OUR files — which does NOT settle the 2.5 TB, because the cap is on a company-shared project our key can only partly list (corrected 2026-09-05)."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [openai sdk pagination, openai python sdk, auto_paging_iter, SyncCursorPage, SyncCursorPage FileObject, client.files.list, files.list pagination, has_next_page, get_next_page, next_page_info, after cursor, AttributeError auto_paging_iter, list response auto fetch, cursor page, stripe idiom pagination, delete_all_storage_files, batch_cleanup crash, shopper_graph#297, shopper_graph#298, shopper_graph#299, shopper_graph#305, shopper_graph#306, openai file cleanup pagination, files.list order, order asc, created_at desc default, limit 10000 cap, newest 10000 files window, oldest-first cleanup, age-based cleanup order, Total number of files to delete 0, file storage quota 2.5TB, exceeded your file storage quota, openai file cleanup order defect, deploy_openai_dockerhub_gcp, AUDI-1321, quota wall resolved, first green submit since 08-28, 1132 of 1132 deleted, batch_submit succeeded 57 minutes, kill criterion never triggered, storage was ours, zero-delete alarm, STORAGE_ALARM_MIN_FILES, zero delete looks like quiet day, silent no-op observability, short page is not the last page, len files < PAGE break, after cursor empty page, Deleted 0 of 0 files having listed at least 28, 4622 seen, 416 files deleted by batch_fetch, partial listing, ALARM_MIN_FILES partial page, shopper_graph#307, shopper_graph#308, per-file bytes purpose inventory, 19bc1af, 2.4TB unaccounted, 40 MB per input file, 40.3 GB inputs per day, 46 GB results per day, 100 GB pipeline footprint, storage ownership challenged, storage ownership settled, 129 files 4.2 GiB, cursor deleted mid-listing, NotFoundError after cursor, 404 on after cursor, paging cursor race, shopper_graph#309, shopper_graph#310, 5527 deletable inputs, 193.4 GiB, OPENAI_FILE_MAX_AGE_HOURS 26h, input retention 26 hours, 12h retention failed batches, purpose=batch inventory, company shared openai project, shared default project quota, key lists only own uploads, ownership retracted, 6221 files 198.8 GiB, 6040 files 191.8 GiB, deterministic 400 quota, scoped credential blind spot]
domain: [repos, infra]
lifecycle: active
last_verified: 2026-09-05
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

**TRAP 2 (AUDI-1191 / AUDI-1321, 2026-09-03) — ORDER, not pagination: `files.list` can only see the NEWEST
10,000 files. RESOLVED AND PROVEN, see the verdict at the end of this section.**
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
- **Fix = list `order="asc"` with explicit paging** — break at the first file inside the retention window
  (`shopper_graph#306`, MERGED; image deployed via `deploy_openai_dockerhub_gcp.yml` run `33775001798`).
  **Proven live the same hour:** the first sweep on the new image deleted **1,132 of 1,132** files with 0 skips,
  where every prior run found 0. **CORRECTION 2026-09-04:** #306 also stopped paging when a page came back
  shorter than `limit`, which is a second, independent defect — see TRAP 3 below. The `order="asc"` half stands;
  the "page only while a page comes back full" half is WRONG.
- **Ascending is also FASTER in steady state:** page one starts at the oldest file and the loop breaks at the cutoff
  (one API call); descending had to page past every young file to reach the old ones.
- **Rule:** for any age-based cleanup over a cursor-paged list API, sort **oldest-first** and stop at the cutoff.
  Newest-first plus a page cap degrades silently to a no-op exactly under load. Quota/storage context:
  [[reference_mntn_matched_batch_pipeline]].
- **VERDICT — the fix cleared the wall, end to end (AUDI-1321, 2026-09-03).** `#306` deployed **15:50**; the
  next cleanup deleted **1,132 of 1,132** with 0 skips; **`batch_submit` on submit logical 2026-09-02 then
  SUCCEEDED at ~19:00 UTC after running ~57 minutes** — the **first green submit since 2026-08-28**, where every
  prior attempt died in **~27 seconds** on the storage `400`. The 27s-vs-57min gap is the tell: the old failures
  never got past the upload. **AUDI-1321's kill criterion never triggered** ("if it still 400s the storage is not
  ours, escalate to Alyson for dashboard access") — deleting the names our own sweep owns (`part-*` / `batch_*`)
  freed enough headroom for the submit, so the list-order defect was a real and sufficient cause of OUR share of
  the wall. **The stronger claim written here on 09-03/09-04 — "the storage WAS ours, no other producer was
  holding it, do not reopen the shared-account hypothesis" — is WRONG (corrected 2026-09-05).** The project is
  company-shared and our key lists only our own uploads. See the OWNERSHIP section below.
- **Companion alarm (`shopper_graph#305`, merged 18:39 UTC + deployed 2026-09-03):** `delete_all_storage_files.py`
  now **raises** when every eligible delete fails, and when it frees **nothing** while at least
  `STORAGE_ALARM_MIN_FILES` (env, default **10,000**) files are still stored. Normal volume is a few hundred to
  ~1,200 files/day so a quiet day stays silent. **The generalizable point: a sweep that deletes zero looks
  IDENTICAL in the logs to a sweep with nothing to do** — which is why the 08-28 outage ran silent for six days
  behind a green cleanup task. An operation whose success state and whose total-failure state emit the same
  output has no observability, however green it looks.

**TRAP 3 (AUDI-1321, 2026-09-04) — a SHORT PAGE IS NOT THE LAST PAGE.**
Independent of traps 1 and 2. **FIXED by `shopper_graph#308`, deployed 2026-09-04** (the walk now ends on an
EMPTY page); the trap itself is the durable lesson.

- `delete_all_storage_files.py` ends its walk on `if too_young or len(files) < PAGE: break`. `GET /v1/files` can
  return fewer than `limit` rows and still have more behind the cursor, so that loop stops early and the sweep
  reasons about a list it never fully retrieved.
- **Proof:** `batch_cleanup_1` on BOTH DAGs at 2026-09-04 09:00 logged `Deleted 0 of 0 files, having listed at
  least 28.` **Four minutes later `batch_fetch` downloaded and deleted 416 OpenAI output files.** Those 416
  existed at 09:00 and were not among the 28. Earlier readings that day ran 4,622 / 4,621 / 4,623 / 4,622 through
  05:11, then 28 from 09:00 on, with no delete in between that could explain the drop — the number is a
  first-page size, not a store size.
- **Correct form:** page with `after=files[-1].id` until the API returns an **empty** page. Do not trust
  `len(page) < limit`, and do not trust `has_more` alone on this SDK's page object (trap 1).
- **The SDK pinned in the `openai_batch_runner` image has no `auto_paging_iter`** — `shopper_graph` commit
  `19bc1af` removed its use — so the auto-paging escape hatch of trap 1 is not available here; the loop must be
  hand-rolled correctly.
- **Knock-on: the #305 zero-delete alarm cannot fire on this.** `ALARM_MIN_FILES` defaults to `PAGE` (10,000) and
  `seen` never exceeded 4,623, so `not deleted and seen >= ALARM_MIN_FILES` stayed false while `batch_cleanup_1`
  went green having freed nothing and `batch_submit` died on the storage 400 at 10:45. **A threshold compared
  against a partial page is not a threshold.**
- **Rule (same shape as trap 2):** a cleanup that acts on a truncated listing fails silently and looks green.
  Terminate a cursor walk on an EMPTY page, never on a short one.

**TRAP 4 (AUDI-1321, 2026-09-04) — THE `after` CURSOR FILE CAN BE DELETED WHILE YOU ARE STILL LISTING.**
- Both `mntn_match_incrementals_submit` and `..._fetch` run `batch_cleanup` on `0 9 * * *`, so two sweeps
  walk the same store concurrently. The other run deletes the file id our `after=` cursor names, the next
  `files.list(after=<deleted id>)` raises **`NotFoundError` (404)**, and the exception aborted the WHOLE run.
- **In prod it killed a run that had already enumerated 5,527 deletable inputs holding 193.4 GiB** — every
  one of them re-listed from scratch the next cycle.
- **Fix (`shopper_graph#309`, deployed 2026-09-04):** catch `NotFoundError` on the cursor fetch, log a
  warning naming the cursor id, stop listing, and delete what was VALIDLY enumerated so far. A truncated
  listing must never trip the quota alarm — its counts are partial, so the alarm is suppressed for that run.
- **The warning fired on the very next successful run.** This race is ROUTINE, not theoretical: do not treat
  a cursor 404 as flakiness worth one retry, treat it as the expected steady state of two concurrent sweeps.
  Same family as the benign `Skipped file-xxx: 404 No such File object` during delete
  ([[reference_mntn_matched_batch_pipeline]]), but this one is fatal rather than benign because it lands on
  the LISTING, not on an individual delete.
- **Rule:** any cursor walk over a store another process is mutating must treat a missing cursor as a normal
  end-of-walk condition, and must mark its own results as partial so no downstream threshold reads them as a total.

**OWNERSHIP OF THE 2.5TB — NOT OURS TO SETTLE (corrected 2026-09-05; the 09-04 inventory below is kept as
evidence, its ownership conclusion is retracted).** The 2.5 TB file-storage cap belongs to the **company-shared
default OpenAI project**, confirmed by Malachi 2026-09-05, and **our API key can list only the files WE
uploaded** — every other team's files are invisible to `files.list`. So the inventory measures our own footprint
and is structurally incapable of naming the holder of the rest. Contradicting evidence from the same week: two
consecutive sweeps read `Listed 6221 files holding 198.8 GiB, 7.8% of the 2.5TB project limit` and `6040 files
holding 191.8 GiB` while `files.create` still returned a deterministic `400` exceeded-quota. **A submit
succeeding right after our sweep is equally consistent with a shared pool our deletions merely made room in.**
What settled it was a person's direct knowledge of an account our instrumentation cannot inspect
([[feedback_scoped_credential_cannot_prove_ownership]]). Practical consequence: our own footprint peaked near
**200 GiB** while holding four days of inputs, and headroom now depends on other teams' usage — AUDI-1301's
dedicated project is the fix for the ambiguity itself.

The 09-04 measurement, kept as evidence of what our key CAN see: once #308 and #309 shipped, the sweep on submit
`scheduled__2026-09-03T09:00` enumerated everything visible to us and logged it by bytes and purpose:

    Listed 129 files holding 4.2 GiB, 0.2% of the 2.5TB project limit. This pipeline holds 2.8 GiB.
       input  purpose=batch                80 files  2.8 GiB
       other  purpose=batch                11 files  1.1 GiB
       other  purpose=fine-tune            21 files  0.3 GiB
       other  purpose=assistants            5 files  0.0 GiB
       other  purpose=fine-tune-results    12 files  0.0 GiB

- **Everything VISIBLE TO OUR KEY that is not this pipeline totals 1.4 GiB.** That is not "there is no other
  large producer" — files uploaded by another team's key never appear in this listing at all.
- **Our own share of the wall WAS a multi-day backlog of our `part-` batch inputs** that the short-page listing
  (trap 3) could never reach: one run of the fixed sweep enumerated **5,527 deletable inputs holding 193.4 GiB**,
  aged 21.8h to 54.6h, and later runs read 6,221 files / 198.8 GiB. The earlier "28 files" reading was a first
  page, never a count.
- **AUDI-1321's §0 kill criterion never fired and no OpenAI dashboard access was obtained** — which is why the
  shared-project question stayed open until a person answered it. The "~2.4 TB unaccounted" arithmetic of
  2026-09-04 morning was right about the STEADY-STATE footprint (~100 GB at a 48h window) and wrong to conclude
  anything from it in either direction. **Lesson: a footprint computed from one normal day says nothing about a
  store whose cleanup has been failing for a week, and an inventory taken through a scoped credential says
  nothing about what that credential cannot see.**
- **`batch_submit` still dies on the FIRST ~40 MB `files.create`**, so a green submit proves "there was room
  for one file", not "the store is clean". Read the inventory line, not the submit's exit code.

**Provenance (INC-007 / AUDI-1042, 2026-07-30):** the OpenAI file-cleanup rewrite `#297` replaced the proven
`for file in client.files.list():` with `client.files.list().auto_paging_iter()` → every `batch_cleanup`
(4×/day across the submit + fetch DAGs) crashed the `AttributeError` before deleting anything, so the deploy
shipped a cleanup that deleted 0 files and did NOT fix the 2.5TB quota wall. **Real fix `#298`** reverted to
direct iteration; **`#299`** (a manual after-cursor `has_more` loop) was closed as over-engineered/possibly
page-1-only. See [[reference_shopper_graph_deploy]] (deploy path), [[reference_oncall_runbook]] (INC-007).
