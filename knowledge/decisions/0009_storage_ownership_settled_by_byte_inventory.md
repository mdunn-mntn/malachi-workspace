---
doc_type: decision
title: "0009 — Settle who holds the OpenAI 2.5 TB with per-file byte inventory logging, not by lowering the retention window"
summary: "shopper_graph #308 makes the sweep page on an empty page and log per-file bytes + purpose plus a total; no retention-window change and no further submit re-run until it reports, because the pipeline's own measured footprint (~100 GB at 48h) leaves ~2.4 TB unaccounted. OUTCOME 2026-09-04 evening: the inventory reported 129 files / 4.2 GiB and settled the 2.5 TB as our own un-swept backlog"
status: accepted
date: 2026-09-04
last_verified: 2026-09-04
keywords: [openai storage quota, 2.5TB cap, storage ownership, per-file bytes, file purpose, inventory logging, shopper_graph#308, shopper_graph#307, shopper_graph#309, OPENAI_FILE_MAX_AGE_HOURS, retention window, delete_all_storage_files, short page pagination, after cursor, cursor deleted mid-listing, batch_submit headroom, AUDI-1321, AUDI-1301, 40 GB headroom, 100 GB footprint, 2.4 TB unaccounted, storage ownership settled, 129 files 4.2 GiB, 5527 deletable inputs, 193.4 GiB]
supersedes: null
tags: [shopper_graph, openai, storage, observability]
---

# 0009 — Settle who holds the OpenAI 2.5 TB with per-file byte inventory logging, not by lowering the retention window

## Context
AUDI-1321's 2026-09-03 verdict was that the 2.5 TB was the pipeline's own, inferred from one green `batch_submit`
after a sweep that deleted 1,132 of 1,132 eligible files. Measurement on 2026-09-04 does not fit that inference:
one day of batch inputs is ~1,014 files x ~40 MB = 40.3 GB and one day of results is ~46 GB, so the whole pipeline
holds ~100 GB under its 48h retention window — roughly 2.4 TB of the cap belongs to something else. Two defects sit
underneath the uncertainty: the sweep stops paging on a page shorter than `limit`, so every count it logs is a
first-page size (`Deleted 0 of 0 files, having listed at least 28`, four minutes before `batch_fetch` deleted 416
files it never listed); and no log records file SIZES at all, so no run has ever measured bytes. The handoff plan for
`dt=2026-09-03` proposed shortening the retention window through `OPENAI_FILE_MAX_AGE_HOURS` (shopper_graph #307),
which is separately inert because an Astro deployment variable does not reach a `KubernetesPodOperator` pod.

## Decision
Ship `shopper_graph` **#308**: page on an EMPTY page (`after=files[-1].id`) and log per-file `bytes` and `purpose`
plus a total. Do not lower the retention window and do not re-run any submit until that inventory reports. It is a
shopper_graph-only change on a deploy path already exercised the same day (~2 minutes), and it answers the ownership
question directly instead of by inference.

## Alternatives considered
- **Lower the retention window (`OPENAI_FILE_MAX_AGE_HOURS`, #307)** — rejected as the next step: it is inert until
  an `airflow-ti` PR adds the variable to the DAG's `env_vars`, and lowering a window on a sweep that cannot
  enumerate its own store deletes an unknown set for an unknown gain. It also risks deleting inputs still attached
  to live batches (dt=2026-09-02 had 427 of 895 batches `in_progress` at 09:44 with ~20-21h-old inputs).
- **Re-run submit and read the outcome** — rejected: `batch_submit` dies on the FIRST ~40 MB `files.create`, so it
  needs only ~40 GB of headroom. A green submit therefore proves "there was room for one file", not "the storage is
  ours", which is exactly the inference that produced the challenged 09-03 verdict.
- **Ask Alyson for the OpenAI dashboard** — kept as the escalation, not the first move: the inventory is cheaper,
  key-free, repeatable, and it names the holder in our own logs where the next on-call can find it. AUDI-1301
  (dedicated project + audit logging) remains the durable fix either way.

## Consequences
- The five older backfill days (dt=2026-08-27..09-01) stay blocked until the inventory reports. That is the accepted
  cost of not deleting blind.
- Every future sweep log carries bytes, so the storage-economics numbers stop being estimates and the zero-delete
  alarm can be re-thresholded on bytes rather than on a partial-page file count.
- The 2026-09-03 "the storage was ours" verdict is recorded as CHALLENGED, not deleted, in both memory files; #308's
  first run is what settles it.
- **Affected knowledge docs:** `data_knowledge.md` § MNTN Matched pipeline, `data_catalog.md`
  § shopper_graph/openai_batch_submissions and § external.targeted_signal, memory
  `reference_openai_sdk_pagination` (TRAP 3 + ownership), `reference_mntn_matched_batch_pipeline` § 2026-09-04,
  `reference_airflow_ti` § KubernetesPodOperator env, glossary "Short page (cursor pagination)".

## Outcome (2026-09-04, evening) — the inventory reported and the answer was OURS
`#308` (empty-page paging + per-file `bytes`/`purpose`) and `#309` (survive a cursor deleted mid-listing) both
deployed. The sweep on submit `scheduled__2026-09-03T09:00` logged the whole store:

    Listed 129 files holding 4.2 GiB, 0.2% of the 2.5TB project limit. This pipeline holds 2.8 GiB.
       input  purpose=batch                80 files  2.8 GiB
       other  purpose=batch                11 files  1.1 GiB
       other  purpose=fine-tune            21 files  0.3 GiB
       other  purpose=assistants            5 files  0.0 GiB
       other  purpose=fine-tune-results    12 files  0.0 GiB

Everything not this pipeline's totals **1.4 GiB**. The 2.5 TB was a multi-day backlog of our own `part-` inputs the
short-page walk could never reach — one earlier run of the fixed sweep enumerated **5,527 deletable inputs holding
193.4 GiB**, aged 21.8-54.6h. **AUDI-1321's kill criterion never fired: no escalation to Alyson, no OpenAI dashboard
access.** `batch_submit` for `dt=2026-09-03` then wrote 1,004 receipts.

**The decision was right and its premise was wrong**, which is the part worth keeping: the "~2.4 TB unaccounted"
arithmetic was a correct steady-state footprint and an incorrect inference. **A footprint computed from one normal
day cannot see a store whose cleanup has been failing for a week.** Measuring rather than inferring is what settled
it; refusing to lower the retention window blind is what kept the measurement clean. The retention window was then
changed on evidence, in decision `0010`.
