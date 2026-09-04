---
doc_type: ticket
title: "AUDI-1321: OpenAI storage sweep could not see past the newest 10,000 files"
status: in_progress
date: 2026-09-03
summary: "The MNTN Matched keyword pipeline stalled 2026-08-28 on the OpenAI project's 2.5TB storage ceiling. Root cause: the nightly cleanup lists files newest-first and the API caps a page at 10,000, so on a heavy day every slot on that page is younger than the sweep's 48h delete floor and it frees nothing — exactly when churn is highest. Fix shipped in shopper_graph #306 (order=asc + explicit paging); the six blocked days still need backfilling. Split out of AUDI-1191, which caught it live."
result: "REOPENED 2026-09-04: batch_submit hit the storage 400 again. The 2026-09-03 fix was real but partial — order=asc (shopper_graph #306) cleared the wall once (1,132 of 1,132 deleted, first green submit since 08-28 after ~57 min), and the sweep still breaks its walk on a page shorter than the limit, so it has never enumerated its own store: it logged 28 files four minutes before batch_fetch deleted 416. Measured volumes put the whole pipeline at ~100 GB against the 2.5 TB cap, so 'the storage was ours' is challenged, not settled. #305's zero-delete alarm cannot fire because its 10,000 threshold is compared against a partial page. Next: shopper_graph #308 (page on an empty page, log per-file bytes + purpose). Backfill dt=2026-08-27..09-01 stays blocked behind it."
question: "Does a correctly ordered file sweep free enough of the OpenAI 2.5TB ceiling for batch_submit to run again, and how much of the 08-27 to 09-01 gap is worth recovering?"
framing_state: locked
---

# AUDI-1321: OpenAI storage sweep could not see past the newest 10,000 files

**Jira:** https://mntn.atlassian.net/browse/AUDI-1321
**Status:** in_progress
**Date Started:** 2026-09-03
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** Does a correctly ordered file sweep free enough of the OpenAI project's 2.5TB to let `batch_submit` run again, and how much of the 2026-08-27 → 09-01 gap is worth recovering once we know what one backfilled day actually costs?
- **Goal (why / the decision):** `keyword_ddp_reporting` has been blocked since 2026-08-28, so DDP product categorization is stale for the MNTN Matched keyword pipeline. The decision this answers: whether the storage ceiling is ours to manage (our sweep was broken, now fixed) or a shared-account problem that needs OpenAI dashboard access from Alyson. Tier 3 — a prod outage on the pipeline that feeds MNTN Matched keyword scoring.
- **Objective (done-when):** `batch_submit` completes without a 400 on the storage quota; the shipped sweep is proven to delete on a normal day; a zero-delete sweep raises an alarm; and each backfilled day either lands or carries a written reason it was skipped. Binary: `keyword_ddp_reporting` runs to success, or it does not.
- **Approach (how):** The fix is already in prod (shopper_graph #306: `order="asc"` + explicit paging + break at the first file inside the 48h window). Validate on today's submit run first, then backfill one day at a time — delete the partial receipts for `dt=D`, clear submit `D`, wait ~2h for the OpenAI batches, then clear fetch `D+1`. Backfill 08-27 first and price the real cost before committing to the remaining five. Ship the zero-delete alarm on AUDI-1279's open PR (shopper_graph #305) rather than a separate one, so the OpenAI pipeline has a single observability change to review (reversed 2026-09-03; the two touch different files, so there is no merge conflict, but splitting them would put two alarms for the same pipeline in two PRs). Assumption to resolve empirically before anything else: that the 2.5TB is held by files matching `part-*` / `batch_*` — our sweep only ever touches those names.
- **What would change the answer:** If `batch_submit` still returns a storage 400 after a sweep that provably deleted its full eligible set, the storage is not ours and the conclusion flips from "our cleanup was broken" to "the shared account is full." That is the stop line: escalate to Alyson for OpenAI dashboard visibility rather than chasing files this pipeline does not own. Equally, if backfilling 08-27 shows the recovered day is no longer useful downstream, the remaining five days are documented as a gap instead of replayed.

## 1. Introduction
The MNTN Matched keyword pipeline runs as two Airflow DAGs on Astro prod. `mntn_match_incrementals_submit`
uploads a day's keyword rows to OpenAI as batch input files and submits the batches;
`mntn_match_incrementals_fetch` reads the responses the next logical day. The cross-DAG contract is a GCS
file, not a sensor: submit logical `D` writes `openai_batch_submissions/dt=D`, and fetch logical `D+1`
reads `dt=D`. Downstream, `keyword_ddp_reporting` waits on `batch_post.product_categorization` through an
`ExternalTaskSensor`.

Both DAGs run a cleanup step (`delete_all_storage_files.py` in the `shopper_graph` repo) that deletes the
pipeline's own OpenAI files once they are older than 48 hours. That sweep is the only thing standing between
the pipeline and the OpenAI project's 2.5TB storage ceiling.

This ticket was split out of AUDI-1191. The debugger built there caught the failure live and root-caused it,
which is how the defect surfaced at all.

## 2. The Problem
`batch_submit` has returned a storage-quota 400 since 2026-08-28, so no keyword batches have been submitted
for six days and `keyword_ddp_reporting` has been blocked behind its sensor for the same period.

The cleanup sweep was the cause, not a victim. `GET /v1/files` caps a page at 10,000 files and defaults to
`created_at desc`, and the sweep only deletes files older than 48 hours. Once more than 10,000 files were
younger than 48 hours, the entire first page was ineligible and the sweep freed nothing — and it fails this
way precisely when churn is highest, which is when the storage matters. The logs show the collapse plainly:
`Total number of files to delete:` reads 13, 14, 28, 131, 181, 357, 788, 1170 on the way up, then 0 on every
run from 08-29 through 09-03.

Nothing alarmed on this. A sweep that deletes zero files looks identical to a sweep with nothing to do, so
the pipeline sat blocked until a human looked.

## 3. Plan of Action
1. Ship the ordering fix — `order="asc"` plus explicit paging, breaking at the first file inside the 48h
   window (shopper_graph #306). **Done 2026-09-03**, merged and deployed.
2. Validate on today's run before touching any history: clear `batch_cleanup_1` on submit logical 09-02 and
   confirm the sweep deletes a real set. **Done** — 1,132 of 1,132 deleted, 0 skips.
3. Confirm `batch_submit` clears the quota on that same run. This is the discriminating test for §0's kill
   criterion: a 400 here means the storage is not ours and the ticket escalates instead of continuing.
   **Done 2026-09-03** — it cleared. See §4.
4. Backfill `dt=2026-08-27` alone — delete its partial receipts, clear submit, wait ~2h for the OpenAI
   batches, then clear fetch `dt=2026-08-28`. Record the wall-clock and dollar cost.
5. Decide from step 4 whether the remaining five days (08-28 → 09-01) are worth replaying or are documented
   as a gap. Do not commit to all six up front.
6. Add an alarm on a sweep that deletes zero files, so this failure mode cannot recur silently.
   **Done 2026-09-03** on shopper_graph #305 (AUDI-1279's observability PR, commit `1d7fb2f`): the script
   now raises when every eligible delete fails, and when it frees nothing while at least
   `STORAGE_ALARM_MIN_FILES` (default 10,000) are still stored. The threshold is a file count standing in
   for bytes; normal volume is a few hundred to ~1,200 files a day, so a quiet run stays silent. The guard
   is straight-line script code with no import surface, so it carries no unit test.
7. Clear `keyword_ddp_reporting`'s `wait_for_product_categorization` once the categorization it waits on
   exists.

**Known false failure — but it is ONE assertion, not the task (corrected 2026-09-04):**
`product_categorization__max_dt` compares against wall-clock `date_sub(current_date, 2)`, so it fails on every
backfilled fetch day regardless of correctness. Mark `test_product_categorization` success ONLY when `max_dt` is
the sole failing assertion. On 2026-09-04 the failure was `product_categorization__record_count` (row count >= 99%
of `openai_batch_results_joined` at the same `dt`) and it was CORRECT; see §8.

## 4. Investigation & Findings

### The quota wall is resolved, and the ordering defect was the whole cause (2026-09-03)

Three observations, in the order they landed, each one a step of the discriminating test in §3:

1. **The fix deployed at 15:50.** shopper_graph #306 (`order="asc"` plus explicit paging, breaking at the
   first file inside the 48h retention window) merged 15:49:43 UTC and the image shipped via
   `deploy_openai_dockerhub_gcp.yml` minutes later.
2. **The next sweep deleted 1,132 of 1,132 eligible files with 0 skips.** Every `delete_all_storage_files.py`
   run from 2026-08-29 through 2026-09-03 on the old image had logged `Total number of files to delete: 0`.
   The count did not creep up; it went from nothing to the whole eligible set on the first run of the new
   code, which is what an ordering defect predicts and what a genuinely-full shared account does not.
3. **`batch_submit` on submit logical 2026-09-02 then SUCCEEDED at roughly 19:00 UTC after running about
   57 minutes.** Every prior attempt since 2026-08-28 had died in about 27 seconds on
   `400 ... exceeded your file storage quota. Projects are limited to 2.5TB`. This is the first green submit
   since 08-28. The 27-seconds-versus-57-minutes gap is itself the tell: the old failures never got past the
   upload, the new run did the work.

**The §0 kill criterion never triggered.** §0 set the stop line as "if `batch_submit` still returns a storage
400 after a sweep that provably deleted its full eligible set, the storage is not ours — escalate to Alyson
for OpenAI dashboard visibility rather than chasing files this pipeline does not own." The sweep provably
deleted its full eligible set and the submit went green on the same run, so the storage was ours and the
list-order defect accounted for the entire outage. No escalation, no dashboard ask, and the assumption in
§0's Approach (that the 2.5TB was held by files matching `part-*` / `batch_*`, the only names our sweep
touches) is confirmed by the fact that deleting only those names cleared the ceiling.

### A zero-delete sweep now alarms (shopper_graph #305, merged 18:39 UTC and deployed 2026-09-03)

`delete_all_storage_files.py` gained two raises: one when every eligible delete fails, and one when the sweep
frees nothing while at least `STORAGE_ALARM_MIN_FILES` (default 10,000) files are still stored. Normal daily
volume is a few hundred to about 1,200 files, so a genuinely quiet day stays silent and only the failure mode
that caused this outage trips the alarm.

**Why the threshold is a file count and not bytes:** the sweep already lists files and counts them, so the
count is free; bytes would need a second pass. 10,000 is the API's own page cap, which is the exact number at
which the old defect became total.

**This is the part worth carrying forward.** A sweep that deletes zero files is byte-for-byte
indistinguishable in the logs from a sweep with nothing to do. That is why the 2026-08-28 outage ran silent
for six days with a green cleanup task every single day. An operation whose success state and whose total
failure state emit the same output has no observability at all, however green it looks.

### Triggering a fetch by hand: the logical date is two steps removed from the dt it reads (2026-09-03)

Recovering `dt=2026-09-02` the same night took three attempts, and the first two failed on date arithmetic
rather than on anything about the pipeline. The DAG renders `yesterday` as
`{{ data_interval_start.subtract(days=1) }}` (`dags/machine_learning/mntn_match_incrementals_fetch.py` L43),
and for a `0 9 * * *` timetable a run's `data_interval_start` is one period BEFORE its logical date. So:

| attempt | logical_date | data_interval_start | dt read | outcome |
|---|---|---|---|---|
| 1 | 2026-09-03T09:00Z | 2026-09-02T09:00Z | 2026-09-01 | FileNotFoundError, wrong partition |
| 2 | 2026-09-04T09:00Z | 2026-09-03T09:00Z | 2026-09-02 | correct, but `run_after` 09-04T09:00 is in the future, so it sat queued |
| 3 | now, with `data_interval_start` and `data_interval_end` passed explicitly | 2026-09-03T09:00Z | 2026-09-02 | ran immediately |

**The rule:** `dt read = data_interval_start - 1 day`, and `run_after = data_interval_end`. To replay a past
partition NOW, POST the dag run with an explicit `data_interval_start` (the dt you want, plus one day) and a
`data_interval_end` in the past. Setting only `logical_date` gives you either the wrong partition or a run the
scheduler correctly refuses to start early. The memory note that read "fetch logical D+1 reads dt=D" holds
only for scheduled runs, where logical date and interval start coincide; it is wrong for manual ones.

**Recovery result:** `manual__recover3_dt_2026-09-02` cleared `batch_cleanup_1`, `batch_transition`,
`batch_fetch` and `batch_post.taxonomy_vector`, which is the first time the fetch side has moved past
`batch_transition` since 2026-08-28.

## 5. Solution

- **shopper_graph #306** (merged 2026-09-03 15:49:43 UTC, deployed 15:50): `delete_all_storage_files.py` lists
  `order="asc"` with explicit paging and breaks at the first file inside the 48h retention window. Ascending
  is also faster in steady state — page one starts at the oldest file and the loop exits at the cutoff,
  usually in one API call, where descending had to page past every young file to reach the old ones.
- **shopper_graph #305** (merged 2026-09-03 18:39:11 UTC, commit `85855ce`, deployed same day): the
  zero-delete alarm described in §4, shipped on AUDI-1279's observability PR rather than a separate one so the
  OpenAI pipeline had a single observability change to review.
- **Validated in prod the same day:** 1,132 of 1,132 deleted, `batch_submit` green on submit logical
  2026-09-02.

## 6. Questions Answered

- **Q:** Does a correctly ordered file sweep free enough of the OpenAI 2.5TB ceiling for `batch_submit` to run
  again?
  **A:** Yes, on the first run. One sweep on the new image deleted 1,132 of 1,132 eligible files and
  `batch_submit` succeeded about 57 minutes later, after six days of ~27-second storage 400s.
- **Q:** Is the storage ceiling ours to manage, or a shared-account problem needing OpenAI dashboard access?
  **A:** Ours. The §0 kill criterion never triggered. Deleting only the files our own sweep owns
  (`part-*` / `batch_*`) cleared the ceiling, so no other producer was holding the 2.5TB.
- **Q:** Can this failure mode recur silently?
  **A:** No longer. #305 raises on a zero-delete sweep while at least `STORAGE_ALARM_MIN_FILES` (default
  10,000) files are still stored, and on every-eligible-delete-failing.

## 7. Data Documentation Updates

- `knowledge/memory/reference_openai_sdk_pagination.md` — the trap-2 section marked RESOLVED with the green
  submit as the proof, and the general rule for age-based cleanups over cursor-paged list APIs.
- `knowledge/memory/reference_mntn_matched_batch_pipeline.md` — #305 moved from OPEN/not-deployed to merged
  and deployed; the zero-delete alarm recorded; the "no further submit clears until OpenAI storage is freed"
  blocker cleared.

**Capture 2026-09-04:**
- `knowledge/memory/reference_airflow_ti.md` — new section: an Astro deployment Environment-tab variable does
  not reach a `KubernetesPodOperator` pod (rendered pod spec as proof); `env_vars` is a `template_field`; the
  `STORAGE_ALARM_MIN_FILES` / `OPENAI_FILE_MAX_AGE_HOURS` corollary. Plus the Airflow 3.1.5 REST detail that
  there is no `/rendered-fields` sub-path.
- `knowledge/memory/reference_openai_sdk_pagination.md` — TRAP 3 (a short page is not the last page); the
  "page only while a page comes back full" fix line corrected in place; the 2026-09-03 storage-ownership
  verdict marked CHALLENGED by the 2026-09-04 sizing (appended, not overwritten).
- `knowledge/memory/reference_mntn_matched_batch_pipeline.md` — 2026-09-04 section: measured per-stage sizes,
  the `product_categorization` dbt python-model `FileExistsError`, the `keyword_ddp_reporting` DS19 lineage and
  the `ds_19` / `ds_19_domain` clearing race; the mark-success and storage-economics lines corrected in place.
- `knowledge/memory/feedback_check_which_dbt_assertion_failed.md` — NEW.
- `knowledge/memory/feedback_shared_worktree_commits.md` — the 2026-09-04 occurrence (commit `16ced108` swept
  this ticket's staged handoff).
- `knowledge/data_knowledge.md`, `knowledge/data_catalog.md`, `knowledge/bq/external/targeted_signal.md`,
  `knowledge/glossary.md` — sizes, the python-model gotcha, the DS19 producer/clearing facts, four terms.
- `knowledge/decisions/0009_storage_ownership_settled_by_byte_inventory.md` — NEW.
- `on-call/oncall_runbook.md` — the `test_product_categorization` mark-success line now requires naming the
  failing assertion.

## 8. Open Items / Follow-ups

- **Backfill `dt=2026-08-27..09-01`, one day at a time** (§3 steps 4-5). All five partial partitions still
  hold `was_submitted=False` receipts with live OpenAI batch ids, so each day's receipts must be deleted
  AGAIN before its re-run or the double-submission guard trips. `dt=2026-09-01` has no receipts at all.
- **Price 08-27 before committing to the remaining five** — §0 leaves open whether a recovered day is still
  useful downstream.
- `batch_test.test_product_categorization` will false-fail on every backfilled fetch day **on the
  `product_categorization__max_dt` assertion only** (wall-clock `current_date-2`). Mark it success only when
  `max_dt` is the sole failure. **Corrected 2026-09-04:** that day the failure was
  `product_categorization__record_count` — `product_categorization` row count >= 99% of
  `openai_batch_results_joined` at the same `dt` — and it was correct. The mark-success let a **408 MiB**
  partition through against a ~4.0-4.3 GB normal day; `keyword_ddp_reporting` consumed it and wrote two short
  downstream partitions that had to be backed up, deleted and rebuilt. Read the assertion name before marking
  anything green (memory `feedback_check_which_dbt_assertion_failed`).
- **Clear `keyword_ddp_reporting`'s `wait_for_product_categorization`** once `batch_post.product_categorization`
  lands on the recovered day.
- **Do not start a backfill while another day's batches are live at OpenAI.** Each submit uploads its whole
  input set, so replaying a day on top of an in-flight one is the same pressure that caused the ceiling. Run
  them strictly one at a time.
- **AUDI-1301 (backlog)** — dedicated OpenAI project, audit logging, and a permissions group; unchanged by
  this ticket.

### The unblock plan for dt=2026-09-03 does not work as written (2026-09-04)

Three things were found while executing the 2026-09-04 handoff. The first two invalidate its step 2.

**1. shopper_graph #307's env var cannot reach the pod it configures.** `OPENAI_FILE_MAX_AGE_HOURS`
set on the Astro prod deployment's Environment tab is invisible to `delete_all_storage_files.py`.
Proven from the rendered pod spec of the cleanup task that actually ran
(`GET .../taskInstances/batch_cleanup_1`, submit `scheduled__2026-09-03T09:00`):

    "env_vars": "[{'name': 'run_date', ...}, {'name': 'env', ...}, {'name': 'yesterday', ...}]"
    "env_from": []
    "pod_template_file": null
    "pod_template_dict": null

`MntnKubePodOperator` builds a fresh pod from `env_vars` plus one mounted secret key
(`OPENAI_API_KEY` from `env-secrets`). Astro deployment variables populate the scheduler and worker
environments and, for secret ones, the `env-secrets` k8s secret; neither path reaches a
KubernetesPodOperator pod, and the operator mounts a single named key rather than the whole secret.
Confirmed against `origin/main` (63d4c4b), not just the local checkout: both DAGs still pass bare
`default_env_vars` to `batch_cleanup_1` and `batch_cleanup_2`. Setting the window therefore needs a
change in `airflow-ti`, not an Astro setting. #307 is deployed (GH Actions run 33890342455, image
`steelhousedev/openai_batch_runner:gcp-prod`) and is inert until something passes the variable.

**2. The sweep's enumeration is incomplete, so the retention window is not the binding constraint.**
`batch_cleanup_1` on both DAGs at 2026-09-04 09:00 logged `Deleted 0 of 0 files, having listed at
least 28.` Four minutes later `batch_fetch` downloaded and deleted 416 OpenAI output files. Those
416 existed at 09:00 and were not among the 28, so the listing the sweep acts on is not the store.

The `seen` readings are first-page sizes, and the loop ends on any short page:

    if too_young or len(files) < PAGE:
        break

`order="asc"` puts the oldest file first, so when everything is inside the retention window the
inner loop breaks on file one and the outer loop breaks immediately; and a page shorter than
`PAGE` is treated as the last page rather than checking `has_more`. Readings ran 4,622 / 4,621 /
4,623 / 4,622 through 09-04 05:11, then 28 from 09:00 onward, with no delete in between that could
explain the drop. This is the same class of defect as AUDI-1321's original finding: the sweep
reasons about a list it never fully retrieves.

**3. The zero-delete alarm from #305 cannot fire here.** `ALARM_MIN_FILES` defaults to `PAGE`
(10,000) and `seen` has never exceeded 4,623, so `not deleted and seen >= ALARM_MIN_FILES` stayed
false through the entire block. `batch_cleanup_1` went green at 09:00 having freed nothing, and
`batch_submit` died on the storage 400 at 10:45. The threshold was calibrated on an assumed few
hundred to ~1,200 files a day; the number it is compared against is a partial page, not a count.

**What is still unknown, and the check that settles it.** No log records file sizes, so whether the
2.5TB is this pipeline's is still unproven either way. dt=2026-09-02 has 895 batches whose ~895
`part-*` inputs were uploaded 2026-09-03 18:11:49-19:25:38 and are ~20-21h old; 427 of those batches
were still `in_progress` at 09:44 on 09-04, so their inputs are attached to live batches and their
outputs have not landed. §0's kill criterion ("if `batch_submit` still returns a storage 400 after a
sweep that provably deleted its full eligible set, the storage is not ours") is not yet met, because
the sweep has not provably enumerated its eligible set at all. The discriminating test is to make the
sweep report `file.bytes` and `file.purpose` per file plus a total, and page on `has_more`. That is a
shopper_graph-only change, deploys in about two minutes on the workflow already exercised today, and
answers the ownership question directly instead of by inference.

**Sequencing note.** The five older days (08-27 to 09-01) stay blocked behind this. Do not lower a
retention window and re-run submit until the sweep can show what it is deleting and how much space
it frees.
