---
doc_type: ticket
title: "AUDI-1321: OpenAI storage sweep could not see past the newest 10,000 files"
status: in_progress
date: 2026-09-03
summary: "The MNTN Matched keyword pipeline stalled 2026-08-28 on the OpenAI project's 2.5TB storage ceiling. Root cause: the nightly cleanup lists files newest-first and the API caps a page at 10,000, so on a heavy day every slot on that page is younger than the sweep's 48h delete floor and it frees nothing — exactly when churn is highest. Fix shipped in shopper_graph #306 (order=asc + explicit paging); the six blocked days still need backfilling. Split out of AUDI-1191, which caught it live."
result: "Quota RESOLVED and proven ours (2026-09-04 evening). With shopper_graph #308 (page to an empty page + per-file byte/purpose inventory) and #309 (survive an `after` cursor deleted mid-listing) deployed, the sweep enumerated the whole OpenAI project store at 129 files / 4.2 GiB, 0.2% of the 2.5 TB cap: this pipeline 2.8 GiB, everything else 1.4 GiB. The ceiling was a multi-day backlog of our own `part-` inputs the short-page listing could never reach; §0's kill criterion never fired, so no escalation and no OpenAI dashboard access is needed. `dt=2026-09-03` submitted (1,004 receipts) and `dt=2026-08-27` re-submitted in full (1,261 receipts) after its 742 old batches probed expired=612/completed=128 and proved unharvestable. Two further defects fixed on the way: a 12h input retention window failed 119 live batches because OpenAI's completion window is 24h (#310 raises the default to 26h, deployed), and the double-submission guard keys on `openai_batch_id.notna()` rather than `was_submitted`, so a dead batch's input is never retried. SEVEN days still to finish: `dt=2026-09-02` is only 46% fetched (468 of 1,014 receipts) and `dt=2026-08-28..09-01` are untouched."
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
  **A:** Ours, and now proven by measurement rather than by inference. The §0 kill criterion never
  triggered. **Settled 2026-09-04 evening:** with #308 and #309 deployed the sweep enumerated the whole
  store at 129 files / 4.2 GiB (0.2% of the cap) — this pipeline 2.8 GiB, everything else 1.4 GiB
  (fine-tune 0.3, other `purpose=batch` 1.1, assistants and fine-tune-results ~0). One earlier run of the
  fixed sweep enumerated 5,527 deletable inputs holding 193.4 GiB aged 21.8-54.6h, which is what the
  ceiling actually was. The 2026-09-04 morning challenge (~2.4 TB apparently unaccounted, from a ~100 GB
  steady-state footprint) was a correct footprint and a wrong inference: a one-normal-day number cannot
  see a store whose cleanup has been failing for a week. No escalation to Alyson, no dashboard access.
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

**Capture 2026-09-04 (evening):**
- `knowledge/memory/reference_openai_sdk_pagination.md` — TRAP 3 marked FIXED (#308); **new TRAP 4**, the `after`
  cursor file deleted mid-listing by the concurrently scheduled sweep (#309); the OWNERSHIP section rewritten from
  CHALLENGED to SETTLED with the 129-file / 4.2 GiB inventory, and the trap-2 verdict pointer corrected in place.
- `knowledge/memory/reference_mntn_matched_batch_pipeline.md` — new 2026-09-04 (evening) section carrying the
  inventory, the 26h input-retention rule, the unrecoverable-batch rule, the one-day-at-a-time rule, the fetch
  DAG's `max_active_runs=1`, the `record_count` completeness hole, the cohort-probe recipe and measured task
  timings; the storage-economics, 09-03 verdict, 2.4 TB and still-open lines all corrected in place; the
  dead-cohort recovery procedure gained a probe step and the real reason the receipt delete is mandatory.
- `knowledge/data_knowledge.md` — settled ownership, the fourth pagination trap, the 26h retention rule, the
  unrecoverable-batch rule, the serial-replay rule with `max_active_runs=1`; the guard's true key
  (`openai_batch_id.notna()`) added to the receipts bullet, `record_count`'s completeness hole added to the dbt
  bullet, and the "#305 PR open, NOT deployed" line corrected to merged and deployed.
- `knowledge/data_catalog.md` § shopper_graph/openai_batch_submissions — the "~2.4 TB is not this pipeline's"
  line corrected to the measured 4.2 GiB store; receipt-count-as-the-only-completeness-check, the guard key, and
  the measured task timings added.
- `knowledge/glossary.md` — three new rows (Input retention window, Cohort probe, Expired batch); Short page and
  Zero-delete sweep updated to say the partial-page defect is fixed.
- `knowledge/decisions/0009_storage_ownership_settled_by_byte_inventory.md` — Outcome section: the inventory
  reported and the answer was ours; the decision was right and its premise was wrong.
- `knowledge/decisions/0010_openai_input_retention_26h.md` — NEW.
- `knowledge/memory/feedback_check_which_dbt_assertion_failed.md` — `record_count` PASSING proves nothing about
  completeness; a test comparing two outputs of the same upstream can only prove consistency.
- `knowledge/memory/reference_pr_gauntlet.md` — never hand-write `.git/pr_gauntlet_pass`; shopper_graph CI also
  runs `isort` (`force_single_line`) and treats the repo's own `openai/` directory as first-party.
- `knowledge/bq/external/targeted_signal.md` — the dt=2026-09-03 rebuild sizes and the open check that settles
  whether the DS19 shortfall is upstream incompleteness.

## 8. Open Items / Follow-ups

- **Backfill is SEVEN days, not five, one day at a time (restated 2026-09-04 evening).** `dt=2026-09-02`
  is 46% fetched, not recovered (468 of 1,014 receipts downloaded), and `dt=2026-08-28`, `08-29`, `08-30`,
  `08-31`, `09-01` are untouched. `dt=2026-08-27` was re-submitted in full on 2026-09-04 (1,261 receipts)
  and needs its fetch. **The batch ids on the old partial receipts are NOT live** — 08-27's probed
  expired=612 / completed=128 / in_progress=2 and even the completed ones 404 on
  `files.content(output_file_id)`, so budget every remaining day as a full re-submit. Delete each day's
  receipts first: the guard keys on `openai_batch_id.notna()`, not `was_submitted`, so a dead batch's input
  file is otherwise never retried and the re-run silently leaves the day short. `dt=2026-09-01` has no
  receipts at all.
- **Probe before deleting any day's receipts.** Clear `batch_transition` on the fetch run whose
  `data_interval_start` is `D+1`; it prints the `cohort dt=D:` status split and is non-destructive apart
  from flipping `was_submitted=True` on progressed rows. It only examines rows where
  `was_downloaded == False & was_submitted == False`, so its `n` is not the cohort size.
- **Keep backfill fetch work out of the 09:00 UTC window.** `mntn_match_incrementals_fetch` has
  `max_active_runs=1` (submit has 16, verified `GET /dags/{dag_id}`), so any backfill probe or fetch holds
  the only slot and blocks the daily fetch unless it finishes first.
- **Re-read the two DS19 partitions once the days are whole.** `targeted_signal/data_source_id=19/dt=2026-09-03`
  rebuilt to 50.6 GB against ~70-72 GB normal and `targeted_signal_domain/dt=2026-09-03` to 36.4 GB against
  ~44.5 GB. If they rise after the backfill, the shortfall was upstream incompleteness; if they stay put,
  the rebuild has another cause. Not a settled finding either way.
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

### The quota was ours after all, and dt=2026-09-03 is submitted (2026-09-04, evening)

**The storage went from the ceiling to 4.2 GiB.** With PR #308 and #309 deployed, the sweep on
submit `scheduled__2026-09-03T09:00` logged:

    Listed 129 files holding 4.2 GiB, 0.2% of the 2.5TB project limit. This pipeline holds 2.8 GiB.
       input  purpose=batch                80 files  2.8 GiB
       other  purpose=batch                11 files  1.1 GiB
       other  purpose=fine-tune            21 files  0.3 GiB
       other  purpose=assistants            5 files  0.0 GiB
       other  purpose=fine-tune-results    12 files  0.0 GiB

Everything not this pipeline's totals 1.4 GiB. **§0's kill criterion never fired.** The 2.5 TB was
a multi-day backlog of our own `part-` inputs that the old short-page listing could not reach: one
run of the fixed sweep enumerated 5,527 deletable inputs holding 193.4 GiB, aged 21.8h to 54.6h.
The earlier reading of "28 files" was a first page, never a count. No escalation to Alyson is
needed and no OpenAI dashboard access is required.

**`batch_submit` for dt=2026-09-03 succeeded** 18:01:52 → 19:24:29 UTC, writing 1,004 receipts to
`openai_batch_submissions/dt=2026-09-03/`, one per input file. Every prior attempt since 08-28 died
in about 30 seconds on the storage 400. The submit DAG run is green.

**Two defects found only because the sweep finally reported bytes.** First, the paging cursor can be
deleted underneath the listing: both DAGs sweep on `0 9 * * *`, so the other run deletes the file our
`after` cursor names and the 404 aborted the whole run after it had enumerated 5,527 files. PR #309
stops listing, warns with the cursor id, and deletes what was validly enumerated; a truncated
listing never trips the quota alarm because its counts are partial. That warning fired on the
successful run, so the race is real and routine, not theoretical. Second, the retention window was
wrong in kind, not degree: an input is spent once its batch is created, minutes after upload, while
only outputs must outlive the next day's fetch. Inputs now expire at 12h and outputs stay at 48h,
which frees a stalled day without any environment variable the pod cannot receive.
**Superseded later the same day: 12h is below OpenAI's 24h completion window and failed 119 live
batches; shopper_graph #310 raises the input default to 26h. See the next section and decision
`knowledge/decisions/0010_openai_input_retention_26h.md`.**

### The dt=2026-09-02 repair, and what it says about the remaining gap

`product_categorization` dt=2026-09-02 rebuilt to 4,336,836,351 bytes across 50 parquet files,
against 4,342,770,189 / 49 on 2026-08-26: a 0.14% difference. `test_product_categorization` then
passed. The short 408 MiB partition is backed up at
`gs://mntn-data-archive-prod/_backups/audi_1321/product_categorization_dt=2026-09-02_20260904/`.

Downstream, `keyword_ddp_reporting manual__consume_dt_2026-09-02` had consumed the short partition
and written dt=2026-09-03 (the DAG uses `run_date = {{ ds }}`). Both affected outputs were backed up
under `_backups/audi_1321/`, deleted and rebuilt:

| output | before | after | normal |
|---|---|---|---|
| `targeted_signal/data_source_id=19/dt=2026-09-03` | 44.6 GB | 50.6 GB | ~70-72 GB |
| `targeted_signal_domain/dt=2026-09-03` | 34.2 GB | 36.4 GB | ~44.5 GB |

`data_source_id=13` (110.6 GB) and `=4` (158.3 GB) were never affected; only the DS19 path reads
`product_categorization`.

**Both rebuilds recovered, and both are still short of a normal day.** That is the open question, not
a settled finding. The hypothesis is that the DS19 signal for a given day draws on a categorization
history that still has the 2026-08-28 → 09-01 hole in it, so it cannot reach full size until those
days are backfilled. **The check that settles it:** after 08-27 through 09-01 are recovered, re-read
these two partitions. If they rise toward ~71 GB and ~44.5 GB the hypothesis holds; if they stay near
50.6 GB and 36.4 GB the shortfall has another cause and the rebuild is not yet correct. Do not treat
the current numbers as a clean recovery until that check is done.

**No downstream cascade.** `external.targeted_signal` is a BigQuery external table over that GCS
prefix, and the only DAGs touching it are producers (`keyword_ddp_reporting`, `targeted_signal_crm`).
The DDP usage report reads it live at month end, so correcting the parquet corrects every reader
retroactively, provided it is corrected before the monthly run.

### The backfill is bigger than six days, and a 12h input window was making it worse (2026-09-04, late)

**dt=2026-09-02 is not recovered. It is 46% fetched.** Its 1,014 receipts read 468 downloaded, 427
submitted-not-downloaded, 119 neither. `openai_batch_results/dt=2026-09-02` is 20.3 GB against
46.2 GB on 2026-08-26. The `product_categorization` rebuild earlier today was correct *relative to
what had been fetched* and is still short of the real day.

**`product_categorization__record_count` cannot detect this.** It asserts that
`product_categorization` is at least 99% of `openai_batch_results_joined` at the same dt. Both are
built from whatever was fetched, so a half-fetched day passes cleanly. The test proves internal
consistency, never completeness. Anything checking a backfilled day for completeness has to compare
against the receipt count, not against a sibling table.

This also supersedes the earlier hypothesis about the short DS19 partitions. `targeted_signal`
`data_source_id=19/dt=2026-09-03` came back at 50.6 GB against ~71 GB normal, and the likelier cause
is that dt=2026-09-02 itself is 46% complete, not the 08-28 → 09-01 hole. Both remain untested until
the days are actually whole.

**A 12h input retention window fails live batches.** `batch_transition` on the 119 untransitioned
receipts for dt=2026-09-02 returned `failed=119, expired=0`, immediately after the 2026-09-04 18:00
sweep deleted their input files at the 12h mark. The 468 that had already completed were untouched.
OpenAI's batch completion window is 24h, so an input must outlive it; 12h never could. shopper_graph
#310 raises the default to 26h and is deployed. Without it, the 2026-09-05 09:00 sweep would have
deleted dt=2026-09-03's inputs at 07:24 and failed all 1,004 of its batches the same way.

**A batch that lapses is unrecoverable, and so is a completed one whose output was swept.** The 742
batches a prior session created for dt=2026-08-27 on 09-03 04:53-05:41 came back
`expired=612, completed=128, in_progress=2`. Harvesting the 130 failed anyway: `batch_fetch` 404s on
`files.content(output_file_id)`, so those outputs are gone from OpenAI. Every one of the 742 had to
be re-submitted. Expect the same for 08-28 (791), 08-29 (653), 08-30 (510) and 08-31 (733); 09-01
has no receipts at all.

**Why those 3,429 batches existed.** On 2026-09-03 a prior session cleared all five blocked days'
submit runs in parallel, between 04:15 and 06:13. Five days of inputs at ~40 GB each is what
exhausted the 2.5 TB, and every batch created before the wall hit then sat unfetched until it
lapsed. One day at a time is not a stylistic preference; parallel replay is what caused this.

### The corrected per-day backfill recipe

The 2026-09-04 handoff says to delete the partial receipts and re-run submit. That is right, but not
for the stated reason, and the probe step is not optional.

1. **Probe before deleting.** Clear `batch_transition` on the fetch run whose
   `data_interval_start` is D+1, which reads `dt=D`. It polls every receipt's `openai_batch_id` and
   prints a `cohort dt=D:` line with the status split. The receipts are NOT empty rows: every one
   carries a real, unique batch id, so deleting them blind can discard recoverable work.
2. **Harvest only what the probe says is `completed`,** and expect it to fail: an output file older
   than the sweep's window is already gone. Clear `batch_fetch` alone, never with downstream, or
   `batch_post` builds a short partition off a fraction of the day.
3. **Back up the receipts,** then delete them so the double-submission guard cannot skip the dead
   batches. `get_files_without_batch` keys on `openai_batch_id.notna()`, **not** `was_submitted`, so
   a receipt naming an expired batch is treated as already submitted and its file is never retried.
   That is the trap: without deleting, a re-run submits only the never-attempted files.
4. **Clear `batch_submit`** on submit `scheduled__D T09:00`. About 1h20m for ~1,260 files.
5. **Fetch the next day,** then rebuild `product_categorization` and the DS19 outputs once the day is
   actually whole, not before.

**Do one day at a time and verify the receipt count matches the input count before moving on.**

---

## Correction, 2026-09-05: the 2.5 TB is a company-shared pool, not this project's own

**This contradicts the "Quota RESOLVED and proven ours" verdict in the front matter and § "The
storage went from the ceiling to 4.2 GiB". Both are kept above; this section is the settling
evidence.** Per the rule that a contradiction is appended rather than overwritten, here is each
side and what decided it.

**The claim as recorded 2026-09-04.** The sweep, with #308 and #309 deployed, enumerated the store
at 129 files / 4.2 GiB, 0.2% of the cap, of which 1.4 GiB was not this pipeline's. A submit
succeeded minutes later. Conclusion drawn: the ceiling had been our own multi-day `part-` backlog
that the short-page listing could never reach, so §0's kill criterion never fired and no dashboard
access was needed.

**What happened 2026-09-05.** dt=2026-08-28's `batch_submit` failed at 20:37 UTC with the same
`400 ... Projects are limited to 2.5TB of files`, about 640 of 1,241 files in. Two consecutive
sweeps then read:

```
Listed 6221 files holding 198.8 GiB, 7.8% of the 2.5TB project limit. This pipeline holds 198.5 GiB.
Listed 6040 files holding 191.8 GiB, 7.5% of the 2.5TB project limit. This pipeline holds 191.6 GiB.
```

Every file the API key can enumerate totals under 200 GiB, 7.8% of the stated cap, and the upload
is still rejected deterministically.

**What settles it: Malachi, 2026-09-05.** The OpenAI account is the company's default project and
the 2.5 TB is shared across every team using it. Our key lists only what we uploaded, so the sweep
is structurally blind to the rest of the pool. That is a person's direct knowledge of an account we
cannot inspect, on a question our instrumentation cannot answer, so it outranks an inference drawn
from a partial listing.

**Which reasoning was wrong, specifically.** Not the measurement. The 4.2 GiB reading was correct
and so was the 198.8. The error was treating "a submit succeeded right after our sweep" as evidence
that our files had been the ceiling. A shared pool predicts exactly the same observation: our
deletions made room in someone else's pool. The original handoff of 2026-09-04 read it as
"roughly 2.4 TB of the ceiling is not this pipeline's", and that reading is restored.

**What follows.**

- **§0's kill criterion is live again.** We cannot free this ceiling and cannot see it. Headroom
  depends on other teams' usage and can vanish without anything changing on our side.
- **The zero-delete alarm scoped to our own bytes (#308) was right for a reason we had not
  established.** A quota filled by another team must never fail `batch_cleanup_1`, which is upstream
  of every task in both DAGs.
- **Our own slice is what we control, and it is too fat.** We reached roughly 200 GiB by holding
  three days of inputs at once. An input is spent the moment its batch is created, minutes after
  upload, so the 26h window exists only to protect against a re-run needing the file. Cutting the
  input window to a few hours would take our footprint from about 140 GiB to about 50.
- **The durable ask is a dedicated OpenAI project for this pipeline.** It converts an unbounded
  shared risk into a quota we own and can reason about. Until then every submit is racing other
  teams for space.

Recorded in the plan: `outputs/audi_1321_backfill_plan_2026_09_05.md`.
