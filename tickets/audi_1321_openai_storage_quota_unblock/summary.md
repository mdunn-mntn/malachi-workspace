---
doc_type: ticket
title: "AUDI-1321: OpenAI storage sweep could not see past the newest 10,000 files"
status: backlog
date: 2026-09-03
summary: "The MNTN Matched keyword pipeline stalled 2026-08-28 on the OpenAI project's 2.5TB storage ceiling. Root cause: the nightly cleanup lists files newest-first and the API caps a page at 10,000, so on a heavy day every slot on that page is younger than the sweep's 48h delete floor and it frees nothing — exactly when churn is highest. Fix shipped in shopper_graph #306 (order=asc + explicit paging); the six blocked days still need backfilling. Split out of AUDI-1191, which caught it live."
result: "not started — #306 merged and deployed 2026-09-03 (first live sweep deleted 1,132 of 1,132 with 0 skips, where every run since 08-29 deleted 0). Submit run for logical 2026-09-02 in flight as the quota test; backfill of dt=2026-08-27..09-01 pending its outcome."
question: "Does a correctly ordered file sweep free enough of the OpenAI 2.5TB ceiling for batch_submit to run again, and how much of the 08-27 to 09-01 gap is worth recovering?"
framing_state: locked
---

# AUDI-1321: OpenAI storage sweep could not see past the newest 10,000 files

**Jira:** https://mntn.atlassian.net/browse/AUDI-1321
**Status:** backlog
**Date Started:** 2026-09-03
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** Does a correctly ordered file sweep free enough of the OpenAI project's 2.5TB to let `batch_submit` run again, and how much of the 2026-08-27 → 09-01 gap is worth recovering once we know what one backfilled day actually costs?
- **Goal (why / the decision):** `keyword_ddp_reporting` has been blocked since 2026-08-28, so DDP product categorization is stale for the MNTN Matched keyword pipeline. The decision this answers: whether the storage ceiling is ours to manage (our sweep was broken, now fixed) or a shared-account problem that needs OpenAI dashboard access from Alyson. Tier 3 — a prod outage on the pipeline that feeds MNTN Matched keyword scoring.
- **Objective (done-when):** `batch_submit` completes without a 400 on the storage quota; the shipped sweep is proven to delete on a normal day; a zero-delete sweep raises an alarm; and each backfilled day either lands or carries a written reason it was skipped. Binary: `keyword_ddp_reporting` runs to success, or it does not.
- **Approach (how):** The fix is already in prod (shopper_graph #306: `order="asc"` + explicit paging + break at the first file inside the 48h window). Validate on today's submit run first, then backfill one day at a time — delete the partial receipts for `dt=D`, clear submit `D`, wait ~2h for the OpenAI batches, then clear fetch `D+1`. Backfill 08-27 first and price the real cost before committing to the remaining five. Build the zero-delete alarm here rather than folding it into AUDI-1279, so the guard ships with the defect that needed it. Assumption to resolve empirically before anything else: that the 2.5TB is held by files matching `part-*` / `batch_*` — our sweep only ever touches those names.
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
4. Backfill `dt=2026-08-27` alone — delete its partial receipts, clear submit, wait ~2h for the OpenAI
   batches, then clear fetch `dt=2026-08-28`. Record the wall-clock and dollar cost.
5. Decide from step 4 whether the remaining five days (08-28 → 09-01) are worth replaying or are documented
   as a gap. Do not commit to all six up front.
6. Add an alarm on a sweep that deletes zero files, so this failure mode cannot recur silently. Built here
   rather than folded into AUDI-1279, so the guard ships with the defect that needed it.
7. Clear `keyword_ddp_reporting`'s `wait_for_product_categorization` once the categorization it waits on
   exists.

**Known false failure:** `batch_test.test_product_categorization` compares against wall-clock `max_dt`, so it
fails on every backfilled fetch day regardless of correctness. Mark it success; do not rerun it.

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
