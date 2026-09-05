---
name: reference_mntn_matched_batch_pipeline
description: "mntn_match_incrementals_{submit,fetch} DAG mechanics: task chains, the cross-DAG contract (a GCS submissions file, NOT an Airflow sensor), backfill order one day at a time, the identical batch_cleanup bookend, the OpenAI input retention floor of 26h (12h fails live batches), why a lapsed batch is unrecoverable, and how image_pull_policy=Always makes a task's image depend on WHEN its pod ran vs deploy time."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [was_submitted flag, dead cohort, dead-cohort recovery, batch_fetcher status completed, get_files_without_batch, inconsistent state guard, double-submission guard, orphan formatted files, openai batch dashboard access, submit run_date logical date, openai_batch_input_formatted, delete submissions receipts, resubmission procedure, mntn_match_incrementals_submit, mntn_match_incrementals_fetch, batch_submit, batch_transition, batch_fetch, batch_prep, batch_validate, batch_post, batch_cleanup, batch_cleanup_1, batch_cleanup_2, batch_test, submit_batch.py, transition_batch.py, fetch_results.py, batch_transitioner, delete_all_storage_files, openai_batch_submissions, cross-dag contract, gcs submissions file, backfill order, mntn matched batch pipeline, DS19 keyword pipeline, openai batch runner, OPEN_AI_BATCH, SHOPPER_GRAPH, image_pull_policy Always, machine_learning dags, dt yesterday contract, FileNotFoundError submissions, openai file storage quota, 2.5TB quota, 30-day file expiry, openai auto-expire files, storage economics, 75 GiB per day, intermittent quota failure, quota fails intermittently, delete_all_storage_files economics, batch_test dbt tests, product_categorization__max_dt, max_dt freshness test, current_date backfill skew, dbt test backfill footgun, mntn_matched_data_quality, post_batch dbt tests, mark test success backfill, keyword_ddp_reporting, keyword_ddp not blocked by batch_test, wait_for_product_categorization, ExternalTaskSensor product_categorization, OSError errno 99 email red herring, IMP-016, IMP-017, alyson dashboard access, one identical error message, ryan kleck openai org reauthentication, org-side outage, org-ldKlX0Pr81MhoY05W9t6oB1V, cannot find file organization access, okta enterprise sso login, audit logging api.admin, AUDI-1301, dedicated openai project, batch_requests manual test batch, usage tier 5, six-day dead cohort, recovery executed 2026-09-02, batch_6a9843249bcc8190bb3b7f6eccedab49, partial receipts partition, rerun submits quota failed, receipts rewritten new batch ids, object count not cohort size, AUDI-1279, shopper_graph#305, DeadCohortError, dead cohort alarm, batch_status.py, DEAD_COHORT_MIN_AGE_HOURS, retrieve_error, per-batch status line, cohort summary line, assert_cohort_alive, retrieve_batch, PYTHONUNBUFFERED, Batch.status literal, request_counts, openai 3.7.0, pandas 3 str dtype, unpinned image, monitor-emr, JobTeamConfig.ML, severity 5 no pagerduty, shopper_graph#306, order asc cleanup, files.list 10000 cap, cleanup deletes nothing, 404 No such File object race, cleanup race two dags, Total number of files to delete 0, quota root cause list order, delete receipts again partial, AUDI-1321, quota wall closed, first green submit since 08-28, batch_submit 57 minutes, 1132 of 1132 deleted, storage was ours, kill criterion never triggered, zero-delete alarm, STORAGE_ALARM_MIN_FILES, shopper_graph 305 merged deployed, 85855ce, shopper_graph#307, shopper_graph#308, per-file bytes purpose inventory, openai_batch_input_formatted 1014 files, 40 MB per input file, 40.3 GB inputs, 46 GB results, 100 GB footprint, 2.4 TB unaccounted, 40 GB headroom, short page break, listed at least 28, ALARM_MIN_FILES partial page, STORAGE_ALARM_MIN_FILES never set, OPENAI_FILE_MAX_AGE_HOURS inert, MntnKubePodOperator env_vars, astro deployment variable not in pod, product_categorization python model, dbt python model append, incremental_strategy append, FileExistsError partition, delete partition before re-running, 408 MiB partition, product_categorization__record_count, keyword_ddp_reporting run_date ds, manual__consume_dt, write_targeted_signal_ds_19, write_targeted_signal_ds_19_domain, write_targeted_signal_ds_13, targeted_signal 70 GB, targeted_signal_domain 44.5 GB, ds_19 ds_19_domain clearing race, sequential task chain, shopper_graph#309, shopper_graph#310, OPENAI_FILE_MAX_AGE_HOURS 26h, input retention window, 12h retention fails batches, 24h completion window, batch_transition failed=119, expired batch unrecoverable, files.content 404 output swept, cohort probe recipe, cohort dt line, max_active_runs 1 fetch, max_active_runs 16 submit, backfill outside 09:00 window, one day at a time backfill, parallel replay caused the ceiling, 3429 batches lapsed, 46 percent fetched, receipt count completeness check, record_count internal consistency only, batch_submit 84 minutes, product_categorization 31 minutes, write_targeted_signal_ds_19 1.7 hours, storage ownership settled, 129 files 4.2 GiB, 5527 deletable inputs 193.4 GiB]
domain: [repos, infra]
lifecycle: active
last_verified: 2026-09-04
---
**The two DAGs that run the MNTN Matched (DS19 keyword) OpenAI batch pipeline** (`SteelHouse/airflow-ti`,
`dags/machine_learning/`). Both schedule **`0 9 * * *`, `catchup=False`**. Deploy/image routing lives in
[[reference_shopper_graph_deploy]]; incidents in [[reference_oncall_runbook]] (INC-006 fetch, INC-007 submit
quota); business flow in `knowledge/data_knowledge.md` "MNTN Matched pipeline".

## Task chains
- **`mntn_match_incrementals_submit`** (severity 1): `batch_cleanup_1 >> batch_prep >> batch_validate >>
  batch_submit >> batch_cleanup_2`. **`batch_submit`** (`submit_batch.py`) uploads the batch input + creates
  the OpenAI batch, then writes `gs://mntn-data-archive-prod/shopper_graph/openai_batch_submissions/dt={run_date}`.
- **`mntn_match_incrementals_fetch`** (severity 5): `batch_cleanup_1 >> batch_transition >> batch_fetch >>
  batch_post >> [batch_test, batch_cleanup_2]`. **`batch_transition`** (`transition_batch.py` →
  `batch_transitioner.transition_to_in_progress`) reads `openai_batch_submissions/dt={yesterday}` where
  **yesterday = `data_interval_start − 1 day`**. `batch_fetch` (`fetch_results.py`) needs the OpenAI batch to
  be **complete**; PR **#296** makes it skip incomplete/errored batches gracefully instead of aborting.

## The cross-DAG contract is a GCS FILE, not an Airflow sensor
Submit logical **D** writes `dt=D`; fetch logical **D+1** reads `dt=D`. There is **no Airflow sensor** wiring
the two DAGs — the handoff is entirely the `openai_batch_submissions/dt=` object.
- **Backfill order:** run **submit-D first, then fetch-(D+1)**.
- **Diagnostic:** fetch `batch_transition` `FileNotFoundError` on `dt=D` ⇒ **submit-D never produced the file**
  (submit failed), not a fetch bug (this was the INC-007 fetch-side corroboration).

## Submissions-parquet flags = the only automated visibility into batch health (2026-08-29)
No one checked (malachi, Matt Brorby) has platform.openai.com/batches dashboard access — it shows
nothing for either account. The API-flag evidence path in the `openai_batch_submissions/dt=` parquet
is therefore the ONLY visibility:
- **`batch_transitioner` sets `was_submitted=True` only when the OpenAI API reports the batch
  `in_progress`/`completed`.** `batch_fetcher` downloads only `status=completed` batches and skips
  others.
- **Diagnostic: "0/N rows flagged across two transition passes" is CONCLUSIVE evidence of a dead
  cohort** — no dashboard needed. Baseline 2026-08-26: 1113/1113 flagged + downloaded; the 08-27
  cohort: 0/1098.
- **The submit pod's `run_date` env = the LOGICAL date;** the wall-clock day is logical+1.
- **UPDATE 2026-08-31 (corrects the reach of the claim above, appended not overwritten): Alyson
  HAS platform.openai.com dashboard access** — the 2026-08-29 check covered only malachi and
  Matt Brorby. On the 08-27..30 dead cohorts she reports ALL failed batches show ONE identical
  error message; the text landed 2026-08-31: `Cannot find file <id>, or organization ... does
  not have access to it` (org-side proof section below). **Ryan Kleck has an MNTN OpenAI org in his account
  switcher needing reauthentication** — a second potential dashboard path. The parquet flags
  remain the only AUTOMATED, key-free visibility; human confirmation now exists via Alyson.

## Dead-cohort recovery procedure (executed 2026-08-29, Matt Brorby approved in #alerts-tpa-pipeline)
**Step 0, added 2026-09-04: PROBE before deleting anything** (the receipts are not empty rows — every one carries a
real, unique batch id, so a blind delete can discard recoverable work). Clear `batch_transition` on the fetch run
whose `data_interval_start` is `D+1`; it polls every receipt and prints the `cohort dt=D:` status split. Recipe and
its blind spot in the 2026-09-04 (evening) section below.
1. Delete `gs://mntn-data-archive-prod/shopper_graph/openai_batch_submissions/dt=<D>/` — receipts
   only; the inputs in `openai_batch_input_formatted/dt=<D>` survive. **The delete is mandatory even when the
   receipts look "already submitted": the double-submission guard keys on `openai_batch_id.notna()`, not on
   `was_submitted`, so a receipt naming a DEAD batch makes its input file invisible to the re-run.**
2. Clear submit run logical `<D>` from `batch_cleanup_1` WITH downstream.
3. Wait up to 24h for the new batches to complete.
4. Clear fetch logical `<D+1>` from `batch_transition`.
5. Clear `keyword_ddp_reporting` `wait_for_product_categorization`.

## `get_files_without_batch` "Inconsistent state" = a double-submission guard, not flakiness
**The guard keys on `openai_batch_id.notna()`, NOT on `was_submitted` (read from source 2026-09-04, AUDI-1321).**
`openai/openai_wrapper/batch_submitter.py` builds `submitted_file_names` from
`batch_submissions_df.query("openai_batch_id.notna()")`, so a receipt naming an **expired or failed** batch counts as
"already submitted" and its input file is never retried. Re-running submit on a partially recovered day therefore
silently leaves it short, with no error. **The receipts must be DELETED first** — that is why every recovery recipe
starts there. (The AUDI-1321 handoff gave the right step for the wrong reason: it said the rows point at batches that
died before creation, but every receipt carries a real, unique batch id.)
`ValueError: Inconsistent state between openai_batch_submissions and openai_batch_input_formatted`
guards against double-submitting after a PARTIAL/killed try. A later try that fails in <3 min hit the
GUARD, not the work. 2026-08-28 case: try 1 was killed by k8s MID-PREP (pod deleted while writing
input files), leaving 1102 orphan formatted files; every retry then saw submissions ≠ formatted and
refused to resubmit. Recovery = the dead-cohort procedure above (clear from `batch_cleanup_1`, never
from `batch_submit`).

## `batch_cleanup_1` and `batch_cleanup_2` are IDENTICAL bookend tasks
Same `openai_batch_runner` image, same `delete_all_storage_files.py`, same env — a pre-run cleanup (frees
OpenAI file-storage quota headroom before submitting) and a post-run sweep. They run in BOTH DAGs, so the
cleanup script executes **~4×/day** total.
- **`Skipped file-xxx: 404 No such File object` during cleanup = the two DAGs RACING, not flakiness (verified
  2026-09-03).** Submit and fetch both schedule `0 9 * * *`, so their `batch_cleanup` tasks list and delete the
  same file ids concurrently; the loser 404s on a file the winner already deleted. Benign, self-limiting, and
  NOT worth fixing — don't chase it again.

## `batch_test` = the dbt DQ gate on `product_categorization` — mechanics + two failure red-herrings
`batch_post >> [batch_test, batch_cleanup_2]` (fetch DAG; `batch_test` is a leaf parallel with `batch_cleanup_2`).
It runs `dbt test --select product_categorization` (6 data tests) via the **`SHOPPER_GRAPH` /
`mntn_matched_data_pipeline`** image against Databricks target **`prod_warehouse_2xs`**; tests live at
`SteelHouse/shopper_graph:dbt/tests/mntn_matched/post_batch/*.sql` (dbt project `mntn_matched_data_quality`).
The 6: `dsc_id__{length,not_null,values}` + `record_count` (data-integrity, hard-fail),
`product_category_and_key` (WARN-only), and **`product_categorization__max_dt`**. **`product_categorization`
is a Databricks table** (schema `mntn_matched`), NOT BigQuery — you cannot query it with `bq_run.sh`; it's
built from `openai_batch_results_joined` + `mntn_matched_taxonomy`.
- **BACKFILL FALSE-POSITIVE (`max_dt`):** it asserts a partition exists at wall-clock
  **`dt = date_sub(current_date, 2)`** (UTC), but the pipeline writes `dt = yesterday`(-of-logical-date). On an
  on-time run these coincide → PASS; on any **late / manual backfill they diverge and `max_dt` FALSE-FAILS**
  (INC-007: logical-07-29 fetch re-run on 07-31 wrote `dt=07-28` correctly, but the test wanted
  `current_date-2 = 07-29` → 1-row FAIL, data clean). The **other 5 tests key off the latest partition**
  (`>= date_sub(current_date, 5)`), so they're backfill-robust. **Diagnostic:** `max_dt` failing ALONE while
  `record_count` / `dsc_id__*` / `product_category_and_key` PASS ⇒ data is fine — **mark
  `test_product_categorization` SUCCESS, do NOT re-run** (fails every time once UTC > target date+2).
  **READ WHICH ASSERTION FAILED FIRST (added 2026-09-04, AUDI-1321) — "known false failure" belongs to `max_dt`
  ALONE, never to the task.** On 2026-09-04 the failing assertion was **`product_categorization__record_count`**
  (row count >= 99% of `openai_batch_results_joined` at the same `dt`) and it was CORRECT; the mark-success let a
  **408 MiB** partition through against a ~4.0-4.3 GB normal day, `keyword_ddp_reporting` consumed it, and two
  downstream partitions had to be backed up, deleted and rebuilt. See [[feedback_check_which_dbt_assertion_failed]].
  A red
  `batch_test` is also **cosmetic to `keyword_ddp_reporting`**: its `wait_for_product_categorization` sensor targets
  `batch_post.product_categorization` (UPSTREAM of batch_test), so clear the sensor independently. Durable fix =
  key it off the pod's `yesterday`/`run_date` env var → **IMP-016**.
- **SMTP `Errno 99` RED-HERRING:** a failed task's log TAIL showing `OSError: [Errno 99] Cannot assign requested
  address` from `smtplib` / `airflow.utils.email` is Airflow's **failure-alert EMAIL callback** failing (email
  alerting is broken on this Astronomer deployment) — NOT the task's real error; scroll UP for it. Seen on both
  `batch_test` and `batch_cleanup_2` → **IMP-017**.

## OpenAI file-storage economics — why the 2.5TB quota fails INTERMITTENTLY, not every day
Daily volume ≈ **75 GiB/day** (input `part-*` ~35 GiB + output `batch_*` ~40 GiB) — **re-measured 2026-09-04 at
~86 GB/day: inputs ~1,014 files x ~40 MB = 40.3 GB, results ~46 GB (sizes in the 2026-09-04 section below)**; the pipeline needs only
~1-2 days retention (enough to submit + fetch). **OpenAI auto-expires uploaded files at 30 days** (evidenced:
a file created 2026-07-28 showed `expires 2026-08-27`). So with a BROKEN cleanup (deleting nothing) storage
does NOT grow unbounded — it plateaus at ~30 days × ~75 GiB ≈ **2.4 TB, right against the 2.5 TB project cap**,
and tips over on heavier days → the `client.files.create` **`400` fails intermittently** (fails one day,
self-clears the next as old files expire). A WORKING 48h cleanup holds only ~150 GiB. **Diagnostic tell:**
intermittent quota `400`s + storage pinned near 2.5 TB ⇒ the cleanup isn't deleting (a code bug), NOT that
daily volume is huge. (INC-007 / AUDI-1042; the abort/regression bugs in [[reference_openai_sdk_pagination]].)
**CONFIRMED 2026-09-04 (AUDI-1321), and the corollary matters:** with a sweep that can finally enumerate the
whole store the project holds **129 files / 4.2 GiB, 0.2% of the cap**, of which only 1.4 GiB is not this
pipeline's. So the 2.5 TB really was our own un-swept backlog. **A steady-state daily footprint (~100 GB at a
48h window) cannot be used to infer a foreign holder** — that arithmetic said "~2.4 TB is not ours" on the
morning of 2026-09-04 and was wrong, because the missing mass was our own history the broken listing could
not see. Measure the store; do not extrapolate one normal day.

- **UPDATE 2026-09-03 (AUDI-1191; appended, the expiry-plateau account above is INCOMPLETE, not wrong).** The
  30-day expiry explains why the quota failed INTERMITTENTLY; it does not explain the six-day PERMANENT wall.
  The mechanism is the cleanup's **LIST ORDER**: `GET /v1/files` caps `limit` at 10,000 (also the default) and
  returns `created_at desc`, so the sweep only ever sees the NEWEST 10,000 files. It deletes only files older
  than 48h, so once >10,000 files were younger than 48h (the outage retry storm plus the 09-03 backfill's
  **3,429+ inputs uploaded in ~2h**) the entire visible page was ineligible and every sweep freed nothing.
  **Evidence (task logs):** 08-25..27 runs found 13/14/28/131/181/357/788/1170 files; every run 08-29 → 09-03
  logged `Total number of files to delete: 0` while `batch_submit` hit `400 ... exceeded your file storage
  quota. Projects are limited to 2.5TB`. **Fixed** by listing `order="asc"` with explicit paging and breaking at
  the retention cutoff (`shopper_graph#306`, MERGED; deployed `deploy_openai_dockerhub_gcp.yml` run
  `33775001798`); the first sweep on the new image deleted **1,132 of 1,132** files, 0 skips. Both evidence
  trails stand: expiry sets the plateau, order decides whether the sweep can ever cut into it. Full trap:
  [[reference_openai_sdk_pagination]].

## `image_pull_policy=Always` → a task's image depends on WHEN its pod ran vs deploy times
Because `MntnKubePodOperator` pulls `image_pull_policy=Always`, two tasks in the same DAG run can run
DIFFERENT image builds if a deploy landed between their pod starts. **INC-007 example:** after the broken
#297 cleanup deployed, `batch_cleanup_1` (pod started **before** the deploy, old good code) was green while
`batch_cleanup_2` (pod started **after**, broken code) crashed on the same script. When reasoning about which
code a task ran, compare its **pod start time** to the deploy time, not just "latest main".

## Image mapping (summary; full rule in reference_shopper_graph_deploy)
`batch_*` tasks use **`DbtImageName.OPEN_AI_BATCH`** (`openai_batch_runner`); the dbt tasks use
**`DbtImageName.SHOPPER_GRAPH`** (`mntn_matched_data_pipeline`). Never the middleware `shopper-graph` image.
So an `openai/` source change (e.g. `batch_fetcher.py` #296, `delete_all_storage_files.py` #298) ships ONLY
via `deploy_openai_dockerhub_gcp.yml`.

## 2026-08-31 (evening) — the 08-28+ outage is ORG-SIDE, proven from the dashboard
- **Org:** `org-ldKlX0Pr81MhoY05W9t6oB1V` ("MNTN", usage tier 5, Verified). Malachi now has org
  access via an emailed invite. **Sign-in gotcha:** the Google-auth path FAILS ("Could not access
  the organization"); the working path is typing the email, then Okta enterprise SSO.
- **The proof:** the batch input file EXISTS with status Ready in the SAME org/project, yet every
  batch since **2026-08-28 06:00 PT** fails validation ~60s after creation with
  `Cannot find file <id>, or organization org-ldKlX0Pr81MhoY05W9t6oB1V does not have access to it`.
  A MANUAL test batch (input under the `batch_requests_*` naming, a different producer) fails
  IDENTICALLY ⇒ org-wide across producers, not our pipeline's code.
- **Audit logging was NEVER enabled on the org** (needs `api.admin`; only org OWNERS — e.g.
  Alyson — can enable it), so no org-side trail exists for the outage window.
- **Durable fix = AUDI-1301 (backlog):** dedicated OpenAI project for the pipeline + audit
  logging + a perms group (`api.admin`, `organization.write`, `spend_limits.read`) for
  Brian/Sean/Ryan/Malachi.
- **Ryan Kleck's caution:** wiping `openai_batch_submissions` wholesale loses fetch tracking. The
  08-29/31 deletes were dead-cohort receipts only (the documented recovery scope).

## 2026-09-02 (night) — six-day dead-cohort recovery EXECUTED after OpenAI's org-side fix

- **OpenAI fixed the org-side outage**, confirmed working by Alyson's manual batch
  `batch_6a9843249bcc8190bb3b7f6eccedab49` before anything was cleared.
- **Scope: all six days `dt=2026-08-27..2026-09-01` were dead (0/N `was_submitted`);**
  08-30/31/09-01 re-verified from the receipts parquet before deletion.
- **Executed (user-authorized), per the recovery procedure above:** receipts deleted for all six
  days; submit runs cleared from `batch_cleanup_1` WITH downstream (7 tasks each).
- **Remaining steps live in
  `tickets/audi_1191_airflow_spark_debugger/outputs/audi_1191_next_actions_2026_08_31.md`:**
  clear fetch logical D+1 from `batch_transition` after each batch completes; clear
  `keyword_ddp_reporting` `wait_for_product_categorization`; expect `max_dt` FALSE-FAILS on the
  backfilled fetch days (mark `test_product_categorization` success ONLY when `max_dt` is the sole failing
  assertion — the `batch_test` section above, corrected 2026-09-04).

## 2026-09-03 — the rerun submits died on the 2.5TB quota; receipts are PARTIAL (observed: AUDI-1191 record + a GCS re-list, AUDI-1279)
- The six rerun submits (dt=2026-08-27..09-01) each created batches 04:43-05:41Z, then failed `batch_submit` try 6 with the
  400 file-storage-quota error at 05:58-06:34Z (`tickets/audi_1191_airflow_spark_debugger/outputs/audi_1191_next_actions_2026_08_31.md`
  § 2026-09-03 06:50 UTC). `batch_cleanup_1` in the same runs deleted 0 files (48h retention spares the night's own uploads).
- GCS at 06:45-06:51Z: dt=08-27 742, 08-28 791, 08-29 653, 08-30 510, 08-31 733 receipts with NEW `openai_batch_id` /
  `openai_input_file_id` and `batch_submit_time` 2026-09-03 04:4x-05:41; dt=09-01 prefix ABSENT; alive dt=08-26 untouched at 1113.
  All three listed partitions stop at 05:41:12-13Z (one process, killed or finished together).
- **Rules that follow:** a `dt=` object count is not a cohort size; a partial partition trips the double-submission guard, so
  re-running a day needs its receipts deleted AGAIN; no further submit clears until OpenAI storage is freed —
  **RESOLVED AND PROVEN 2026-09-03 by the `order="asc"` cleanup fix `shopper_graph#306`, NOT by a dashboard bulk-delete**
  (storage-economics section above; proof in the AUDI-1321 section at the end of this file) — then ONE day at a time. The scheduled fetch for `yesterday=2026-09-01` finds no prefix (today: `FileNotFoundError` in
  `get_batch_ids`; after #305 `assert_cohort_alive` returns first and `get_batch_ids` still raises).
- **Recovery state to resume from:** all five partial partitions (`dt=08-27` 742 rows, `08-28` 791, `08-29` 653, `08-30` 510,
  `08-31` 733, every row `was_submitted=False`) hold NEW batch ids that are live at OpenAI, so **their receipts must be deleted
  AGAIN before any re-run**; `dt=2026-09-01` has no receipts at all, which is why fetch logical 09-02 fails `FileNotFoundError ...
  openai_batch_submissions/dt=2026-09-01` — expected, and it self-heals once submit 09-01 succeeds. **Per-day order:** delete
  receipts `dt=D` → clear submit `D` → wait ~2h for the batches → clear fetch `D+1` → clear `keyword_ddp_reporting`
  `wait_for_product_categorization`.
- **The downstream consumer DAG is `keyword_ddp_reporting`** (`dags/machine_learning/keyword_ddp_reporting.py`); its
  `ExternalTaskSensor` targets **`batch_post.product_categorization`** (upstream of `batch_test`, so a red `batch_test` never
  blocks it — clear the sensor independently).

## 2026-09-03 — AUDI-1279: per-batch status lines + a dead-cohort alarm (shopper_graph#305, MERGED 18:39 UTC, commit `85855ce`, DEPLOYED same day)
Extends "the parquet flags are the only automated visibility" (appended, not overwritten): **#305 is deployed as of
2026-09-03**, so the pod logs ARE now the second signal; the flags stay the only key-free, dashboard-free one. Ticket:
`tickets/audi_1290_pipeline_optimization_hackathon/audi_1279_openai_batch_observability/summary.md`; decision
`knowledge/decisions/0006_dead_cohort_alarm_is_batch_fetch_failure.md`.
- **What ships:** new `openai/openai_wrapper/batch_status.py` (stdlib only); `batch_base.retrieve_batch(batch_id)` returns the
  Batch or the exception; `batch_transitioner` prints one line per unflagged receipt and now flags `was_submitted` on `finalizing`
  too (was `in_progress`/`completed` only); `batch_fetcher.assert_cohort_alive()` runs before `get_batch_ids()` in
  `fetch_results.py`; `openai/Dockerfile` gets `ENV PYTHONUNBUFFERED=1`; `tests/unit/test_openai.py` 0 -> 16 tests.
  **Plus the zero-delete storage alarm (added to #305 rather than a separate PR, so the pipeline had one observability
  change to review):** `delete_all_storage_files.py` raises when EVERY eligible delete fails, and when it frees
  **nothing** while at least `STORAGE_ALARM_MIN_FILES` (env, default **10,000**) files are still stored. Normal volume is
  a few hundred to ~1,200 files/day, so a quiet day stays silent; the guard is straight-line script code with no import
  surface, so it carries no unit test. Rationale: a sweep that deletes zero is indistinguishable in the logs from a quiet
  day, which is exactly why the 08-28 outage stayed silent for six days. See [[reference_openai_sdk_pagination]].
- **Log shape:** `batch=<id> file=<s3_filename> status=<status> submitted_utc=<iso> age_h=<x.x> counts=<completed>/<failed>/<total>
  error=<code>: <message>` per receipt, then `cohort dt=<D>: n=<N> in_progress=.. finalizing=.. completed=.. validating=.. failed=..
  expired=.. cancelling=.. cancelled=.. retrieve_error=.. other=.. flagged_now=..`. A `retrieve` exception is logged as
  `status=retrieve_error error=<ExceptionType>: ...` and never aborts the loop; a None error code/message prints as empty.
- **Dead cohort =** receipts exist AND none `was_submitted` AND every live status is outside {`in_progress`, `finalizing`,
  `completed`} with `request_counts.completed == 0` (an `expired` batch with partial output counts as progressed) AND the youngest
  `batch_submit_time` is >= `DEAD_COHORT_MIN_AGE_HOURS` old (env, default 12; a non-numeric value falls back to 12). Then
  `batch_fetch` raises `DeadCohortError: dead cohort dt=<D>: 0 of <N> batches progressed <age>h after submit (threshold 12.0h);
  first error: <err>` and the pod exits 1. Under threshold: one `WARNING: cohort dt=<D> has 0 of <N> progressed but is only <age>h
  old; below threshold <min>h, not failing` line, exit 0. A missing receipts partition returns silently. The scheduled fetch runs
  ~22-24h after submit, so the default only guards a manual early run.
- **Failure surface = the DAG's existing routing, nothing new wired:** the task fails after `retries: 4` (backoff to 45 min, so
  Slack lands 1-2h after first detection; each retry re-queries OpenAI, so a late-waking cohort self-clears) -> `JobTeamConfig.ML`
  -> Slack `#monitor-emr` (prod; `#monitor-test` non-prod) + `machine-learning-squad@mountain.com`. PagerDuty fires only at
  `severity == 0`; the fetch DAG is severity 5, so NO page without a separate airflow-ti change (open ask for Ryan Kleck). The
  Slack text is the generic `Pod ... returned a failure`; the cause is in the task log under `[base]`.
- **Live consequence once deployed:** every scheduled `batch_fetch` fails on a dead day until AUDI-1301 fixes the org-side file
  access; `batch_post` already failed those days, so no new data loss.
- **Runtime facts the alarm relies on (verified 2026-09-02/03, openai 3.7.0):** `Batch.status` literal =
  `validating|failed|in_progress|finalizing|completed|expired|cancelling|cancelled`; `Batch.errors.data[].{code,line,message,param}`;
  `Batch.request_counts.{completed,failed,total}`; `NotFoundError < APIStatusError < APIError < OpenAIError`. The
  `openai_batch_runner` image builds UNPINNED (`openai/requirements.txt`) and today resolves python 3.11.16, openai 3.7.0, pandas
  3.0.5, pyarrow 25.0.1, gcsfs 2026.8.0; pandas 3 reads the receipts' string columns as `str` dtype (2.x: `object`), `.query()`
  unchanged. A future SDK field rename degrades to `retrieve_error` lines plus a dead-cohort alarm, not a crash. Staging recipe,
  CI limits and deploy gates: [[reference_shopper_graph_deploy]].

## 2026-09-03 (evening) — AUDI-1321: the six-day quota wall is CLOSED, proven by a green submit

The blocker every section above defers to ("no further submit clears until OpenAI storage is freed") is cleared. Sequence:
- **15:50** — `shopper_graph#306` (`order="asc"` + explicit paging) deployed.
- **Next cleanup** — deleted **1,132 of 1,132** eligible files, **0 skips**. Every run 2026-08-29 → 09-03 on the old image had
  logged `Total number of files to delete: 0`. It went from nothing to the entire eligible set on the first run of the new code.
- **~19:00 UTC** — **`batch_submit` on submit logical 2026-09-02 SUCCEEDED after running ~57 minutes.** First green submit since
  **2026-08-28**. Every prior attempt died in **~27 seconds** on `400 ... exceeded your file storage quota`; the runtime gap is
  the tell, since the old failures never got past the upload.
- **18:39** — `shopper_graph#305` merged (commit `85855ce`) and deployed, adding the zero-delete alarm described above.

**AUDI-1321's kill criterion never triggered.** It read: "if it still 400s after a sweep that provably deleted its full eligible
set, the storage is not ours — escalate to Alyson for OpenAI dashboard access." The sweep provably deleted its full set and the
submit went green on the same run, so **the storage WAS ours and the list-order defect accounted for the entire outage**.
Deleting only the names our own sweep touches (`part-*` / `batch_*`) cleared the 2.5TB, which also confirms no other producer was
holding it. Do not reopen the shared-account hypothesis without new evidence. It WAS reopened on 2026-09-04 morning (measured daily
volumes put the pipeline at ~100 GB under a 48h window, leaving ~2.4 TB apparently unaccounted) and **CLOSED again the same
evening by a per-file byte inventory: 129 files / 4.2 GiB, 1.4 GiB of it not ours.** This verdict STANDS. The 2026-09-04
sections below carry the challenge and its resolution in order.

**Still open (restated 2026-09-04 evening — the batch ids on those receipts are NOT live any more):** backfill
`dt=2026-08-27..09-02`, one day at a time. Each partial partition's receipts must be deleted before its re-run or the
double-submission guard skips the files, and the batches those receipts name have LAPSED (see the 2026-09-04 evening section).
`dt=2026-09-01` has no receipts at all; `dt=2026-09-02` is 46% fetched, not recovered. Ticket:
`tickets/audi_1321_openai_storage_quota_unblock/summary.md`. Trap detail: [[reference_openai_sdk_pagination]].

**Manual fetch runs: `dt read = data_interval_start - 1 day`, and `run_after = data_interval_end`
(corrected 2026-09-03).** The DAG renders `yesterday` as `{{ data_interval_start.subtract(days=1) }}`, and for
the `0 9 * * *` timetable a run's `data_interval_start` sits one period BEFORE its logical date. The older note
here, "submit logical D writes dt=D; fetch logical D+1 reads dt=D", is right for SCHEDULED runs only, where the
two coincide. Setting just `logical_date` on a manual POST reads the wrong partition (one day early), and
setting it a day later leaves `run_after` in the future so the scheduler will not start it. To replay a past
partition immediately, POST with explicit `data_interval_start` (the dt you want plus one day) and a
`data_interval_end` in the past. Cost two failed triggers on 2026-09-03 before the third worked
(`manual__recover3_dt_2026-09-02`).

## 2026-09-04 (AUDI-1321) — measured sizes, the `product_categorization` python model, and the DS19 clearing race

**Measured GCS volumes (all `gs://mntn-data-archive-prod/shopper_graph/`, one normal day):**

| stage | path | per day |
|---|---|---|
| batch inputs | `openai_batch_input_formatted/dt=D/` | **~1,014 files x ~40 MB = 40.3 GB** |
| batch results | `openai_batch_results/dt=D/` | **~46 GB** |
| categorization | `product_categorization/dt=D/` | **~4.0-4.3 GB across ~50 parquet files** |

- **Whole-pipeline footprint at a 48h retention window ≈ 100 GB against OpenAI's 2.5 TB per-project file cap.**
  At the time this read as **~2.4 TB the pipeline does not account for**, which challenged the 2026-09-03 "the
  storage was ours" verdict. **RESOLVED the same evening and the inference was WRONG** — `shopper_graph` #308's
  inventory measured the whole store at 129 files / 4.2 GiB. The gap was our own multi-day `part-` backlog that
  the short-page listing could not reach, not a foreign holder. See the 2026-09-04 (evening) section below.
- **`batch_submit` dies on the FIRST ~40 MB `files.create`, so it needs ~40 GB of headroom, not a clean account.**
  Do not read a green submit as "storage is fine"; read it as "there was room for one file".
- **The sweep still under-enumerates:** `delete_all_storage_files.py` breaks its walk on a page shorter than
  `limit`, so its `seen` counts are first-page sizes. `batch_cleanup_1` logged `Deleted 0 of 0 files, having
  listed at least 28` at 09:00 while `batch_fetch` deleted **416** output files four minutes later. Full trap and
  the correct `after=`-cursor form: [[reference_openai_sdk_pagination]] TRAP 3.
- **The #305 zero-delete alarm cannot fire on that shape** — `ALARM_MIN_FILES` defaults to `PAGE` (10,000) and
  `seen` never exceeded 4,623, so the guard stayed false through the whole block. The threshold is compared
  against a partial page, not a count.

**`STORAGE_ALARM_MIN_FILES` and `OPENAI_FILE_MAX_AGE_HOURS` have never actually been set.** Both are read by the
shipped `openai_batch_runner` image and passed nowhere: an Astro deployment Environment-tab variable does NOT
reach a `KubernetesPodOperator` pod, and `MntnKubePodOperator` mounts exactly one `env-secrets` key
(`OPENAI_API_KEY`). Setting either needs an `airflow-ti` change to the DAG's `env_vars` dict; `env_vars` is a
`template_field`, so `"{{ var.value.get('...', '<default>') }}"` makes it tunable by Airflow Variable afterwards.
`shopper_graph#307` is deployed and INERT until that lands. Proof from the prod rendered pod spec:
[[reference_airflow_ti]] § KubernetesPodOperator env.

**`product_categorization` is a dbt PYTHON model with `incremental_strategy="append"`, and it cannot self-heal.**
`dbt/models/mntn_matched/post_batch/product_categorization.py` in `shopper_graph`. If the target `dt` partition
already holds data it raises `FileExistsError` with `"Consider deleting files in the partition before re-running"`.
**Re-running does not overwrite** — the GCS partition
`gs://mntn-data-archive-prod/shopper_graph/product_categorization/dt=D/` must be deleted first, then the task
cleared. Normal day ~4.0-4.3 GB across ~50 parquet files, so a partition materially under that is short, not slow.

**`keyword_ddp_reporting` lineage and the clearing race (the part that cost real rework):**
- The DAG uses `run_date = "{{ ds }}"`, so its **run name does not name the partition it writes**: the run
  `manual__consume_dt_2026-09-02` actually wrote **`dt=2026-09-03`**. Check `ds`, not the run id.
- `write_targeted_signal_ds_19` feeds `gs://mntn-data-archive-prod/signals/targeted_signal/data_source_id=19/dt=D/`
  (normal **~70-72 GB**); `write_targeted_signal_ds_19_domain` feeds `signals/targeted_signal_domain/dt=D/`
  (normal **~44.5 GB**).
- **Only the DS19 path depends on `product_categorization`.** `data_source_id=13` (~105-110 GB) and
  `data_source_id=4` (~157-158 GB) come from other sources and are unaffected by a short or missing
  categorization partition.
- **The task chain is SEQUENTIAL:** `wait_for_product_categorization >> write_targeted_signal_ds_19 >>
  write_targeted_signal_ds_13 >> write_targeted_signal_ds_19_domain`. **Clearing `ds_19` and `ds_19_domain`
  together RACES them**, because `ds_19_domain`'s direct upstream `ds_13` stays green and does not hold it back.
  **Clear `ds_19`, wait for it, THEN clear `ds_19_domain`.**

## 2026-09-04 (evening) — AUDI-1321: ownership SETTLED, and the four rules that came out of the recovery

**The store was ours.** With `#308` (page to an empty page + per-file byte/purpose inventory) and `#309` (survive a
deleted cursor) deployed, the sweep on submit `scheduled__2026-09-03T09:00` listed **129 files holding 4.2 GiB, 0.2%
of the 2.5 TB cap**; this pipeline held 2.8 GiB and everything else totalled **1.4 GiB** (fine-tune 0.3, other
`purpose=batch` 1.1, assistants ~0, fine-tune-results ~0). One earlier run of the fixed sweep enumerated **5,527
deletable inputs holding 193.4 GiB** aged 21.8-54.6h — a multi-day backlog of our own `part-` inputs the short-page
listing could never reach. **§0's kill criterion never fired; no escalation and no OpenAI dashboard access needed.**
`batch_submit` for `dt=2026-09-03` then ran 18:01:52 → 19:24:29 UTC and wrote **1,004 receipts**.

**RULE 1 — never set the OpenAI input retention window below 26h. A 12h window FAILS LIVE BATCHES.**
OpenAI's `completion_window` is **24h**, so an input file must outlive it. When inputs were briefly set to expire at
12h, `batch_transition` on `dt=2026-09-02`'s 119 untransitioned receipts returned **`failed=119, expired=0`** —
immediately after the 2026-09-04 18:00 sweep deleted their input files at the 12h mark. The 468 already-completed
batches were untouched, which is the tell: only batches still holding their input die. Without the fix the
2026-09-05 09:00 sweep would have deleted `dt=2026-09-03`'s inputs at 07:24 and failed all 1,004 the same way.
**`shopper_graph#310` raises the default to 26h and is deployed;** outputs stay at 48h. Generalizes: any retention
window on an async job's INPUT must exceed that job's own completion window, with headroom.

**RULE 2 — a lapsed batch is unrecoverable, and so is a completed one whose output file was swept.**
`dt=2026-08-27`'s 742 batches (created 2026-09-03 04:53-05:41) probed as **expired=612, completed=128,
in_progress=2**. Harvesting the 130 still failed: `batch_fetch` **404s on `files.content(output_file_id)`** because
the output file is gone from OpenAI. All 742 had to be re-submitted. Expect the same shape for 08-28 (791), 08-29
(653), 08-30 (510), 08-31 (733); 09-01 has no receipts at all. **Do not plan a recovery around harvesting an old
cohort** — probe it, expect nothing, and budget a full re-submit.

**RULE 3 — replay ONE day at a time. Parallel replay is what caused the outage's severity.**
On 2026-09-03 between 04:15 and 06:13 a prior session cleared all five blocked days' submit runs **in parallel**.
Five days of inputs at ~40 GB each exhausted the 2.5 TB, and every batch created before the wall then sat unfetched
until it lapsed — **3,429 batches across the five days**. This is a hard operational rule, not a preference.

**RULE 4 — `mntn_match_incrementals_fetch` has `max_active_runs=1` (submit has 16), so backfill fetch work BLOCKS
the daily fetch.** Verified via `GET /dags/{dag_id}`. Any backfill probe or fetch leaves that one slot occupied, so
it must FINISH before the 09:00 UTC daily slot. Schedule backfill fetch work outside the 09:00 window. Submit has no
such constraint, but Rule 3 still caps it at one day.

**`product_categorization__record_count` cannot detect an incomplete day.** It asserts `product_categorization` >=
99% of `openai_batch_results_joined` at the same `dt`, and BOTH are built from whatever was fetched, so a
half-fetched day passes cleanly. `dt=2026-09-02` passed it while only **46% fetched** (468 of 1,014 receipts
downloaded; `openai_batch_results` 20.3 GB against 46.2 GB normal). The test proves internal consistency, never
completeness. **Completeness is checked against the RECEIPT COUNT, never against a sibling table.** This corrects
the earlier reading that 09-02 was repaired: its `product_categorization` rebuild was correct relative to what had
been fetched and is still short of the real day. See [[feedback_check_which_dbt_assertion_failed]].

**Batch-cohort probe recipe (non-destructive, use it before touching any day's receipts).**
Clear `batch_transition` on the fetch run whose `data_interval_start` is `D+1` (that run reads `dt=D`). It polls
every receipt's `openai_batch_id` and prints one summary line:
`cohort dt=D: n=.. in_progress=.. finalizing=.. completed=.. validating=.. failed=.. expired=.. cancelling=..
cancelled=.. retrieve_error=.. other=.. flagged_now=..`. The only mutation is flipping `was_submitted=True` on rows
that progressed. **Blind spot: it only examines rows where `was_downloaded == False & was_submitted == False`**, so
rows already transitioned are invisible to it and `n` is not the cohort size — compare against the receipt count.

**Measured task timings (2026-09-04):** `batch_submit` **~84 min for 1,261 files**, **~82 min for 1,004** (about
1h20m, so budget a day's re-submit at ~1.5h). `write_targeted_signal_ds_19` **1.7-1.8h** on a normal day.
`product_categorization` **~31 min**.
