---
name: reference_mntn_matched_batch_pipeline
description: "mntn_match_incrementals_{submit,fetch} DAG mechanics: task chains, the cross-DAG contract (a GCS submissions file, NOT an Airflow sensor), backfill order, the identical batch_cleanup bookend, and how image_pull_policy=Always makes a task's image depend on WHEN its pod ran vs deploy time."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [was_submitted flag, dead cohort, dead-cohort recovery, batch_fetcher status completed, get_files_without_batch, inconsistent state guard, double-submission guard, orphan formatted files, openai batch dashboard access, submit run_date logical date, openai_batch_input_formatted, delete submissions receipts, resubmission procedure, mntn_match_incrementals_submit, mntn_match_incrementals_fetch, batch_submit, batch_transition, batch_fetch, batch_prep, batch_validate, batch_post, batch_cleanup, batch_cleanup_1, batch_cleanup_2, batch_test, submit_batch.py, transition_batch.py, fetch_results.py, batch_transitioner, delete_all_storage_files, openai_batch_submissions, cross-dag contract, gcs submissions file, backfill order, mntn matched batch pipeline, DS19 keyword pipeline, openai batch runner, OPEN_AI_BATCH, SHOPPER_GRAPH, image_pull_policy Always, machine_learning dags, dt yesterday contract, FileNotFoundError submissions, openai file storage quota, 2.5TB quota, 30-day file expiry, openai auto-expire files, storage economics, 75 GiB per day, intermittent quota failure, quota fails intermittently, delete_all_storage_files economics, batch_test dbt tests, product_categorization__max_dt, max_dt freshness test, current_date backfill skew, dbt test backfill footgun, mntn_matched_data_quality, post_batch dbt tests, mark test success backfill, keyword_ddp not blocked by batch_test, OSError errno 99 email red herring, IMP-016, IMP-017, alyson dashboard access, one identical error message, ryan kleck openai org reauthentication, org-side outage, org-ldKlX0Pr81MhoY05W9t6oB1V, cannot find file organization access, okta enterprise sso login, audit logging api.admin, AUDI-1301, dedicated openai project, batch_requests manual test batch, usage tier 5, six-day dead cohort, recovery executed 2026-09-02, batch_6a9843249bcc8190bb3b7f6eccedab49, partial receipts partition, rerun submits quota failed, receipts rewritten new batch ids, object count not cohort size, AUDI-1279, shopper_graph#305, DeadCohortError, dead cohort alarm, batch_status.py, DEAD_COHORT_MIN_AGE_HOURS, retrieve_error, per-batch status line, cohort summary line, assert_cohort_alive, retrieve_batch, PYTHONUNBUFFERED, Batch.status literal, request_counts, openai 3.7.0, pandas 3 str dtype, unpinned image, monitor-emr, JobTeamConfig.ML, severity 5 no pagerduty]
domain: [repos, infra]
lifecycle: active
last_verified: 2026-09-03
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
1. Delete `gs://mntn-data-archive-prod/shopper_graph/openai_batch_submissions/dt=<D>/` — receipts
   only; the inputs in `openai_batch_input_formatted/dt=<D>` survive.
2. Clear submit run logical `<D>` from `batch_cleanup_1` WITH downstream.
3. Wait up to 24h for the new batches to complete.
4. Clear fetch logical `<D+1>` from `batch_transition`.
5. Clear `keyword_ddp` `wait_for_product_categorization`.

## `get_files_without_batch` "Inconsistent state" = a double-submission guard, not flakiness
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
  `test_product_categorization` SUCCESS, do NOT re-run** (fails every time once UTC > target date+2). A red
  `batch_test` is also **cosmetic to `keyword_ddp`**: its `wait_for_product_categorization` sensor targets
  `batch_post.product_categorization` (UPSTREAM of batch_test), so clear the sensor independently. Durable fix =
  key it off the pod's `yesterday`/`run_date` env var → **IMP-016**.
- **SMTP `Errno 99` RED-HERRING:** a failed task's log TAIL showing `OSError: [Errno 99] Cannot assign requested
  address` from `smtplib` / `airflow.utils.email` is Airflow's **failure-alert EMAIL callback** failing (email
  alerting is broken on this Astronomer deployment) — NOT the task's real error; scroll UP for it. Seen on both
  `batch_test` and `batch_cleanup_2` → **IMP-017**.

## OpenAI file-storage economics — why the 2.5TB quota fails INTERMITTENTLY, not every day
Daily volume ≈ **75 GiB/day** (input `part-*` ~35 GiB + output `batch_*` ~40 GiB); the pipeline needs only
~1-2 days retention (enough to submit + fetch). **OpenAI auto-expires uploaded files at 30 days** (evidenced:
a file created 2026-07-28 showed `expires 2026-08-27`). So with a BROKEN cleanup (deleting nothing) storage
does NOT grow unbounded — it plateaus at ~30 days × ~75 GiB ≈ **2.4 TB, right against the 2.5 TB project cap**,
and tips over on heavier days → the `client.files.create` **`400` fails intermittently** (fails one day,
self-clears the next as old files expire). A WORKING 48h cleanup holds only ~150 GiB. **Diagnostic tell:**
intermittent quota `400`s + storage pinned near 2.5 TB ⇒ the cleanup isn't deleting (a code bug), NOT that
daily volume is huge. (INC-007 / AUDI-1042; the abort/regression bugs in [[reference_openai_sdk_pagination]].)

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
  `keyword_ddp` `wait_for_product_categorization`; expect `max_dt` FALSE-FAILS on the
  backfilled fetch days (mark `test_product_categorization` success, do not rerun — the
  `batch_test` section above).

## 2026-09-03 — the rerun submits died on the 2.5TB quota; receipts are PARTIAL (observed: AUDI-1191 record + a GCS re-list, AUDI-1279)
- The six rerun submits (dt=2026-08-27..09-01) each created batches 04:43-05:41Z, then failed `batch_submit` try 6 with the
  400 file-storage-quota error at 05:58-06:34Z (`tickets/audi_1191_airflow_spark_debugger/outputs/audi_1191_next_actions_2026_08_31.md`
  § 2026-09-03 06:50 UTC). `batch_cleanup_1` in the same runs deleted 0 files (48h retention spares the night's own uploads).
- GCS at 06:45-06:51Z: dt=08-27 742, 08-28 791, 08-29 653, 08-30 510, 08-31 733 receipts with NEW `openai_batch_id` /
  `openai_input_file_id` and `batch_submit_time` 2026-09-03 04:4x-05:41; dt=09-01 prefix ABSENT; alive dt=08-26 untouched at 1113.
  All three listed partitions stop at 05:41:12-13Z (one process, killed or finished together).
- **Rules that follow:** a `dt=` object count is not a cohort size; a partial partition trips the double-submission guard, so
  re-running a day needs its receipts deleted AGAIN; no further submit clears until OpenAI storage is freed (Alyson / dashboard),
  then ONE day at a time. The scheduled fetch for `yesterday=2026-09-01` finds no prefix (today: `FileNotFoundError` in
  `get_batch_ids`; after #305 `assert_cohort_alive` returns first and `get_batch_ids` still raises).

## 2026-09-03 — AUDI-1279: per-batch status lines + a dead-cohort alarm (shopper_graph#305, OPEN, not deployed)
Extends "the parquet flags are the only automated visibility" (appended, not overwritten): once #305 is deployed the pod logs
become the second signal; the flags stay the only key-free, dashboard-free one. Ticket:
`tickets/audi_1290_pipeline_optimization_hackathon/audi_1279_openai_batch_observability/summary.md`; decision
`knowledge/decisions/0006_dead_cohort_alarm_is_batch_fetch_failure.md`.
- **What ships:** new `openai/openai_wrapper/batch_status.py` (stdlib only); `batch_base.retrieve_batch(batch_id)` returns the
  Batch or the exception; `batch_transitioner` prints one line per unflagged receipt and now flags `was_submitted` on `finalizing`
  too (was `in_progress`/`completed` only); `batch_fetcher.assert_cohort_alive()` runs before `get_batch_ids()` in
  `fetch_results.py`; `openai/Dockerfile` gets `ENV PYTHONUNBUFFERED=1`; `tests/unit/test_openai.py` 0 -> 16 tests.
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
