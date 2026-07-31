---
name: reference_mntn_matched_batch_pipeline
description: "mntn_match_incrementals_{submit,fetch} DAG mechanics: task chains, the cross-DAG contract (a GCS submissions file, NOT an Airflow sensor), backfill order, the identical batch_cleanup bookend, and how image_pull_policy=Always makes a task's image depend on WHEN its pod ran vs deploy time."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [mntn_match_incrementals_submit, mntn_match_incrementals_fetch, batch_submit, batch_transition, batch_fetch, batch_prep, batch_validate, batch_post, batch_cleanup, batch_cleanup_1, batch_cleanup_2, batch_test, submit_batch.py, transition_batch.py, fetch_results.py, batch_transitioner, delete_all_storage_files, openai_batch_submissions, cross-dag contract, gcs submissions file, backfill order, mntn matched batch pipeline, DS19 keyword pipeline, openai batch runner, OPEN_AI_BATCH, SHOPPER_GRAPH, image_pull_policy Always, machine_learning dags, dt yesterday contract, FileNotFoundError submissions, openai file storage quota, 2.5TB quota, 30-day file expiry, openai auto-expire files, storage economics, 75 GiB per day, intermittent quota failure, quota fails intermittently, delete_all_storage_files economics, batch_test dbt tests, product_categorization__max_dt, max_dt freshness test, current_date backfill skew, dbt test backfill footgun, mntn_matched_data_quality, post_batch dbt tests, mark test success backfill, keyword_ddp not blocked by batch_test, OSError errno 99 email red herring, IMP-016, IMP-017]
domain: [repos, infra]
lifecycle: active
last_verified: 2026-07-31
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

## `batch_cleanup_1` and `batch_cleanup_2` are IDENTICAL bookend tasks
Same `openai_batch_runner` image, same `delete_all_storage_files.py`, same env — a pre-run cleanup (frees
OpenAI file-storage quota headroom before submitting) and a post-run sweep. They run in BOTH DAGs, so the
cleanup script executes **~4×/day** total.

## `batch_test` post-batch dbt DQ tests — and the backfill CALENDAR-SKEW footgun
`batch_post >> [batch_test, batch_cleanup_2]`. **`batch_test`** runs `dbt test --select product_categorization`
(dbt project `mntn_matched_data_quality`; tests at `SteelHouse/shopper_graph/dbt/tests/mntn_matched/post_batch/*.sql`):
6 tests — `dsc_id__{length,not_null,values}` + `record_count` (data-integrity, hard-fail),
`product_category_and_key` (WARN-only), and **`product_categorization__max_dt`**.
- **`max_dt` keys off WALL-CLOCK, not the logical date** — it asserts a partition exists for
  `date_sub(current_date, 2)` (UTC). On an on-time run that equals the `dt=yesterday` the DAG writes → PASS.
  On a **late / manual backfill of an OLD logical date it fails SPURIOUSLY**: the backfill correctly writes
  `dt=yesterday-of-logical`, but the test looks at `current_date-2`. INC-007 backfill: re-ran logical 07-29 on
  07-31 → wrote `dt=2026-07-28`, test wanted `dt=2026-07-29` → 1-row FAIL though the data was clean
  (record_count + id tests PASS, `dt=07-28/_SUCCESS` in GCS). **Handling a backfill: mark
  `test_product_categorization` SUCCESS, do NOT re-run** (fails every time once UTC > the target date+2).
  Durable fix = key the test off the run's `yesterday`/`run_date` env var → **IMP-016**.
- **A red `batch_test` does NOT block `keyword_ddp`** — the downstream `wait_for_product_categorization` sensor
  targets `batch_post.product_categorization` (UPSTREAM of batch_test); batch_test is a leaf parallel with
  batch_cleanup_2. So its failure is cosmetic to the consumer; clear the sensor independently.
- **Red-herring in the failure log:** a trailing `OSError: [Errno 99] Cannot assign requested address` from
  `airflow.utils.email`/`smtplib` is the POST-failure notification email failing (SMTP unreachable from the
  pod), never the task's cause — task-failure emails silently don't send in this deployment (**IMP-017**).

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
