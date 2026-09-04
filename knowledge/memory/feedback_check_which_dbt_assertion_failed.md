---
name: feedback_check_which_dbt_assertion_failed
description: "A known false failure belongs to ONE dbt assertion, never to the task: test_product_categorization is spurious only when product_categorization__max_dt fails alone. Read which assertion failed before marking any dbt test success — on 2026-09-04 it was record_count, it was correct, and marking it green shipped a 408 MiB partition into keyword_ddp_reporting."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [mark dbt test success, which assertion failed, batch_test, test_product_categorization, product_categorization__max_dt, product_categorization__record_count, known false failure, false failure is per-assertion, dbt test triage, short partition shipped downstream, keyword_ddp_reporting, openai_batch_results_joined, 99 percent record count, mntn_matched_data_quality, 408 MiB partition, backfill dbt test, mark success footgun, AUDI-1321, IMP-016]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-09-04
---
**A "known false failure" is a property of ONE assertion, never of the task that runs it.** `batch_test.test_product_categorization` runs 6 dbt data tests. The standing note "this fails on every backfilled day, mark it success" is true **only** for `product_categorization__max_dt`, which compares against wall-clock `date_sub(current_date, 2)` and therefore diverges on any late or manual run. The other five key off the latest partition and are backfill-robust.

**Why (2026-09-04, AUDI-1321):** the failing assertion that day was **`product_categorization__record_count`** — it asserts `product_categorization` row count >= 99% of `openai_batch_results_joined` at the same `dt` — and it was **CORRECT**. Marking the task success let a **408 MiB** partition through against a **~4.0-4.3 GB** normal day. `keyword_ddp_reporting` then consumed it and wrote two short downstream partitions, which had to be backed up, deleted and rebuilt. The mark-success cost more work than reading the failure would have.

**How to apply:**
- Open the failing test's output and read the assertion NAME before any mark-success. In `batch_test` the log names each test; `max_dt` failing **alone**, with `record_count` / `dsc_id__{length,not_null,values}` / `product_category_and_key` green, is the only shape that licenses mark-success.
- `record_count` failing means the upstream write is short. Do not mark it; find the missing data.
- Generalize: any runbook line of the form "X always false-fails, mark it success" must name the assertion, the comparison it makes, and the condition under which it is spurious. A line that names only the task is a footgun, because the task fails for real reasons too.
- Same class as [[feedback_validated_is_not_correct]] and [[feedback_hold_evidenced_verdict]]: a green mark is a claim about the data, and a claim needs its evidence checked.

Pipeline mechanics and the `max_dt` derivation: [[reference_mntn_matched_batch_pipeline]]. Durable fix for `max_dt` (key it off the pod's `run_date`/`yesterday` env rather than `current_date`) is **IMP-016**.
