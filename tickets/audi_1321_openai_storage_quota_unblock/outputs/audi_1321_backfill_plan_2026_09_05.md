# AUDI-1321 backfill plan — dt=2026-08-27 through 2026-09-03

State read from GCS and the Airflow REST API at 2026-09-05 01:30 UTC. Supersedes the ordering in
`audi_1321_handoff_2026_09_04b.md`; the per-day recipe in `summary.md` § "The corrected per-day
backfill recipe" is still the procedure and is not restated here.

## Where every day actually stands

Object counts under `gs://mntn-data-archive-prod/shopper_graph/`.

| dt | inputs (`openai_batch_input_formatted`) | receipts (`openai_batch_submissions`) | results (`openai_batch_results`) | `product_categorization` | what it needs |
|---|---|---|---|---|---|
| 2026-08-27 | 1,261 | 1,261 ✅ | 0 | 0 | fetch only — batches are live at OpenAI |
| 2026-08-28 | 1,241 | 791 partial | 0 | 0 | full day: probe, delete receipts, submit, fetch |
| 2026-08-29 | 1,021 | 653 partial | 0 | 0 | full day |
| 2026-08-30 | 868 | 510 partial | 0 | 0 | full day |
| 2026-08-31 | 1,068 | 733 partial | 0 | 0 | full day |
| 2026-09-01 | 1,226 | 0 | 0 | 0 | submit only — no receipts to delete |
| 2026-09-02 | 1,014 | 1,014 | 468 (46%) | 4.0 GB | 546 dead batches: delete receipts, re-submit, re-fetch, rebuild |
| 2026-09-03 | 1,004 | 1,004 ✅ | 0 | 0 | fetch only — today's 09:00 run does it unattended |

A receipt count below the input count is the Aug 27-30 outage's signature: `was_submitted=False`
rows that still carry a real `openai_batch_id`, which the double-submission guard reads as
"already sent". They must be deleted or the re-submit only picks up the never-attempted files.

## Run-id arithmetic

For a target `dt=D`: submit run is `scheduled__D T09:00:00+00:00`, fetch run is
`scheduled__(D+1) T09:00:00+00:00`. Submit writes `dt = data_interval_start`; fetch reads
`dt = data_interval_start - 1 day`.

## Hard constraints on the ordering

- **`mntn_match_incrementals_fetch` is `max_active_runs=1`.** Clearing a task on an old fetch run
  puts that run back to `running` and holds the only slot, so the 09:00 scheduled fetch queues
  behind it. Never clear two fetch runs at once.
- **One day of batches at a time.** A day is ~1,014-1,261 input files at ~40 MB (~40 GB) plus
  ~46 GB of results. Two days do not fit under the 2.5 TB project ceiling, which is what broke the
  09-03 submit in the first place.
- **OpenAI's completion window is 24h from submit.** dt=08-27's 1,261 batches went in at
  2026-09-04 22:30-23:54 UTC, so they complete around 06:00-07:00 UTC on 09-05 and hit their window
  at 23:54 UTC on 09-05.
- **`batch_submit` takes about 1h20m** for ~1,260 files, and the batches then need roughly 7 hours
  to complete. One day per calendar day is the realistic cadence.

## Step 1 — dt=2026-08-27, today, before or after the 09:00 pair

`batch_fetch` on fetch run `scheduled__2026-08-28T09:00` already ran at 2026-09-04 22:54 UTC and
downloaded nothing, because its own submit did not finish until 23:54 UTC. That run is `failed`
with `batch_post.openai_batch_joined` failed at try 15 back on 2026-08-30 and every task below it
`upstream_failed`.

1. Wait for the READY marker from the armed watcher (~07:30 UTC) or confirm completion directly.
2. Clear `batch_fetch` **alone** on `scheduled__2026-08-28T09:00`.
3. Verify the results partition is whole: `openai_batch_results/dt=2026-08-27/` should reach
   about 1,261 objects, not a fraction.
4. Only then clear the `batch_post` group and `batch_test` on that run.

If this slips past the 09:00 scheduled fetch, let the scheduled run finish first. It is the daily
run for dt=09-03 and must not be starved.

## Step 2 — dt=2026-09-03, today, unattended

The 09:00 UTC pair on 09-05 runs submit for dt=09-04 and fetch for dt=09-03. dt=09-03's 1,004
batches were submitted 2026-09-04 19:25-20:47 and complete around 02:30 UTC, so the scheduled fetch
should find them. No action unless the watcher reports a failure.

## Step 3 — the four partial days, one per day, in this order

`2026-08-28` (791 receipts) → `2026-08-29` (653) → `2026-08-30` (510) → `2026-08-31` (733).

Per day, per the recipe in `summary.md`:

1. Clear `batch_transition` on fetch `scheduled__(D+1)T09:00` and read its `cohort dt=D:` line.
   Do not skip this: the receipts name real batches, and anything still `completed` is free work.
2. Harvest whatever the probe calls `completed` by clearing `batch_fetch` alone. Expect zero;
   an output file older than the sweep window is already deleted.
3. Back up the receipts to `gs://mntn-data-archive-prod/_backups/audi_1321/`, then delete
   `openai_batch_submissions/dt=D/`.
4. Clear `batch_submit` on submit `scheduled__D T09:00`. About 1h20m.
5. Confirm the receipt count equals the input count before starting the next day.
6. Roughly 7 hours later, clear `batch_fetch` on `scheduled__(D+1)T09:00`, then its `batch_post`
   and `batch_test` groups.

## Step 4 — dt=2026-09-01

No receipts exist, so there is nothing to back up or delete and no guard to trip. Clear
`batch_submit` on `scheduled__2026-09-01T09:00` and follow steps 4-6 above.

## Step 5 — dt=2026-09-02, last

This day is the messiest because it was half-recovered by hand. 468 of 1,014 batches downloaded;
the other 546 are dead. `product_categorization/dt=2026-09-02/` is 4.0 GB, which reads as a normal
day and is therefore misleading: it was rebuilt off the 468. Treat the 4.0 GB as unverified until
the day is whole.

1. Back up and delete `openai_batch_submissions/dt=2026-09-02/` (all 1,014 receipts).
2. Clear `batch_submit` on submit `scheduled__2026-09-02T09:00`.
3. Fetch on `scheduled__2026-09-03T09:00` once the batches complete.
4. Back up and delete `product_categorization/dt=2026-09-02/`, then clear
   `batch_post.product_categorization` and `batch_test.test_product_categorization`.
   The model is `incremental_strategy="append"` and raises `FileExistsError` while the partition
   has data, so it cannot self-heal.
5. Re-run `keyword_ddp_reporting` for that day. Its `run_date = {{ ds }}`, so the run that consumes
   dt=09-02 writes dt=09-03. Clear `write_targeted_signal_ds_19` and let the chain run forward;
   the chain is sequential (`ds_19 >> ds_13 >> ds_19_domain`), so clearing `ds_19` and
   `ds_19_domain` together races them.

## Step 6 — verify the DS19 outputs recovered

Once 09-02 and 09-03 are both whole, re-measure:

- `gs://mntn-data-archive-prod/signals/targeted_signal/data_source_id=19/dt=2026-09-03/` —
  last read 50.6 GB against a normal 70-72 GB.
- `gs://mntn-data-archive-prod/signals/targeted_signal_domain/dt=2026-09-03/` —
  last read 36.4 GB against a normal ~44.5 GB.

The open question is whether that shortfall is 09-02's own incompleteness or the 08-28 to 09-01
hole. If both partitions rise toward normal once 09-02 is whole, it was 09-02. If they stay short,
the missing days are feeding it and the backfill has to finish before the numbers mean anything.

## Keeping the daily runs unblocked throughout

The 26h input / 48h output retention split shipped in shopper_graph #310 frees one day of headroom
per sweep with no environment variable and no manual step, which is what unblocked 09-03. The
daily 09:00 pair therefore needs no intervention. The only way a backfill day starves it is by
holding the single fetch slot, so a backfill fetch is always cleared either before 09:00 or after
the scheduled run has finished.

## What is watching

A persistent monitor polls the Airflow REST API every five minutes and emits on any state change
in `mntn_match_incrementals_submit`, `mntn_match_incrementals_fetch`, or `keyword_ddp_reporting`,
naming the failed tasks on a failure. It also emits a READY marker at 07:30 UTC for the 08-27
fetch and deadline markers at 6h and 2h before the 24h window closes at 23:54 UTC.
It is session-scoped: it dies when this session ends.
