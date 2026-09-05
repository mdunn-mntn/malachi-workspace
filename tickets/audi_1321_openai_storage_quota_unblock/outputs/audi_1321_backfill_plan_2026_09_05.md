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

## Update 2026-09-05 07:36 UTC — `batch_fetch` is all-or-nothing

Cleared `batch_fetch` alone on fetch `scheduled__2026-08-28T09:00`. It ran 07:32-07:36 and exited
`success` having downloaded nothing. Its log ends with:

```
cohort dt=2026-08-27: n=1261 in_progress=169 finalizing=181 completed=911
                      validating=0 failed=0 expired=0 cancelling=0 cancelled=0 retrieve_error=0
```

**The task downloads nothing unless every batch in the cohort is complete.** 911 of 1,261 were
ready and it still wrote nothing. This is the same "succeeds while downloading nothing" behavior
the first handoff noted, now with the threshold pinned: it is the whole cohort, not a per-batch
decision. Retrying costs about four minutes and is non-destructive, so the recipe is to retry
until the cohort line reads all-complete rather than to guess the ready time.

Also note the run's DAG-level state went back to `failed` immediately. That is **not** this fetch
failing. It is the stale `batch_post.openai_batch_joined` (failed at try 15 on 2026-08-30) that has
sat in the run since the outage. Read the task states, not the run state, on any of these
half-recovered runs.

Nothing failed or expired in the cohort at 7.7h of a 24h window, so the day is not at risk yet.
Retry windows are armed at 11:30, 14:30, 17:30 and 20:30 UTC, all after the 09:00 scheduled fetch
for dt=09-03 has had the slot.


## Caveat 2026-09-05 11:35 UTC — the GCS object counts need re-reading

`gsutil` on this Mac started returning `ReauthUnattendedError`: the credential for
`malachi@mountain.com` still exists but its reauth challenge expired, and the challenge cannot be
answered outside an interactive session. Every `gsutil ls` after roughly 07:30 UTC therefore
returned an empty listing that reads as zero objects rather than as an error, because the failure
went to stderr.

**The per-day table above was read at 01:30 UTC, before this, and stands.** What does not stand is
any count taken after: the parent `openai_batch_results/` prefix listed as empty while the daily
fetch for dt=09-03 was demonstrably writing into it.

The conclusion that dt=2026-08-27 downloaded nothing does not rest on those counts. It rests on the
task's own log, which printed `completed=911` of `n=1261` and no results-written line.

Fix with `gcloud auth login` in an interactive terminal, then re-read the table before acting on
any object count.

## Update 2026-09-05 13:15 UTC — today's 15:00 run is the discriminating test

dt=2026-09-03 finished end to end at 11:5x UTC: fetch `scheduled__2026-09-04T09:00` succeeded
through `batch_post` and `batch_test`, unattended, with no quota error on the paired submit for
dt=09-04. That is the first clean daily pair since 08-28.

`keyword_ddp_reporting` runs `0 15 * * *` with `max_active_runs=1` and is not paused. Its
`scheduled__2026-09-03T15:00` run was never created: the scheduler dropped that interval and moved
`next_dagrun_logical_date` to 2026-09-04T15:00. **That gap is already covered** by
`manual__consume_dt_2026-09-02`, which wrote the same partition by hand, so nothing is missing.

Today's `scheduled__2026-09-04T15:00` run fires at 15:00 UTC. Its sensor looks for the fetch run at
`logical - 6h` = `2026-09-04T09:00`, which is the run that just succeeded, so it should start
unattended and write `data_source_id=19/dt=2026-09-04` from a whole `product_categorization`.

**This settles the open DS19 question.** dt=2026-09-03's DS19 partition came in at 50.6 GB against
a normal 70-72 GB, and the two candidate causes were 09-02's own incompleteness and the 08-28 to
09-01 hole. dt=09-04 is built from a complete day while the hole is still open:

- If `data_source_id=19/dt=2026-09-04` lands near 70-72 GB, the shortfall was 09-02 alone and the
  backfill is not blocking DS19.
- If it lands short again, the missing days feed DS19 and the numbers stay unreliable until the
  backfill finishes.

Measuring it needs the GCS credential repaired first, per the caveat above.

`batch_fetch` for dt=2026-08-27 was re-cleared at 13:13 UTC, cohort age 13.3h, with the fetch slot
free after the daily run finished.

## Update 2026-09-05 14:40 UTC — `batch_fetch` alone is not enough after a re-submit

The 13:13 retry ran with the cohort fully ready and still downloaded nothing:

```
cohort dt=2026-08-27: n=1261 in_progress=0 finalizing=0 completed=1261
                      validating=0 failed=0 expired=0 retrieve_error=0
```

**The gate is not batch completion. It is the `was_submitted` flag on the receipts.** Reading the
source settles it:

- `fetch_results.py` calls `get_batch_ids()`, which is
  `read_parquet(openai_batch_submissions/dt=<yesterday>).query("was_downloaded == False & was_submitted == True")`.
  A fresh submit writes `was_submitted = False`, so that query returns nothing and the download loop
  never executes.
- `assert_cohort_alive()` returns early unless **no** receipt has `was_submitted` set. The cohort
  line is therefore proof that every receipt is still `False`, which is exactly the state in which
  nothing will be downloaded. The line that looks like progress is the symptom.
- `batch_transitioner.transition_to_in_progress()` is what flips it: it selects
  `was_downloaded == False & was_submitted == False` and writes `was_submitted = True` for every
  batch whose status is in `PROGRESSED_STATUSES`.

So `batch_transition` is a required step after a re-submit, not just the probe the recipe describes.
On this run it last succeeded at 2026-09-04 20:54, three hours **before** the submit finished at
23:54, so it had nothing to flag and the two later `batch_fetch` clears could not have worked.

**Corrects Step 1 above and the recipe in `summary.md`.** After a re-submit, clear
`batch_transition` **and** `batch_fetch`, in that order, and still without downstream. The
"clear `batch_fetch` alone" rule only applies to a day whose receipts are already transitioned.

Cleared both at 14:40 UTC. Transition is about 6-8 minutes for this cohort size and the download
about 90 minutes, judging by dt=09-03's 87.

## Update 2026-09-05 15:55 UTC — credential repaired, and the size benchmarks were wrong

`gcloud auth login` was run, so `gsutil` reads are trustworthy again. Measured, in GiB:

| dt | `data_source_id=19` | `targeted_signal_domain` | `product_categorization` | note |
|---|---|---|---|---|
| 2026-08-25 | 65.3 | 40.6 | 4.3 | healthy, pre-outage |
| 2026-08-26 | 66.9 | 41.6 | 4.0 | healthy, pre-outage |
| 2026-08-27 | 65.8 | 41.2 | 0.0 | signals predate the outage; its own categorization never built |
| 2026-08-28 | 0.0 | 0.0 | 0.0 | the hole |
| 2026-09-01 | 0.0 | 0.0 | 0.0 | the hole |
| 2026-09-02 | 0.0 | 0.0 | 4.0 | categorization built from 468 of 1,014 results |
| 2026-09-03 | 47.1 | 33.9 | 7.5 | signals written from 09-02's partial; categorization is its own, whole |
| 2026-09-04 | writing | writing | 0.0 | the 15:00 run, in flight |

**Two benchmarks recorded earlier in this ticket are wrong and are corrected here.** DS19 normal is
65-67 GiB, not 70-72, and `targeted_signal_domain` normal is 40.6-41.6 GiB, not 44.5. The dt=09-03
DS19 partition is 47.1 GiB, not the 50.6 quoted earlier. Against the corrected baseline it is 28%
short and the domain partition is 17% short, which is the same conclusion from better numbers.

**`product_categorization` at 7.5 GiB for dt=09-03 is the odd one.** Healthy days are 4.0-4.3 and
this is nearly double, from a single `try=1` run of a model that appends. It is not obviously wrong
and it is not the shortfall's cause, but the "normal is 4.0-4.3 GiB" line should not be used to
judge a partition as complete until this is explained. Do not chase it while the backfill is open.

The discriminating test is still pending: `data_source_id=19/dt=2026-09-04` is being written now
from dt=09-03's whole categorization. 65-67 GiB clears the missing days of blame for DS19.

**dt=2026-08-27 is downloading.** `batch_transition` ran 14:35-14:42 and flagged the cohort,
`batch_fetch` started 14:42, and `openai_batch_results/dt=2026-08-27/` held 815 of 1,261 objects at
15:51. That confirms the `was_submitted` diagnosis directly: same cohort, same task, nothing
changed but the transition.
