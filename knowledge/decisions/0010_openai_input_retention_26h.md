---
doc_type: decision
title: "0010 — OpenAI batch INPUT files retain 26h, outputs 48h: an input must outlive the 24h completion window"
summary: "shopper_graph #310 sets the input retention default to 26h after a 12h window deleted the inputs of 119 live batches and failed all of them (failed=119, expired=0); outputs stay at 48h because the next day's fetch reads them"
status: accepted
date: 2026-09-04
last_verified: 2026-09-04
keywords: [openai input retention, OPENAI_FILE_MAX_AGE_HOURS, 26 hours, 12 hour window failed batches, completion_window 24h, batch_transition failed 119, delete_all_storage_files, purpose=batch inputs, output retention 48h, shopper_graph#310, shopper_graph#307, AUDI-1321, retention below completion window, live batch input deleted, async job input retention rule]
supersedes: null
tags: [shopper_graph, openai, storage, retention]
---

# 0010 — OpenAI batch INPUT files retain 26h, outputs 48h: an input must outlive the 24h completion window

## Context
Decision `0009` deliberately refused to lower the OpenAI retention window until the store had been measured. Once
`#308`/`#309` measured it (129 files / 4.2 GiB of files visible to our key; the ownership reading of that number is
retracted in decision `0009`'s 2026-09-05 correction, but the store WAS measured), lowering the
window became defensible and the split between inputs and outputs became the real question. An input is spent once
its batch is created, minutes after upload; only outputs must survive until the next day's `batch_fetch`. A short
input window is therefore what frees a stalled day without needing an environment variable the pod cannot receive.

An input window of **12h** was tried on that reasoning and it is wrong in kind, not in degree. OpenAI's batch
`completion_window` is **24h**, so an input file must outlive the batch it feeds. On 2026-09-04 the 18:00 sweep
deleted the input files of `dt=2026-09-02`'s still-running batches at the 12h mark, and `batch_transition` on the
119 untransitioned receipts came back **`failed=119, expired=0`**. The 468 batches that had already completed were
untouched — the tell that this kills live batches specifically, not old ones. Left in place, the 2026-09-05 09:00
sweep would have deleted `dt=2026-09-03`'s inputs at 07:24 and failed all 1,004 of its batches the same way.

## Decision
Ship `shopper_graph` **#310**: input retention default **26h**, output retention unchanged at **48h**. 26h is 24h
plus two hours of headroom for submit-to-create latency and sweep timing, and it is still short enough that a
stalled day's inputs age out inside one cycle. Deployed 2026-09-04.

## Alternatives considered
- **12h inputs** — this is what failed. Any window below the 24h completion window deletes inputs out from under
  live batches, and the failure is silent at the sweep (it deleted eligible files, exactly as told) and only shows
  up a cycle later as a cohort of failed batches.
- **Keep inputs at 48h** — safe but it was the shape that let a multi-day backlog accumulate to the cap while the
  listing defect hid it. With the listing fixed, 48h works; 26h leaves less standing inventory for the next latent
  defect to pile onto.
- **Make it an Astro deployment variable (`OPENAI_FILE_MAX_AGE_HOURS`, #307)** — still inert. An Astro deployment
  Environment-tab variable does not reach a `KubernetesPodOperator` pod, so the value must live in the image
  default or in an `airflow-ti` PR that adds it to the DAG's `env_vars`. Changing the default in the image is the
  only route that takes effect today.

## Consequences
- **Rule, generalized beyond OpenAI:** a retention window on an ASYNC job's INPUT must exceed that job's own
  completion window, with headroom. The sweep cannot tell a spent input from a live one, so the window is the only
  guard.
- `dt=2026-09-02`'s 119 failed batches are unrecoverable and roll into the re-submit for that day.
- Outputs at 48h keep the `files.content(output_file_id)` 404 risk for any cohort older than two days, which is a
  separate constraint on backfill (see `reference_mntn_matched_batch_pipeline` RULE 2).
- **Affected knowledge docs:** `data_knowledge.md` § MNTN Matched pipeline, `data_catalog.md`
  § shopper_graph/openai_batch_submissions, memory `reference_mntn_matched_batch_pipeline` § 2026-09-04 (evening),
  `reference_openai_sdk_pagination`, glossary "Input retention window (OpenAI batch)".
