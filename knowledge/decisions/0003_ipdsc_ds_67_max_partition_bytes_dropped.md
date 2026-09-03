---
doc_type: decision
title: "0003 — ipdsc_ds_67 spill: maxPartitionBytes dropped, keys wont_fix, the fix is the join or the writer"
summary: "Drop the 32 MiB maxPartitionBytes edit for ipdsc_ds_67 (its DS4 input is single-row-group parquet the knob cannot split); set disk_spill:3/:5 wont_fix; the real fix is F.broadcast on the audience_uploads join or smaller row groups at the DS4 writer"
status: accepted
date: 2026-09-02
last_verified: 2026-09-03
keywords: [ipdsc_ds_67, maxPartitionBytes, parquet row group, single row group, wont_fix, optimizer ledger, broadcast join, parquet.block.size, data_source_id=4, AUDI-1273, AUDI-1290]
supersedes: null
tags: [spark, airflow-ti, optimizer]
---

# 0003 — ipdsc_ds_67 spill: maxPartitionBytes dropped, keys wont_fix, the fix is the join or the writer

## Context
AUDI-1273 (hackathon epic AUDI-1290) specified `spark.sql.files.maxPartitionBytes` on three map-side-spill
DAGs, 32 MiB for `ipdsc_ds_67`. Its stage 3/5 input
`gs://mntn-data-archive-prod/ipdsc/dt=<date>/data_source_id=4/` is ~160 files of 60 MiB with ONE parquet row
group each, and Spark 3.5.3 assigns a row group to the split holding its midpoint, so a 32 MiB cap yields one
task reading the whole file and one reading nothing (confirmed locally with
`tickets/audi_1290_pipeline_optimization_hackathon/audi_1273_max_partition_bytes/artifacts/audi_1273_split_probe.py`).
The spill itself is the 81.6 GiB sort-merge exchange stages 3 and 5 each write to join `ui.audience_uploads`,
which AQE later converts to a broadcast anyway.

## Decision
Ship AUDI-1273 with the `ipdsc_ds_49` and `conv_log_derived_ip` edits only. `ipdsc_ds_67` is untouched; its
ledger keys `disk_spill:3` and `disk_spill:5` are set `wont_fix` with the reason. The broadcast fix
(`F.broadcast(audience_upload_ids)` at `models/ipdsc/ipdsc_ds_67.py:80`, plus caching `upload_ips`) is a
backlog row (IMP-102) for the model owner, not a ticket. User decision D1 = Option A, 2026-09-02.

## Alternatives considered
- **Apply the 32 MiB cap as specified** — rejected: a no-op on single-row-group files that adds ~160 empty
  tasks per pass, and the ledger would mark the PR `fix_not_working` after three sweeps.
- **Replace the config edit with the one-line broadcast hint in the same PR (Option B)** — deferred: the
  largest expected win (~150 GiB disk spill and ~9.4 exec-h/day) but a code change in a model owned by
  Alyson Lefkowitz, outside a config-only ticket; handed over as IMP-102.
- **Have the DS4 writer emit smaller row groups (Option C)** — out of scope: the writer is the targeted-signal
  pipeline (`spark/data_source/populate_data_source.py`), another team's code.

## Consequences
- A read-stage `disk_spill` finding is not a `maxPartitionBytes` finding until the input's row groups per
  file are checked; the optimizer's fix text must not presume splittable input.
- `wont_fix` is a sticky ledger state that survives the daily replay; the two keys stay visible with the reason.
- **Affected knowledge docs:** [`../memory/reference_dataproc_eventlog_profiling.md`](../memory/reference_dataproc_eventlog_profiling.md), [`../bq/external/ipdsc__v1.md`](../bq/external/ipdsc__v1.md), [`../data_catalog.md`](../data_catalog.md) § bronze.external.ipdsc__v1, [`../memory/project_airflow_optimizer.md`](../memory/project_airflow_optimizer.md), `improvements_backlog.md` IMP-102.
