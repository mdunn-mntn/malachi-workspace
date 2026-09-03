---
doc_type: decision
title: "0004 — Speculation on GCS writers: safe iff every writer discards the losing attempt; canary first"
summary: "spark.speculation=true is safe for a Spark application only when every writer in it discards the losing duplicate attempt (FileOutputCommitter v2 under the commit coordinator, Iceberg); 11 of the 13 straggler DAGs qualify by source, one canary (site_network_hourly, airflow-ti #1271) ships first, the manifest-committer pair stays owner-gated"
status: accepted
date: 2026-09-02
last_verified: 2026-09-03
keywords: [spark.speculation, straggler, GCS writer, FileOutputCommitter v2, OutputCommitCoordinator, CommitDeniedException, Iceberg SparkWrite, manifest committer, site_network_hourly, canary, PR 1271, audience_intent scoring, advertiser_join, prospecting_join, AUDI-1275, AUDI-1290, ryan kleck speculation caveat, speculation adds executors under skew, canary kill criterion, executor-hours rise wall-clock flat, speculative_tasks fires not helps, merged b9428f4]
supersedes: null
tags: [spark, airflow-ti, optimizer]
---

# 0004 — Speculation on GCS writers: safe iff every writer discards the losing attempt; canary first

## Context
AUDI-1275 (hackathon epic AUDI-1290) had to settle the straggler remedy for 13 airflow-ti DAGs after
`spark.speculation=true` was proposed and refuted twice as unsafe for GCS writers (the 2026-08-27 gauntlet revert on
`ipdsc_ds_35`; the repo's `spark.speculation=false` pins, comment "Disabled to prevent race conditions with
ManifestCommitter"). Read from source at the exact tags and from 18 prod event logs on 2026-09-02: Spark 3.5.3's
`OutputCommitCoordinator` authorizes one attempt per partition and denies the rest with `CommitDeniedException` for
any Hadoop `OutputCommitter`; Hadoop 3.3.5 `FileOutputCommitter` v2 (injected by Dataproc Serverless runtime 2.3 on
every batch) moves an attempt's files into the destination only in `commitTask` and deletes the attempt dir on
`abortTask`; Iceberg 1.10.2 `SparkWrite` deletes an aborted attempt's files and commits only accepted `TaskCommit`
messages. The pins exist on 2 of the 13 (`advertiser_join`, `prospecting_join`, manifest committer), and the Nov 2025
incident behind them was a `FileNotFoundException` in the manifest committer's job-commit rename phase that persisted
after speculation was off. The `audience_intent` scoring batches have run speculation on v2 to GCS since 2025-08-15
with hundreds of duplicates killed per run and no failures. Speculation is application-wide; Spark 3.5.3 has no
per-stage switch.

## Decision
A DAG may take `spark.speculation=true` only when EVERY writer in its Spark application provably discards the losing
duplicate attempt (FileOutputCommitter v2 under the commit coordinator, or Iceberg). Eleven of the 13 qualify by
source; ship it to ONE canary first, `site_network_hourly` (ours, hourly, each hour written twice, PR
[airflow-ti#1271](https://github.com/SteelHouse/airflow-ti/pull/1271), reviewer Ryan Kleck). The other 10 wait for
three clean optimizer sweeps on the canary plus Ryan's answer to the Slack ask; `advertiser_join` and
`prospecting_join` (manifest committer, Ryan's pin) stay owner-gated regardless. User decision D1+D2, 2026-09-02.

## Alternatives considered
- **Apply speculation to all 11 safe-by-source DAGs in one PR (the planned step 11)** — rejected: a 13-DAG blast
  radius on another team's pipelines for a twice-refuted change; a canary settles safety in prod with a one-line revert.
- **Per-stage speculation (compute stages only)** — not available: Spark 3.5.3 has no per-stage switch, and "the
  straggler stage is compute-only" does not stop a duplicate attempt in the write stage.
- **Move the manifest pair back to the default v2 committer, or shrink their shuffle tasks** — deferred to Ryan (ask 2
  in the Slack draft); either removes the need for speculation there but touches his pipelines.
- **Memo and ask only, no PR (the §0 fallback)** — not taken: a remedy was provable from source, and only a prod run
  turns the source reading into a prod fact.

## Amendment 2026-09-03 — merged, plus Ryan Kleck's caveat, which becomes the canary's kill criterion

The canary **merged 2026-09-03 20:20 UTC** (PR #1271, branch `audi-1275-straggler-gcs-writers`, squash `b9428f4`) and
deployed. Note the numbering offset: **PR #1271 is AUDI-1275**, not AUDI-1271.

Ryan's review added a caveat this decision did not carry, and it is about EFFICACY, not safety: **with skew,
speculation often just adds executors**, because the duplicate attempts chase the same long tail rather than
shortening it. A speculative copy of a task that is slow *because it holds more data* is exactly as slow, and both
attempts are billed. Safety (does the writer discard the loser) and usefulness (does the tail shorten) are separate
questions, and this decision only settled the first.

**Kill criterion for the canary: kill it if executor-hours rise while wall-clock stays flat.** That is the exact
signature of speculation buying nothing. Both numbers are already on the optimizer ledger, so nothing new needs
instrumenting. Corollary for reading the post-merge watch: `speculative_tasks > 0` proves speculation is FIRING, not
that it is HELPING.

## Consequences
- Straggler recommendations on GCS writers are decided by writer class per application, not by the straggler stage;
  the optimizer's fix text must name the committer check, and a `FetchFailed` count per stage comes first because the
  detector also fires on fetch-wait tails speculation cannot fix.
- The canary measures safety first (`Task Info.Speculative=true` attempts ending `TaskKilled` / `Success`, `_SUCCESS`
  and per-hour file counts inside the 7-day band); a runtime win is not expected because `site_network_hourly`'s long
  runs are FetchFailed storms.
- The 2026-08-27 contradiction record is appended to, not overwritten, until Ryan's account and the first post-merge
  log are in.
- **Affected knowledge docs:** [`../memory/reference_dataproc_eventlog_profiling.md`](../memory/reference_dataproc_eventlog_profiling.md), [`../memory/project_airflow_optimizer.md`](../memory/project_airflow_optimizer.md), [`../memory/reference_airflow_ti.md`](../memory/reference_airflow_ti.md), [`../data_catalog.md`](../data_catalog.md) § ipdsc_site_network/site_network_hourly, [`../data_knowledge.md`](../data_knowledge.md) § augmentor_log TTL and Archives, `improvements_backlog.md` IMP-104.
