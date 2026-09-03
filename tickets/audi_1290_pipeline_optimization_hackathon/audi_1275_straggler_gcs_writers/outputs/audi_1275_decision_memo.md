# AUDI-1275 decision memo: the safe straggler remedy for Spark jobs that write GCS

**Answer:** `spark.speculation=true` is safe for 11 of the 13 DAGs, because every writer in those Spark applications discards the losing duplicate attempt before it can reach the destination (Hadoop FileOutputCommitter v2 under Spark's commit coordinator for 10 of them, Iceberg for identity_targeted_signal). It is applied now to one canary, `site_network_hourly` (ours, hourly, re-run tolerant). The other 10 safe DAGs wait for three clean optimizer sweeps on the canary plus Ryan Kleck's answer. `advertiser_join` and `prospecting_join` run the manifest committer under Ryan's explicit pin and stay owner-gated regardless.

Date: 2026-09-02. Evidence read on airflow-ti main `825b07e3`, 16 Spark event logs from `gs://mntn-data-archive-prod/spark-events/` plus 2 from the persistent history server, the live optimizer ledger, and the Spark 3.5.3 / Hadoop 3.3.5 / Iceberg 1.10.2 sources at the exact tags.

## 1. The rule

Speculation is application-wide in Spark 3.5.3: every `spark.speculation.*` knob applies to all stages, there is no per-stage switch. A DAG is changeable only if **every** stage that writes output tolerates a second attempt of the same task running concurrently. "Straggler stage is compute-only" is not sufficient: a speculative copy can also be launched in the write stage.

What a duplicate attempt does depends on the committer:

| Writer class | Where an attempt writes | What happens to the loser | Verdict |
|---|---|---|---|
| Hadoop FileOutputCommitter, algorithm 2 (default on Dataproc Serverless) | its own `_temporary/0/_temporary/attempt_<id>` directory; files reach the destination only in `commitTask` | Spark asks the driver's `OutputCommitCoordinator` before `commitTask`; the first attempt per partition is authorized, every other attempt gets `CommitDeniedException` and `abortTask` deletes its attempt directory | safe |
| Iceberg 1.10.2 `overwritePartitions()` | new data files under the table location, referenced only by the task's commit message | the winner's `TaskCommit` message is the one Spark hands to job commit; a killed or failed attempt runs `DataWriter.abort()`, which deletes that attempt's files; the snapshot only ever references files carried in accepted messages | safe (an attempt that finished before the kill can leave an unreferenced orphan file; readers never see it) |
| Hadoop manifest committer (`ManifestCommitterFactory`, `PathOutputCommitProtocol`) | per-attempt directory plus a manifest written at task commit; job commit renames manifest-listed files | the same coordinator rule applies (it is a Hadoop `OutputCommitter`, called through `SparkHadoopMapRedUtil.commitTask`), so the design is also attempt-isolated; but the repo owner pinned speculation off on this committer after an incident and the Hadoop doc calls concurrent writes to one directory "NOT TESTED" | owner-gated |

Task-side side effects that a duplicate attempt would repeat (network calls, database writes, GCS clients inside `foreachPartition` / `mapPartitions` / UDFs): none in the 13 models. The only UDFs are pure functions (`pandas_udf` with `tldextract` in `aug_log_ip_vertical_id_hourly`, `F.udf` in `site_visit_signal_advertiser_id_dsc_id`). `tpa_mntn_id_export` calls `storage.Client()` and `blob.upload_from_string` on the driver after the write (`models/tpa_export/tpa_mntn_id_export.py` L343-350), not inside a task.

## 2. Source quotes the rule rests on

**Spark 3.5.3, `core/src/main/scala/org/apache/spark/mapred/SparkHadoopMapRedUtil.scala` L62-91.** `commitTask` first checks `committer.needsTaskCommit`; then, with `spark.hadoop.outputCommitCoordination.enabled` (default `true`), it calls `outputCommitCoordinator.canCommit(stageId, stageAttemptNumber, splitId, attemptNumber)`. If denied: `committer.abortTask(mrTaskContext)` then `throw new CommitDeniedException(...)`. The source comment: "We only need to coordinate with the driver if there are concurrent task attempts. Note that this could happen even when speculation is not enabled (e.g. see SPARK-8029)."

**Spark 3.5.3, `scheduler/OutputCommitCoordinator.scala` L174-199, `handleAskPermissionToCommit`.** Per stage and partition, `authorizedCommitters(partition)` is set to the first `TaskIdentifier(stageAttempt, attemptNumber)` that asks; any later ask logs "Commit denied ... already committed by $existing" and returns `false`. An attempt already marked failed is also denied.

**Hadoop 3.3.5, `FileOutputCommitter.java`.** `getTaskAttemptPath` = `<out>/_temporary/<jobAttempt>/_temporary/<taskAttemptID>` (L257-280). `commitTask` with `algorithmVersion` 2 (L575-620): "directly merge everything from taskAttemptPath to output directory" via `mergePaths(fs, taskAttemptDirStatus, outputPath, context)`, then deletes the attempt directory. `abortTask` (L641-653): `fs.delete(taskAttemptPath, true)`. `needsTaskCommit` (L667-675): `fs.exists(taskAttemptPath)`. So the only path by which a task's bytes reach the destination is the authorized `commitTask`; an aborted attempt leaves nothing there.

**Spark 3.5.3, `sql/.../FileFormatWriter.scala` L400-411.** The task body is `dataWriter.writeWithIterator(iterator); dataWriter.commit()` inside `tryWithSafeFinallyAndFailureCallbacks`, whose catch block is `dataWriter.abort()`. A killed speculative attempt (`TaskKilled`) goes through the same abort.

**Spark 3.5.3, `WriteToDataSourceV2Exec.scala` L458-488 (Iceberg path).** When `batchWrite.useCommitCoordinator` is false the writer commits directly ("Writer for partition ... is committing."); on any failure the catch block runs `dataWriter.abort()`.

**Iceberg 1.10.2, `spark/v3.5/.../SparkWrite.java`.** `BaseBatchWrite.useCommitCoordinator()` returns `false` (L277-279). `DataWriter.abort()` = `close(); SparkCleanupUtil.deleteTaskFiles(io, result.dataFiles())` (L769-774). `DynamicOverwrite.commit(messages)` builds the `ReplacePartitions` operation only from `files(messages)`, the `TaskCommit` messages Spark passed in (L308-343). Spark's scheduler accepts one result per partition, so the snapshot never references a duplicate's file.

**Hadoop 3.3.5 `manifest_committer.md`.** "On Google GCS, neither the v1 nor v2 algorithm are _safe_ because the google filesystem doesn't have the atomic directory rename which the v1 algorithm requires" (this is about job-level atomicity of a directory rename, which v2 does not use at task commit; it is the reason the manifest committer exists, not a statement about duplicate attempts). `validate.output` "triggers a check of every renamed file to verify it has the expected length ... recommended for testing only". Section "Support for concurrent jobs to the same directory": "This has *NOT BEEN TESTED*".

## 3. What actually runs in prod (event logs, all Spark 3.5.3, Dataproc Serverless runtime 2.3)

Every one of the 16 model logs carries `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2`, injected by Dataproc (none of the models set it). Classpath in every log: `gcs-connector.jar` (unversioned basename), `hadoop-cloud-storage-3.3.5.jar`, `spark-hadoop-cloud_2.13-3.5.3.jar`, `iceberg-spark-runtime-3.5_2.13-1.9.0-google-5.jar` (identity_targeted_signal ships its own `iceberg-spark-runtime-3.5_2.13-1.10.2.jar`). Only `advertiser_join` and `prospecting_join` set `mapreduce.outputcommitter.factory.scheme.gs=ManifestCommitterFactory`, `commitProtocolClass=PathOutputCommitProtocol`, `parquet.output.committer.class=BindingParquetOutputCommitter`, `manifest.committer.validate.output=false`, and `spark.speculation=false`. The other 11 leave `spark.speculation` unset (Spark default `false`) and `outputCommitCoordination.enabled` unset (default `true`). Full table: `outputs/audi_1275_app_props.csv`; per-stage detail: `outputs/audi_1275_stage_evidence.csv`.

**Prod precedent (persistent history server logs).** The `audience_intent` DAG's script batches have run `spark.speculation=true` with FileOutputCommitter v2 and `df.write.mode("overwrite").parquet(gs://...)` since 2025-08-15 (commit `40ecc73`, `dags/audience_intent/audience_intent.py` L109). Two logs read on 2026-09-02:

| batch | app id | speculative task attempts | task end reasons | write stage | result |
|---|---|---|---|---|---|
| scoring-vertical-mid | `app-20260902044737985-0424` | 366 | Success 29,469; TaskKilled 366; failures 0 | stage 29, 2,000 tasks, 42.0 GB | SUCCEEDED |
| scoring-prospecting-high | `app-20260902050521545-0199` | 88 | Success 16,261; TaskKilled 88; failures 0 | stage 20, 5,000 tasks, 1,377.9 GB | SUCCEEDED |

Every speculative attempt in both logs ended as `TaskKilled` (the loser was killed once the first attempt finished); no `CommitDenied`, no task failure, `_SUCCESS` written. Exposure the API can still show: `gcloud dataproc batches list --project mntn-prj-prod-00 --filter=labels.airflow-dag-id=audience-intent` retains about seven days; in 2026-08-27 to 2026-09-03 the five speculation batches (`scoring-vertical-high`, `-vertical-mid`, `-prospecting-high`, `-prospecting-mid`, `-prospecting-keywords`) show 38 SUCCEEDED and 0 failed (one RUNNING at list time). Five batches a day since 2025-08-15 is roughly 1,900 runs; only the last 38 are API-verifiable (`outputs/audi_1275_audience_intent_batches_all.csv`).

## 4. The Nov 2025 pin, reread from git

| commit | date | author | change |
|---|---|---|---|
| `64afca4` | 2025-10-08 | Dustin Niehoff | manifest committer added to `include/spark/data_source/ipdsc_emr_cluster.py` (ipdsc and export jobs), **speculation left at `true`** |
| `009e99a` | 2025-11-12 | rkleck-mntn | `spark.speculation` `true` to `false` in the same file, comment "Disabled to prevent race conditions with ManifestCommitter", empty body |
| `5fbeb38` | 2025-11-17 | rkleck-mntn | "Fix mntn_select ManifestCommitter race condition": body names "ISSUE 1: FileNotFoundException during ManifestCommitter parallel rename", fixed with `manifest.committer.io.threads=1` and `validate.output=false`; "Fewer files = fewer parallel renames = less chance of race condition" |
| `a3352d9` | 2025-11-20 | rkleck-mntn | same two settings applied to `tpa_ipdsc_export` (ipdsc, ipdsc_geo, tpa_export batches), "prevents FileNotFoundException during parallel rename phase" |
| `6afc07f` | 2025-12-09 | rkleck-mntn | manifest committer plus the `false` pin copied to `advertiser_join` and `prospecting_join`; `io.threads=1` removed from ipdsc |

Reading: the failure that persisted after speculation was disabled on 11-12 was a `FileNotFoundException` in the manifest committer's **job-commit rename phase**, and it was fixed on 11-17 and 11-20 by serializing that phase and switching off the per-file length check. Job commit runs on the driver after every task has committed; a speculative duplicate is already dead by then. So the git record does not show speculation causing the incident, and it does show the incident continuing after speculation was off. That is the discriminating fact to put to Ryan (ask 1). It does not by itself clear the manifest pair: the pin is the owner's explicit instruction and the incident mechanism on GCS listing/rename under the manifest committer is not fully explained by the commits.

## 5. Per-DAG verdicts

Straggler keys and states from the live ledger (`outputs/optimization_ledger_live.jsonl`, last sweep 2026-09-02). "compute" = the flagged stage wrote 0 output bytes; "write" = it wrote output. Stage ids drift run to run.

| DAG (model) | Launcher | Writer class | Live straggler keys (state) | Flagged stage: class, tasks, median s, max s | Write stages in the app | Verdict |
|---|---|---|---|---|---|---|
| site_network_hourly | site_network_hourly (TPA_EXPORT, ours) | parquet overwrite `/dt=/hh=`, v2 | 9 chronic(4); 11, 13, 15, 18, 21-24, 29, 39 new | compute: 11 (10,589 t, 0.7 s, 991.7 s), 13 (4,771 t, 0.8 s, 497.5 s), 15 (12,737 t, 0.8 s, 496.7 s); write: 39 (37 t, 4.0 s, 161.3 s) | 19, 39 (30-37 tasks, 0.4-0.5 GB each; two hours per run) | **apply now (canary, this PR)** |
| ipdsc_ds_47 | tpa_ipdsc_export (TPA_EXPORT) | parquet overwrite `.save()`, v2 | 2 chronic(6), 5 chronic(4) | compute: 2 (2,985 t, 104.4 s, 3,080.5 s), 5 (1,952 t, 31.2 s, 184.8 s) | 7 (5,000 t, 25.4 GB) | safe; wait for canary + Ryan |
| ipdsc_ds_42 | tpa_ipdsc_export | parquet overwrite, v2 | 3 resolved | compute: 3 (12,599 t, 10.4 s, 89.1 s) | 8 (13 t, 2.3 GB) | safe; no live key, no-op unless re-flagged |
| ipdsc_ds_63 | tpa_ipdsc_export | parquet overwrite, v2 (from source; no log parsed) | none | none | (not parsed) | safe by class; no-op unless re-flagged |
| hhdsc_ds_19 | hhdsc_build (TGT) | parquet overwrite, `repartition(35, household_id)`, v2 | none | none (log `app-20260901033636287-0177`: write 40, 35 t, 2.5 GB) | 40 | safe by class; no-op unless re-flagged |
| aug_log_ip_vertical_id_hourly | feature_store_hourly | parquet overwrite, `repartition(8, "ip")`, v2 | 7 new, 31 new (9, 11 resolved) | compute: 7 (8,804 t, 3.1 s, 696.3 s), 31 (9,604 t, 2.5 s, 67.6 s) | 23, 47 (8 t, 0.07-0.1 GB) | safe; wait for canary + Ryan |
| site_visit_signal_advertiser_id_dsc_id | feature_store_setup_model (TGT) | parquet overwrite `dt=<run_date>`, v2 | 4, 6 resolved | compute: 4 (5,526 t, 14.6 s, 83.1 s), 6 (28 t, 13.8 s, 69.5 s) | 16 (8 t, 0.5 GB) | safe; no live key, no-op unless re-flagged |
| fangorn_score_monitor | audience_intent | parquet overwrite, `coalesce(1)`, v2 | 12 recurring(2), 15 new | compute: 12 (30,000 t, 5.8 s, 61.5 s), 15 (2,048 t, 71.2 s, 431.3 s) | 48 | safe; wait for canary + Ryan |
| tpa_mntn_id_export | tpa_mntn_id_export (TPA_EXPORT) | JSON to a new random-suffix prefix, no overwrite, v2; driver-side prefix upload | 7 new, 8 new | compute: 7 (59,497 t, 7.7 s, 83.2 s), 8 (46,314 t, 8.4 s, 82.6 s) | 11 (1,000 t, 49.6 GB) | safe; wait for canary + Ryan |
| advertiser_high | audience_intent | parquet overwrite `/<partition>`, v2 (from source; no log parsed) | none | none | (not parsed) | safe by class; no-op unless re-flagged |
| identity_targeted_signal | targeted_signal_crm (TGT) | Iceberg `overwritePartitions()`, 1.10.2 | 3 chronic(7) | **write**: 3 (634 t, 31.3 s, 236.3 s, 104.2 GB out) | 3 | safe by Iceberg semantics; the straggler is the write stage itself, so the canary result matters most here; wait |
| advertiser_join | audience_intent | parquet overwrite, `coalesce(14000)`, manifest committer, pin `false` | 3 chronic(7), 83.5 exec-h | compute: 3 (4,798 t, 19.5 s, 214.1 s; 418.9 GB in, 2,591.8 GB shuffle write) | 5 (14,000 t, 1,467 GB) | owner-gated |
| prospecting_join | audience_intent | parquet overwrite, manifest committer, pin `false` | 10 new (24, 51 resolved) | compute: 10 (10,385 t, 3.8 s, 65.4 s) | 62 (20,000 t, 3,895 GB) | owner-gated |

## 6. The remedy, exactly

Add one line to the model decorator's `runtime_properties`: `"spark.speculation": "true"`. Nothing else. Spark 3.5.3 defaults stay in force: `spark.speculation.quantile` 0.9 (a stage must have 90 % of its tasks finished before copies launch), `spark.speculation.multiplier` 3 (a task must run 3x the median), `spark.speculation.minTaskRuntime` 100 ms, `spark.speculation.efficiency.enabled` true (a slow task that is also processing more data than the median is not duplicated), `spark.speculation.interval` 100 ms. The regenerated `dags/model_task_config.json` carries the same key under the model's `runtime_config.properties`, which is what `ModelPysparkBatchOperator` submits.

For the two manifest-committer DAGs, alternatives that need no speculation: shrink the tasks in the flagged shuffle stage (advertiser_join stage 3 writes 2.6 TB of shuffle from 4,798 tasks; a slow task there is the tail of a 214 s max against a 19.5 s median), or move the pair back to the default v2 committer the other 10 models use, which removes the rename phase that failed in Nov 2025. Both are Ryan's call.

## 7. What the canary tells us, and what it cannot

`site_network_hourly` runs 24 times a day, writes two small partitions per run (30-37 files, 0.4-0.6 GB per hour), every hour is written twice by consecutive runs, and a bad hour is repaired by the next run. Baseline from GCS for 2026-08-27 to 2026-09-02 (`outputs/audi_1275_site_network_output_baseline.txt`): 167 hour partitions, 166 with `_SUCCESS`, 4-75 files per hour, 0.204-0.621 GB per hour, 7.8-11.7 GB per day; `dt=2026-08-31/hh=18` absent.

Two pre-existing behaviours the post-merge check must not blame on speculation:

- **Fetch-failure storms already dominate the long runs.** All four site_network_hourly logs show thousands of `FetchFailed` task ends (2,115 to 8,244 per run) from executors decommissioned during dynamic-allocation scale-down, with stages re-submitted up to 116 times. `app-20260901205120829-0863` (batch `sit-net-hou-y3a-20260901-195000-1`, 2 h 25 min wall clock): both hours' Spark jobs failed ("ShuffleMapStage 9 / 25 has failed the maximum allowable number of times: 4"), the model's `except Exception` swallowed both, the batch reported SUCCEEDED and wrote 0 bytes. Speculation addresses a slow task on a live executor; it does not repair a lost shuffle. The straggler keys on that run (15, 18, 21-24) are fetch-wait tails, not slow-executor tails.
- **A partition without `_SUCCESS` can already appear without speculation.** `dt=2026-09-02/hh=05` (written by batch `sit-net-hou-hy9-20260902-075000-1`, log `app-20260902085134727-0157.zstd.inprogress`): the write job failed at `ResultStage 19` after four attempts, the attempts that had already been authorized had merged their files into the destination (v2 commits at task commit), no `_SUCCESS`, exception swallowed, batch SUCCEEDED. Files carry two timestamps (09:45 and 10:02 UTC) from the two stage attempts.

So the canary measures, in this order: (1) safety, i.e. speculative attempts appear in the event log (`Task Info.Speculative=true`) and end as `TaskKilled` or `Success`, never as a task failure, and `_SUCCESS` plus the file count and bytes per hour stay inside the 7-day band; (2) effect, i.e. the `straggler:*` keys for `site_network_hourly` go `resolved` over three sweeps and the batch wall clock distribution tightens. A clean (1) with a flat (2) still clears the other 10 DAGs, because their stragglers are slow-executor tails on stages with no fetch failures.

## 8. Contradiction record (kept, not overwritten)

- **2026-08-27 refutation (gauntlet, AUDI-1194 queue item 4, `ipdsc_ds_35`):** "airflow-ti pins `spark.speculation=false` on every GCS-writing model (ManifestCommitter race)"; reconciling hypothesis at the time: "the ManifestCommitter's GCS commit protocol does not tolerate two attempts of the same write task". Evidence: the repo's pins and comments, read 2026-08-27.
- **2026-09-02 evidence (this memo):** the pins exist on 2 of the 13 models only; the other 11 run FileOutputCommitter v2 or Iceberg with the commit coordinator on; the coordinator source denies every attempt after the first; the audience_intent scoring batches have run speculation on v2 in prod since 2025-08-15 with hundreds of duplicates killed per run and no failures; the Nov 2025 incident continued after speculation was off and was fixed in the manifest committer's rename phase.
- **What still separates them:** Ryan's account of what he saw in Nov 2025 (ask 1 in the Slack draft), and the canary's first post-merge event logs. Until both are in, the manifest pair keeps its pin and the other 10 wait.

## 9. Post-merge checklist (human, prod is not touched by this ticket's agents)

1. Bundle lag: the decorator change reaches prod on the deploy after merge, observed up to 12 h. On the first run after that, the Airflow task log line `Compute batch:` must show `'spark.speculation': 'true'`.
2. Batch SUCCEEDED; `gcloud storage ls -l gs://mntn-data-archive-prod/ipdsc_site_network/site_network_hourly/dt=<D>/hh=<HH>/` for the two hours shows `_SUCCESS`, 4-75 files, 0.2-0.7 GB.
3. Event log (`gs://mntn-data-archive-prod/spark-events/app-<id>.zstd`, parse with `artifacts/audi_1275_eventlog_props.py`): `speculative_tasks > 0`, task end reasons only `Success`, `TaskKilled`, `FetchFailed`; no `ExceptionFailure` naming `CommitDenied` or `FileAlreadyExists`.
4. Stamp provenance on the ledger keys present at merge (`straggler:9`, `:11`, `:13`, `:15`, `:18`, `:21`, `:22`, `:23`, `:24`, `:29`, `:39`) with `python3 -m include.spark_optimizer.ledger applied site_network_hourly straggler:<n> <PR#> <merge-date>` on a downloaded copy, then watch three sweeps for `resolved` versus `fix_not_working`.
5. Three clean sweeps plus Ryan's answer to asks 1 and 2 clear the next PR for the 10 default-committer and Iceberg models; ask 3 governs advertiser_join and prospecting_join.
