---
doc_type: ticket
title: "AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs"
status: backlog
date: 2026-09-02
summary: "Speculation is unsafe on GCS writers; settle a safe straggler remedy then apply to 13 DAGs"
result: "planned 2026-09-02, awaiting scope decision"
question: "Which straggler remedy is safe for Spark jobs that overwrite GCS output, and which of the 13 DAGs can take it now?"
framing_state: locked
---

# AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1275
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Which straggler remedy is safe for Spark jobs that overwrite GCS output, and which of the 13 DAGs can take it now?
- **Goal (why / the decision):** Speculation was proposed and refuted twice as unsafe for GCS writers; the answer unblocks 13 straggler findings. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A decision memo in outputs/ (committer guarantees per writer path, safe remedy, per-DAG verdict), a Slack ask drafted for Ryan Kleck in artifacts/ for the user to send, and one PR (branch AUDI-1275) applying the remedy only to DAGs whose safety is proven from source; the rest wait on the owner's answer.
- **Approach (how):** For each DAG read the writer path (format, mode, committer, GCS connector version) on airflow-ti main; check Spark and GCS-connector committer semantics (FileOutputCommitter v1/v2, Dataproc GCS committer, task-attempt isolation); from the event log confirm whether the straggler is in a write stage; user's decision 2026-09-02: draft the ask, execute the safe subset.
- **What would change the answer:** A DAG whose writer is not idempotent under duplicate task attempts is never changed without the owner's word; if no remedy is provably safe the deliverable is the memo and the ask alone.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Thirteen jobs regularly sit waiting on one slow task (a straggler). Spark's built-in remedy is unsafe for jobs that write to GCS, so first pick a safe remedy, then apply it to all 13.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the built-in fix (speculative execution) runs a second copy of the slow task, and it is app-wide; two copies writing the same GCS output can corrupt it. It was proposed and refuted twice for that reason.

**Task:** settle the safe pattern with the owning team (per-stage alternatives, committer guarantees), then apply to:
- [advertiser_join](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/advertiser_join.py), [advertiser_high](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/advertiser_high.py), [prospecting_join](https://github.com/SteelHouse/airflow-ti/blob/main/models/audience_intent/prospecting_join.py)
- [identity_targeted_signal](https://github.com/SteelHouse/airflow-ti/blob/main/models/signals/identity_targeted_signal.py)
- [fangorn_score_monitor](https://github.com/SteelHouse/airflow-ti/blob/main/models/monitoring/fangorn_score_monitor.py)
- [ipdsc_ds_42](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_42.py), [ipdsc_ds_47](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_47.py), [ipdsc_ds_63](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/ipdsc_ds_63.py), [hhdsc_ds_19](https://github.com/SteelHouse/airflow-ti/blob/main/models/ipdsc/hhdsc_ds_19.py)
- [aug_log_ip_vertical_id_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py)
- [site_network_hourly](https://github.com/SteelHouse/airflow-ti/blob/main/models/bidstream_hourly/site_network_hourly.py)
- [site_visit_signal_advertiser_id_dsc_id](https://github.com/SteelHouse/airflow-ti/blob/main/models/feature_store/feature_group_1_source/site_visit_signal_advertiser_id_dsc_id.py)
- [tpa_mntn_id_export](https://github.com/SteelHouse/airflow-ti/blob/main/models/tpa_export/tpa_mntn_id_export.py)

**Done-when:** decision recorded and fix PRs merged.

## 3. Plan of Action
Planning wave written 2026-09-02 (read-only reconnaissance; nothing executed). Working rule from §0: speculation is application-wide, so a DAG is changeable only when EVERY writer in that Spark application provably tolerates a duplicate task attempt. The plan is grouped by writer class, not by straggler stage, because the straggler stage being compute-only was already shown insufficient (AUDI-1194 row 6, `aug_log_ip_hourly`: stage 2 compute, stages 7/15 write, refuted).

### 3.1 Verified facts the plan rests on (all read on 2026-09-02, sources in 3.7)
- **Fleet runtime:** Dataproc Serverless runtime 2.3.39 = Spark 3.5.3, Java 17, Scala 2.13; classpath carries `gcs-connector-3.1.16.jar`, `hadoop-cloud-storage-3.3.5.jar`, `spark-hadoop-cloud_2.13-3.5.3.jar` (event log `app-20260825041857964-0297`). Google's runtime page lists connector 3.1.2 for 2.3.39; the event log classpath says 3.1.16. The log wins; record both.
- **Dataproc injects `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` on every batch** (present in logs for models that never set it: identity_targeted_signal, ipdsc_ds_47). No manifest-committer factory is injected by default (`mapreduce.outputcommitter.factory.scheme.gs` absent in those logs).
- **Writer class of each of the 13 (from `models/` on airflow-ti main 825b07e3, plus `dags/model_task_config.json`):**
  | DAG (model) | Launcher DAG (team) | Writer | Committer in effect | Speculation today |
  |---|---|---|---|---|
  | advertiser_join | audience_intent (TPA_EXPORT) | parquet `mode("overwrite").save(/<partition>)`, `coalesce(14000)` | ManifestCommitter + `PathOutputCommitProtocol` + `BindingParquetOutputCommitter`, `validate.output=false` (model L35-40) | pinned `false` with comment "race conditions with ManifestCommitter" |
  | prospecting_join | audience_intent | parquet overwrite, `coalesce(PROSPECTING_INTENT_WRITE_COALESCE)` | ManifestCommitter (model L113-123) | pinned `false` |
  | identity_targeted_signal | targeted_signal_crm (TGT) | Iceberg `overwritePartitions()` (`IcebergBigqueryDwMainBronzeModel`, iceberg 1.10.2 jars) | Iceberg snapshot commit, no Hadoop committer | unset (Spark default false) |
  | advertiser_high | audience_intent | parquet overwrite to `/<partition>` | default FileOutputCommitter v2 | unset |
  | fangorn_score_monitor | audience_intent | parquet overwrite, `coalesce(1)` | default v2 | unset |
  | ipdsc_ds_42 / ipdsc_ds_47 / ipdsc_ds_63 | tpa_ipdsc_export (TPA_EXPORT), via `ModelPysparkBatchOperator` (NOT the legacy `export_tpa.py` config that pins speculation off) | parquet overwrite `.save()` | default v2 (verified in log `app-20260825040446892-0297` for ds_47) | unset |
  | hhdsc_ds_19 | hhdsc_build (TGT) | parquet overwrite, `repartition(35, household_id)` | default v2 | unset |
  | aug_log_ip_vertical_id_hourly | feature_store_hourly | parquet overwrite, `repartition(8, "ip")` | default v2 | unset |
  | site_network_hourly | site_network_hourly (TPA_EXPORT, ours since 2026-08-27) | parquet overwrite, `coalesce(target_partitions)` | default v2 | unset |
  | site_visit_signal_advertiser_id_dsc_id | feature_store_setup_model (TGT) | parquet overwrite to `dt=<run_date>` | default v2 | unset |
  | tpa_mntn_id_export | tpa_mntn_id_export (TPA_EXPORT) | JSON to a NEW random-suffix prefix (`.write.option("multiline","true").json(path)`, no overwrite) + driver-side `blob.upload_from_string` | default v2 | unset |
  "default v2" = no committer keys in the model's `runtime_properties` and none in its `model_task_config.json` entry; the Dataproc-injected v2 applies. Must be confirmed per DAG from its own event log in step 3.2.1 (only ds_47 and identity_targeted_signal are log-verified so far).
- **Every model sets Spark props ONLY through the decorator `runtime_properties`** (no `.config("spark.speculation", ...)` in any builder; grep of the 13 files). `spark.speculation` is a scheduler property, so the decorator/batch-properties path is the only one that works.
- **Prod precedent, verified live:** the `audience_intent` DAG's script batches (`vertical_high`, `vertical_mid`, `prospecting_high`, `prospecting_mid`, `prospecting_keywords`; `dags/audience_intent/audience_intent.py:109`) run with `spark.speculation=true` AND FileOutputCommitter v2 AND `df.write.mode("overwrite").parquet(...)` to GCS, since TI-172 (commit 40ecc73, 2025-08-15). Confirmed on `gcloud dataproc batches describe aud-int-pro-hig-20260901-20260902-050355-1` and `aud-int-ver-mid-20260901-20260902-044633-1` (`spark:spark.speculation=true`, `...algorithm.version=2`, no manifest factory), both SUCCEEDED 2026-09-02. Same committer class as 10 of the 13 targets.
- **Origin of the pin (the owner to ask):** `009e99a` 2025-11-12 rkleck-mntn "Disable Spark speculation for all ipdsc and mntn_select jobs" (one line, `include/spark/data_source/ipdsc_emr_cluster.py`, the legacy `export_tpa.py` launcher, which already used the ManifestCommitter); `6afc07f` 2025-12-09 rkleck-mntn "Add ManifestCommitter to prospecting_join and advertiser_join, remove io.threads=1 from ipdsc". Neither commit body records the incident. `validate.output=false` was added with a "FileNotFound" comment. The ask must recover what actually broke.
- **Commit-protocol facts from source:** Spark 3.5.3 `SparkHadoopMapRedUtil.commitTask` consults the driver's `OutputCommitCoordinator` for every Hadoop `OutputCommitter` when `spark.hadoop.outputCommitCoordination.enabled` (default true); the coordinator is "first committer wins", a second attempt gets `CommitDeniedException` and `abortTask`. Hadoop 3.3.5 `FileOutputCommitter`: each attempt writes under its own `_temporary/<jobAttempt>/_temporary/<taskAttemptID>` dir; v2 `commitTask` merges that dir into the destination at TASK commit; `abortTask` deletes the attempt dir. Hadoop manifest committer doc: per-attempt dirs plus a manifest, job commit renames listed files; it does NOT state a speculation guarantee and says concurrent jobs to one directory are "NOT TESTED". Iceberg 1.10.2 `SparkWrite`: task `abort()` deletes that attempt's data files; job commit appends only the `DataFile`s carried in the commit messages. Spark docs list no per-stage speculation switch; every `spark.speculation.*` knob is application-wide (`quantile` 0.9, `multiplier` 3, `minTaskRuntime` 100ms, `efficiency.enabled` true are the 3.5.3 defaults).
- **Straggler evidence exists at the grain needed.** Flat archive `gs://mntn-data-archive-prod/spark-events/` holds 4,011 `app-*.zstd` logs (2026-08-04 to today, ~135/day) plus 31 `eventlog_v2_batch-*` dirs; every ledger app id checked (ds_47, advertiser_join, identity_targeted_signal, site_network_hourly) is still there. PHS dirs `gs://dataproc-temp-us-central1-995798185124-svhwvc6j/<uuid>/spark-job-history/` list under the current PAM grant (checked uuid d9ccae11); they hold the script-batch logs, not the 13 models. `include.spark_optimizer.eventlog.parse_eventlog` returns per-stage `input_bytes / output_bytes / shuffle_write_bytes / skew_ratio` and `run.spark_props`; `output_bytes > 0` marks a write stage. Sample: ds_47 stage 2 = `first at ...` scan (2,962 tasks, 8.6x, no output), stage 7 = write (24.9 GB out); advertiser_join stage 3 = shuffle/compute (439 GB in, 2.55 TB shuffle write, 8.1x), stage 5 = write (1.42 TB out); identity_targeted_signal stage 3 = the Iceberg WRITE stage itself (100.6 GB out, 6.4x), so its straggler cannot be isolated from the commit path.
- **Ledger keys per DAG (local copy `tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimization_ledger.jsonl`, stale at 2026-08-26; live copy `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` current to 2026-09-02):** advertiser_join `straggler:3` (chronic, 4 sweeps); prospecting_join `straggler:51` (resolved 08-26, stopped firing); identity_targeted_signal `straggler:1`, `straggler:3`; fangorn_score_monitor `straggler:12`; ipdsc_ds_42 `straggler:3`; ipdsc_ds_47 `straggler:2`, `straggler:5` (chronic); ipdsc_ds_63 `straggler:2`; hhdsc_ds_19 `straggler:23`; aug_log_ip_vertical_id_hourly `straggler:7/9/11`; site_network_hourly `straggler:5/9`; site_visit_signal_advertiser_id_dsc_id `straggler:4/6`; tpa_mntn_id_export `straggler:778`; advertiser_high `straggler:3`. Stage ids drift run to run, so provenance stamps use the keys present at merge time.
- **Ownership:** all 13 launchers route to `Team.TARGETING` (`JobTeamConfig.TPA_EXPORT` -> #alerts-tpa-pipeline, `JobTeamConfig.TGT` -> TPA_MONITOR). Ryan Kleck (rkleck-mntn) authored both speculation pins and owns the feature-store pipelines; Victor owns the model framework. `site_network_hourly` is ours. No CODEOWNERS file in the repo.
- **Validation constraints:** a `runtime_properties` change requires regenerating `dags/model_task_config.json` (`MNTN_SDLC_ENV=dev python model_upload.py --dryrun`, uv group `models`) or the `model-upload-dryrun` check fails; decorator changes reach prod only on the bundle redeploy after merge. The user cannot submit a dev batch (`model_run.py` needs `iam.serviceAccounts.actAs` on the dev SA); dev buckets are not a mirror of prod (29/38 models read from dev). CI `model-unit-test` is broken repo-wide since #1209 (not a required check). No manual prod trigger for first-run validation (feedback_airflow_prod_safety): the first prod execution is the next scheduled run.
- **No BigQuery query is needed for this ticket.** Savings measure on the optimizer ledger (straggler keys going quiet) and the Mode cost dashboard (e81786de8403).

### 3.2 Steps
**Phase A: evidence per DAG (read-only, airflow-ti-main + GCS)**
1. Download the live ledger to `outputs/optimization_ledger_live.jsonl` (`gsutil -o "GSUtil:check_hashes=never" cp gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl outputs/optimization_ledger_live.jsonl`). For each of the 13 `dag_id`s list every `straggler:*` row (state, streak, app_id, exec_h). Note `prospecting_join` is `resolved` and `intent_score_household_map` was dropped (model deleted 2026-08-26, PR 1209).
2. For each DAG fetch its two most recent straggler-flagged logs (`app_id` column) into `outputs/` with the sequential `gsutil -o "GSUtil:check_hashes=never" -o "GSUtil:sliced_object_download_threshold=0" cp` (size-check with `gsutil du` first; skip anything over 200 MB or download and delete after parsing; never `gsutil -m`). If a flat log is missing, the model's log is not in PHS (models log to `spark-events`); record the gap and use the next-newest ledger app id.
3. Parse each log from `/Users/malachi/Developer/work/mntn/airflow-ti-main` with `python3 -c 'import sys; sys.path.insert(0,"."); from include.spark_optimizer.eventlog import parse_eventlog; ...'` and record into `outputs/audi_1275_stage_evidence.csv`: dag, app_id, spark version (`SparkListenerLogStart`), effective `spark.speculation`, `spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs`, `spark.sql.sources.commitProtocolClass`, `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version` (from `run.spark_props`), gcs-connector jar (Classpath Entries, read the raw `SparkListenerEnvironmentUpdate` event with `zstd -dc | python3`), and for every stage with >1 task: stage id, name, tasks, input/output/shuffle-write bytes, skew ratio. Mark the straggler stage as compute or write (`output_bytes > 0`) and list every write stage in the app. Save the helper as `artifacts/audi_1275_eventlog_props.py`.
4. From `models/` on airflow-ti main, for each of the 13 confirm from source: writer call (format, mode, path pattern, `partitionBy`), and grep for task-side side effects that speculation would duplicate (`foreachPartition`, `mapPartitions` with network/DB calls, `jdbc`, `.format("bigquery").save`, `requests.`, `gcs_client` inside a UDF). Driver-side calls (tpa_mntn_id_export `blob.upload_from_string`) are not duplicated by speculation. Record in the memo's per-DAG table.
5. Pin the committer semantics from source at the exact versions and quote them in the memo: Spark v3.5.3 `core/src/main/scala/org/apache/spark/mapred/SparkHadoopMapRedUtil.scala` (`commitTask`, coordination flag) and `scheduler/OutputCommitCoordinator.scala` (first committer wins); Hadoop rel/release-3.3.5 `FileOutputCommitter.java` (`commitTask` v2 `mergePaths`, `abortTask`, `getTaskAttemptPath`); Hadoop `manifest_committer.md` (task attempt dirs, job-commit rename, the "concurrent jobs NOT TESTED" line) plus `ManifestCommitter.java` `abortTask`/`commitTask` to state whether a denied duplicate attempt can leave files in the destination; Iceberg apache-iceberg-1.10.2 `spark/v3.5/.../SparkWrite.java` (`abort` deletes task files, `useCommitCoordinator` value, commit assembled from `WriterCommitMessage`s). Also check the GCS connector 3.1.16 rename semantics for single objects (copy-then-delete, per object) from `hadoop-connectors` docs.
6. Capture the prod precedent formally: `gcloud dataproc batches describe <id> --format=json` for one SUCCEEDED batch of each of `aud-int-ver-hig`, `aud-int-ver-mid`, `aud-int-pro-hig`, `aud-int-pro-mid`, `aud-int-pro-key` (keys are prefixed `spark:` in the JSON), and `git log -S'"spark.speculation": "true"' -- dags/audience_intent/audience_intent.py` for the start date. Count SUCCEEDED runs of those batches since 2025-08-15 from `gcloud dataproc batches list --filter="labels.airflow-dag-id=audience-intent"` as the exposure number. Read `git show 009e99a` and `6afc07f` and search Confluence "TI On Call Playbook" (page 2908061697) and `on-call/oncall_runbook.md` for the Nov-2025 incident behind the pin.

**Phase B: decision memo `outputs/audi_1275_decision_memo.md` (the ticket's primary deliverable)**
7. Structure: answer line; the rule (speculation is app-wide; safe iff every writer tolerates a denied duplicate attempt); per writer class verdict with the quoted source: (i) FileOutputCommitter v2 default, 10 DAGs; (ii) Iceberg, identity_targeted_signal; (iii) ManifestCommitter with the owner's explicit pin, advertiser_join + prospecting_join; then the per-DAG verdict table (DAG, launcher, writer class, straggler stage compute/write, all write stages, task-side side effects, verdict: apply now / owner-gated / no-op, evidence app ids). Draft the (i) and (ii) verdicts from steps 3 to 6; the plan's expectation is that (i) and (ii) are provable from source plus the audience_intent precedent and (iii) is owner-gated by the author's pin, but the verdicts are written from the evidence, not assumed. If step 5 shows the manifest committer also isolates attempts (task-attempt dirs plus manifest written only at task commit), say so and keep (iii) owner-gated anyway: the pin is the author's explicit instruction and `validate.output=false` records an unexplained FileNotFound.
8. Remedy spec for approved DAGs: add exactly `"spark.speculation": "true"` to the decorator `runtime_properties`; keep Spark 3.5.3 defaults (`quantile` 0.9, `multiplier` 3, `efficiency.enabled` true, `minTaskRuntime` 100ms) and state them in the memo. No other knob unless the memo justifies it. For the two owner-gated DAGs list the alternatives that do not need speculation: smaller tasks in the straggler stage (advertiser_join stage 3 is the 2.55 TB shuffle stage before the write), and note that Spark 3.5.3 has no per-stage speculation switch.
9. Contradiction record inside the memo: keep the 2026-08-27 gauntlet refutation (ipdsc_ds_35, a default-v2 model refuted by analogy with the manifest pin) next to the new evidence (effective committer per event log, coordinator semantics, prod precedent); name the discriminating check (Ryan's account of the Nov-2025 incident plus the first post-merge run's event log showing `Task Info.Speculative=true` attempts with a clean `_SUCCESS`).

**Phase C: Slack ask for Ryan Kleck, `artifacts/audi_1275_slack_ask_ryan.md` (user sends; human prose, asks first)**
10. Three numbered asks, one paragraph each: (1) what broke in Nov 2025 when speculation ran with the manifest committer on the ipdsc export jobs (009e99a) and whether `validate.output=false` was the same incident; (2) whether he accepts `spark.speculation=true` on the 10 default-committer models, given his own audience_intent scoring batches have run it on that committer since 2025-08-15; (3) whether advertiser_join / prospecting_join can take it under the manifest committer, or which alternative he prefers. Then the 2-3 facts that justify it (committer per job from the event logs, the coordinator's first-committer-wins rule, the exposure count from step 6). Lint with `python3 .claude/scripts/lint_comms.py --kind comment --file artifacts/audi_1275_slack_ask_ryan.md`.

**Phase D: PR on branch AUDI-1275 (execute agent edits the worktree only; dispatcher commits, runs the gauntlet, opens the PR)**
11. Edit exactly the `runtime_properties` dict of each approved model file, one added line `"spark.speculation": "true",` (no comment): `models/audience_intent/advertiser_high.py` (L16), `models/monitoring/fangorn_score_monitor.py` (L79), `models/ipdsc/ipdsc_ds_42.py` (L10), `models/ipdsc/ipdsc_ds_47.py` (L9), `models/ipdsc/ipdsc_ds_63.py` (L9), `models/ipdsc/hhdsc_ds_19.py` (L53), `models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py` (L70), `models/bidstream_hourly/site_network_hourly.py` (L30), `models/feature_store/feature_group_1_source/site_visit_signal_advertiser_id_dsc_id.py` (L52), `models/tpa_export/tpa_mntn_id_export.py` (L103), `models/signals/identity_targeted_signal.py` (L17). Do NOT touch `advertiser_join.py`, `prospecting_join.py`, `include/spark/data_source/ipdsc_emr_cluster.py`, or any DAG file. Drop any DAG whose memo verdict is not "apply now".
12. Regenerate the config: `MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun` (confirm the exact invocation against `.github/workflows/pr_model.yaml` first); `git diff --stat dags/model_task_config.json` must show only the approved models' `spark.speculation` entries. Both the model files and the regenerated JSON go in the PR.
13. Pre-PR validation (all local, no prod): dryrun compile clean; `ruff check` on the touched files with the repo's pinned ruff (memory: lint like CI); `python3 -c` DagBag parse is unnecessary (no DAG file changes); tests: run `pytest tests/models -q` and record that `model-unit-test` is already broken repo-wide (#1209) so a failure there is pre-existing; JSON sanity: `python3 -c 'import json; c=json.load(open("dags/model_task_config.json")); ...'` printing `spark.speculation` for the 13 models. Dev batch validation is NOT available to us (no actAs on the dev SA, dev inputs not mirrored); state that in the PR body and rely on the prod precedent plus the post-merge checklist.
14. PR body (lint `--kind pr`): answer line; What (one property on N models, regenerated config); Why (13 chronic stragglers, app-wide speculation, committer proof per class, audience_intent precedent); Validation (dryrun, ruff, JSON diff, post-merge checklist). Reviewer: rkleck-mntn. Link AUDI-1275 and the memo. No Release Type field.
15. Post-merge checklist (user/dispatcher, prod is not touched by the agent): on each model's next scheduled run confirm the Airflow log line `Compute batch:` shows `'spark.speculation': 'true'` (bundle redeploy can lag up to 12 h); run SUCCEEDED, `_SUCCESS` present, output object count and bytes within the prior 7-day band; event log shows `Task Info.Speculative=true` attempts and no `CommitDenied`-caused task failure beyond the denied duplicates; then stamp provenance on the prod ledger for each straggler key present at merge time (`OPTIMIZER_LEDGER=<downloaded copy> python3 -m include.spark_optimizer.ledger applied <dag> straggler:<stage> <PR#> <merge-date>` on a downloaded copy, re-uploaded by the user) and watch three sweeps for `resolved` vs `fix_not_working`.

**Phase E: write-backs (execute agent fills the ticket; the dispatcher lands knowledge)**
16. `summary.md` §4 (evidence tables, app ids, refutations), §5 (memo, ask, PR), §6 (Q/A), §8 (owner-gated pair, post-merge watch). Hand these facts back for `knowledge/`: Dataproc injects committer v2 by default; `audience_intent` script batches run speculation on v2 in prod since 2025-08-15; `batches describe` JSON prefixes property keys with `spark:`; runtime 2.3.39 ships gcs-connector 3.1.16 despite the docs page; PHS dirs readable under the PAM grant. Self-review entry after the PR merges.

### 3.3 Assumptions to resolve empirically before editing anything
- A1. Each of the 10 "default v2" models really runs FileOutputCommitter v2 with no manifest factory (step 3 confirms from its own event log; verified so far only for ipdsc_ds_47 and identity_targeted_signal).
- A2. None of the 13 has a task-side side effect that speculation would duplicate (step 4 grep).
- A3. Iceberg 1.10.2 `SparkWrite` uses the commit coordinator or otherwise discards the duplicate attempt's files (step 5); if it neither aborts nor coordinates, identity_targeted_signal is owner-gated.
- A4. The manifest committer at Hadoop 3.3.5 writes the manifest only at task commit and job commit renames only manifest-listed files (step 5); this decides whether the Nov-2025 race is explainable or needs Ryan's incident detail.
- A5. The live ledger still lists a straggler key for every DAG in scope (prospecting_join resolved on 2026-08-26; if a DAG has no live key it gets the change only if the memo proves safety AND the sweep still flags it, otherwise it is a no-op in the memo).
- A6. `model_upload.py --dryrun` under `MNTN_SDLC_ENV=dev` regenerates the JSON deterministically so the diff is limited to the 11 entries.

### 3.4 Risks
- R1. Speculation duplicates compute: on a slot-saturated app it can add executor-hours before it saves them; the ledger's `idle_reserved_executors` and `straggler` keys measure the net effect over three sweeps, and the PR can be reverted per model.
- R2. A duplicate attempt under FileOutputCommitter v2 that is killed mid-`commitTask` leaves partial files; the coordinator denies commit before the loser starts committing, so this needs a failure of the coordinator path itself. Kept as the residual risk in the memo, with the audience_intent exposure count as the evidence bound.
- R3. Ryan's pin may encode an incident the source cannot explain (a GCS listing or rename anomaly under 3.1.x); until his answer the manifest pair stays untouched, and if his answer implicates the default committer too, the PR scope shrinks to what he accepts.
- R4. Bundle lag: decorator changes take effect only on the redeploy after merge (observed up to 12 h); the post-merge check reads the `Compute batch:` line, not the merge.
- R5. `model-unit-test` CI is red repo-wide; a reviewer may block on it. Cite #1209 and the diagnosis on #1231.

### 3.5 Decisions for the user (not taken here)
- D1. Scope of the PR: include the default-committer models (up to 10) and identity_targeted_signal now, with Ryan as required reviewer, or hold every DAG until his answer. The 2026-08-27 gauntlet reverted the same class (ipdsc_ds_35); the new evidence (effective committer from the logs, coordinator rule, prod precedent since 2025-08-15) is what changes it.
- D2. Rollout shape: all approved models in one PR versus a canary PR on one chronic, re-run-tolerant DAG (ipdsc_ds_47 or site_network_hourly, which we own) followed by the rest after three clean sweeps.

### 3.6 Effort
2 SP as filed. Phase A-C about half a day of agent time (13 logs, 6 source reads, memo, ask); Phase D under two hours once D1 is answered; Phase E after the first three post-merge sweeps.

### 3.7 Sources
- Ticket spec: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` (#20), `audi_1194_hackathon_optimizations_2026_08_27.md` (rows 37-67, row 6 refutation), `artifacts/audi_1194_implementation_queue.md` (#4), `outputs/optimization_ledger.jsonl`; live ledger `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`.
- Memory: `knowledge/memory/project_airflow_optimizer.md` (2026-08-27 revert), `reference_dataproc_eventlog_profiling.md` (contradiction record), `reference_airflow_ti.md` (pin, deploy lag, dev constraints, JSON regen), `reference_pyspark_optimization_skill.md`, `feedback_airflow_prod_safety.md`.
- airflow-ti main 825b07e3 (`/Users/malachi/Developer/work/mntn/airflow-ti-main`): the 13 model files; `dags/model_task_config.json`; `dags/audience_intent/audience_intent.py` (L60-130 `get_dataproc_config`, L144 `create_spark_batch`, L533-570 model operators); `dags/tpa_export/{tpa_ipdsc_export,hhdsc_build,targeted_signal_crm,tpa_mntn_id_export}.py`; `dags/models/{feature_store_hourly,feature_store_setup_model}.py`; `dags/models/bidstream_hourly/site_network_hourly.py`; `include/spark/data_source/ipdsc_emr_cluster.py`; `include/models/operators.py` (`ModelPysparkBatchOperator.execute`); `utils_model/base_model/{base_model,writer,writer_iceberg,compute_component}.py`; `include/spark_optimizer/{eventlog,optimizations,ledger}.py`; `include/job_config/job_team_config.py`; commits 009e99a, 6afc07f, 40ecc73.
- Event logs read: `app-20260825041857964-0297` (identity_targeted_signal), `app-20260825040446892-0297` (ipdsc_ds_47), `app-20260825051825582-0893` (advertiser_join); downloaded to `outputs/` and deleted after parsing (re-fetch with the step-2 command).
- Prod batches: `aud-int-pro-hig-20260901-20260902-050355-1`, `aud-int-ver-mid-20260901-20260902-044633-1` (`gcloud dataproc batches describe --format=json`).
- External: Spark 3.5.3 `SparkHadoopMapRedUtil.scala`, `OutputCommitCoordinator.scala` (raw.githubusercontent.com, tag v3.5.3); Hadoop rel/release-3.3.5 `FileOutputCommitter.java`; Hadoop manifest committer doc (hadoop.apache.org stable); Hadoop S3A committer architecture doc (speculation requirement); Spark `configuration.html` and `cloud-integration.html` (latest); Iceberg apache-iceberg-1.10.2 `SparkWrite.java`; Google Dataproc Serverless runtime 2.3 page (docs.cloud.google.com).

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
