---
doc_type: ticket
title: "AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs"
status: in_progress
date: 2026-09-02
summary: "Speculation is safe where every writer discards the losing duplicate attempt; canary PR on site_network_hourly, 10 wait on 3 clean sweeps + Ryan, 2 owner-gated"
result: "memo + Slack ask drafted 2026-09-02; canary PR #1271 open 2026-09-03 (reviewer Ryan Kleck); awaiting merge, 3 clean sweeps, Ryan answer"
question: "Which straggler remedy is safe for Spark jobs that overwrite GCS output, and which of the 13 DAGs can take it now?"
framing_state: locked
---

# AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs

**Jira:** https://mntn.atlassian.net/browse/AUDI-1275
**Status:** in_progress
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

## 3. Plan of Action (as executed)
Planning wave written 2026-09-02 (read-only reconnaissance). **Rewritten 2026-09-02/03 to match what was executed** after the user answered D1 and D2 together: CANARY only. The PR enables speculation on `site_network_hourly` alone (ours since #1232, chronic, re-run tolerant) with Ryan Kleck as required reviewer; the memo covers all 13 with per-DAG verdicts; the Slack ask is drafted for the user to send; the other 12 wait for three clean sweeps on the canary plus Ryan's answer. The original step 11 list of 11 model edits is therefore not what shipped; the deviation is recorded in §8.

Working rule from §0: speculation is application-wide, so a DAG is changeable only when EVERY writer in that Spark application provably tolerates a duplicate task attempt. Grouped by writer class, not by straggler stage (AUDI-1194 row 6, `aug_log_ip_hourly`, already showed "compute-only straggler stage" is insufficient).

### 3.1 Verified facts the plan rests on (planning wave 2026-09-02, re-verified during execution where marked)
- **Fleet runtime:** Dataproc Serverless runtime 2.3 = Spark 3.5.3 (all 18 logs read), Java 17, Scala 2.13; classpath carries `hadoop-cloud-storage-3.3.5.jar`, `spark-hadoop-cloud_2.13-3.5.3.jar`, `iceberg-spark-runtime-3.5_2.13-1.9.0-google-5.jar`; the GCS connector jar basename in these 18 logs is the unversioned `gcs-connector.jar` (the planning wave read `gcs-connector-3.1.16.jar` in `app-20260825041857964-0297`; both recorded, the version string was not re-verified in execution).
- **Dataproc injects `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` on every batch** (present in all 18 logs; no model sets it). No manifest-committer factory is injected by default.
- **Writer class of each of the 13** (from `models/` on airflow-ti main 825b07e3, plus `dags/model_task_config.json`; committer per DAG now log-verified for 11 of 13, see §4.1):
  | DAG (model) | Launcher DAG (team) | Writer | Committer in effect | Speculation today |
  |---|---|---|---|---|
  | advertiser_join | audience_intent (TPA_EXPORT) | parquet `mode("overwrite").save(/<partition>)`, `coalesce(14000)` | ManifestCommitter + `PathOutputCommitProtocol` + `BindingParquetOutputCommitter`, `validate.output=false` | pinned `false` |
  | prospecting_join | audience_intent | parquet overwrite, `coalesce(PROSPECTING_INTENT_WRITE_COALESCE)` | ManifestCommitter | pinned `false` |
  | identity_targeted_signal | targeted_signal_crm (TGT) | Iceberg `overwritePartitions()` (iceberg 1.10.2 jars) | Iceberg snapshot commit, no Hadoop committer | unset |
  | advertiser_high | audience_intent | parquet overwrite to `/<partition>` | default FileOutputCommitter v2 (source only) | unset |
  | fangorn_score_monitor | audience_intent | parquet overwrite, `coalesce(1)` | default v2 (log-verified) | unset |
  | ipdsc_ds_42 / ipdsc_ds_47 / ipdsc_ds_63 | tpa_ipdsc_export (TPA_EXPORT), via `ModelPysparkBatchOperator` (NOT the legacy `export_tpa.py` config that pins speculation off) | parquet overwrite `.save()` | default v2 (42, 47 log-verified; 63 source only) | unset |
  | hhdsc_ds_19 | hhdsc_build (TGT) | parquet overwrite, `repartition(35, household_id)` | default v2 (log-verified) | unset |
  | aug_log_ip_vertical_id_hourly | feature_store_hourly | parquet overwrite, `repartition(8, "ip")` | default v2 (log-verified) | unset |
  | site_network_hourly | site_network_hourly (TPA_EXPORT, ours since 2026-08-27) | parquet overwrite `/dt=/hh=`, `coalesce(target_partitions)`, two hours per run | default v2 (log-verified, 4 logs) | unset, **`true` in this PR** |
  | site_visit_signal_advertiser_id_dsc_id | feature_store_setup_model (TGT) | parquet overwrite to `dt=<run_date>` | default v2 (log-verified) | unset |
  | tpa_mntn_id_export | tpa_mntn_id_export (TPA_EXPORT) | JSON to a NEW random-suffix prefix, no overwrite, plus driver-side `blob.upload_from_string` | default v2 (log-verified) | unset |
- **Every model sets Spark props ONLY through the decorator `runtime_properties`**; `spark.speculation` is a scheduler property, so that path is the only one that works.
- **Prod precedent, verified live:** the `audience_intent` DAG's script batches (`vertical_high`, `vertical_mid`, `prospecting_high`, `prospecting_mid`, `prospecting_keywords`; `dags/audience_intent/audience_intent.py:109`) run `spark.speculation=true` AND FileOutputCommitter v2 AND `df.write.mode("overwrite").parquet(...)` to GCS since TI-172 (commit 40ecc73, 2025-08-15). Execution added the PHS event logs proving speculation fires there (§4.2).
- **Origin of the pin:** `009e99a` 2025-11-12 rkleck-mntn, one line in `include/spark/data_source/ipdsc_emr_cluster.py`; `6afc07f` 2025-12-09 copied it to the two join models. Execution recovered the surrounding commits (§4.3).
- **Commit-protocol facts from source (quoted in the memo §2):** Spark 3.5.3 `SparkHadoopMapRedUtil.commitTask` consults the driver's `OutputCommitCoordinator` for every Hadoop `OutputCommitter` (default on); first committer wins, the loser gets `CommitDeniedException` after `abortTask`. Hadoop 3.3.5 `FileOutputCommitter` v2 merges the attempt dir into the destination only at `commitTask`; `abortTask` deletes the attempt dir. Iceberg 1.10.2 `SparkWrite`: `useCommitCoordinator()` is `false`, task `abort()` deletes that attempt's files, job commit adds only the files in the accepted commit messages. No per-stage speculation switch in Spark 3.5.3.
- **Straggler evidence at the grain needed:** flat archive `gs://mntn-data-archive-prod/spark-events/` (models); PHS dirs `gs://dataproc-temp-us-central1-995798185124-svhwvc6j/<uuid>/spark-job-history/` (script batches) readable under the PAM grant. `include.spark_optimizer.eventlog` gives per-stage `output_bytes` (write stage marker), `skew_ratio`, and `run.spark_props`.
- **Ledger keys per DAG:** live ledger downloaded to `outputs/optimization_ledger_live.jsonl` (last sweep 2026-09-02); rows in §4.4.
- **Ownership:** all 13 launchers route to `Team.TARGETING`. Ryan Kleck (rkleck-mntn) authored both speculation pins and owns the feature-store pipelines. `site_network_hourly` is ours.
- **Validation constraints:** a `runtime_properties` change requires regenerating `dags/model_task_config.json` (`MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun`); decorator changes reach prod on the bundle redeploy after merge (up to 12 h). No dev batch from a laptop (no `iam.serviceAccounts.actAs` on the dev SA). `model-unit-test` CI is broken repo-wide since #1209 (not a required check); locally the Spark-backed tests need a Java runtime this Mac does not have.
- **No BigQuery query is needed for this ticket.**

### 3.2 Steps (status)
**Phase A: evidence per DAG** — DONE (previous execute agent, resumed): live ledger downloaded; 16 model logs + 2 PHS logs fetched and parsed with `artifacts/audi_1275_eventlog_props.py` into `outputs/audi_1275_app_props.csv` and `outputs/audi_1275_stage_evidence.csv` (PHS pair under `outputs/phs/`); the 13 models grepped for task-side side effects (none); committer semantics pinned from source at v3.5.3 / rel/release-3.3.5 / apache-iceberg-1.10.2; `audience_intent` batches listed from the API (`outputs/audi_1275_audience_intent_batches_all.csv`, 7-day retention); commits 64afca4 / 009e99a / 5fbeb38 / a3352d9 / 6afc07f read; Confluence "TI On Call Playbook" NOT searched (no Confluence access from this agent); `on-call/oncall_runbook.md` grepped (no Nov-2025 speculation incident recorded there; it predates the runbook).
**Phase B: decision memo** — DONE: `outputs/audi_1275_decision_memo.md` (rule, source quotes, prod config, Nov-2025 git record, 13 verdicts, remedy spec, canary expectations, contradiction record, post-merge checklist).
**Phase C: Slack ask** — DONE: `artifacts/audi_1275_slack_ask_ryan.md` (three numbered asks, lint `--kind comment` OK 74 words / 480 chars). The user sends it; the agent never did.
**Phase D: PR on branch `audi-1275-straggler-gcs-writers`** — DONE in the worktree (dispatcher commits, runs the gauntlet, opens the PR): `models/bidstream_hourly/site_network_hourly.py` +1 line (`"spark.speculation": "true"` in `runtime_properties`), `dags/model_task_config.json` regenerated (+1 line, the same key under `site_network_hourly.batch.runtime_config.properties`). PR body `artifacts/audi_1275_pr_body.md` (lint `--kind pr` OK 128 words / 894 chars), reviewer rkleck-mntn, no Release Type. Opened 2026-09-03 PT as https://github.com/SteelHouse/airflow-ti/pull/1271 (gauntlet fast tier, 1 finding refuted, 0 confirmed); ticket folder committed, Jira comment posted, status In Progress.
**Phase E: write-backs** — this file (§4-§8), result comment `artifacts/audi_1275_result_comment.txt` (lint `--kind completion` OK 118 words / 796 chars). Knowledge routed by `/capture` on 2026-09-03 (§7). Self-review entry written 2026-09-03 with the outcome marked open; amend at close.

### 3.3 Assumptions, resolved
- A1 (each "default v2" model really runs FileOutputCommitter v2 with no manifest factory): CONFIRMED from each model's own event log for 9 of the 10 (advertiser_high and ipdsc_ds_63 have no ledger app id, source only).
- A2 (no task-side side effects): CONFIRMED by grep (§4.5).
- A3 (Iceberg discards the duplicate): CONFIRMED from `SparkWrite.java` (task `abort()` deletes files; `useCommitCoordinator()` false, so the scheduler's one-result-per-partition rule is what picks the winner; an already-finished duplicate can leave an unreferenced orphan file).
- A4 (manifest committer writes the manifest at task commit, job commit renames listed files): CONFIRMED from the Hadoop doc; the Nov-2025 failure was in that job-commit rename phase (§4.3), which is why the pair stays owner-gated rather than cleared.
- A5 (live straggler key per DAG): NOT for all. advertiser_high, ipdsc_ds_63, hhdsc_ds_19 have no straggler row at all; ipdsc_ds_42 and site_visit_signal_advertiser_id_dsc_id are `resolved`; those five are no-ops unless re-flagged.
- A6 (dryrun regenerates deterministically): CONFIRMED, `git diff --stat` = 2 files, 2 insertions.

### 3.4 Risks (unchanged, R2 now has an observed analogue)
- R1 duplicate compute on a saturated app; measured over three sweeps; revert is one line.
- R2 a duplicate killed mid-`commitTask` under v2: the coordinator denies the loser before it starts committing. The analogous pre-existing case (stage re-attempt after `FetchFailed`, not speculation) already leaves task-committed files without `_SUCCESS`: `dt=2026-09-02/hh=05` (§4.6).
- R3 Ryan's pin may encode an incident the git record does not explain; the manifest pair waits.
- R4 bundle lag up to 12 h; check the `Compute batch:` log line, not the merge.
- R5 `model-unit-test` CI red repo-wide (#1209).

### 3.5 Decisions taken by the user (2026-09-02)
- D1 + D2: CANARY only. `site_network_hourly` in this PR with Ryan as required reviewer; the other 10 safe DAGs after three clean sweeps and Ryan's answer; advertiser_join and prospecting_join owner-gated.

### 3.6 Effort
2 SP as filed. Phase A-D took two agent sessions on 2026-09-02 (the first was cut off after Phase A). Phase E after the first three post-merge sweeps.

### 3.7 Sources
- Ticket spec: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` (#20), `audi_1194_hackathon_optimizations_2026_08_27.md` (rows 37-67, row 6 refutation), `artifacts/audi_1194_implementation_queue.md` (#4); live ledger `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl`.
- Memory: `knowledge/memory/project_airflow_optimizer.md` (2026-08-27 revert), `reference_dataproc_eventlog_profiling.md` (contradiction record, fetch-wait finding on site_network_hourly), `reference_airflow_ti.md` (pin, deploy lag, dev constraints, JSON regen, PAM), `feedback_airflow_prod_safety.md`, `reference_jira_conventions.md`.
- airflow-ti main 825b07e3 (worktree `scratchpad/wt/audi_1275`): the 13 model files; `dags/model_task_config.json`; `dags/audience_intent/audience_intent.py` (L95-130); `dags/models/bidstream_hourly/site_network_hourly.py`; `include/spark/data_source/ipdsc_emr_cluster.py`; `include/models/operators.py:331` (`Compute batch:` log line); `utils_model/base_model/{base_model,writer}.py`; `include/spark_optimizer/{eventlog,optimizations,ledger}.py`; `.github/workflows/pr_model.yaml`; commits 40ecc73, 64afca4, 009e99a, 5fbeb38, a3352d9, 6afc07f, 82d68fa (#1232).
- Event logs parsed (all deleted from `outputs/` after parsing, list in `outputs/audi_1275_event_logs_parsed.txt`; re-fetch with `gsutil -o "GSUtil:check_hashes=never" -o "GSUtil:sliced_object_download_threshold=0" cp gs://mntn-data-archive-prod/spark-events/<app>.zstd outputs/`): `app-20260826010824671-0442`, `app-20260827011147010-0844` (site_visit_signal_advertiser_id_dsc_id); `app-20260827051712075-0747` (ipdsc_ds_42); `app-20260831041632940-0428`, `app-20260901141637986-0672` (aug_log_ip_vertical_id_hourly); `app-20260901033636287-0177` (hhdsc_ds_19); `app-20260901055729824-0243` (prospecting_join); `app-20260901060920436-0979` (advertiser_join); `app-20260901083658620-0891`, `app-20260902091759702-0256` (tpa_mntn_id_export); `app-20260901205120829-0863`, `app-20260902005143827-0954`, `app-20260902115131178-0350`, `app-20260902085134727-0157.zstd.inprogress` (site_network_hourly); `app-20260902030204550-0576` (ipdsc_ds_47); `app-20260902041711901-0077` (identity_targeted_signal); `app-20260902075146336-0574` (fangorn_score_monitor). PHS: `app-20260902044737985-0424` (scoring-vertical-mid), `app-20260902050521545-0199` (scoring-prospecting-high).
- Prod batches: `gcloud dataproc batches list --project mntn-prj-prod-00 --region us-central1` with `labels.airflow-dag-id=audience-intent` and `labels.job_type=site_network_hourly` (`outputs/audi_1275_audience_intent_batches_all.csv`, `outputs/audi_1275_site_network_batches_recent.csv`). The default gcloud project `dw-main-silver` returns PERMISSION_DENIED for `dataproc.batches.list`; `--project mntn-prj-prod-00` is required.
- GCS baseline: `outputs/audi_1275_site_network_output_baseline.txt` (`gsutil ls -l` over `gs://mntn-data-archive-prod/ipdsc_site_network/site_network_hourly/dt=2026-08-27..2026-09-02/hh=*/`).
- External at exact tags (raw.githubusercontent.com): Spark v3.5.3 `SparkHadoopMapRedUtil.scala`, `OutputCommitCoordinator.scala`, `HadoopMapReduceCommitProtocol.scala`, `FileFormatWriter.scala`, `WriteToDataSourceV2Exec.scala`; Hadoop rel/release-3.3.5 `FileOutputCommitter.java`, `manifest_committer.md`; Iceberg apache-iceberg-1.10.2 `SparkWrite.java`.

## 4. Investigation & Findings

### 4.1 Committer and speculation per DAG, from each DAG's own event log
`outputs/audi_1275_app_props.csv` (16 model logs) and `outputs/phs/audi_1275_app_props.csv` (2 script-batch logs). Every log: Spark 3.5.3, `fileoutputcommitter.algorithm.version=2`, `outputCommitCoordination.enabled` unset (default true), `hadoop-cloud-storage-3.3.5.jar`, `spark-hadoop-cloud_2.13-3.5.3.jar`. Manifest committer keys (`ManifestCommitterFactory`, `PathOutputCommitProtocol`, `BindingParquetOutputCommitter`, `validate.output=false`) and `spark.speculation=false` appear ONLY in advertiser_join (`app-20260901060920436-0979`) and prospecting_join (`app-20260901055729824-0243`). identity_targeted_signal ships `iceberg-spark-runtime-3.5_2.13-1.10.2.jar`; every other model carries the runtime's `1.9.0-google-5` jar. The two PHS logs carry `spark.speculation=true`, v2, no manifest factory. `spark.speculation.quantile` / `.multiplier` are unset everywhere (Spark defaults 0.9 / 3).

### 4.2 Prod precedent, now with event-log proof that speculation fires
| batch | app id | speculative attempts | task ends | write stage | result |
|---|---|---|---|---|---|
| scoring-vertical-mid | `app-20260902044737985-0424` | 366 | Success 29,469; TaskKilled 366; 0 failures | 29 (2,000 tasks, 42.0 GB) | SUCCEEDED |
| scoring-prospecting-high | `app-20260902050521545-0199` | 88 | Success 16,261; TaskKilled 88; 0 failures | 20 (5,000 tasks, 1,377.9 GB) | SUCCEEDED |

Every speculative attempt ended `TaskKilled` (loser killed when the winner finished), none reached a commit, no `CommitDenied`, no task failure. API exposure (7-day retention, 2026-08-27 to 2026-09-03): the five speculation batches show 38 SUCCEEDED, 0 failed, 1 RUNNING at list time. Config in place since 2025-08-15 (`40ecc73`); five batches a day is roughly 1,900 runs, only the last 38 are API-verifiable.

### 4.3 The Nov 2025 pin, reread from git (the incident was in the manifest committer's rename phase)
`64afca4` 2025-10-08 (Dustin) added the manifest committer to `ipdsc_emr_cluster.py` with speculation still `true`. `009e99a` 2025-11-12 (Ryan) flipped speculation to `false`, comment "Disabled to prevent race conditions with ManifestCommitter", empty body. `5fbeb38` 2025-11-17 (Ryan, mntn_select): body "ISSUE 1: FileNotFoundException during ManifestCommitter parallel rename", fixed with `manifest.committer.io.threads=1` + `validate.output=false`, "Fewer files = fewer parallel renames = less chance of race condition". `a3352d9` 2025-11-20 (Ryan): same two settings on `tpa_ipdsc_export`, "prevents FileNotFoundException during parallel rename phase". `6afc07f` 2025-12-09: manifest committer + `false` pin copied to advertiser_join and prospecting_join, `io.threads=1` removed from ipdsc. So: the FileNotFound persisted 5-8 days after speculation was off and was fixed in the job-commit rename phase (driver side, after all tasks committed, no duplicate attempts alive). The git record does not show speculation causing the incident. It also does not fully explain the GCS rename anomaly, so the manifest pair stays owner-gated; ask 1 to Ryan targets exactly this.

### 4.4 Live ledger straggler rows (last sweep 2026-09-02)
advertiser_join `straggler:3` chronic streak 7 (83.5 exec-h). prospecting_join `:10` new (`:24`, `:51` resolved). identity_targeted_signal `:3` chronic streak 7 (`:1` resolved). fangorn_score_monitor `:12` recurring 2, `:15` new (`:10` resolved). ipdsc_ds_42 `:3` resolved. ipdsc_ds_47 `:2` chronic 6, `:5` chronic 4 (186.1 exec-h). ipdsc_ds_63, hhdsc_ds_19, advertiser_high: no straggler rows. aug_log_ip_vertical_id_hourly `:7` new, `:31` new (`:9`, `:11` resolved). site_network_hourly `:9` chronic 4; `:11`, `:13`, `:15`, `:18`, `:21`, `:22`, `:23`, `:24`, `:29`, `:39` new (`:5`, `:6` resolved). site_visit_signal_advertiser_id_dsc_id `:4`, `:6` resolved. tpa_mntn_id_export `:7` new, `:8` new. The ledger's `exec_h` on site_network_hourly rows is 3,653.1 (DAG-level month figure, not per run).

### 4.5 Straggler stage class per DAG (compute vs write) and side effects
From `outputs/audi_1275_stage_evidence.csv` (`output_bytes > 0` marks a write stage). Compute-stage stragglers: ipdsc_ds_47 stage 2 (2,985 tasks, median 104.4 s, max 3,080.5 s) and 5; ipdsc_ds_42 stage 3 (12,599 t, 10.4 s / 89.1 s); aug_log_ip_vertical_id_hourly 7 (8,804 t, 3.1 s / 696.3 s) and 31; site_visit_signal 4 and 6; fangorn_score_monitor 12 (30,000 t, 5.8 s / 61.5 s) and 15 (2,048 t, 71.2 s / 431.3 s); tpa_mntn_id_export 7 and 8 (46-59k tasks, ~8 s / ~83 s); advertiser_join 3 (4,798 t, 19.5 s / 214.1 s, 2,591.8 GB shuffle write); prospecting_join 10; site_network_hourly 9, 11 (10,589 t, 0.7 s / 991.7 s), 13, 15, 18, 21-24. Write-stage stragglers: identity_targeted_signal stage 3 (634 t, 31.3 s / 236.3 s, 104.2 GB out, the Iceberg write itself) and site_network_hourly stage 39 on `app-20260902005143827-0954` (37 t, 4.0 s / 161.3 s, 0.53 GB). Task-side side effects: none; only pure UDFs (`pandas_udf` + `tldextract` in aug_log_ip_vertical_id_hourly L147, `F.udf` in site_visit_signal L31); tpa_mntn_id_export's `storage.Client()` / `upload_from_string` (L161, L343-350) run on the driver after the write.

### 4.6 site_network_hourly, the canary: what its logs really show
- Writes: two per run (`/dt=<D>/hh=<HH>` for hour-2 and hour-1), 30-37 tasks, 0.4-0.5 GB each, `mode("overwrite")`, through `StorageWriter` = `df.write.format("parquet")` (`utils_model/base_model/writer.py:30`). Each hour is written twice by consecutive runs; every per-hour exception is swallowed (`models/bidstream_hourly/site_network_hourly.py` L245-250), so the batch always SUCCEEDS.
- GCS baseline 2026-08-27 to 2026-09-02: 167 hour partitions, 166 with `_SUCCESS`; per day 612-782 files and 7.8-11.7 GB; per hour 4-75 files, 0.204-0.621 GB; `dt=2026-08-31/hh=18` absent; `dt=2026-09-02/hh=05` has 29 files and no `_SUCCESS`.
- Wall clock (40 batches, 2026-09-01 14:50 to 2026-09-03 05:50 UTC, all SUCCEEDED): 2 min to 2 h 25 min. The long runs are `FetchFailed` storms, not slow executors: `app-20260901205120829-0863` (batch `sit-net-hou-y3a-20260901-195000-1`, 2 h 25 min) had 8,244 `FetchFailed` task ends, stage 8 re-submitted 116 times, both hour jobs failed ("ShuffleMapStage 9 / 25 has failed the maximum allowable number of times: 4"), 0 bytes written, batch SUCCEEDED. `app-20260902115131178-0350`: 3,134 FetchFailed, 24 jobs, 0 failed. `app-20260902005143827-0954`: 2,115 FetchFailed, 21 jobs, 0 failed. Executor removals are all "Executor decommission finished: spark scale down" (dynamic allocation with `shuffleTracking.timeout=300s`), so the lost shuffle blocks are the scale-down's.
- The `hh=05` partition without `_SUCCESS` comes from `app-20260902085134727-0157` (batch `sit-net-hou-hy9-20260902-075000-1`, log still `.inprogress`, no `SparkListenerApplicationEnd`): job 12 (the hour-05 write) failed at `ResultStage 19` after the maximum attempts (13 FetchFailed in the write stage, 1,833 in stage 15); the attempts that had been authorized had already merged their files into the destination (v2 commits at task commit); files carry two timestamps (09:45 and 10:02 UTC) from two stage attempts; 0.482 GB written; the exception was swallowed. This is the pre-existing partial-partition exposure of v2 + swallowed errors, without speculation.
- Consequence for the canary: it measures safety (speculative attempts present, ending `TaskKilled`/`Success`, `_SUCCESS` and per-hour file counts inside the band) before runtime. Speculation cannot repair a lost shuffle, so the `straggler:*` keys that are fetch-wait tails may not resolve; the ones on stages without FetchFailed (11, 13 in `0350`; 39 in `0954`) can.

### 4.7 Dead ends and gaps
- `gcloud dataproc batches list` under the default project `dw-main-silver` is PERMISSION_DENIED; `--project mntn-prj-prod-00` works. Batch retention in the API is about seven days, so the exposure count since 2025-08-15 cannot be verified beyond the last 38 runs.
- The 2025-11 incident is not in `on-call/oncall_runbook.md` (predates it) and Confluence was not reachable from this agent.
- `pytest tests/models`: 6 passed, 1 error (`test_export_tpa.py` needs a Java runtime for PySpark, none on this Mac); CI `model-unit-test` is red repo-wide anyway (#1209).
- `ruff check` (0.16.1, repo has no root ruff config): the six default-rule findings on `site_network_hourly.py` (3x BLE001, 2x DTZ007, 1x I001) all pre-exist on main at the same positions; the change adds none.

## 5. Solution
**PR:** https://github.com/SteelHouse/airflow-ti/pull/1271 (opened 2026-09-03 PT; fast tier, 1 finding refuted, 0 confirmed; reviewer Ryan Kleck)

- **Decision memo:** `outputs/audi_1275_decision_memo.md`.
- **Slack ask for Ryan Kleck (user sends):** `artifacts/audi_1275_slack_ask_ryan.md`.
- **Canary PR (worktree `scratchpad/wt/audi_1275`, branch `audi-1275-straggler-gcs-writers`, dispatcher commits/gauntlets/opens):** `models/bidstream_hourly/site_network_hourly.py` +1 line, `dags/model_task_config.json` +1 line (regenerated by `MNTN_SDLC_ENV=dev uv run --group models python model_upload.py --dryrun`, exit 0, "Compiling all models / Skipping all models upload to 'dev' env"). PR body `artifacts/audi_1275_pr_body.md`. Reviewer rkleck-mntn. No Release Type.
- **Result comment:** `artifacts/audi_1275_result_comment.txt`.
- **Evidence files:** `outputs/audi_1275_app_props.csv`, `outputs/audi_1275_stage_evidence.csv`, `outputs/phs/*.csv`, `outputs/optimization_ledger_live.jsonl`, `outputs/audi_1275_audience_intent_batches{,_all}.csv`, `outputs/audi_1275_site_network_batches_recent.csv`, `outputs/audi_1275_site_network_output_baseline.txt`, `outputs/audi_1275_event_logs_parsed.txt`; helper `artifacts/audi_1275_eventlog_props.py`.

## 6. Questions Answered
- **Q:** Which straggler remedy is safe for Spark jobs that overwrite GCS output?
  **A:** `spark.speculation=true` with Spark 3.5.3 defaults, wherever every writer in the application discards the losing duplicate attempt: Hadoop FileOutputCommitter v2 under Spark's commit coordinator (first attempt per partition commits, the rest are denied and delete their attempt directory) and Iceberg (task abort deletes the attempt's files, the snapshot references only accepted commit messages). There is no per-stage switch.
- **Q:** Which of the 13 can take it now?
  **A:** 11 are safe by source (10 v2 + identity_targeted_signal on Iceberg); one, site_network_hourly, takes it now as the canary; the other 10 wait for three clean sweeps and Ryan's answer; advertiser_join and prospecting_join (manifest committer, owner's pin) wait for Ryan regardless.
- **Q:** What was the Nov 2025 incident behind the pin?
  **A:** Per git, a `FileNotFoundException` in the manifest committer's parallel rename phase (job commit), fixed 5-8 days after speculation was disabled by `io.threads=1` and `validate.output=false`. Ryan's own account is still needed (ask 1).
- **Q:** Does the prod precedent actually exercise speculation?
  **A:** Yes: 366 and 88 speculative attempts in the two PHS logs read, all `TaskKilled`, both batches SUCCEEDED with `_SUCCESS`.
- **Q:** Will the canary show a runtime win?
  **A:** Not necessarily. site_network_hourly's long runs are FetchFailed storms from scale-down shuffle loss; speculation addresses slow tasks on live executors. The canary is the safety test first.

## 7. Data Documentation Updates
Routed by `/capture` on 2026-09-03 (write-only sweep; the dispatcher commits):
- `knowledge/memory/reference_dataproc_eventlog_profiling.md`: contradiction appended under the 2026-08-27 "speculation unsafe on every GCS writer" line (both claims kept with evidence, reconciling hypothesis = a one-committer incident generalized to all writers, settling check = first post-merge site_network_hourly log with `Task Info.Speculative=true` attempts + clean `_SUCCESS`, plus Ryan's account); facts 1-5 (Dataproc injects FileOutputCommitter v2 and no manifest factory; `OutputCommitCoordinator` denies every attempt after the first for any Hadoop committer; Iceberg 1.10.2 opts out of the coordinator and deletes aborted attempts' files; audience_intent scoring batches run speculation on v2 since 2025-08-15, 366 / 88 attempts all `TaskKilled`; the Nov 2025 pin's incident was the manifest committer's rename phase, fixed after speculation was off); the FetchFailed-storm-vs-straggler discriminator; the straggler detector's thresholds and fetch-wait blind spot; `.zstd.inprogress` logs still parse.
- `knowledge/memory/project_airflow_optimizer.md`: dated contradiction pointer on the 2026-08-27 `ipdsc_ds_35` gauntlet revert; new 2026-09-03 section (decision, canary PR #1271, what the canary can show, the 11 ledger keys to stamp).
- `knowledge/memory/reference_airflow_ti.md`: contradiction pointer on the "speculation pinned on every GCS writer" bullet; the Nov 2025 pin's git record (64afca4 / 009e99a / 5fbeb38 / a3352d9 / 6afc07f); `gcloud dataproc batches` needs `--project mntn-prj-prod-00 --region us-central1` (default `dw-main-silver` PERMISSION_DENIED, ~7-day retention, `labels.airflow-dag-id` / `labels.job_type` / `labels.airflow-task-id`, `spark:` key prefix in describe); dryrun re-verified, pytest-without-Java and ruff pre-existing findings merged into the AUDI-1273 / AUDI-1274 lines.
- `knowledge/data_catalog.md`: new § `ipdsc_site_network/site_network_hourly` (path, writer, two hours per run, `_SUCCESS` marker, 7-day volume band, consumer).
- `knowledge/data_knowledge.md`: § augmentor_log TTL and Archives, new bullet "site_network_hourly SUCCEEDS with a missing or partial hour" (swallowed exception, FetchFailed storms, partial partition without `_SUCCESS`, not caused by speculation).
- `knowledge/decisions/0004_speculation_safe_where_writer_discards_loser.md`: the writer-class rule and the canary decision (accepted 2026-09-02).
- `improvements_backlog.md`: IMP-104, site_network_hourly FetchFailed storms + swallowed hour writes.
- `self_review/self_review_2.md`: AUDI-1275 entry (outcome marked open until merge + sweeps).
- Nothing new for `mntn_business.md`, `experimentation.md`, the glossary, or the MEMORY.md hot tier.

## 8. Open Items / Follow-ups
- **Deviation from the planned §3 step 11:** only `site_network_hourly` was edited (user decision D1+D2 = canary); the 11-model edit list did not ship. §3 rewritten above to match.
- **Waiting on the canary + Ryan (verdict "safe by source, wait"):** ipdsc_ds_47 (chronic `:2`, `:5`), aug_log_ip_vertical_id_hourly (`:7`, `:31`), fangorn_score_monitor (`:12`, `:15`), tpa_mntn_id_export (`:7`, `:8`), identity_targeted_signal (`:3` chronic, write-stage straggler on Iceberg). **No-op unless re-flagged (safe by class, no live key):** ipdsc_ds_42, ipdsc_ds_63, hhdsc_ds_19, advertiser_high, site_visit_signal_advertiser_id_dsc_id. **Owner-gated (manifest committer, Ryan's pin):** advertiser_join (`:3` chronic 7, 83.5 exec-h), prospecting_join (`:10` new).
- Post-merge watch (human): bundle lag up to 12 h; `Compute batch:` line shows `'spark.speculation': 'true'`; `_SUCCESS` + 4-75 files + 0.2-0.7 GB per hour; event log `speculative_tasks > 0`, no `CommitDenied`/`FileAlreadyExists` failures; stamp `applied` on the 11 site_network_hourly keys present at merge; three sweeps.
- Ryan's answers to the three asks decide the second PR (10 models) and the manifest pair.
- Not done: Confluence "TI On Call Playbook" search for the Nov-2025 incident; the gcs-connector version string (planning wave read 3.1.16, execution logs show the unversioned basename).
- site_network_hourly's FetchFailed storms (scale-down shuffle loss, swallowed failed writes) are a separate defect, logged 2026-09-03 as `improvements_backlog.md` IMP-104 (speculation cannot repair a lost shuffle, so it is outside this ticket).
- Self-review entry written 2026-09-03 (`self_review/self_review_2.md`, outcome marked open); amend the outcome line at close.

## Verification (adversarial check, 2026-09-03)
**Verdict: partial.** Diff, artifacts, config precedence, ruff, and lint claims all reproduce exactly from source; one number in §4.6 contradicts the file it cites.

### Confirmed accurate (re-derived, not re-read)
- **Diff matches §5 exactly:** worktree `git diff --stat` = `dags/model_task_config.json` +1, `models/bidstream_hourly/site_network_hourly.py` +1, both adding `"spark.speculation": "true"`; no other tracked or untracked change anywhere in the worktree.
- **Builder/decorator precedence, checked from source:** `include/models/operators.py` reads `dags/model_task_config.json` via `ComputeConfigReader(...).read_config()` straight into the submitted `self.batch`; the decorator's `runtime_properties` is never read at task-execute time, only at compile (`model_upload.py` → `ctx.compile_models()`). The JSON is the actual runtime source and carries the same key, so there is no builder/decorator conflict.
- **"Regenerated, not hand-edited" corroborated:** all 108 `properties` blocks in `model_task_config.json` are alphabetically sorted (checked programmatically), including the edited one — the new key lands exactly where a sorted serializer would place it (`speculation` before `sql.files...`), while the `.py` decorator's dict keeps insertion order with the key appended at the end. Consistent with a real tool regen.
- **Ruff claim reproduced exactly:** `ruff check` on the worktree file gives 6 findings (3x BLE001, 2x DTZ007, 1x I001); the same 6 findings, same code, at positions shifted by exactly the one inserted line, on `git show main:models/bidstream_hourly/site_network_hourly.py`. Zero new findings from the PR, confirmed.
- **Lint claims reproduced exactly:** `lint_comms.py --kind comment` on the Slack ask = 74w/480c/0 bullets OK; `--kind pr` on the PR body = 128w/894c/9 bullets OK; `--kind completion` on the result comment = 118w/796c/6 bullets OK. All match §3.2.
- **Commit history checked:** all 7 cited commits (`64afca4`, `009e99a`, `5fbeb38`, `a3352d9`, `6afc07f`, `82d68fa`, `40ecc73`) exist with the stated author/date/message; `82d68fa` (2026-08-27, mdunn-mntn) confirms "ours since #1232".
- **GCS baseline re-derived from `outputs/audi_1275_site_network_output_baseline.txt`:** 167 hour partitions, 166 with `_SUCCESS` (only `dt=2026-09-02/hh=05` missing it), per-hour 4-75 files / 0.204-0.621 GB, per-day 612-782 files / 7.79-11.74 GB, `dt=2026-08-31/hh=18` absent — all match §4.6 and the memo §7 exactly.
- **Canary stage numbers spot-checked against `outputs/audi_1275_stage_evidence.csv`:** stages 11, 13, 15, 39 (tasks / median s / max s) match the memo's §5 table to the decimal.
- **All 20 referenced output/artifact files exist** and hold real, varied data; no untracked files anywhere outside this ticket folder and the two worktree files above.

### Defect found
- **§4.6:** "`dt=2026-09-02/hh=05` has 19 files and no `_SUCCESS`." The cited source (`outputs/audi_1275_site_network_output_baseline.txt`) shows **29** part files for that partition (0.482 GB total, matching the "0.482 GB written" figure two sentences later, so only the file count is wrong). Reads as a slip from "ResultStage 19," named in the same sentence. Contained to this one bullet: the decision memo §7 correctly states no file count for that partition, and the PR body / Slack ask / result comment don't mention it either.

### jira_comment
`artifacts/audi_1275_result_comment.txt` carried through unchanged — the wrong number does not appear in it; lint OK (118w/796c/6 bullets).
