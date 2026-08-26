---
name: airflow-ti-repo-and-deployment
description: "Feature store pipeline repo — architecture, deployment workflow, local testing, backfill process. SteelHouse/airflow-ti."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 60b2f7af-ea4c-4042-bcd9-027f6c6ad945
doc_type: memory
keywords: [airflow-ti, feature store pipeline, dataproc serverless, ryan kleck, model_run.py, model_upload.py, backfill, feature_group, parquet schema gotchas, GCS feature store paths, persistent history server, PHS, spark event logging, spark-job-history, PR 1169, deploy to prod, spark eventLog, dataproc-debug pam, dataproc-temp bucket access, ExternalTaskSensor, skipped_states, failed_states, skip as failure, ExternalTaskFailedError, wait_fpa, globStatus, flat glob, fs.gs.glob.flat.enable, gcs list timeout, literal partition paths, augmentor_log glob, INC-012, PR 1176, basePath, getFileInfoInternal, root stat, directory marker, COLUMN_ALREADY_EXISTS, PR 1177, batch-id attach, create_batch_id, batch with given id already exists, ti_resources spark deploy lag, model_backfill.sh, backfill runbook, PR 1180, INC-013, PR 1179, dsid30 augmentor reader, silent try except degrade, mntn_global_data, feature_store_setup_model schedule, dt D+1 offset, catchup false latest interval only, bae_ip disabled, INC-015, retired model gcs dir, guid_log_derived_advertiser_id_dsc_id, conversion_log_derived_advertiser_id_dsc_id, guid_and_conv_log_derived_advertiser_id_dsc_id, false missing partition, dev not a mirror of prod, INC-016, INC-017, PR 1195, dag_vars.py, create_batch_id task decorator, batch id try_number, retry reattaches failed batch, materialize_mntn_first_party, materialize_mntn_select, tpa_ipdsc_export call sites, IMP-041, IMP-042, already_exported _SUCCESS, XComArg templated field, xcom_pull template render, dropped dependency edge, template_fields batch_id, logical_date nullable manual run, DagBag parse validation, AIRFLOW_VAR env var, SUPERVISOR_COMMS ImportError, no pytest in venv, parse against main control, create_batch cached spec, create_batch clear with downstream, batch spec xcom cache, config rerun trap, run with latest bundle version insufficient, false success cleared running batch, cancelled batch green tick, materialize success under 3 min, driver memory 9600m default, MapOutputTracker OOM, map-output-dispatcher, java heap space driver, constant death interval, resource ceiling vs data bug, shuffle partitions 5000, PR 1198, PR 1196, pam grants create, pam grants describe, gcloud storage cat driveroutput, gsutil wildcard 403, dataproc staging bucket, INC-018, ip_data_staging retention, sh-dw-external-tables-prod retention, 5002 objects, PR 1209, hh_id_data, household export, tpa_mntn_id_export, MemDB, ID-431, ip_data data_sources empty cats, operator timeout not wall clock, wait_for_batch no deadline, _cancel_previous_tries, load_graph_ids 1to1 ipv4, dag_run.conf param validation, ParamValidationError]
domain: [repos, infra]
lifecycle: active
last_verified: 2026-08-26
---
## Repo
- **GitHub:** SteelHouse/airflow-ti
- **Local clone:** ~/Developer/work/mntn/airflow-ti
- **Framework owner:** Victor (model framework), Ryan Kleck (feature store pipelines)
- **Decider:** Alyson Lefkowitz
- **Site-visit-signal (svs) DS feeders:** `spark/fpa/dsid{NN}_*_processing.py` (Spark jobs, DAG `fpa_site_visit_batch_serverless`), authored by **Sean Yang**. Each DS-source has its own file: e.g. `dsid30_augmentor_log_processing.py` (**filters augmentor to `placement_type IN (BANNER, BANNER_AND_VIDEO)`** → svs DS30 = banner slice only; URL from page+referrer), `dsid23_guid_log_processing.py` (guid, pixel-isolation anti-join, URL=`product_referer`). This is where a free-log (guid/augmentor) coverage or filter change would land (AUDI-1091/1093). See [[reference_ddp_billing_logic]].

## Architecture
Three-layer feature store on GCP Dataproc Serverless via Airflow (Astronomer):
- **Layer 1 (source):** `feature_group_1_source/` — daily/hourly rollups, raw counts, `dt=` partition
  - **INVARIANT (Ryan Kleck):** every L1 feature must be **aggregatable over 30 days via `sum`/`min`/`max`/`hll_merge`/etc.** — nothing that can't be aggregated goes in L1. This is why the MNTN-ID household re-key is mechanically clean: household `GROUP BY mntn_id` reuses the same aggregation primitives as the temporal rollup (**temporal rollup ≡ household rollup**; distincts = HLL sketches → `hll_merge`, not sum). See [[project_fangorn_on_mntn_id]].
- **Layer 2 (derived):** `feature_group_2_derived/` — 7/14/30d windows, joins, transforms, `effective_date=` partition
- **Layer 3 (pivoted):** `feature_group_3_pivoted/` — wide format for model consumption

## Key Files
- `model_run.py` — submits model to **Dataproc Serverless** (NOT local Spark). Branch-aware: reads prod, writes dev with branch suffix.
- `model_upload.py` — compiles all models, uploads code artifacts to GCS. `--dryrun` validates compilation only.
- `dags/model_task_config.json` — auto-generated by compilation, must be committed.
- `dags/models/feature_store_setup_model.py` — daily DAG (schedule `3 1 * * *`, 01:03 UTC), all layers with deps. **Logical date D writes `dt=D+1`; `catchup=False`** — unpausing runs ONLY the latest interval, so paused days become permanent `dt=` holes unless triggered manually (INC-015). `bae_ip` task commented out by Brian 2026-07-20 "temporarily disabled while fixing upstream" (commit `a0359299`). **Avoid the 90-day UI backfill on this DAG** — it started the INC-015 mess (whole-DAG only; runbook INC-015 decision tree).
- `dags/models/feature_store_hourly.py` — hourly DAG (:15 past each hour).
- `dags/models/feature_store_snapshot.py` — monthly snapshots (day 2/15/22).
- `docs/feature_store_naming_standards.md` — authoritative naming conventions.

## Base Classes
- `FileStorageBaseModel` — single output path
- `MultiSnapshotFileStorageBaseModel` — supports "base" + "monthly" snapshots
- Key methods: `self.read_model("module.ClassName")`, `self.df_write(df)`, `self.spark`
- BQ reads: `self.read_model("bigquery_data.BQ").query(sql)` (NOT `bq_data.BqData`)
- Postgres: `self.read_model("core_db_data.CoreDb").dbtable(query)`

## Naming Conventions
- Table: `{source_dataset}_{dimensions}` (Layer 1), `{source}_derived_{dimensions}` (Layer 2), `{source}_pivot_{dimensions}` (Layer 3)
- Hourly suffix: `_hourly`
- Source prefixes: `aug_log`, `guid_log`, `conversion_log`/`conv_log`, `site_visit_signal`, `ipdsc`
- Columns: snake_case, metric names include lookback suffix `_7d`/`_14d`/`_30d`
- Partition: Layer 1 `dt=YYYY-MM-DD`, Layer 2/3 `effective_date=YYYY-MM-DD`

## Deployment Workflow
1. **Branch:** create feature branch off main
2. **Write models:** in `models/feature_store/feature_group_{N}_{type}/`
3. **Compile:** `uv run python model_upload.py --dryrun` (validate)
4. **Upload to dev:** `uv run python model_upload.py` (compiles + uploads to GCS dev)
5. **Test on Dataproc:** `uv run python model_run.py {model_name} -a '{"run_date": "YYYY-MM-DD"}'` — submits to Dataproc Serverless, writes to dev bucket with branch suffix
6. **Backfill:** use `scripts/model_backfill.sh` + `docs/model_backfill.md` (merged airflow-ti#1180, 2026-08-07) — the canonical dev-run+copy method (seed/mirror/daily/monthly/copy modes, read-resolution rules). Detail: [[reference_airflow3_backfill_scoping]]
7. **Verify:** check output in `gs://mntn-data-archive-dev/feature_store/...`
8. **PR:** create PR, get Ryan's review
9. **Move to prod:** after PR approved, copy backfilled data from dev to prod (or re-run in prod)
10. **DAG wiring:** Ryan/owner adds tasks to DAG files and sets dependencies
11. **Deploy:** merge PR triggers GitHub Actions deploy to prod GCS

## GCS Paths
- Prod: `gs://mntn-data-archive-prod/feature_store/feature_group_{N}_{type}/`
- Dev: `gs://mntn-data-archive-dev/feature_store/feature_group_{N}_{type}/` — **NOT a mirror of prod.** 29 of 38 models compile with `read_location` = dev, so dev holds only hand-seeded backfill remnants; a dev run of an existing model reads stale/absent inputs. Census + inspection recipe: [[reference_airflow3_backfill_scoping]].
- Branch-aware: dev outputs get `_{branch_suffix}` in dataset naming
- **TPA export outputs (`gs://sh-dw-external-tables-prod/ip_data_staging/YYYY/MM/DD/` and `ip_data/YYYY/MM/DD/`) retain only ~6 DAYS** (observed 2026-08-11, INC-016). You cannot use them to audit whether an older run shipped — only the last ~6 days are checkable. A healthy staging day = **5002 objects**; sizes ranged **224-491 GiB across 08/06-08/10** (08/10 = 416.98 GiB, mid-band). Object count is the stable check, size is not.
- **RETIRED models leave live-looking directories (INC-015, 2026-08-10).** `feature_group_2_derived/guid_log_derived_advertiser_id_dsc_id` and `.../conversion_log_derived_advertiser_id_dsc_id` stop at `dt=2026-02-08` — superseded by `guid_and_conv_log_derived_advertiser_id_dsc_id`, but the old prefixes still exist with historical data. A GCS-directory sweep for a missing partition flags them as gaps; they are not. **Always cross-check a "missing partition" against the DAG's actual task list before calling it a hole** (this corrected an "11 missing models" read to the true 9).

## Spark Config Placement (Ryan, 2026-04-07)
- **`@compute.dataproc_batch(runtime_properties=...)`** — ONLY cluster infra settings: `dynamicAllocation.*`, `executor.cores`
- **`SparkSession.builder.config(...)`** — ALL Spark behavior settings: `sql.shuffle.partitions`, `sql.files.*`, `sql.parquet.*`
- **Avoid timeout/retry overrides** (`network.timeout`, `rpc.askTimeout`, `shuffle.io.*`) unless specifically needed — fewer settings = easier to port
- **Driver memory vs `shuffle.partitions` — the MapOutputTracker OOM (INC-018, 2026-08-15):** Dataproc Serverless applies its own **`spark.driver.memory=9600m`** default when the batch spec sets none. Map-status memory scales with **map tasks x reduce partitions**, so 9600m against `spark.sql.shuffle.partitions=5000` sits at the ceiling and tips over on heavier hours: `java.lang.OutOfMemoryError: Java heap space` in `map-output-dispatcher-*` threads, with the GCS reads logging SUCCESS first. **Triage signature: batches dying at a CONSTANT interval (12.0-12.6 min) while healthy runs vary around ~7 min. Constant death interval = resource ceiling; variable = data bug.** Fixed by [airflow-ti#1198](https://github.com/SteelHouse/airflow-ti/pull/1198) (merged 2026-08-15 21:53:28Z): `16g` + `memoryOverhead=4g` on that ONE DAG, NOT in the shared `get_config` every ipdsc job reads. **Headroom only** — map-status memory keeps growing with volume, so on recurrence the real lever is lowering `spark.sql.shuffle.partitions` for that job, not climbing driver memory again.
- **Duplicate-setting trap (AUDI-1194, 2026-08-07):** when a SQL prop is set in BOTH places, the **builder value wins at `getOrCreate`** — e.g. `intent_score_map.py` hardcodes `spark.sql.shuffle.partitions=4915` in the builder (line ~89) AND in `runtime_properties`; a decorator-only change is a **no-op**. Grep the model's builder before assuming a runtime_properties tweak took effect.

## Parquet Schema Gotchas (discovered TI-810)
- **guid_log `product` column:** STRUCT in parquet (not string like BQ silver view). Use `F.col("product").isNotNull()`, not `F.col("product") != "null"`
- **Parquet legacy LIST fields** (pmp, iab_categories, mntn_segments in augmentor_log): Schema is `struct<list: array<struct<element: T>>>`. `F.size(F.col("pmp.list"))` fails — Spark interprets as map subscript. Use `F.col("pmp").isNotNull()` as workaround.
- **Always check parquet schema** before writing aggregation models — BQ silver views and raw parquet have different type representations

## Backfill Patterns
- **Sequential, not concurrent** — concurrent `model_run.py` calls cause Dataproc batch ID collisions
- **Check existing output first** — `gsutil -q stat "${path}/_SUCCESS"` to skip already-done dates
- **Hourly models are slow to backfill** — each 2-hour chunk is a separate Dataproc Serverless cold-start (~5 min overhead per job). 31 days × 12 runs/day = 372 jobs = ~30 hours.
- **Copy dev→prod with gsutil** — `gsutil -m cp -r` from dev to prod bucket (Ryan confirmed approach)

## CI Pipeline
- **model_task_config.json must be regenerated** after any model config change — run `uv run python model_upload.py --dryrun` and commit
- **GitHub Actions runs model-upload-dryrun** — validates compilation and checks config freshness
- **Runtime read-split (non-obvious, INC-005 2026-07-29):** at task exec the operator reads the model **`.py` LIVE from `gs://mntn-data-archive-prod/ti_resources_v2/main/models/...`** (synced by the prod deploy on merge, ~1 min after), BUT the batch **`ttl` + spark `runtime_properties`** come from **`model_task_config.json` baked into the Astronomer DAG bundle**, which only refreshes on an `astro deploy` (the "Deploy to Prod" Action, `on: push` to main). So a **`.py` logic change applies on the next re-run right after merge**, but a **decorator-only change (ttl, `spark.sql.shuffle.partitions`) does NOT take effect until the bundle redeploys** past the merge — true for scheduled runs too. Verify a config change landed by checking the `Compute batch:` log line's `ttl`/properties on the next run, not just that the PR merged.

## Spark observability (SparkJobMonitor + event logs)
- **Event logs land in `gs://mntn-data-archive-prod/spark-events/`** as `app-<ts>-<n>.zstd` (self-identify the job via `spark.app.name`). Download gotcha: `gcloud storage cp` corrupts `.zstd` (crc32c/decompress gatekeeper → 0 bytes); use `gsutil -o "GSUtil:check_hashes=never" cp`. Bucket needs a TTL (Ryan, 2026-08-04). Consumed by the AUDI-1194 optimizer (`airflow_optimizer/`); see [[project_airflow_optimizer]] (RCA debugger = [[project_airflow_debugger]]). **PR #1169 (merged prod 2026-08-04, cef446a3) turns this on fleet-wide for the batch-operator models** — `ModelPysparkBatchOperator.execute` injects `spark.eventLog.enabled/dir/compress`, env-aware dir `gs://mntn-data-archive-{env}/spark-events`, kill switch Variable `SPARK_EVENT_LOG_ENABLED` (default `"true"`). Same block on the local runner (`utils_runner/dataproc.py`, env `SPARK_EVENT_LOG_ENABLED`).
- **Persistent History Server (PHS) ⊕ eventLog (AUDI-1191, 2026-08-04):** the **ipdsc/tpa** raw-batch path attaches a PHS via `peripherals_config.spark_history_server_config = persistent_history_cluster.get(env)` (`include/spark/data_source/ipdsc_emr_cluster.py` `get_env`:68). The PHS reads event logs from `gs://{temp_bucket}/*/spark-job-history` (`spark.history.fs.logDirectory`, `include/util/persistent_history_cluster.py:143`); temp_bucket prod=`dataproc-temp-us-central1-995798185124-svhwvc6j`, dev=`dataproc-temp-us-central1-411678625229-rfctkpug`. **So a PHS-attached Serverless batch ALREADY writes event logs to GCS** → the optimizer/crawler reads them at that prefix (no code change); do NOT remove the PHS to add a custom `eventLog.dir` (redundant + loses the Spark UI). You **cannot** set `spark.eventLog.*` on a batch that has a PHS attached — Dataproc manages those props and **rejects the override (400)**. The `@compute.dataproc_batch` operator models attach **no** PHS (0/88) → they need their own `eventLog.dir` (the archive bucket) — that is what PR #1169 shipped. PR #1169 kept the PHS on ipdsc/tpa (the eventLog-there commit was reverted, `d8b535c`); the managed-cluster workflow-operator eventLog is deferred to a follow-up PR.
- **PHS temp-bucket access (AUDI-1194, 2026-08-05):** `malachi@mountain.com` has **NO standing `storage.objects.list`** on `gs://dataproc-temp-us-central1-995798185124-svhwvc6j`. Interim read = the **`dataproc-debug` PAM bundle** (Compute Viewer + Dataproc Viewer + Storage Object Viewer; self-service ~1h, 18h max with L1 devops-squad approval; propagates ~30s after activation). Logs are per-batch at `gs://{temp_bucket}/<dataproc-batch-uuid>/spark-job-history/app-<id>.zstd` — SPARSE + scattered across thousands of unsorted per-uuid temp dirs → a flat prefix scan is infeasible; enumerate via `gcloud dataproc batches list/describe` (→ uuid) then read that uuid's `spark-job-history`. **Standing grant for the weekly cron (Slack/mountain-devops → Cristina): `roles/dataproc.viewer` on `mntn-prj-prod-00` + `roles/storage.objectViewer` on that temp bucket** — the PAM grant can't run the cron. See [[project_airflow_optimizer]].
- **Reading `driveroutput` under `dataproc-debug` PAM — the working recipe (INC-018, 2026-08-15).** The Airflow log for a failed Dataproc batch is always the boilerplate "Dataproc Agent reports job failure"; the real exception exists ONLY in the staging bucket `gs://dataproc-staging-us-central1-995798185124-d8mf0cme/.../driveroutput.*`, which 403s without the grant. Request, poll, read:
```bash
gcloud pam grants create --entitlement=dataproc-debug --location=global \
  --project=mntn-prj-prod-00 --requested-duration=14400s --justification="<incident>"
gcloud pam grants describe <grant-name> --format='value(state)'   # APPROVAL_AWAITED -> ACTIVE in minutes
gcloud storage cat gs://dataproc-staging-.../driveroutput.000000000
```
  A 4h (14400s) request went `APPROVAL_AWAITED` → `ACTIVE` within minutes on 2026-08-15. The bundle confers compute.viewer + dataproc.viewer + storage.objectViewer. **`gsutil` still 403s on a WILDCARD path** (a glob needs `storage.objects.list`), but **`gcloud storage cat` on the EXACT `driveroutput.000000000` object works** — take the exact object name from the batch's `stateMessage` / `driverOutputResourceUri`, do not glob. Same wall for the PHS temp bucket above; a standing grant must cover BOTH buckets.
- **`spark/utils/spark_job_monitor.py` (Victor's `SparkJobMonitor`)** — MCP-breadcrumb logging for Dataproc Serverless jobs (base64 to stdout → Cloud Logging, keyed by batch_id): `log_script_content(__file__)` (→ `MCP_SCRIPT_BASE64`, maps an app-id event log to its exact `.py`), `log_execution_plan(df)` (Physical/Optimized/Analyzed plans + missing-stats advisory, richer than the event log's `physicalPlanDescription`), `log_event_logging_status()` (warns if `spark.eventLog.enabled` is off + emits `MCP_EVENT_LOGGING_CONFIG_BASE64`), volume analysis. Ryan (2026-08-04): models should call the script + plan loggers so events are correlatable to scripts + carry the explain plan.

## Important Notes
- **NEVER modify DAG files casually** — they are production pipelines
- **NEVER push to main** — feature branches only, Ryan merges
- **model_run.py uses Dataproc Serverless** — not local Spark. Requires GCP auth. Writes to dev with branch suffix.
- **dags/model_task_config.json is auto-generated** — commit it after compilation
- **HLL sketches:** use ZetaSketch UDF JAR for BQ-compatible HLL++ merges (see CLAUDE.md §HLL)
- **Layer 1 stores raw counts ONLY** — ratios, percentages, and windows computed in Layer 2
- **Layer 1 `dt=` is event date** — Layer 2 `dt=` shifts to snapshot date (dt+1, consuming N prior days)
- **cost_impression_log has NO parquet archive** — use `bigquery_data.BQ` read_model
- **Partition date required for BQ reads** — same constraint as direct BQ queries (Ryan confirmed 2026-04-01)
- **Dataproc costs are monitored** — Zach Schoenberger watches GCP spend. Large backfills should be flagged to Ryan/team.
- **Naming standards doc now in git** — `docs/feature_store_naming_standards.md`, auto-published to Confluence via GitHub Action

## Deploy + runtime gotchas (learned AUDI-1191 dev validation, 2026-08-04)
- **Dev deploys AUTO on push to the `dev` branch** — Astro git-integration bumps the running DAG-bundle version (v1→v2→…, visible as "Dag Version" in the UI). The GH Actions "Deploy to Dev" workflow (`deploy_dev.yaml` → `deploy_gcs` + `deploy_model_to_gcs`) only copies code/model files to `gs://mntn-data-archive-dev/ti_resources_v2/{branch}/`; it does NOT deploy the bundle. There is **no manual `astro deploy`**. To test a branch in dev: cherry-pick/merge onto `dev` + push (auto-deploys). Prod deploys from `main`.
- **Prod deploys from `main` (confirmed AUDI-1191 PR #1169 merge, 2026-08-04):** merging a PR to `main` auto-triggers the **"Deploy to Prod"** GH Actions workflow and the prod DAG bundle picks up `main` (a real bundle refresh, unlike the dev-side "Deploy to Dev" action which only copies code to GCS). So a merge to main both syncs the model `.py` to prod GCS **and** redeploys the bundle (config/decorator changes take effect). Contrast: the dev `dev`-branch push auto-deploys the bundle via Astro git-integration.
- **Code root resolution:** `CodeRootLocation` (`include/models/operators.py`) reads `dags/current_branch.json` (`dev_branch_name`) — a **gitignored, local-only** file — else defaults to `dev` (prod → `main`). A bundle deployed with a stale local `current_branch.json` pins an old branch (e.g. `ds63`) whose GCS root may be gone → batch fails "File not found". A clean bundle → `dev` root.
- **Airflow 3 `airflow.sdk.Variable.get(key, default=..., deserialize_json=...)` uses `default=`, NOT `default_var=`.** The classic `airflow.models.Variable.get` used `default_var`; the SDK renamed it. Passing `default_var` → `TypeError` at task runtime.
- **Dataproc Serverless has a property ALLOWLIST** — it rejects unknown `spark.*` props with `INVALID_ARGUMENT: Attempted to set unsupported properties: [...]`. Confirmed unsupported: **`spark.eventLog.logBlockUpdates.enabled`**. Supported eventLog props: `spark.eventLog.enabled`/`dir`/`compress`. So RDD block-update (cache) events are NOT captured on Dataproc Serverless.
- **Dev Dataproc SA `airflow-ti-dev@mntn-prj-dev-00.iam.gserviceaccount.com` CAN write `gs://mntn-data-archive-dev/spark-events`** (confirmed via a live dev run writing `app-*.zstd`). Can't submit a dev batch from the CLI/`model_run.py` yourself — user lacks `iam.serviceAccounts.actAs` on that SA; the Astro runner has it.
- **`spark/` scripts sync to `gs://mntn-data-archive-prod/ti_resources/spark/` after a merge to main, with VARIABLE lag** (observed range **40s–2h15m**: INC-012 fix v1 merged ~21:55Z → deployed 00:09Z, ~2h15m; fix v2 merged ~02:1xZ → deployed 02:20Z, minutes; #1179 merged 16:22Z → deployed 16:23:09Z, ~40s, 2026-08-07). Always verify the GCS object timestamp AND grep the content before declaring a fix live or re-running — that check is how the failed 00:45Z batch was proven to have run the v1 code.

## GCS globStatus flat-glob gotchas (INC-012, 2026-08-06)
- **Hadoop `globStatus` on GCS resolves a glob by listing EVERY object under the first wildcard, then filtering client-side** (flat glob; `fs.gs.glob.flat.enable` default true). A mid-path glob like `region={east,west}/dt=X/hh=Y` therefore lists the entire prefix — for `gs://mntn-data-archive-prod/augmentor_log/` that is ~17M objects (measured 2026-08-06: one leaf hh= list = 18,401 names in 7s; a 50K-name crawl sample = 29s), and the list is latency-fragile → `SocketTimeoutException` killed `materialize_mntn_select` twice (INC-012). **Safe patterns:** literal partition paths (no wildcard → leaf-dir listing only), or a trailing-star-only glob whose FIRST wildcard is at the file level — `materialize_mntn_first_party`'s `yyyy/mm/dd/hh/*` is bounded and fine.
- **`globStatus` returns null (NOT an empty array) for a wildcard-free path that does not exist** — iterate it unguarded and you crash. Guarded in `spark/spark_utils.py` `get_paths` by PR #1176 (merged 2026-08-06, `3a97ea3`), the same PR that expanded mntn-select's `region=` glob to literal paths.
- **Spark's `basePath` read option makes Spark STAT the basePath — and on a marker-less GCS prefix the connector resolves that stat by LISTING the root** (`getFileInfoInternal` → `Error listing <root>` → SocketTimeout). On the ~17M-object `augmentor_log/` this times out exactly like the flat glob — it re-failed INC-012 ON the v1 fix (16:45 PT run, new code confirmed deployed). **Generalized rule: every code path that hands a huge root prefix to the GCS connector is a timeout surface — glob expansion AND basePath stat; sweep ALL call sites that touch the resource, not just the one in the traceback.** Fix v2 = drop `basePath` (airflow-ti#1177, merged + prod-verified 2026-08-06: hh=23 re-run succeeded in 7.4 min vs ~11.5 min historical healthy runs — the root-list overhead is gone, not just the failure).
- **Discriminator for whether `basePath` is needed: are partition columns actually SELECTED?** Here the parquet files carried `region` internally (the COLUMN_ALREADY_EXISTS warning is the tell) and the job selected only ip/pmp/partner_id — `basePath` was pure overhead.
- **Neither `augmentor_log/` nor `bidder_auction_events/` has a directory marker object;** bidder survived the same basePath stat only because its root lists fast enough.
- **INC-013 (2026-08-07): the same class recurred in the sibling readers — all 3 fixed by [airflow-ti#1179](https://github.com/SteelHouse/airflow-ti/pull/1179)** (merged + prod-verified 2026-08-07; dsid30 retry ~6 min vs ~19-min deaths). Scripts: `spark/fpa/dsid30_augmentor_log_processing.py`, `spark/auction_log_augmentor_process_gcs.py`, `spark/create_mntn_global_data_pyspark.py` — same pattern: literal region paths, drop `basePath`, existence guards. The third had its read wrapped in try/except, so a listing timeout SILENTLY degraded output: its 2026-08-07 00:24Z run went GREEN while the driver said "No data in augmentor_log", shipping `mntn_global_data/dt=2026-08-06` with zero augmentor rows.
- OBSERVED, UNCONFIRMED (2026-08-06): a `dataproc-debug` PAM grant was still readable ~3h past its nominal 1h window — unknown whether auto-extended or re-granted; don't design around it.

## `create_batch_id` shared-helper defect: batch-id attach on EVERY retry (INC-012 / INC-016 / INC-017)
- **`create_batch_id` (`include/util/dag_vars.py:31`) is `@task`-decorated, so it runs ONCE per dag run and caches the id in XCom.** Every downstream retry re-reads that same id, so `DataprocCreateBatchOperator` hits `Batch with given id already exists.`, **ATTACHES** to the existing (already FAILED) batch ("Attaching to the job ... if it is still running"), and inherits its terminal state in 6-18s. The alert on try 2+ therefore quotes try 1's error verbatim, and the declared retries are decorative.
- **AUTOMATIC retries are affected identically to a manual clear** (corrected 2026-08-15 — this section previously described only the manual-clear case, which understated it as an operator footgun rather than a defect that makes every try-1 failure a page).
- **The id FORMULA is not the bug.** `f"{_name}-{_dt}-{int(datetime.now().timestamp())}"` is unique per call; the call just happens once, upstream. Adding entropy to the formula fixes nothing. The fix is per-try entropy at the point of use (`task_instance.try_number`) — the pattern the in-repo `ipdsc_ds_*` tasks already use.
- **Five prod call sites across three DAGs:** `dags/targeting/materialize_mntn_first_party_dag.py:71` (`mntn-first-party`) · `dags/tpa_export/materialize_mntn_select.py:76` (`mntn-select`) · `dags/tpa_export/tpa_ipdsc_export.py:309/458/502` (`ipdsc`, `ipdsc-geo`, `tpa-export`). All three DAGs have paged: INC-012, INC-016, INC-017.
- **FIXED on the two materialize DAGs by [airflow-ti#1195](https://github.com/SteelHouse/airflow-ti/pull/1195)** — merged 2026-08-15 04:49:59Z by Ryan Kleck, prod-verified the same day. Batch id = the xcom_pull template plus `-{{ task_instance.try_number }}`. Proof is in the batch names: 04:45/04:50 ran `mntn-select-2026-08-15-1786769118` (no suffix, old bundle), 05:45 onward `...-1786772724-1`; six consecutive runs across both DAGs succeeded post-deploy.
- **(superseded 2026-08-17, fixed by #1196) The three `tpa_ipdsc_export` sites were deliberately held back.** That DAG's `already_exported` check tests for ANY blob rather than `_SUCCESS`, so making its retries real would let a retry skip on a PARTIAL write and report success (workspace `improvements_backlog.md` IMP-041 must ship before IMP-042). The materialize DAGs write `.mode("overwrite")` with no skip path, which is why they were safe to fix alone.
- **CLOSED 2026-08-17: [airflow-ti#1196](https://github.com/SteelHouse/airflow-ti/pull/1196) merged 23:47:04Z (`9f20749b`), all five call sites now fixed.** It shipped three things together: the `_SUCCESS` gate (IMP-041), the three remaining `tpa_ipdsc_export` batch ids (IMP-042), and a new guardrail (below). Both deploy halves were confirmed live within ~6.5 min of merge: the spark script in `gs://mntn-data-archive-prod/ti_resources/spark/exporter/export_tpa.py` and Astro bundle `2026-08-17T23:47:38Z` (`is_stale: false`). Dev-validated first: three distinct batches `tpa-export-2026-08-15-1786992151-{1,2,3}` doing real work, against a same-day control on unmodified `main` where try 2 created **no batch at all**.
- **`RetrySafeDataprocCreateBatchOperator` (`include/dataproc/serverless_operators.py`) is the guardrail** the reviewer asked for, on all three `tpa_ipdsc_export` Dataproc tasks. Because each try now submits its own batch, a worker killed WITHOUT `on_kill` strands the previous batch and two could write the same path. Before creating try N's batch it polls try N-1's (and earlier) until SUCCEEDED / FAILED / CANCELLED; `NotFound` means nothing was submitted, so it proceeds. Still non-terminal at `previous_try_timeout_seconds` (default 5400 = 90 min) → raises rather than starting a second writer. **New failure mode to expect in prod: a task that fails after 90 min waiting on a stuck predecessor.** 90 min is ~1.8x the worst batch prod has produced (below).
- **`on_kill` DOES cancel the batch** — `DataprocCreateBatchOperator.on_kill` calls `self.operation.cancel()` (provider `dataproc.py:214`). So a cleared task, a scheduler kill, or a dag timeout all cancel the in-flight batch (this is the INC-018 false-success mechanism). Only a non-graceful worker death (SIGKILL / pod eviction) strands a batch. That is the ONLY window the guardrail exists for, and it has never been observed.
- **PROD-VERIFIED 2026-08-18 (first run after the #1196 merge).** All three guarded tasks green on try 1 with the suffix live: `ipdsc-2026-08-17-1787020528-1` 30.2 min, `ipdsc-geo-...-1` 9.0 min, `tpa-export-...-1` 27.6 min. Output `ip_data_staging/2026/08/17/` = 5003 objects / 799.91 GiB with `_SUCCESS`. No guardrail wait lines, as expected on a clean run.
- **Export SIZE varies far more than the object count.** `ip_data_staging` is always **5003 objects** (5002 parts + `_SUCCESS`, fixed partitioning), so object count is a completeness check, NOT a volume check. Retained-window sizes: 08/11 297.6 · 08/12 438.5 · 08/13 602.1 · 08/14 342.6 · 08/15 255.9 · 08/16 237.8 · **08/17 799.9 GiB**. 08/17 is 1.33x the prior max and ~3.4x the low, but the band already swings 2.5x day to day, and nothing in #1196 changes what gets exported (it only touches batch ids and the skip gate; `force=false` with no `_SUCCESS` runs a full export either way). Treat size alone as weak evidence; the earlier "225-491 GiB" band recorded from INC-016 was a 5-day sample and is too narrow.
- **Prod duration baselines for the three guarded tasks** (last 20 runs, 2026-08-17): `ipdsc` median 21.5 min / max 48.9 · `ipdsc_geo` median 10.9 / max 11.7 · `tpa_export` median 13.2 / max 30.1. Use these before changing any timeout on this DAG.
- **The `_SUCCESS` gate is the thing that actually stops a double write, not the batch id.** Verified in dev: a fresh batch against a completed export ran 1m23s and wrote NOTHING (output stayed 559 objects / 12.79 GiB, `_SUCCESS` kept its original timestamp) versus 3m08s for the real export. This is why the gate and the batch-id change had to ship together rather than in sequence.
- **Manual re-run while any site is unfixed:** clear `create_batch_id` **WITH downstream** so a fresh id is minted; clearing only the operator task re-attaches. Deleting the Dataproc batch also frees the id, but it **destroys `driveroutput` — the only copy of the root cause — so capture that first.** Same class as the `tpa_mntn_id_export` batch-id trap (runbook INC-001).

## Config re-run trap: `create_batch` caches the WHOLE batch spec (INC-018, 2026-08-15)
Sibling of the `create_batch_id` defect above, and the costlier one. After merging a Dataproc **config** fix (driver memory, ttl, spark props), **clearing the Dataproc task does NOT pick it up — and neither does the UI's "Run with latest bundle version".** Three independent caches sit in front of the config; the bundle version controls only one of them.

| Task | Caches in XCom | Clearing it gets you |
|---|---|---|
| `create_batch` (`@task`) | the **entire batch spec including `runtime_config`** (driver memory, ttl, spark props) | the NEW config |
| `create_batch_id` (`@task`) | just the batch id string | a fresh batch id (unnecessary post-#1195; `try_number` already mints one) |
| `materialize` / the operator task alone | nothing | the OLD cached spec, re-submitted |

- **Proof (2026-08-15).** The dag run WAS on the post-merge bundle (`bundle_version=2026-08-15T21:54:01Z`, merge 21:53:28Z) and the task still submitted `driver.memory=9600m` — because `create_batch` had last run at 12:45Z and its cached XCom carried **no `driver.memory` key at all**, so Dataproc applied its 9600m default.
- **Fix = clear `create_batch` WITH downstream.** Verified: `create_batch` try 2 at 22:16:32Z → the batch reported `16g`/`4g` → 7.1 min → `hh=11` landed 77 objects / 6.15 GiB. A freshly TRIGGERED run with `{"dt": ..., "hhs": [...]}` works too, since it rebuilds every upstream task.
- **Generalises to EVERY DAG of this shape:** an `@task` that builds an artifact consumed by a downstream operator pins that artifact for the whole dag run. Re-running the consumer replays the cached artifact — you must clear the PRODUCER. Bundle version is necessary, not sufficient: [[feedback_astronomer_clear_with_latest_bundle]].
- **⚠ NEVER clear a task whose Dataproc batch is still RUNNING — it produces a FALSE SUCCESS.** The clear cancels the in-flight batch, and Airflow records that try as **SUCCESS with zero output**. Observed: batch `mntn-select-2026-08-15-1786831127-3` went `CANCELLED` at 22:01:24Z while Airflow showed try 3 green with a 2:28 duration, and `hh=11` stayed empty. **Heuristic: a `materialize` success under ~3 min is a lie** (healthy ~7 min). Confirm the `hh=` partition in GCS, never the green tick. Same family as [[feedback_validated_is_not_correct]].

## DAG-authoring gotchas confirmed while fixing #1195 (2026-08-15)
- **An `XComArg` passed into a templated field is just an xcom_pull template.** Verified by DagBag parse on unmodified `main`: `batch_id=create_batch_id(...)` renders as `{{ task_instance.xcom_pull(task_ids='create_batch_id', dag_id='...', key='return_value') }}`. So replacing the XComArg with an explicit `{{ ti.xcom_pull(...) }}` string preserves the VALUE — **but it silently DROPS the implicit dependency edge**, so you must re-add `create_batch_id >> <downstream>` by hand. Check `dag.get_task('<t>').upstream_task_ids` before and after (here `['create_batch','create_batch_id']` both ways). `batch_id` IS in `DataprocCreateBatchOperator.template_fields` (confirmed in the installed provider), which is what makes the string form work at all.
- **Airflow 3 forbids the metadata-DB ORM from inside a task.** `create_session()` / any
  `session.query(...)` in task code raises `airflow session use is forbidden in this context`
  (Astro executor, runtime 3.1-9). Parsing the DAG bundle with `DagBag` from a task is fine; the
  DB is not. Anything needing DagModel state (paused, next run, etc.) has to go through the REST
  API with a deployment token. Verified in prod 2026-08-21 by `spark_optimizer_daily`.
- **A task pod gets `default_task_pod_cpu` / `default_task_pod_memory` unless the DAG sets
  `executor_config`.** On the `ti` prod deployment that is **0.25 CPU / 0.5 Gi**, which is small
  enough to turn a 3-minute job into 19. Check `astro deployment inspect <id>` before assuming a
  task has room.
- **`Deploy to Prod` failing on an unrelated job blocks the DAG bundle from reaching Astro.** On
  2026-08-21 a merged DAG did not appear until a second, green run: `current_tag` stayed on the
  previous deploy. If a merged DAG is missing from the deployment, check the workflow run before
  the DAG.
- **Jobs that install with `uv` must not set `cache: 'pip'` on `actions/setup-python`.**
  `~/.cache/pip` is never written, so the `Post Setup Python` cache-save step fails the job after
  every real step has passed. Broke `Deploy to Prod` and `Models PR checks` fleet-wide from
  2026-08-21 until airflow-ti#1213. The failure looks like a deploy failure and is not one.
- **Triggering a DAG through the REST API: always send an explicit `logical_date`.** `POST /api/v2/dags/<id>/dagRuns` with `{"logical_date": null}` creates a run with **no data interval at all**, so `{{ ds }}` raises `KeyError('ds')` and the task dies in ~5s. Verified on `spark_optimizer_daily` 2026-08-25: the null-date run failed instantly, the same trigger with `"logical_date": "2026-08-25T09:00:00Z"` succeeded. The 9am schedule is unaffected — a scheduled run always carries one. Resolve the base with `astro deployment inspect <id> --key metadata.airflow_api_url` (it returns the host WITHOUT a scheme, so prefix `https://`) and auth with the deployment token from the login Keychain (`security find-generic-password -a spark-optimizer -s astro-deployment-token -w`).
- **Airflow 3: `logical_date` is NULLABLE for manual and asset-triggered runs**, so `{{ logical_date.format(...) }}` in a template raises on a hand-triggered run. Never use it in a DAG that supports manual triggering — these DAGs carry `dt`/`hhs` params exactly for that. Caught in review before merge. Same nullability that forces the log puller to window on `start_date_gte/lte`: [[reference_airflow_log_puller]].

## Offline DAG-parse validation (no live Airflow, no pytest)
Validate a DAG edit by parsing it into a DagBag locally:
```bash
AIRFLOW_VAR_ENV=prod AIRFLOW_VAR_ARCHIVE_GCS_BUCKET=<placeholder> AIRFLOW_VAR_ARCHIVE_S3_BUCKET=x \
PYTHONPATH=$HOME/Developer/work/mntn/airflow-ti AIRFLOW_HOME=$HOME/Developer/work/mntn/airflow-ti \
$HOME/Developer/work/mntn/airflow-ti/.venv/bin/python -c "
from airflow.models.dagbag import DagBag
b = DagBag(dag_folder='dags', include_examples=False)
print(b.import_errors)
print(b.dags['<dag_id>'].get_task('<task>').upstream_task_ids)"
```
- **Two traps.** (1) DAGs call `Variable.get` at PARSE time, which fails with a `SUPERVISOR_COMMS` ImportError unless the value is supplied as an `AIRFLOW_VAR_<KEY>` env var. (2) `include/...` imports fail unless the repo root is on `PYTHONPATH`.
- The venv ships **no pytest**, so run the DagBag directly rather than the repo's `tests/`.
- **ALWAYS run the same parse against unmodified `main` as a control.** The bag carries pre-existing import errors; without the control you will attribute them to your change (and a rendered-template diff is only meaningful against the baseline render).

## Cross-DAG ExternalTaskSensor: skip-as-failure gotcha (INC-011, 2026-08-05)
- **An `ExternalTaskSensor` with `"skipped"` in `failed_states` HARD-FAILS (fast, ~5s, single poke) on a BENIGN upstream skip** and raises the **IDENTICAL** `ExternalTaskFailedError: Some of the external tasks [...] failed.` message it raises for a true failure. So **a log regex cannot tell a skip from a fail** — you must resolve the external task's ACTUAL final state (`skipped` vs `failed`/`upstream_failed`) via the REST taskInstances endpoint before judging it.
- **The provider's documented remedy:** move `skipped` out of `failed_states` into `skipped_states=[State.SKIPPED]` — then a legitimate no-data upstream skip makes the sensor **SKIP** (propagates the skip) instead of FAIL+page. Shipped for both `wait_fpa` DAGs in airflow-ti#1175 (AUDI-1195). Only apply where an upstream skip is genuinely benign (a producer short-circuit on missing partner data); where a skip means a real break (e.g. `keyword_ddp_reporting`), keep it in `failed_states`. Incident detail: runbook §2/§3 INC-011.

**Airflow 3 REST: `page_limit` / `page_offset`, not `limit` / `offset` (2026-08-24).** The
`POST /api/v2/dags/~/dagRuns/~/taskInstances/list` body is a **strict** schema and returns
`422 extra_forbidden` on the query-param names. Window it on `start_date_gte`/`start_date_lte`,
not `logical_date`, which is nullable for asset and manual runs. Also: the deployment's API base
is `https://<deployment-id>.iq.astronomer.run/<suffix>/api/v2` — take it from
`astro deployment inspect ... --key metadata.airflow_api_url`, do not construct it from the id.
Found the first time a real deployment token existed; no mocked test can catch it.

**A green "Deploy to Prod" does NOT mean the DAG bundle refreshed — CONTRADICTION, 2026-08-24.**
Line above (recorded 2026-08-04 from the #1169 merge) says merging to `main` triggers the workflow
*and* refreshes the prod bundle. Direct observation on the #1214 merge says otherwise, and both
claims are kept because only one has been tested against the workflow file:

- **Evidence for the new claim:** `deploy_prod.yaml` calls only `deploy_gcs.yaml` (uploads the
  `spark/` folder to `gs://mntn-data-archive-<env>/ti_resources`) and `deploy_model_to_gcs.yaml`.
  **Neither pushes a DAG bundle.** After the #1214 merge (`504fe947`, 18:59Z) the workflow went
  green, yet 25+ minutes later `bundle_version` was still `2026-08-21T20:02:24Z` and
  `airflow_debugger_daily` was absent from `/dags` with **zero import errors**.
- **Why the old claim looked true:** the 2026-08-21 bundle stamp (20:02:24Z) sits one minute after
  that day's `deploy_prod` run (20:01). That is consistent with causation *and* with both reacting
  to the same push, which is the reconciling hypothesis: **prod refreshes via Astro's git
  integration on `main`, the same mechanism dev uses on `dev`** — usually fast, so it is easy to
  credit the Actions run for it.
- **The check that settles it either way:** `GET /api/v2/dags/<any_dag>` returns `bundle_name`
  (`main`) and `bundle_version` (an ISO timestamp). **A DAG is live when that timestamp moves past
  your merge, not when CI is green.** `GET /dags?dag_id_pattern=<x>` confirms registration, and
  `GET /importErrors` distinguishes "not deployed" (0 errors, absent) from "deployed but broken".

**RESOLVED the same evening — the bundle is stamped at merge time but ADOPTED with a lag.** The
new bundle turned out to be stamped `2026-08-24T19:00:21Z`, ~1.5 min after the 18:59Z merge, yet
the running deployment kept serving `2026-08-21T20:02:24Z` for roughly **25-40 minutes**, during
which `/dags` had no `airflow_debugger_daily` and `/importErrors` was empty. So:

- **`deploy_prod.yaml` still does not push DAGs** (that part of the finding holds): it only calls
  `deploy_gcs.yaml` and `deploy_model_to_gcs.yaml`. Astro's git integration on `main` creates the
  bundle, which is why its stamp tracks the merge, not the workflow.
- **The lag is adoption, not creation.** Do not conclude from a stale `bundle_version` that the
  deploy failed; it usually means the deployment has not switched over yet. The 2026-08-04 note is
  therefore right in effect (a merge does refresh the prod bundle) and wrong in mechanism (the
  Actions run is not what does it).

**So: never report a DAG as shipped on the strength of a merge plus a green deploy — poll until
`bundle_version` moves past the merge, and expect tens of minutes.** Same class as the `spark/`
sync lag recorded above (40s to 2h15m observed).

**A newly deployed DAG arrives PAUSED.** `airflow_debugger_daily` registered with
`is_paused: true`; it will not run until someone unpauses it. With `catchup=False` and a
`0 17 * * *` schedule, `next_dagrun_logical_date` was already in the past on arrival
(`2026-08-23T17:00`), so unpausing fires a run immediately for the most recent closed interval
rather than waiting for tomorrow. Budget for that before unpausing anything expensive.

**`airflow_pull.sh` was silently broken for any unfiltered pull until 2026-08-25.** With `set -u`, `"${PY_ARGS[@]}"` on an EMPTY array is an unbound-variable error on this bash, so `--date <D>` alone died with `PY_ARGS[@]: unbound variable` while `--date <D> --dag <x>` worked. Every backfill anyone ever ran had happened to pass `--dag` or `--tag`, so the break went unnoticed and five days of corpus were never pulled. Fix is `${PY_ARGS[@]+"${PY_ARGS[@]}"}`. **Any bash array expansion under `set -u` needs that guard**, and the bug only shows on the code path nobody exercises.

**A green `Deploy to Prod` does NOT mean your code is in prod, and the gap can be TWELVE HOURS (2026-08-26).** Two PRs merged at 03:42Z with both deploy workflows green. At 16:03Z a live run still loaded `/tmp/airflow/dag_bundles/astro/main/dags/2026-08-25T23:15:06.../` — the PREVIOUS day's bundle. A new bundle finally appeared stamped 15:55:02Z and was adopted around 16:18Z. Re-triggering the deploy by `workflow_dispatch` got no credit: the bundle predates the dispatch.

**Two separate delays, do not conflate them.** (a) CREATION: merge to new bundle stamp — normally under a minute (#1217: 23:14:22 merge, 23:15:06 bundle), but 12 hours on 2026-08-26, cause unexplained. (b) ADOPTION: bundle stamp to the deployment serving it — 8 to 40 minutes observed.

**The only check that settles it is the DagBag line in a real task log**, because `bundle_version` on the DAG record is the version it was last PARSED from: a PR touching only `include/` changes no DAG file, so no re-parse happens and the field stays stale even after the bundle refreshes. `GET /dags/<id>` and `/dagVersions` both mislead here.

```bash
# fire a run, then grep its log:
#   "Filling up the DagBag from /tmp/airflow/dag_bundles/astro/main/dags/<BUNDLE>/..."
```

**Never report a merge as shipped on a green workflow alone.** Confirm the bundle path in a run that executed after it.

## PR 1209 review facts (household TPA export → MemDB, 2026-08-26)
[airflow-ti#1209](https://github.com/SteelHouse/airflow-ti/pull/1209) (Ryan, branch ID-431, OPEN): rewrites `tpa_mntn_id_export` to read prod `ip_data` JSONL, join the identity graph, keep ONE row per household (min-ip tie-break, no cross-IP merge), and write `hh_id_data/` for MemDB; deletes `intent_score_household_map`. Reviewed 2026-08-26, 9 inline comments posted ([review 5035551769](https://github.com/SteelHouse/airflow-ti/pull/1209#pullrequestreview-5035551769)); majors: empty-cats trim no-op, allowlist SQL narrower than the tested Python walker, household pick before DS trim, PACC bare-except, geo_version silent "0" default. Approval held for Ryan's response. Facts verified during the review:
- **ip_data export rows carry ONE `data_sources` entry per configured DS id with `cats` coalesced to `[]`** (`spark/exporter/export_tpa.py` `_final_dataframe`), so `size(data_sources) > 0` on raw rows is a near no-op — filter on cats content, not entry count.
- **The Dataproc batch operator `timeout` param is a per-gRPC-request timeout, NOT a task wall clock** (provider 17.1.0: `wait_for_batch` polls to a terminal state with no overall deadline). The batch `ttl` is the real killer. The ipdsc "ttl plus headroom" timeout comments encode the wall-clock misconception.
- **`ModelPysparkBatchOperator._cancel_previous_tries`** (pre-existing, `include/models/operators.py`) lists live batches by dag/task/run labels, cancels and waits ≤300s (raises if one stays live) before submitting — the concurrent-writer guard on the model-operator path, sibling of ipdsc's `RetrySafeDataprocCreateBatchOperator`.
- **Airflow 3.0.3 merges `dag_run.conf` into params AT CONTEXT BUILD with validation** (`process_params(suppress_exception=False)` → `ParamValidationError` on type mismatch), so conf does NOT bypass `Param` schemas; a wrong-typed conf fails the task loudly before any template renders.
- **`load_graph_ids(dedupe=True)` is 1:1 ip→household for IPv4** (the graph assigns each current IPv4 to one household; dedupe is a no-op there), so an ip join against the graph has no fan-out skew surface.

