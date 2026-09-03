---
doc_type: ticket
title: "AUDI-1278: Label python-client BigQuery jobs for cost attribution"
status: backlog
date: 2026-09-02
summary: "Add airflow-dag/airflow-task labels to python-client BQ submits so every job is attributed"
result: "not started"
question: "Which submitters produce the roughly 600 unlabeled BigQuery jobs a day (1,185 slot-hours), and does adding airflow-dag and airflow-task labels in the python client attribute them?"
framing_state: locked
---

# AUDI-1278: Label python-client BigQuery jobs for cost attribution

**Jira:** https://mntn.atlassian.net/browse/AUDI-1278
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Which submitters produce the roughly 600 unlabeled BigQuery jobs a day (1,185 slot-hours), and does adding airflow-dag and airflow-task labels in the python client attribute them?
- **Goal (why / the decision):** The 2026-09-02 finding that the ledger's unattributed bucket is empty means the 1,185 figure may lie outside the airflow-launched set; settle that first. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** A table in outputs/ of unlabeled jobs by principal, client and query fingerprint joined against the ledger population, PRs adding labels where the submitter is ours, and the unattributed share down on the Mode optimizer BQ report.
- **Approach (how):** JOBS_BY_PROJECT via bq_run.sh where labels are missing, grouped by user_email and job pattern; join to the optimizer ledger population; add labels via the python client's job_config.labels where airflow-ti submits.
- **What would change the answer:** The unlabeled jobs are not submitted from airflow-ti at all (Mode, humans, another service); then the deliverable is the attribution table and an owner hand-off, no PR.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

About 600 BigQuery jobs a day (1,185 slot-hours) show up with no owner on the [cost dashboard](https://app.mode.com/mntn/reports/e81786de8403), so a third of BQ spend cannot be traced to a pipeline.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** jobs submitted through the python client carry no labels, while Airflow-submitted jobs are labeled with their DAG and task automatically.

**Task:** add airflow-dag and airflow-task labels where the python client submits BQ jobs, so every job is attributed.

**Done-when:** the unattributed share drops on the [optimizer BQ report](https://app.mode.com/mntn/reports/e81786de8403).

## 3. Plan of Action
Planning wave 2026-09-02 (plan only, nothing executed beyond read-only verification). Another agent executes this in a per-ticket worktree of airflow-ti on branch `AUDI-1278`; the dispatcher commits, runs the gauntlet and opens the PR.

### 3.0 What the planning wave established (verified, cite these, do not re-derive)
- **The "unattributed" figure is fleet-service-account jobs without labels, not outsiders.** `include/spark_optimizer/bq_profile.py` (`PROFILE_SQL`) reads `dw-main-bronze.region-us-central1.INFORMATION_SCHEMA.JOBS_BY_PROJECT` filtered to `user_email IN (airflow-ti-prod@, airflow-camperbid-prod@)` and buckets `airflow-dag=''` as `unattributed`. So every one of those jobs was submitted by one of our two Airflow deployments. The §0 hypothesis "jobs outside the airflow-launched set" is refuted.
- **The ledger's unattributed bucket is empty BY DESIGN, not because the jobs vanished.** `bq_profile.reports()` never attaches a finding to the unattributed report ("a fix cannot be filed against a job no dag will admit to"), and `ledger.record()` skips any report with no findings (`include/spark_optimizer/ledger.py` line 255, `if not getattr(r, "findings", None): continue`). Pinned by `include/spark_optimizer/tests/test_bq_profile.py::test_heavy_task_is_a_finding_and_unattributed_is_not`. The bucket lives only in the daily GCS report: `gs://mntn-data-archive-prod/optimizer/optimizer_bq_<date>.md` shows unattributed 592 jobs / 1,009.8 slot-h (08-31), 620 / 977.6 (09-01), 527 / 779.7 (09-02). Copies in `outputs/optimizer_bq_2026-0{8-31,9-01,9-02}.md`; prod ledger copy in `outputs/prod_optimization_ledger.jsonl` (18 `surface=bq` rows, all `bq_heavy_task:*` on bos__spend / intent_score_threshold_v4 / category_taxonomy, zero unattributed rows).
- **Who submits them (2026-09-01, `outputs/audi_1278_jobs_by_principal_2026_09_01.csv` and `outputs/audi_1278_unlabeled_fingerprint_2026_09_01.csv`, from `queries/audi_1278_jobs_by_principal_1d.sql` and `queries/audi_1278_unlabeled_fingerprint_1d.sql`, 4.86 GB each dry-run):**
  - `airflow-camperbid-prod@`: 434 SELECT + 10 other + 7 LOAD = **451 jobs, 977.6 slot-h, 53.6 TiB** with no airflow labels. Job ids are bare UUIDs, destination is the anonymous dataset `_54eb8ea0...`, statement heads match Spark scripts in `SteelHouse/airflow-camperbid` (main `707d739`): `DATE_TRUNC(sp.time, MINUTE) ... sp.campaign_id` = `spark_scripts/bos/campaign_flight_end_cost.py` (92 jobs, 463.5 slot-h), `... c.campaign_group_id` = `spark_scripts/bos/campaign_group_flight_end_cost.py` (92, 457.7), `pmd.private_marketplace_deal_id` = `spark_scripts/bos/sum_by_private_marketplace_by_hour.py` (93, 23.2), `cil.campaign_id, DATE_TRUNC(cil.time, HOUR)` = `spark_scripts/bos/campaign_utc_yesterday_costs_impressions_by_hour.py` (92, 2.0), `SELECT campaign_id, SUM(bids)` (24, 29.3, intent_score_threshold / campaign_avg_cpm family). All go through `spark_scripts/utils/util_spark.py::bigquery_load_query` = `spark.read.format("bigquery").option("viewsEnabled","true").option("materializationDataset", "temp__<hex>").option("query", sql)` on Dataproc Serverless batches created by `dag_utils/google.py::run_dataproc_serverless` running as `airflow-camperbid-prod@` (confirmed with `gcloud dataproc batches list`, batch labels `dag_id=bos__spend`). The Spark-BigQuery connector runs the query job itself, so no Airflow operator ever sees it. `materializationDataset` is deprecated since connector 0.42.1, hence the anonymous-dataset destination. Residual python-client jobs on the same SA: `perml.campaign_* WHERE run_date = @run_date` reads, a `-- AUTOTOF` script, and the `external.camperbid_prod__hhst_v#__campaign_bucket` MERGE, about 15 jobs / <1 slot-h a day; their source file was not found by code search in airflow-camperbid or olympus (owner's grep).
  - `airflow-ti-prod@`: 163 SELECT + 3 CTAS + 3 misc = **169 jobs, 0.0 slot-h**. Query heads `attr.url_paths_#_#d_#` (paged reads + CTAS) = `dags/attribution/url_pattern_pipeline.py` helpers called from `url_pattern_identification.py` and `dlv_pattern_identification.py`; `core_advertisers_x_feat...` = `tmobile_blocked_ip_workflow.py` / `tmobile_blocked_guids_workflow.py` `ADVERTISER_QUERY`. All are `BigQueryHook.get_client().query()` (plain google-cloud-bigquery client, no labels) or `BigQueryHook.get_df()` (pandas_gbq.read_gbq, no labels).
  - Everyone else in the project (dagctl 11,600 slot-h MERGE, cds-dpp 13,456, compute@ 41,278 LOAD_DATA, mode-analytics 4,992, humans) is outside the fleet filter and outside this ticket.
- **Where labels come from today:** provider `apache-airflow-providers-google` 22.4.0 (local venv; prod image astro runtime 3.1-9 pins `>=22.0.0`) `operators/bigquery.py` `BigQueryInsertJobOperator._add_job_labels` (lines 2427-2441): `airflow-dag = dag_id.lower()`, `airflow-task = task_id.lower().replace(".", "-")`, applied only when both match `LABEL_REGEX = ^[\w-]{0,63}$`. `BigQueryHook.get_client()` (hooks/bigquery.py:257) returns a bare `google.cloud.bigquery.Client` with no default labels; `hook.labels` (connection extra) is only applied on the cursor `run_query` / `insert_job` paths. `BigQueryHook.get_df(sql, **kwargs)` forwards kwargs to `pandas_gbq.read_gbq`, whose `configuration` parameter is the BigQuery JobConfiguration resource (top-level `labels` is a valid key).
- **The Mode "optimizer BQ report" cannot show the unattributed share as built.** Report `e81786de8403` query "BigQuery cost by task" (token `3ead7301daa8`, read via the Mode API) is `SELECT ... FROM mntn-prj-prod-00.optimizer.optimization_ledger WHERE surface = 'bq'`, i.e. findings-only ledger rows. The Jira Done-when ("unattributed share drops on the optimizer BQ report") needs a measurement surface decision (step 6).
- **Validation path in airflow-ti:** `tests/test_url_pattern_pipeline.py` exists (22 tests: 20 pass, 2 pre-existing failures on main `825b07e`: `test_url_and_dlv_dag_calls_pass_shared_location[url|dlv]`, an AST check that the DAG files pass `location=BQ_LOCATION`). CI does NOT run root `tests/` (`.github/workflows/pr_model.yaml` runs `tests/models` only; the debugger/optimizer workflows run their own packages), so the local run is the gate: `PYTHONDONTWRITEBYTECODE=1 /Users/malachi/Developer/work/mntn/airflow-ti/.venv/bin/python -m pytest tests/test_url_pattern_pipeline.py -q -p no:cacheprovider`. DAG code imports shared code as `from include.<pkg> import ...`; Airflow 3 task context is `from airflow.sdk import get_current_context` (already used in `dags/gcp_pixel_page_view_signal_*_backfill_workflow.py`).
- **Owner of the camperbid side:** `SteelHouse/airflow-camperbid/.github/CODEOWNERS`: `* @SteelHouse/pacing @SteelHouse/performance-ml`; `dag_utils/` has no specific rule, so both teams own the change. Deployment: Astro runtime 3.2-5, prod SA `airflow-camperbid-prod@mntn-prj-prod-00`, dev SA `airflow-camperbid-dev@mntn-prj-dev-00` (dev jobs also bill in dw-main-bronze: 270 dev SELECTs seen 09-01).
- **Connector doc (README, GoogleCloudDataproc/spark-bigquery-connector, fetched 2026-09-02):** option `bigQueryJobLabel` "Can be used to add labels to the connector initiated query and load BigQuery jobs. Multiple labels can be set."; "Options can also be set outside of the code ... prepend the prefix `spark.datasource.bigquery.` to any of the options." Current Spark 3.5 connector 0.45.0; Dataproc Serverless runtime 2.3 bundles the 0.42 line (the cluster path in `dag_utils/google.py` pins `spark-3.5-bigquery-0.42.2.jar`).

### 3.1 Numbered steps
1. **Attribution table (Objective deliverable 1).** For each day 2026-08-26..2026-09-01 run `queries/audi_1278_jobs_by_principal_1d.sql` and `queries/audi_1278_unlabeled_fingerprint_1d.sql` with the date literals swapped (`bash .claude/scripts/bq_run.sh --ticket AUDI-1278 --project_id=dw-main-bronze --dry_run ...` first; each day is 4.86 GB, one 7-day query is 12.3 GB and breaks the 5 GB cap, so run per day). Union in python into `outputs/audi_1278_unlabeled_jobs_by_submitter.csv` with columns `date, user_email, job_type, statement_type, query_head, source_file, owner, jobs, slot_h, tib_billed`, mapping `query_head` to source file with the table in 3.0. Join to the ledger population: `outputs/prod_optimization_ledger.jsonl` `surface=bq` `dag_id`s (bos__spend, intent_score_threshold_v4, category_taxonomy); the expected reading is that the camperbid unlabeled jobs belong to `bos__spend`, a DAG the ledger already tracks through its labeled operator tasks, so labeling moves ~975 slot-h/day into an existing DAG rather than creating a new one. Ship as the branded `.xlsx` via `lib/mntn_xlsx.py` (`MntnWorkbook`, DRAFT) to `My Drive/Tickets/AUDI-1278/audi_1278_unlabeled_bq_jobs.xlsx`, ranked by slot-hours descending, one tab per submitter plus a summary tab; read `documentation/docs/xlsx_deliverable_standard.md` first.
2. **airflow-ti helper (ours).** New file `include/util/bq_job_labels.py`: `def airflow_job_labels(context: dict | None = None) -> dict[str, str]` that reads `airflow.sdk.get_current_context()` when `context` is None, returns `{"airflow-dag": dag_id.lower(), "airflow-task": task_id.lower().replace(".", "-")}` when both values match `re.compile(r"^[\w-]{0,63}$")`, else `{}`; returns `{}` when no task context exists so module import and local tests never fail. This mirrors the provider's `_add_job_labels` exactly so `bq_profile.py` groups operator-submitted and client-submitted jobs under the same `airflow-dag` key. One-line docstring, no comments. Unit test `tests/test_bq_job_labels.py`: formatting, the dot-to-dash rule, the regex guard, the no-context fallback.
3. **airflow-ti call sites (ours), exactly these files:**
   - `dags/attribution/url_pattern_pipeline.py`: add `labels: dict | None = None` to `run_query_to_destination()` and `iter_destination_rows()`; resolve `labels = airflow_job_labels() if labels is None else labels` inside the function body (task context exists only at run time) and pass `labels=labels` into all three `bigquery.QueryJobConfig(...)` constructions (the `as_script` branch, the destination branch, the paging branch). Callers in `url_pattern_identification.py` and `dlv_pattern_identification.py` stay untouched so the AST call-count test keeps its expectations.
   - `dags/attribution/tmobile_blocked_ip_workflow.py` and `dags/attribution/tmobile_blocked_guids_workflow.py` (`fetch_advertiser_ids`): `client.query(ADVERTISER_QUERY, job_config=bigquery.QueryJobConfig(labels=airflow_job_labels()), project=BQ_PROJECT_ID, location=BQ_LOCATION)`; add `from google.cloud import bigquery` where missing.
   - `dags/attribution/set_gaclid_enabled_flag.py` (`fetch_aids`), `dags/attribution/blocked_ip_addresses_export.py` (`fetch_icloud_ips`), `dags/attribution/blocked_guids_export.py`: `bigquery_hook.get_df(sql, configuration={"labels": airflow_job_labels()})`.
   - Extend `tests/test_url_pattern_pipeline.py`: with a monkeypatched `airflow_job_labels` returning a fixed dict, assert `client.query_calls[0]["job_config"].labels` equals it for both `run_query_to_destination` branches and the paging query. Baseline is 20 pass / 2 pre-existing failures; the PR must not change that count and must not touch the two failing AST tests (scope).
4. **Validate before the PR.** (a) pytest as in 3.0 plus the new test module; ruff per the repo's pre-commit config. (b) Dev run: deploy the branch to the airflow-ti dev deployment (`cmcvcbd3j03vk01p91ksvm1vd`) through the repo's `deploy_dev.yaml` path, trigger `url_pattern_identification` (or `tmobile_blocked_ip_export_dataproc`) once on dev, then `bq_run.sh --project_id=dw-main-bronze` on `JOBS_BY_PROJECT` with `user_email = 'airflow-ti-dev@mntn-prj-dev-00.iam.gserviceaccount.com'`, `creation_time` bounded to the run, `LIMIT 100`: every python-client job from that run carries `airflow-dag`/`airflow-task`. If dev deploy is not available to the agent, state it and fall back to unit tests plus prod verification on the first post-merge `optimizer_bq_<date>.md` (the sweep reads labels straight from JOBS_BY_PROJECT, so the effect shows within one day).
5. **Owner hand-off for airflow-camperbid (not ours).** Write `artifacts/audi_1278_camperbid_handoff.md` containing the exact diff plus a send-draft (lead with the ask, §9 rules):
   - `dag_utils/google.py::run_dataproc_serverless`, `runtime_config.properties`, add `"spark.datasource.bigquery.bigQueryJobLabel.airflow-dag": "{{ dag.dag_id | lower }}"` and `"spark.datasource.bigquery.bigQueryJobLabel.airflow-task": "{{ task.task_id | lower | replace('.', '-') }}"` (the `batch` dict is a template field; its `labels` block already renders `{{ dag.dag_id }}`).
   - `dag_utils/google.py::DataprocConfig.asJson`, `pyspark_job.properties`, the same two keys (covers the cluster-based DAGs using `asOperator`: win_rate, intent_score, bid_price_log_aggregation, media_plan_change_log_sync, media_plan_regeneration, network_performance_metrics_sync, tmul_unnested_intent_scores_7day, ml_scores_bidder_sync_verification).
   - This labels every connector-initiated query and load job from `bigquery_load_query`, `bigquery_load_query_v2`, `bigquery_load_table` (bos/*, intent_score_threshold_v3/v4, media_plan_analytics, network_performance_metrics_sync, campaign_avg_cpm) with no script edits. Residual python-client sites for the owner: `spark_scripts/initial_bvp_V7/bvp_data_refresh_v7.py::load_bq` (`QueryJobConfig(labels=...)`) and the perml/AUTOTOF/hhst_v# parameterized jobs (their file, ~15 jobs/day).
   - Owner validation: one `bos__spend` `tables.*.create` task on the camperbid dev deployment, then the same JOBS_BY_PROJECT check for `airflow-camperbid-dev@`. Note for the owner: after merge `bos__spend` will read ~2,600 slot-h/day in the optimizer report (was ~1,630) and the four connector tasks will trigger `bq_heavy_task` findings; that is attribution moving, not new spend.
   - Recipients: @SteelHouse/pacing and @SteelHouse/performance-ml (CODEOWNERS); the user sends the Slack draft.
6. **Measurement surface (user decision, see Decisions).** Default = C: baseline and after-merge numbers from `optimizer_bq_<date>.md` (7-day means of the unattributed row: jobs and slot-h), recorded in §4. If the user picks A, add a Mode query "BigQuery jobs by attribution" over `dw-main-bronze.region-us-central1.INFORMATION_SCHEMA.JOBS_BY_PROJECT` filtered to the two fleet SAs, which needs `bigquery.jobs.listAll` (`roles/bigquery.resourceViewer`) for `mode-analytics@dw-main-bronze` on dw-main-bronze via a mntn-devops PR (today it holds only the layer reader role, `terragrunt/gcp/resources/mntn/dplat/modules/dw-medallion-layer/iam_bronze_extras.tf` line 158). Option B (record the unattributed bucket as a ledger row) reverses a deliberate design and flips a pinned test; not recommended.
7. **After merges.** Re-run step 1 for 7 post-merge days, add the after column to the xlsx, update §4/§5/§6, `/capture` the facts in 3.4, self-review entry. No `ledger applied` provenance stamp: labels move attribution, they do not save slot-hours, so the Mode savings numbers will not and should not move.

### 3.2 Assumptions to resolve empirically first
1. `bigQueryJobLabel.<key>` is the per-label key syntax and is honoured from Spark conf on the Dataproc Serverless 2.3 connector build (README confirms the option and the `spark.datasource.bigquery.` prefix; the exact `.airflow-dag` suffix form is from the connector docs' "multiple labels" wording). Settled by the owner's one dev batch in step 5.
2. `pandas_gbq.read_gbq(configuration={"labels": ...})` puts the labels on the job (pandas_gbq passes `configuration` through `_transform_read_gbq_configuration` untouched except `timeoutMs`; top-level `labels` is part of the JobConfiguration resource). Settled by the dev run in step 4 on `set_gaclid_enabled_flag`; fallback is `hook.get_client().query(sql, job_config=QueryJobConfig(labels=...)).to_dataframe()`.
3. Jinja filters `lower` and `replace` render inside nested `batch` property strings on the camperbid Airflow 3.2 deployment (the same dict already templates `{{ dag.dag_id }}`). Settled by the owner's dev batch.
4. The execute agent can deploy to the airflow-ti dev deployment; if not, step 4(b) degrades to post-merge verification.
5. bos task ids render under 63 chars after `tables.<name>.create` becomes `tables-<name>-create` (longest today: `tables-campaign_utc_yesterday_costs_impressions_by_hour-create`, 62 chars). Check the length in the hand-off.

### 3.3 Risks
- Label transform drift between the provider and our helper splits one DAG into two keys in the optimizer; mitigated by mirroring `_add_job_labels` verbatim and pinning it in `tests/test_bq_job_labels.py`.
- A label value outside `[a-z0-9_-]{0,63}` fails the BigQuery job outright; the helper's regex guard returns `{}` instead of failing the task; the camperbid jinja `lower` filter covers the connector path (all current camperbid dag ids are already lowercase).
- Root `tests/` are not run by CI, so the local pytest plus the gauntlet is the whole gate; the two pre-existing failures are out of scope and must be named in the PR body, not fixed.
- The camperbid merge is owned by another team and may land after the sprint; the airflow-ti PR alone moves job counts (~169/day), not slot-hours.
- After the camperbid change lands, `bos__spend` grows by ~975 slot-h/day in the optimizer ledger and the Mode "BigQuery cost by task" table, and four new `bq_heavy_task` findings appear; expected, and it feeds AUDI-1277.
- `OPTIMIZER_BQ_SAS` lists prod SAs only, so dev validation must query JOBS_BY_PROJECT directly rather than rely on the sweep.

### 3.4 Facts to route via /capture at close (this agent may not write knowledge/)
- `knowledge/memory/project_airflow_optimizer.md` and `reference_bq_job_attribution.md`: the 2026-09-02 "reconciling hypothesis = jobs outside the airflow-launched set" is refuted; the unattributed bucket is fleet-SA jobs, the ledger never records it by design, and 977 of 978 slot-h/day are camperbid Spark-BigQuery-connector reads from `bos__spend` batches. Append, do not overwrite.
- `knowledge/data_knowledge.md`: Spark-BigQuery connector query reads (`viewsEnabled` + `query`) appear in JOBS_BY_PROJECT as UUID-id SELECT jobs with an anonymous-dataset destination (`materializationDataset` deprecated in 0.42.1) and carry no labels unless `bigQueryJobLabel.*` is set.
- `reference_mode_api.md`: the Mode BQ section reads the ledger, so unattributed spend is not on Mode; `mode-analytics@dw-main-bronze` has no `jobs.listAll`.
- `reference_airflow_ti.md`: root `tests/` not in CI; `tests/test_url_pattern_pipeline.py` has 2 pre-existing failures on main `825b07e`.

### 3.5 Sources
- Spec: `/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` item 24; memory `/Users/malachi/Developer/work/mntn/workspace/knowledge/memory/project_airflow_optimizer.md` (2026-08-31 and 2026-09-02 sections), `reference_bq_job_attribution.md`, `reference_mode_api.md`, `reference_idso_repo.md`.
- Code (read-only main `825b07e`): `/Users/malachi/Developer/work/mntn/airflow-ti-main/include/spark_optimizer/{bq_profile,ledger,sweep}.py`, `include/spark_optimizer/tests/test_bq_profile.py`, `dags/attribution/{url_pattern_pipeline,url_pattern_identification,dlv_pattern_identification,tmobile_blocked_ip_workflow,tmobile_blocked_guids_workflow,set_gaclid_enabled_flag,blocked_ip_addresses_export,blocked_guids_export}.py`, `tests/test_url_pattern_pipeline.py`, `.github/workflows/*.yaml`.
- Provider 22.4.0 in `/Users/malachi/Developer/work/mntn/airflow-ti/.venv/lib/python3.11/site-packages/airflow/providers/google/cloud/{operators,hooks}/bigquery.py`; `pandas_gbq/gbq.py` in the same venv.
- Camperbid (GitHub API, main `707d739`): `dag_utils/google.py`, `dags/bos/bos__spend.py`, `spark_scripts/utils/util_spark.py`, `spark_scripts/bos/campaign_flight_end_cost.py`, `spark_scripts/initial_bvp_V7/bvp_data_refresh_v7.py`, `.github/CODEOWNERS`, `Dockerfile`, `requirements.txt`.
- IAM: `/Users/malachi/Developer/work/mntn/mntn-devops/terragrunt/gcp/resources/mntn/dplat/modules/dw-medallion-layer/iam_bronze_extras.tf`.
- Data: `queries/audi_1278_*.sql`, `outputs/audi_1278_*_2026_09_01.csv`, `outputs/optimizer_bq_2026-0*.md`, `outputs/prod_optimization_ledger.jsonl`; Mode report `e81786de8403` queries via `GET /api/mntn/reports/e81786de8403/queries`.

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
