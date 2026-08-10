# TI-1058 — Complete File Manifest: MNTN Matched DS13/DS19 OpenAI Pipeline

**Provenance:** GitHub discovery sweep (multi-angle, completeness-gated) run 2026-06-26 across the SteelHouse org.
222 raw hits → **~95 distinct files across 5 repos**: `shopper_graph` (core dbt + OpenAI-batch + middleware serving +
notebooks + redis-ops), `airflow-ti` (live DAGs, Spark FPA ingestion, vertical-classification Spark jobs, feature-store
models, K8s operator infra), `airflow` (legacy/near-duplicate vertical-classification + site_visit jobs), `dbt`
(ml_squad vertical_categorization + DS13/DS19 reporting), `sqlmesh` (site_visit_signal external-table def).

> **CAVEAT — read this before trusting the `DS` column.** The per-row `DS` tag below is the discovery agent's
> *inference* and is **unreliable** (it defaulted most rows to "DS13"). The **authoritative** DS split (verified by
> primary code reads — see `summary.md` §3/§5) is by LEG, not by this column:
> - **DS13 = vertical leg (domain→vertical, cached ~quarterly):** Steps 0–2 — `site_visit_signal` ingestion → distinct
>   domains → Common-Crawl homepage HTML → OpenAI → `website_crawl_verticals`. Code lives in
>   `airflow-ti/spark/vertical_classification/*`, `airflow-ti/dags/vertical_classification/*`,
>   `airflow-ti/dags/targeting/fetch_common_crawl.py`, and `SteelHouse/dbt` `ml_squad/models/vertical_categorization/*`.
> - **DS19 = keyword leg (URL→keyword, daily, the cost driver):** Steps 3–7, 9–10 — `shopper_graph`
>   `dbt/models/mntn_matched/*` + `openai/*` + DAGs `mntn_match_incrementals_{submit,fetch}`.
> - **Shared:** Steps 11–14 — the Flask/Lambda serving runtime, Redis/Postgres ops, and dbt/Airflow infra.
>
> Both legs use the OpenAI **Batch API** but are independent pipelines. The legacy `notebooks/` (GPT-3.5-turbo) and
> the `SteelHouse/airflow` repo are **older/duplicate** copies — confirm live-vs-deprecated before relying on them.

---

## MNTN Matched (DS13/DS19) OpenAI Pipeline — File Manifest

Grouped in data-flow order. One row per distinct path (duplicates in the source list collapsed; DS reconciled to strongest evidence).

### Step 0 — Shared input: FPA vendor + internal-signal ingestion into site_visit_signal

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Ingestion: vendor DDP (DS25 5x5) | SteelHouse/airflow-ti | /spark/fpa/dsid25_5x5_processing.py | Spark job processing 5x5 (DS25) vendor site-visit data into site_visit_signal | DS13 | high |
| Ingestion: internal signal (DS23 guid_log) | SteelHouse/airflow-ti | /spark/fpa/dsid23_guid_log_processing.py | Spark job processing internal guid_log (DS23) MNTN pixel into site_visit_signal | DS13 | high |
| Ingestion: vendor DDP (DS26 Predactiv) | SteelHouse/airflow-ti | /spark/fpa/dsid26_predactiv_processing.py | Spark job processing Predactiv (DS26) vendor data into site_visit_signal | DS13 | high |
| Ingestion: vendor DDP (DS28 33Across) | SteelHouse/airflow-ti | /spark/fpa/dsid28_33across_processing.py | Spark job processing 33Across (DS28) vendor data into site_visit_signal | DS13 | high |
| Ingestion: internal signal (DS30 augmentor) | SteelHouse/airflow-ti | /spark/fpa/dsid30_augmentor_log_processing.py | Spark job processing internal augmentor_log (DS30) bidstream data into site_visit_signal | DS13 | high |
| Ingestion: vendor DDP (DS36 Cybba) | SteelHouse/airflow-ti | /spark/fpa/dsid36_cybba_processing.py | Spark job processing Cybba (DS36) vendor data into site_visit_signal | DS13 | high |
| Ingestion: FPA vendor log consolidation | SteelHouse/airflow-ti | /dags/fpa/fpa_vendor_log_batch_ingestion_consolidated.py | DAG consolidating FPA vendor site-visit logs | DS13 | high |
| Ingestion: legacy DS23 site-visit (airflow) | SteelHouse/airflow | /dags/targeting/site_visit_signals_ds_id_23.py | Legacy DAG ingesting site-visit from DS ID 23 vendor | DS13 | high |
| Schema: site_visit_signal external table | SteelHouse/sqlmesh | /dataform/external_tables/definitions/v1/site_visit_signal.sqlx | External-table definition for site_visit_signal | DS13 | high |
| Feature store: site_visit_signal source | SteelHouse/airflow-ti | /models/feature_store/feature_group_1_source/site_visit_signal_advertiser_id_dsc_id.py | Feature-store model: site_visit_signal joined with advertiser/DSC IDs (MM input) | DS13 | high |
| Feature store: site_visit_signal derived | SteelHouse/airflow-ti | /models/feature_store/feature_group_2_derived/site_visit_signal_derived_advertiser_id_dsc_id.py | Feature-store derived features from site-visit signal | DS13 | high |
| Feature store: guid_log (DS23) source | SteelHouse/airflow-ti | /models/feature_store/feature_group_1_source/guid_log_advertiser_id_dsc_id.py | Feature-store model: internal guid_log (DS23) as feature source | DS13 | high |
| Feature store: augmentor IP+vertical | SteelHouse/airflow-ti | /models/feature_store/feature_group_1_source/aug_log_ip_vertical_id.py | Feature-store model: augmentor IPs with vertical assignments | DS13 | high |
| Feature store: augmentor IP+vertical (hourly) | SteelHouse/airflow-ti | /models/feature_store/feature_group_1_source/aug_log_ip_vertical_id_hourly.py | Feature-store model joining site_visit_signal→vertical with augmentor_log for per-IP hourly MM features | DS13 | medium |
| Feature store: conversion_log source | SteelHouse/airflow-ti | /models/feature_store/feature_group_1_source/conversion_log_advertiser_id_dsc_id.py | Feature-store model: conversion signals joined with advertiser/DSC IDs | DS13 | high |

### Step 1 — Domain extraction + Common-Crawl HTML retrieval

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Domain extraction (airflow-ti) | SteelHouse/airflow-ti | /spark/vertical_classification/distinct_site_visit_signal_domains.py | Extract distinct domains from 31-day site_visit_signal feeds (excludes DS23); tldextract domain extraction | DS13 | high |
| Domain extraction (airflow legacy) | SteelHouse/airflow | /dags/targeting/jobs/distinct_site_visit_signal_domains.py | Legacy job identifying domains from site-visit signals | DS13 | high |
| Common-Crawl HTML ingestion DAG | SteelHouse/airflow-ti | /dags/targeting/fetch_common_crawl.py | Weekly DAG fetching homepage HTML from Common Crawl index; runs 3 dbt CC tasks | DS13 | high |
| Common-Crawl homepage content (dbt) | SteelHouse/dbt | ml_squad/models/vertical_categorization/common_crawl_home_page_content.py | Common Crawl homepage content fetching | DS19 | high |
| HTML prep for classification | SteelHouse/airflow-ti | /spark/vertical_classification/prepare_html_content.py | Fetch Common-Crawl homepage HTML, filter site_visit_signal domains, format OpenAI batch requests w/ taxonomy prompt | DS13 | high |

### Step 2 — OpenAI batch vertical classification (vertical_classification path)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Batch submit (Spark) | SteelHouse/airflow-ti | /spark/vertical_classification/submit_html_content.py | Upload formatted JSON to OpenAI Batch Files API (/v1/chat/completions, 24h), track batch IDs in GCS | DS13 | high |
| Batch fetch (Spark) | SteelHouse/airflow-ti | /spark/vertical_classification/fetch_vertical_response.py | Poll OpenAI Batch API, download JSONL results, parse predicted_subindustry, upload to GCS | DS13 | high |
| Post-batch vertical update | SteelHouse/airflow-ti | /spark/vertical_classification/update_website_verticals.py | Parse OpenAI JSON, join domains + Postgres taxonomy + manual overrides, upsert website_crawl_verticals | DS13 | high |
| Post-batch vertical update (airflow legacy) | SteelHouse/airflow | /dags/targeting/jobs/update_website_verticals.py | Legacy job updating website vertical classifications | DS19 | high |
| Orchestration: classification submit DAG | SteelHouse/airflow-ti | /dags/vertical_classification/vertical_classification_submit.py | DAG orchestrating vertical-classification submission | DS19 | high |
| Orchestration: classification fetch DAG | SteelHouse/airflow-ti | /dags/vertical_classification/vertical_classification_fetch.py | 2-task Dataproc Serverless DAG: poll OpenAI results + update production verticals table | DS13 | high |
| Orchestration: classification submit (airflow legacy) | SteelHouse/airflow | /dags/targeting/vertical_classification_submit.py | Legacy DAG orchestrating vertical-classification submission | DS19 | high |
| Orchestration: classification fetch (airflow legacy) | SteelHouse/airflow | /dags/targeting/vertical_classification_fetch.py | Legacy DAG orchestrating vertical-classification result retrieval | DS19 | high |
| Orchestration: DDP classification API DAG | SteelHouse/airflow-ti | /dags/machine_learning/ddp_vertical_classification_api.py | DAG orchestrating vertical-classification API for DDPs | DS19 | high |
| DDP URL→vertical model (dbt) | SteelHouse/dbt | ml_squad/models/vertical_categorization/ddp_url_verticals.py | Maps URLs to DDP-classified verticals | DS19 | high |
| DDP vertical classification API model (dbt) | SteelHouse/dbt | ml_squad/models/vertical_categorization/ddp_vertical_classification_api.py | API model for DDP vertical classifications | DS19 | high |
| Vertical auto-assignment model | SteelHouse/airflow-ti | /models/vertical_categorization/verticals_auto_assignment.py | Auto-assigns verticals from classification results | DS13 | medium |
| Custom Spark/Databricks operators | SteelHouse/airflow-ti | /include/models/operators.py | Airflow custom operators for Pyspark/Databricks job execution (vertical auto-assignment) | DS13 | high |

### Step 3 — DS19 product/shopper-graph OpenAI batch (notebooks legacy + openai/ module)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Notebook 1: product uniques | SteelHouse/shopper_graph | notebooks/1. Get Product Uniques.py | Databricks notebook extracting unique product combos from guid_log, anti-join vs historical uniques, persist to S3 | DS13 | high |
| Notebook 2: OpenAI submit batch | SteelHouse/shopper_graph | notebooks/2. OpenAI Submit Batch.py | Databricks notebook formatting product data into OpenAI batch requests (GPT-3.5-turbo) and submitting | DS13 | high |
| Notebook 3: OpenAI retrieve batch | SteelHouse/shopper_graph | notebooks/3. OpenAI Retrieve Batch.py | Placeholder/stub notebook to retrieve/parse OpenAI batch results | DS13 | medium |
| Notebook 4: build taxonomy | SteelHouse/shopper_graph | notebooks/4. Shopper Graph Build Taxonomy.py | Databricks notebook parsing OpenAI responses, building product_taxonomy w/ sequential DSC IDs | DS13 | high |
| Notebook 6: insert to TPA | SteelHouse/shopper_graph | notebooks/6. Insert Shopper Graph to TPA.py | Databricks notebook mapping classified products to IPs, exporting shopper-graph targeting for TPA | DS13 | high |
| Notebook: update VectorDB | SteelHouse/shopper_graph | notebooks/Update VectorDB.sql | SQL notebook truncating/repopulating vector_db_test from product_taxonomy | DS19 | high |
| Notebook: legacy post-LLM (DO NOT USE) | SteelHouse/shopper_graph | notebooks/Shopper Graph Post LLM(Legacy) DO NOT USE.sql | Deprecated DLT streaming ETL for OpenAI batch results (marked DO NOT USE) | DS13 | high |
| OpenAI: submit entrypoint | SteelHouse/shopper_graph | /openai/submit_batch.py | Entry point submitting formatted batch requests to OpenAI (GPT) for classification | DS13 | high |
| OpenAI: fetch entrypoint | SteelHouse/shopper_graph | /openai/fetch_results.py | Entry point fetching completed classification results from OpenAI batch API | DS13 | high |
| OpenAI: transition entrypoint | SteelHouse/shopper_graph | /openai/transition_batch.py | Transitions batch metadata unsubmitted→submitted when batch in_progress/completed | DS19 | high |
| OpenAI: storage cleanup | SteelHouse/shopper_graph | /openai/delete_all_storage_files.py | Cleanup deleting old OpenAI storage files (>72h, part-/batch_ prefixed) | DS19 | high |
| OpenAI: input validation | SteelHouse/shopper_graph | /openai/validate_files.py | Pre-submission validation: ≤45000 records per input file | DS19 | high |
| OpenAI wrapper: batch base | SteelHouse/shopper_graph | /openai/openai_wrapper/batch_base.py | Base class: OpenAI client init, OPENAI_API_KEY retrieval, S3 bucket determination | DS19 | high |
| OpenAI wrapper: batch submitter | SteelHouse/shopper_graph | /openai/openai_wrapper/batch_submitter.py | OpenAI batch submitter class: format+upload requests, track batch IDs | DS13 | high |
| OpenAI wrapper: batch fetcher | SteelHouse/shopper_graph | /openai/openai_wrapper/batch_fetcher.py | OpenAI batch fetcher class: poll+retrieve completed results | DS13 | high |
| OpenAI wrapper: batch transitioner | SteelHouse/shopper_graph | /openai/openai_wrapper/batch_transitioner.py | Batch state machine: manage transitions between batch statuses | DS13 | high |
| OpenAI: external categories DDL | SteelHouse/shopper_graph | /openai/create_external_categories_ddl.sql | BQ DDL creating external table for batch input/output files | DS13 | medium |
| OpenAI: dependencies | SteelHouse/shopper_graph | /openai/requirements.txt | Python deps for OpenAI batch (openai, pandas, gcsfs, pyarrow, google-cloud-storage) | DS19 | high |
| OpenAI: container image | SteelHouse/shopper_graph | /openai/Dockerfile | python:3.11 image for OpenAI batch tasks (submit/fetch/transition + wrapper) | DS19 | high |
| OpenAI: build automation | SteelHouse/shopper_graph | /openai/Makefile | Build automation for OpenAI utilities Docker image and batch processing | DS13 | high |

### Step 4 — dbt mntn_matched pre-batch (product uniques → batch input)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Pre-batch: raw batch input | SteelHouse/shopper_graph | dbt/models/mntn_matched/pre_batch/openai_batch_input_raw.py | Gathers site_visit_signal product data for OpenAI batch input (DS13 vertical prep) | DS13 | high |
| Pre-batch: formatted batch input | SteelHouse/shopper_graph | dbt/models/mntn_matched/pre_batch/openai_batch_input_formatted.py | Formats OpenAI batch input from raw product data w/ vectors and taxonomy | DS13 | high |
| Pre-batch: product uniques | SteelHouse/shopper_graph | dbt/models/mntn_matched/pre_batch/product_uniques.py | Deduplicates product records from site_visit_signal for batch processing | DS13 | high |
| Pre-batch: reprocess queue | SteelHouse/shopper_graph | dbt/models/mntn_matched/pre_batch/product_uniques_to_reprocess.py | Tracks products that failed classification and need resubmission | DS13 | high |
| Config: raw batch input meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/pre_batch/openai_batch_input_raw.yml | dbt YAML config for openai_batch_input_raw model | DS13 | high |
| Config: formatted batch input meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/pre_batch/openai_batch_input_formatted.yml | dbt YAML config for openai_batch_input_formatted model | DS13 | high |
| Config: product uniques meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/pre_batch/product_uniques.yml | dbt YAML config for product_uniques model | DS13 | high |
| Config: reprocess queue meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/pre_batch/product_uniques_to_reprocess.yml | dbt YAML config for product_uniques_to_reprocess model | DS13 | high |

### Step 5 — dbt mntn_matched post-batch (categorization)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Post-batch: results joined | SteelHouse/shopper_graph | dbt/models/mntn_matched/post_batch/openai_batch_results_joined.py | Joins OpenAI classification results back to original product records | DS13 | high |
| Post-batch: product categorization | SteelHouse/shopper_graph | dbt/models/mntn_matched/post_batch/product_categorization.py | Applies OpenAI labels to products; core MM categorization table | DS13 | high |
| Post-batch: categorization temp | SteelHouse/shopper_graph | dbt/models/mntn_matched/post_batch/product_categorization_temp.py | Temp staging table bridging OpenAI results to final taxonomy mapping | DS19 | medium |
| Config: results joined meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/post_batch/openai_batch_results_joined.yml | dbt YAML config for openai_batch_results_joined model | DS13 | high |
| Config: product categorization meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/post_batch/product_categorization.yml | dbt YAML config for product_categorization model | DS13 | high |
| Config: categorization temp meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/post_batch/product_categorization_temp.yml | dbt YAML config for product_categorization_temp staging model | DS13 | high |

### Step 6 — dbt mntn_matched taxonomy + vector index (bge_large)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Taxonomy: master | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy.py | Master taxonomy of verticals for MM classification | DS13 | high |
| Taxonomy: BQ variant | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy_bq.py | BigQuery sync of MNTN taxonomy for cross-system access | DS19 | high |
| Taxonomy: vector index | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy_vector.py | Vector embeddings of taxonomy (bge_large) → etl_mm_taxonomy_vector_index | DS13 | high |
| Taxonomy: generic vector | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/taxonomy_vector.py | Generic taxonomy vector table (reusable vector index) | DS13 | high |
| Taxonomy: vector source | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/taxonomy_vector_source.py | Source data for taxonomy vector generation | DS13 | high |
| Config: taxonomy meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy.yml | dbt YAML config for mntn_matched_taxonomy model | DS13 | high |
| Config: taxonomy BQ meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy_bq.yml | dbt YAML config for mntn_matched_taxonomy_bq model | DS13 | high |
| Config: taxonomy vector meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/mntn_matched_taxonomy_vector.yml | dbt YAML config for mntn_matched_taxonomy_vector (vector embedding settings) | DS13 | high |
| Config: generic vector meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/taxonomy_vector.yml | dbt YAML config for taxonomy_vector model | DS13 | high |
| Config: vector source meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/taxonomy/taxonomy_vector_source.yml | dbt YAML config for taxonomy_vector_source model | DS13 | high |

### Step 7 — DS19 advertiser-vertical matching + seed data

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Advertiser verticals: vector source | SteelHouse/shopper_graph | dbt/models/mntn_matched/advertiser_verticals/verticals_vector_source.py | Advertiser vertical assignments as vector source for MM matching | DS19 | high |
| Config: advertiser verticals meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/advertiser_verticals/verticals_vector_source.yml | dbt YAML config for verticals_vector_source model | DS19 | high |
| Seed: advertiser-vertical CSV | SteelHouse/shopper_graph | /advertiser_verticals_seed.csv | DS19 advertiser-vertical seed data (364KB) loaded into fpa.advertiser_verticals | DS19 | high |
| DB init / seed loader | SteelHouse/shopper_graph | /init.sql | Creates Postgres schema tables; COPY-seeds advertiser_verticals from CSV | DS19 | high |
| Precache: advertiser verticals DAG | SteelHouse/airflow-ti | /dags/machine_learning/mntn_match_verticals_precache_v1_1.py | DAG pre-caching advertiser vertical assignments for MM scoring | DS19 | high |
| Keyword pipeline (DS19) DAG | SteelHouse/airflow-ti | /dags/machine_learning/bottom_up_keywords_pipeline_run.py | Keyword-extraction pipeline DAG for DS19 vertical keywords | DS19 | medium |

### Step 8 — Staging / scratch dbt (tmp_models)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Staging: categories external table | SteelHouse/shopper_graph | tmp_models/categories.sql | External Hive table over S3 PARQUET for mntn_matched_taxonomy (vendor taxonomy schema) | DS19 | high |
| Staging: orphaned categories | SteelHouse/shopper_graph | tmp_models/orphaned_categories.py | Incremental dbt model flagging categories w/ NULL data_source_category_id (data-quality) | DS19 | medium |
| Staging: send taxonomy to Redshift | SteelHouse/shopper_graph | tmp_models/send_taxonomy_to_redshift.sql | Rebuild Redshift external + internal tpa.mntn_matched_taxonomy from S3 PARQUET for TPA access | DS13 | high |
| Staging: disabled taxonomy builder | SteelHouse/shopper_graph | tmp_models/taxonomy/mntn_matched_taxonomy.py | DISABLED dbt model (exit() guard) attempting new taxonomy rows w/ auto-increment DSC ID (DS19) | DS19 | high |

### Step 9 — Outputs: reporting, audience sizing, TPA export

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Reporting: MM reporting model | SteelHouse/shopper_graph | dbt/models/mntn_matched/reporting/mntn_matched_reporting.py | Reporting/analytics layer for MM metrics and KPIs | DS13 | high |
| Config: MM reporting meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/reporting/mntn_matched_reporting.yml | dbt YAML config for mntn_matched_reporting model | DS13 | high |
| Reporting: targeted signal DS13 (dbt) | SteelHouse/dbt | ml_squad/models/reporting/targeted_signal_ds_13.py | Reporting for DS13 targeted signals | DS13 | high |
| Reporting: targeted signal DS19 (dbt) | SteelHouse/dbt | ml_squad/models/reporting/targeted_signal_ds_19.py | Reporting for DS19 targeted signals | DS19 | high |
| Audience sizes model | SteelHouse/shopper_graph | dbt/models/mntn_matched/audience_sizes.py | Audience size metrics per vertical for reporting/optimization | DS13 | medium |
| Config: audience sizes meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/audience_sizes.yml | dbt YAML config for audience_sizes model | DS13 | high |
| TPA export model | SteelHouse/shopper_graph | dbt/models/mntn_matched/tpa_export.py | Exports MM results to TPA (Third Party Advertiser) system | DS13 | high |
| Config: TPA export meta | SteelHouse/shopper_graph | dbt/models/mntn_matched/tpa_export.yml | dbt YAML config for tpa_export model | DS13 | high |

### Step 10 — Orchestration: machine_learning DAGs (submit/fetch/audit/export)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| DAG: incrementals submit | SteelHouse/airflow-ti | /dags/machine_learning/mntn_match_incrementals_submit.py | DAG orchestrating daily MNTN Matched batch submission to OpenAI | DS13 | high |
| DAG: incrementals fetch | SteelHouse/airflow-ti | /dags/machine_learning/mntn_match_incrementals_fetch.py | DAG fetching+ingesting completed OpenAI batch results | DS13 | high |
| DAG: audience sizes | SteelHouse/airflow-ti | /dags/machine_learning/mntn_match_audience_sizes.py | DAG computing MM audience-size metrics per vertical | DS13 | high |
| DAG: TPA export prep | SteelHouse/airflow-ti | /dags/machine_learning/mntn_match_tpa_export_prep.py | DAG preparing MM data for export to TPA system | DS13 | high |
| DAG: DDP audit | SteelHouse/airflow-ti | /dags/machine_learning/mntn_match_ddp_audit.py | DAG auditing MM DDP contributions and quality | DS13 | medium |
| DAG: DDP rolling audit | SteelHouse/airflow-ti | /dags/machine_learning/mntn_match_ddp_rolling_audit.py | DAG rolling audit of MM DDP data freshness/quality | DS13 | medium |

### Step 11 — Serving runtime: middleware k8s Flask API + wrappers + utils

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Serving: vector-search Lambda | SteelHouse/shopper_graph | middleware/src/lambda_function.py | Lambda: vector-search similarity vs Databricks taxonomy → data_source_category_ids; Kafka logging | DS13 | high |
| Serving: src package init | SteelHouse/shopper_graph | middleware/src/__init__.py | Empty package init for middleware.src | shared | high |
| Serving: Lambda Dockerfile | SteelHouse/shopper_graph | middleware/Dockerfile | Lambda python:3.11 image; copies lambda_function.py as handler | shared | high |
| Serving: Lambda requirements | SteelHouse/shopper_graph | middleware/requirements.txt | Lambda deps: redis, requests, profanity-check, kafka, databricks-vectorsearch | shared | high |
| Serving: Lambda build/deploy | SteelHouse/shopper_graph | middleware/Makefile | Build/deploy Lambda image to ECR + update Lambda function | shared | high |
| Serving: Flask REST API | SteelHouse/shopper_graph | middleware/k8s/api.py | Flask API: /autopilot,/search_term,/vertical,/domain_map,/copy_profile,/select_recommendation,/health,/prometheus | DS13 | high |
| Serving: K8s API Dockerfile | SteelHouse/shopper_graph | middleware/k8s/Dockerfile | python:3.11 image; api.py Flask entrypoint; port 5000; kubectl+elinks | shared | high |
| Serving: K8s API requirements | SteelHouse/shopper_graph | middleware/k8s/requirements.txt | K8s API deps: Flask, langchain-openai, databricks-vectorsearch/cli, kafka, psycopg2, prometheus, waitress | shared | high |
| Serving: OpenAPI spec | SteelHouse/shopper_graph | middleware/k8s/api_spec.yaml | OpenAPI 3.0 spec for MM REST API endpoints | shared | high |
| Serving: K8s API build/deploy | SteelHouse/shopper_graph | middleware/k8s/Makefile | Docker build/push to ECR + Helm deploy + local run for K8s API | shared | high |
| Serving: autopilot wrapper | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/autopilot_wrapper.py | Generates/serves company profiles (description, keywords, categorization) via LLM or Postgres cache | DS13 | high |
| Serving: autopilot regenerate wrapper | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/autopilot_regenerate_wrapper.py | Regenerates profiles w/ override to bypass cache | DS13 | high |
| Serving: search-term wrapper | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/search_term_wrapper.py | Generates products from search terms via GPT-4o; Postgres cache + vector-search fallback for DSC ID | DS19 | high |
| Serving: vertical wrapper | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/vertical_wrapper.py | Handles vertical classification/assignments; get/post vertical taxonomy links in Postgres | DS13 | high |
| Serving: select-recommendation wrapper | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/select_recommendation_wrapper.py | Processes user selections of recommended verticals; updates Postgres | DS13 | high |
| Serving: domain-map wrapper | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/domain_map_wrapper.py | Domain↔advertiser mapping (GET/POST/DELETE) over Postgres domain_map | DS13 | high |
| Serving: random-keyword wrapper | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/random_keyword_wrapper.py | Generates random keywords/products from taxonomy for testing | DS13 | high |
| Serving: copy-profile wrapper | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/copy_profile_wrapper.py | Copies full advertiser profile (autopilot+search terms+verticals) between advertiser_ids | DS13 | high |
| Serving: wrapper package init | SteelHouse/shopper_graph | middleware/k8s/shopper_graph_wrapper/__init__.py | Empty package init for shopper_graph_wrapper | shared | high |
| Serving util: Postgres ops | SteelHouse/shopper_graph | middleware/k8s/utils/postgres.py | Postgres ops layer (autopilot, search_term_products, dsc_mappings, vertical_assignments, domain_map) + batch ops | shared | high |
| Serving util: Postgres pool | SteelHouse/shopper_graph | middleware/k8s/utils/postgres_pool.py | psycopg2 connection pooling; get_db_connection() context manager; K8s readiness health check | shared | high |
| Serving util: vector search | SteelHouse/shopper_graph | middleware/k8s/utils/vector_search.py | Parses Databricks vector-search API responses into column-aligned dicts | shared | high |
| Serving util: langchain | SteelHouse/shopper_graph | middleware/k8s/utils/langchain.py | Langchain integration: ChatOpenAI, retriever chains, RAG for website→profile | shared | high |
| Serving util: scraping | SteelHouse/shopper_graph | middleware/k8s/utils/scraping.py | Web scraping (BeautifulSoup+elinks) for autopilot content | shared | high |
| Serving util: secrets | SteelHouse/shopper_graph | middleware/k8s/utils/secrets.py | Secrets management wrapper for env + external secret stores | shared | medium |
| Serving util: audience service | SteelHouse/shopper_graph | middleware/k8s/utils/audience_service.py | Audience/advertiser metadata service client | shared | medium |
| Serving util: keyword hierarchy | SteelHouse/shopper_graph | middleware/k8s/utils/keyword_hierarchy.py | Builds parent-child keyword taxonomy for search_term products + vertical classification | DS13 | high |
| Serving util: keyword hierarchy README | SteelHouse/shopper_graph | middleware/k8s/utils/KEYWORD_HIERARCHY_README.md | Docs for keyword hierarchy structure/usage | shared | medium |
| Serving util: prometheus | SteelHouse/shopper_graph | middleware/k8s/utils/prometheus.py | Prometheus metrics (REQUEST_TIME_ST histogram) decorating handlers | shared | high |
| Serving util: utils package init | SteelHouse/shopper_graph | middleware/k8s/utils/__init__.py | Empty package init for middleware.k8s.utils | shared | high |
| Serving: k8s package init | SteelHouse/shopper_graph | middleware/k8s/__init__.py | Empty package init for middleware.k8s | shared | high |
| Serving: Slack perf monitor | SteelHouse/shopper_graph | middleware/k8s/scripts/slack_performance_monitor.py | Tracks MM pipeline perf (latency/error/throughput), alerts to Slack | shared | medium |
| Serving: dbt manifest | SteelHouse/shopper_graph | middleware/k8s/dbt/manifest.json | dbt manifest (model/macro/source metadata) for K8s API lineage/introspection | shared | high |
| Serving: dbt catalog | SteelHouse/shopper_graph | middleware/k8s/dbt/catalog.json | dbt catalog (table/view schemas + column metadata) | shared | high |
| Serving: dbt run results | SteelHouse/shopper_graph | middleware/k8s/dbt/run_results.json | Results of last dbt run (status, timings, outcomes) | shared | high |
| Serving: dbt docs index | SteelHouse/shopper_graph | middleware/k8s/dbt/index.html | dbt-generated docs HTML / lineage graph (served at /dbt) | shared | high |
| Serving: dbt docs index2 | SteelHouse/shopper_graph | middleware/k8s/dbt/index2.html | Alternate/newer dbt docs HTML served at /dbt | shared | high |

### Step 12 — Autopilot Lambda (audience auto-build)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Autopilot: Lambda function | SteelHouse/shopper_graph | autopilot/src/lambda_function.py | Lambda generating advertiser audience profiles via LangChain multi-step LLM (URL/brand/keyword) + Redis cache | shared | high |
| Autopilot: Dockerfile | SteelHouse/shopper_graph | autopilot/Dockerfile | Lambda python:3.11 image; Databricks creds + env for autopilot Lambda | shared | high |
| Autopilot: requirements | SteelHouse/shopper_graph | autopilot/requirements.txt | Autopilot Lambda deps: langchain, mlflow, redis, databricks-cli, awscli | shared | high |

### Step 13 — Ops: Redis↔Postgres migration + Redis cluster scripts

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Ops: Redis→Postgres migrator | SteelHouse/shopper_graph | /scripts/migrate_redis_to_postgres.py | Migrates Redis (autopilot/search terms/DSC IDs) to Postgres tpa.* tables w/ UPSERT | shared | high |
| Ops: Redis vs Postgres compare | SteelHouse/shopper_graph | /scripts/compare_redis_vs_postgres.py | Benchmarks Redis vs Postgres fetch perf + validates parity | shared | high |
| Ops: migration schema DDL | SteelHouse/shopper_graph | /scripts/redis_migration_schema.sql | Postgres migration-table DDL (autopilot_profile, search_term, dsc_mapping) w/ temporal audit | shared | high |
| Ops: redis scripts README | SteelHouse/shopper_graph | /scripts/redis/README.md | Docs for Redis cluster operation/management scripts | shared | high |
| Ops: check advertiser keys | SteelHouse/shopper_graph | /scripts/redis/check_advertiser_keys.sh | Check Redis keys for a specific advertiser | shared | high |
| Ops: check all bad keys (fast) | SteelHouse/shopper_graph | /scripts/redis/check_all_advertisers_bad_keys_fast.sh | Bulk-scan all advertisers for malformed Redis keys (prod) | shared | high |
| Ops: check all bad keys (QA) | SteelHouse/shopper_graph | /scripts/redis/check_all_advertisers_bad_keys_fast_qa.sh | QA variant of bad-keys checker | shared | high |
| Ops: check bad keys (prod) | SteelHouse/shopper_graph | /scripts/redis/check_bad_keys_prod.sh | Check bad keys w/ targeting specificity (prod) | shared | high |
| Ops: check bad keys sample (prod) | SteelHouse/shopper_graph | /scripts/redis/check_bad_keys_sample_prod.sh | Check bad keys on a sample subset (prod) | shared | high |
| Ops: delete bad keys (prod) | SteelHouse/shopper_graph | /scripts/redis/delete_bad_keys_prod_fast.sh | Delete malformed Redis keys (destructive, prod) | shared | high |
| Ops: delete bad keys (QA) | SteelHouse/shopper_graph | /scripts/redis/delete_bad_keys_qa_fast.sh | Delete malformed Redis keys (QA) | shared | high |
| Ops: migrate Redis→Redis | SteelHouse/shopper_graph | /scripts/redis/migrate_redis_to_redis.sh | Migrate Redis data between Redis instances | shared | high |
| Ops: migrate Redis→Redis (RedisJSON) | SteelHouse/shopper_graph | /scripts/redis/migrate_redis_to_redis_rejson_only.sh | Specialized migration for RedisJSON-format keys | shared | high |
| Ops: QA delete keys only | SteelHouse/shopper_graph | /scripts/redis/qa_delete_keys_only.sh | QA-only key deletion (non-prod-safe wrapper) | shared | high |
| Ops: redis cluster repair | SteelHouse/shopper_graph | /scripts/redis/redis_cluster_repair.sh | Diagnose/repair Redis cluster state/rebalancing | shared | high |
| Ops: restore advertiser keys | SteelHouse/shopper_graph | /scripts/redis/restore_advertiser_keys.sh | Restore Redis keys for an advertiser from backup | shared | high |
| Ops: restore RDB (fast) | SteelHouse/shopper_graph | /scripts/redis/restore_rdb_fast.sh | Fast restore of Redis from RDB snapshot | shared | high |
| Ops: restore RedisJSON RDB | SteelHouse/shopper_graph | /scripts/redis/restore_rejson_rdb_to_redis.sh | Restore RedisJSON data from RDB snapshot | shared | high |
| Ops: update keys from file | SteelHouse/shopper_graph | /scripts/redis/update_keys_from_file.sh | Batch update Redis keys from a file | shared | high |

### Step 14 — Shared dbt + Airflow configuration / infra

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Config: dbt project | SteelHouse/shopper_graph | /dbt/dbt_project.yml | dbt project config; defines vector_index etl_mm_taxonomy_vector_index + bge_large_en_v1_5 | DS13 | high |
| Config: dbt profiles | SteelHouse/shopper_graph | /dbt/profiles.yml | dbt profile config for DB connections / execution context | DS13 | high |
| Config: dbt requirements | SteelHouse/shopper_graph | /dbt/requirements.txt | Python deps for dbt models (dbt-core, adapters, custom libs) | DS13 | high |
| Config: dbt Dockerfile | SteelHouse/shopper_graph | /dbt/Dockerfile | Container image for dbt executions (OpenAI batch submit/fetch) | DS13 | high |
| Infra: K8s operators (airflow-ti) | SteelHouse/airflow-ti | /include/dbx/kube_operators.py | DbxDbtOperator (runs dbt) + MntnKubePodOperator (containerized py w/ OPENAI_API_KEY secret); DbtImageName enum | shared | high |
| Infra: dbx package exports | SteelHouse/airflow-ti | /include/dbx/__init__.py | Exports DbxDbtOperator, MntnKubePodOperator, DbtImageName | shared | high |
| Infra: ML team job config | SteelHouse/airflow-ti | /include/job_config/job_team_config.py | ML-team DAG defaults (retries, email, severity) | shared | medium |
| Infra: job_config exports | SteelHouse/airflow-ti | /include/job_config/__init__.py | Exports JobTeamConfig from job_team_config | shared | high |
| Config: airflow-ti requirements | SteelHouse/airflow-ti | /requirements.txt | Python deps for Airflow DAGs and utilities | DS13 | high |
| Config: airflow-ti Dockerfile | SteelHouse/airflow-ti | /Dockerfile | Container image for Airflow worker/scheduler | DS13 | high |

### Step 15 — Pipeline documentation (workspace knowledge base)

| Step | Repo | Path | Purpose | DS | Confidence |
|---|---|---|---|---|---|
| Docs: site-visit-signal lineage | SteelHouse/workspace | knowledge/data_knowledge.md §Site Visit Signal | End-to-end lineage: FPA ingestion → distinct domains → OpenAI classification → website_crawl_verticals → feature store | DS13 | high |
| Docs: DS13 business context | SteelHouse/workspace | knowledge/ds_catalog.md DS13 section | DS13 bucket/vertical classification for MM 2.0, stored in household_score via batch scoring; Peak Performance backbone | DS13 | high |

---

## Remaining unknowns (referenced but not yet pinned — next-look notes)

- Per-vendor DDP ingestion jobs for DS24 (Justuno), DS27 (LaunchLabs), DS33 (Sovrn), DS39 (Klickly), DS40 (33Across-API) are missing — only DS25/26/28/36 (+23/30 internal) Spark files are present. Look in airflow-ti/spark/fpa/ for dsid24_*/dsid27_launchlabs_processing.py/dsid33_*/dsid39_*/dsid40_*; cross-check ENABLED_DSIDS in the fpa_site_visit_batch_serverless DAG.
- The @hourly orchestrator DAG itself (fpa_site_visit_batch_serverless, ENABLED_DSIDS=[23,25,26,28,30,36]) is named in data_knowledge.md but its exact file path is unconfirmed — enumerate airflow-ti/dags/fpa/ to locate it.
- The ecommerce classifier SCORING path was located (AUDI-431, 2026-08-10): `SteelHouse/dbt ml_squad/models/vertical_categorization/ddp_url_verticals.py` loads MLflow `{env}.ml.ecommerce_classifier@champion` (Databricks UC registry) and scores every svs URL at threshold 0.4, writing `gs://mntn-data-archive-prod/vertical_categorizations/ddp_url_verticals/`. The model's TRAINING code remains unlocated (lives behind the MLflow registry, not in dbt/airflow repos).
- The household_score batch-scoring writer (the MM 2.0 state machine that writes vertical→household_score, per ds_catalog) is not represented by any file. Search SteelHouse for 'household_score' writers in airflow-ti/models, sqlmesh, or a bidder/scoring repo.
- Canonical website_crawl_verticals table/DDL is unlocated — it is written by update_website_verticals.py and consumed by 4+ jobs but has no schema/external-def file. Search 'website_crawl_verticals' across the org (likely SteelHouse/sqlmesh dataform external_tables or a feature-store DDL).
- airflow vs airflow-ti duplication: both repos contain distinct_site_visit_signal_domains, update_website_verticals, and vertical_classification_submit/fetch. Confirm which is LIVE vs deprecated (check DAG schedules / recent commits) so the same step isn't double-counted — both kept here pending that confirmation.
- fetch_common_crawl downstream dbt models (common_crawl_index, common_crawl_home_page_index, common_crawl_website_home_pages) are run by the DAG but only common_crawl_home_page_content.py was located. Search SteelHouse/dbt ml_squad/models/vertical_categorization/ and airflow-ti/models for the other three.
- DDP-audit model files (ddp_audit.py, ddp_domains_added_per_vertical.py, ddp_ips_added_per_vertical.py, ddp_rolling_audit.py) referenced by mntn_match_ddp_audit/_rolling_audit DAGs are unlocated. Enumerate airflow-ti/models/vertical_categorization/.
- verticals_precache_v1_1 target models (verticals_pre_cache / verticals_auto_assignment source) and the DS19 advertiser-match query (the JOIN of advertiser vector vs domain/taxonomy vector that assigns an advertiser's MM vertical) are not pinned to a single file. Trace the precache DAG's referenced dbt/Spark model.
- Taxonomy auto-add '>=500x' new-vertical promotion threshold rule not found as a file. Keyword-search shopper_graph for '500','>= 500','min_domains','min_ips','auto_add' (likely in vertical_wrapper.py, api.py, or dbt config).
- tpa_export write target: the actual loader pushing tpa_export output into TPA serving (Aerospike/Redshift/DW) — represented only by mntn_match_tpa_export_prep DAG + send_taxonomy_to_redshift.sql. Confirm the final write table/store.
- CI/CD glue absent: enumerate shopper_graph/.github/workflows and airflow-ti/.github/workflows to document build/deploy of steelhousedev/{shopper_graph,open_ai_batch} images referenced in kube_operators.py.
- Deployment topology: shopper_graph helm/ chart and docker-compose.yaml (middleware/autopilot serving) not captured. Enumerate shopper_graph/helm/ and repo root.
- dbt/macros/, dbt/seeds/, dbt/tests/ in shopper_graph never enumerated (taxonomy/vertical seed CSVs likely in seeds/). Enumerate these three directories.
- airflow-ti/include/job_config/job_config.py (core job-config module, beyond job_team_config.py) needed to resolve DAG default args / Dataproc batch config across MM DAGs — locate and read.
- feature_group_2_derived companions (guid_log/conversion_log/aug_log derived models feeding MM scoring) — only site_visit_signal_derived is listed. Enumerate airflow-ti/models/feature_store/feature_group_2_derived/.
- SteelHouse/sqlmesh broader enumeration — only site_visit_signal.sqlx found. Enumerate dataform/external_tables/definitions for vertical/website_crawl_verticals/household_score model defs.
- fpa.mm_domain_map upstream source (domain↔advertiser mappings consumed by domain_map_wrapper) not identified — find the producer of fpa.mm_domain_map.
- advertiser_verticals_seed.csv exact column schema not confirmed (file too large to read); upstream creation/maintenance owner undocumented.
