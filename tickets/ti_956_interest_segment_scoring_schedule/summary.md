# TI-956: Scheduled Interest Segment Scoring Job

**Jira:** https://mntn.atlassian.net/browse/TI-956
**Status:** In Progress
**Date Started:** 2026-05-27
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction
Make LiveRamp interest-segment quality + performance scores available on a
recurring schedule, written to a GCS path. Not a production deployment — a
scheduled job that runs Alex's scoring class on a weekly or monthly cadence so
consumers (Macie, downstream UI/admin tooling) don't have to ask Alex to rerun
the notebook every time.

Source code: [targeting-infra-ml#57](https://github.com/SteelHouse/targeting-infra-ml/pull/57)
— quality scoring utilities + example notebook. Class is PySpark, ~two lines
to invoke the full scoring pipeline given the input dataset.

Origin: ask from Paulo + Allison to "use interest segments more." Allison +
Alex first proposed classifying by keywords/verticals; Paulo asked for
"something more nuanced." This scoring framework is the nuanced answer.

**Empirical sizing of the prize (TI-999 Finding 15, 2026-05-28):** The
bidder is scored-first within campaign pacing, falls through to unscored
3P-added IPs once the MM ceiling for the campaign's targeted segment is
hit (FICO single-advertiser test: MM_only and MM+3P campaigns deliver
the SAME scored-IP volume ~60-72K/day; the bigger MM+3P campaign spent
4x more, all extra spend went to 236K unscored 3P-added impressions
per day). Buyers add 3P inclusion clauses to **intentionally expand
reach beyond MM's ceiling** — the unscored delivery is the buyer's
chosen overflow path, not a bidder accident.

Quantified prize zone:
- MM + 3P inclusion campaigns spend ~$643K / 30d (~$7.7M annualized)
  on the unscored-IP overflow portion.
- Pure-3P campaigns spend ~$2.15M / 30d (~$25.8M annualized) on
  unscored 3P delivery (no MM ceiling at all — the whole budget is
  3P-driven).
- **Combined ~$50M+/year is the intentional buyer-driven reach
  expansion via 3P clauses.** TI-956's per-segment quality score tells
  buyers WHICH 3P segments to expand into. Today they pick blindly;
  tomorrow they pick quality-informed.

**Elevator pitch:** *"MM has a ceiling. When buyers' budgets exceed
what MM delivers at quality, they expand into 3P interest segments to
keep spending. Today they pick those 3P segments blindly. ~$50M/year
of MNTN delivery is this intentional reach expansion. TI-956 ranks
LiveRamp segments by 9 quality axes so buyers expand into the best
ones — direct lift on currently-blind spend."*

**Product implication for Macie / admin UI:** because MM ceiling is
measurable per (campaign × MM segment × day), the UI can surface it
directly to buyers as part of the campaign-setup flow:
*"Budget $Y exceeds MM segment X's ceiling of ~$Z. The remaining
$(Y - Z) will expand into 3P — pick high-quality 3P segments here →"*
This ties TI-956 scores to the buyer's actual decision moment.

Confluence design docs (saved as offline copies in `artifacts/`):
- [Liveramp Segment Quality Steps - Archive](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3274506251/Liveramp+Segment+Quality+Steps+-+Archive) (page id 3274506251) — initial 7-component design.
- [Liveramp Segment Quality Steps - Update](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3523477544/Liveramp+Segment+Quality+Steps+-+Update) (page id 3523477544) — **current canonical version, supersedes Archive**. Adds targetability + performance components; switches final mapping from percentile rank to sigmoid of standardized z_combo.

## 2. The Problem
- LiveRamp = ~90% of MNTN's interest segments. There are 200k+ interest segments total.
- Search for "households with $100K+ income" returns 90 segments — search is improved (Jeff Capone's AT demo) but there's **no quality signal**.
- Today the UI only surfaces segment size. Users have no way to pick the best one of 90.
- 11Lab is the first concrete use case: Mike asked Alex to score 150 interest segments to drop ~10 garbage ones and add ~20 BUK keywords while maintaining audience size (theory: keyword targeting > interest segments at the margin).

The scoring framework exists in a notebook. It needs to run on a schedule and
write to GCS so the rest of the org can build on top of it. Initial consumer
is admin users in the MNTN UI.

## 3. Plan of Action
1. Read [targeting-infra-ml#57](https://github.com/SteelHouse/targeting-infra-ml/pull/57)
   — `utils/segment` + the example notebook Alex will add.
2. Understand the inputs the scoring class needs (input dataset construction).
3. Decide hosting (per Alex: defer to me, consult Victor / Ryan):
   - Databricks scheduled notebook, OR
   - Vertex AI job triggered by Airflow DAG, OR
   - Regular PySpark on GCP compute in an Airflow DAG.
4. Define the schedule: **weekly or monthly** (LiveRamp segments update on
   LiveRamp's cadence — not daily). Monthly likely sufficient; confirm with Alex.
5. Define the GCS output path + schema. Coordinate with Macie on consumer expectations.
6. Run once end-to-end, verify outputs against Alex's manual notebook run.
7. Stand up the recurring schedule.

## 4. Investigation & Findings

### Scope
- ~252,075 LiveRamp segments total. Goal: produce a 0–100 quality score per `dscid` (data_source_category_id) on a recurring schedule.

### Sampling foundation (Bernoulli + Horvitz–Thompson)
Full IPDSC explode (date × IP × dscid) is prohibitively expensive. The pipeline keeps a Bernoulli-sampled subset of edges at rate **p = 0.0001** and applies HT-style weights so each sampled row stands for many unsampled rows.

- Row kept iff `u01(ip, dscid[, day]) < p`. `hash_scope='edge'` reuses the same draw across days; `edge_day` draws independently per day.
- `build_edges_with_weights_estimator_only` computes:
  - `m_hat = k_samp / p` — estimated total edges per (ip, day)
  - `pi_ipday = 1 − (1−p)^m_hat` — HT inclusion probability
  - `rep_ipday = 1 / max(pi_ipday, floor)` — IP-day weight (with optional cap)
  - `p_edge` carried per edge for segment-reach sums (`1/p_edge`)
- Pair-level intersections weight by `1/p²`.

### Scoring components (canonical Update version, 9 axes)
Each axis is computed per `dscid`, then transformed (log1p / logit / −log1p(CV)) so direction is "higher is better," z-scored across the cohort (winsorized at ±5σ, nulls → z=0), and combined.

| Axis | Default weight | What it measures | Where computed |
|------|---:|---|---|
| activity | 20.0 | Sum of daily HT reach over 30d window. Weak signal if a segment barely appears. | `segment_quality_utils/reach.py` |
| stability | 9.0 | Coefficient of variation (sd/mean) of daily reach over 14d. Low CV = steady. Composite uses `−log1p(cv14)`. | reach.py |
| share | 5.0 | Σ reach_hat / Σ N_ip_hat over the window. Avoids "ubiquitous" segments dominating. Composite uses `logit`. | reach.py |
| uniqueness | 25.0 | Mean Jaccard to top-5 neighbors (intersection-ranked from both sides). `uniqueness_unit = 1 − mean_topk_jaccard` then logit. | `segment_quality_utils/uniqueness.py` |
| sample | 9.0 | Effective sample size proxy: distinct sampled (dscid, ip, event_date) hits. Fidelity check against HT extrapolation. | reach.py |
| staleness | 2.0 | Exponential decay from last `updated_date` (90d half-life), fallback to `created_date`. `deprecated=True` → 0. Missing date → 0. | `segment_quality_utils/attributes.py` |
| specificity | 30.0 | IDF: treat each IP-day as a document, each segment as a term. `idf = log((N+1)/(k+1))`; `idf_norm = idf / log(N+1)`. Rare segments score higher. | attributes.py |
| **targetability** | **20.0** | HT-weighted reach restricted to IPs in `targetable_ips_df` ÷ total HT reach for the segment. Filters "waste" reach outside our addressable graph. (**NEW vs Archive**) | attributes.py |
| **performance** | **12.0** | Optional. Association of segment with campaign delivery outcomes (visit lift, conversion lift, consistency, support, specificity-of-targeting). Skipped if `performance_df` / `campaign_segment_targets` missing → `z_performance = 0`. (**NEW vs Archive**) | `segment_quality_utils/performance.py` |

### Final composite mapping (Update version)
```
z_combo      = Σ weight_i * z_i           # weighted sum of axis z-scores
z_combo_std  = standardize(z_combo)        # within cohort
quality_score = round(100 * sigmoid(z_combo_std / 1.5), 1)
```
Archive version used percentile rank of `z_combo`; the Update switched to sigmoid so extreme cohorts don't pile at 0 or 100.

### Performance layer mechanics (when inputs present)
1. **`build_campaign_segment_targets_panel`** — join active campaigns to their segment target rows; assign `target_weight` by crowdedness (`equal | 1/n | 1/√n`). Derive visit rate, conversion rate, post-visit CVR, ROAS, cost metrics.
2. **`add_campaign_performance_lifts`** — for each campaign, compare rates to **advertiser-level leave-one-out baseline** (other campaigns of the same advertiser, excluding this one; fallback to global). Clipped log lifts to limit outlier leverage.
3. **`summarize_performance_by_segment`** — roll up to `dscid`: weighted-avg clipped log lifts, medians, dispersion, advertiser/campaign counts, "campaign_equiv" mass, spend/impression support, top-advertiser concentration (HHI), targeting specificity.
4. **`performance_score_per_segment`** — map features to unit scores `u_*` (sigmoid of median log lifts, consistency, support, targeting specificity) → logit → cohort z → weighted `z_perf_combo`. Apply `perf_diversity_confidence = 1 − top_advertiser_campaign_equiv_share` to down-weight scores dominated by one advertiser. Final `performance_blend_signal = z_perf_combo_std × perf_diversity_confidence` becomes `x_performance` in the main composite.

Performance sub-weights (inside the performance score, not the main composite):
| Sub-component | Default |
|---|---:|
| visit_lift | 35.0 |
| conversion_lift | 27.0 |
| consistency | 18.0 |
| support | 12.0 |
| specificity (crowded targeting) | 8.0 |

**Caveat (Alex's own callout in the doc):** these are lifts vs LOO/global baselines, not randomized holdouts. Selection effects (budget, creative, geo) can drive segment-vs-baseline differences. Performance is a **supportive** signal, not a causal one.

### Code layout (Update doc)
| Topic | File |
|-------|------|
| Sampling (Bernoulli, HT panel) | `sampling_logic.py` (a.k.a. `sampling_quality_sampling.py`) |
| Shared helpers, default weights | `segment_quality_utils/base.py` |
| Reach / activity / volatility / share / ESS | `segment_quality_utils/reach.py` |
| Top-k Jaccard | `segment_quality_utils/uniqueness.py` |
| Specificity, staleness, targetable share | `segment_quality_utils/attributes.py` |
| Performance panels / scores | `segment_quality_utils/performance.py` |
| End-to-end composite | `segment_quality_utils/composite.py` |
| Notebook façade | `segment_quality_utils/facade.py` |

Entry point: `ThirdPartySegmentQuality(panel).quality_score_per_segment(...)` — accepts `weights`, `targetable_ips_df`, performance inputs.

### Inputs the scheduled job will need

Confirmed against PR #57 source (`facade.py`, `base.py`, `sampling_logic.py`, `attributes.py`, `performance.py`).

#### Required

1. **Panel** — output of `build_edges_with_weights_estimator_only(ipdsc_df, p=0.0001, hash_scope='edge_day', date_col='event_date')`. Produces schema `ip, event_date, dscid, k_samp, m_hat, pi_ipday, w_ipday, rep_ipday, p_edge, w_edge`.
   - **Source:** `dw-main-bronze.external.ipdsc__v1` filtered to **LiveRamp (`data_source_id = 35`)** and the desired window.
   - **Required columns on `ipdsc_df`:** `ip` (string), `data_source_category_ids` (array<bigint>), date column (`dt` is auto-cast).
   - **`data_source_category_ids` shape:** RECORD with nested `.list[].element` (INTEGER). Must flatten to `array<bigint>` before passing — `sample_ipdsc_edges` calls `F.transform(data_source_category_ids, ...)` directly.
   - **Scale:** ~103M LiveRamp rows in `ipdsc__v1` per day (2026-05-20 sample). At p=0.0001 over 30 days → ~310k sampled edges. Panel cost is manageable.
   - **TTL note:** `ipdsc__v1` is partitioned by `dt`. Confirm retention covers 30d primary + 14d volatility windows.

2. **`seg_meta_df`** — `dw-main-bronze.tpa.categories` filtered to `data_source_id = 35`.
   - **Required columns (used by `segment_staleness`):** `data_source_category_id` (INTEGER → cast to long as `dscid`), `updated_date` (DATE), `created_date` (DATE), `deprecated` (BOOLEAN).
   - **Confirmed counts (2026-05-28):** 428,372 rows, 428,135 distinct `dscid`, 214,743 deprecated → ~213k active LiveRamp segments. (Design doc says ~252k; difference is likely an older snapshot or different filter.)
   - **Coverage:** `created_date` from 2022-03-30, `updated_date` through 2026-05-28.

3. **`targetable_ips_df`** — distinct list of IPs in MNTN's addressable universe.
   - **Required columns:** just `ip` (`segment_targetable_share_30d` calls `.select(F.col("ip")).distinct()`).
   - **Weight in composite:** 20.0 (tied for second-highest — significant impact on scores).
   - **Semantic constraint:** must be **stricter than IPDSC itself**. If `targetable_ips_df` ⊇ all IPs in `ipdsc__v1`, then `pct_targetable_30d = 1.0` for every segment and the axis becomes useless. The point is to distinguish segments whose IPs we can actually act on from segments dominated by IPs we never deliver to.
   - **Candidate comparison (scoped 2026-05-28):**
     | Candidate | n_ips | Cost (30d) | Reading |
     |---|---:|---:|---|
     | `dw-main-silver.logdata.impression_log` distinct `bid_ip`, 30d | 66.4M | ~130 GB scan | "IPs we actually delivered to in the window" — tightest, matches the activity-axis temporal window |
     | `dw-main-bronze.tpa.graph_ips_aa_100pct_ip` distinct `ip` (no date) | 245M | ~20 GB scan | Full identity-graph universe (147M households) — too broad; includes IPs we've never delivered to |
     | `bid_logs` distinct IPs, 30d | TBD | not scoped (90d TTL) | RTB-eligible universe (everything we got a bid request for) — broader than delivered, narrower than graph |
     | audience-platform expression resolution | TBD | TBD | "IPs an active campaign expression would currently include" — most product-aligned but complex to extract |
   - **Recommendation (to bring to Alex + Zach):** start with `impression_log` 30d distinct `bid_ip`. It's cheap, matches the activity-axis window, and is unambiguous in semantics ("we delivered here"). Bid-logs is the obvious next step if Alex wants "could have delivered" rather than "did deliver."
   - **Authority memory `[[reference_audience_platform_authority]]`:** Zach Schoenberger owns audience-platform / addressable questions — get a sanity check before committing.

#### Optional (performance layer — adds weight 12.0 to composite)

4. **`performance_df`** — campaign-level delivery KPIs over the window.
   - **Required columns** (per `build_campaign_segment_targets_panel`): `advertiser_id`, `campaign_id`, `total_spend`, `impressions`, `visits`, `conversions`, `revenue`.
   - **Candidate sources:**
     - `dw-main-silver.aggregates.agg__daily_sum_by_campaign` — campaign × day aggregates. Effective start 2025-09-01 per memory.
     - `dw-main-silver.summarydata.sum_by_campaign_by_day` — goes back to 2024-01-01; better for longer windows.
   - **Roll-up:** sum across the chosen window per `(advertiser_id, campaign_id)`. Cap to active campaigns in window.

5. **`campaign_segment_targets`** — which campaigns target which LiveRamp dscids.
   - **Required columns:** `advertiser_id`, `campaign_id`, `data_source_category_id` (cast to `dscid`).
   - **Candidate source:** `audience.audience_segments` (per memory: "actual targeting expressions, NOT `audience.audiences` which are templates"). Need to extract `data_source_category_id` literals from expression nodes filtered to `data_source_id = 35`.
   - **Open:** confirm the expression-parsing path with Zach (audience-platform authority).

6. **Sample notebook** — Alex committed to adding a usage notebook to the repo. Not in PR #57 head as of 2026-05-28 (12 files, no `.ipynb`). Track for next Alex sync.

#### Optional bypass
- Pass `perf_seg_score_df=...` directly to `quality_score_per_segment` to skip recomputation if performance scores are precomputed elsewhere.

### Hosting trade-offs (notes for Victor/Ryan consult)

Three real options. Decision should be made in the early-next-week tech deep-dive after consulting Victor or Ryan — Alex deferred. Memory `[[reference_databricks_for_heavy_queries]]` says we have memory-optimized clusters for shuffle-heavy Spark; memory `[[reference_airflow_ti]]` describes the airflow-ti feature-store deployment pattern.

| Dimension | Databricks scheduled notebook | Vertex AI + Airflow DAG | Airflow + PySpark (Dataproc) in airflow-ti |
|---|---|---|---|
| Iteration speed | Fastest — same env Alex prototyped in | Moderate — Docker/IAM each iteration | Slowest — PR + deploy each iteration |
| Standardization | Off-pattern for TI prod (one-off) | Off-pattern for TI prod (one-off) | On-pattern (matches feature-store DAGs) |
| Code review | Notebook can hold imports of `segment_quality_utils`; the package is reviewed in PR #57 either way | Same | Same — plus DAG code itself is in PR |
| Compute fit | Memory-optimized clusters; uniqueness pairwise step covered | Dataproc Serverless under the hood — fine for PySpark | Dataproc on-demand cluster per run — fine |
| GCS write | DBFS mount or direct GCS write | Direct GCS write | Direct GCS write |
| Ops/monitor | Databricks Workflows alerts + dashboards | Airflow alerts + Vertex job logs | Airflow alerts |
| Ownership | DS-leaning (Alex's territory) | Split DS / data-eng | Data-eng owned |
| Notebook→deck flow | Alex already shows Fangorn dashboards from a Databricks notebook (meeting 5/27) | N/A | N/A |

**My initial read** (subject to Victor/Ryan input):
- For the explicit "interim, not production" framing in the ticket → **Databricks scheduled notebook** is the path of least resistance. Same system Alex already uses, fastest iteration while Alex is still tuning weights, scheduling + GCS write are first-class. Macie can consume the GCS output the same way regardless of host.
- For "build it like production from day one" → **Airflow + PySpark in airflow-ti** matches the feature-store pattern Ryan owns. Alex's "good opportunity to learn pipelining" hint points this way.
- **Vertex AI in the middle** adds Docker/IAM overhead without the airflow-ti integration — likely the wrong trade-off unless someone makes a specific case for it.

**Question to bring to Victor/Ryan:** for an interim DS-led scoring job whose output will be consumed by an admin UI, do you prefer Databricks now and migrate later, or put it in airflow-ti from the start? Either is defensible; team norms drive the answer.

**airflow-ti safety reminder (`[[feedback_airflow_prod_safety]]`):** if Option 3, models go in feature branches; Ryan wires DAG deps; never push to main directly.

### Malachi suggestions for additional metrics (not yet in v1)
- **Visits per user** over 30-day window
- **Impact-weighted ranking** — for campaigns currently using interest segments, what would be the perceived improvement if they switched to the #1-ranked alternative? Helps prioritize which scores to surface where.
- **Filter aggressively in the UI** — don't show users 90 choices. Use the quality score to cut low-quality segments before surfacing.

## 5. Solution

### Implementation plan (revised 2026-06-04 after Victor pushback)

**Architecture:** airflow-ti DAG → Dataproc Serverless PySpark batch → Iceberg table on GCS, BigLake-cataloged for BQ read access.

**Why this and not Databricks** (per Victor 2026-06-04 + agent re-read of PR #57): Alex's code is pure PySpark with zero Databricks lock-in (no `dbutils`, no DBFS, no magic). Data lives in BQ; the BQ → compute → BQ round-trip costs are similar regardless of compute backend (both Databricks and Dataproc run on GCE). Choosing Databricks would mean paying for a second compute platform with no benefit. Dataproc Serverless scales to zero between weekly runs and matches MNTN's existing orchestration patterns.

**Why not SQLMesh:** ~2,000 lines of PySpark including a pairwise Jaccard self-join + HT estimator + multi-stage z-score composites. Multi-week rewrite to SQL and the Jaccard step alone is hard to make cost-competitive in BQ. Would mean throwing out Alex's framework.

**Why Iceberg + BigLake (vs BQ-managed table):** reading ipdsc directly from GCS Parquet skips the BQ external-table overhead per query. Writing output to Iceberg + BigLake gives BQ query access without re-ingest, and Iceberg's partition overwrite semantics make reruns idempotent.

**What's been built (this ticket):**

1. **Operative-3P extraction SQL** — `queries/ti_956_operative_3p_campaign_segments.sql`. Embeds the Pass 26 LCA classifier as a TEMP FUNCTION; returns one row per (campaign × dscid × polarity) tuple where the 3P clause actually drives delivery. Drops bidder-inert theater (MM + 3P-OR-incl). The TI-999 contribution — without this filter, the performance layer's segment-quality KPIs would reflect MM-only delivery with a 3P label slapped on.
2. **Dataproc Serverless PySpark job** — `artifacts/ti_956_segment_quality_scoring_job.py`. End-to-end:
   - Reads ipdsc from `gs://<ipdsc-bucket>/dt=YYYY-MM-DD/data_source_id=35/*.parquet` (GCS direct, no BQ external-table layer)
   - Reads seg_meta / targetable_ips / performance / operative_3p (campaign×dscid) from BQ via the Spark connector
   - Calls `build_edges_with_weights_estimator_only` to construct the HT panel
   - Invokes `ThirdPartySegmentQuality(panel).quality_score_per_segment(...)` with the theater-filtered targets
   - Adds `size_distinct_ips`, `quality_rank`, `anti_quality_rank`, `size_rank` (global dense ranks) — UI team requirement
   - Adds `low_confidence_flag` (`ess_30d < 100`) for downstream de-ranking
   - Writes via `.writeTo(ICEBERG_TABLE).overwritePartitions()` — idempotent rerun of the same `as_of_date` partition
   - Smoke validation (row count, score distribution, rank monotonicity, low-confidence share)
   - Self-contained: SQL inlined; argparse-driven (`--as_of_date`, `--window_days`, `--sample_rate`)
3. **airflow-ti DAG stub** — `artifacts/ti_956_airflow_ti_dag_stub.py`. Template for Victor to drop into the airflow-ti repo. `DataprocCreateBatchOperator` submits the PySpark batch weekly (Sunday 06:00 UTC) with the right Iceberg + BigLake + BQ-connector jar packages. Connection IDs, service account, subnet config left as placeholders for Victor's conventions.

**Resolved values from airflow-ti convention scan (2026-06-04):**

| Concern | Value | Source |
|---|---|---|
| **ipdsc GCS path** | `gs://mntn-data-archive-prod/ipdsc/dt={DATE}/data_source_id={DS}/*.parquet` | Confirmed via `bq show --format=prettyjson dw-main-bronze:external.ipdsc__v1` — Hive-partitioned by `dt` + `data_source_id` |
| **Iceberg catalog type** | `bigquery` (BigQuery Metastore — NOT BLMS) | `airflow-ti/utils_model/base_model/compute_component.py` |
| **Iceberg catalog name** | `DW_MAIN_BRONZE` (prod) / `MNTN_PRJ_DEV_00` (dev) — uppercased project_id w/ underscores | Same |
| **Catalog config keys** | `spark.sql.catalog.<NAME>.type=bigquery`, `.gcp.bigquery.project-id=<project>`, `.gcp.bigquery.location=us-central1` | Same |
| **Iceberg warehouse path** | Per-table via `tableProperty("location", "gs://mntn-data-archive-{env}/airflow_vs/{env}/<layer>/<model_id>/")` — NOT a global warehouse config | `airflow-ti/utils_model/base_model/writer_iceberg.py` |
| **Iceberg jar versions** | `iceberg-bigquery-1.10.2.jar`, `iceberg-gcp-1.10.2.jar`, `iceberg-gcp-bundle-1.10.2.jar`, `iceberg-spark-runtime-3.5_2.13-1.10.2.jar` — pre-staged in `gs://mntn-data-archive-prod/ti_resources/spark/drivers/` | Same |
| **Jar loading** | `spark.jars=<GCS uris>` — NOT `spark.jars.packages` (no Maven resolution at runtime) | Same |
| **Dataproc Serverless runtime** | `"2.3"` (use `"3.0"` with `spark_scala="4.0_2.13"`) | `airflow-ti/utils_runner/dataproc.py` |
| **Region** | `us-central1` | Same |
| **Dev project** | `mntn-prj-dev-00` | Same |
| **Dev service account** | `airflow-ti-dev@mntn-prj-dev-00.iam.gserviceaccount.com` | Same |
| **Dev subnet** | `projects/mntn-host-ntwrk-nonprod-00/regions/us-central1/subnetworks/mntn-dev-prj-snet-central1` | Same |
| **Network tags (dev)** | `["dataproc-dev"]` | Same |
| **Required runtime env** | `MNTN_RUNTIME_ENV` propagated via `spark.dataproc.driverEnv.MNTN_RUNTIME_ENV` + `spark.executorEnv.MNTN_RUNTIME_ENV` | Same |
| **batch_id format** | `<model-id-dashes>-local-YYYYMMDD-HHMM` | Same |
| **Labels schema** | `{"team": "ti", "application": "<short_name>"}` | Same |
| **Path convention** | `gs://mntn-data-archive-{dev\|prod}/airflow_vs/{env}/<layer>/<model_id>/` | Same |
| **Default landing schema (dev)** | `mntn-prj-dev-00.spark_bq` | Same |

These values are now baked into `artifacts/ti_956_segment_quality_scoring_job.py` and `artifacts/ti_956_airflow_ti_dag_stub.py` — the dev/prod split is driven off `MNTN_RUNTIME_ENV`.

**Still unresolved — confirm with Victor:**

1. **Prod service account + subnet** — agent only found dev values in airflow-ti. What's the prod equivalent of `airflow-ti-dev@mntn-prj-dev-00.iam.gserviceaccount.com` / the dev subnet?
2. **spark-bigquery connector jar** — NOT pre-staged in `ti_resources/spark/drivers/` for Iceberg jobs. Our job's BQ reads (seg_meta, targetable_ips, performance, operative_3p) need this connector. Options: (a) add `com.google.cloud.spark:spark-3.5-bigquery:0.34.0` via `spark.jars.packages` only for this jar (mixing with `spark.jars`), (b) stage it alongside the Iceberg jars in GCS, (c) refactor those 4 BQ reads to load via the Iceberg catalog if those tables are Iceberg (they're not today).
3. **Optional refactor onto `IcebergBigqueryMntnPrjDevModel`** — airflow-ti has a base-class + `@compute.dataproc_batch` + `@model_config` decorator pattern that abstracts the Spark/catalog config. More idiomatic but requires familiarity with the framework. The current artifacts use raw `DataprocCreateBatchOperator` submission — both work, Victor's call.

**What Victor still needs to set up:**

1. **Upload the job + utils bundle to GCS:**
   ```
   gs://mntn-data-archive-dev/airflow_vs/dev/code/ti_956/ti_956_segment_quality_scoring_job.py
   gs://mntn-data-archive-dev/airflow_vs/dev/code/ti_956/utils.zip   # bundled targeting-infra-ml/utils/
   ```
2. **Confirm Iceberg table creation idiom.** The job uses `writeTo(...).create()` on first run with `tableProperty("location", <gcs path>)`. Alternative: pre-create via Spark SQL DDL. Pick one; both supported by airflow-ti convention.
3. **airflow-ti DAG deploy** — drop `artifacts/ti_956_airflow_ti_dag_stub.py` into `dags/ti/` on a feature branch (per `[[feedback_airflow_prod_safety]]`). Ryan reviews + wires deps.
4. **Alerts** — DataprocCreateBatchOperator emits failure events through Airflow; route to TI Slack. Late-data alert if no Iceberg partition for >9 days.

**Pre-flight checklist:**

- [ ] PR #57 merged in `SteelHouse/targeting-infra-ml` (currently `mergeable_state: blocked` since 2026-05-14, needs theastrocat / Ryan review)
- [ ] Alex publishes benchmarks (cluster size, wall-clock at p=1e-4, dscid count) — pairwise Jaccard is the cost hot-path with no published numbers yet
- [ ] Iceberg table created (DDL above)
- [ ] First Dataproc batch run end-to-end, smoke validation passes
- [ ] airflow-ti DAG deployed on feature branch + reviewed
- [ ] Macie confirms output schema matches admin UI needs

### Storage plan

| Surface | Location | Refresh | Consumer |
|---|---|---|---|
| Primary (Iceberg, GCS-backed) | `gs://household-scoring-prod/iceberg/household_scoring/segment_quality_daily/` | Weekly Sun 06:00 UTC | Dataproc / Spark consumers |
| BQ read access | `biglake.household_scoring.segment_quality_daily` via BigLake external | Same — no re-ingest | UI team, Mode dashboards, admin tooling |
| GCS Parquet export (optional, for Macie) | `gs://household-scoring-prod/output/segment_quality/year=YYYY/month=MM/day=DD/*.parquet` | Same | Macie / non-BQ consumers |

Idempotent reruns: `writeTo(...).overwritePartitions()` replaces just the `as_of_date` partition. Earlier weekly snapshots remain intact for backfill / audit.

## 6. Questions Answered
- **Q:** Why does this need a schedule rather than ad-hoc reruns?
  **A:** Consumers (Macie + admin UI) need fresh scores without asking Alex. LiveRamp segments update on their cadence — not daily, but they do update.
- **Q:** Daily / weekly / monthly?
  **A:** Weekly or monthly. Not daily — LiveRamp doesn't change that fast.
- **Q:** Where does output go?
  **A:** GCS path. Macie will consume it to build an admin-user-facing surface in the MNTN UI.
- **Q:** Is this production-grade?
  **A:** No. Interim step — get scores accessible to admin users. Productionization is a later ticket.
- **Q:** Where does the scoring code live?
  **A:** `targeting-infra-ml` PR #57, `utils/segment`. PySpark class, two lines to invoke.

## 7. Data Documentation Updates
_Pending._
- Possible: add LiveRamp interest segment scoring inputs/outputs to `knowledge/data_knowledge.md` once schema is settled.

## 8. Open Items / Follow-ups

### Resolved during 2026-05-28 PR-read pass
- ✅ **PR #57 read end-to-end.** Confirmed entry point `ThirdPartySegmentQuality(panel).quality_score_per_segment(seg_meta_df, targetable_ips_df=..., performance_df=..., campaign_segment_targets=...)`. No notebook in PR yet — Alex still owes one.
- ✅ **`tpa.categories` schema pinned.** Has `data_source_category_id, updated_date, created_date, deprecated` — exact columns `segment_staleness` expects. LiveRamp filter is `data_source_id = 35` (428k categories, 213k non-deprecated).
- ✅ **`ipdsc__v1` LiveRamp filter identified.** `data_source_id = 35` ("LiveRamp IP" in `bronze.integrationprod.data_sources`). ~103M rows/day; p=0.0001 → ~10k sampled edges/day → ~310k panel rows over 30d.
- ✅ **`targetable_ips_df` candidate analysis.** Recommendation: `impression_log` distinct `bid_ip` over the 30d window (66.4M IPs). Full identity graph (245M) is too broad — would dilute the axis. See §4 Input source mapping for details.
- ✅ **Hosting trade-offs documented.** Three options compared; awaiting Victor/Ryan input.

### Resolved during 2026-05-28 PM — Finding 15 empirical bidder-semantics gate
- ✅ **Bidder treats inclusion as OR-additive, exclusion as AND-NOT.** Prior verbal "AND-intersection across all clauses" model refuted for inclusion. `MM + 3P incl_only` delivers 23.3% unscored impressions vs `MM_only`'s 4.2% (only OR-additive explains the increase). See [TI-999 Finding 15 Pass 3](../ti_999_interest_segment_sizing/summary.md).
- ✅ **3P inclusion is NOT dead weight.** TI-956's per-segment quality score has a real, measurable prize zone:
  - MM + 3P incl_only: $643K / 30d on unscored IPs reached via 3P inclusion → ~$7.7M annualized
  - Pure 3P_only: $2.15M / 30d on unscored IPs → ~$25.8M annualized
  - Combined: ~$50M+/year of delivery currently flowing to IPs the household score knows nothing about
- ✅ **Phase 1 LiveRamp-only scope stays right.** LiveRamp = 97% of active 3P categories. ShareThis + Dstillery are categorically stale and a smaller prize.
- ✅ **Updated elevator pitch:** "MM scores IPs at bid time. Nothing scores segments. Today buyers add 3P inclusion clauses to MM campaigns blindly — the bidder reaches the 3P IPs (~23% of delivery on unscored IPs), but buyers can't tell good segments from bad. TI-956 makes ~$50M/year of currently-blind delivery quality-informed."

### To resolve in Alex tech deep-dive (early next week)
1. **What did Alex use for `targetable_ips_df` in his Databricks notebook?** If different from `impression_log` 30d, understand why — may reveal a constraint I'm missing.
2. **Performance layer in v1: yes or no?** Skipping reduces complexity (no `performance_df` / `campaign_segment_targets` plumbing) but loses 12.0 weight + the lift signals. Alex's own caveat in the design doc says performance is supportive, not causal. Lean: skip v1, add v2 once admin UI is live and consumers ask.
3. **Where does `campaign_segment_targets` come from?** If we include performance, need to extract `advertiser_id × campaign_id × dscid` from audience expressions filtered to `data_source_id = 35`. Likely `audience.audience_segments` — confirm with Zach.
4. **Cadence: weekly vs monthly?** LiveRamp doesn't change daily but `updated_date` *is* per-segment. Weekly probably aligns better with `tpa.categories` refresh; monthly is cheaper. Alex's call.
5. **Output schema confirmation.** Minimum proposal: `dscid, quality_score, z_combo, z_combo_std, z_activity, z_stability, z_share, z_uniqueness, z_sample, z_staleness, z_specificity, z_targetability, z_performance` + raw component values (`reach_hat_30d, cv14, avg_share_30d, ess_30d, mean_topk_jaccard_30d, idf_norm, staleness_unit_score, pct_targetable_30d`) + `as_of_date`. Confirm with Macie what the admin UI needs.

### To resolve with Victor/Ryan
6. **Host decision.** See §4 Hosting trade-offs table for the framing question.

### To resolve with Macie
7. **GCS output path + format.** Parquet vs JSON vs CSV. Single file vs partitioned by `as_of_date`. Naming convention.

### Lower priority
- Should v1 surface impact-weighted ranking metrics (Malachi suggestion §4), or hold for v2?
- `ipdsc__v1` retention: confirm 30d primary + 14d volatility windows are always available (no TTL surprise).
- After first end-to-end run, sanity-check scores against Alex's manual notebook outputs.

## 9. Meeting Notes
- `meetings/ti_956_01_malachi_alex_catchup_2026_05_27.txt` — same Malachi + Alex catchup. Interest-segment scoring is the second half of the meeting.
- **Next step:** Alex to add a usage-notebook example to the repo so Malachi can read through it. Tech deep-dive on hosting + schedule scheduled for early next week.

## Acceptance Criteria (from Jira)
- Data pipeline running which generates LiveRamp interest segment scores.
