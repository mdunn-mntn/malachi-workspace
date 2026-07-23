---
doc_type: ticket
title: "TI-956: Scheduled Interest Segment Scoring Job"
status: in_progress
date: 2026-06-09
summary: "Run LiveRamp interest-segment quality scoring weekly, output to GCS and BigQuery"
result: "in progress — airflow-ti model + DAG merged; first Dataproc run pending wheel-fix deploy"
keywords: [ti-956, segment quality scoring, liveramp, ds35, ipdsc__v1, horvitz-thompson, targetability, airflow-ti, dataproc, iceberg, driverpippackages, pyfiles, targeting-infra-ml, household_scoring]
---

## TL;DR

**Q:** TI-956: what is the scheduled interest-segment scoring job, its architecture/inputs, and current status?

**A:** TI-956 runs Alex's LiveRamp interest-segment quality scoring (targeting-infra-ml PR #57) on a recurring schedule so consumers (Macie / admin UI) get fresh 0-100 per-segment scores without asking Alex to rerun the notebook. Scope: ~252,075 LiveRamp segments; goal is one quality score per dscid.

Method: Bernoulli-sampled IPDSC edges at p=0.0001 with Horvitz-Thompson weights, then 9 z-scored axes (activity 20, stability 9, share 5, uniqueness 25, sample 9, staleness 2, specificity 30, targetability 20 [NEW], performance 12 [NEW/optional]) combined into z_combo -> quality_score = 100*sigmoid(z_combo_std/1.5). Inputs: panel from bronze.external.ipdsc__v1 filtered to LiveRamp (data_source_id=35); seg_meta_df from bronze.tpa.categories (DS35); targetable_ips_df (recommend impression_log 30d distinct bid_ip); optional performance_df + campaign_segment_targets.

Architecture (revised after Victor 1:1, 2026-06-05): airflow-ti model class subclassing IcebergBigqueryDwMainBronzeModel -> Dataproc Serverless PySpark batch -> Iceberg table on GCS, BigQuery-Metastore-cataloged. Output: dw-main-bronze.household_scoring.segment_quality_daily, weekly Sunday 06:00 UTC. Cross-repo dep resolved via GCS-hosted wheel (targeting_infra_ml wheel) rather than Artifact Registry (TI-1023 backlog).

Status: in progress. Model class + DAG merged to airflow-ti (PRs #1064/#1065/#1068/#1073); pyproject/wheel built + uploaded; Astronomer deploy shipped. First Dataproc trigger FAILED (ModuleNotFoundError: 'utils' — spark.dataproc.driverPipPackages silently ignored the GCS wheel URL); fix pivoted to spark.submit.pyFiles (branch TI-956-fix-wheel-install, merged). First real Dataproc run + smoke validation still pending re-trigger. Open: cluster sizing on first run, cadence (weekly vs biweekly), performance layer in v1 (leaning skip), output schema confirmation with Macie.

**How:** Read the full summary.md and listed queries/ (only ti_956_operative_3p_campaign_segments.sql present); grepped the four knowledge docs for candidate durable facts.

**Tables:** bronze.external.ipdsc__v1, dw-main-bronze.tpa.categories, dw-main-silver.logdata.impression_log, dw-main-bronze.tpa.graph_ips_aa_100pct_ip, dw-main-silver.aggregates.agg__daily_sum_by_campaign, dw-main-silver.summarydata.sum_by_campaign_by_day, dw-main-bronze.household_scoring.segment_quality_daily, audience.audience_segments

**Learned:**
- Scoring uses 9 z-scored axes; targetability (weight 20) and performance (weight 12, optional) are NEW in the canonical Update version vs the 7-component Archive; final map switched from percentile rank to sigmoid of standardized z_combo
- IPDSC pipeline samples edges at p=0.0001 with Horvitz-Thompson weights (pi_ipday=1-(1-p)^m_hat) to make full IP x dscid explode affordable
- targetable_ips_df must be STRICTER than ipdsc__v1 itself or pct_targetable_30d=1.0 for every segment and the axis is useless; recommended impression_log 30d distinct bid_ip (66.4M IPs), not the full identity graph (245M IPs)
- Architecture landed as an airflow-ti model class (IcebergBigqueryDwMainBronzeModel) writing Iceberg to GCS, BQ-Metastore-cataloged; closest template is models/machine_learning/fangorn_14day_lookback.py
- spark.dataproc.driverPipPackages expects PyPI package specifiers, not GCS wheel URLs — it silently ignores a gs:// URL; use spark.submit.pyFiles for a GCS-hosted wheel
- Model + DAG are separate merges in airflow-ti; DAG scheduled weekly Sunday 06:00 UTC via ModelPysparkBatchOperator

**Reuse when:**
- scheduling or productionizing Alex's segment_quality_utils / interest-segment scoring
- picking a targetable_ips_df universe for scoring
- deploying a PySpark job to airflow-ti / Dataproc with a cross-repo Python dependency
- questions about LiveRamp DS35 segment quality scoring for admin UI / Macie
- quantifying the 3P-inclusion overflow prize (~$50M/yr) that segment scoring informs

---

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

### Implementation plan (revised 2026-06-05 after Victor 1:1)

**Architecture:** airflow-ti model class (`IcebergBigqueryDwMainBronzeModel`) → Dataproc Serverless PySpark batch → Iceberg table on GCS, BigQuery Metastore-cataloged for BQ read access.

**Pattern (per Victor 1:1, 2026-06-05, transcript: `meetings/ti_956_02_malachi_victor_dataproc_2026_06_05.txt`):**

Use airflow-ti's model abstractions instead of hand-rolling Spark session + Iceberg jar versions + catalog config. The base class + decorators hide all the version/library complexity ("Iceberg different version of Spark and different version of Scala which you run it on can require different set of libraries and configurations so it can get tricky real fast — we have this addressed behind this model type"). All reads go through `self.read_model(...)`; all writes through `self.df_write(df)`. Compute (DataprocBatch / DataprocWorkflow / Databricks) selectable via the `@compute.*` decorator.

Verbatim from the meeting (line 297): *"I'm here just to advise that our latest updates on what we have... you implement the way you see fit."* — Victor didn't block any approach; he showed the more idiomatic path. We're taking it.

**Reference patterns** (from airflow-ti `models/`):
- **`models/machine_learning/fangorn_14day_lookback.py`** — closest template. Same base class (`IcebergBigqueryDwMainBronzeModel`), same `dw-main-bronze.household_scoring` target schema, same partitioned-Iceberg-write idiom (`table_exists()` → `overwritePartitions()` / `create()` with table properties).
- **`models/ipdsc/ipdsc_ds_47.py`** — BQ read pattern (`self.read_model("bigquery_data.BQ").option("parentProject", ...).option("billingProject", ...).query(...).load()`).

### Old artifacts (REMOVED 2026-06-05 — superseded)

These were the pre-Victor-meeting standalone approach. Deleted because the airflow-ti model class consolidates them:

- ~~`artifacts/ti_956_segment_quality_scoring_job.py`~~ (standalone Dataproc PySpark job, hand-rolled Spark + Iceberg config) — replaced by the model class which inherits all that config from `IcebergBigqueryDwMainBronzeModel`.
- ~~`artifacts/ti_956_airflow_ti_dag_stub.py`~~ (raw `DataprocCreateBatchOperator` template) — replaced by airflow-ti's standard model-scheduling operators that take a model alias.

### What's been built (this ticket)

1. **Operative-3P extraction SQL** — `queries/ti_956_operative_3p_campaign_segments.sql`. Embeds the Pass 26 LCA classifier as a BQ TEMP FUNCTION; returns one row per (campaign × dscid × polarity) tuple where the 3P clause actually drives delivery. Drops bidder-inert theater (MM + 3P-OR-incl). The TI-999 contribution to TI-956. The same SQL is inlined as `OPERATIVE_3P_QUERY` inside the model file for deployment self-containment.
2. **airflow-ti model class** — `artifacts/ti_956_segment_quality_scoring_model.py`. End-to-end:
   - `@compute.dataproc_batch(...)` + `@model_config(alias="ti_956_segment_quality_daily", ...)` decorators
   - Subclasses `IcebergBigqueryDwMainBronzeModel` (output goes to `dw-main-bronze.household_scoring`)
   - `__init__` parses `--as_of_date`, builds the SparkSession with Iceberg extensions
   - `model()` runs the 7-step pipeline (read 5 inputs → build HT panel → score → rank → write → lifecycle hooks → validate)
   - All BQ reads use `self.read_model("bigquery_data.BQ").query(...).load()` per Victor's preferred pattern
   - Iceberg write uses `self.df_write(df).overwritePartitions()` (subsequent runs) / `.create()` with table properties (first run) — mirrors Fangorn
   - Lifecycle: `create_success_file`, `delete_where` (1-year retention), `expire_snapshots`
   - Smoke validation (row count ≥50k, score range [0, 100], low-confidence share)

### Cross-repo dependency — RESOLVED 2026-06-08 (Brian McAdams suggestion)

Earlier plan was to stand up an internal Artifact Registry Python repo (TI-1023). Brian flagged a simpler v1 pattern he uses for his vault wheel on Databricks compute: **GCS-hosted wheel, installed at compute startup via `pip` properties**.

Dataproc Serverless supports the same pattern via `spark.dataproc.driverPipPackages` + `spark.dataproc.executorPipPackages` — these accept a GCS URI to a wheel file. Pip installs at startup, no AR auth gymnastics, no DevOps dependency.

**Pivoted to GCS-hosted wheel for v1.** AR is the long-term right answer once we have multiple consumers; tracked in TI-1023 (backlog).

**Wheel location** (mirrors airflow-ti's Iceberg-drivers convention at `ti_resources/spark/drivers/`):

    gs://mntn-data-archive-prod/ti_resources/python/wheels/targeting_infra_ml-{VERSION}-py3-none-any.whl

Path is a placeholder — Victor to confirm whether `ti_resources/python/wheels/` is the right subdirectory or something else.

### v1 deployment plan

| # | Step | Owner | Effort |
|---|---|---|---|
| 1 | ~~Add `pyproject.toml` to `SteelHouse/targeting-infra-ml`~~ — **MERGED 2026-06-08** ([PR #68](https://github.com/SteelHouse/targeting-infra-ml/pull/68)) | Malachi → Alex | ✅ DONE |
| 2 | ~~Build first wheel locally: `python -m build` from the repo root~~ — **DONE 2026-06-08**. Produces `targeting_infra_ml-0.1.0-py3-none-any.whl` (23 KB) | Anyone | ✅ DONE |
| 3 | ~~Upload wheel to `gs://mntn-data-archive-prod/ti_resources/python/wheels/`~~ — **DONE 2026-06-08**. Path is a sibling to `ti_resources/spark/drivers/` (Iceberg jars); Victor TBC if different preference | Malachi | ✅ DONE |
| 4 | Model file's `@compute.dataproc_batch` already wired to install from that exact path. Bump version pin in `spark.dataproc.driverPipPackages` when a new wheel is published | Done | ✅ DONE |
| 5 | ~~Drop model file into airflow-ti~~ — **DONE 2026-06-08**. Branch `TI-956` on `SteelHouse/airflow-ti` adds `models/machine_learning/segment_quality_scoring.py` (renamed to match Fangorn's un-prefixed convention; class `SegmentQualityScoring`). PR to open: https://github.com/SteelHouse/airflow-ti/pull/new/TI-956 | Malachi | ✅ DONE — awaiting PR open + review |
| 6 | ~~Open + merge the PR~~ — **MERGED 2026-06-08**. Model landed in **[airflow-ti PR #1064](https://github.com/SteelHouse/airflow-ti/pull/1064)**; wheel-install fix in **[#1068](https://github.com/SteelHouse/airflow-ti/pull/1068)**; accumulated dev-loop fixes (final code state) in **[#1073](https://github.com/SteelHouse/airflow-ti/pull/1073)**. (NB: earlier drafts of this doc + the PR bodies mis-cited "#190" — that is an unrelated wei-mntn "updated dags" PR from Sept 2025, NOT this work. Verified via GitHub 2026-07-21.) CI surfaced two issues, both fixed in-PR: (a) module-level cross-repo import broke `model_upload.py --dryrun` → moved imports inside `model()`; (b) `dags/model_task_config.json` needed regeneration → ran dryrun locally + committed. Knowledge captured in `documentation/docs/airflow_ti_workflow.md`. | Malachi | ✅ DONE |
| 7 | ~~Wait for `deploy_prod.yaml` Astronomer deploy~~ — **DONE 2026-06-08**. Deploy commit `2a5ce04` shipped the model file + regenerated config. | auto | ✅ |
| 7b | ~~Add DAG file~~ — **MERGED 2026-06-08** ([airflow-ti PR #1065](https://github.com/SteelHouse/airflow-ti/pull/1065), branch `TI-956`). `dags/machine_learning/segment_quality_scoring_dag.py` uses `ModelPysparkBatchOperator(model_id="segment_quality_scoring")`, scheduled weekly Sunday 06:00 UTC, severity=2, `--as_of_date {{ ds }}` arg. Workflow doc updated with the model+DAG split gotcha. | Malachi | ✅ DONE |
| 8 | Wire DAG on Astro (find `segment_quality_scoring_weekly` in Astro UI, trigger first run) | Malachi | 15 min |
| 8a | ~~First trigger attempt failed~~ — batch `seg-qua-sco-gi5-20260608-212616-1` died at 1m26s with `ModuleNotFoundError: No module named 'utils'`. Driver logs confirmed `spark.dataproc.driverPipPackages` was silently ignored — that property expects PyPI package specifiers, not GCS file URLs. | Diagnosed | ❌ FIXED below |
| 8b | ~~Pivoted to `spark.submit.pyFiles`~~ — **MERGED 2026-06-08**. Branch `TI-956-fix-wheel-install`. Workflow doc + memory updated with the gotcha. | Malachi | ✅ DONE |
| 9 | Re-trigger after PR-8b merges + deploys → first Dataproc batch run; smoke validation passes | DAG triggers | 30 min wall |
| 10 | Tune cluster sizing after seeing the pairwise Jaccard step's actual shuffle volume | Malachi | 1-2h iteration |

**Optional polish (not v1-blocking):** add a GH Action to `targeting-infra-ml` that auto-builds + uploads to GCS on every `v*` tag. Until then, manual `python -m build && gsutil cp` works fine.

**Graduation path to custom compute image** (long-term, when v1 starts to creak): bake `targeting-infra-ml` into a Dataproc custom image alongside whatever other shared deps emerge. Brian McAdams flagged this 2026-06-08: Fangorn already uses `fangorn-dataproc-runtime` (image family `dataproc-custom-image`, project `mntn-targeting-prj-prod`, last built 2026-05-29) for the same purpose. Either extend that image or build a new one when needed. Trade-off vs the v1 GCS-wheel approach:

| Concern | Wheel-at-startup (v1) | Custom image (graduation) |
|---|---|---|
| Iteration speed | Fast — just `gsutil cp` a new wheel | Slow — image rebuild per change |
| Per-batch startup | +30-60s pip install | Instant — already installed |
| Cross-job sharing | Each job pins its own wheel URI | One image, many consumers |
| Maintenance | Zero | DevOps rebuilds per version |

For TI-956 (single weekly batch, single consumer) the startup overhead is negligible. Graduate when we have multiple cross-job shared deps or iteration speed stops mattering.

### What Victor + Brian's input still gates

1. **Confirm the wheel GCS path.** Suggested `ti_resources/python/wheels/` to match `ti_resources/spark/drivers/`. Victor may have a different preference. 1 Slack message.
2. **Cluster sizing on first run.** First-run `runtime_properties` in the model file are scaled to "lighter than Fangorn" (maxExecutors 100 vs 180, no driver memory cranking). Tune after seeing actual shuffle volume.
3. **Cadence decision** — Victor suggested weekly is likely fine: *"we don't even get the vendor drop... every two weeks I think we do it... probably only needs to be done once every two weeks"* (meeting line 40). Default to weekly Sunday in airflow-ti schedule; tune to biweekly if first runs confirm no per-week change.
4. **Cost vs convenience choice** — DataprocBatch (serverless) is more convenient but more expensive than DataprocWorkflow (small persistent cluster). Default to batch; revisit after first runs show actual cost.

### Pre-flight checklist

- [x] **PR #57 reviewed + approved** (2026-06-05) — Alex Knorr swapped Malachi onto the reviewer slot after Ryan/theastrocat stayed stale since 2026-05-14. Three blocking comments addressed by Alex; approve posted.
- [x] **PR #57 merged** (2026-06-05) — `utils.segment_quality_utils` + `utils.sampling_logic` now on `main`
- [ ] Alex publishes benchmarks (cluster size, wall-clock at p=1e-4)
- [ ] Cross-repo dependency resolution path picked + tested (Victor — option (a)/(b)/(c) from above)
- [ ] Model file deployed to airflow-ti feature branch; Ryan reviews
- [ ] First Dataproc batch run end-to-end, smoke validation passes
- [ ] Macie confirms output schema matches admin UI needs

### Storage plan

| Surface | Location | Refresh | Consumer |
|---|---|---|---|
| Primary (Iceberg) | `gs://household-scoring-prod/data_without_ttl/scoring/ti_956_segment_quality_daily/` (dev: `gs://household-scoring-dev/...`) | Weekly | Spark / Dataproc consumers |
| BQ read access | `dw-main-bronze.household_scoring.segment_quality_daily` (dev: `dw-main-bronze.test.segment_quality_daily`) via BigQuery Metastore — no re-ingest | Same | UI team, Mode, admin tooling |

Idempotent reruns: `writeTo(...).overwritePartitions()` replaces just the `as_of_date` partition. Other partitions stay intact for backfill / audit. 1-year retention via `delete_where`.

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
