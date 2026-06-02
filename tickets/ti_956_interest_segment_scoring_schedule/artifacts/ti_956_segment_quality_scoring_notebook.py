# Databricks notebook source
# MAGIC %md
# MAGIC # TI-956 — Scheduled Interest Segment Quality Scoring
# MAGIC
# MAGIC Runs Alex Knorr's `ThirdPartySegmentQuality` scoring pipeline on a recurring schedule and writes per-segment quality scores to BQ.
# MAGIC
# MAGIC **Source code**: `SteelHouse/targeting-infra-ml` (PR #57) — `utils.segment_quality_utils` package.
# MAGIC
# MAGIC **Inputs**:
# MAGIC - `ipdsc_df` — `dw-main-bronze.external.ipdsc__v1` filtered to LiveRamp (`data_source_id = 35`), 30-day window
# MAGIC - `seg_meta_df` — `dw-main-bronze.tpa.categories` filtered to `data_source_id = 35`
# MAGIC - `targetable_ips_df` — `dw-main-silver.logdata.impression_log` distinct `bid_ip` over 30d
# MAGIC - `performance_df` — `dw-main-silver.summarydata.sum_by_campaign_by_day` rolled up to (advertiser × campaign × KPIs)
# MAGIC - `campaign_segment_targets` — operative 3P (campaign × dscid) tuples from `queries/ti_956_operative_3p_campaign_segments.sql`. **Theater-filtered** per TI-999 Pass 26 / Ryan Kleck 2026-06-01: MM + 3P-OR-include impressions excluded because the 3P clause is bidder-inert under HHST > 0.
# MAGIC
# MAGIC **Output**: `bronze.household_scoring.segment_quality_daily` (BQ table, partitioned by `as_of_date`)
# MAGIC
# MAGIC **Schedule**: weekly, Sunday 06:00 UTC (LiveRamp segment metadata refreshes weekly; no value in daily reruns).
# MAGIC
# MAGIC **Owner**: Malachi Dunn. Initial setup help: Victor Savitskiy.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import datetime as dt
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType, IntegerType, ArrayType, StructType, StructField

# Alex's scoring package — pip-installable from the targeting-infra-ml repo or
# vendored into a Databricks workspace folder. See PR #57 of SteelHouse/targeting-infra-ml.
from utils.segment_quality_utils.facade import ThirdPartySegmentQuality
from utils.sampling_logic import build_edges_with_weights_estimator_only

# Run-time parameters
AS_OF_DATE   = dt.date.today()
WINDOW_DAYS  = 30
SAMPLE_RATE  = 1e-4
LR_DS_ID     = 35  # LiveRamp data_source_id

BQ_PROJECT   = "dw-main-silver"
GCS_TEMP     = "gs://household-scoring-prod/databricks_temp/segment_quality/"
OUTPUT_TABLE = "dw-main-bronze.household_scoring.segment_quality_daily"

window_start = AS_OF_DATE - dt.timedelta(days=WINDOW_DAYS)
window_end   = AS_OF_DATE - dt.timedelta(days=1)
print(f"Run as_of={AS_OF_DATE} | window {window_start} → {window_end} | sample p={SAMPLE_RATE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read inputs from BigQuery
# MAGIC
# MAGIC Uses the BQ Spark connector. The connector pushes simple filters down to BQ so
# MAGIC we don't pay for full table scans.

# COMMAND ----------

def read_bq(query: str):
    return (spark.read
        .format("bigquery")
        .option("project", BQ_PROJECT)
        .option("materializationProject", BQ_PROJECT)
        .option("materializationDataset", "scratch")
        .option("query", query)
        .load())


# 2a. IPDSC for LiveRamp segments over the window
ipdsc_df = read_bq(f"""
    SELECT
        ip,
        dt AS event_date,
        data_source_category_ids
    FROM `dw-main-bronze.external.ipdsc__v1`
    WHERE data_source_id = {LR_DS_ID}
      AND dt BETWEEN DATE('{window_start}') AND DATE('{window_end}')
""")

# Flatten data_source_category_ids: it's a RECORD with .list[].element nesting in BQ.
# After connector read, may need a transform to plain array<long>. The connector
# typically delivers it as an ArrayType already — verify on first run.
ipdsc_df = ipdsc_df.withColumn(
    "data_source_category_ids",
    F.transform("data_source_category_ids", lambda x: x.cast("long"))
)
print(f"ipdsc_df schema:")
ipdsc_df.printSchema()

# 2b. Segment metadata
seg_meta_df = read_bq(f"""
    SELECT
        CAST(data_source_category_id AS INT64) AS dscid,
        updated_date,
        created_date,
        deprecated
    FROM `dw-main-bronze.tpa.categories`
    WHERE data_source_id = {LR_DS_ID}
""")

# 2c. Targetable IPs — distinct bid_ip from impression_log over the window
# Cheap source per TI-956 summary §4 input mapping (66.4M IPs / ~130 GB scan).
targetable_ips_df = read_bq(f"""
    SELECT DISTINCT bid_ip AS ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE DATE(time) BETWEEN DATE('{window_start}') AND DATE('{window_end}')
      AND bid_ip IS NOT NULL
""")

# 2d. Performance: campaign × window KPI rollup (prospecting only)
performance_df = read_bq(f"""
    SELECT
        advertiser_id,
        campaign_id,
        SUM(media_spend + data_spend + platform_spend) AS total_spend,
        SUM(impressions) AS impressions,
        HLL_COUNT.MERGE(site_visitors) AS visits,
        SUM(click_conversions + view_conversions) AS conversions,
        SUM(revenue) AS revenue
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
    WHERE s.day BETWEEN DATE('{window_start}') AND DATE('{window_end}')
      AND c.objective_id IN (1, 5, 6)
    GROUP BY 1, 2
    HAVING SUM(impressions) > 0
""")

# 2e. Operative campaign × dscid tuples (theater-filtered).
# Embeds the SQL from `queries/ti_956_operative_3p_campaign_segments.sql`.
# Filters to dscid where the 3P clause is operative (AND-include in MM context,
# any AND-exclude, or any 3P clause in non-MM campaigns).
# This is the TI-999 contribution to TI-956: drops bidder-inert theater impressions
# from the performance signal so segment-quality KPIs reflect actual delivery.
with open("queries/ti_956_operative_3p_campaign_segments.sql") as f:
    OPERATIVE_3P_QUERY = f.read()

# The BQ connector accepts the full SQL including the CREATE TEMP FUNCTION.
campaign_segment_targets = read_bq(OPERATIVE_3P_QUERY).filter(
    (F.col("polarity") == F.lit("positive")) & (F.col("dscid").isNotNull())
).select("advertiser_id", "campaign_id", F.col("dscid").cast("long").alias("dscid"))

print(f"campaign_segment_targets rows: {campaign_segment_targets.count():,}")
print(f"distinct campaigns: {campaign_segment_targets.select('campaign_id').distinct().count():,}")
print(f"distinct dscids: {campaign_segment_targets.select('dscid').distinct().count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Build the sampled edge panel (HT estimator inputs)
# MAGIC
# MAGIC Alex's panel construction. ~310k sampled edges expected over 30d at p=1e-4.

# COMMAND ----------

panel_df = build_edges_with_weights_estimator_only(
    ipdsc_df=ipdsc_df,
    p=SAMPLE_RATE,
    hash_scope="edge_day",
    date_col="event_date",
)
panel_df.persist()
print(f"panel rows: {panel_df.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Run Alex's segment-quality scoring

# COMMAND ----------

scorer = ThirdPartySegmentQuality(panel_df)
scores_df = scorer.quality_score_per_segment(
    seg_meta_df=seg_meta_df,
    targetable_ips_df=targetable_ips_df,
    performance_df=performance_df,
    campaign_segment_targets=campaign_segment_targets,
)
scores_df.persist()
print(f"scored dscids: {scores_df.count():,}")
scores_df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Add size + ranks (UI team requirement)
# MAGIC
# MAGIC Alex's output is per-`dscid` global. Per TI-999 Finding 16 / TI-956 design doc,
# MAGIC the UI team needs three ranks (quality, anti-quality, size) to drive the per-pattern
# MAGIC application logic. Compute them here as dense ranks across the global cohort.
# MAGIC
# MAGIC `size_distinct_ips` proxy: `reach_hat_30d` from Alex's reach axis (HT-extrapolated).

# COMMAND ----------

# All windowed ranks are GLOBAL (no PARTITION BY) — v1 ships global per-segment scores.
# Per-advertiser ranks are a v2 layer (see TI-956 summary §8 Open Items).
ranked_df = (
    scores_df
    .withColumn("size_distinct_ips", F.col("reach_hat_30d").cast("long"))
    .withColumn("quality_rank",      F.dense_rank().over(Window.orderBy(F.desc("quality_score"))))
    .withColumn("anti_quality_rank", F.dense_rank().over(Window.orderBy(F.asc("quality_score"))))
    .withColumn("size_rank",         F.dense_rank().over(Window.orderBy(F.desc("size_distinct_ips"))))
    .withColumn("as_of_date",        F.lit(AS_OF_DATE))
    .withColumn("window_start",      F.lit(window_start))
    .withColumn("window_end",        F.lit(window_end))
    .withColumn("sample_rate",       F.lit(SAMPLE_RATE))
)

# Sample-size / confidence flag for downstream UI to de-rank noisy long-tail segments
# (TI-956 design doc open Q #2 — HT variance is brutal at p=1e-4 on rare segments)
N_OBS_FLOOR = 100  # provisional; tune after first run
ranked_df = ranked_df.withColumn(
    "low_confidence_flag",
    F.when(F.col("ess_30d") < N_OBS_FLOOR, F.lit(True)).otherwise(F.lit(False))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to BigQuery (partitioned by `as_of_date`)

# COMMAND ----------

# Final output columns:
#   identity:        dscid, as_of_date, window_start, window_end
#   primary metrics: quality_score, size_distinct_ips
#   ranks:           quality_rank, anti_quality_rank, size_rank
#   sub-scores:      z_activity, z_stability, z_share, z_uniqueness, z_sample,
#                    z_staleness, z_specificity, z_targetability, z_performance, z_combo
#   raw axes:        reach_hat_30d, cv14, avg_share_30d, mean_topk_jaccard_30d,
#                    idf_norm, staleness_unit_score, pct_targetable_30d, ess_30d
#   diagnostics:     sample_rate, low_confidence_flag

(ranked_df.write
    .format("bigquery")
    .option("table",                 OUTPUT_TABLE)
    .option("temporaryGcsBucket",    GCS_TEMP.replace("gs://", "").split("/")[0])
    .option("partitionField",        "as_of_date")
    .option("partitionType",         "DAY")
    .option("clusteredFields",       "dscid")
    .option("writeMethod",           "indirect")
    .mode("append")
    .save())

print(f"Wrote {OUTPUT_TABLE} partition as_of_date={AS_OF_DATE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Validation
# MAGIC
# MAGIC Smoke checks before declaring the run successful.

# COMMAND ----------

# 7a. Row count sanity
n_segments = ranked_df.count()
assert n_segments > 50_000, f"Too few scored segments ({n_segments:,}). Expected ~200k+ active LiveRamp."
print(f"✓ {n_segments:,} segments scored")

# 7b. Quality score distribution
qs_stats = ranked_df.select(
    F.min("quality_score").alias("min"),
    F.expr("percentile_approx(quality_score, 0.25)").alias("p25"),
    F.expr("percentile_approx(quality_score, 0.50)").alias("p50"),
    F.expr("percentile_approx(quality_score, 0.75)").alias("p75"),
    F.max("quality_score").alias("max"),
).collect()[0]
print(f"✓ quality_score distribution: min={qs_stats['min']:.1f} p25={qs_stats['p25']:.1f} "
      f"p50={qs_stats['p50']:.1f} p75={qs_stats['p75']:.1f} max={qs_stats['max']:.1f}")
assert 0 <= qs_stats["min"] and qs_stats["max"] <= 100, "quality_score out of [0, 100] range"

# 7c. Rank monotonicity
assert ranked_df.filter(F.col("quality_rank") == 1).count() >= 1, "no top-quality segment"
assert ranked_df.filter(F.col("size_rank") == 1).count() >= 1, "no top-size segment"

# 7d. Low-confidence share — flag if >50% are noisy
low_conf_share = ranked_df.filter(F.col("low_confidence_flag")).count() / n_segments
print(f"✓ low-confidence share: {low_conf_share:.1%}")
if low_conf_share > 0.5:
    print(f"⚠ >50% low confidence — consider raising sample_rate or window")

# 7e. Operative-targeting filter sanity (TI-999 contribution)
n_operative_segments = campaign_segment_targets.select("dscid").distinct().count()
print(f"✓ {n_operative_segments:,} distinct dscids appear in operative-targeting campaigns "
      f"(performance layer signal source)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Cleanup
# MAGIC
# MAGIC Unpersist cached DataFrames. Critical in Airflow workers per `[[reference_airflow_ti]]`.

# COMMAND ----------

panel_df.unpersist()
scores_df.unpersist()
print(f"✓ Run complete: as_of={AS_OF_DATE} | {n_segments:,} segments scored | "
      f"output={OUTPUT_TABLE}")
