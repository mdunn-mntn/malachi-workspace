#!/usr/bin/env python3
"""TI-956 — Segment quality scoring (Dataproc Serverless PySpark job).

Reads:
  - ipdsc: GCS Parquet directly (skips BQ external-table overhead)
    Path: gs://<ipdsc-bucket>/dt=YYYY-MM-DD/data_source_id=35/*.parquet
    [Victor to confirm exact GCS layout — placeholders below.]
  - seg_meta, targetable_ips, performance, campaign_segment_targets: BQ via Spark connector

Writes:
  - Iceberg table on GCS, BigLake-cataloged
  - BigLake gives BQ query access without re-ingest

Invocation (from airflow-ti DAG):
    gcloud dataproc batches submit pyspark \\
        gs://mntn-targeting-jobs/ti_956/ti_956_segment_quality_scoring_job.py \\
        --batch=ti-956-${EXECUTION_DATE_NODASH} \\
        --region=us-central1 \\
        --version=2.2 \\
        --properties=spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 \\
        -- \\
        --as_of_date=${EXECUTION_DATE} \\
        --window_days=30 \\
        --sample_rate=0.0001

Depends on `utils.segment_quality_utils` from SteelHouse/targeting-infra-ml#57.
Install via Dataproc image custom build or `--py-files` upload.
"""

import argparse
import datetime as dt
import logging
import sys

from pyspark.sql import SparkSession, functions as F, Window

# Alex's scoring package — must be on PYTHONPATH at runtime.
from utils.segment_quality_utils.facade import ThirdPartySegmentQuality
from utils.sampling_logic import build_edges_with_weights_estimator_only


# -----------------------------------------------------------------------------
# Constants — derived from airflow-ti conventions (utils_model/base_model/*).
# Prod values; dev override below if MNTN_RUNTIME_ENV != "prod".
# -----------------------------------------------------------------------------
import os

LR_DS_ID = 35  # LiveRamp data_source_id

# BQ project for read-only sources (audience_segments, sum_by_campaign_by_day, etc.)
BQ_PROJECT = "dw-main-silver"

# IPDSC GCS layout (confirmed from BQ external table sourceUriPrefix):
# Hive-partitioned by `dt` and `data_source_id`.
IPDSC_GCS_BASE = "gs://mntn-data-archive-prod/ipdsc"

# Iceberg catalog config follows airflow-ti convention:
#   catalog name = uppercased project_id with underscores
#   type = bigquery (BigQuery Metastore, NOT BLMS)
# Prod catalog hosts the segment_quality table in dw-main-bronze.
RUNTIME_ENV       = os.environ.get("MNTN_RUNTIME_ENV", "dev")
if RUNTIME_ENV == "prod":
    ICEBERG_CATALOG_NAME    = "DW_MAIN_BRONZE"
    ICEBERG_METASTORE_PROJ  = "dw-main-bronze"
    ICEBERG_LOCATION_ROOT   = "gs://mntn-data-archive-prod/airflow_vs/prod/household_scoring/segment_quality_daily"
    ICEBERG_SCHEMA          = "household_scoring"
else:
    ICEBERG_CATALOG_NAME    = "MNTN_PRJ_DEV_00"
    ICEBERG_METASTORE_PROJ  = "mntn-prj-dev-00"
    ICEBERG_LOCATION_ROOT   = "gs://mntn-data-archive-dev/airflow_vs/dev/household_scoring/segment_quality_daily"
    ICEBERG_SCHEMA          = "spark_bq"  # default dev landing per airflow-ti convention

ICEBERG_TABLE_3PART = f"{ICEBERG_METASTORE_PROJ}.{ICEBERG_SCHEMA}.segment_quality_daily"

logger = logging.getLogger("ti_956_scoring")


# -----------------------------------------------------------------------------
# Operative 3P extraction SQL — inlined so this is one self-contained script.
# Source: queries/ti_956_operative_3p_campaign_segments.sql (kept in sync manually).
# -----------------------------------------------------------------------------
OPERATIVE_3P_SQL = r"""
CREATE TEMP FUNCTION extract_operative_3p(expr STRING) RETURNS ARRAY<STRUCT<dscid INT64, polarity STRING, is_mm_touching BOOL>>
LANGUAGE js AS r\"\"\"
  const out = [];
  if (!expr) return out;
  let parsed; try { parsed = JSON.parse(expr); } catch (e) { return out; }
  if (!parsed) return out;
  const catRoot = parsed.categories && parsed.categories.where;
  if (!catRoot) return out;

  const mmDS = [13, 19, 38, 46];
  const tpDS = [17, 18, 35];

  const clauses = [];
  function walk(node, parents, neg) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, parents, neg); return; }
    const op = node.op;
    if (op === 'not') { walk(node.value, parents.concat([{op:'not', node:node}]), neg + 1); return; }
    if (op === 'or' || op === 'and') {
      if (Array.isArray(node.value)) {
        const np = parents.concat([{op:op, node:node}]);
        for (const n of node.value) walk(n, np, neg);
      }
      return;
    }
    if (op === 'any') {
      const v = node.value || {};
      const ds = v.data_source_id;
      const categoryIds = (v.category_ids && Array.isArray(v.category_ids)) ? v.category_ids : [];
      const polarity = (neg % 2 === 1) ? 'negative' : 'positive';
      clauses.push({ds:ds, categoryIds:categoryIds, polarity:polarity, parents:parents});
      return;
    }
    if (node.value !== undefined) walk(node.value, parents, neg);
  }
  walk(catRoot, [], 0);

  const hasMM = clauses.some(c => c.polarity === 'positive' && mmDS.indexOf(c.ds) >= 0);
  const posClauses = clauses.filter(c => c.polarity === 'positive');

  function isOrConnected(c) {
    for (const other of posClauses) {
      if (other === c) continue;
      let lcaOp = null;
      const minLen = Math.min(c.parents.length, other.parents.length);
      for (let i = 0; i < minLen; i++) {
        if (c.parents[i].node === other.parents[i].node) lcaOp = c.parents[i].op;
        else break;
      }
      if (lcaOp === 'or') return true;
    }
    return false;
  }

  for (const c of clauses) {
    if (tpDS.indexOf(c.ds) < 0) continue;
    let operative = false;
    if (c.polarity === 'negative') operative = true;
    else if (!hasMM) operative = true;
    else operative = !isOrConnected(c);
    if (!operative) continue;
    for (const cid of c.categoryIds) {
      if (typeof cid === 'number') {
        out.push({dscid:cid, polarity:c.polarity, is_mm_touching:hasMM});
      }
    }
  }
  return out;
\"\"\";

SELECT
  s.advertiser_id, s.campaign_id, o.dscid, o.polarity, o.is_mm_touching
FROM (
  SELECT advertiser_id, campaign_id, expression,
         ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
  FROM `dw-main-silver.audience.audience_segments`
  WHERE expression_type_id = 2 AND is_targeted = TRUE
) s
JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
CROSS JOIN UNNEST(extract_operative_3p(s.expression)) AS o
WHERE s.rn = 1
  AND c.objective_id IN (1, 5, 6)
"""


def parse_args(argv):
    p = argparse.ArgumentParser(description="TI-956 segment quality scoring")
    p.add_argument("--as_of_date",    required=True, help="ISO date (YYYY-MM-DD)")
    p.add_argument("--window_days",   type=int,   default=30)
    p.add_argument("--sample_rate",   type=float, default=1e-4)
    p.add_argument("--n_obs_floor",   type=int,   default=100,
                   help="ess_30d threshold for low_confidence_flag (provisional)")
    return p.parse_args(argv)


def build_spark():
    """SparkSession configured for Iceberg (BigQuery Metastore catalog) + BigQuery reads.

    Catalog config mirrors airflow-ti convention (utils_model/base_model/compute_component.py):
      - catalog name = uppercased project_id with underscores
      - type = bigquery (BigQuery Metastore, NOT BLMS)
      - per-table location set via tableProperty at create time
    """
    cat = ICEBERG_CATALOG_NAME
    return (SparkSession.builder
        .appName("ti_956_segment_quality_scoring")
        # Iceberg BigQuery Metastore catalog
        .config(f"spark.sql.catalog.{cat}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cat}.type", "bigquery")
        .config(f"spark.sql.catalog.{cat}.gcp.bigquery.project-id", ICEBERG_METASTORE_PROJ)
        .config(f"spark.sql.catalog.{cat}.gcp.bigquery.location",   "us-central1")
        # Airflow-ti convention: prevent runtime jar conflicts
        .config("dataproc.artifacts.remove", "iceberg")
        # BigQuery connector default project for read-only sources
        .config("spark.bigquery.project", BQ_PROJECT)
        .getOrCreate())


def read_ipdsc_from_gcs(spark, window_start, window_end):
    """Read ipdsc directly from GCS Parquet for the full window.

    Skips BQ external-table overhead. Each day is a separate prefix; we union them.
    GCS layout (placeholder — confirm with Victor):
      gs://<base>/dt=YYYY-MM-DD/data_source_id=35/*.parquet
    """
    n_days = (window_end - window_start).days + 1
    dates = [(window_start + dt.timedelta(days=i)).isoformat() for i in range(n_days)]
    paths = [f"{IPDSC_GCS_BASE}/dt={d}/data_source_id={LR_DS_ID}/*.parquet" for d in dates]
    logger.info(f"Reading ipdsc: {n_days} GCS partitions [{paths[0]} … {paths[-1]}]")

    df = spark.read.parquet(*paths)
    # Normalize column names to what Alex's panel builder expects.
    return df.select(
        F.col("ip"),
        F.to_date(F.col("dt")).alias("event_date"),
        F.col("data_source_category_ids"),
    )


def read_bq_query(spark, query):
    """Execute a BQ SQL query via the Spark BQ connector."""
    return (spark.read
        .format("bigquery")
        .option("project", BQ_PROJECT)
        .option("materializationProject", BQ_PROJECT)
        .option("materializationDataset", "scratch")
        .option("query", query)
        .load())


def read_seg_meta(spark):
    return read_bq_query(spark, f"""
        SELECT
            CAST(data_source_category_id AS INT64) AS dscid,
            updated_date, created_date, deprecated
        FROM `dw-main-bronze.tpa.categories`
        WHERE data_source_id = {LR_DS_ID}
    """)


def read_targetable_ips(spark, window_start, window_end):
    return read_bq_query(spark, f"""
        SELECT DISTINCT bid_ip AS ip
        FROM `dw-main-silver.logdata.impression_log`
        WHERE DATE(time) BETWEEN DATE('{window_start}') AND DATE('{window_end}')
          AND bid_ip IS NOT NULL
    """)


def read_performance(spark, window_start, window_end):
    return read_bq_query(spark, f"""
        SELECT
            advertiser_id, campaign_id,
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


def read_operative_3p(spark):
    """Operative 3P (campaign × dscid) tuples — drops theater impressions.

    See queries/ti_956_operative_3p_campaign_segments.sql for the source SQL.
    Inlined as OPERATIVE_3P_SQL above; keep both in sync if modified.
    """
    df = read_bq_query(spark, OPERATIVE_3P_SQL)
    return df.filter(
        (F.col("polarity") == F.lit("positive")) & (F.col("dscid").isNotNull())
    ).select(
        "advertiser_id", "campaign_id",
        F.col("dscid").cast("long").alias("dscid"),
    )


def add_ranks_and_flags(scores_df, as_of_date, window_start, window_end,
                        sample_rate, n_obs_floor):
    """Add size + 3 dense ranks + low-confidence flag on top of Alex's per-dscid scores."""
    return (scores_df
        .withColumn("size_distinct_ips", F.col("reach_hat_30d").cast("long"))
        .withColumn("quality_rank",      F.dense_rank().over(Window.orderBy(F.desc("quality_score"))))
        .withColumn("anti_quality_rank", F.dense_rank().over(Window.orderBy(F.asc("quality_score"))))
        .withColumn("size_rank",         F.dense_rank().over(Window.orderBy(F.desc("size_distinct_ips"))))
        .withColumn("as_of_date",        F.lit(as_of_date))
        .withColumn("window_start",      F.lit(window_start))
        .withColumn("window_end",        F.lit(window_end))
        .withColumn("sample_rate",       F.lit(sample_rate))
        .withColumn("low_confidence_flag",
                    F.when(F.col("ess_30d") < n_obs_floor, F.lit(True)).otherwise(F.lit(False))))


def write_iceberg(spark, ranked_df):
    """Append (or overwrite) today's `as_of_date` partition into the Iceberg table.

    Uses airflow-ti's idiom (utils_model/base_model/writer_iceberg.py):
      - If the table exists: `overwritePartitions()` replaces just the as_of_date
        partition we're writing. Other partitions untouched.
      - Else: create partitioned by `as_of_date` with the per-table `location`
        property pointing into our GCS warehouse path.

    Table FQN uses the 3-part form `<catalog>.<schema>.<table>` where catalog
    is the uppercased project id; schema and project follow the dev/prod split
    from MNTN_RUNTIME_ENV.
    """
    table_fqn = f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SCHEMA}.segment_quality_daily"
    table_exists = spark.catalog.tableExists(table_fqn)
    if table_exists:
        ranked_df.writeTo(table_fqn).overwritePartitions()
        logger.info(f"Wrote {table_fqn} partition (overwritePartitions)")
    else:
        (ranked_df.writeTo(table_fqn)
            .using("iceberg")
            .tableProperty("location", ICEBERG_LOCATION_ROOT)
            .partitionedBy("as_of_date")
            .create())
        logger.info(f"Created {table_fqn} (first run) at {ICEBERG_LOCATION_ROOT}")


def validate(ranked_df):
    n_segments = ranked_df.count()
    assert n_segments > 50_000, f"Too few scored segments: {n_segments:,}"

    qs = ranked_df.select(
        F.min("quality_score").alias("min"),
        F.expr("percentile_approx(quality_score, 0.5)").alias("p50"),
        F.max("quality_score").alias("max"),
    ).collect()[0]
    assert 0 <= qs["min"] and qs["max"] <= 100, "quality_score out of [0, 100]"

    low_conf = ranked_df.filter(F.col("low_confidence_flag")).count() / n_segments
    logger.info(
        f"validation OK: {n_segments:,} segments | "
        f"qs[min={qs['min']:.1f} p50={qs['p50']:.1f} max={qs['max']:.1f}] | "
        f"low_conf={low_conf:.1%}"
    )
    if low_conf > 0.5:
        logger.warning("⚠ >50% low-confidence segments — consider raising sample_rate or window")


def main(argv):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    as_of_date   = dt.date.fromisoformat(args.as_of_date)
    window_start = as_of_date - dt.timedelta(days=args.window_days)
    window_end   = as_of_date - dt.timedelta(days=1)

    logger.info(f"as_of={as_of_date} | window {window_start} → {window_end} | p={args.sample_rate}")

    spark = build_spark()

    # 1. Read inputs
    ipdsc_df                 = read_ipdsc_from_gcs(spark, window_start, window_end)
    seg_meta_df              = read_seg_meta(spark)
    targetable_ips_df        = read_targetable_ips(spark, window_start, window_end)
    performance_df           = read_performance(spark, window_start, window_end)
    campaign_segment_targets = read_operative_3p(spark)
    logger.info(f"operative campaign-segment tuples: {campaign_segment_targets.count():,}")

    # 2. Build HT-sampled edge panel
    panel_df = build_edges_with_weights_estimator_only(
        ipdsc_df=ipdsc_df,
        p=args.sample_rate,
        hash_scope="edge_day",
        date_col="event_date",
    ).persist()
    logger.info(f"panel rows: {panel_df.count():,}")

    # 3. Score (Alex's facade)
    scorer = ThirdPartySegmentQuality(panel_df)
    scores_df = scorer.quality_score_per_segment(
        seg_meta_df=seg_meta_df,
        targetable_ips_df=targetable_ips_df,
        performance_df=performance_df,
        campaign_segment_targets=campaign_segment_targets,
    ).persist()
    logger.info(f"scored dscids: {scores_df.count():,}")

    # 4. Add size + ranks + confidence flag
    ranked_df = add_ranks_and_flags(
        scores_df, as_of_date, window_start, window_end,
        args.sample_rate, args.n_obs_floor,
    )

    # 5. Write to Iceberg (BigQuery Metastore)
    write_iceberg(spark, ranked_df)

    # 6. Validate
    validate(ranked_df)

    # 7. Cleanup
    panel_df.unpersist()
    scores_df.unpersist()
    logger.info(f"✓ run complete: as_of={as_of_date}")


if __name__ == "__main__":
    main(sys.argv[1:])
