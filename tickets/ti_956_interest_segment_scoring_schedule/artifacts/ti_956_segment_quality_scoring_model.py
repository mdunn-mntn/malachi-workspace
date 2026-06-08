"""TI-956 — Segment quality scoring (airflow-ti model).

Weekly LiveRamp segment-quality scoring using Alex Knorr's
ThirdPartySegmentQuality framework (SteelHouse/targeting-infra-ml#57).
Output: `dw-main-bronze.household_scoring.segment_quality_daily` (Iceberg).

Filters out theater impressions (MM + 3P-OR-include) before computing the
performance sub-composite — per TI-999 Pass 26 + Ryan Kleck 2026-06-01,
those 3P clauses are bidder-inert under HHST > 0 and including them would
attribute MM-only delivery to 3P segments.

**Deploy location**: airflow-ti/models/machine_learning/segment_quality_scoring.py
**Cadence**: weekly (LiveRamp metadata refreshes weekly; daily is overkill)
**Invocation**: `python segment_quality_scoring.py --as_of_date YYYY-MM-DD`
                Scheduling handled by the @compute.dataproc_batch decorator +
                airflow-ti's standard model-scheduling operators.

**Cross-repo dep resolved 2026-06-08 (Brian McAdams):** wheel hosted in GCS,
installed at Dataproc batch startup via `spark.dataproc.driverPipPackages` +
`executorPipPackages`. Same pattern Brian uses for the vault wheel on Databricks
compute. Avoids the Artifact Registry setup overhead for a single-consumer v1.
Wheel path (mirrors the airflow-ti Iceberg-drivers convention at
`ti_resources/spark/drivers/`):

    gs://mntn-data-archive-prod/ti_resources/python/wheels/targeting_infra_ml-{VERSION}-py3-none-any.whl

Future graduation to a proper internal AR is tracked in TI-1023 (backlog —
not blocking).
"""

import argparse
import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

from utils_model.base_model import compute
from utils_model.base_model import model_config
from utils_model.base_model import IcebergBigqueryDwMainBronzeModel

# Alex's scoring package — see "Open issue" above for resolution path
from utils.segment_quality_utils.facade import ThirdPartySegmentQuality
from utils.sampling_logic import build_edges_with_weights_estimator_only


LR_DS_ID = 35   # LiveRamp data_source_id


# Operative 3P (campaign × dscid) extraction — drops bidder-inert theater
# (MM + 3P-OR-include) per TI-999 Pass 26 + Ryan Kleck (2026-06-01).
# Reference: tickets/ti_999_interest_segment_sizing/queries/ti_999_pass26_*.sql
OPERATIVE_3P_QUERY = r"""
CREATE TEMP FUNCTION extract_operative_3p(expr STRING) RETURNS ARRAY<STRUCT<dscid INT64, polarity STRING, is_mm_touching BOOL>>
LANGUAGE js AS r'''
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
''';

SELECT s.advertiser_id, s.campaign_id, o.dscid, o.polarity, o.is_mm_touching
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


@compute.dataproc_batch(
    timeout=18000,
    runtime_properties={
        "spark.driver.memory":                          "8g",
        "spark.dynamicAllocation.minExecutors":         "10",
        "spark.dynamicAllocation.maxExecutors":         "100",
        "spark.dynamicAllocation.executorIdleTimeout":  "600s",
        "spark.network.timeout":                        "600s",
        "spark.executor.heartbeatInterval":             "60s",
        "spark.executor.memory":                        "16g",
        "spark.executor.memoryOverhead":                "2g",
        "spark.executor.cores":                         "4",
        "spark.sql.adaptive.enabled":                   "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled":          "true",
        # Pairwise Jaccard self-join in Alex's uniqueness axis is shuffle-heavy
        "spark.sql.shuffle.partitions":                 "4000",
        # Install Alex's package from GCS-hosted wheel at batch startup (cross-repo dep).
        # Bump the version pin here when a new wheel is published.
        "spark.dataproc.driverPipPackages":   "gs://mntn-data-archive-prod/ti_resources/python/wheels/targeting_infra_ml-0.1.0-py3-none-any.whl",
        "spark.dataproc.executorPipPackages": "gs://mntn-data-archive-prod/ti_resources/python/wheels/targeting_infra_ml-0.1.0-py3-none-any.whl",
    },
    labels={
        "team":        "ti",
        "application": "household_scoring",
        "job_type":    "ti_956_segment_quality_scoring",
    },
)
@model_config(
    alias="ti_956_segment_quality_daily",
    location_root="gs://household-scoring-prod/data_without_ttl/scoring",
    location_root_dev="gs://household-scoring-dev/data_without_ttl/scoring",
    schema="dw-main-bronze.household_scoring",
    schema_dev="dw-main-bronze.test",
)
class SegmentQualityScoring(IcebergBigqueryDwMainBronzeModel):
    """TI-956 — weekly LiveRamp segment-quality scoring → Iceberg."""

    WINDOW_DAYS  = 30
    SAMPLE_RATE  = 1e-4
    N_OBS_FLOOR  = 100  # provisional ess_30d threshold for low_confidence_flag

    def __init__(self):
        parser = argparse.ArgumentParser(
            description="TI-956 segment quality scoring (weekly LiveRamp ranked segments)",
        )
        parser.add_argument("--as_of_date", required=True, help="YYYY-MM-DD")
        self._args = parser.parse_args()
        self.__spark = (
            SparkSession.builder.appName("ti_956_segment_quality_scoring")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate()
        )

    @property
    def spark(self) -> SparkSession:
        return self.__spark

    def model(self) -> None:
        as_of_date   = datetime.date.fromisoformat(self._args.as_of_date)
        window_start = as_of_date - datetime.timedelta(days=self.WINDOW_DAYS)
        window_end   = as_of_date - datetime.timedelta(days=1)
        env          = self.runtime_env

        print(
            f"[TI-956] as_of={as_of_date} window=[{window_start}, {window_end}] "
            f"env={env} write_base={self.write_location()}"
        )

        panel_df = scores_df = None
        try:
            # 1. Read inputs via airflow-ti `read_model` abstraction
            ipdsc_df                  = self._read_ipdsc(window_start, window_end)
            seg_meta_df               = self._read_seg_meta()
            targetable_ips_df         = self._read_targetable_ips(window_start, window_end)
            performance_df            = self._read_performance(window_start, window_end)
            campaign_segment_targets  = self._read_operative_3p()

            # 2. Build HT-sampled edge panel (Alex's sampling logic)
            panel_df = build_edges_with_weights_estimator_only(
                ipdsc_df=ipdsc_df,
                p=self.SAMPLE_RATE,
                hash_scope="edge_day",
                date_col="event_date",
            ).persist()

            # 3. Score (Alex's facade — 9-axis composite, 0-100 per dscid)
            scorer = ThirdPartySegmentQuality(panel_df)
            scores_df = scorer.quality_score_per_segment(
                seg_meta_df=seg_meta_df,
                targetable_ips_df=targetable_ips_df,
                performance_df=performance_df,
                campaign_segment_targets=campaign_segment_targets,
            ).persist()

            # 4. Add size + 3 dense ranks + low-confidence flag (UI team requirement)
            ranked_df = self._add_ranks_and_flags(
                scores_df, as_of_date, window_start, window_end,
            )

            # 5. Write Iceberg (idempotent partition overwrite — mirrors Fangorn pattern)
            if self.table_exists():
                self.df_write(ranked_df).overwritePartitions()
            else:
                (self.df_write(ranked_df)
                    .tableProperty("history.expire.max-snapshot-age-ms", "604800000")  # 7 days
                    .tableProperty("write.metadata.previous-versions-max", "60")
                    .tableProperty("write.metadata.delete-after-commit.enabled", "true")
                    .tableProperty("write.distribution-mode", "hash")
                    .partitionedBy(F.col("as_of_date"))
                    .create())

            # 6. Lifecycle hooks (per Fangorn pattern)
            self.create_success_file(f"/as_of_date={as_of_date.isoformat()}")
            self.delete_where("as_of_date <= date_sub(current_date, 365)")  # 1-year retention
            self.expire_snapshots()

            # 7. Smoke validation
            self._validate(ranked_df)

        finally:
            if panel_df  is not None: panel_df.unpersist()
            if scores_df is not None: scores_df.unpersist()
            self.spark.stop()

    # -----------------------------------------------------------------------
    # Input readers — all via airflow-ti `read_model` abstraction per Victor
    # -----------------------------------------------------------------------

    def _bq(self):
        """Shared BQ reader config — both projects set on parent + billing."""
        return (self.read_model("bigquery_data.BQ")
            .option("parentProject",  "dw-main-bronze")
            .option("billingProject", "dw-main-bronze"))

    def _read_ipdsc(self, window_start, window_end):
        """LiveRamp ipdsc rows over the 30d window.

        Read via BQ external table (which sits on top of
        gs://mntn-data-archive-prod/ipdsc/dt={DATE}/data_source_id={DS}/*.parquet).
        Going through BQ avoids hand-rolling the GCS layout in this job.
        """
        return self._bq().query(f"""
            SELECT ip, dt AS event_date, data_source_category_ids
            FROM `dw-main-bronze.external.ipdsc__v1`
            WHERE data_source_id = {LR_DS_ID}
              AND dt BETWEEN DATE('{window_start}') AND DATE('{window_end}')
        """).load()

    def _read_seg_meta(self):
        """Segment metadata for staleness axis."""
        return self._bq().query(f"""
            SELECT CAST(data_source_category_id AS INT64) AS dscid,
                   updated_date, created_date, deprecated
            FROM `dw-main-bronze.tpa.categories`
            WHERE data_source_id = {LR_DS_ID}
        """).load()

    def _read_targetable_ips(self, window_start, window_end):
        """IPs we actually delivered to in the window — targetability axis denominator."""
        return self._bq().query(f"""
            SELECT DISTINCT bid_ip AS ip
            FROM `dw-main-silver.logdata.impression_log`
            WHERE DATE(time) BETWEEN DATE('{window_start}') AND DATE('{window_end}')
              AND bid_ip IS NOT NULL
        """).load()

    def _read_performance(self, window_start, window_end):
        """Campaign-window KPI rollup (prospecting only) for the performance axis."""
        return self._bq().query(f"""
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
        """).load()

    def _read_operative_3p(self):
        """Operative (campaign × dscid) tuples — theater-filtered.

        TI-999 contribution. Without this filter the performance axis would
        attribute MM-only delivery to 3P segments that aren't actually driving
        delivery, inflating their apparent quality.
        """
        df = self._bq().query(OPERATIVE_3P_QUERY).load()
        return df.filter(
            (F.col("polarity") == F.lit("positive")) & (F.col("dscid").isNotNull())
        ).select(
            "advertiser_id", "campaign_id",
            F.col("dscid").cast("long").alias("dscid"),
        )

    # -----------------------------------------------------------------------
    # Output enrichment
    # -----------------------------------------------------------------------

    def _add_ranks_and_flags(self, scores_df, as_of_date, window_start, window_end):
        """Three global dense ranks + low-confidence flag for the UI team."""
        return (scores_df
            .withColumn("size_distinct_ips", F.col("reach_hat_30d").cast("long"))
            .withColumn("quality_rank",      F.dense_rank().over(Window.orderBy(F.desc("quality_score"))))
            .withColumn("anti_quality_rank", F.dense_rank().over(Window.orderBy(F.asc("quality_score"))))
            .withColumn("size_rank",         F.dense_rank().over(Window.orderBy(F.desc("size_distinct_ips"))))
            .withColumn("as_of_date",        F.lit(as_of_date).cast("date"))
            .withColumn("window_start",      F.lit(window_start).cast("date"))
            .withColumn("window_end",        F.lit(window_end).cast("date"))
            .withColumn("sample_rate",       F.lit(self.SAMPLE_RATE))
            .withColumn("low_confidence_flag",
                        F.when(F.col("ess_30d") < self.N_OBS_FLOOR, F.lit(True))
                         .otherwise(F.lit(False))))

    # -----------------------------------------------------------------------
    # Validation
    # -----------------------------------------------------------------------

    def _validate(self, ranked_df):
        n = ranked_df.count()
        assert n > 50_000, f"too few scored segments: {n:,}"

        qs = ranked_df.select(
            F.min("quality_score").alias("min"),
            F.expr("percentile_approx(quality_score, 0.5)").alias("p50"),
            F.max("quality_score").alias("max"),
        ).collect()[0]
        assert 0 <= qs["min"] and qs["max"] <= 100, "quality_score out of [0, 100]"

        low_conf = ranked_df.filter(F.col("low_confidence_flag")).count() / n
        print(
            f"[TI-956] validation OK: {n:,} segments | "
            f"qs[min={qs['min']:.1f} p50={qs['p50']:.1f} max={qs['max']:.1f}] | "
            f"low_conf={low_conf:.1%}"
        )
        if low_conf > 0.5:
            print(f"[TI-956] WARN: >50% low-confidence — consider raising sample_rate or window")


if __name__ == "__main__":
    SegmentQualityScoring().model()
