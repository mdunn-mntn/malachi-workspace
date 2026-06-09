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

**Cross-repo dep (revised 2026-06-08 after first prod run):** `spark.dataproc.driverPipPackages`
turned out to be silently ignored on Dataproc Serverless when given a GCS URL
(it expects PyPI package specifiers, not file URLs). Pivoted to the same mechanism
airflow-ti's framework already uses for `utils_model.zip` — a zip of the `utils/`
directory dropped on PYTHONPATH via `spark.submit.pyFiles`. Build + upload steps:

    cd ~/Developer/work/mntn/targeting-infra-ml
    zip -r /tmp/utils.zip utils/ -x "utils/**/__pycache__/*" "utils/**/*.pyc"
    gsutil cp /tmp/utils.zip gs://mntn-data-archive-prod/ti_resources/python/wheels/utils.zip

Re-run those steps when targeting-infra-ml's utils/ changes.

Future graduation to a proper internal AR (or custom Dataproc container) is
tracked in TI-1023 (backlog — not blocking).
"""

import argparse
import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

from utils_model.base_model import compute
from utils_model.base_model import model_config
from utils_model.base_model import IcebergBigqueryDwMainBronzeModel

# NOTE: imports from `utils.segment_quality_utils` and `utils.sampling_logic`
# (Alex's scoring package from SteelHouse/targeting-infra-ml) are done INSIDE
# `model()` below, NOT at module load. The package is installed via
# `spark.dataproc.driverPipPackages` at Dataproc batch startup — it is NOT
# present in CI's model-compilation environment, which imports every model
# file via `python model_upload.py --dryrun`.


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

SELECT c.advertiser_id, s.campaign_id, o.dscid, o.polarity, o.is_mm_touching
FROM (
  SELECT campaign_id, expression,
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
        # Cluster sizing matched to Alex Knorr's Databricks job config (per
        # Slack 2026-06-09): c3-standard-22 workers (88g/22-core), 10-45
        # autoscale → ~3.96TB max memory + ~990 cores. Mirroring on Dataproc
        # Serverless with bigger workers (64g/16-core × 50 max). Yesterday's
        # 16g/4-core × 100 ran 3+ hours and didn't finish; Alex's profile
        # consistently lands at ~1h.
        "spark.driver.memory":                          "16g",  # 32g blew per-core cap (Dataproc Serverless standard tier limits to 7.4 GB/core; driver is 4 cores)
        "spark.dynamicAllocation.minExecutors":         "10",
        "spark.dynamicAllocation.maxExecutors":         "50",
        "spark.dynamicAllocation.executorIdleTimeout":  "600s",
        "spark.network.timeout":                        "600s",
        "spark.executor.heartbeatInterval":             "60s",
        "spark.executor.memory":                        "64g",
        "spark.executor.memoryOverhead":                "8g",
        "spark.executor.cores":                         "16",
        "spark.sql.adaptive.enabled":                   "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled":          "true",
        # Pairwise Jaccard self-join in Alex's uniqueness axis is shuffle-heavy
        "spark.sql.shuffle.partitions":                 "4000",
        # Alex's targeting-infra-ml package is pip-installed at runtime inside
        # model() (see _install_targeting_infra_ml below). Dataproc Serverless
        # rejects `spark.submit.pyFiles` and `spark.dataproc.driverPipPackages`
        # in runtime_properties; the `python_file_uris` field on PySparkBatch is
        # the only sanctioned wiring but the @compute.dataproc_batch decorator
        # doesn't expose it. Subprocess install on the driver works because
        # Alex's code only builds Spark plans (no Python UDFs) → executors don't
        # need the package, only the driver.
    },
    labels={
        "team":        "ti",
        "application": "household_scoring",
        "job_type":    "ti_956_segment_quality_scoring",
    },
)
@model_config(
    alias="interest_segment_quality_daily",
    location_root="gs://mntn-data-archive-prod/data_without_ttl",
    location_root_dev="gs://mntn-data-archive-dev/data_without_ttl",
    schema="dw-main-bronze.external_ti",
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
        # Pip-install Alex's targeting-infra-ml wheel onto the driver at runtime.
        # Dataproc Serverless rejects both driverPipPackages (silently — expects
        # PyPI specifiers, not GCS URLs) and submit.pyFiles (explicitly 400).
        # Driver-only install is sufficient because Alex's scoring framework
        # only builds Spark DataFrame transforms (no Python UDFs that get
        # shipped to executors).
        self._install_targeting_infra_ml()

        # Lazy import — must come AFTER the pip install above. Also lives inside
        # model() so CI's `model_upload.py --dryrun` compile pass doesn't fail
        # when this package isn't on the build environment's PYTHONPATH.
        from utils.segment_quality_utils.facade import ThirdPartySegmentQuality
        from utils.sampling_logic import build_edges_with_weights_estimator_only

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
    # Runtime install of the cross-repo targeting-infra-ml wheel
    # -----------------------------------------------------------------------

    WHEEL_GCS_URI = (
        "gs://mntn-data-archive-prod/ti_resources/python/wheels/"
        "targeting_infra_ml-0.1.0-py3-none-any.whl"
    )

    def _install_targeting_infra_ml(self) -> None:
        """Download + pip install the targeting-infra-ml wheel onto the driver.

        Background: Dataproc Serverless rejects both `spark.dataproc.driverPipPackages`
        (silently — expects PyPI specifiers, not URLs) and `spark.submit.pyFiles`
        (explicitly returns 400 "Attempted to set unsupported properties"). The
        sanctioned wiring is the `python_file_uris` field on `PySparkBatch`,
        which the airflow-ti framework auto-populates with `utils_model.zip`
        but doesn't expose to model authors. Until @compute.dataproc_batch grows
        that knob, the most reliable path is to pip-install at runtime from GCS.

        Driver-only is enough: Alex's scoring framework uses
        `pyspark.sql.functions` and `Window` to build DataFrame transforms;
        there are no Python UDFs that get serialized to executors, so executors
        run the Spark plan without needing the package.

        Re-upload the wheel when targeting-infra-ml is tagged with a new version:
            cd ~/Developer/work/mntn/targeting-infra-ml
            python -m build
            gsutil cp dist/*.whl gs://mntn-data-archive-prod/ti_resources/python/wheels/
        Then bump the WHEEL_GCS_URI version pin above.
        """
        import os
        import subprocess
        import sys

        local_wheel = os.path.join("/tmp", os.path.basename(self.WHEEL_GCS_URI))
        print(f"[TI-956] downloading {self.WHEEL_GCS_URI} → {local_wheel}")
        subprocess.check_call(["gsutil", "-q", "cp", self.WHEEL_GCS_URI, local_wheel])
        print(f"[TI-956] pip installing {local_wheel}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", local_wheel])
        print("[TI-956] targeting-infra-ml install complete")

    # -----------------------------------------------------------------------
    # Input readers — all via airflow-ti `read_model` abstraction per Victor
    # -----------------------------------------------------------------------

    def _bq(self):
        """Shared BQ reader config — both projects set on parent + billing."""
        return (self.read_model("bigquery_data.BQ")
            .option("parentProject",  "dw-main-bronze")
            .option("billingProject", "dw-main-bronze"))

    def _read_ipdsc(self, window_start, window_end):
        """LiveRamp ipdsc rows over the window.

        Reads via the existing `ipdsc_ds_35.DS35` model output (per Victor 2026-06-09
        PR review). That model already aggregates LiveRamp IP→segments per day
        and writes Parquet partitioned by `dt`. Pattern mirrors Fangorn's
        `read_model(...).load(relpath=dt_relpaths, optional=True)` — passing an
        explicit list of `dt=YYYY-MM-DD` relpaths avoids the cost of Spark
        discovering every partition folder before pruning.
        """
        n_days = (window_end - window_start).days + 1
        # Per Victor PR #1073 review (2026-06-09): narrow to data_source_id=35
        # subpath so Spark only reads LiveRamp partitions instead of every DS's
        # partition under each dt= folder. Alex's notebook scopes the same way.
        dt_relpaths = [
            f"dt={(window_start + datetime.timedelta(days=i)).isoformat()}/data_source_id=35"
            for i in range(n_days)
        ]
        df = self.read_model("ipdsc_ds_35.DS35").load(relpath=dt_relpaths, optional=True)
        if df is None:
            raise FileNotFoundError(
                f"No ipdsc_ds_35.DS35 partitions found in window [{window_start}, {window_end}]"
            )
        # DS35 output schema: (ip, data_source_category_ids ARRAY<BIGINT>).
        # The `dt` partition column is auto-discovered by Spark from the load path.
        return df.select(
            F.col("ip"),
            F.to_date(F.col("dt")).alias("event_date"),
            F.col("data_source_category_ids"),
        )

    def _read_seg_meta(self):
        """Segment metadata for staleness axis."""
        return self._bq().query(f"""
            SELECT CAST(data_source_category_id AS INT64) AS dscid,
                   updated_date, created_date, deprecated
            FROM `dw-main-bronze.tpa.categories`
            WHERE data_source_id = {LR_DS_ID}
        """)

    def _read_targetable_ips(self, window_start, window_end):
        """IPs we actually delivered to in the window — targetability axis denominator."""
        return self._bq().query(f"""
            SELECT DISTINCT bid_ip AS ip
            FROM `dw-main-silver.logdata.impression_log`
            WHERE DATE(time) BETWEEN DATE('{window_start}') AND DATE('{window_end}')
              AND bid_ip IS NOT NULL
        """)

    def _read_performance(self, window_start, window_end):
        """Campaign-window KPI rollup (prospecting only) for the performance axis."""
        return self._bq().query(f"""
            SELECT
                s.advertiser_id, s.campaign_id,
                SUM(s.media_spend + s.data_spend + s.platform_spend) AS total_spend,
                SUM(s.impressions) AS impressions,
                HLL_COUNT.MERGE(s.site_visitors) AS visits,
                SUM(s.click_conversions + s.view_conversions) AS conversions,
                CAST(0 AS FLOAT64) AS revenue  -- TBD: source from a different table; not load-bearing for v1 ranking
            FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
            JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
            WHERE s.day BETWEEN DATE('{window_start}') AND DATE('{window_end}')
              AND c.objective_id IN (1, 5, 6)
            GROUP BY 1, 2
            HAVING SUM(s.impressions) > 0
        """)

    def _read_operative_3p(self):
        """Operative (campaign × dscid) tuples — theater-filtered.

        TI-999 contribution. Without this filter the performance axis would
        attribute MM-only delivery to 3P segments that aren't actually driving
        delivery, inflating their apparent quality.
        """
        df = self._bq().query(OPERATIVE_3P_QUERY)
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
