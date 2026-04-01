"""
Hourly augmentor_log rollup: processes the last 2 hours per run.

Contains the full pipeline (augmentor_log → blocklist/verticals → rollup by ip/vertical_id).
Invoked by feature_store_hourly DAG with --run_date (e.g. data_interval_start as
YYYY-MM-DD HH:mm:ss). Floors to the hour, then processes (floor - 2h) and (floor - 1h),
overwriting /dt={date}/hh={HH} (2-digit) for each. If input for an hour is missing or empty,
that hour is skipped (logged); the job still succeeds so Airflow marks the run success.
Installs tldextract at runtime and distributes it to executors via SparkContext.addPyFile.
"""
import argparse
import os
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta
from typing import Optional


def _ensure_tldextract_on_executors(spark: "SparkSession") -> None:
    """
    Install tldextract into a temp dir, zip it, and add to Spark so driver and
    executors can import it (required for pandas_udf that uses tldextract).
    """
    package = "tldextract"
    with tempfile.TemporaryDirectory(prefix="ti_tldextract_") as tmpdir:
        print("📦 Installing tldextract for driver and executors...")
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "--quiet", "--target", tmpdir, package
        ])
        zip_path = os.path.join(tempfile.gettempdir(), "ti_tldextract.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _dirs, files in os.walk(tmpdir):
                for f in files:
                    path = os.path.join(root, f)
                    arcname = os.path.relpath(path, tmpdir)
                    zf.write(path, arcname)
        spark.sparkContext.addPyFile(zip_path)
        print("✅ tldextract zip added to driver and executors")


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

from utils_model.base_model import FileStorageBaseModel
from utils_model.base_model import model_config
from utils_model.base_model import compute
from utils_model.spark_job_monitor import SparkJobMonitor


AUGMENTOR_LOG_BASE = "gs://mntn-data-archive-prod/augmentor_log"
PLACEMENT_TYPES = ("BANNER", "BANNER_AND_VIDEO")
BLOCKLIST_PATH = (
    "gs://mntn-data-archive-prod/vertical_categorizations/"
    "ecommerce_domain_whitelist/ecommerce_blocklist.csv"
)
WEBSITE_CRAWL_VERTICALS_PATH = (
    "gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/"
)


@compute.dataproc_batch(
    runtime_properties={
        "spark.dynamicAllocation.minExecutors": "50",
        "spark.dynamicAllocation.initialExecutors": "100",
        "spark.dynamicAllocation.maxExecutors": "200",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "4000",
        "spark.network.timeout": "600s",
        "spark.rpc.askTimeout": "300s",
        "spark.shuffle.io.maxRetries": "20",
        "spark.shuffle.io.retryWait": "30s",
    },
    labels={
        "team": "targeting",
        "job_type": "aug_log_rollup",
    },
)
@model_config(
    location_root="gs://mntn-data-archive-prod/feature_store/feature_group_1_source",
    location_root_dev="gs://mntn-data-archive-dev/feature_store/feature_group_1_source",
    file_format="parquet",
)
class AugLogIpVerticalIdHourly(FileStorageBaseModel):
    def __init__(self):
        self.__spark = (
            SparkSession.builder.appName(f"Populate {self.model_id()}")
            .config("spark.sql.files.maxPartitionBytes", "268435456")
            .config("spark.sql.files.openCostInBytes", "8388608")
            .config("spark.sql.parquet.block.size", "134217728")
            .getOrCreate()
        )

    @property
    def spark(self) -> SparkSession:
        return self.__spark

    @staticmethod
    def _parse_run_date_and_hour(args_run_date: str) -> tuple[str, int]:
        """Parse run_date to (YYYY-MM-DD, hour). Expects datetime with time component."""
        s = args_run_date.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H"):
            try:
                dt_parsed = datetime.strptime(s, fmt)
                run_date = dt_parsed.strftime("%Y-%m-%d")
                run_hour = dt_parsed.hour
                return run_date, run_hour
            except ValueError:
                continue
        raise ValueError(f"run_date must be YYYY-MM-DD HH:00:00 (or similar), got: {args_run_date!r}")

    def model(self, args_run_date: str) -> bool:
        """
        Process a single hour: read augmentor_log for that hour, rollup, write dt/hh partition.
        Returns True if data was written, False if input was missing/empty (hour skipped).
        """
        _ensure_tldextract_on_executors(self.spark)
        import pandas as pd
        from tldextract import tldextract

        run_date, run_hour = self._parse_run_date_and_hour(args_run_date)

        monitor = SparkJobMonitor(self.spark)
        monitor.log_script_content(__file__)

        input_path = f"{AUGMENTOR_LOG_BASE}/region={{east,west}}/dt={run_date}/hh={run_hour:02d}"

        try:
            raw_log = (
                self.spark.read.option("basePath", AUGMENTOR_LOG_BASE)
                .parquet(input_path)
            )
        except Exception as e:
            print(f"[aug_log_ip_vertical_id_hourly] Skip dt={run_date} hh={run_hour:02d}: input missing or unreadable: {e}")
            return False
        if raw_log.isEmpty():
            print(f"[aug_log_ip_vertical_id_hourly] Skip dt={run_date} hh={run_hour:02d}: no data")
            return False

        @pandas_udf(StringType())  # type: ignore[call-overload]
        def extract_domain(domain_series: pd.Series) -> pd.Series:
            def get_domain(d: str) -> str:
                try:
                    ext = tldextract.extract(d)
                    return str(ext.domain + "." + ext.suffix)
                except Exception:
                    return "None"

            return domain_series.apply(get_domain)

        blocklist_schema = StructType([StructField("domain", StringType(), True)])
        blocklist_df = (
            self.spark.read.csv(BLOCKLIST_PATH, header=False, schema=blocklist_schema)
            .select(F.trim(F.col("domain")).alias("domain_name"))
            .filter(F.col("domain_name") != "")
            .distinct()
        )
        website_verticals_df = (
            self.spark.read.parquet(WEBSITE_CRAWL_VERTICALS_PATH)
            .select("domain_name", "vertical_id")
            .distinct()
        )

        augmentor_log_df = (
            raw_log
            .filter(F.col("placement_type").isin(*PLACEMENT_TYPES))
            .select("ip", "domain", "time")
            .filter(
                F.col("ip").isNotNull()
                & (F.trim(F.col("ip")) != "")
                & F.col("domain").isNotNull()
                & (F.trim(F.col("domain")) != "")
                & (F.trim(F.col("domain")) != "yahoo.com")
                & (F.trim(F.col("domain")) != "www.yahoo.com")
                & (F.trim(F.col("domain")) != "finance.yahoo.com")
                & (F.trim(F.col("domain")) != "mail.yahoo.com")
                & (F.trim(F.col("domain")) != "mail.aol.com")
                & (F.trim(F.col("domain")) != "easybrain.com")
                & (F.instr(F.trim(F.col("domain")), ".") > 0)
            )
            .withColumn("domain_name", extract_domain(F.col("domain")))
            .filter(F.col("domain_name") != "None")
            .withColumn("hh", F.hour(F.col("time")))
            .filter(F.col("hh") == run_hour)
        )
        augmentor_log_df = (
            augmentor_log_df
            .groupBy("ip", "domain_name", "hh")
            .agg(
                F.count("*").alias("visits"),
                F.min("time").alias("first_visit_time"),
                F.max("time").alias("last_visit_time"),
            )
            .groupBy("ip", "domain_name")
            .agg(
                F.sum("visits").alias("visits"),
                F.min("first_visit_time").alias("first_visit_time"),
                F.max("last_visit_time").alias("last_visit_time"),
            )
        )
        augmentor_log_df = (
            augmentor_log_df
            .join(F.broadcast(blocklist_df), on="domain_name", how="left_anti")
            .join(
                F.broadcast(website_verticals_df),
                on="domain_name",
                how="inner",
            )
        )

        rollup_df = (
            augmentor_log_df
            .withColumn("dt", F.lit(run_date))
            .groupBy("ip", "vertical_id", "dt")
            .agg(
                F.sum("visits").alias("visits"),
                F.min("first_visit_time").alias("first_visit_time"),
                F.max("last_visit_time").alias("last_visit_time"),
            )
        )

        output_df = (
            rollup_df
            .select("dt", "ip", "vertical_id", "visits", "first_visit_time", "last_visit_time")
            .repartition(8, "ip")
        )

        save_path = f"/dt={run_date}/hh={run_hour:02d}"
        (
            self.df_write(output_df)
            .mode("overwrite")
            .save(save_path)
        )
        return True


def _parse_run_date(run_date_str: str) -> datetime:
    """Parse run_date to datetime for the hour loop."""
    s = run_date_str.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"run_date must be YYYY-MM-DD or YYYY-MM-DD HH:mm:ss, got: {run_date_str!r}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process last 2 hours of augmentor_log per run; skip hours with missing input."
    )
    parser.add_argument(
        "--run_date",
        required=True,
        help="Reference time (e.g. data_interval_start): YYYY-MM-DD HH:mm:ss",
    )
    args = parser.parse_args()

    dt = _parse_run_date(args.run_date)
    hour_start = dt.replace(minute=0, second=0, microsecond=0)
    model = AugLogIpVerticalIdHourly()
    for delta_h in (2, 1):
        target = hour_start - timedelta(hours=delta_h)
        run_date_arg = target.strftime("%Y-%m-%d %H:%M:%S")
        try:
            model.model(run_date_arg)
        except Exception as e:
            run_date, run_hour = target.strftime("%Y-%m-%d"), target.hour
            print(f"[aug_log_ip_vertical_id_hourly] Skip dt={run_date} hh={run_hour:02d}: {e}")
    # Job always succeeds so Airflow marks run success; missing hours are picked up next run.
