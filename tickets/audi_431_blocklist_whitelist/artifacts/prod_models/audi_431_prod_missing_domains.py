from datetime import datetime
from datetime import timedelta

import pandas as pd
import pyspark.sql.functions as F
import tldextract
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def model(dbt, session: SparkSession) -> DataFrame:  # type: ignore
    run_date = dbt.config.get("run_date")
    env = dbt.config.get("target_name")
    if env.startswith("prod"):
        env = "prod"
    else:
        env = "dev"

    dbt.config(file_format="parquet")
    dbt.config(partition_by="dt")
    dbt.config(materialized="table")
    dbt.config(incremental_strategy="append")
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    run_date_datetime = datetime.strptime(run_date, "%Y-%m-%d").date()

    GC_BASE_PATH = "gs://mntn-data-archive-prod/signals/site_visit_signal/"
    GC_DOMAIN_VERTICAL_PATH = "gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/"
    WHITELIST_PATH = ("gs://mntn-data-archive-prod/vertical_categorizations/"
                      + "ecommerce_domain_whitelist/ecommerce_whitelist.csv.gz")
    BLOCKLIST_PATH = ("gs://mntn-data-archive-prod/vertical_categorizations/"
                      + "ecommerce_domain_whitelist/ecommerce_blocklist.csv")

    # get domain udf
    @F.pandas_udf(StringType())
    def get_domain(s: pd.Series) -> pd.Series:
        def parse(domain: str) -> str:
            try:
                ext = tldextract.extract(domain)
                return ext.domain + '.' + ext.suffix
            except Exception as e:
                return f"Unable to parse domain: {e}"

        return s.apply(lambda x: parse(x))

    # Get two days worth of data in a single read operation
    two_days_folders = [
        f"{GC_BASE_PATH}/dt={(run_date_datetime - timedelta(1)).strftime('%Y-%m-%d')}/",
        f"{GC_BASE_PATH}/dt={run_date_datetime.strftime('%Y-%m-%d')}/"
    ]

    signal_table_df = (
        session.read.option("basePath", f"{GC_BASE_PATH}")
        .format("parquet")
        .load(two_days_folders)
        .filter("ip NOT LIKE '%:%'")
        .filter("ip != '0.0.0.0'")
        .filter("url IS NOT NULL")
        .filter('data_source_id != 23')
        .filter(F.trim(F.col('url')) != '')
    )

    ddp_df = (
        signal_table_df
        .withColumnRenamed("url", "product_referrer")
        .withColumn("domain", get_domain("product_referrer"))
        .withColumn("contains_qs", F.col("product_referrer").contains("?"))
        .withColumn("product_sku", F.lit(1))
        .withColumn(
            "product_name",
            F.when(
                F.col("contains_qs"), F.split("product_referrer", "\\?")[0]
            ).otherwise(F.col("product_referrer")),
        )
        .withColumn(
            "composite_key",
            F.concat(
                "product_name",
                F.lit("_"),
                F.lit(1),
            ),
        )
        .filter("composite_key != '1_1'")
    )

    whitelist_schema = StructType([
        StructField("domain", StringType(), True)
    ])
    # Load whitelist as DataFrame and broadcast join
    whitelist_df = (
        session.read.csv(WHITELIST_PATH, header=False, schema=whitelist_schema)
        .distinct()
    )

    # Load blocklist - CSV has no header, single column that we'll name "domain"
    blocklist_df = (
        session.read.csv(BLOCKLIST_PATH, header=False)
        .toDF("domain")  # Assign column name "domain" to the single column
        .distinct()
    )
    ddp_df = ddp_df.join(F.broadcast(whitelist_df).alias("w"), on="domain", how="left_anti")

    # Filter out domains that are in the blocklist using anti-join
    ddp_df = ddp_df.join(F.broadcast(blocklist_df), on="domain", how="left_anti")

    # Load domain-to-vertical mapping
    domain_to_vertical_df = (
        session.read.parquet(GC_DOMAIN_VERTICAL_PATH)
        .withColumnRenamed("domain_name", "domain")
    )

    # Perform ANTI-JOIN to find missing domains
    # This finds domains in site visits that are NOT in the vertical mapping
    missing_domains_df = (
        ddp_df
        .select("domain", "dt")
        .join(domain_to_vertical_df, "domain", "left_anti")  # Anti-join: left table rows with no match in right
    )

    # Aggregate to get frequency counts per domain, including whitelist status
    missing_domains_with_counts = (
        missing_domains_df
        .groupBy("domain", "dt")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))  # Order by frequency for easier analysis
    )

    return missing_domains_with_counts
