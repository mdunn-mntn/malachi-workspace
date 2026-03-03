from datetime import datetime
from datetime import timedelta
from urllib.parse import urlparse

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


@F.pandas_udf("string")
def get_domain(s: pd.Series) -> pd.Series:
    def parse(domain: str) -> str:
        try:
            return str(urlparse(domain).hostname)
        except Exception as e:
            return f"Unable to parse domain: {e}"

    return s.apply(lambda x: parse(x))


def model(dbt, session: SparkSession) -> DataFrame:  # type: ignore
    run_date = dbt.config.get("run_date")
    dbt.config(file_format="parquet")
    dbt.config(partition_by="dt")
    dbt.config.get("location_root")
    dbt.config(materialized="incremental")
    dbt.config(incremental_strategy="append")
    dbt.config(unique_key=["domain"])

    run_date_datetime = datetime.strptime(run_date, "%Y-%m-%d").date() - timedelta(1)

    S3_BASE_PATH = "s3://mntn-data-archive-prod/signals/site_visit_signal/"

    folders = [
        f"{S3_BASE_PATH}/dt={(run_date_datetime - timedelta(i)).strftime('%Y-%m-%d')}/"
        for i in range(0, 30)
    ]

    signal_table_df = (
        session.read.option("basePath", f"{S3_BASE_PATH}")
        .format("parquet")
        .load(folders)
        .filter("ip NOT LIKE '%:%'")
        .filter("data_source_id != 23")
        .withColumnRenamed("url", "product_referrer")
        .withColumn("domain", get_domain("product_referrer"))
        .withColumn("contains_qs", F.col("product_referrer").contains("?"))
        .withColumn("product_sku", F.lit(1))
        .withColumn(
            "product_name",
            F.when(
                F.col("contains_qs") == True,  # noqa
                F.split("product_referrer", "\?")[0],  # noqa
            ).otherwise(
                F.col("product_referrer")
            ),  # noqa
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

    domain_ip_counts = signal_table_df.groupBy("domain").agg(
        F.countDistinct("ip").alias("distinct_ips")
    )

    product_categorization_df = session.read.table(
        "prod.mntn_matched.product_categorization"
    )

    keyword_df = (
        signal_table_df.groupBy("composite_key")
        .agg(F.collect_set("ip").alias("ip_set"))
        .join(product_categorization_df, "composite_key", "inner")
    )

    # Define the window specification
    windowSpec = Window.partitionBy("domain").orderBy(F.desc("industry_count"))

    final_df = (
        keyword_df.filter("domain != 'mntn.com'")
        .groupBy("domain", "product_subindustry")
        .agg(F.count("product_subindustry").alias("industry_count"))
        .withColumn("row_number", F.row_number().over(windowSpec))
        .filter("row_number = 1")
        .select("domain", "product_subindustry")
        .join(domain_ip_counts, "domain", "inner")
        .filter("distinct_ips > 1000")
        .withColumn("dt", F.lit(run_date))
    )

    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_from_this = f"select max(dt) from {dbt.this}"
        final_df = final_df.filter(
            final_df.dt > session.sql(max_from_this).collect()[0][0]
        )

    return final_df.filter("product_subindustry IS NOT NULL")
