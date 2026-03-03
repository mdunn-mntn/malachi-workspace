from datetime import datetime
from datetime import timedelta

import mlflow
import pandas as pd
import pyspark.sql.functions as F
import tldextract
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

ECOMMERCE_THRESHOLD = 0.4


def model(dbt, session: SparkSession) -> DataFrame:  # type: ignore
    run_date = dbt.config.get("run_date")
    env = dbt.config.get("target_name")
    if env.startswith("prod"):
        env = "prod"
    else:
        env = "dev"
    dbt.config(file_format="parquet")
    dbt.config(partition_by="dt")
    dbt.config.get("location_root")
    dbt.config(materialized="table")
    dbt.config(incremental_strategy="append")
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    run_date_datetime = datetime.strptime(run_date, "%Y-%m-%d").date()

    S3_BASE_PATH = "s3://mntn-data-archive-prod/signals/site_visit_signal/"
    S3_DOMAIN_VERTICAL_PATH = "s3://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/"
    WHITELIST_PATH = ("s3://mntn-data-archive-prod/vertical_categorizations/"
                      + "ecommerce_domain_whitelist/ecommerce_whitelist.csv.gz")

    # Set MLflow tracking and registry URIs
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")

    model_name = f"{env}.ml.ecommerce_classifier"
    model_uri = f"models:/{model_name}@champion"

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

    todays_folders = [
        f"{S3_BASE_PATH}/dt={run_date_datetime.strftime('%Y-%m-%d')}/"
    ]

    signal_table_today_df = (
        session.read.option("basePath", f"{S3_BASE_PATH}")
        .format("parquet")
        .load(todays_folders)
        .filter("ip NOT LIKE '%:%'")
        .filter("ip != '0.0.0.0'")
        .filter("url IS NOT NULL")
        .filter('data_source_id != 23')
        .filter(F.trim(F.col('url')) != '')
        .withColumn("dt", F.lit(run_date_datetime.strftime('%Y-%m-%d')))
    )

    yesterday_folders = [
        f"{S3_BASE_PATH}/dt={(run_date_datetime - timedelta(1)).strftime('%Y-%m-%d')}/"
    ]

    signal_table_yesterday_df = (
        session.read.option("basePath", f"{S3_BASE_PATH}")
        .format("parquet")
        .load(yesterday_folders)
        .filter("ip NOT LIKE '%:%'")
        .filter("ip != '0.0.0.0'")
        .filter("url IS NOT NULL")
        .filter('data_source_id != 23')
        .filter(F.trim(F.col('url')) != '')
        .withColumn("dt", F.lit((run_date_datetime - timedelta(1)).strftime('%Y-%m-%d')))
    )

    signal_table_df = signal_table_today_df.unionByName(signal_table_yesterday_df)

    ddp_df = (
        signal_table_df
        .withColumnRenamed("url", "product_referrer")
        .withColumn("domain", get_domain("product_referrer"))
        .withColumn("contains_qs", F.col("product_referrer").contains("?"))
        .withColumn("product_sku", F.lit(1))
        .withColumn(
            "product_name",
            F.when(
                F.col("contains_qs") == True, F.split("product_referrer", "\?")[0]  # noqa
            ).otherwise(F.col("product_referrer")), # noqa
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

    ddp_df = (
        ddp_df.join(F.broadcast(whitelist_df).alias("w"), on="domain", how="left")
        .withColumn("is_whitelist", F.expr("w.domain IS NOT NULL"))
    )

    # Broadcast only the URI (pickle-safe) and load the model lazily per executor
    model_uri_bc = session.sparkContext.broadcast(model_uri)

    _model_holder = {"model": None}  # mutable container to persist across tasks

    @F.pandas_udf(DoubleType())
    def predict_proba_udf(url_series: pd.Series) -> pd.Series:  # type: ignore[valid-type]
        if _model_holder["model"] is None:
            _model_holder["model"] = mlflow.sklearn.load_model(model_uri_bc.value)
        probs = _model_holder["model"].predict_proba(url_series)[:, 1]  # type: ignore[attr-defined]
        return pd.Series(probs, index=url_series.index)

    ddp_scored = (
        ddp_df
        .withColumn("ecommerce_score", predict_proba_udf("product_referrer"))
        .withColumn("is_ecommerce", F.col("ecommerce_score") >= F.lit(ECOMMERCE_THRESHOLD))
    )

    domain_to_vertical_df = (session.read.parquet(S3_DOMAIN_VERTICAL_PATH)
                             .withColumnRenamed("domain_name", "domain"))

    final_df = (ddp_scored.join(domain_to_vertical_df.alias('v'), "domain", "left")
                .select("ip", "domain", "vertical_id", "bucket_id", "vertical_name", "is_ecommerce",
                        F.expr("v.domain IS NOT NULL").alias("is_in_vertical_mapping"), "data_source_id",
                        F.col("time").alias("input_timestamp"),
                        F.col("product_referrer").alias("url"), "ecommerce_score", "is_whitelist", "dt"))

    return final_df