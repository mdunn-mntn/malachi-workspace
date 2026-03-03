import json
from datetime import datetime
from datetime import timedelta
from typing import Dict
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyspark.sql.functions as F
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def get_secret(secret_name: str) -> Dict:
    region_name = "us-west-2"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


secrets = get_secret("redshift-prod")
secrets["jdbcUrl"] = (
    f"""jdbc:{secrets.get("engine")}://{secrets.get("host")}:{secrets.get("port")}/coredw"""
)

redshiftOptions = {
    "url": secrets.get("jdbcUrl"),
    "tempdir": "s3://aws-glue-assets-077854988703-us-west-2/temporary/",
    "unload_s3_format": "PARQUET",
    "aws_iam_role": "arn:aws:iam::077854988703:role/service-role/prod-redshift_commands_access_role",
    "user": secrets.get("username"),
    "password": secrets.get("password"),
}


def loadRedshiftQuery(query: str, session: SparkSession) -> DataFrame:
    redshiftOptions["query"] = query
    return (
        session.read.format("com.databricks.spark.redshift")
        .options(**redshiftOptions)
        .load()
    )


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
    dbt.config(unique_key=["ip"])

    run_date_datetime = datetime.strptime(run_date, "%Y-%m-%d").date()

    S3_BASE_PATH = "s3://mntn-data-archive-prod/signals/site_visit_signal/"

    folders = [
        f"{S3_BASE_PATH}/dt={(run_date_datetime - timedelta(i)).strftime('%Y-%m-%d')}/"
        for i in range(0, 29)
    ]

    signal_table_df = (
        session.read.option("basePath", f"{S3_BASE_PATH}")
        .format("parquet")
        .load(folders)
        .filter("ip NOT LIKE '%:%'")
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

    # Get the Signal table data and roll up by advertiser_id to join with verticals table
    grouped_df = (
        signal_table_df.filter("ip != '0.0.0.0'")
        .groupBy("advertiser_id", "ip")
        .agg(F.count("*").alias("count"))
        .orderBy("ip", "advertiser_id")
        .drop("count")
    )

    verticals_df = loadRedshiftQuery("SELECT * FROM fpa.advertiser_verticals", session)
    joined_df = grouped_df.join(verticals_df, "advertiser_id", "left_outer").filter(
        "vertical_id IS NOT NULL"
    )

    vector_response_df = (
        session.read.table("prod.ml.domain_vertical_mappings")
        .drop("distinct_ips")
        .filter("scores > 0.65")
    )

    # Get the Signal table data for only DDP URLs and roll up by domain for left join to vector responses
    grouped_ddp_df = (
        signal_table_df.filter("ip != '0.0.0.0'")
        .filter("data_source_id != 23")
        .groupBy("domain", "ip")
        .agg(F.count("*").alias("count"))
        .orderBy("ip", "domain")
        .drop("count")
    )
    joined_ddp_df = grouped_ddp_df.join(vector_response_df, "domain", "left").filter(
        "vertical_id IS NOT NULL AND bucket_id IS NOT NULL"
    ).groupBy("ip", "bucket_id", "vertical_id").count().drop("count")

    unioned_df = (
        joined_df.select("ip", "vertical_id")
        .union(joined_ddp_df.select("ip", "vertical_id"))
        .union(joined_ddp_df.select("ip", F.col("bucket_id").alias("vertical_id")))
        .groupby("vertical_id", "ip")
        .count()
    )

    ipdsc_df = unioned_df.select(
        "ip",
        F.lit(f"{run_date}").cast("date").alias("date"),
        F.lit(13).alias("data_source_id"),
        F.col("vertical_id").alias("data_source_category_id"),
    ).withColumn("dt", F.lit(run_date))

    return ipdsc_df
