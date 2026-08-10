import json
from datetime import date
from datetime import timedelta
from typing import Dict
from urllib.parse import urlparse

import google.auth.transport.requests as g_request
import pandas as pd
import pyspark.sql.functions as F
import requests  # type: ignore
from google.auth import compute_engine
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def token_for_url(url: str) -> str:
    request = g_request.Request()
    credentials = compute_engine.IDTokenCredentials(
        request=request,
        target_audience=url,
        use_metadata_identity_endpoint=True,
    )
    credentials.refresh(request)
    return credentials.token


def get_secret(secret_name: str) -> Dict:
    """Retrieve secret from Vault using GCP workload identity authentication."""
    vault_address = "https://vault.prod.in.mountain.com"
    role = "targeting-workloads"
    path = "teams/team-engineering-targeting"

    # Exchange metadata identity for a Vault login
    jwt = token_for_url(f"{vault_address}/vault/{role}")

    auth_resp = requests.post(
        f"{vault_address}/v1/auth/gcp/login",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"role": role, "jwt": jwt}),
    )
    auth_resp.raise_for_status()
    vault_token = auth_resp.json()["auth"]["client_token"]

    # Fetch the actual secret
    secret_resp = requests.get(
        f"{vault_address}/v1/secret/data/{path}/{secret_name}",
        headers={"X-Vault-Token": vault_token},
    )
    secret_resp.raise_for_status()

    secret_data = secret_resp.json().get("data", {}).get("data")
    return secret_data


def loadPostgresQuery(query: str, session: SparkSession) -> DataFrame:
    secrets = get_secret("coredb")

    results = (
        session.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{secrets['hostname']}:{secrets['port']}/{secrets['database']}")
        .option("query", query)
        .option("user", secrets['username'])
        .option("password", secrets['password'])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return results


@F.pandas_udf("string")
def get_domain(s: pd.Series) -> pd.Series:
    def parse(domain: str) -> str:
        try:
            return str(urlparse(domain).hostname)
        except Exception as e:
            return f"Unable to parse domain: {e}"

    return s.apply(lambda x: parse(x))


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(file_format="parquet")
    dbt.config(materialized="table")  # has to be "table" for this model
    dbt.config(partition_by=["dt"])
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    run_date = dbt.config.get("run_date")
    days_to_reload = dbt.config.get("days_to_reload")

    dt_end_str = run_date
    dt_end = date.fromisoformat(run_date)
    dt_start_str = (date.fromisoformat(run_date) - timedelta(days=(days_to_reload - 1))).isoformat()

    svs_base_path = "gs://mntn-data-archive-prod/signals/site_visit_signal"
    svs_ds_23_partitions = [
        f"{svs_base_path}/dt={dt_end - timedelta(days=ofst)}/" for ofst in range(days_to_reload)
    ]

    BLOCKLIST_PATH = ("gs://mntn-data-archive-prod/vertical_categorizations/"
                      + "ecommerce_domain_whitelist/ecommerce_blocklist.csv")

    print(f"loading ddp url ddp verticals between {dt_start_str} and {dt_end_str}")
    print("Site visit signal partitions", svs_ds_23_partitions)

    output_columns = [
        F.col("ip").cast("string"),
        F.col("domain").cast("string"),
        F.col("url").cast("string"),
        F.col("uid").cast("string"),
        F.col("time").cast("timestamp"),
        F.col("vertical_id").cast("long"),
        F.col("bucket_id").cast("string"),
        F.col("vertical_name").cast("string"),
        F.col("is_ecommerce").cast("boolean"),
        F.col("is_in_vertical_mapping").cast("boolean"),
        F.col("input_timestamp").cast("timestamp"),
        F.col("ecommerce_score").cast("double"),
        F.col("is_whitelist").cast("boolean"),
        F.col("data_source_id").cast("int"),
        F.col("dt").cast("string")
    ]

    blocklist_df = (
        session.read
        .csv(
            BLOCKLIST_PATH,
            header=False,
            schema=StructType([StructField("domain", StringType(), True)])
        )
        .distinct()
    )

    ddp_url_df = (
        dbt.ref("ddp_url_verticals")
        .where(F.col("dt").between(dt_start_str, dt_end_str))
        .where("data_source_id != 23")
        .select(*output_columns)
        .where("is_ecommerce = true or is_whitelist = true")
        .where("vertical_id IS NOT NULL")
        .join(F.broadcast(blocklist_df), on="domain", how="left_anti")
    )

    advertiser_verticals_df = loadPostgresQuery(
        """
            select
                advertiser_id,
                vertical_name,
                vertical_id,
                type
            from fpa.advertiser_verticals
        """,
        session
    )

    vertical_lookup_df = (
        advertiser_verticals_df
        .select(
            "advertiser_id",
            F.when(
                F.col("type") == 1,
                F.named_struct(
                    F.lit("id"),
                    F.col("vertical_id"),
                    F.lit("name"),
                    F.col("vertical_name"),
                )
            ).otherwise(F.lit(None)).alias("vertical"),
            F.when(
                F.col("type") == 0,
                F.named_struct(
                    F.lit("id"),
                    F.col("vertical_id"),
                    F.lit("name"),
                    F.col("vertical_name"),
                )
            ).otherwise(F.lit(None)).alias("bucket"),
        )
        .groupBy("advertiser_id")
        .agg(
            F.max("vertical").alias("vertical"),
            F.max("bucket").alias("bucket")
        )
        .select(
            "advertiser_id",
            F.col("vertical.id").alias("vertical_id"),
            F.col("bucket.id").alias("bucket_id"),
            F.col("vertical.name").alias("vertical_name"),
        )
    )

    guid_base_df = (
        session.read
        .option("basePath", f"{svs_base_path}")
        .format("parquet")
        .load(svs_ds_23_partitions)
        .where("data_source_id = 23")
        .where("ip NOT LIKE '%:%'")
        .where("ip != '0.0.0.0'")
        .where(F.trim(F.col("url")) != '')
        .where(F.col('url').isNotNull())
        .withColumn("domain", get_domain("url"))
    )

    guid_url_df = (
        guid_base_df
        .join(
            F.broadcast(vertical_lookup_df.alias("vl")),
            on="advertiser_id",
            how="left"
        )
        .select(
            "ip",
            "domain",
            "url",
            "uid",
            "time",
            "vl.vertical_id",
            "vl.bucket_id",
            "vl.vertical_name",
            F.lit(True).alias("is_ecommerce"),  # Setting to True as it is guid log data
            F.expr("domain IS NOT NULL").alias("is_in_vertical_mapping"),
            F.col("time").alias("input_timestamp"),
            F.lit(1.0).alias("ecommerce_score"),
            F.lit(True).alias("is_whitelist"),  # Setting to True as it is guid log data
            "data_source_id",
            "dt",
        )
    )

    final_df = (
        ddp_url_df.unionByName(guid_url_df, allowMissingColumns=False)
        .select(*output_columns)
        .repartitionByRange(int(days_to_reload * 180), "dt", "url")
    )

    return final_df
