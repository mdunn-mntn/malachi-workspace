import json
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyspark.sql.functions as F
from botocore.exceptions import ClientError
from pretty_html_table import build_table
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import From
from sendgrid.helpers.mail import Mail


def model(dbt, session):
    run_date = dbt.config.get("run_date")
    env = dbt.config.get("sdlc_env")
    dbt.config(partition_by="dt")
    dbt.config(file_format="parquet")
    dbt.config.get("location_root")
    dbt.config(materialized="incremental")
    dbt.config(incremental_strategy="append")
    dbt.config(unique_key=["composite_key"])
    email_distro = 'machine-learning-squad@mountain.com'

    # AWS FUNCTIONS NEEDED TO FETCH SECRETS
    def get_secret(secret_name: str) -> dict:
        region_name = "us-west-2"
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e

        secret = get_secret_value_response['SecretString']
        return json.loads(secret)

    @F.pandas_udf("string")
    def get_domain(s: pd.Series) -> pd.Series:
        def parse(domain: str) -> str:
            try:
                return urlparse(domain).hostname
            except:
                return "Unable to parse domain"

        return s.apply(lambda x: parse(x))

    s3_uniques_path = f"s3://mntn-data-archive-{env}/shopper_graph/product_uniques/"

    uniques_df = (
        session.read.format("parquet")
        .load(s3_uniques_path)
        .withColumn(
            "composite_key",
            F.concat(
                F.coalesce(F.col("product_name"), F.lit(1)),
                F.lit("_"),
                F.coalesce(F.col("product_sku"), F.lit(1)),
            ),
        )
    )

    # get the current day vendor log data
    parsed_signal_df = (
        session.read.format("parquet")
        .load(f"s3://mntn-data-archive-prod/signals/site_visit_signal/dt={run_date}")
        .withColumnRenamed("url", "product_referrer")
        .withColumn("domain", get_domain("product_referrer"))
    )
    parsed_signal_df.cache()

    # if there's funky domains send an email to the team
    bad_domain_df = parsed_signal_df.filter("domain = 'Unable to parse domain'")
    bad_domain_count = bad_domain_df.count()

    if bad_domain_count > 0:
        # try to summarize the URLs we can't parse, if it fails though, keep going with the pipeline
        try:
            bad_domain_df = (bad_domain_df
                             .select(F.col('product_referrer').alias('Bad Url'), F.col('domain').alias('Parsed Domain'),
                                     F.col('time').alias('Site Visit Time'),
                                     F.lit(run_date).alias('Site Visit Day'),
                                     F.col('hh').alias('Site Visit Hour'), F.col('ip').alias('Site Visit IP'),
                                     F.col('data_source_id').alias('Data Source ID'),
                                     F.lit('site_visit_signal').alias('S3 Data Source'),
                                     F.lit('product_uniques.py').alias('Python Script')))
            bad_domain_html_table = build_table(bad_domain_df.toPandas(), 'blue_light')

            bad_domain_html_email_content = f"""
            <html><body>
            <p style="font-size:18px">Invalid URL Alert</p>
            <br/>
            On {run_date} there was {bad_domain_count} URL(s) we couldn't parse.
            """ + bad_domain_html_table + """
            <br/>
            </body></html>
            """
            sendgrid_apikey = get_secret("prod/sendgrid/apikey")["sendgrid_api_key"]
            message = Mail(
                from_email=From("admin@mountain.com", "MNTN Match Invalid URL Alert"),
                to_emails=[email_distro],
                subject="MNTN Match Invalid URL Alert - Product Uniques",
                html_content=bad_domain_html_email_content)
            sg = SendGridAPIClient(sendgrid_apikey)
            sg.send(message)
        except:
            print("Unable To Send URL Parse Error Email")

    # Process the good domains as usual
    signal_df = (parsed_signal_df.filter("domain != 'Unable to parse domain'")
                 .withColumn("contains_qs", F.col("product_referrer").contains("?"))
                 .withColumn("product_sku", F.lit(1))
                 .withColumn("product_name",
                             F.when(
                                 F.col("contains_qs") == True, F.split("product_referrer", "\?")[0] # noqa
                             ).otherwise(F.col("product_referrer")),
                             )
                 .withColumn("composite_key",
                             F.concat("product_name", F.lit("_"), F.lit(1),
                                      )
                             )
                 .filter("composite_key != '1_1'")
                 )

    signal_df.createOrReplaceTempView("vendor_log")

    current_uniques = (
        session.sql(
            """SELECT uid as unique_id,
            advertiser_id,
            product_name,
            product_sku,
            product_referrer,
            data_source_id,
            domain
            FROM vendor_log
            GROUP BY
            unique_id,
            advertiser_id,
            product_name,
            product_sku,
            product_referrer,
            data_source_id,
            domain"""
        )
        .dropDuplicates()
        .withColumn(
            "composite_key",
            F.concat(
                F.coalesce(F.col("product_name"), F.lit(1)),
                F.lit("_"),
                F.lit(1),  # This was product_sku
            ),
        )
    )

    # Anti join
    incremental_df = current_uniques.join(uniques_df, ["composite_key"], "anti")

    write_df = (
        incremental_df.groupBy(
            "unique_id", "product_name", "product_sku", "domain", "data_source_id"
        )
        .agg(F.first("product_referrer").alias("product_referrer"))
        .withColumn("dt", F.lit(run_date))
        .withColumn("advertiser_id", F.lit("0000"))
        .withColumn(
            "composite_key",
            F.concat(
                F.coalesce(F.col("product_name"), F.lit(1)),
                F.lit("_"),
                F.coalesce(F.col("product_sku"), F.lit(1)),
            ),
        )
        .select(
            "unique_id",
            "advertiser_id",
            "product_name",
            "product_sku",
            "product_referrer",
            "data_source_id",
            "domain",
            "composite_key",
            "dt",
        )
    )

    if dbt.is_incremental:
        write_df.filter(f"dt = '{run_date}'")

    return write_df
