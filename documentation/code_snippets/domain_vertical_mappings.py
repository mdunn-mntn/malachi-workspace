from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def get_valid_domains(session: SparkSession, env: str, run_date: str) -> DataFrame:
    return session.read.format("parquet").load(
        f"s3://mntn-data-archive-{env}/vertical_categorizations/verticals_valid_domains/dt={run_date}/"
    ).filter("product_subindustry IS NOT NULL")


def find_closest_categories(df: DataFrame, session: SparkSession) -> DataFrame:
    df.createOrReplaceTempView("valid_domains")
    return session.sql(
        """
            with t as (
            SELECT /*+ REPARTITION(620) */
              product_subindustry,
              domain
            FROM
              valid_domains
            )
            select
              domain,
              product_subindustry,
              0 as distinct_ips,
              bucket_id,
              vertical_id,
              category_name as predicted_category,
              search_score as scores
            FROM
              t,
              LATERAL(
            SELECT * FROM
            VECTOR_SEARCH(
            index => "prod.mntn_matched.verticals_vector_index",
            query => product_subindustry,
            num_results => 1)
              ) as search"""
    )


def model(dbt, session: SparkSession):  # type: ignore
    run_date = dbt.config.get("run_date")
    env = dbt.config.get("target_name")
    dbt.config(file_format="delta")
    dbt.config.get("location_root")
    dbt.config(materialized="incremental")
    dbt.config(incremental_strategy="merge")
    dbt.config(unique_key=["domain"])
    valid_domains = get_valid_domains(session, env, run_date)

    domains_with_predictions = find_closest_categories(valid_domains, session)

    final_df = (
        domains_with_predictions.groupBy(
            "domain",
            "product_subindustry",
            "distinct_ips",
            "bucket_id",
            "vertical_id",
            "predicted_category",
            "scores",
        )
        .count()
        .drop("count")
        .withColumn("dt", lit(run_date))
    )

    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_from_this = f"select max(dt) from {dbt.this}"
        final_df = final_df.filter(
            final_df.dt > session.sql(max_from_this).collect()[0][0]
        )

    return final_df
