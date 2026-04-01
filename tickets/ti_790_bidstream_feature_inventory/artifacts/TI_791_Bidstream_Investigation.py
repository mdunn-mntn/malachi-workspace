# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

import json
import math
from typing import Dict
from datetime import datetime, timedelta
import google.auth.transport.requests as g_request
import requests
from google.auth import compute_engine
import matplotlib.pyplot as plt

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

end_date = "2026-03-20"
num_days = 2

run_date = datetime.fromisoformat(end_date)

# COMMAND ----------

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
    role = "gcp-workloads"
    path = "shared/global/ti"

    # Exchange metadata identity for a Vault login
    jwt = token_for_url(f"{vault_address}/vault/gcp-workloads")

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
        .option("dbtable", query)
        .option("user", secrets['username'])
        .option("password", secrets['password'])
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return results


def load_postgres_query(
    query: str,
    session: SparkSession,
    partition_col: str,
    lower_bound: int,
    upper_bound: int,
    num_partitions: int = 32,
    fetchsize: int = 10000
) -> DataFrame:
    secrets = get_secret("coredw")

    return (
        session.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{secrets['hostname']}:{secrets['port']}/{secrets['database']}")
        .option("dbtable", query)
        .option("user", secrets["username"])
        .option("password", secrets["password"])
        .option("driver", "org.postgresql.Driver")
        .option("partitionColumn", partition_col)
        .option("lowerBound", str(lower_bound))
        .option("upperBound", str(upper_bound))
        .option("numPartitions", str(num_partitions))
        .option("fetchsize", str(fetchsize))
        .load()
    )


def get_data(run_date: datetime.date, data_source_id: int, num_days: int = 30) -> DataFrame:
    folders = [
        f"gs://mntn-data-archive-prod/ipdsc/dt={(run_date - timedelta(i)).strftime('%Y-%m-%d')}/data_source_id={data_source_id}/*" for i in
        range(1, num_days)
    ]

    base_path = "gs://mntn-data-archive-prod/ipdsc"
    ipdsc_df = spark.read.option("basePath", base_path).parquet(*folders)

    return ipdsc_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bidder Auction Events

# COMMAND ----------

root = "gs://mntn-data-archive-prod/augmentor_log"
regions = ["west", "east"]
dates = [(run_date - timedelta(i)).strftime('%Y-%m-%d') for i in range(1, num_days)]

paths = [
    f"{root}/region={region}/dt={dt}"
    for region in regions
    for dt in dates
]

aug_bid_df = (spark.read
              .option("basePath", root)
              .parquet(*paths))

def extract_geo(field):
    return F.regexp_extract("geo", rf"{field}=([^,)]*)", 1)

aug_bid_df = (
    aug_bid_df
    .withColumn("geo_ip", extract_geo("ip"))
    .withColumn("country", extract_geo("country"))
    #.withColumn("region", extract_geo("region"))
    #.withColumn("city", extract_geo("city"))
    #.withColumn("latitude", extract_geo("latitude").cast("double"))
    #.withColumn("longitude", extract_geo("longitude").cast("double"))
    #.withColumn("metro", extract_geo("metro"))
    #.withColumn("zip", extract_geo("zip"))
)

## Basic cleanup - '' IPs.
## Basic cleanup - US only.
aug_bid_df = aug_bid_df.filter(F.col("ip")!="")
aug_bid_df = aug_bid_df.filter(F.col("country").isin(["USA", "US", "us"]))
aug_bid_df = aug_bid_df.filter(F.col("placement_type") != "UNKNOWN_PLACEMENT_TYPE")
#aug_bid_df = aug_bid_df.filter(F.col("hh").between(8, 9))

# COMMAND ----------

sample_df = aug_bid_df.sample(withReplacement=False, fraction=0.01, seed=42)

# COMMAND ----------

sample_df = sample_df.persist(StorageLevel.MEMORY_AND_DISK)
sample_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse and Validate Geo

# COMMAND ----------

# def extract_geo(field):
#     return F.regexp_extract("geo", rf"{field}=([^,)]*)", 1)

# sample_df = (
#     sample_df
#     .withColumn("geo_ip", extract_geo("ip"))
#     .withColumn("country", extract_geo("country"))
#     .withColumn("region", extract_geo("region"))
#     .withColumn("city", extract_geo("city"))
#     .withColumn("latitude", extract_geo("latitude").cast("double"))
#     .withColumn("longitude", extract_geo("longitude").cast("double"))
#     .withColumn("metro", extract_geo("metro"))
#     .withColumn("zip", extract_geo("zip"))
# )

# COMMAND ----------

#Contains Non US countries. 
#display(sample_df.filter(~F.col("country").isin(["USA", "US"])))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate IP

# COMMAND ----------

#IPs all align! 
#display(sample_df.filter(F.col("ip")!=F.col("geo_ip")))

# COMMAND ----------

display(sample_df.limit(5))

# COMMAND ----------

display(sample_df.select(F.col("device_type")).distinct())

# COMMAND ----------

display(sample_df.select(F.col("placement_type")).distinct())

# COMMAND ----------

display(sample_df.select(F.col("device_type"), F.col("environment_type")).distinct())

# COMMAND ----------

display(sample_df.filter(F.col("placement_type")=="BANNER_AND_VIDEO"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columns of Interest, First pass: 
# MAGIC * iab_categories
# MAGIC * page
# MAGIC * domain
# MAGIC * app_bundle
# MAGIC * site_name
# MAGIC
# MAGIC #### Notes:
# MAGIC - Domain has full coverage and seems to contain app_bundle/site_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing but interesting: 
# MAGIC * Content details - https://help.magnite.com/help/openrtb-specification#2-1-7-%C2%A0-content-object

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coverage

# COMMAND ----------

has_iab_cat = (sample_df
               .withColumn("iab_categories_size", F.size(F.col("iab_categories")))
               .groupBy(F.col("device_type"))
               .agg(
                   F.sum(F.when(F.col("iab_categories_size")>0, 1).otherwise(0)).alias("iab_size"),
                   F.count("*").alias("total"))
               .withColumn("coverage", F.col("iab_size")/F.col("total"))
               )
display(has_iab_cat)

# COMMAND ----------

has_domain = (sample_df
               .withColumn("has_domain", F.when(F.col("domain").isNotNull(), 1).otherwise(0))
               .groupBy(F.col("device_type"))
               .agg(
                   F.sum(F.col("has_domain")).alias("domain_size"),
                   F.count("*").alias("total"))
               .withColumn("coverage", F.col("domain_size")/F.col("total"))
               )
display(has_domain)

# COMMAND ----------

has_page = (sample_df
               .withColumn("has_page", F.when(F.col("page").isNotNull(), 1).otherwise(0))
               .groupBy(F.col("device_type"))
               .agg(
                   F.sum(F.col("has_page")).alias("page_size"),
                   F.count("*").alias("total"))
               .withColumn("coverage", F.col("page_size")/F.col("total"))
               )
display(has_page)

# COMMAND ----------

has_app = (sample_df
               .withColumn("has_app", F.when(F.col("app_bundle").isNotNull(), 1).otherwise(0))
               .groupBy(F.col("device_type"))
               .agg(
                   F.sum(F.col("has_app")).alias("app_size"),
                   F.count("*").alias("total"))
               .withColumn("coverage", F.col("app_size")/F.col("total"))
               )
display(has_app)

# COMMAND ----------

has_site = (sample_df
               .withColumn("has_site", F.when(F.col("site_name").isNotNull(), 1).otherwise(0))
               .groupBy(F.col("device_type"))
               .agg(
                   F.sum(F.col("has_site")).alias("site_size"),
                   F.count("*").alias("total"))
               .withColumn("coverage", F.col("site_size")/F.col("total"))
               )
display(has_site)

# COMMAND ----------

has_refer = (sample_df
               .withColumn("has_refer", F.when(F.col("referrer").isNotNull(), 1).otherwise(0))
               .groupBy(F.col("device_type"))
               .agg(
                   F.sum(F.col("has_refer")).alias("ref_size"),
                   F.count("*").alias("total"))
               .withColumn("coverage", F.col("ref_size")/F.col("total"))
               )
display(has_refer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate IAB Category Counts

# COMMAND ----------

iab_cats_df = (spark.read
               .option("header", True)
               .option("sep", "\t")
               .option("inferSchema", True)
               .csv("gs://mntn-data-archive-dev/alex.knorr/Content Taxonomy 1.0.tsv"))
iab_cats_df = (iab_cats_df
               .withColumn("iab_code_norm", F.regexp_replace(F.col("IAB Code"), "-", "_")))
display(iab_cats_df)

# COMMAND ----------

# 1) Explode the array so each IAB code is on its own row
exploded = (sample_df.withColumn("iab_code", F.explode_outer("iab_categories")))

# 2) Join to lookup using normalized code
joined = (exploded.alias("d")
          .join(iab_cats_df
                .select(
                    F.col("iab_code_norm"),
                    F.col("IAB Category").alias("iab_category_name")).alias("l"),
                F.col("d.iab_code") == F.col("l.iab_code_norm"), "left"))

# 3) Group back to original rows and collect category names
#    Adjust the grouping columns if needed for your table
group_cols = [c for c in sample_df.columns]

df_out = (joined
          .groupBy(*group_cols)
          .agg(
              F.expr("filter(collect_list(iab_category_name), x -> x is not null)").alias("iab_category_names"))
          .withColumn("iab_category_names_text", F.concat_ws(", ", F.col("iab_category_names"))))

display(df_out)

# COMMAND ----------

# all observed IAB codes in your data + frequency
observed_code_counts = (
    sample_df
    .select(F.explode_outer("iab_categories").alias("iab_code"))
    .where(F.col("iab_code").isNotNull())
    .groupBy("iab_code")
    .count()
)

# supplied codes that do NOT exist in the lookup
unmapped_code_counts = (
    observed_code_counts.alias("o")
    .join(
        iab_cats_df.alias("l"),
        F.col("o.iab_code") == F.col("l.iab_code_norm"),
        "left"
    )
    .where(F.col("l.iab_code_norm").isNull())
    .select(
        F.col("o.iab_code").alias("unmapped_iab_code"),
        F.col("o.count").alias("occurrence_count")
    )
    .orderBy(F.desc("occurrence_count"))
)

display(unmapped_code_counts)

#What version of IAB categories are being used? 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top IAB Categories Per Device Type

# COMMAND ----------

top_n = 10

explode = df_out.select(F.col("ip"), F.col("device_type"), F.explode("iab_category_names").alias("iab_name"))

counts = (
    explode
    .groupBy("device_type", "iab_name")
    .agg(F.countDistinct("ip").alias("ip_count"))
)

w = Window.partitionBy("device_type").orderBy(F.desc("ip_count"), F.asc("iab_name"))

top_iab_per_device = (
    counts
    .withColumn("rank", F.row_number().over(w))
    .filter(F.col("rank") <= top_n)
    .orderBy("device_type", "rank")
)

display(top_iab_per_device)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Domain per Device Type

# COMMAND ----------

top_n = 10

subset = df_out.select(F.col("ip"), F.col("device_type"), F.col("domain"))

counts = (subset
          .groupBy("device_type", "domain")
          .agg(F.countDistinct("ip").alias("ip_count")))

w = Window.partitionBy("device_type").orderBy(F.desc("ip_count"), F.asc("domain"))

top_domain_per_device = (counts
                         .withColumn("rank", F.row_number().over(w))
                         .filter(F.col("rank") <= top_n)
                         .orderBy("device_type", "rank"))

display(top_domain_per_device)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Verticals

# COMMAND ----------

vertical_query = (
f"""
    (SELECT DISTINCT vertical_name, vertical_id
    FROM fpa.advertiser_verticals) as subquery 
""")
vertical_df = loadPostgresQuery(vertical_query, spark)
display(vertical_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plan
# MAGIC ####The end-to-end plan is:
# MAGIC
# MAGIC audit fields
# MAGIC * normalize and type the overloaded domain
# MAGIC * split the pipeline into web vs CTV/app
# MAGIC * use rules and priors for common entities
# MAGIC * use embeddings only as a fallback/generalization layer
# MAGIC * classify entities first, then aggregate to IP
# MAGIC * learn the final blend after you have baseline evidence

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize Domain

# COMMAND ----------

# obvious genre / content tokens that show up in the CTV-style domain values
genre_terms = [
    "comedy", "drama", "documentary", "entertainment", "sports",
    "news", "movies", "movie", "television", "reality",
    "action", "thriller", "lifestyle", "kids", "family",
    "horror", "crime", "history", "science", "travel"
]

genre_terms_arr = F.array(*[F.lit(x) for x in genre_terms])

domain_df = (
    df_out

    # ----------------------------
    # 1) standardize raw strings
    # ----------------------------
    .withColumn("domain_raw", F.lower(F.trim(F.col("domain"))))
    .withColumn("app_bundle_raw", F.lower(F.trim(F.col("app_bundle"))))
    .withColumn("site_name_raw", F.lower(F.trim(F.col("site_name"))))

    .withColumn(
        "domain_is_missing",
        F.col("domain_raw").isNull() | (F.col("domain_raw") == "")
    )

    # helpful validation flags for your hypothesis that domain often mirrors app_bundle or site_name
    .withColumn(
        "domain_matches_app_bundle",
        F.when(
            F.col("domain_raw").isNull() | F.col("app_bundle_raw").isNull(),
            F.lit(False)
        ).otherwise(F.col("domain_raw") == F.col("app_bundle_raw"))
    )
    .withColumn(
        "domain_matches_site_name",
        F.when(
            F.col("domain_raw").isNull() | F.col("site_name_raw").isNull(),
            F.lit(False)
        ).otherwise(F.col("domain_raw") == F.col("site_name_raw"))
    )

    # if the field ever contains a full URL, pull just the host
    .withColumn(
        "domain_url_host",
        F.regexp_extract("domain_raw", r"^(?:https?://)?(?:www\.)?([^/?#]+)", 1)
    )
    .withColumn(
        "domain_host_or_raw",
        F.when(F.col("domain_raw").rlike(r"^https?://"), F.col("domain_url_host"))
         .otherwise(F.col("domain_raw"))
    )

    # remove leading www for web-style domains
    .withColumn(
        "domain_clean_base",
        F.regexp_replace(F.col("domain_host_or_raw"), r"^www\.", "")
    )

    # ----------------------------
    # 2) type the domain semantically
    # ----------------------------
    .withColumn(
        "domain_type",
        F.when(F.col("domain_is_missing"), F.lit("missing"))
         .when(F.col("domain_clean_base").rlike(r"^[0-9]+$"), F.lit("opaque_numeric_id"))
         .when(F.col("environment_type") == "WEB", F.lit("web_domain_like"))
         .when(F.col("page").isNotNull() | F.col("referrer").isNotNull(), F.lit("web_domain_like"))
         .when(F.col("environment_type") == "APP", F.lit("app_or_channel_like"))
         .when(F.col("app_bundle").isNotNull(), F.lit("app_or_channel_like"))
         .otherwise(F.lit("other"))
    )

    # ----------------------------
    # 3) normalize to readable text
    # ----------------------------
    # for web domains, strip common TLDs at the end
    .withColumn(
        "domain_norm_text_pre",
        F.when(
            F.col("domain_type") == "web_domain_like",
            F.regexp_replace(
                F.col("domain_clean_base"),
                r"\.(com|net|org|co|io|tv|us|uk|ca|de|fr|jp|edu|gov|mx|do)$",
                ""
            )
        ).otherwise(F.col("domain_clean_base"))
    )

    # replace separators with spaces
    .withColumn(
        "domain_norm_text",
        F.trim(
            F.regexp_replace(
                F.regexp_replace(F.col("domain_norm_text_pre"), r"[_\.\-\/]+", " "),
                r"\s+",
                " "
            )
        )
    )

    # tokenize
    .withColumn(
        "domain_tokens",
        F.expr("filter(split(domain_norm_text, ' '), x -> x <> '')")
    )

    # obvious CTV/app genre hints
    .withColumn(
        "domain_genre_tokens",
        F.array_intersect(F.col("domain_tokens"), genre_terms_arr)
    )

    # subtype is useful inside the APP/CTV branch
    .withColumn(
        "domain_subtype",
        F.when(F.col("domain_type") != "app_or_channel_like", F.lit(None))
         .when(F.size(F.col("domain_genre_tokens")) > 0, F.lit("genre_labeled_app_channel"))
         .when(F.col("domain_matches_app_bundle"), F.lit("app_bundle_value"))
         .when(F.col("domain_matches_site_name"), F.lit("site_name_value"))
         .when(F.col("domain_clean_base").rlike(r"^[a-z0-9_]+$"), F.lit("brand_or_id_label"))
         .otherwise(F.lit("other_app_channel"))
    )
)

# COMMAND ----------

display(
    domain_df.select(
        "device_type",
        "environment_type",
        "domain",
        "app_bundle",
        "site_name",
        "domain_type",
        "domain_subtype",
        "domain_norm_text",
        "domain_genre_tokens",
        "domain_matches_app_bundle",
        "domain_matches_site_name"
    ).limit(100)
)

# COMMAND ----------

display(
    domain_df
    .groupBy("device_type", "domain_type", "domain_subtype")
    .count()
    .orderBy("device_type", F.desc("count"))
)

# COMMAND ----------

display(
    domain_df
    .groupBy("device_type")
    .agg(
        F.avg(F.when(F.col("domain_matches_app_bundle"), 1).otherwise(0)).alias("pct_match_app_bundle"),
        F.avg(F.when(F.col("domain_matches_site_name"), 1).otherwise(0)).alias("pct_match_site_name")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Out Bad Data

# COMMAND ----------

domain_df = domain_df.filter(~F.col("domain_type").isin(["missing", "opaque_numeric_id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Usage Fields

# COMMAND ----------

semantic_df = (
    domain_df

    .withColumn(
        "app_bundle_text",
        F.when(
            F.col("app_bundle").isNotNull(),
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.col("app_bundle")), r"[_\.\-\/]+", " "),
                    r"\s+",
                    " "
                )
            )
        )
    )
    .withColumn(
        "site_name_text",
        F.when(
            F.col("site_name").isNotNull(),
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.col("site_name")), r"[_\.\-\/]+", " "),
                    r"\s+",
                    " "
                )
            )
        )
    )
    .withColumn(
        "network_text",
        F.when(
            F.col("network").isNotNull(),
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.col("network")), r"[_\.\-\/]+", " "),
                    r"\s+",
                    " "
                )
            )
        )
    )

    .withColumn("has_domain_text", F.col("domain_norm_text").isNotNull() & (F.col("domain_norm_text") != ""))
    .withColumn("has_app_bundle_text", F.col("app_bundle_text").isNotNull() & (F.col("app_bundle_text") != ""))
    .withColumn("has_site_name_text", F.col("site_name_text").isNotNull() & (F.col("site_name_text") != ""))
    .withColumn("has_network_text", F.col("network_text").isNotNull() & (F.col("network_text") != ""))
    .withColumn("has_iab_text", F.col("iab_category_names_text").isNotNull() & (F.col("iab_category_names_text") != ""))

    # quality flags
    .withColumn(
        "domain_is_opaque_numeric",
        (F.col("domain_type") == "opaque_numeric_id")
    )
    .withColumn(
        "app_bundle_is_opaque_numeric",
        F.col("app_bundle_raw").rlike(r"^[0-9]+$")
    )
    .withColumn(
        "app_bundle_match_is_textual",
        F.col("domain_matches_app_bundle") & (~F.col("app_bundle_is_opaque_numeric")) & F.col("has_app_bundle_text")
    )
    .withColumn(
        "app_bundle_match_is_opaque",
        F.col("domain_matches_app_bundle") & F.col("app_bundle_is_opaque_numeric")
    )

    # ----------------------------
    # 1) choose a canonical source
    # ----------------------------
    .withColumn(
        "semantic_entity_source",
        F.when(
            (F.col("domain_type") == "web_domain_like") & F.col("has_domain_text"),
            F.lit("web_domain")
        )
        # prefer explicit genre-labeled app/channel values before generic bundle match
        .when(
            (F.col("domain_subtype") == "genre_labeled_app_channel") & F.col("has_domain_text"),
            F.lit("genre_labeled_app_channel")
        )
        .when(
            F.col("app_bundle_match_is_textual"),
            F.lit("app_bundle_match_textual")
        )
        .when(
            (F.col("domain_type") == "app_or_channel_like") & F.col("has_domain_text") & (~F.col("domain_is_opaque_numeric")),
            F.lit("app_channel_domain")
        )
        # opaque numeric rows should fall back to better human-readable fields if available
        .when(
            F.col("app_bundle_match_is_opaque") & F.col("has_site_name_text"),
            F.lit("opaque_id_site_name_fallback")
        )
        .when(
            F.col("app_bundle_match_is_opaque") & F.col("has_network_text"),
            F.lit("opaque_id_network_fallback")
        )
        .when(
            F.col("domain_is_opaque_numeric") & F.col("has_site_name_text"),
            F.lit("opaque_id_site_name_fallback")
        )
        .when(
            F.col("domain_is_opaque_numeric") & F.col("has_network_text"),
            F.lit("opaque_id_network_fallback")
        )
        .when(
            F.col("domain_is_opaque_numeric") & F.col("has_iab_text"),
            F.lit("opaque_id_iab_fallback")
        )
        .when(
            F.col("has_site_name_text"),
            F.lit("site_name_fallback")
        )
        .when(
            F.col("has_network_text"),
            F.lit("network_fallback")
        )
        .when(
            F.col("has_iab_text"),
            F.lit("iab_fallback")
        )
        .when(
            F.col("domain_is_opaque_numeric") | F.col("app_bundle_match_is_opaque"),
            F.lit("opaque_id_unresolved")
        )
        .otherwise(F.lit("missing"))
    )

    # ----------------------------
    # 2) build canonical semantic text
    # ----------------------------
    .withColumn(
        "semantic_entity_text",
        F.when(
            F.col("semantic_entity_source") == "web_domain",
            F.trim(F.concat_ws(" ", F.col("site_name_text"), F.col("domain_norm_text")))
        )
        .when(
            F.col("semantic_entity_source") == "genre_labeled_app_channel",
            F.trim(F.concat_ws(" ", F.col("site_name_text"), F.col("domain_norm_text")))
        )
        .when(
            F.col("semantic_entity_source") == "app_bundle_match_textual",
            F.trim(F.concat_ws(" ", F.col("site_name_text"), F.col("app_bundle_text")))
        )
        .when(
            F.col("semantic_entity_source") == "app_channel_domain",
            F.trim(F.concat_ws(" ", F.col("site_name_text"), F.col("domain_norm_text")))
        )
        .when(
            F.col("semantic_entity_source") == "opaque_id_site_name_fallback",
            F.col("site_name_text")
        )
        .when(
            F.col("semantic_entity_source") == "opaque_id_network_fallback",
            F.col("network_text")
        )
        .when(
            F.col("semantic_entity_source") == "opaque_id_iab_fallback",
            F.col("iab_category_names_text")
        )
        .when(
            F.col("semantic_entity_source") == "site_name_fallback",
            F.col("site_name_text")
        )
        .when(
            F.col("semantic_entity_source") == "network_fallback",
            F.col("network_text")
        )
        .when(
            F.col("semantic_entity_source") == "iab_fallback",
            F.col("iab_category_names_text")
        )
        .otherwise(F.lit(None))
    )

    .withColumn(
        "semantic_entity_text",
        F.trim(F.regexp_replace(F.col("semantic_entity_text"), r"\s+", " "))
    )

    # ----------------------------
    # 3) assign confidence tier
    # ----------------------------
    .withColumn(
        "semantic_confidence_tier",
        F.when(
            F.col("semantic_entity_source").isin(
                "web_domain",
                "genre_labeled_app_channel",
                "app_bundle_match_textual"
            ),
            F.lit("high")
        )
        .when(
            F.col("semantic_entity_source").isin(
                "app_channel_domain",
                "opaque_id_site_name_fallback",
                "site_name_fallback"
            ),
            F.lit("medium")
        )
        .when(
            F.col("semantic_entity_source").isin(
                "opaque_id_network_fallback",
                "opaque_id_iab_fallback",
                "network_fallback",
                "iab_fallback"
            ),
            F.lit("low")
        )
        .when(
            F.col("semantic_entity_source") == "opaque_id_unresolved",
            F.lit("very_low")
        )
        .otherwise(F.lit("missing"))
    )
)

# COMMAND ----------

display(
    semantic_df.select(
        "device_type",
        "environment_type",
        "domain",
        "app_bundle",
        "site_name",
        "network",
        "domain_type",
        "domain_subtype",
        "semantic_entity_source",
        "semantic_entity_text",
        "semantic_confidence_tier"
    ).limit(100)
)

# COMMAND ----------

display(
    semantic_df
    .groupBy("device_type", "semantic_entity_source", "semantic_confidence_tier")
    .count()
    .orderBy("device_type", F.desc("count"))
)

# COMMAND ----------

top_n = 10

entity_counts = (
    semantic_df
    .filter(F.col("semantic_entity_text").isNotNull() & (F.col("semantic_entity_text") != ""))
    .groupBy("device_type", "semantic_entity_source", "semantic_entity_text")
    .agg(F.countDistinct("ip").alias("ip_count"))
)

w = Window.partitionBy("device_type", "semantic_entity_source").orderBy(F.desc("ip_count"), F.asc("semantic_entity_text"))

display(
    entity_counts
    .withColumn("rank", F.row_number().over(w))
    .filter(F.col("rank") <= top_n)
    .orderBy("device_type", "semantic_entity_source", "rank")
)

# COMMAND ----------



# COMMAND ----------

