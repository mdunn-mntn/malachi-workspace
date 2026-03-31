# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

import math
import logging
import random
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import google.auth.transport.requests as g_request
import requests
from google.auth import compute_engine
from typing import Tuple, Dict, Iterable, Optional

import mlflow
from mlflow.client import MlflowClient
from mlflow.models.signature import infer_signature
from mlflow.spark import log_model

from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame, Window, Row
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils

from pyspark.ml.evaluation import RankingEvaluator, RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.param.shared import Param, Params
from pyspark.ml import Model

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # reasonable starting point

# COMMAND ----------

def token_for_url(url: str) -> str:
    request = g_request.Request()
    credentials = compute_engine.IDTokenCredentials(
        request=request,
        target_audience=url,
        use_metadata_identity_endpoint=True
    )
    credentials.refresh(request)
    return credentials.token


def get_secret(secret_name: str) -> Dict:
    """Retrieve secret from GCP Vault using service account authentication."""
    vault_address = "https://vault.prod.in.mountain.com"
    role = "gcp-workloads"
    path = "shared/global/ti"

    jwt = token_for_url(f"{vault_address}/vault/gcp-workloads")

    auth_resp = requests.post(
        f"{vault_address}/v1/auth/gcp/login",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"role": role, "jwt": jwt}),
    )
    auth_resp.raise_for_status()  # Better error handling
    vault_token = auth_resp.json()["auth"]["client_token"]

    # Get secret from Vault
    secret_resp = requests.get(
        f"{vault_address}/v1/secret/data/{path}/{secret_name}",
        headers={"X-Vault-Token": vault_token},
    )
    secret_resp.raise_for_status()  # Better error handling

    secret_data = secret_resp.json().get("data", {}).get("data")
    return secret_data


def loadPostgresQuery(query: str, session: SparkSession) -> DataFrame:
    secrets = get_secret("coredw")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Scores

# COMMAND ----------

ip_adv_scores = spark.read.parquet("gs://mntn-data-archive-dev/alex.knorr/test_keyword_ip_scoring")

# COMMAND ----------

SEED = 42
N_ADVS = 1000

sampled_advs_df = (
  ip_adv_scores
    .select("advertiser_id")
    .distinct()
    # pseudo-random but deterministic ordering key
    .withColumn("h", F.xxhash64(F.lit(SEED), F.col("advertiser_id")))
    .orderBy("h")
    .limit(N_ADVS)
    .select("advertiser_id")
)

ip_adv_scores_sub = ip_adv_scores.join(F.broadcast(sampled_advs_df), "advertiser_id", "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Visits and Impressions

# COMMAND ----------

#reference_date = "2026-02-22"
reference_date = "2026-03-16"
days_from_ref = 10

end_date = datetime.strftime(datetime.strptime(reference_date, "%Y-%m-%d") + timedelta(days=days_from_ref), "%Y-%m-%d")
impression_date = datetime.strftime(datetime.strptime(reference_date, "%Y-%m-%d") - timedelta(days=30), "%Y-%m-%d")

adv_ids = [r["advertiser_id"] for r in sampled_advs_df.collect()]
adv_ids_sql = ",".join(str(int(x)) for x in adv_ids)  # ensure ints

# COMMAND ----------

combined_query = f"""
(
  WITH visits AS (
    SELECT c.advertiser_id,
           split_part(COALESCE(v.impression_ip::text, v.ip::text), '/', 1) AS ip,
           COUNT(*) AS visits
    FROM summarydata.ui_visits v
    JOIN public.campaigns c USING (advertiser_id, campaign_id)
    WHERE v.time >= '{reference_date}'
      AND v.time <  '{end_date}'
      AND c.advertiser_id IN ({adv_ids_sql})
    GROUP BY 1,2
  ),
  imps AS (
    SELECT c.advertiser_id,
           split_part(cil.ip::text, '/', 1) AS ip,
           COUNT(*) AS impressions
    FROM logdata.cost_impression_log cil
    JOIN public.campaigns c USING (advertiser_id, campaign_id)
    WHERE cil.time >= '{impression_date}'
      AND cil.time <  '{end_date}'
      AND NOT unlinked
      AND c.advertiser_id IN ({adv_ids_sql})
    GROUP BY 1,2
  )
  SELECT COALESCE(i.advertiser_id, v.advertiser_id) AS advertiser_id,
         COALESCE(i.ip, v.ip) AS ip,
         COALESCE(i.impressions, 0) AS impressions,
         COALESCE(v.visits, 0) AS visits
  FROM imps i
  FULL OUTER JOIN visits v USING (advertiser_id, ip)
) AS subquery
"""

# COMMAND ----------

visits_query = f"""
(
  WITH visits AS (
    SELECT c.advertiser_id,
           split_part(COALESCE(v.impression_ip::text, v.ip::text), '/', 1) AS ip,
           COUNT(*) AS visits
    FROM summarydata.ui_visits v
    JOIN public.campaigns c USING (advertiser_id, campaign_id)
    WHERE v.time >= '{reference_date}'
      AND v.time <  '{end_date}'
      AND c.advertiser_id IN ({adv_ids_sql})
    GROUP BY 1,2
  )
  SELECT *
  FROM visits i
) AS subquery
"""

# COMMAND ----------

visit_df = (loadPostgresQuery(visits_query, spark))
#display(visit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine Scores and Visits

# COMMAND ----------

keys = ["advertiser_id", "ip"]

ip_adv_scores_small = (
  ip_adv_scores_sub
    .select(*keys, F.col("adjusted_keyword_score").alias("score"))
    .filter(F.col("ip").isNotNull() & (F.col("ip") != ""))
    .dropDuplicates(keys)
)

combined_df = (
  ip_adv_scores_small
    .join(F.broadcast(visit_df), keys, "left")   # broadcast: usually a big win here
    .fillna({"visits": 0})
    .withColumn("visited_any", (F.col("visits") > 0).cast("int"))
)

# COMMAND ----------

bin_w = 0.05
max_bin = 1.0 - bin_w  # 0.95

binned = (
    combined_df
      .withColumn("score_clip", F.least(F.greatest(F.col("score"), F.lit(0.0)), F.lit(1.0)))
      .withColumn("score_bin_raw", (F.floor(F.col("score_clip") / F.lit(bin_w)) * F.lit(bin_w)).cast("double"))
      .withColumn("score_bin", F.least(F.col("score_bin_raw"), F.lit(max_bin)))
      .withColumn("bin_center", F.col("score_bin") + F.lit(bin_w/2))
      .drop("score_clip", "score_bin_raw")
)

# COMMAND ----------

curve = (
    binned.groupBy("score_bin", "bin_center")
        .agg(
            F.count(F.lit(1)).alias("n_ips"),
            F.sum("visited_any").alias("n_visitors")
        )
        .withColumn("visit_rate", F.col("n_visitors") / F.col("n_ips"))
        .orderBy("score_bin")
)

# COMMAND ----------

display(curve.select("score_bin","bin_center","n_ips","n_visitors","visit_rate").orderBy("score_bin"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot Curve

# COMMAND ----------

pdf = curve.toPandas()
p = pdf["visit_rate"].to_numpy()
n = pdf["n_ips"].to_numpy()  # rename n_pairs if you can
se = np.sqrt(p*(1-p)/n)
pdf["lo"] = np.clip(p - 1.96*se, 0, 1)
pdf["hi"] = np.clip(p + 1.96*se, 0, 1)

x = pdf["bin_center"].to_numpy()
y = pdf["visit_rate"].to_numpy()
n = pdf["n_ips"].to_numpy()

fig, ax1 = plt.subplots(figsize=(10,6))
ax1.plot(x, y, marker="o")
ax1.fill_between(x, pdf["lo"].to_numpy(), pdf["hi"].to_numpy(), alpha=0.2)
ax1.set_xlabel("Adjusted score (binned)")
ax1.set_ylabel("P(any post-period visit)")
ax1.set_title("Visit propensity vs adjusted score")
ax1.set_yscale("log")

ax2 = ax1.twinx()
ax2.bar(x, n, width=0.04, alpha=0.3)
ax2.set_ylabel("# pairs in bin")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quantile Binning

# COMMAND ----------

# from pyspark.ml.feature import Bucketizer

# B = 20  # bins
# qs = [i / B for i in range(1, B)]  # 0.05, 0.10, ..., 0.95

# # approximate cut points (cheap compared to global sort)
# cuts = combined_df.stat.approxQuantile("score", qs, 0.001)
# splits = [-float("inf")] + cuts + [float("inf")]

# bucketizer = Bucketizer(splits=splits, inputCol="score", outputCol="qbin")
# binned_q = bucketizer.transform(combined_df)

# curve_q = (
#     binned_q.groupBy("qbin")
#             .agg(
#                 F.count(F.lit(1)).alias("n_pairs"),
#                 F.sum("visited_any").alias("n_visitors"),
#                 F.avg("score").alias("mean_score"),
#             )
#             .withColumn("visit_rate", F.col("n_visitors") / F.col("n_pairs"))
#             .orderBy("qbin")
# )

# COMMAND ----------

# pdfq = curve_q.toPandas()

# # Extract arrays
# x = pdfq["mean_score"].to_numpy()
# y = pdfq["visit_rate"].to_numpy()
# n = pdfq["n_pairs"].to_numpy()

# fig, ax1 = plt.subplots(figsize=(10, 6))

# # Visit rate line
# ax1.plot(x, y, marker="o")
# ax1.set_xlabel("Mean adjusted score in quantile bin")
# ax1.set_ylabel("P(any post-period visit)")
# ax1.set_title("Visit propensity vs adjusted score (quantile bins)")
# ax1.set_yscale("log")

# # Volume bars on secondary axis
# ax2 = ax1.twinx()

# # Bar width: use spacing between x points (robust)
# if len(x) > 1:
#     w = 0.8 * np.min(np.diff(np.sort(x)))
# else:
#     w = 0.02

# ax2.bar(x, n, width=w, alpha=0.3)
# ax2.set_ylabel("# pairs in bin")

# plt.tight_layout()
# plt.show()

# COMMAND ----------



# COMMAND ----------

