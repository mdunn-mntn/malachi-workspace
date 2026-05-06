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

# MAGIC %md
# MAGIC ### Read Scores

# COMMAND ----------

ip_adv_scores = spark.read.parquet("gs://mntn-data-archive-dev/alex.knorr/test_keyword_ip_scoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solve Optimal Beta

# COMMAND ----------

# qs = [0.50, 0.75, 0.90, 0.95, 0.99]
# rel_err = 0.001  # smaller = more accurate, more work

# q_vals = ip_adv_scores.stat.approxQuantile("dcg", qs, rel_err)
# list(zip(qs, q_vals))

# [(0.5, 0.27023815442731974),
#  (0.75, 0.5575771575509088),
#  (0.9, 1.0531536051576205),
#  (0.95, 1.5575771575509088),
#  (0.99, 3.4217595207793585)]

#updated model
# [(0.5, 0.3034467203076032),
#  (0.75, 0.6309297535714575),
#  (0.9, 1.2357086912638504),
#  (0.95, 1.8555819312830624),
#  (0.99, 3.9941464198551904)]

# COMMAND ----------

#p95 = q_vals[qs.index(0.95)]
#beta = -math.log(1 - 0.90) / p95
#beta 

# #1.4783120578209858

#updated model
#1.2408964832945417

# COMMAND ----------

#p90 = q_vals[qs.index(0.90)]
#beta_90 = -math.log(1 - 0.90) / p90
#beta_90

# #2.186375426733972

#updated model
#1.8633720951165458

# COMMAND ----------

# p95_by_adv = (
#     ip_adv_scores.where(F.col("dcg") > 0)
#       .groupBy("advertiser_id")
#       .agg(F.expr("percentile_approx(dcg, 0.95, 10000)").alias("p95_dcg"))
# )

# x = p95_by_adv.selectExpr("percentile_approx(p95_dcg, 0.5, 10000) as med_p95").collect()[0]["med_p95"]
# # then beta = -ln(1-y)/x
# beta = -math.log(1 - 0.90) / x
# beta

# #1.8624891694172232

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizations

# COMMAND ----------

#beta = 1.4783120578209858
#beta = 2.186375426733972 
beta = 1.8633720951165458

# COMMAND ----------

# cap computed from Spark (approx is fine for visualization)
dcg_cap = ip_adv_scores.where("dcg > 0").stat.approxQuantile("dcg", [0.995], 0.001)[0]

sample_sdf = (
    ip_adv_scores
      .where(F.col("dcg") > 0)
      .where(F.col("dcg") <= F.lit(dcg_cap))
      .where(F.pmod(F.xxhash64("ip"), F.lit(10_000)) == 0)
      .select(
          F.col("dcg").cast("float").alias("dcg"),
          F.col("adjusted_keyword_score").cast("float").alias("adj"),
      )
      .limit(1_000_000)
)

pdf = sample_sdf.toPandas()

# COMMAND ----------

q_points = {
    0.50: 0.3034467203076032,
    0.75: 0.6309297535714575,
    0.90: 1.2357086912638504,
    0.95: 1.8555819312830624,
    0.99: 3.9941464198551904
}

p99 = q_points[0.99]
xmax = p99  # or q_points[0.95] if you want to zoom in

x = np.linspace(0, xmax, 500)
y = 1 - np.exp(-beta * x)

plt.figure(figsize=(10, 6))
plt.plot(x, y)

# Horizontal guide lines (helpful thresholds)
for h in [0.5, 0.8, 0.9, 0.95]:
    plt.axhline(h, linewidth=1)

# Vertical lines + annotated points at quantiles
for q, dcg_q in q_points.items():
    adj_q = 1 - np.exp(-beta * dcg_q)
    plt.axvline(dcg_q, linewidth=1)
    plt.scatter([dcg_q], [adj_q])
    plt.text(dcg_q, adj_q, f" p{int(q*100)}\n dcg={dcg_q:.3f}\n adj={adj_q:.3f}", va="bottom")

plt.title("DCG → Adjusted score mapping: 1 - exp(-beta * dcg)")
plt.xlabel("dcg")
plt.ylabel("adjusted score")
plt.ylim(0, 1.02)
plt.tight_layout()
plt.show()

# COMMAND ----------

dcg = pdf["dcg"].to_numpy()
adj = pdf["adj"].to_numpy()

# Choose DCG histogram range to avoid tail dominating the plot (cap at p99)
dcg_cap = np.quantile(dcg, 0.99)
dcg_clipped = np.clip(dcg, 0, dcg_cap)

plt.figure(figsize=(12, 4))
plt.hist(dcg_clipped, bins=100)
plt.title("DCG distribution (clipped at p99)")
plt.xlabel("dcg (clipped)")
plt.ylabel("count")
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 4))
plt.hist(np.log1p(dcg), bins=100)
plt.title("log1p(DCG) distribution")
plt.xlabel("log1p(dcg)")
plt.ylabel("count")
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 4))
plt.hist(adj, bins=100, range=(0, 1))
plt.title("Adjusted score distribution")
plt.xlabel("adjusted score")
plt.ylabel("count")
plt.tight_layout()
plt.show()

# COMMAND ----------

nx, ny = 80, 80
dcg_cap = ip_adv_scores.where("dcg > 0").stat.approxQuantile("dcg", [0.995], 0.001)[0]

binned = (
    ip_adv_scores
      .where(F.col("dcg") > 0)
      .where(F.col("dcg") <= F.lit(dcg_cap))
      .select(
          F.col("dcg").alias("dcg"),
          F.col("adjusted_keyword_score").alias("adj"),
      )
      .withColumn("xbin", F.floor(F.col("dcg") / F.lit(dcg_cap) * F.lit(nx)).cast("int"))
      .withColumn("ybin", F.floor(F.col("adj") * F.lit(ny)).cast("int"))
      .where((F.col("xbin") >= 0) & (F.col("xbin") < nx) & (F.col("ybin") >= 0) & (F.col("ybin") < ny))
      .groupBy("xbin", "ybin").count()
)

pdf_bins = binned.toPandas()

# Build a dense grid for plotting
grid = np.zeros((ny, nx), dtype=np.float64)
for r in pdf_bins.itertuples(index=False):
    grid[r.ybin, r.xbin] = r.count

plt.figure(figsize=(10, 6))
plt.imshow(
    np.log10(grid + 1),  # log scale
    origin="lower",
    aspect="auto",
    extent=[0, dcg_cap, 0, 1.0],
)
plt.colorbar(label="log10(count + 1)")
plt.title("Binned density of (dcg, adjusted)")
plt.xlabel(f"dcg (capped at p99.5 = {dcg_cap:.3f})")
plt.ylabel("adjusted score")
plt.tight_layout()
plt.show()

# COMMAND ----------

thresholds = np.array([0.5, 0.7, 0.8, 0.9, 0.95])
fracs = [(adj >= t).mean() for t in thresholds]

plt.figure(figsize=(8, 4))
plt.bar([str(t) for t in thresholds], fracs)
plt.title("Fraction of pairs above adjusted-score thresholds")
plt.xlabel("threshold")
plt.ylabel("fraction")
plt.tight_layout()
plt.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

