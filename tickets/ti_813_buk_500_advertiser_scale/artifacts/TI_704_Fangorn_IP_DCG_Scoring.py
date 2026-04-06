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

# Increase shuffle partitions to reduce per-task memory pressure
spark.conf.set("spark.sql.shuffle.partitions", "12000")   # try 4000/8000/12000/20000
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")

# Optional: if you suspect broadcast OOMs, disable automatic broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# COMMAND ----------

def get_data(run_date: datetime.date, data_source_id: int, num_days: int = 30) -> DataFrame:
    folders = [
        f"gs://mntn-data-archive-prod/ipdsc/dt={(run_date - timedelta(i)).strftime('%Y-%m-%d')}/data_source_id={data_source_id}/*" for i in
        range(1, num_days)
    ]

    base_path = "gs://mntn-data-archive-prod/ipdsc"
    ipdsc_df = spark.read.option("basePath", base_path).parquet(*folders)

    return ipdsc_df

# COMMAND ----------

exp_advertiser_list = [
    42692,
    40956, 
    46920,
    42273,
    36420
]

#Experiment date range: 2026-03-04 to 2026-04-02

# COMMAND ----------

num_days = 30

reference_date = "2026-03-01"

bucket_name = "targeting-infra-vertex-pipelines-dev"

pred_path = f"gs://{bucket_name}/bottom-up-keywords/batch-predictions/dt={reference_date}/"
preds_df = spark.read.parquet(pred_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read IPDSC 19

# COMMAND ----------

ipdsc_19_df = get_data(datetime.fromisoformat(reference_date), 19, num_days)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode IP Category in Dedup Edges

# COMMAND ----------

preds_scored = (
    preds_df
      .filter(F.col("advertiser_id").isin(exp_advertiser_list))
      .select("advertiser_id", "data_source_category_id", "rank")
      .withColumn("discount", 1.0 / F.log2(F.col("rank") + F.lit(1)))
      .select("advertiser_id", "data_source_category_id", "discount")
)

# COMMAND ----------

#SUBSET TO SELECTED AUDIENCE 

# COMMAND ----------

# Small table: all DSCIDs that can possibly contribute to scores
pred_dscids_df = preds_scored.select("data_source_category_id").distinct()

ip_dsc_distinct = (
    ipdsc_19_df
      .select("ip", F.array_distinct("data_source_category_ids").alias("cats"))
      .withColumn("data_source_category_id", F.explode("cats"))
      .join(F.broadcast(pred_dscids_df), "data_source_category_id", "inner")
      .select("ip", "data_source_category_id")
      .dropDuplicates(["ip", "data_source_category_id"])
)

# COMMAND ----------

app_id = spark.sparkContext.applicationId
spark.sparkContext.setCheckpointDir(f"dbfs:/tmp/checkpoints/{app_id}")

ip_dsc_distinct = ip_dsc_distinct.persist(StorageLevel.DISK_ONLY)
ip_dsc_distinct = ip_dsc_distinct.checkpoint(eager=True)

# COMMAND ----------

ip_adv_scores = (
    ip_dsc_distinct
      .join(preds_scored, "data_source_category_id", "inner")
      .groupBy("advertiser_id", "ip")
      .agg(
          F.sum("discount").alias("dcg"),
          F.count("*").alias("n_hit_cats")
      )
)

#beta = 1.4783120578209858 #derived where 95th percentile should = 0.9 
#beta = 2.186375426733972 #derived where 90th percentile should = 0.9 
beta = 1.8633720951165458 #derived where 90th percentile should = 0.9, updated model

ip_adv_scores = (ip_adv_scores
                 .withColumn("adjusted_keyword_score", 
                             F.lit(1.0) - F.exp(-F.lit(beta) * F.col("dcg"))))

ip_adv_scores = (ip_adv_scores
                 .withColumn("reference_date", F.to_date(F.lit(reference_date), "yyyy-MM-dd"))
                 .withColumn("year",  F.year("reference_date"))
                 .withColumn("month", F.month("reference_date"))
                 .withColumn("day",   F.dayofmonth("reference_date"))
                 .drop("reference_date"))

# COMMAND ----------

(ip_adv_scores
 .write
 .mode("Overwrite")
 .partitionBy("year", "month", "day")
 .parquet("gs://mntn-data-archive-dev/alex.knorr/fangorn_keyword_ip_scoring"))

# COMMAND ----------

