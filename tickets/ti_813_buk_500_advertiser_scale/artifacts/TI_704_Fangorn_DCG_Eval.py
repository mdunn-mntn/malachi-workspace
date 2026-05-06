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

ip_adv_scores = spark.read.parquet("gs://mntn-data-archive-dev/alex.knorr/fangorn_keyword_ip_scoring")
ip_adv_scores = ip_adv_scores.drop("year", "month", "day")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Visits and Impressions

# COMMAND ----------

start_date = "2026-03-04"
end_date = "2026-04-02"

exp_advertiser_list = [
    42692,
    40956, 
    46920,
    42273,
    36420
]

exp_id = "EX-50"

#Experiment date range: 2026-03-04 to 2026-04-02

# COMMAND ----------

exp_results_query = f"""
(
WITH experiment_campaigns AS (
    SELECT COALESCE((regexp_match(cg.name, '^(EX-[0-9]+) '))[1], 'No Experiment Key')    AS experiment_key,
           cg.advertiser_id                                                              AS advertiser_id,
           a.company_name                                                                AS company_name,
           COALESCE(trim((regexp_match(cg.name, ' - (.+)$'))[1]), 'No Experiment Group') AS experiment_group,
           cg.campaign_group_id                                                          AS campaign_group_id,
           cg.name                                                                       AS campaign_name,
           ex.cloned_from_campaign_group_id                                              AS source_campaign_id,
           cg2.name                                                                      AS source_campaign_name
    FROM public.campaign_groups cg
             JOIN public.advertisers a
                  ON a.advertiser_id = cg.advertiser_id
             LEFT JOIN ui.campaign_group_experiments ex ON ex.campaign_group_id = cg.campaign_group_id
             LEFT JOIN public.campaign_groups cg2 ON cg2.campaign_group_id = ex.campaign_group_id
    WHERE cg.campaign_group_id NOT IN (SELECT DISTINCT campaign_group_id FROM experiments.archived_campaign_groups)
      AND (regexp_match(cg.name, '^(EX-[0-9]+) '))[1] IS NOT NULL
      AND COALESCE((regexp_match(cg.name, '^(EX-[0-9]+) '))[1], 'No Experiment Key') = '{exp_id}'
),

spend AS (
    SELECT c.campaign_group_id,
           cil.ip::text                                               AS ip,
           SUM(cil.data_spend + cil.platform_spend + cil.media_spend) AS total_spend,
           COUNT(1)                                                   AS impressions,
           0                                                          AS visits,
           0                                                          AS conversions,
           0                                                          AS order_value
    FROM logdata.cost_impression_log cil
             JOIN public.campaigns c USING (advertiser_id, campaign_id)
             JOIN public.advertisers a USING (advertiser_id)
    WHERE cil.time >= '{start_date}'
      AND cil.time <= '{end_date}'
      AND unlinked = False
      AND datetz(cil.time, a.time_zone) < current_date
      AND EXISTS(SELECT 1
                 FROM experiment_campaigns e
                 WHERE c.campaign_group_id = e.campaign_group_id)
    GROUP BY 1, 2
),

visits AS (
    SELECT c.campaign_group_id,
           regexp_replace(COALESCE(v.impression_ip::text, v.ip::text), '/[0-9]+$', '') AS ip,
           0                                                                           AS total_spend,
           0                                                                           AS impressions,
           COUNT(1)                                                                    AS visits,
           0                                                                           AS conversions,
           0                                                                           AS order_value
    FROM summarydata.ui_visits v
             JOIN public.campaigns c USING (advertiser_id, campaign_id)
             JOIN public.advertisers a USING (advertiser_id)
    WHERE v.time >= '{start_date}'
      AND v.time <= '{end_date}'
      AND datetz(v.time, a.time_zone) < current_date
      AND EXISTS(SELECT 1
                 FROM experiment_campaigns e
                 WHERE c.campaign_group_id = e.campaign_group_id)
    GROUP BY 1, 2
),

conversions AS (
    SELECT c.campaign_group_id,
           regexp_replace(COALESCE(ui.impression_ip::text, ui.ip::text), '/[0-9]+$', '') AS ip,
           0                                                                             AS total_spend,
           0                                                                             AS impressions,
           0                                                                             AS visits,
           COUNT(1)                                                                      AS conversions,
           SUM(ui.order_amt)                                                             AS order_value
    FROM summarydata.ui_conversions ui
             JOIN public.campaigns c USING (advertiser_id, campaign_id)
             JOIN public.advertisers a USING (advertiser_id)
    WHERE ui.time >= '{start_date}'
      AND ui.time <= '{end_date}'
      AND datetz(ui.time, a.time_zone) < current_date
      AND EXISTS(SELECT 1
                 FROM experiment_campaigns e
                 WHERE c.campaign_group_id = e.campaign_group_id)
    GROUP BY 1, 2
)

SELECT e.experiment_key,
       e.advertiser_id,
       e.company_name,
       e.experiment_group,
       a.campaign_group_id,
       a.ip,
       ROUND(SUM(total_spend)::numeric, 2)  AS "total_spend",
       SUM(impressions)::int                 AS "impressions",
       SUM(visits)::int                      AS "visits",
       SUM(conversions)::int                 AS "conversions",
       ROUND(SUM(order_value)::numeric, 2)   AS "revenue"
FROM (SELECT * FROM spend UNION ALL SELECT * FROM visits UNION ALL SELECT * FROM conversions) a
         JOIN experiment_campaigns e USING (campaign_group_id)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5
) AS subquery
"""

# COMMAND ----------

exp_results_df = loadPostgresQuery(exp_results_query, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine Scores and Visits

# COMMAND ----------

keys = ["advertiser_id", "ip"]

ip_adv_scores_small = (
  ip_adv_scores
    .select(*keys, F.col("adjusted_keyword_score"))
    .filter(F.col("ip").isNotNull() & (F.col("ip") != ""))
    .dropDuplicates(keys)
)

combined_df = (
  exp_results_df
    .join(ip_adv_scores_small, keys, "left")
    .fillna({"adjusted_keyword_score": 0.0})
)

# COMMAND ----------

bin_w = 0.05
max_bin = 1.0 - bin_w  # 0.95

binned = (
    combined_df
      .withColumn("score_clip", F.least(F.greatest(F.col("adjusted_keyword_score"), F.lit(0.0)), F.lit(1.0)))
      .withColumn("score_bin_raw", (F.floor(F.col("score_clip") / F.lit(bin_w)) * F.lit(bin_w)).cast("double"))
      .withColumn("score_bin", F.least(F.col("score_bin_raw"), F.lit(max_bin)))
      .withColumn("bin_center", F.col("score_bin") + F.lit(bin_w/2))
      .drop("score_clip", "score_bin_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results and Evaluation

# COMMAND ----------

# ============================================================
# 3. Aggregations
# ============================================================
curve_by_adv_group = (
    binned.groupBy("advertiser_id", "company_name", "experiment_group", "score_bin", "bin_center")
    .agg(
        F.sum("impressions").alias("impressions"),
        F.sum("visits").alias("visits"),
    )
    .withColumn("visit_rate", F.col("visits") / F.col("impressions"))
    .orderBy("advertiser_id", "experiment_group", "score_bin")
)

curve_by_group = (
    binned.groupBy("experiment_group", "score_bin", "bin_center")
    .agg(
        F.sum("impressions").alias("impressions"),
        F.sum("visits").alias("visits"),
    )
    .withColumn("visit_rate", F.col("visits") / F.col("impressions"))
    .orderBy("experiment_group", "score_bin")
)

curve_agg = (
    binned.groupBy("score_bin", "bin_center")
    .agg(
        F.sum("impressions").alias("impressions"),
        F.sum("visits").alias("visits"),
    )
    .withColumn("visit_rate", F.col("visits") / F.col("impressions"))
    .orderBy("score_bin")
)

# COMMAND ----------

# ============================================================
# 4. Summary table
# ============================================================
group_adv_summary = (
    binned.groupBy("advertiser_id", "company_name", "experiment_group")
    .agg(
        F.expr("percentile_approx(adjusted_keyword_score, 0.5)").alias("median_score"),
        F.mean("adjusted_keyword_score").alias("mean_score"),
        F.sum("impressions").alias("total_impressions"),
        F.sum("visits").alias("total_visits"),
        F.countDistinct("ip").alias("unique_ips"),
    )
    .withColumn("overall_visit_rate", F.col("total_visits") / F.col("total_impressions"))
    .orderBy("advertiser_id", "experiment_group")
)

display(group_adv_summary)

# COMMAND ----------

# ============================================================
# Helper: order experiment groups so matched tiers are adjacent
# ============================================================
TIER_ORDER = ["HI", "MI", "PP", "MI with PP"]

def group_sort_key(group_name):
    """Sort so Control X and Treatment X are adjacent, in tier order."""
    for i, tier in enumerate(TIER_ORDER):
        if tier in group_name:
            # Control before Treatment within each tier
            prefix_order = 0 if "Control" in group_name else 1
            return (i, prefix_order)
    return (len(TIER_ORDER), 0)

# ============================================================
# 5. Per-advertiser faceted plots — paired by tier
# ============================================================
pdf_adv = curve_by_adv_group.toPandas()

# Filter out bins with zero visit rate (they drop to -inf on log scale)
pdf_adv_nonzero = pdf_adv[pdf_adv["visit_rate"] > 0.0].copy()

advertisers = sorted(pdf_adv["advertiser_id"].unique())

for adv_id in advertisers:
    adv_data_all = pdf_adv[pdf_adv["advertiser_id"] == adv_id]
    adv_data = pdf_adv_nonzero[pdf_adv_nonzero["advertiser_id"] == adv_id]
    company = adv_data_all["company_name"].iloc[0]

    adv_groups = sorted(adv_data_all["experiment_group"].unique(), key=group_sort_key)
    n_grp = len(adv_groups)

    fig, axes = plt.subplots(1, n_grp, figsize=(5 * n_grp, 5), sharey=True, squeeze=False)
    axes = axes.flatten()

    for idx, grp in enumerate(adv_groups):
        ax1 = axes[idx]

        # Bars use ALL data (including zero bin) for context
        g_all = adv_data_all[adv_data_all["experiment_group"] == grp].sort_values("score_bin")
        ax2 = ax1.twinx()
        ax2.bar(g_all["bin_center"], g_all["impressions"], width=0.04, alpha=0.25, color="gray")
        ax2.set_ylabel("# impressions in bin" if idx == n_grp - 1 else "")

        # Lines exclude zero bin
        g = adv_data[adv_data["experiment_group"] == grp].sort_values("score_bin")
        if len(g) == 0:
            ax1.set_title(f"{grp}")
            continue

        x = g["bin_center"].to_numpy()
        y = g["visit_rate"].to_numpy()
        n = g["impressions"].to_numpy()

        se = np.sqrt(y * (1 - y) / n)
        lo = np.clip(y - 1.96 * se, 0, 1)
        hi = np.clip(y + 1.96 * se, 0, 1)

        ax1.plot(x, y, marker="o")
        #ax1.fill_between(x, lo, hi, alpha=0.2)
        ax1.set_xlabel("Adjusted BUK Score (binned)")
        ax1.set_ylabel("Visit Rate" if idx == 0 else "")
        ax1.set_title(f"{grp}")
        ax1.set_yscale("log")

    fig.suptitle(f"{company} ({adv_id}) — BUK Score vs Visit Rate", fontsize=14, y=1.02)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# ============================================================
# 6. Per-advertiser overlay — exclude zero bin
# ============================================================
for adv_id in advertisers:
    adv_data = pdf_adv_nonzero[pdf_adv_nonzero["advertiser_id"] == adv_id]
    company = adv_data["company_name"].iloc[0] if len(adv_data) > 0 else str(adv_id)
    adv_groups = sorted(adv_data["experiment_group"].unique(), key=group_sort_key)

    fig, ax = plt.subplots(figsize=(10, 6))
    for grp in adv_groups:
        g = adv_data[adv_data["experiment_group"] == grp].sort_values("score_bin")
        ax.plot(g["bin_center"], g["visit_rate"], marker="o", label=grp)

    ax.set_xlabel("Adjusted BUK Score (binned)")
    ax.set_ylabel("Visit Rate")
    ax.set_title(f"{company} ({adv_id}) — Experiment Group Overlay")
    ax.set_yscale("log")
    ax.legend()
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# ============================================================
# 7. Scatter: median BUK score vs visit rate
#    Color = advertiser, marker = control/treatment, label = tier
# ============================================================
pdf_summary = group_adv_summary.toPandas()

# Assign advertiser colors
adv_ids = sorted(pdf_summary["advertiser_id"].unique())
cmap = plt.cm.get_cmap("tab10", len(adv_ids))
adv_color = {aid: cmap(i) for i, aid in enumerate(adv_ids)}
adv_label_map = pdf_summary.groupby("advertiser_id")["company_name"].first().to_dict()

# Marker: control = circle, treatment = diamond
marker_map = {"Control": "o", "Treatment": "D"}

# Extract tier label and control/treatment prefix
pdf_summary["prefix"] = pdf_summary["experiment_group"].apply(
    lambda x: "Control" if "Control" in x else "Treatment"
)
pdf_summary["tier"] = pdf_summary["experiment_group"].apply(
    lambda x: x.replace("Control ", "").replace("Treatment ", "")
)

fig, ax = plt.subplots(figsize=(10, 7))

# Track what's been added to legend
legend_adv = set()
legend_prefix = set()

for _, row in pdf_summary.iterrows():
    c = adv_color[row["advertiser_id"]]
    m = marker_map[row["prefix"]]
    
    # Scatter point
    ax.scatter(
        row["median_score"], row["overall_visit_rate"],
        color=c, marker=m, s=90, edgecolors="white", linewidths=0.5, zorder=3
    )
    
    # Text label = tier abbreviation, offset slightly
    ax.annotate(
        row["tier"], (row["median_score"], row["overall_visit_rate"]),
        textcoords="offset points", xytext=(6, 4), fontsize=7, color=c, alpha=0.85
    )

# Build legend manually: one entry per advertiser (color) + one per prefix (marker)
from matplotlib.lines import Line2D

handles = []
# Advertiser entries
for aid in adv_ids:
    handles.append(Line2D([0], [0], marker="s", color="w", markerfacecolor=adv_color[aid],
                          markersize=9, label=adv_label_map[aid]))
# Separator
handles.append(Line2D([0], [0], color="w", label=""))
# Marker entries
handles.append(Line2D([0], [0], marker="o", color="gray", linestyle="None",
                       markersize=8, label="Control"))
handles.append(Line2D([0], [0], marker="D", color="gray", linestyle="None",
                       markersize=8, label="Treatment"))

ax.legend(handles=handles, fontsize=8, loc="upper left", framealpha=0.9)
ax.set_xlabel("Median BUK Score")
ax.set_ylabel("Overall Visit Rate")
ax.set_title("Median BUK Score vs Visit Rate by Advertiser & Experiment Group")
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# ============================================================
# 8. Aggregate across-advertiser overlay — exclude zero bin
# ============================================================
pdf_group = curve_by_group.toPandas()
pdf_group_nonzero = pdf_group[pdf_group["visit_rate"] > 0.0]

fig, ax = plt.subplots(figsize=(10, 6))
for grp in sorted(pdf_group_nonzero["experiment_group"].unique(), key=group_sort_key):
    g = pdf_group_nonzero[pdf_group_nonzero["experiment_group"] == grp].sort_values("score_bin")
    ax.plot(g["bin_center"], g["visit_rate"], marker="o", label=grp)

ax.set_xlabel("Adjusted BUK Score (binned)")
ax.set_ylabel("Visit Rate")
ax.set_title("BUK Score vs Visit Rate — Aggregate by Experiment Group")
ax.set_yscale("log")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

