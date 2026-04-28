# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### TODO:
# MAGIC * Find More Advertisers to compare
# MAGIC * Use Existing Campaigns Too

# COMMAND ----------

import math
import logging
import random
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

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

num_days = 30
reference_date = "2026-04-01"

# COMMAND ----------

def get_data(run_date: datetime.date, data_source_id: int, num_days: int = 30) -> DataFrame:
    folders = [
        f"gs://mntn-data-archive-prod/ipdsc/dt={(run_date - timedelta(i)).strftime('%Y-%m-%d')}/data_source_id={data_source_id}/*" for i in
        range(1, num_days)
    ]

    base_path = "gs://mntn-data-archive-prod/ipdsc"
    ipdsc_df = spark.read.option("basePath", base_path).parquet(*folders)

    return ipdsc_df

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get DS19 Audiences

# COMMAND ----------

ds19_query = """
(
WITH valid_expressions AS (
    SELECT
        a.advertiser_id,
        b.campaign_id,
        a.audience_id,
        c.campaign_group_id,
        a.expression::jsonb AS expr
    FROM audience.audiences a
    JOIN audience.audience_segments b
        ON a.audience_id = b.audience_id
    JOIN public.campaigns c
        ON a.advertiser_id = c.advertiser_id AND b.campaign_id = c.campaign_id
    WHERE a.expression IS NOT NULL
      AND a.expression::text ~ '^\\{.*\\}$'
),
parsed AS (
    SELECT
        advertiser_id,
        campaign_id,
        audience_id,
        campaign_group_id,
        jsonb_array_elements(
            COALESCE(expr->'interest'->'include', '[]'::jsonb)
        ) AS include_group
    FROM valid_expressions
),
or_groups AS (
    SELECT
        advertiser_id,
        campaign_id,
        audience_id,
        campaign_group_id,
        jsonb_array_elements(
            COALESCE(include_group->'or', '[]'::jsonb)
        ) AS or_item
    FROM parsed
),
ds19_data AS (
    SELECT
        advertiser_id,
        campaign_id,
        audience_id,
        campaign_group_id,
        or_item->'cats' AS cats_array
    FROM or_groups
    WHERE (or_item->>'data_source_id')::int = 19
)
SELECT DISTINCT
    ds19_data.advertiser_id,
    ds19_data.campaign_id
FROM ds19_data
WHERE cats_array IS NOT NULL
) AS subquery
"""

ds19_targets_raw = loadPostgresQuery(
    ds19_query,
    spark,
)

# COMMAND ----------

active_campaign_query = """
(
WITH active_campaigns AS (
    select distinct c.advertiser_id,
               c.campaign_id,
               c.campaign_group_id,
               c.campaign_template_id,
               CASE
                   WHEN ac.campaign_id IS NOT NULL AND ata.campaign_group_id IS NOT NULL THEN 1
                   ELSE 0 END as is_active_campaign
    from public.campaigns c
            inner join audience.audience_x_campaign_groups axcg
                    on c.campaign_group_id = axcg.campaign_group_id
            inner join audience.audiences a
                    on axcg.audience_id = a.audience_id
            left join audience.active_campaigns ac
                    on c.campaign_id = ac.campaign_id
                        and c.campaign_group_id = ac.campaign_group_id
            left join audience.audience_type_alpha ata
                    on c.campaign_group_id = ata.campaign_group_id
    where 1 = 1
    and c.objective_id = 1
    and a.expression_type_id = 2
    and ac.campaign_id IS NOT NULL AND ata.campaign_group_id IS NOT NULL
    and c.campaign_template_id = 10
)
SELECT a.*, b.company_name
FROM active_campaigns a
JOIN (SELECT DISTINCT advertiser_id, company_name FROM public.advertisers) b
ON a.advertiser_id = b.advertiser_id
) AS subquery
"""

active_campaign_df = loadPostgresQuery(
    active_campaign_query,
    spark,
)

# COMMAND ----------

targets = ds19_targets_raw.join(active_campaign_df, ['advertiser_id', 'campaign_id'], 'inner')
targets = targets.select(F.col("advertiser_id"), F.col("campaign_id"), F.col("company_name")).distinct()
display(targets)

# COMMAND ----------

#TO USE:
#31276, 443273, Ferguson Home
#30480,	257722, Lull Mattress
#34838,	383878, Clayton Homes
#37775,	311968, Zazzle
#34611,	446801, HexClad
#50642	523548,	Boosted Safe
#46109	506056,	KOALA
#57882	557525	Xero Shoes
#58106	564496	Greenlight Networks
#57766	556948	Trailer Boss


# COMMAND ----------

data = [
    {"advertiser_id": "31276", "campaign_id": "443273", "company_name": "Ferguson Home"},
    {"advertiser_id": "30480", "campaign_id": "257722", "company_name": "Lull Mattress"},
    {"advertiser_id": "34838", "campaign_id": "383878", "company_name": "Clayton Homes"},
    {"advertiser_id": "37775", "campaign_id": "311968", "company_name": "Zazzle"},
    {"advertiser_id": "34611", "campaign_id": "446801", "company_name": "HexClad"},
    {"advertiser_id": "50642", "campaign_id": "523548", "company_name": "Boosted Safe"},
    {"advertiser_id": "46109", "campaign_id": "506056", "company_name": "KOALA"},
    {"advertiser_id": "57882", "campaign_id": "557525", "company_name": "Xero Shoes"},
    {"advertiser_id": "58106", "campaign_id": "564496", "company_name": "Greenlight Networks"},
    {"advertiser_id": "57766", "campaign_id": "556948", "company_name": "Trailer Boss"},
]

advertiser_targets_df = spark.createDataFrame(data)
display(advertiser_targets_df)

# COMMAND ----------



# COMMAND ----------

