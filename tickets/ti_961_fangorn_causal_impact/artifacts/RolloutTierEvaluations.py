# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# DBTITLE 1,Install google-cloud-bigquery
# MAGIC %pip install google-cloud-bigquery db-dtypes --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import pandas as pd
from google.cloud import bigquery
import matplotlib.pyplot as plt

import google.auth.transport.requests as g_request
import requests
from google.auth import compute_engine
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.dbutils import DBUtils

# COMMAND ----------

dbutils = DBUtils(spark)

dbutils.widgets.dropdown("lookback_days",     "14",   ["14", "21"], "Pre lookback window (days from cutover)")
dbutils.widgets.text("change_threshold",      "0.10", "Visit-rate change threshold (e.g. 0.10 = 10%)")
dbutils.widgets.text("min_impressions_floor", "1000", "Min pre+post impressions for Top-N examples")
dbutils.widgets.text("control_tiers",         "auto", "Control tier nums (comma-sep) or 'auto' = tiers not yet flipped")

lookback_days         = int(dbutils.widgets.get("lookback_days"))
CHANGE_THRESHOLD      = float(dbutils.widgets.get("change_threshold"))
MIN_IMPRESSIONS_FLOOR = int(dbutils.widgets.get("min_impressions_floor"))
_control_tiers_raw    = dbutils.widgets.get("control_tiers").strip()

bq_client = bigquery.Client(project="dw-main-gold")
print(f"lookback: {lookback_days}d · threshold: {CHANGE_THRESHOLD} · min_imps: {MIN_IMPRESSIONS_FLOOR} · control: {_control_tiers_raw}")


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
# MAGIC ### Pull Rollout Tier Advertiser List

# COMMAND ----------

rollout_tier_query = """
(
    SELECT *
    FROM tpa.fangorn_advertiser_inclusion
) as subquery
"""

rollout_tier_df = loadPostgresQuery(rollout_tier_query, spark)

display(rollout_tier_df
        .groupBy(F.col("fangorn_rollout_tier_num"), F.col("fangorn_advertiser_inclusion_date"))
        .agg(F.countDistinct("advertiser_id")))

# COMMAND ----------

from datetime import datetime, timedelta, UTC

today_date      = datetime.now(UTC)
window_end_date = today_date - timedelta(days=1)

min_inclusion = (
    rollout_tier_df
    .filter(F.col("fangorn_advertiser_inclusion_date") <= F.lit(window_end_date))
    .agg(F.min("fangorn_advertiser_inclusion_date").alias("min_inclusion"))
    .first()["min_inclusion"]
)
if min_inclusion is None:
    raise ValueError("No treated advertisers in rollout tier table (all inclusion dates are in the future).")

window_start_date = min_inclusion - timedelta(days=lookback_days)
window_start = window_start_date.strftime("%Y-%m-%d")
window_end   = window_end_date.strftime("%Y-%m-%d")
print(f"Earliest cutover: {min_inclusion} · pre lookback: {lookback_days}d · window {window_start} → {window_end}")


# COMMAND ----------

advertiser_vertical_query = """
SELECT
  a.advertiser_id,
  a.company_name,
  v.vertical_id,
  v.vertical_name
FROM `dw-main-bronze.integrationprod.advertisers` a
LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` v
  ON a.advertiser_id = v.advertiser_id AND v.type = 1
WHERE a.deleted = FALSE AND a.is_test = FALSE
"""
advertiser_vertical_df = spark.createDataFrame(
    bq_client.query(advertiser_vertical_query).to_dataframe()
).dropDuplicates(["advertiser_id"])

display(advertiser_vertical_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull Daily Performance

# COMMAND ----------

daily_performance_query = f"""
WITH mntn_matched_cgids AS (
  -- Campaign groups with an MNTN Matched audience (data_source_id 13 or 19).
  -- Mirrors dw-main-bronze.integrationprod.audience_audience_type_alpha.
  SELECT DISTINCT cg.campaign_group_id
  FROM `dw-main-bronze.integrationprod.audience_audience_x_campaign_groups` axcg
  JOIN `dw-main-bronze.integrationprod.audience_audiences` aus
    ON axcg.audience_id = aus.audience_id
  JOIN `dw-main-bronze.integrationprod.public_campaign_groups` cg
    ON cg.campaign_group_id = axcg.campaign_group_id
    OR cg.parent_campaign_group_id = axcg.campaign_group_id
  WHERE aus.expression_type_id = 2
    AND REGEXP_CONTAINS(aus.expression, r'."data_source_id":\\s?(13|19|46)\\s?[,}}].')
),

prospecting_campaigns AS (
  SELECT DISTINCT
    c.campaign_id,
    c.advertiser_id,
    c.campaign_group_id,
    cg.name AS campaign_group_name
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN `dw-main-bronze.integrationprod.public_campaign_groups` cg
    ON c.campaign_group_id = cg.campaign_group_id
  JOIN mntn_matched_cgids mm ON c.campaign_group_id = mm.campaign_group_id
  WHERE c.funnel_level = 1
    AND c.objective_id = 1
    AND c.deleted = FALSE
    AND c.is_test = FALSE
),

imp AS (
  SELECT
    i.advertiser_id, DATE(i.hour) AS day,
    CAST(SUM(i.display_impressions + i.ctv_impressions) AS INT64) AS impressions,
    CAST(HLL_COUNT.MERGE(i.uniques) AS INT64) AS uniques,
    --CAST(COUNT(DISTINCT pc.campaign_group_id) AS INT64) AS active_cgs,
    CAST(SUM(i.vast_start) AS INT64) AS vast_start,
    CAST(SUM(i.vast_complete) AS INT64) AS vast_complete
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(i.hour) BETWEEN '{window_start}' AND '{window_end}'
  GROUP BY i.advertiser_id, day
),

vis AS (
  SELECT
    v.advertiser_id, DATE(v.hour) AS day,
    CAST(SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS INT64) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(v.hour) BETWEEN '{window_start}' AND '{window_end}'
  GROUP BY v.advertiser_id, day
),

con AS (
  SELECT
    c.advertiser_id, DATE(c.hour) AS day,
    CAST(SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS INT64) AS conversions,
    CAST(SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS FLOAT64) AS order_value
  FROM `dw-main-silver.summarydata.conversion_facts` c
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(c.hour) BETWEEN '{window_start}' AND '{window_end}'
  GROUP BY c.advertiser_id, day
),

sp AS (
  SELECT
    s.advertiser_id, DATE(s.hour) AS day,
    CAST(SUM(s.media_spend + s.data_spend + s.platform_spend) AS FLOAT64) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(s.hour) BETWEEN '{window_start}' AND '{window_end}'
  GROUP BY s.advertiser_id, day
)

SELECT
  imp.advertiser_id,
  a.company_name,
  v.vertical_id,
  v.vertical_name,
  imp.day,
  imp.impressions,
  imp.uniques,
  --imp.active_cgs,
  imp.vast_start,
  imp.vast_complete,
  CAST(COALESCE(vis.vv, 0)          AS INT64)   AS vv,
  CAST(COALESCE(con.conversions, 0) AS INT64)   AS conversions,
  CAST(COALESCE(con.order_value, 0) AS FLOAT64) AS order_value,
  CAST(COALESCE(sp.spend, 0)        AS FLOAT64) AS spend
FROM imp
LEFT JOIN vis ON imp.advertiser_id = vis.advertiser_id AND imp.day = vis.day
LEFT JOIN con ON imp.advertiser_id = con.advertiser_id AND imp.day = con.day
LEFT JOIN sp  ON imp.advertiser_id = sp.advertiser_id  AND imp.day = sp.day
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON imp.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE AND a.is_test = FALSE
LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` v
  ON imp.advertiser_id = v.advertiser_id AND v.type = 1
WHERE imp.impressions > 0
ORDER BY imp.advertiser_id, imp.day;
"""
daily_performance_pd = bq_client.query(daily_performance_query).to_dataframe()
daily_performance_df = spark.createDataFrame(daily_performance_pd)

daily_performance_df = daily_performance_df.join(rollout_tier_df.select(F.col("advertiser_id"), F.col("fangorn_advertiser_inclusion_date"), F.col("fangorn_rollout_tier_num")), "advertiser_id", "inner")

daily_performance_df = daily_performance_df.filter(F.col("day") != F.col("fangorn_advertiser_inclusion_date")) # exclude flip day

# COMMAND ----------

display(daily_performance_df
        .groupBy(F.col("fangorn_rollout_tier_num"), F.col("fangorn_advertiser_inclusion_date"))
        .agg(F.countDistinct("advertiser_id")))

# COMMAND ----------

display(daily_performance_df.filter(F.col("advertiser_id")==32320))
#38659
#32320

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Tier Summary Statistics

# COMMAND ----------

period_df = (
    daily_performance_df
    .withColumn(
        "_lookback_start",
        F.date_sub(F.col("fangorn_advertiser_inclusion_date"), lookback_days),
    )
    .withColumn(
        "period",
        F.when(
            (F.col("day") >= F.col("_lookback_start")) &
            (F.col("day") <  F.col("fangorn_advertiser_inclusion_date")),
            "pre",
        ).when(
            F.col("day") > F.col("fangorn_advertiser_inclusion_date"),
            "post",
        ).otherwise(None),
    )
    .filter(F.col("period").isNotNull())
    .drop("_lookback_start")
)

tier_summary_df = (
    period_df
    .groupBy("fangorn_rollout_tier_num", "period")
    .agg(
        F.countDistinct("advertiser_id").alias("advertisers"),
        F.countDistinct("day").alias("days"),
        F.sum("impressions").alias("impressions"),
        F.sum("vv").alias("visits"),
        F.sum("conversions").alias("conversions"),
        F.sum("order_value").alias("order_value"),
        F.sum("spend").alias("spend"),
    )
    .withColumn("visit_rate", F.col("visits") / F.col("impressions"))
    .withColumn("conv_rate", F.col("conversions") / F.col("impressions"))
    .withColumn("cpv", F.col("spend") / F.col("visits"))
    .withColumn("cpa", F.col("spend") / F.col("conversions"))
    .withColumn("roas", F.col("order_value") / F.col("spend"))
    .orderBy("fangorn_rollout_tier_num", "period")
)

display(tier_summary_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Tier Vertical Summary Stats

# COMMAND ----------

tier_vert_summary_df = (
    period_df
    .groupBy("fangorn_rollout_tier_num", "vertical_id", "vertical_name", "period")
    .agg(
        F.countDistinct("advertiser_id").alias("advertisers"),
        F.countDistinct("day").alias("days"),
        F.sum("impressions").alias("impressions"),
        F.sum("vv").alias("visits"),
        F.sum("conversions").alias("conversions"),
        F.sum("order_value").alias("order_value"),
        F.sum("spend").alias("spend"),
    )
    .withColumn("visit_rate", F.col("visits") / F.col("impressions"))
    .withColumn("conv_rate", F.col("conversions") / F.col("impressions"))
    .withColumn("cpv", F.when(F.col("visits") != 0, F.col("spend") / F.col("visits")).otherwise(None))
    .withColumn("cpa", F.when(F.col("conversions") != 0, F.col("spend") / F.col("conversions")).otherwise(None))
    .withColumn("roas", F.col("order_value") / F.col("spend"))
    .orderBy("fangorn_rollout_tier_num", "vertical_name", "period")
)

display(tier_vert_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Median (Advertiser-Level) Summary
# MAGIC
# MAGIC Aggregate to the advertiser × period grain first, compute rates per advertiser, then take the median across advertisers within each tier (and tier × vertical). This answers "for any random advertiser in this tier, what would we expect."

# COMMAND ----------

advertiser_period_df = (
    period_df
    .groupBy(
        "advertiser_id", "company_name", "vertical_id", "vertical_name",
        "fangorn_rollout_tier_num", "period"
    )
    .agg(
        F.countDistinct("day").alias("days"),
        F.sum("impressions").alias("impressions"),
        F.sum("vv").alias("visits"),
        F.sum("conversions").alias("conversions"),
        F.sum("order_value").alias("order_value"),
        F.sum("spend").alias("spend"),
    )
    .withColumn("visit_rate", F.col("visits") / F.col("impressions"))
    .withColumn("conv_rate",  F.col("conversions") / F.col("impressions"))
    .withColumn("cpv",  F.when(F.col("visits")      != 0, F.col("spend") / F.col("visits")).otherwise(None))
    .withColumn("cpa",  F.when(F.col("conversions") != 0, F.col("spend") / F.col("conversions")).otherwise(None))
    .withColumn("roas", F.when(F.col("spend")       != 0, F.col("order_value") / F.col("spend")).otherwise(None))
)

advertiser_period_df.cache()
display(advertiser_period_df.orderBy("fangorn_rollout_tier_num", "advertiser_id", "period"))

# COMMAND ----------

median_metrics = ["impressions", "visits", "conversions", "spend", "order_value",
                  "visit_rate", "conv_rate", "cpv", "cpa", "roas"]

def median_summary(df: DataFrame, group_cols: list) -> DataFrame:
    agg_exprs = [F.countDistinct("advertiser_id").alias("advertisers")]
    agg_exprs += [F.percentile_approx(c, 0.5).alias(f"median_{c}") for c in median_metrics]
    return df.groupBy(*group_cols).agg(*agg_exprs).orderBy(*group_cols)

tier_median_df = median_summary(
    advertiser_period_df,
    ["fangorn_rollout_tier_num", "period"]
)
display(tier_median_df)

# COMMAND ----------

tier_vert_median_df = median_summary(
    advertiser_period_df,
    ["fangorn_rollout_tier_num", "vertical_id", "vertical_name", "period"]
)
display(tier_vert_median_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Visit Rate Charts

# COMMAND ----------

from matplotlib.ticker import PercentFormatter
import matplotlib.dates as mdates

plt.rcParams.update({
    "figure.facecolor":    "#1e1e1e",
    "axes.facecolor":      "#1e1e1e",
    "savefig.facecolor":   "#1e1e1e",
    "axes.edgecolor":      "#888",
    "axes.labelcolor":     "#ddd",
    "axes.titlecolor":     "#f5f5f5",
    "axes.titleweight":    "semibold",
    "axes.titlesize":      12,
    "axes.labelsize":      10,
    "axes.spines.top":     False,
    "axes.spines.right":   False,
    "axes.grid":           True,
    "grid.color":          "#3a3a3a",
    "grid.linestyle":      "-",
    "grid.linewidth":      0.6,
    "text.color":          "#e5e5e5",
    "xtick.color":         "#bbb",
    "ytick.color":         "#bbb",
    "xtick.labelsize":     9,
    "ytick.labelsize":     9,
    "legend.frameon":      False,
    "legend.labelcolor":   "#e5e5e5",
    "legend.fontsize":     9,
    "lines.linewidth":     1.8,
    "font.family":         "sans-serif",
})

TIER_COLORS = {1: "#5dade2", 2: "#58d68d", 3: "#f5b041", 4: "#bb8fce", 5: "#ec7063"}
SWITCH_COLOR = "#ff6b6b"

def style_axis(ax, ylabel=None, as_percent=False, percent_decimals=2):
    if ylabel:
        ax.set_ylabel(ylabel)
    if as_percent:
        ax.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=percent_decimals))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.tick_params(axis="x", rotation=0)

def add_switch_line(ax, inc):
    if inc is None:
        return
    x = pd.to_datetime(inc)
    ax.axvline(x, color=SWITCH_COLOR, linestyle="--", linewidth=1.4, alpha=0.9)
    ax.text(
        x, ax.get_ylim()[1], f" switch: {pd.Timestamp(inc).date()}",
        color=SWITCH_COLOR, va="top", ha="left", fontsize=9, fontweight="semibold",
    )

window_end_ts = pd.to_datetime(window_end)
tier_inclusion_dates = {
    row["fangorn_rollout_tier_num"]: row["inclusion_date"]
    for row in (
        rollout_tier_df
        .groupBy("fangorn_rollout_tier_num")
        .agg(F.min("fangorn_advertiser_inclusion_date").alias("inclusion_date"))
        .collect()
    )
}
tier_inclusion_dates = {
    t: d for t, d in tier_inclusion_dates.items()
    if d is not None and pd.to_datetime(d) <= window_end_ts
}

daily_tier_pd = (
    daily_performance_df
    .groupBy("fangorn_rollout_tier_num", "day")
    .agg(F.sum("impressions").alias("impressions"),
         F.sum("vv").alias("visits"))
    .withColumn("visit_rate", F.col("visits") / F.col("impressions"))
    .orderBy("fangorn_rollout_tier_num", "day")
    .toPandas()
)
daily_tier_pd["day"] = pd.to_datetime(daily_tier_pd["day"])

tiers = sorted(daily_tier_pd["fangorn_rollout_tier_num"].unique())
fig, axes = plt.subplots(len(tiers), 1, figsize=(12, 2.8 * len(tiers)), sharex=True)
if len(tiers) == 1:
    axes = [axes]
for ax, tier in zip(axes, tiers):
    sub = daily_tier_pd[daily_tier_pd["fangorn_rollout_tier_num"] == tier]
    color = TIER_COLORS.get(tier, "#5dade2")
    ax.plot(sub["day"], sub["visit_rate"], marker="o", markersize=4,
            color=color, label=f"Tier {tier}")
    ax.fill_between(sub["day"], sub["visit_rate"], alpha=0.12, color=color)
    ax.set_title(f"Tier {tier} — Daily Visit Rate")
    style_axis(ax, ylabel="visit rate", as_percent=True)
    add_switch_line(ax, tier_inclusion_dates.get(tier))
axes[-1].set_xlabel("date")
fig.suptitle("Daily Visit Rate by Rollout Tier", fontsize=14, fontweight="semibold",
             color="#f5f5f5", y=1.0)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Difference-in-Differences vs. Untreated Tiers
# MAGIC
# MAGIC For each treated tier, use that tier's inclusion date as the cutoff and apply the same cutoff to the control set (any tier whose inclusion date is after `window_end`, or whose inclusion date is null). DiD = (treated_post − treated_pre) − (control_post − control_pre). Override the control set via the `control_tiers` widget if needed.

# COMMAND ----------

all_tier_inclusion_dates = {
    row["fangorn_rollout_tier_num"]: row["inclusion_date"]
    for row in (
        rollout_tier_df
        .groupBy("fangorn_rollout_tier_num")
        .agg(F.min("fangorn_advertiser_inclusion_date").alias("inclusion_date"))
        .collect()
    )
}

if _control_tiers_raw.lower() == "auto":
    control_tiers = [
        t for t, d in all_tier_inclusion_dates.items()
        if d is None or pd.to_datetime(d) > window_end_ts
    ]
else:
    control_tiers = [int(x) for x in _control_tiers_raw.split(",") if x.strip()]

treated_tiers = [t for t in tier_inclusion_dates.keys() if t not in control_tiers]
print(f"treated tiers: {treated_tiers} · control tiers: {control_tiers}")

def did_visit_rate(daily_pd: pd.DataFrame, treated_tier: int, control_tiers: list, cutoff) -> dict:
    cutoff = pd.to_datetime(cutoff)
    pre_start = cutoff - timedelta(days=lookback_days)
    treated = daily_pd[daily_pd["fangorn_rollout_tier_num"] == treated_tier]
    control = daily_pd[daily_pd["fangorn_rollout_tier_num"].isin(control_tiers)]

    def rate(df, mask):
        sub = df[mask]
        imp = sub["impressions"].sum()
        return (sub["visits"].sum() / imp) if imp else float("nan")

    t_pre  = rate(treated, (treated["day"] >= pre_start) & (treated["day"] < cutoff))
    t_post = rate(treated,  treated["day"] > cutoff)
    c_pre  = rate(control, (control["day"] >= pre_start) & (control["day"] < cutoff))
    c_post = rate(control,  control["day"] > cutoff)

    return {
        "treated_tier": treated_tier,
        "control_tiers": ",".join(map(str, control_tiers)),
        "cutoff": cutoff.date(),
        "treated_pre": t_pre,
        "treated_post": t_post,
        "treated_delta": t_post - t_pre,
        "control_pre": c_pre,
        "control_post": c_post,
        "control_delta": c_post - c_pre,
        "did_additive": (t_post - t_pre) - (c_post - c_pre),
        "did_lift": ((t_post / t_pre) / (c_post / c_pre) - 1) if (t_pre and c_pre and c_post) else float("nan"),
    }

if control_tiers and treated_tiers:
    did_rows = [
        did_visit_rate(daily_tier_pd, tier, control_tiers, tier_inclusion_dates[tier])
        for tier in treated_tiers
    ]
    did_df = pd.DataFrame(did_rows)
    display(did_df)
else:
    print("Skipping DiD: need both treated and control tiers in the window.")

# COMMAND ----------

advertiser_change_df = (
    advertiser_period_df
    .groupBy("advertiser_id", "company_name", "vertical_id", "vertical_name", "fangorn_rollout_tier_num")
    .pivot("period", ["pre", "post"])
    .agg(
        F.first("impressions").alias("impressions"),
        F.first("visits").alias("visits"),
        F.first("visit_rate").alias("visit_rate"),
        F.first("spend").alias("spend"),
    )
    .withColumn(
        "visit_rate_pct_change",
        F.when((F.col("pre_visit_rate") > 0) & F.col("post_visit_rate").isNotNull(),
               (F.col("post_visit_rate") - F.col("pre_visit_rate")) / F.col("pre_visit_rate"))
         .otherwise(None)
    )
    .withColumn(
        "change_bucket",
        F.when(F.col("pre_visit_rate").isNull() | (F.col("pre_visit_rate") == 0), "no_pre_data")
         .when(F.col("post_visit_rate").isNull(),                                  "no_post_data")
         .when(F.col("visit_rate_pct_change") <= -CHANGE_THRESHOLD,                "drop_ge_threshold")
         .when(F.col("visit_rate_pct_change") >=  CHANGE_THRESHOLD,                "rise_ge_threshold")
         .otherwise("within_threshold")
    )
)

advertiser_change_df.cache()
display(
    advertiser_change_df
    .orderBy("fangorn_rollout_tier_num", F.col("visit_rate_pct_change").asc_nulls_last())
)

# COMMAND ----------

change_bucket_tier_df = (
    advertiser_change_df
    .groupBy("fangorn_rollout_tier_num")
    .agg(
        F.countDistinct("advertiser_id").alias("advertisers"),
        F.sum(F.when(F.col("change_bucket") == "drop_ge_threshold", 1).otherwise(0)).alias("drop_ge_threshold"),
        F.sum(F.when(F.col("change_bucket") == "within_threshold",  1).otherwise(0)).alias("within_threshold"),
        F.sum(F.when(F.col("change_bucket") == "rise_ge_threshold", 1).otherwise(0)).alias("rise_ge_threshold"),
        F.sum(F.when(F.col("change_bucket") == "no_pre_data",       1).otherwise(0)).alias("no_pre_data"),
        F.sum(F.when(F.col("change_bucket") == "no_post_data",      1).otherwise(0)).alias("no_post_data"),
        F.percentile_approx("visit_rate_pct_change", 0.5).alias("median_pct_change"),
    )
    .withColumn(
        "evaluable_advertisers",
        F.col("drop_ge_threshold") + F.col("within_threshold") + F.col("rise_ge_threshold"),
    )
    .withColumn(
        "pct_drop_ge_threshold",
        F.when(F.col("evaluable_advertisers") > 0,
               F.col("drop_ge_threshold") / F.col("evaluable_advertisers")).otherwise(None),
    )
    .orderBy("fangorn_rollout_tier_num")
)
display(change_bucket_tier_df)

# COMMAND ----------

threshold_summary_df = (
    advertiser_change_df
    .filter(F.col("visit_rate_pct_change").isNotNull())
    .groupBy("fangorn_rollout_tier_num")
    .agg(
        F.countDistinct("advertiser_id").alias("advertisers_with_pre_post"),
        F.sum(F.when(F.col("change_bucket") == "drop_ge_threshold", 1).otherwise(0)).alias("n_drop_ge_threshold"),
        F.sum(F.when(F.col("change_bucket") == "rise_ge_threshold", 1).otherwise(0)).alias("n_rise_ge_threshold"),
        F.sum(F.when(F.abs(F.col("visit_rate_pct_change")) >= CHANGE_THRESHOLD, 1).otherwise(0))
            .alias("n_change_ge_threshold_either"),
    )
    .withColumn("pct_drop_ge_threshold",          F.col("n_drop_ge_threshold")          / F.col("advertisers_with_pre_post"))
    .withColumn("pct_rise_ge_threshold",          F.col("n_rise_ge_threshold")          / F.col("advertisers_with_pre_post"))
    .withColumn("pct_change_ge_threshold_either", F.col("n_change_ge_threshold_either") / F.col("advertisers_with_pre_post"))
    .orderBy("fangorn_rollout_tier_num")
)
display(threshold_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 drops (worst pre→post visit-rate change)

# COMMAND ----------

example_cols = [
    "fangorn_rollout_tier_num", "advertiser_id", "company_name", "vertical_name",
    "pre_visits", "pre_impressions", "pre_visit_rate", "pre_spend",
    "post_visits", "post_impressions", "post_visit_rate", "post_spend",
    "visit_rate_pct_change",
]

examples_df = (
    advertiser_change_df
    .filter(F.col("visit_rate_pct_change").isNotNull())
    .filter(F.col("pre_impressions")  >= MIN_IMPRESSIONS_FLOOR)
    .filter(F.col("post_impressions") >= MIN_IMPRESSIONS_FLOOR)
    .select(*example_cols)
)

examples_df = (examples_df
               .select(F.col("fangorn_rollout_tier_num").alias("rollout_tier"),
                       F.col("advertiser_id"), F.col("company_name"), F.col("vertical_name"),
                       F.round(F.col("pre_visit_rate"), 3).alias("pre_visit_rate"), 
                       F.round(F.col("pre_spend"), 2).alias("pre_spend"),
                       F.round(F.col("post_visit_rate"), 3).alias("post_visit_rate"), 
                       F.round(F.col("post_spend"), 2).alias("post_spend"),
                       F.round(F.col("visit_rate_pct_change"), 2).alias("visit_rate_pct_change")
                       )
               )

print(f"Top 10 drops (worst pre→post visit-rate change, min {MIN_IMPRESSIONS_FLOOR} imps per period):")
display(examples_df.filter(F.col("visit_rate_pct_change")<0).orderBy(F.col("visit_rate_pct_change").asc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 rises (best pre→post visit-rate change)

# COMMAND ----------

print(f"Top 10 rises (best pre→post visit-rate change, min {MIN_IMPRESSIONS_FLOOR} imps per period):")
display(examples_df.filter(F.col("visit_rate_pct_change")>0).orderBy(F.col("visit_rate_pct_change").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pacing Review

# COMMAND ----------

pacing_query = f"""
(
    SELECT c.advertiser_id, 
           DATE(ct.archived_time) as date, 
           CAST(MIN(ct.pacing_pct) AS FLOAT) as min_pacing_pct
    FROM performance.optimized_intent_threshold_archives ct
    JOIN public.campaigns c
    ON ct.campaign_id = c.campaign_id
    WHERE DATE(ct.archived_time) >= '{window_start}'
    AND DATE(ct.archived_time) <= '{window_end}'
    AND c.deleted = FALSE
    AND c.is_test = FALSE
    AND c.objective_id IN (1, 3, 5)
    GROUP BY 1, 2
) AS subquery
"""

pacing_df = loadPostgresQuery(pacing_query, spark)

# COMMAND ----------

daily_pacing_pd = (
    pacing_df
    .join(
        rollout_tier_df.select("advertiser_id", "fangorn_rollout_tier_num"),
        "advertiser_id",
        "inner",
    )
    .toPandas()
)
daily_pacing_pd["date"] = pd.to_datetime(daily_pacing_pd["date"])

tiers_pacing = sorted(daily_pacing_pd["fangorn_rollout_tier_num"].unique())
fig, axes = plt.subplots(len(tiers_pacing), 1, figsize=(12, 3.2 * len(tiers_pacing)), sharex=True)
if len(tiers_pacing) == 1:
    axes = [axes]
for ax, tier in zip(axes, tiers_pacing):
    sub = daily_pacing_pd[daily_pacing_pd["fangorn_rollout_tier_num"] == tier]
    color = TIER_COLORS.get(tier, "#888")
    for _, adv_df in sub.groupby("advertiser_id"):
        adv_df = adv_df.sort_values("date")
        ax.plot(adv_df["date"], adv_df["min_pacing_pct"],
                color=color, alpha=0.18, linewidth=0.8)
    daily_median = sub.groupby("date")["min_pacing_pct"].median().sort_index()
    ax.plot(daily_median.index, daily_median.values,
            color=color, linewidth=2.4, label="daily median")
    ax.set_title(f"Tier {tier} — Daily Min Pacing % per Advertiser  ·  n={sub['advertiser_id'].nunique()}")
    style_axis(ax, ylabel="min pacing %", as_percent=True, percent_decimals=0)
    add_switch_line(ax, tier_inclusion_dates.get(tier))
    ax.legend(loc="lower right")
axes[-1].set_xlabel("date")
fig.suptitle("Daily Pacing by Rollout Tier", fontsize=14, fontweight="semibold", y=1.0)
plt.tight_layout()
plt.show()

# COMMAND ----------

test = (pacing_df
        .join(
            rollout_tier_df.select("advertiser_id", "fangorn_rollout_tier_num"),
            "advertiser_id", "inner",))
display(test.filter(F.col("fangorn_rollout_tier_num")==3))

# COMMAND ----------

pacing_period_df = (
    pacing_df
    .join(
        rollout_tier_df.select(
            "advertiser_id",
            "fangorn_rollout_tier_num",
            "fangorn_advertiser_inclusion_date",
        ),
        "advertiser_id",
        "inner",
    )
    .join(advertiser_vertical_df, "advertiser_id", "left")
    .withColumn(
        "_lookback_start",
        F.date_sub(F.col("fangorn_advertiser_inclusion_date"), lookback_days),
    )
    .withColumn(
        "period",
        F.when(
            (F.col("date") >= F.col("_lookback_start")) &
            (F.col("date") <  F.col("fangorn_advertiser_inclusion_date")),
            "pre",
        ).when(
            F.col("date") > F.col("fangorn_advertiser_inclusion_date"),
            "post",
        ).otherwise(None),
    )
    .filter(F.col("period").isNotNull())
    .drop("_lookback_start")
)

advertiser_pacing_df = (
    pacing_period_df
    .groupBy(
        "advertiser_id", "company_name", "vertical_id", "vertical_name",
        "fangorn_rollout_tier_num", "period",
    )
    .agg(
        F.count("*").alias("pacing_days"),
        F.avg("min_pacing_pct").alias("avg_pacing_pct"),
        F.percentile_approx("min_pacing_pct", 0.5).alias("median_pacing_pct"),
    )
)

advertiser_pacing_df.cache()
display(advertiser_pacing_df.orderBy("fangorn_rollout_tier_num", "advertiser_id", "period"))


# COMMAND ----------

def pacing_rollup(df: DataFrame, group_cols: list) -> DataFrame:
    return (
        df.groupBy(*group_cols)
        .agg(
            F.countDistinct("advertiser_id").alias("advertisers"),
            F.avg("avg_pacing_pct").alias("avg_of_advertiser_avg_pacing_pct"),
            F.percentile_approx("avg_pacing_pct", 0.5).alias("median_of_advertiser_avg_pacing_pct"),
            F.avg("median_pacing_pct").alias("avg_of_advertiser_median_pacing_pct"),
            F.percentile_approx("median_pacing_pct", 0.5).alias("median_of_advertiser_median_pacing_pct"),
        )
        .orderBy(*group_cols)
    )

tier_pacing_summary_df = pacing_rollup(
    advertiser_pacing_df, ["fangorn_rollout_tier_num", "period"]
)
display(tier_pacing_summary_df)

# COMMAND ----------

tier_vert_pacing_summary_df = pacing_rollup(
    advertiser_pacing_df,
    ["fangorn_rollout_tier_num", "vertical_id", "vertical_name", "period"],
)
display(tier_vert_pacing_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advertiser Funnel Diagnostic
# MAGIC
# MAGIC Track where advertisers drop out between the tracking table and the evaluable set used in the threshold/DiD analyses.
# MAGIC
# MAGIC - **tracked** — advertisers in `tpa.fangorn_advertiser_inclusion`
# MAGIC - **with_impressions** — had ≥1 impression in window
# MAGIC - **with_pre / with_post** — had impressions in each period (excluding flip day)
# MAGIC - **evaluable** — both pre and post (counted in `threshold_summary_df`)
# MAGIC - **dropped_off** — tracked but not evaluable

# COMMAND ----------

tracked_per_tier = (
    rollout_tier_df
    .groupBy("fangorn_rollout_tier_num")
    .agg(F.countDistinct("advertiser_id").alias("tracked"))
)

with_imps_per_tier = (
    daily_performance_df
    .groupBy("fangorn_rollout_tier_num")
    .agg(F.countDistinct("advertiser_id").alias("with_impressions"))
)

with_period_per_tier = (
    advertiser_period_df
    .groupBy("fangorn_rollout_tier_num")
    .pivot("period", ["pre", "post"])
    .agg(F.countDistinct("advertiser_id"))
    .withColumnRenamed("pre", "with_pre")
    .withColumnRenamed("post", "with_post")
)

evaluable_per_tier = (
    advertiser_change_df
    .filter(F.col("visit_rate_pct_change").isNotNull())
    .groupBy("fangorn_rollout_tier_num")
    .agg(F.countDistinct("advertiser_id").alias("evaluable"))
)

funnel_df = (
    tracked_per_tier
    .join(with_imps_per_tier,   "fangorn_rollout_tier_num", "left")
    .join(with_period_per_tier, "fangorn_rollout_tier_num", "left")
    .join(evaluable_per_tier,   "fangorn_rollout_tier_num", "left")
    .fillna(0, ["with_impressions", "with_pre", "with_post", "evaluable"])
    .withColumn("dropped_off", F.col("tracked") - F.col("evaluable"))
    .orderBy("fangorn_rollout_tier_num")
)
display(funnel_df)

# COMMAND ----------

evaluable_ids = (
    advertiser_change_df
    .filter(F.col("visit_rate_pct_change").isNotNull())
    .select("advertiser_id")
)
with_imps_ids = daily_performance_df.select("advertiser_id").distinct()

dropped_advertisers_df = (
    rollout_tier_df
    .select("advertiser_id", "fangorn_rollout_tier_num", "fangorn_advertiser_inclusion_date")
    .join(evaluable_ids, "advertiser_id", "left_anti")
    .join(advertiser_vertical_df, "advertiser_id", "left")
    .join(
        with_imps_ids.withColumn("has_impressions", F.lit(True)),
        "advertiser_id", "left",
    )
    .withColumn("has_impressions", F.coalesce(F.col("has_impressions"), F.lit(False)))
    .withColumn(
        "drop_reason",
        F.when(~F.col("has_impressions"), "no_impressions_in_window")
         .otherwise("missing_pre_or_post"),
    )
    .select(
        "fangorn_rollout_tier_num", "advertiser_id", "company_name",
        "vertical_name", "fangorn_advertiser_inclusion_date", "drop_reason",
    )
    .orderBy("fangorn_rollout_tier_num", "company_name")
)
display(dropped_advertisers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Headline KPIs
# MAGIC
# MAGIC Each tile shows the treated tier's pre/post ratio and the lift after subtracting the same-window change in the control set:
# MAGIC
# MAGIC `lift = (1 + treated_change) / (1 + control_change) − 1`
# MAGIC
# MAGIC Higher is better for IVR / CVR / ROAS; lower is better for CPA.

# COMMAND ----------

daily_tier_metrics_pd = (
    daily_performance_df
    .groupBy("fangorn_rollout_tier_num", "day")
    .agg(
        F.sum("impressions").alias("impressions"),
        F.sum("vv").alias("visits"),
        F.sum("conversions").alias("conversions"),
        F.sum("order_value").alias("order_value"),
        F.sum("spend").alias("spend"),
    )
    .toPandas()
)
daily_tier_metrics_pd["day"] = pd.to_datetime(daily_tier_metrics_pd["day"])


def _safe_div(n, d):
    return n / d if d else float("nan")

def kpi_did(numerator: str, denominator: str, treated_tier: int, control_tiers: list, cutoff) -> dict:
    cutoff = pd.to_datetime(cutoff)
    pre_start = cutoff - timedelta(days=lookback_days)
    df = daily_tier_metrics_pd
    treated = df[df["fangorn_rollout_tier_num"] == treated_tier]
    control = df[df["fangorn_rollout_tier_num"].isin(control_tiers)]

    def rate(sub, mask):
        s = sub[mask]
        return _safe_div(s[numerator].sum(), s[denominator].sum())

    t_pre  = rate(treated, (treated["day"] >= pre_start) & (treated["day"] < cutoff))
    t_post = rate(treated,  treated["day"] > cutoff)
    c_pre  = rate(control, (control["day"] >= pre_start) & (control["day"] < cutoff))
    c_post = rate(control,  control["day"] > cutoff)

    t_lift = _safe_div(t_post, t_pre) - 1 if (t_pre and not pd.isna(t_pre)) else float("nan")
    c_lift = _safe_div(c_post, c_pre) - 1 if (c_pre and not pd.isna(c_pre)) else float("nan")
    did_lift = _safe_div(1 + t_lift, 1 + c_lift) - 1 if not pd.isna(c_lift) else float("nan")

    return {"t_pre": t_pre, "t_post": t_post, "t_lift": t_lift, "did_lift": did_lift}


KPI_SPECS = [
    {"label": "IVR",  "sub": "VISITS / IMPS",     "num": "visits",      "den": "impressions",  "fmt": "pct", "lower_is_better": False},
    {"label": "CVR",  "sub": "CONV / VISITS",     "num": "conversions", "den": "visits",       "fmt": "pct", "lower_is_better": False},
    {"label": "ROAS", "sub": "ORDER / SPEND",     "num": "order_value", "den": "spend",        "fmt": "num", "lower_is_better": False},
    {"label": "CPA",  "sub": "SPEND / CONV",      "num": "spend",       "den": "conversions",  "fmt": "num", "lower_is_better": True},
]


def fmt_kpi_value(x, kind):
    if x is None or pd.isna(x):
        return "—"
    if kind == "pct":
        return f"{x * 100:.2f}%"
    return f"{x:.2f}"

def fmt_lift(x):
    if x is None or pd.isna(x):
        return "—"
    return f"{x * 100:+.1f}%"

def lift_color(x, lower_is_better):
    if x is None or pd.isna(x):
        return "#999"
    better = (x < 0) if lower_is_better else (x > 0)
    return "#1a7f37" if better else "#b91c1c"


sections_html = []
for tier in sorted(treated_tiers):
    if tier not in tier_inclusion_dates:
        continue
    cutoff = tier_inclusion_dates[tier]
    tiles = []
    for spec in KPI_SPECS:
        r = kpi_did(spec["num"], spec["den"], tier, control_tiers, cutoff)
        t_color   = lift_color(r["t_lift"],   spec["lower_is_better"])
        did_color = lift_color(r["did_lift"], spec["lower_is_better"])
        tiles.append(f"""
        <div style="flex:1 1 0; min-width:200px; background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:14px;">
          <div style="font-size:11px; font-weight:600; color:#6b7280; letter-spacing:0.04em; text-transform:uppercase;">
            {spec['label']} <span style="color:#9ca3af; font-weight:500;">({spec['sub']})</span>
          </div>
          <div style="display:flex; justify-content:space-between; margin-top:10px; font-size:14px;">
            <span style="color:#6b7280;">Pre</span><span>{fmt_kpi_value(r['t_pre'], spec['fmt'])}</span>
          </div>
          <div style="display:flex; justify-content:space-between; margin-top:4px; font-size:14px;">
            <span style="color:#6b7280;">Post</span><span>{fmt_kpi_value(r['t_post'], spec['fmt'])}</span>
          </div>
          <div style="margin-top:10px; padding-top:8px; border-top:1px solid #f3f4f6; display:flex; justify-content:space-between; font-weight:600; color:{t_color};">
            <span>Treated lift</span><span>{fmt_lift(r['t_lift'])}</span>
          </div>
          <div style="margin-top:4px; display:flex; justify-content:space-between; font-weight:600; color:{did_color};">
            <span>DiD-adjusted</span><span>{fmt_lift(r['did_lift'])}</span>
          </div>
        </div>
        """)
    sections_html.append(f"""
    <div style="margin-bottom:18px;">
      <div style="font-weight:600; color:#374151; margin-bottom:8px; font-size:13px;">
        Tier {tier} &nbsp;·&nbsp; <span style="color:#6b7280; font-weight:500;">switched {cutoff} · vs control {', '.join(f'Tier {t}' for t in control_tiers)}</span>
      </div>
      <div style="display:flex; gap:12px; flex-wrap:wrap;">{"".join(tiles)}</div>
    </div>
    """)

html = f"""
<div style="font-family:-apple-system, BlinkMacSystemFont, sans-serif; width:100%;">
  {"".join(sections_html)}
</div>
"""

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CausalImpact (Synthetic Control)
# MAGIC
# MAGIC Independent validation of the DiD headline. For each treated tier we fit a
# MAGIC Bayesian structural time-series (BSTS) model where the treated tier's daily
# MAGIC visit rate is the response and the impression-weighted visit rate of the
# MAGIC control tiers is the synthetic-control covariate. CausalImpact estimates
# MAGIC what the treated tier's visit rate would have been WITHOUT Fangorn and
# MAGIC compares that counterfactual to the observed post-period series.
# MAGIC
# MAGIC `ci_pre_days` widget controls the pre-period length used for the BSTS fit
# MAGIC — independent of the DiD `lookback_days` widget. Default 60 days gives
# MAGIC enough headroom for a stable trend/seasonality decomposition.
# MAGIC
# MAGIC Caveat: needs `pip install causalimpact` on the cluster.

# COMMAND ----------

# DBTITLE 1,CausalImpact setup and data pull
dbutils.widgets.text("ci_pre_days", "60", "CausalImpact pre-period length (days from earliest cutover)")
ci_pre_days = int(dbutils.widgets.get("ci_pre_days"))

ci_window_start_date = min_inclusion - timedelta(days=ci_pre_days)
ci_window_start = ci_window_start_date.strftime("%Y-%m-%d")
print(f"CI window: {ci_window_start} → {window_end} ({ci_pre_days}d pre + {(window_end_date.replace(tzinfo=None) - min_inclusion).days}d post)")

# Build the CI panel by combining what daily_performance_df already has
# (window_start_date → window_end_date) with a lean DELTA pull for the
# incremental pre-period (ci_window_start_date → window_start_date - 1d).
#
# Why a lean delta: re-running Alex's full daily_performance_query with the
# 60d window scans ~1 TB (impression_facts + visit_facts + conversion_facts +
# spend_facts × 87 days). CI only needs impressions + visits, and we already
# have window_start → window_end covered. The delta-only lean pull is ~100 GB.

# Reuse already-pulled data (imp + vv only)
existing_pd = (
    daily_performance_df
    .select("advertiser_id", "day", "impressions", "vv")
    .toPandas()
)
existing_pd["day"] = pd.to_datetime(existing_pd["day"])

if ci_window_start_date < window_start_date:
    delta_window_end = (window_start_date - timedelta(days=1)).strftime("%Y-%m-%d")
    delta_query = f"""
    WITH mntn_matched_cgids AS (
      SELECT DISTINCT cg.campaign_group_id
      FROM `dw-main-bronze.integrationprod.audience_audience_x_campaign_groups` axcg
      JOIN `dw-main-bronze.integrationprod.audience_audiences` aus
        ON axcg.audience_id = aus.audience_id
      JOIN `dw-main-bronze.integrationprod.public_campaign_groups` cg
        ON cg.campaign_group_id = axcg.campaign_group_id
        OR cg.parent_campaign_group_id = axcg.campaign_group_id
      WHERE aus.expression_type_id = 2
        AND REGEXP_CONTAINS(aus.expression, r'."data_source_id":\\s?(13|19|46)\\s?[,}}].')
    ),
    prospecting_campaigns AS (
      SELECT DISTINCT c.campaign_id, c.advertiser_id
      FROM `dw-main-bronze.integrationprod.campaigns` c
      JOIN mntn_matched_cgids mm ON c.campaign_group_id = mm.campaign_group_id
      WHERE c.funnel_level = 1
        AND c.objective_id = 1
        AND c.deleted = FALSE
        AND c.is_test = FALSE
    ),
    imp AS (
      SELECT i.advertiser_id, DATE(i.hour) AS day,
             CAST(SUM(i.display_impressions + i.ctv_impressions) AS INT64) AS impressions
      FROM `dw-main-silver.summarydata.impression_facts` i
      JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
      WHERE DATE(i.hour) BETWEEN '{ci_window_start}' AND '{delta_window_end}'
      GROUP BY i.advertiser_id, day
    ),
    vis AS (
      SELECT v.advertiser_id, DATE(v.hour) AS day,
             CAST(SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS INT64) AS vv
      FROM `dw-main-silver.summarydata.visit_facts` v
      JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
      WHERE DATE(v.hour) BETWEEN '{ci_window_start}' AND '{delta_window_end}'
      GROUP BY v.advertiser_id, day
    )
    SELECT imp.advertiser_id, imp.day, imp.impressions,
           CAST(COALESCE(vis.vv, 0) AS INT64) AS vv
    FROM imp
    LEFT JOIN vis USING (advertiser_id, day)
    WHERE imp.impressions > 0
    """
    print(f"[ci] Pulling lean delta: {ci_window_start} → {delta_window_end} (imp + vv only)...")
    delta_pd = bq_client.query(delta_query).to_dataframe()
    delta_pd["day"] = pd.to_datetime(delta_pd["day"])
    ci_daily_pd_raw = pd.concat([delta_pd, existing_pd], ignore_index=True)
    print(f"[ci] Delta rows: {len(delta_pd):,} | combined with existing: {len(ci_daily_pd_raw):,}")
else:
    print(f"[ci] lookback_days={lookback_days} already covers {ci_pre_days}d pre — no delta pull needed")
    ci_daily_pd_raw = existing_pd

# Join rollout tier metadata and exclude flip day
_rollout_pd = (
    rollout_tier_df
    .select("advertiser_id", "fangorn_rollout_tier_num", "fangorn_advertiser_inclusion_date")
    .toPandas()
)
ci_daily_pd = ci_daily_pd_raw.merge(_rollout_pd, on="advertiser_id", how="inner")
ci_daily_pd["day"] = pd.to_datetime(ci_daily_pd["day"])
ci_daily_pd["fangorn_advertiser_inclusion_date"] = pd.to_datetime(ci_daily_pd["fangorn_advertiser_inclusion_date"])
ci_daily_pd = ci_daily_pd[ci_daily_pd["day"] != ci_daily_pd["fangorn_advertiser_inclusion_date"]]

# Aggregate to tier × day
ci_tier_daily = (
    ci_daily_pd
    .groupby(["fangorn_rollout_tier_num", "day"], as_index=False)
    .agg(impressions=("impressions", "sum"), vv=("vv", "sum"))
)
ci_tier_daily["visit_rate"] = ci_tier_daily["vv"] / ci_tier_daily["impressions"]
print(f"[ci] tier × day rows: {len(ci_tier_daily):,} | tiers: {sorted(ci_tier_daily['fangorn_rollout_tier_num'].unique().tolist())}")


# COMMAND ----------

# DBTITLE 1,CausalImpact fits (statsmodels UCM)
import warnings
from statsmodels.tsa.statespace.structural import UnobservedComponents
import numpy as np

def run_ci_for_tier(treated_tier: int, control_tier_list: list, cutoff) -> dict:
    """Fit CausalImpact for one treated tier vs the impression-weighted
    aggregate of control_tier_list as the synthetic-control covariate.
    Uses statsmodels UnobservedComponents (local level + exogenous) instead
    of the deprecated causalimpact package."""
    cutoff = pd.to_datetime(cutoff)
    # Treated series — pooled tier visit rate
    t = (ci_tier_daily[ci_tier_daily["fangorn_rollout_tier_num"] == treated_tier]
         .set_index("day")[["visit_rate"]]
         .rename(columns={"visit_rate": "y"}))
    # Control covariate — impression-weighted aggregate visit rate over control tiers
    c_raw = ci_tier_daily[ci_tier_daily["fangorn_rollout_tier_num"].isin(control_tier_list)]
    c = (c_raw.groupby("day", as_index=True)
              .agg(impressions=("impressions", "sum"), vv=("vv", "sum")))
    c["control_vr"] = c["vv"] / c["impressions"]

    df = t.join(c[["control_vr"]]).dropna().sort_index()
    pre_period  = [df.index.min().strftime("%Y-%m-%d"),
                   (cutoff - timedelta(days=1)).strftime("%Y-%m-%d")]
    post_period = [(cutoff + timedelta(days=1)).strftime("%Y-%m-%d"),
                   df.index.max().strftime("%Y-%m-%d")]
    n_pre  = len(df.loc[pre_period[0]:pre_period[1]])
    n_post = len(df.loc[post_period[0]:post_period[1]])

    base = {
        "treated_tier": treated_tier,
        "control_tiers": ",".join(map(str, control_tier_list)),
        "cutoff": cutoff.strftime("%Y-%m-%d"),
        "pre_start": pre_period[0], "pre_end": pre_period[1], "n_pre": n_pre,
        "post_start": post_period[0], "post_end": post_period[1], "n_post": n_post,
    }
    if n_pre < 30 or n_post < 5:
        return {**base, "skip_reason": f"insufficient days (n_pre={n_pre}, n_post={n_post})"}

    # --- statsmodels UnobservedComponents approach ---
    y_all = df["y"].values.astype(float)
    X_all = df[["control_vr"]].values.astype(float)

    y_pre = y_all[:n_pre]
    X_pre = X_all[:n_pre]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = UnobservedComponents(y_pre, level="local level", exog=X_pre)
        res = model.fit(maxiter=200, disp=False)

    # Forecast counterfactual for post-period
    X_post = X_all[n_pre:n_pre + n_post]
    forecast = res.get_forecast(steps=n_post, exog=X_post)
    predicted_post = forecast.predicted_mean
    ci_post = forecast.conf_int(alpha=0.05)

    # conf_int may return DataFrame or ndarray depending on statsmodels version
    if hasattr(ci_post, 'iloc'):
        lower_bound = ci_post.iloc[:, 0].values
        upper_bound = ci_post.iloc[:, 1].values
    else:
        ci_arr = np.asarray(ci_post)
        lower_bound = ci_arr[:, 0]
        upper_bound = ci_arr[:, 1]

    avg_actual    = float(np.mean(y_all[n_pre:n_pre + n_post]))
    avg_predicted = float(np.mean(predicted_post))
    avg_lower     = float(np.mean(lower_bound))
    avg_upper     = float(np.mean(upper_bound))

    rel_effect    = avg_actual / avg_predicted - 1.0 if avg_predicted else float("nan")
    rel_ci_lower  = avg_actual / avg_upper - 1.0 if avg_upper else float("nan")
    rel_ci_upper  = avg_actual / avg_lower - 1.0 if avg_lower else float("nan")

    # p-value via two-sided test on the effect
    abs_effect = avg_actual - avg_predicted
    se = (avg_upper - avg_lower) / (2 * 1.96)
    from scipy import stats
    p_value = 2 * (1 - stats.norm.cdf(abs(abs_effect) / se)) if se > 0 else 1.0

    return {
        **base,
        "avg_actual":       avg_actual,
        "avg_predicted":    avg_predicted,
        "rel_effect":       rel_effect,
        "rel_ci_95_lower":  rel_ci_lower,
        "rel_ci_95_upper":  rel_ci_upper,
        "p_value":          p_value,
    }

ci_rows = []
for tier in sorted(treated_tiers):
    if tier not in tier_inclusion_dates:
        continue
    print(f"[ci] Fitting tier {tier}...")
    try:
        ci_rows.append(run_ci_for_tier(tier, control_tiers, tier_inclusion_dates[tier]))
    except Exception as e:
        print(f"  [err] tier {tier}: {e}")
        ci_rows.append({"treated_tier": tier, "skip_reason": str(e)})

ci_results_df = pd.DataFrame(ci_rows)
display(ci_results_df)


# COMMAND ----------

# Render CI results as styled tiles consistent with the DiD HTML block above
def fmt_pct_signed(x, digits=1):
    if x is None or pd.isna(x):
        return "—"
    return f"{x * 100:+.{digits}f}%"

def fmt_pct(x, digits=2):
    if x is None or pd.isna(x):
        return "—"
    return f"{x * 100:.{digits}f}%"

def ci_color(row):
    if "rel_effect" not in row or pd.isna(row.get("rel_effect")):
        return "#999"
    sig = row.get("p_value", 1.0) < 0.10
    if row["rel_effect"] > 0:
        return "#1a7f37" if sig else "#86a886"
    return "#b91c1c" if sig else "#c98080"

tiles = []
for row in ci_rows:
    tier = row.get("treated_tier")
    if "rel_effect" not in row:
        tiles.append(f"""
        <div style="flex:1 1 0; min-width:240px; background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:14px;">
          <div style="font-size:11px; font-weight:600; color:#6b7280; letter-spacing:0.04em; text-transform:uppercase;">
            Tier {tier} — CI
          </div>
          <div style="margin-top:10px; color:#6b7280; font-size:13px;">
            {row.get("skip_reason", "no result")}
          </div>
        </div>
        """)
        continue
    color = ci_color(row)
    tiles.append(f"""
    <div style="flex:1 1 0; min-width:240px; background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:14px;">
      <div style="font-size:11px; font-weight:600; color:#6b7280; letter-spacing:0.04em; text-transform:uppercase;">
        Tier {tier} — CI <span style="color:#9ca3af; font-weight:500;">(visit rate vs synthetic)</span>
      </div>
      <div style="display:flex; justify-content:space-between; margin-top:10px; font-size:14px;">
        <span style="color:#6b7280;">Actual avg</span><span>{fmt_pct(row["avg_actual"])}</span>
      </div>
      <div style="display:flex; justify-content:space-between; margin-top:4px; font-size:14px;">
        <span style="color:#6b7280;">Predicted (no flip)</span><span>{fmt_pct(row["avg_predicted"])}</span>
      </div>
      <div style="margin-top:10px; padding-top:8px; border-top:1px solid #f3f4f6; display:flex; justify-content:space-between; font-weight:600; color:{color};">
        <span>Relative effect</span><span>{fmt_pct_signed(row["rel_effect"])}</span>
      </div>
      <div style="display:flex; justify-content:space-between; font-size:12px; color:#6b7280; margin-top:2px;">
        <span>95% CrI</span><span>[{fmt_pct_signed(row["rel_ci_95_lower"])}, {fmt_pct_signed(row["rel_ci_95_upper"])}]</span>
      </div>
      <div style="display:flex; justify-content:space-between; font-size:12px; color:#6b7280; margin-top:2px;">
        <span>p-value</span><span>{row["p_value"]:.3f}</span>
      </div>
      <div style="display:flex; justify-content:space-between; font-size:11px; color:#9ca3af; margin-top:6px;">
        <span>{row["n_pre"]}d pre · {row["n_post"]}d post</span><span>control: tiers {row["control_tiers"]}</span>
      </div>
    </div>
    """)

ci_html = f"""
<div style="font-family:-apple-system, BlinkMacSystemFont, sans-serif; width:100%;">
  <div style="background:#f6f8fa; padding:12px 16px; border-radius:6px; margin-bottom:12px;">
    <b>CausalImpact</b> &nbsp;·&nbsp;
    <b>CI window:</b> {ci_window_start} → {window_end} &nbsp;·&nbsp;
    <b>Covariate:</b> impression-weighted visit rate across control tiers
  </div>
  <div style="display:flex; gap:12px; flex-wrap:wrap;">{"".join(tiles)}</div>
</div>
"""
displayHTML(ci_html)

# COMMAND ----------

# Diagnostic plot — one panel per treated tier, actual vs counterfactual + pointwise effect
import matplotlib.gridspec as gridspec

ci_panels = [r for r in ci_rows if "rel_effect" in r]
if ci_panels:
    fig = plt.figure(figsize=(12, 3.0 * len(ci_panels)))
    gs = gridspec.GridSpec(len(ci_panels), 1, hspace=0.5)

    for i, row in enumerate(ci_panels):
        tier = row["treated_tier"]
        cutoff = pd.to_datetime(row["cutoff"])
        # Reconstruct the series for plotting
        t = (ci_tier_daily[ci_tier_daily["fangorn_rollout_tier_num"] == tier]
             .set_index("day")[["visit_rate"]].rename(columns={"visit_rate": "y"}))
        c_raw = ci_tier_daily[ci_tier_daily["fangorn_rollout_tier_num"].isin(control_tiers)]
        c = (c_raw.groupby("day", as_index=True)
                  .agg(impressions=("impressions", "sum"), vv=("vv", "sum")))
        c["control_vr"] = c["vv"] / c["impressions"]
        df = t.join(c[["control_vr"]]).dropna().sort_index()

        ax = fig.add_subplot(gs[i, 0])
        color = TIER_COLORS.get(tier, "#5dade2")
        ax.plot(df.index, df["y"], color=color, marker="o", markersize=3, label=f"Tier {tier} actual")
        ax.plot(df.index, df["control_vr"], color="#888", linestyle="--", linewidth=1.4, label="control (synthetic) covariate")
        ax.axvline(cutoff, color=SWITCH_COLOR, linestyle="--", linewidth=1.4, alpha=0.9)
        ax.text(cutoff, ax.get_ylim()[1], f" switch: {cutoff.date()}",
                color=SWITCH_COLOR, va="top", ha="left", fontsize=9, fontweight="semibold")
        ax.set_title(f"Tier {tier} — CI rel_effect={fmt_pct_signed(row['rel_effect'])} "
                     f"(95% CrI [{fmt_pct_signed(row['rel_ci_95_lower'])}, {fmt_pct_signed(row['rel_ci_95_upper'])}], "
                     f"p={row['p_value']:.3f})")
        style_axis(ax, ylabel="visit rate", as_percent=True)
        ax.legend(loc="upper left", fontsize=8)

    fig.suptitle(f"CausalImpact daily series — {ci_window_start} → {window_end}",
                 fontsize=14, fontweight="semibold", color="#f5f5f5", y=1.0)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary
# MAGIC
# MAGIC Headline read for GTM. Built from the analyses below — re-runs automatically on each notebook execution. Intended to be promoted to the top of the Databricks Dashboard view.
# MAGIC
# MAGIC **Verdict thresholds** (tunable below):
# MAGIC - 🟢 healthy — DiD non-negative and ≤25% of evaluable advertisers with ≥10% visit-rate drop
# MAGIC - 🟡 monitor — small negative DiD or 25–40% of advertisers with ≥10% drop
# MAGIC - 🔴 concern — DiD < −5% of control baseline or >40% of advertisers with ≥10% drop

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary

# COMMAND ----------

HEALTHY_DROP_PCT_MAX  = 0.25   # ≤25% of evaluable advertisers dropped ≥ threshold → healthy
MONITOR_DROP_PCT_MAX  = 0.40   # 25–40% → monitor; >40% → concern
CONCERN_DID_PCT       = -0.05  # DiD ≤ -5% of control baseline → concern

threshold_pd = threshold_summary_df.toPandas().set_index("fangorn_rollout_tier_num")
pacing_pd    = tier_pacing_summary_df.toPandas()
did_pd       = did_df.set_index("treated_tier") if (control_tiers and treated_tiers) else pd.DataFrame()
tracked_pd   = (
    rollout_tier_df
    .groupBy("fangorn_rollout_tier_num")
    .agg(F.countDistinct("advertiser_id").alias("tracked"))
    .toPandas()
    .set_index("fangorn_rollout_tier_num")
)

tier_summary_pd = (
    tier_summary_df.toPandas()
    .pivot(index="fangorn_rollout_tier_num", columns="period",
           values=["advertisers", "visit_rate", "spend", "days"])
)


def verdict(tier: int) -> tuple:
    did_pct = did_pd.loc[tier, "did_lift"] if tier in did_pd.index else None
    drop_pct = (threshold_pd.loc[tier, "pct_drop_ge_threshold"]
                if tier in threshold_pd.index else None)

    if did_pct is not None and did_pct <= CONCERN_DID_PCT:
        return "🔴", "concern", "DiD shows visit-rate loss vs control"
    if drop_pct is not None and drop_pct > MONITOR_DROP_PCT_MAX:
        return "🔴", "concern", f"{drop_pct:.0%} of advertisers dropped ≥{CHANGE_THRESHOLD:.0%}"
    if did_pct is not None and did_pct < 0:
        return "🟡", "monitor", "Small negative DiD — watch trend"
    if drop_pct is not None and drop_pct > HEALTHY_DROP_PCT_MAX:
        return "🟡", "monitor", f"{drop_pct:.0%} of advertisers dropped ≥{CHANGE_THRESHOLD:.0%}"
    return "🟢", "healthy", "No degradation flagged"


def pct(x, digits=1):
    return "—" if x is None or pd.isna(x) else f"{x * 100:.{digits}f}%"

def fmt(x, digits=4):
    return "—" if x is None or pd.isna(x) else f"{x:.{digits}f}"

def fmt_int(x):
    return "—" if x is None or pd.isna(x) else f"{int(x)}"


MUTED = "#666"

rows_html = []
for tier in sorted(treated_tiers):
    icon, status, reason = verdict(tier)

    pre_vr    = tier_summary_pd.loc[tier, ("visit_rate", "pre")]  if tier in tier_summary_pd.index else None
    post_vr   = tier_summary_pd.loc[tier, ("visit_rate", "post")] if tier in tier_summary_pd.index else None
    pre_days  = tier_summary_pd.loc[tier, ("days", "pre")]  if tier in tier_summary_pd.index else None
    post_days = tier_summary_pd.loc[tier, ("days", "post")] if tier in tier_summary_pd.index else None
    n_eval    = threshold_pd.loc[tier, "advertisers_with_pre_post"] if tier in threshold_pd.index else None
    n_track   = tracked_pd.loc[tier, "tracked"] if tier in tracked_pd.index else None
    n_drop    = threshold_pd.loc[tier, "n_drop_ge_threshold"] if tier in threshold_pd.index else None
    did_pct_v = did_pd.loc[tier, "did_lift"] if tier in did_pd.index else None

    pre_pacing  = pacing_pd[(pacing_pd["fangorn_rollout_tier_num"] == tier) & (pacing_pd["period"] == "pre")]
    post_pacing = pacing_pd[(pacing_pd["fangorn_rollout_tier_num"] == tier) & (pacing_pd["period"] == "post")]
    pre_pacing_med  = pre_pacing["median_of_advertiser_median_pacing_pct"].iloc[0]  if not pre_pacing.empty  else None
    post_pacing_med = post_pacing["median_of_advertiser_median_pacing_pct"].iloc[0] if not post_pacing.empty else None
    drop_pct_v = threshold_pd.loc[tier, "pct_drop_ge_threshold"] if tier in threshold_pd.index else None

    rows_html.append(f"""
    <tr>
      <td style="font-size:20px;text-align:center;padding:10px;">{icon}</td>
      <td style="padding:10px;"><b>Tier {tier}</b><br/><span style="color:{MUTED};font-size:11px;">switched {tier_inclusion_dates.get(tier)}</span></td>
      <td style="padding:10px;"><b>{status}</b><br/><span style="color:{MUTED};font-size:11px;">{reason}</span></td>
      <td style="padding:10px;">{fmt_int(n_track)}<br/><span style="color:{MUTED};font-size:11px;">{fmt_int(n_eval)} evaluable</span></td>
      <td style="padding:10px;">{fmt_int(pre_days)} → {fmt_int(post_days)}</td>
      <td style="padding:10px;">{fmt(pre_vr)} → {fmt(post_vr)}</td>
      <td style="padding:10px;">{pct(did_pct_v)}<br/><span style="color:{MUTED};font-size:11px;">DiD-adjusted lift</span></td>
      <td style="padding:10px;">{fmt_int(n_drop)} / {fmt_int(n_eval)}<br/><span style="color:{MUTED};font-size:11px;">({pct(drop_pct_v, 0)})</span></td>
      <td style="padding:10px;">{pct(pre_pacing_med, 0)} → {pct(post_pacing_med, 0)}</td>
    </tr>
    """)

control_label = ", ".join(f"Tier {t}" for t in control_tiers) if control_tiers else "—"

html = f"""
<div style="font-family:-apple-system, BlinkMacSystemFont, sans-serif; width:100%;">
  <div style="background:#f6f8fa; padding:12px 16px; border-radius:6px; margin-bottom:12px;">
    <b>Window:</b> {window_start} → {window_end} &nbsp;·&nbsp;
    <b>Treated:</b> {', '.join(f'Tier {t}' for t in sorted(treated_tiers)) or '—'} &nbsp;·&nbsp;
    <b>Control:</b> {control_label} &nbsp;·&nbsp;
    <b>Change threshold:</b> ±{CHANGE_THRESHOLD:.0%}
  </div>
  <table style="border-collapse:collapse; width:100%; font-size:13px;">
    <thead>
      <tr style="background:#eef; text-align:left;">
        <th style="padding:10px;"></th>
        <th style="padding:10px;">Tier</th>
        <th style="padding:10px;">Status</th>
        <th style="padding:10px;">Advertisers<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">tracked / evaluable</span></th>
        <th style="padding:10px;">Days<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">pre → post</span></th>
        <th style="padding:10px;">Visit rate<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">pre → post</span></th>
        <th style="padding:10px;">DiD-adjusted lift<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">visit rate, vs control</span></th>
        <th style="padding:10px;">Dropped ≥{int(CHANGE_THRESHOLD*100)}%</th>
        <th style="padding:10px;">Median pacing %<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">pre → post</span></th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>
</div>
"""

displayHTML(html)

# COMMAND ----------

tier_median_pd = (
    tier_median_df.toPandas()
    .pivot(index="fangorn_rollout_tier_num", columns="period",
           values=["median_visit_rate"])
)

tier_median_change_pd = (
    advertiser_change_df
    .filter(F.col("visit_rate_pct_change").isNotNull())
    .groupBy("fangorn_rollout_tier_num")
    .agg(F.percentile_approx("visit_rate_pct_change", 0.5).alias("median_pct_change"))
    .toPandas()
    .set_index("fangorn_rollout_tier_num")
)


def verdict_median(tier: int) -> tuple:
    med_change = (tier_median_change_pd.loc[tier, "median_pct_change"]
                  if tier in tier_median_change_pd.index else None)
    drop_pct = (threshold_pd.loc[tier, "pct_drop_ge_threshold"]
                if tier in threshold_pd.index else None)

    if med_change is not None and med_change <= CONCERN_DID_PCT:
        return "🔴", "concern", f"Typical advertiser visit rate {med_change:+.0%}"
    if drop_pct is not None and drop_pct > MONITOR_DROP_PCT_MAX:
        return "🔴", "concern", f"{drop_pct:.0%} of advertisers dropped ≥{CHANGE_THRESHOLD:.0%}"
    if med_change is not None and med_change < 0:
        return "🟡", "monitor", f"Typical advertiser visit rate {med_change:+.0%}"
    if drop_pct is not None and drop_pct > HEALTHY_DROP_PCT_MAX:
        return "🟡", "monitor", f"{drop_pct:.0%} of advertisers dropped ≥{CHANGE_THRESHOLD:.0%}"
    return "🟢", "healthy", "No degradation flagged"


rows_html = []
for tier in sorted(treated_tiers):
    icon, status, reason = verdict_median(tier)

    pre_vr    = tier_median_pd.loc[tier, ("median_visit_rate", "pre")]  if tier in tier_median_pd.index else None
    post_vr   = tier_median_pd.loc[tier, ("median_visit_rate", "post")] if tier in tier_median_pd.index else None
    med_chg   = tier_median_change_pd.loc[tier, "median_pct_change"] if tier in tier_median_change_pd.index else None
    pre_days  = tier_summary_pd.loc[tier, ("days", "pre")]  if tier in tier_summary_pd.index else None
    post_days = tier_summary_pd.loc[tier, ("days", "post")] if tier in tier_summary_pd.index else None
    n_eval    = threshold_pd.loc[tier, "advertisers_with_pre_post"] if tier in threshold_pd.index else None
    n_track   = tracked_pd.loc[tier, "tracked"] if tier in tracked_pd.index else None
    n_drop    = threshold_pd.loc[tier, "n_drop_ge_threshold"] if tier in threshold_pd.index else None
    drop_pct_v = threshold_pd.loc[tier, "pct_drop_ge_threshold"] if tier in threshold_pd.index else None

    pre_pacing  = pacing_pd[(pacing_pd["fangorn_rollout_tier_num"] == tier) & (pacing_pd["period"] == "pre")]
    post_pacing = pacing_pd[(pacing_pd["fangorn_rollout_tier_num"] == tier) & (pacing_pd["period"] == "post")]
    pre_pacing_med  = pre_pacing["median_of_advertiser_median_pacing_pct"].iloc[0]  if not pre_pacing.empty  else None
    post_pacing_med = post_pacing["median_of_advertiser_median_pacing_pct"].iloc[0] if not post_pacing.empty else None

    rows_html.append(f"""
    <tr>
      <td style="font-size:20px;text-align:center;padding:10px;">{icon}</td>
      <td style="padding:10px;"><b>Tier {tier}</b><br/><span style="color:{MUTED};font-size:11px;">switched {tier_inclusion_dates.get(tier)}</span></td>
      <td style="padding:10px;"><b>{status}</b><br/><span style="color:{MUTED};font-size:11px;">{reason}</span></td>
      <td style="padding:10px;">{fmt_int(n_track)}<br/><span style="color:{MUTED};font-size:11px;">{fmt_int(n_eval)} evaluable</span></td>
      <td style="padding:10px;">{fmt_int(pre_days)} → {fmt_int(post_days)}</td>
      <td style="padding:10px;">{fmt(pre_vr)} → {fmt(post_vr)}</td>
      <td style="padding:10px;">{pct(med_chg)}<br/><span style="color:{MUTED};font-size:11px;">per advertiser</span></td>
      <td style="padding:10px;">{fmt_int(n_drop)} / {fmt_int(n_eval)}<br/><span style="color:{MUTED};font-size:11px;">({pct(drop_pct_v, 0)})</span></td>
      <td style="padding:10px;">{pct(pre_pacing_med, 0)} → {pct(post_pacing_med, 0)}</td>
    </tr>
    """)

html = f"""
<div style="font-family:-apple-system, BlinkMacSystemFont, sans-serif; width:100%;">
  <div style="background:#f6f8fa; padding:12px 16px; border-radius:6px; margin-bottom:12px;">
    <b>Typical advertiser view</b> &nbsp;·&nbsp;
    <b>Window:</b> {window_start} → {window_end} &nbsp;·&nbsp;
    <b>Treated:</b> {', '.join(f'Tier {t}' for t in sorted(treated_tiers)) or '—'} &nbsp;·&nbsp;
    <b>Control:</b> {control_label} &nbsp;·&nbsp;
    <b>Change threshold:</b> ±{CHANGE_THRESHOLD:.0%}
  </div>
  <table style="border-collapse:collapse; width:100%; font-size:13px;">
    <thead>
      <tr style="background:#eef; text-align:left;">
        <th style="padding:10px;"></th>
        <th style="padding:10px;">Tier</th>
        <th style="padding:10px;">Status</th>
        <th style="padding:10px;">Advertisers<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">tracked / evaluable</span></th>
        <th style="padding:10px;">Days<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">pre → post</span></th>
        <th style="padding:10px;">Median visit rate<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">pre → post</span></th>
        <th style="padding:10px;">Median Δ<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">per advertiser</span></th>
        <th style="padding:10px;">Dropped ≥{int(CHANGE_THRESHOLD*100)}%</th>
        <th style="padding:10px;">Median pacing %<br/><span style="font-weight:normal;color:{MUTED};font-size:11px;">pre → post</span></th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>
</div>
"""

displayHTML(html)
