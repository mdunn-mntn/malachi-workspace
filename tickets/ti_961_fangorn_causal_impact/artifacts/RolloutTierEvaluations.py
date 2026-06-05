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
dbutils.widgets.text("control_tiers",         "5",    "Control tier nums (comma-sep) or 'auto' = tiers not yet flipped. Default 5 = permanent holdout. Tier 4 (about-to-flip) and Tier 99 (auto-enrolled) are mixed populations — don't auto-include.")
dbutils.widgets.text("exclude_dates",         "2026-05-29,2026-05-30", "Days to exclude from all analyses (comma-sep YYYY-MM-DD) — e.g. pacing-issue days")
dbutils.widgets.text("exclude_tiers",         "99",   "Tier nums to exclude from analysis entirely (comma-sep). Default 99 = Express / auto-vertical advertisers (on Fangorn via auto-enrollment, not the structured rollout — exclude from causal analysis).")

lookback_days         = int(dbutils.widgets.get("lookback_days"))
CHANGE_THRESHOLD      = float(dbutils.widgets.get("change_threshold"))
MIN_IMPRESSIONS_FLOOR = int(dbutils.widgets.get("min_impressions_floor"))
_control_tiers_raw    = dbutils.widgets.get("control_tiers").strip()
_exclude_dates_raw    = dbutils.widgets.get("exclude_dates").strip()
_exclude_tiers_raw    = dbutils.widgets.get("exclude_tiers").strip()
EXCLUDE_DATES         = [d.strip() for d in _exclude_dates_raw.split(",") if d.strip()]
EXCLUDE_TIERS         = [int(t.strip()) for t in _exclude_tiers_raw.split(",") if t.strip()]

bq_client = bigquery.Client(project="dw-main-gold")
print(f"lookback: {lookback_days}d · threshold: {CHANGE_THRESHOLD} · min_imps: {MIN_IMPRESSIONS_FLOOR} · control: {_control_tiers_raw} · exclude_dates: {EXCLUDE_DATES or '—'} · exclude_tiers: {EXCLUDE_TIERS or '—'}")


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

if EXCLUDE_TIERS:
    before_n = rollout_tier_df.count()
    rollout_tier_df = rollout_tier_df.filter(~F.col("fangorn_rollout_tier_num").isin(EXCLUDE_TIERS))
    after_n = rollout_tier_df.count()
    print(f"Excluded tiers {EXCLUDE_TIERS} from rollout_tier_df: dropped {before_n - after_n} advertisers ({before_n} → {after_n})")

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

if EXCLUDE_DATES:
    daily_performance_df = daily_performance_df.filter(~F.col("day").isin(EXCLUDE_DATES))
    print(f"Excluded {len(EXCLUDE_DATES)} day(s) from daily_performance_df: {EXCLUDE_DATES}")

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

if EXCLUDE_DATES:
    pacing_df = pacing_df.filter(~F.col("date").isin(EXCLUDE_DATES))
    print(f"Excluded {len(EXCLUDE_DATES)} day(s) from pacing_df: {EXCLUDE_DATES}")

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
    {"label": "IVR",  "sub": "VISITS / IMPS",     "num": "vv",          "den": "impressions",  "fmt": "pct", "lower_is_better": False, "kpi_num_col": "visits"},
    {"label": "CVR",  "sub": "CONV / VISITS",     "num": "conversions", "den": "vv",           "fmt": "pct", "lower_is_better": False, "kpi_num_col": "conversions"},
    {"label": "ROAS", "sub": "ORDER / SPEND",     "num": "order_value", "den": "spend",        "fmt": "num", "lower_is_better": False, "kpi_num_col": "order_value"},
    {"label": "CPA",  "sub": "SPEND / CONV",      "num": "spend",       "den": "conversions",  "fmt": "num", "lower_is_better": True,  "kpi_num_col": "spend"},
]
# `num`/`den` use the daily_performance_df column names (vv not visits, etc.) — used by the bootstrap.
# `kpi_num_col` is the same column under daily_tier_metrics_pd's renamed columns — used by Alex's kpi_did().


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


# ───────────────────────────────────────────────────────────────────────
# Cluster-bootstrap inference for DiD lift
# ───────────────────────────────────────────────────────────────────────
# Resample advertisers with replacement (clustered bootstrap — advertiser is
# the right level of clustering). For each resample, recompute the pooled
# DiD lift. The empirical distribution of bootstrap lifts gives us SE, 95%
# CI, and a two-sided p-value (fraction crossing zero × 2).
import numpy as np

DID_N_BOOT = 1000      # 1000 resamples per (tier, KPI) — ~30s total at 3 tiers × 4 KPIs
DID_SEED   = 42

def _per_aid_pre_post(daily_perf_pd, treated_tier, control_tiers, cutoff, lookback_days):
    """Aggregate per-advertiser pre/post sums of every metric.
    Returns (treated_wide, control_wide) pandas DataFrames with columns:
      advertiser_id, fangorn_rollout_tier_num,
      impressions_pre, impressions_post, vv_pre, vv_post,
      conversions_pre, conversions_post, order_value_pre, order_value_post,
      spend_pre, spend_post.
    """
    cutoff_ts = pd.to_datetime(cutoff)
    pre_start = cutoff_ts - pd.Timedelta(days=lookback_days)

    pdf = daily_perf_pd.copy()
    pre_mask  = (pdf["day"] >= pre_start) & (pdf["day"] < cutoff_ts)
    post_mask =  pdf["day"] > cutoff_ts
    pdf["period"] = np.where(pre_mask, "pre", np.where(post_mask, "post", None))
    pdf = pdf[pdf["period"].notna()]

    agg = pdf.groupby(
        ["advertiser_id", "fangorn_rollout_tier_num", "period"], as_index=False
    ).agg(
        impressions=("impressions", "sum"),
        vv         =("vv",          "sum"),
        conversions=("conversions", "sum"),
        order_value=("order_value", "sum"),
        spend      =("spend",       "sum"),
    )

    metrics = ["impressions", "vv", "conversions", "order_value", "spend"]
    wide = agg.pivot_table(
        index=["advertiser_id", "fangorn_rollout_tier_num"],
        columns="period",
        values=metrics,
    )
    # Force both pre + post columns to exist for every metric — protects
    # against a tier that just flipped having zero post-period days after
    # the EXCLUDE_DATES filter, which would otherwise drop the _post
    # columns and trigger KeyError: 'vv_post' downstream in
    # _did_lift_from_wide. Missing periods get 0.0; the lift function
    # already returns NaN cleanly when denominators are 0.
    full_cols = pd.MultiIndex.from_product(
        [metrics, ["pre", "post"]], names=[None, "period"])
    wide = wide.reindex(columns=full_cols, fill_value=0.0).reset_index()
    wide.columns = [f"{a}_{b}" if b else a for a, b in wide.columns]

    treated = wide[wide["fangorn_rollout_tier_num"] == treated_tier]
    control = wide[wide["fangorn_rollout_tier_num"].isin(control_tiers)]
    return treated, control


def _did_lift_from_wide(treated, control, num, den):
    """Pooled DiD lift = (t_post/t_pre) / (c_post/c_pre) − 1."""
    def rate(df, period):
        n = df[f"{num}_{period}"].sum()
        d = df[f"{den}_{period}"].sum()
        return n / d if d else float("nan")
    t_pre, t_post = rate(treated, "pre"),  rate(treated, "post")
    c_pre, c_post = rate(control, "pre"),  rate(control, "post")
    if any(pd.isna([t_pre, t_post, c_pre, c_post])) or not all([t_pre, c_pre, c_post]):
        return float("nan")
    return (t_post / t_pre) / (c_post / c_pre) - 1


def _did_bootstrap(treated, control, num, den, n_boot=DID_N_BOOT, seed=DID_SEED):
    rng = np.random.default_rng(seed)
    n_t, n_c = len(treated), len(control)
    if n_t == 0 or n_c == 0:
        return np.array([])
    treated_arr = treated.reset_index(drop=True)
    control_arr = control.reset_index(drop=True)
    boot = np.empty(n_boot, dtype=float)
    for i in range(n_boot):
        t_idx = rng.integers(0, n_t, n_t)
        c_idx = rng.integers(0, n_c, n_c)
        boot[i] = _did_lift_from_wide(
            treated_arr.iloc[t_idx], control_arr.iloc[c_idx], num, den
        )
    return boot[~np.isnan(boot)]


def did_inference(treated, control, num, den, n_boot=DID_N_BOOT):
    """Returns dict with point, 95% CI, two-sided p-value, n_t/n_c/n_boot."""
    point = _did_lift_from_wide(treated, control, num, den)
    boot  = _did_bootstrap(treated, control, num, den, n_boot=n_boot)
    if len(boot) == 0:
        return {"point": point, "se": float("nan"), "ci_95_lower": float("nan"),
                "ci_95_upper": float("nan"), "p_value": float("nan"),
                "n_t_aids": len(treated), "n_c_aids": len(control), "n_boot": 0}
    return {
        "point":        point,
        "se":           float(boot.std()),
        "ci_95_lower":  float(np.percentile(boot, 2.5)),
        "ci_95_upper":  float(np.percentile(boot, 97.5)),
        "p_value":      float(2 * min((boot >= 0).mean(), (boot <= 0).mean())),
        "n_t_aids":     len(treated),
        "n_c_aids":     len(control),
        "n_boot":       int(len(boot)),
    }


# Convert daily_performance_df → pandas ONCE for bootstrap (per-tier cache reuses).
_daily_perf_pd_did = (
    daily_performance_df
    .select("advertiser_id", "fangorn_rollout_tier_num", "day",
            "impressions", "vv", "conversions", "order_value", "spend")
    .toPandas()
)
_daily_perf_pd_did["day"] = pd.to_datetime(_daily_perf_pd_did["day"])

# Cache per-advertiser pre/post pivots per tier (one pivot, reused across all 4 KPIs).
_did_aid_cache = {}
def _get_aid_pivots(treated_tier, control_tiers, cutoff):
    key = (treated_tier, tuple(sorted(control_tiers)), str(cutoff))
    if key not in _did_aid_cache:
        _did_aid_cache[key] = _per_aid_pre_post(
            _daily_perf_pd_did, treated_tier, control_tiers, cutoff, lookback_days
        )
    return _did_aid_cache[key]


def sig_color(p, point, lower_is_better, threshold=0.10):
    """Green if significant in the expected direction, gray otherwise."""
    if pd.isna(p) or pd.isna(point):
        return "#6b7280"
    expected_dir = (point < 0) if lower_is_better else (point > 0)
    return "#1a7f37" if (p < threshold and expected_dir) else "#6b7280"


sections_html = []
for tier in sorted(treated_tiers):
    if tier not in tier_inclusion_dates:
        continue
    cutoff = tier_inclusion_dates[tier]
    # Pre-compute per-advertiser pivots once per tier (reused across all 4 KPIs)
    treated_aid, control_aid = _get_aid_pivots(tier, control_tiers, cutoff)
    print(f"[did-inference] Tier {tier}: bootstrapping {len(treated_aid)} treated × {len(control_aid)} control AIDs across {len(KPI_SPECS)} KPIs...")

    tiles = []
    for spec in KPI_SPECS:
        r   = kpi_did(spec["num"] if spec["num"] != "vv" else "visits",
                      spec["den"] if spec["den"] != "vv" else "visits",
                      tier, control_tiers, cutoff)
        inf = did_inference(treated_aid, control_aid, spec["num"], spec["den"], n_boot=DID_N_BOOT)
        t_color    = lift_color(r["t_lift"],   spec["lower_is_better"])
        did_color  = lift_color(r["did_lift"], spec["lower_is_better"])
        pval_color = sig_color(inf["p_value"], inf["point"], spec["lower_is_better"])

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
          <div style="display:flex; justify-content:space-between; font-size:11px; color:#6b7280; margin-top:6px;">
            <span>95% CI</span><span>[{fmt_lift(inf['ci_95_lower'])}, {fmt_lift(inf['ci_95_upper'])}]</span>
          </div>
          <div style="display:flex; justify-content:space-between; font-size:11px; margin-top:2px; font-weight:600; color:{pval_color};">
            <span>p-value</span><span>{'—' if pd.isna(inf['p_value']) else f"{inf['p_value']:.3f}"}</span>
          </div>
          <div style="font-size:10px; color:#9ca3af; margin-top:4px;">
            bootstrap n={inf['n_boot']} · treated={inf['n_t_aids']} aids · control={inf['n_c_aids']} aids
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
# MAGIC Independent validation of the DiD headline. For each treated tier × metric
# MAGIC (IVR + CVR), we fit a UCM / state-space model on the treated tier's daily
# MAGIC metric during the pre-period, then forecast the counterfactual post-period
# MAGIC and compare to actuals.
# MAGIC
# MAGIC **Model:** `level="local level"` + `freq_seasonal=[{"period": 7, "harmonics": 2}]`
# MAGIC + selected `exog`. Weekly seasonality is handled by the state-space
# MAGIC component (not an `is_weekend` dummy); the local-level state handles
# MAGIC temporal correlation in `y` (no lags-of-`y` allowed as covariates — that
# MAGIC would leak post-treatment values into the counterfactual).
# MAGIC
# MAGIC **Covariate candidates: VIF → BIC over 4 exogenous-only candidates per
# MAGIC (tier, metric):**
# MAGIC - `control_vr` / `control_cvr` — denom-weighted rate across control tiers
# MAGIC - `control_vr_lag1` / `control_cvr_lag1` — control rate t−1 (momentum)
# MAGIC - `control_imps` / `control_visits` — control-tier scale covariate (scaled)
# MAGIC - `holiday` — binary US-holiday flag
# MAGIC
# MAGIC VIF iteratively drops any covariate with VIF ≥ 10. BIC does a best-subset
# MAGIC search up to size 5. The winning subset becomes the `exog` matrix for the
# MAGIC UCM. Selected covariates are surfaced on each tile.
# MAGIC
# MAGIC **Inference by simulation, not hand-rolled SE:** the effect's uncertainty
# MAGIC is the counterfactual's uncertainty (actuals are observed/fixed). We draw
# MAGIC N=2000 sample paths from the fitted forecast distribution — this carries
# MAGIC the correct cross-day covariance of the local-level forecast and makes no
# MAGIC normality assumption. 95% CrI = percentiles of the simulated effect
# MAGIC distribution; p-value = two-sided tail probability the simulated
# MAGIC counterfactual beats the observed actual.
# MAGIC
# MAGIC **Pre-period fit diagnostic:** R² and MAPE of one-step-ahead in-sample
# MAGIC predictions vs actual on the pre-period (skipping a short diffuse-init
# MAGIC burn-in). This is the trust check for the whole method: if the UCM can't
# MAGIC track the treated series BEFORE the switch, its post-period counterfactual
# MAGIC is not credible. R² is surfaced on each tile and in each diagnostic chart.
# MAGIC
# MAGIC `ci_pre_days` widget controls the pre-period length (default 60d), independent
# MAGIC of the DiD `lookback_days` widget.

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

# Reuse already-pulled data (imp + vv + conversions)
existing_pd = (
    daily_performance_df
    .select("advertiser_id", "day", "impressions", "vv", "conversions")
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
    ),
    con AS (
      SELECT c.advertiser_id, DATE(c.hour) AS day,
             CAST(SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS INT64) AS conversions
      FROM `dw-main-silver.summarydata.conversion_facts` c
      JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
      WHERE DATE(c.hour) BETWEEN '{ci_window_start}' AND '{delta_window_end}'
      GROUP BY c.advertiser_id, day
    )
    SELECT imp.advertiser_id, imp.day, imp.impressions,
           CAST(COALESCE(vis.vv, 0) AS INT64) AS vv,
           CAST(COALESCE(con.conversions, 0) AS INT64) AS conversions
    FROM imp
    LEFT JOIN vis USING (advertiser_id, day)
    LEFT JOIN con USING (advertiser_id, day)
    WHERE imp.impressions > 0
    """
    print(f"[ci] Pulling lean delta: {ci_window_start} → {delta_window_end} (imp + vv + conversions)...")
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

if EXCLUDE_DATES:
    _exclude_ts = pd.to_datetime(EXCLUDE_DATES)
    before = len(ci_daily_pd)
    ci_daily_pd = ci_daily_pd[~ci_daily_pd["day"].isin(_exclude_ts)]
    print(f"[ci] Excluded {len(EXCLUDE_DATES)} day(s) from ci_daily_pd: dropped {before - len(ci_daily_pd):,} rows")

# Aggregate to tier × day
ci_tier_daily = (
    ci_daily_pd
    .groupby(["fangorn_rollout_tier_num", "day"], as_index=False)
    .agg(impressions=("impressions", "sum"), vv=("vv", "sum"), conversions=("conversions", "sum"))
)
ci_tier_daily["visit_rate"] = ci_tier_daily["vv"] / ci_tier_daily["impressions"]
ci_tier_daily["cvr"]        = np.where(ci_tier_daily["vv"] > 0,
                                       ci_tier_daily["conversions"] / ci_tier_daily["vv"],
                                       np.nan)
print(f"[ci] tier × day rows: {len(ci_tier_daily):,} | tiers: {sorted(ci_tier_daily['fangorn_rollout_tier_num'].unique().tolist())}")


# COMMAND ----------

# DBTITLE 1,CausalImpact fits (statsmodels UCM + VIF→BIC covariate selection)
import warnings
import numpy as np
import statsmodels.api as sm
from itertools import combinations
from statsmodels.tsa.statespace.structural import UnobservedComponents
from statsmodels.stats.outliers_influence import variance_inflation_factor

# Disable MLflow autologging for this section. Databricks autolog wraps every
# sm.OLS().fit() (60+ per tier × metric from the BIC best-subset search) and
# every UnobservedComponents.fit() with a sync tracking-server write. With
# IVR + CVR that's ~400 fits per run — the autolog overhead dominates wall
# time (a 5-min cell becomes 45+ min). We don't need per-fit MLflow runs
# here; the cell produces a results DataFrame that's the actual artifact.
try:
    import mlflow
    mlflow.autolog(disable=True)
    mlflow.statsmodels.autolog(disable=True)
    mlflow.sklearn.autolog(disable=True)
except Exception as _e:
    print(f"[ci] mlflow autolog disable skipped: {_e}")

# US holidays in the analysis window (extend if window pushes earlier/later)
CI_HOLIDAYS = pd.to_datetime([
    "2026-02-14", "2026-02-16",   # Valentine's, Presidents' Day
    "2026-03-08", "2026-03-17",   # Daylight Saving, St. Patrick's
    "2026-04-05",                  # Easter
    "2026-05-10", "2026-05-25",   # Mother's Day, Memorial Day
])

# Candidate covariates at the TIER × DAY grain. Canonical TI-748/849 pattern
# adapted for tier-level series. Notably excludes treated-tier OWN impressions
# (could absorb part of the treatment effect — Fangorn changes who gets
# impressions, so own-impressions is post-treatment downstream).
#
# Per-metric specs control:
#   - target:             column in ci_tier_daily used as the treated y series
#   - control_num/den:    columns summed across control tiers to compute the
#                         control rate (analogue of treated y)
#   - control_scale_col:  column summed across control tiers, divided by
#                         scale_factor for numerical stability
#   - scale_factor:       1e9 for impressions, 1e6 for visits — keep ~O(1)
#   - control_rate_name / control_scale_name:  named covariates in the model
CI_METRIC_SPECS = [
    {
        "label":              "IVR",
        "sub":                "visits / impressions",
        "target":             "visit_rate",
        "control_num":        "vv",
        "control_den":        "impressions",
        "control_scale_col":  "impressions",
        "scale_factor":       1e9,
        "control_rate_name":  "control_vr",
        "control_scale_name": "control_imps",
    },
    {
        "label":              "CVR",
        "sub":                "conversions / visits",
        "target":             "cvr",
        "control_num":        "conversions",
        "control_den":        "vv",
        "control_scale_col":  "vv",
        "scale_factor":       1e6,
        "control_rate_name":  "control_cvr",
        "control_scale_name": "control_visits",
    },
]

def _candidates_for(spec: dict) -> list:
    # 4 candidates per (tier, metric). Deliberately exogenous-only.
    # - control_rate / control_rate_lag1 / control_scale: come from CONTROL
    #   tiers, no treatment leakage
    # - holiday: calendar event, exogenous
    # Note: NO metric_lag1 / metric_lag7 here. Those are lags of the TREATED
    # outcome y; conditioning the forecast on them would leak post-treatment
    # values into the counterfactual and bias the effect toward zero. The
    # UCM local-level state handles temporal correlation in y without leakage.
    # Note: NO is_weekend dummy either. Weekly periodicity is captured properly
    # by the freq_seasonal=[{"period": 7}] component added in run_ci_for_tier.
    return [
        spec["control_rate_name"],
        f"{spec['control_rate_name']}_lag1",
        spec["control_scale_name"],
        "holiday",
    ]

def drop_high_vif(features: pd.DataFrame, vif_threshold: float = 10.0) -> list:
    """Iteratively drop the highest-VIF covariate until all are below threshold."""
    keep = list(features.columns)
    while len(keep) > 1:
        X = features[keep].fillna(0.0)
        X_const = sm.add_constant(X, has_constant="add")
        try:
            vifs = [variance_inflation_factor(X_const.values, i + 1)
                    for i in range(len(keep))]
        except Exception:
            break
        max_v = max(vifs)
        if max_v < vif_threshold:
            break
        worst = keep[vifs.index(max_v)]
        keep.remove(worst)
    return keep

def best_subset_by_bic(target: pd.Series, features: pd.DataFrame,
                       max_size: int = 5):
    """Search all subsets up to max_size, return (best_subset, best_bic)."""
    cols = list(features.columns)
    best_bic = float("inf")
    best_subset = []
    y = target.dropna()
    for k in range(1, min(max_size, len(cols)) + 1):
        for subset in combinations(cols, k):
            X = sm.add_constant(features[list(subset)].loc[y.index].fillna(0.0),
                                has_constant="add")
            try:
                bic = sm.OLS(y, X).fit().bic
            except Exception:
                continue
            if bic < best_bic:
                best_bic = bic
                best_subset = list(subset)
    return best_subset, best_bic

def run_ci_for_tier(treated_tier: int, control_tier_list: list, cutoff,
                    metric_spec: dict) -> dict:
    """CausalImpact-style synthetic control via UCM with VIF→BIC-selected exog.

    Pipeline:
      1. Build candidate covariates at tier × day grain (control rate +
         control scale + holiday + is_weekend + metric_lag1/7). The two
         control covariates are metric-specific via metric_spec.
      2. VIF — iteratively drop the highest-VIF covariate until all VIF < 10.
      3. BIC — best-subset search up to size 5; OLS on pre-period y.
      4. UCM (local level + selected exog) on pre-period; forecast post.
      5. Relative effect = avg(actual) / avg(predicted_counterfactual) − 1,
         with 95% prediction interval and a two-sided z-test p-value.
    """
    cutoff = pd.to_datetime(cutoff)
    candidates = _candidates_for(metric_spec)
    target_col = metric_spec["target"]

    # Treated series — pooled tier metric
    t = (ci_tier_daily[ci_tier_daily["fangorn_rollout_tier_num"] == treated_tier]
         .set_index("day")[[target_col]]
         .rename(columns={target_col: "y"}))

    # Control aggregates (across control tiers) — sum the underlying counts,
    # then compute the rate so it's impression/visit-weighted not equal-weighted.
    c_raw = ci_tier_daily[ci_tier_daily["fangorn_rollout_tier_num"].isin(control_tier_list)]
    sum_cols = list({metric_spec["control_num"], metric_spec["control_den"], metric_spec["control_scale_col"]})
    c = c_raw.groupby("day", as_index=True)[sum_cols].sum()
    c[metric_spec["control_rate_name"]] = np.where(
        c[metric_spec["control_den"]] > 0,
        c[metric_spec["control_num"]] / c[metric_spec["control_den"]],
        np.nan,
    )
    c[metric_spec["control_scale_name"]] = c[metric_spec["control_scale_col"]] / metric_spec["scale_factor"]

    # Build candidate frame — exogenous covariates only (no lags of y).
    df = t.join(c[[metric_spec["control_rate_name"], metric_spec["control_scale_name"]]]).sort_index()
    df[f"{metric_spec['control_rate_name']}_lag1"] = df[metric_spec["control_rate_name"]].shift(1)
    df["holiday"]      = df.index.isin(CI_HOLIDAYS).astype(float)
    df = df.dropna()

    pre_period  = [df.index.min().strftime("%Y-%m-%d"),
                   (cutoff - timedelta(days=1)).strftime("%Y-%m-%d")]
    post_period = [(cutoff + timedelta(days=1)).strftime("%Y-%m-%d"),
                   df.index.max().strftime("%Y-%m-%d")]
    n_pre  = len(df.loc[pre_period[0]:pre_period[1]])
    n_post = len(df.loc[post_period[0]:post_period[1]])

    base = {
        "treated_tier": treated_tier,
        "metric": metric_spec["label"],
        "metric_sub": metric_spec["sub"],
        "control_tiers": ",".join(map(str, control_tier_list)),
        "cutoff": cutoff.strftime("%Y-%m-%d"),
        "pre_start": pre_period[0], "pre_end": pre_period[1], "n_pre": n_pre,
        "post_start": post_period[0], "post_end": post_period[1], "n_post": n_post,
        "n_candidates": len(candidates),
    }
    if n_pre < 30 or n_post < 5:
        return {**base, "skip_reason": f"insufficient days (n_pre={n_pre}, n_post={n_post})"}

    # --- Covariate selection on pre-period ---
    pre_df = df.loc[pre_period[0]:pre_period[1]]
    feats_pre  = pre_df[candidates].fillna(0.0)
    target_pre = pre_df["y"]

    kept_after_vif = drop_high_vif(feats_pre, vif_threshold=10.0)
    selected, best_bic = best_subset_by_bic(target_pre, feats_pre[kept_after_vif], max_size=5)
    if not selected:
        selected = kept_after_vif[:3]   # safety fallback

    # --- UCM fit on pre, forecast post ---
    y_all = df["y"].values.astype(float)
    X_all = df[selected].fillna(0.0).values.astype(float)

    y_pre = y_all[:n_pre]
    X_pre = X_all[:n_pre]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # Local level + stochastic weekly seasonality (period=7). harmonics=2
        # (4 trig states) captures the dominant weekday/weekend swing without
        # overfitting short pre-windows; full day-of-week would be harmonics=3.
        # Weekly cycle handled HERE — that's why is_weekend dummy is no longer
        # a candidate covariate.
        model = UnobservedComponents(
            y_pre,
            level="local level",
            freq_seasonal=[{"period": 7, "harmonics": 2}],
            exog=X_pre,
        )
        res = model.fit(maxiter=50, disp=False)

    X_post = X_all[n_pre:n_pre + n_post]
    predicted_post = np.asarray(
        res.get_forecast(steps=n_post, exog=X_post).predicted_mean, dtype=float)

    # --- Counterfactual uncertainty by simulation ---
    # The effect's uncertainty is ENTIRELY the counterfactual's uncertainty
    # (post-period actuals are observed/fixed). Averaging per-day forecast CI
    # bounds is NOT the SD of the post-period average — it drops the 1/n
    # scaling AND ignores the strong positive cross-day covariance of a
    # local-level forecast. Instead, draw N=2000 sample paths from the
    # forecast distribution (initial-state + state-shock + observation
    # uncertainty, with the correct cross-day covariance), apply each
    # summary to each path, and read percentiles. No hand-derived SE, no
    # ratio-of-bounds blow-up, no normality assumption.
    N_SIM = 2000
    np.random.seed(0)  # reproducible: simulate draws random init state + shocks
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        sim = res.simulate(nsimulations=n_post, anchor="end",
                           repetitions=N_SIM, exog=X_post)
    sim_paths = np.asarray(sim).reshape(n_post, N_SIM)   # (n_post, N_SIM)

    # Per-day percentile envelope across simulated paths — used by the chart band
    lower_bound = np.percentile(sim_paths, 2.5, axis=1)
    upper_bound = np.percentile(sim_paths, 97.5, axis=1)

    # Distribution of the post-period AVERAGE counterfactual (one value per path)
    avg_cf_dist = sim_paths.mean(axis=0)                 # (N_SIM,)

    # --- Pre-period fit diagnostic ---
    # One-step-ahead in-sample predictions vs actual on the pre-period. This
    # is the trust check for the whole method: if the UCM cannot track the
    # treated series BEFORE the switch, its post-period counterfactual is not
    # credible. Skip a short burn-in (diffuse state init inflates early error).
    pre_fitted = np.asarray(res.fittedvalues, dtype=float)
    burn = min(7, max(1, n_pre // 10))
    pf_resid = y_pre[burn:] - pre_fitted[burn:]
    pf_denom = np.where(np.abs(y_pre[burn:]) > 0, np.abs(y_pre[burn:]), np.nan)
    pre_mape = float(np.nanmean(np.abs(pf_resid) / pf_denom))
    ss_res   = float(np.sum(pf_resid ** 2))
    ss_tot   = float(np.sum((y_pre[burn:] - np.mean(y_pre[burn:])) ** 2))
    pre_r2   = float(1 - ss_res / ss_tot) if ss_tot > 0 else float("nan")

    # Stash full per-day series for the actual-vs-counterfactual diagnostic chart
    CI_SERIES[(treated_tier, metric_spec["label"])] = {
        "dates_pre":   df.index[:n_pre],
        "dates_post":  df.index[n_pre:n_pre + n_post],
        "actual_pre":  y_pre,
        "actual_post": y_all[n_pre:n_pre + n_post],
        "fitted_pre":  pre_fitted,
        "pred_post":   np.asarray(predicted_post, dtype=float),
        "lower_post":  lower_bound,
        "upper_post":  upper_bound,
        "cutoff":      cutoff,
    }

    avg_actual    = float(np.mean(y_all[n_pre:n_pre + n_post]))
    avg_predicted = float(np.mean(predicted_post))

    # Effect distributions: actual is fixed, so subtract/divide the simulated
    # counterfactual-average distribution. Point estimate uses the analytic mean.
    abs_dist = avg_actual - avg_cf_dist
    rel_dist = np.where(avg_cf_dist != 0.0, avg_actual / avg_cf_dist - 1.0, np.nan)

    abs_effect = avg_actual - avg_predicted
    rel_effect = avg_actual / avg_predicted - 1.0 if avg_predicted else float("nan")
    abs_ci_lower, abs_ci_upper = (float(x) for x in np.percentile(abs_dist, [2.5, 97.5]))
    rel_ci_lower, rel_ci_upper = (float(x) for x in np.nanpercentile(rel_dist, [2.5, 97.5]))

    # Two-sided tail probability: how often the simulated counterfactual lands
    # on the wrong side of the observed actual (posterior-predictive analogue).
    p_value = float(min(1.0, 2.0 * min(
        float((avg_cf_dist >= avg_actual).mean()),
        float((avg_cf_dist <= avg_actual).mean()))))

    return {
        **base,
        "kept_after_vif":      ",".join(kept_after_vif),
        "selected_covariates": ",".join(selected),
        "n_selected":          len(selected),
        "best_bic":            best_bic,
        "pre_r2":              pre_r2,
        "pre_mape":            pre_mape,
        "avg_actual":          avg_actual,
        "avg_predicted":       avg_predicted,
        "abs_effect":          abs_effect,
        "abs_ci_95_lower":     abs_ci_lower,
        "abs_ci_95_upper":     abs_ci_upper,
        "rel_effect":          rel_effect,
        "rel_ci_95_lower":     rel_ci_lower,
        "rel_ci_95_upper":     rel_ci_upper,
        "p_value":             p_value,
        "n_sim":               N_SIM,
    }

CI_SERIES = {}      # populated inside run_ci_for_tier; used by the diagnostic chart
ci_rows = []
for tier in sorted(treated_tiers):
    if tier not in tier_inclusion_dates:
        continue
    for spec in CI_METRIC_SPECS:
        print(f"[ci] Fitting tier {tier} · metric {spec['label']}...")
        try:
            ci_rows.append(run_ci_for_tier(tier, control_tiers, tier_inclusion_dates[tier], spec))
        except Exception as e:
            print(f"  [err] tier {tier} · {spec['label']}: {e}")
            ci_rows.append({"treated_tier": tier, "metric": spec["label"], "skip_reason": str(e)})

ci_results_df = pd.DataFrame(ci_rows)
display(ci_results_df)


# COMMAND ----------

# Render CausalImpact results as styled tiles — actual vs counterfactual.
# Headline is the ABSOLUTE effect (in percentage points): unambiguous in
# magnitude and direction, doesn't blow up when the counterfactual approaches
# zero. Relative effect is shown as a secondary metric.

def fmt_pct_signed(x, digits=1):
    if x is None or pd.isna(x):
        return "—"
    return f"{x * 100:+.{digits}f}%"

def fmt_pct(x, digits=2):
    if x is None or pd.isna(x):
        return "—"
    return f"{x * 100:.{digits}f}%"

def fmt_pp(x, digits=2):
    if x is None or pd.isna(x):
        return "—"
    return f"{x * 100:+.{digits}f}pp"

def ci_color(row):
    if "abs_effect" not in row or pd.isna(row.get("abs_effect")):
        return "#9ca3af"
    sig = row.get("p_value", 1.0) < 0.10
    if row["abs_effect"] > 0:
        return "#1a7f37" if sig else "#86a886"
    return "#b91c1c" if sig else "#c98080"

def _effect_bar(row, color):
    """Horizontal absolute-effect bar: 95% CrI span + point estimate against
    a zero reference. Auto-scaled per tile, so the visual question is simply
    'does the interval clear the zero line?' (cleared = effect distinguishable
    from no-flip counterfactual)."""
    lo, pt, hi = row.get("abs_ci_95_lower"), row.get("abs_effect"), row.get("abs_ci_95_upper")
    if any(v is None or pd.isna(v) for v in (lo, pt, hi)):
        return ""
    M = (max(abs(lo), abs(hi), abs(pt)) * 1.15) or 1e-9
    to_pct = lambda v: (v + M) / (2 * M) * 100.0
    lo_p, pt_p, hi_p = to_pct(lo), to_pct(pt), to_pct(hi)
    return f"""
      <div style="position:relative; height:24px; margin-top:8px;">
        <div style="position:absolute; top:10px; left:0; right:0; height:4px; background:#eef0f2; border-radius:2px;"></div>
        <div style="position:absolute; top:3px; left:50%; width:1px; height:18px; background:#9ca3af;"></div>
        <div style="position:absolute; top:8px; left:{lo_p:.1f}%; width:{max(hi_p - lo_p, 0.6):.1f}%; height:8px; background:{color}40; border-radius:4px;"></div>
        <div style="position:absolute; top:6px; left:{pt_p:.1f}%; width:10px; height:10px; margin-left:-5px; background:{color}; border-radius:50%;"></div>
      </div>
    """

def _render_tile(row: dict) -> str:
    metric = row.get("metric", "—")
    sub    = row.get("metric_sub", "")
    if "abs_effect" not in row or pd.isna(row.get("abs_effect")):
        return f"""
        <div style="flex:1 1 0; min-width:250px; background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:14px;">
          <div style="font-size:11px; font-weight:600; color:#6b7280; letter-spacing:0.04em; text-transform:uppercase;">
            {metric} <span style="color:#9ca3af; font-weight:500;">({sub})</span>
          </div>
          <div style="margin-top:10px; color:#6b7280; font-size:13px;">
            {row.get("skip_reason", "no result")}
          </div>
        </div>
        """
    color = ci_color(row)
    pre_r2_str = ("%.2f" % row["pre_r2"]) if row.get("pre_r2") == row.get("pre_r2") else "—"
    return f"""
    <div style="flex:1 1 0; min-width:250px; background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:14px;">
      <div style="font-size:11px; font-weight:600; color:#6b7280; letter-spacing:0.04em; text-transform:uppercase;">
        {metric} <span style="color:#9ca3af; font-weight:500;">({sub})</span>
      </div>
      <div style="display:flex; justify-content:space-between; margin-top:10px; font-size:13px;">
        <span style="color:#6b7280;">Actual avg</span><span>{fmt_pct(row["avg_actual"])}</span>
      </div>
      <div style="display:flex; justify-content:space-between; margin-top:3px; font-size:13px;">
        <span style="color:#6b7280;">Counterfactual (no flip)</span><span>{fmt_pct(row["avg_predicted"])}</span>
      </div>
      <div style="margin-top:10px; padding-top:8px; border-top:1px solid #f3f4f6; display:flex; justify-content:space-between; align-items:baseline; font-weight:700; color:{color};">
        <span style="font-size:13px;">Absolute effect</span><span style="font-size:18px;">{fmt_pp(row["abs_effect"])}</span>
      </div>
      {_effect_bar(row, color)}
      <div style="display:flex; justify-content:space-between; font-size:12px; color:#6b7280; margin-top:4px;">
        <span>95% CrI</span><span>[{fmt_pp(row["abs_ci_95_lower"])}, {fmt_pp(row["abs_ci_95_upper"])}]</span>
      </div>
      <div style="display:flex; justify-content:space-between; font-size:12px; color:#6b7280; margin-top:2px;">
        <span>Relative effect</span><span style="color:{color}; font-weight:600;">{fmt_pct_signed(row["rel_effect"])}</span>
      </div>
      <div style="display:flex; justify-content:space-between; font-size:12px; color:#6b7280; margin-top:2px;">
        <span>p-value</span><span>{row["p_value"]:.3f}</span>
      </div>
      <div style="display:flex; justify-content:space-between; font-size:11px; color:#9ca3af; margin-top:6px;">
        <span>{row["n_pre"]}d pre · {row["n_post"]}d post</span><span>pre-fit R² {pre_r2_str}</span>
      </div>
      <div style="font-size:11px; color:#9ca3af; margin-top:2px;">
        covariates: {row.get("selected_covariates", "—")}
      </div>
    </div>
    """

# Group tiles by tier so each tier renders IVR + CVR side-by-side
sections_html = []
for tier in sorted(treated_tiers):
    if tier not in tier_inclusion_dates:
        continue
    tier_rows = [r for r in ci_rows if r.get("treated_tier") == tier]
    if not tier_rows:
        continue
    cutoff = tier_inclusion_dates[tier]
    tile_html = "".join(_render_tile(r) for r in tier_rows)
    sections_html.append(f"""
    <div style="margin-bottom:18px;">
      <div style="font-weight:600; color:#374151; margin-bottom:8px; font-size:13px;">
        Tier {tier} &nbsp;·&nbsp; <span style="color:#6b7280; font-weight:500;">switched {cutoff} · actual vs no-flip counterfactual</span>
      </div>
      <div style="display:flex; gap:12px; flex-wrap:wrap;">{tile_html}</div>
    </div>
    """)

_n_sim = next((r.get("n_sim") for r in ci_rows if r.get("n_sim")), 2000)
ci_html = f"""
<div style="font-family:-apple-system, BlinkMacSystemFont, sans-serif; width:100%;">
  <div style="background:#f6f8fa; padding:12px 16px; border-radius:6px; margin-bottom:12px; font-size:13px; color:#374151;">
    <b>CausalImpact — actual vs counterfactual</b> &nbsp;·&nbsp;
    <b>window:</b> {ci_window_start} → {window_end} &nbsp;·&nbsp;
    <b>metrics:</b> {', '.join(s['label'] for s in CI_METRIC_SPECS)}
    <div style="margin-top:6px; color:#6b7280; font-size:12px; line-height:1.5;">
      Counterfactual = UCM (local level + weekly freq_seasonal) forecast fit on the pre-period; effect = actual − counterfactual.
      95% CrI &amp; p-value from {_n_sim:,} simulated counterfactual paths (p = two-sided tail prob the counterfactual beats the actual).
      Control tiers {', '.join(str(t) for t in control_tiers)} enter only as forecast covariates (VIF→BIC over 4 candidates: control rate, control rate lag1, control scale, holiday).
      {(' · Excluded days: ' + ', '.join(EXCLUDE_DATES)) if EXCLUDE_DATES else ''}
    </div>
  </div>
  {''.join(sections_html)}
</div>
"""
displayHTML(ci_html)

# COMMAND ----------

# Diagnostic plot — one panel per (treated tier × metric).
# Shows the ACTUAL series against the model: in-sample fit during the
# pre-period (the trust check) and the forecast counterfactual + 95% band
# during the post-period (the no-treatment baseline). The post-period gap
# between actual and counterfactual IS the estimated effect.
import matplotlib.gridspec as gridspec

ci_panels = [r for r in ci_rows if "rel_effect" in r]
if ci_panels:
    fig = plt.figure(figsize=(12, 3.2 * len(ci_panels)))
    gs = gridspec.GridSpec(len(ci_panels), 1, hspace=0.7)

    for i, row in enumerate(ci_panels):
        tier   = row["treated_tier"]
        metric = row["metric"]
        s = CI_SERIES.get((tier, metric))
        if s is None:
            continue
        cutoff = pd.to_datetime(s["cutoff"])
        color  = TIER_COLORS.get(tier, "#5dade2")

        ax = fig.add_subplot(gs[i, 0])

        # Actual — continuous across pre + post
        all_dates  = s["dates_pre"].append(s["dates_post"])
        all_actual = np.concatenate([s["actual_pre"], s["actual_post"]])
        ax.plot(all_dates, all_actual, color=color, marker="o", markersize=3,
                label=f"Tier {tier} {metric} actual")

        # Pre-period in-sample fit (how well the UCM tracks before the switch)
        ax.plot(s["dates_pre"], s["fitted_pre"], color="#9aa0a6", linestyle=":",
                linewidth=1.5, label="model fit (pre)")

        # Post-period counterfactual forecast + 95% band
        ax.plot(s["dates_post"], s["pred_post"], color="#d0d0d0", linestyle="--",
                linewidth=1.8, label="counterfactual (no flip)")
        ax.fill_between(s["dates_post"], s["lower_post"], s["upper_post"],
                        color="#d0d0d0", alpha=0.15, linewidth=0)

        ax.axvline(cutoff, color=SWITCH_COLOR, linestyle="--", linewidth=1.4, alpha=0.9)
        ax.text(cutoff, ax.get_ylim()[1], f" switch: {cutoff.date()}",
                color=SWITCH_COLOR, va="top", ha="left", fontsize=9, fontweight="semibold")

        pre_r2 = row.get("pre_r2", float("nan"))
        ax.set_title(
            f"Tier {tier} — {metric}  ·  effect={fmt_pct_signed(row['rel_effect'])} "
            f"(95% CrI [{fmt_pct_signed(row['rel_ci_95_lower'])}, {fmt_pct_signed(row['rel_ci_95_upper'])}], "
            f"p={row['p_value']:.3f})  ·  pre-fit R²={pre_r2:.2f}")
        style_axis(ax, ylabel=metric.lower(), as_percent=True)
        ax.legend(loc="upper left", fontsize=8, ncol=3)

    fig.suptitle(f"CausalImpact — actual vs counterfactual — {ci_window_start} → {window_end}",
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
