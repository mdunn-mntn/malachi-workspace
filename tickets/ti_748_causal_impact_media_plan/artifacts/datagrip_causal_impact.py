# Databricks notebook source
# MAGIC %md
# MAGIC # Causal Impact Analysis: Jaguar Release
# MAGIC
# MAGIC ## Executive Summary
# MAGIC
# MAGIC This notebook measures the **incremental lift (or decline)** of the Jaguar release on advertising KPIs using **Bayesian Structural Time Series** with synthetic control methodology.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Analysis Period
# MAGIC
# MAGIC | Period | Dates | Purpose |
# MAGIC |--------|-------|---------|
# MAGIC | **Pre-period** | `RELEASE_DATE - PERIOD_DAYS` to `RELEASE_DATE - 1` | Train the model to learn baseline patterns |
# MAGIC | **Intervention** | `RELEASE_DATE` | Excluded from analysis |
# MAGIC | **Post-period** | `RELEASE_DATE + 1` to `RELEASE_DATE + PERIOD_DAYS` | Measure actual vs predicted (counterfactual) |
# MAGIC
# MAGIC The period length is automatically calculated as the maximum number of complete 7-day weeks available since release.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What We're Measuring
# MAGIC
# MAGIC **Primary Metric:** IVR (Impression-to-Visit Rate) = `verified_visits / impressions`
# MAGIC
# MAGIC **All Available Metrics:**
# MAGIC | Metric | Formula | Interpretation |
# MAGIC |--------|---------|----------------|
# MAGIC | IVR | `vv / impressions` | Visit engagement per impression |
# MAGIC | CVR | `conversions / vv` | Conversion rate per visit |
# MAGIC | VVR | `vv / uniques` | Visit frequency per unique |
# MAGIC | CPA | `spend / conversions` | Cost efficiency (lower = better) |
# MAGIC | CPV | `spend / vv` | Cost per visit (lower = better) |
# MAGIC | ROAS | `order_value / spend` | Revenue return (higher = better) |
# MAGIC | AOV | `order_value / conversions` | Average order size |
# MAGIC
# MAGIC **Interpretation of Results:**
# MAGIC - **Relative Effect**: % change attributable to the intervention (e.g., -32% means IVR dropped 32% more than expected)
# MAGIC - **P-value**: Statistical significance (p < 0.05 = significant)
# MAGIC - **Confidence Interval**: Range of plausible effect sizes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How Synthetic Control Works
# MAGIC
# MAGIC ### The Core Idea
# MAGIC
# MAGIC We can't observe what *would have happened* without Jaguar (the counterfactual). Instead, we **construct a synthetic counterfactual** using covariates that:
# MAGIC 1. Are predictive of the target metric
# MAGIC 2. Are **not affected** by the intervention (or affected in known ways)
# MAGIC
# MAGIC ### The Process
# MAGIC ```
# MAGIC PRE-PERIOD:
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ Model learns: IVR = f(spend, impressions, uniques)  │
# MAGIC │ "When spend is X and impressions are Y, IVR is Z"   │
# MAGIC └─────────────────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ▼
# MAGIC POST-PERIOD:
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ Model predicts: "Given post-period covariates,      │
# MAGIC │ IVR *should have been* Z' (if nothing changed)"     │
# MAGIC │                                                     │
# MAGIC │ Actual IVR = Z''                                    │
# MAGIC │ Causal Effect = Z'' - Z'                            │
# MAGIC └─────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Current Covariates
# MAGIC
# MAGIC | Covariate | Why It's Used |
# MAGIC |-----------|---------------|
# MAGIC | `spend` | Controls for budget changes; more spend → more impressions → potentially different IVR |
# MAGIC | `impressions` | Direct denominator of IVR; controls for volume shifts |
# MAGIC | `uniques` | Controls for audience size changes |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Covariate Variations to Try
# MAGIC
# MAGIC ### Option 1: Minimal Covariates (Current)
# MAGIC ```python
# MAGIC COVARIATES = ["spend", "impressions", "uniques"]
# MAGIC ```
# MAGIC - **Pros**: Simple, interpretable
# MAGIC - **Cons**: May not capture all confounders
# MAGIC
# MAGIC ### Option 2: Volume-Only
# MAGIC ```python
# MAGIC COVARIATES = ["impressions"]
# MAGIC ```
# MAGIC - **Pros**: Directly controls for the IVR denominator
# MAGIC - **Cons**: Doesn't account for spend/budget shifts
# MAGIC
# MAGIC ### Option 3: Extended Covariates
# MAGIC ```python
# MAGIC COVARIATES = ["spend", "impressions", "uniques", "active_campaigns", "active_advertisers"]
# MAGIC ```
# MAGIC - **Pros**: Controls for advertiser mix changes
# MAGIC - **Cons**: More complexity, potential overfitting with short time series
# MAGIC
# MAGIC ### Option 4: Lagged Metric (if available)
# MAGIC ```python
# MAGIC # Add previous day's IVR as a covariate
# MAGIC daily_kpis_pd["ivr_lag1"] = daily_kpis_pd.groupby("vertical_name")["ivr"].shift(1)
# MAGIC COVARIATES = ["spend", "impressions", "ivr_lag1"]
# MAGIC ```
# MAGIC - **Pros**: Captures autocorrelation/momentum
# MAGIC - **Cons**: Loses first observation
# MAGIC
# MAGIC ### ⚠️ Covariate Selection Rules
# MAGIC
# MAGIC 1. **Don't use metrics affected by the intervention** as covariates for that same metric
# MAGIC    - Bad: Using `vv` as covariate when measuring IVR (vv is the numerator)
# MAGIC    - OK: Using `spend` as covariate for IVR (spend is upstream)
# MAGIC
# MAGIC 2. **Don't use post-treatment outcomes** to predict pre-treatment
# MAGIC
# MAGIC 3. **More data points = more covariates allowed**
# MAGIC    - Rule of thumb: ≥10 pre-period observations per covariate
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Alternative Control Strategies
# MAGIC
# MAGIC ### 1. Non-Jaguar Verticals as Control Group
# MAGIC
# MAGIC Instead of synthetic control, use **verticals without Jaguar** as a comparison group.
# MAGIC ```python
# MAGIC # Treated verticals (have Jaguar)
# MAGIC TREATED_VERTICALS = [113002]
# MAGIC
# MAGIC # Control verticals (no Jaguar - need to identify these)
# MAGIC CONTROL_VERTICALS = [xxx, yyy, zzz]
# MAGIC ```
# MAGIC
# MAGIC **Approach**: Difference-in-Differences (DiD)
# MAGIC - Compare pre/post change in treated vs control
# MAGIC - Effect = (Treated_post - Treated_pre) - (Control_post - Control_pre)
# MAGIC
# MAGIC **Pros**: More robust if good control verticals exist
# MAGIC **Cons**: Requires truly comparable untreated verticals
# MAGIC
# MAGIC ### 2. Staggered Rollout Analysis
# MAGIC
# MAGIC If Jaguar rolled out to different verticals on different dates:
# MAGIC ```python
# MAGIC vertical_release_dates = {
# MAGIC     113002: date(2025, 11, 20),
# MAGIC     119001: date(2025, 11, 25),
# MAGIC     101001: date(2025, 12, 01),
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Approach**: Use not-yet-treated verticals as controls for early-treated ones
# MAGIC
# MAGIC ### 3. Matched Advertiser Pairs
# MAGIC
# MAGIC Within a vertical, compare:
# MAGIC - Advertisers heavily using Jaguar-added IPs
# MAGIC - Advertisers primarily using organic IPs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Other Methods to Test This Effect
# MAGIC
# MAGIC ### 1. Simple Pre/Post Comparison (No Synthetic Control)
# MAGIC ```python
# MAGIC pre_avg = daily_kpis_pd[daily_kpis_pd["day"] < RELEASE_DATE]["ivr"].mean()
# MAGIC post_avg = daily_kpis_pd[daily_kpis_pd["day"] > RELEASE_DATE]["ivr"].mean()
# MAGIC pct_change = (post_avg - pre_avg) / pre_avg
# MAGIC ```
# MAGIC - **Pros**: Simple, transparent
# MAGIC - **Cons**: Doesn't control for anything; confounded by time trends
# MAGIC
# MAGIC ### 2. Regression with Time Trend
# MAGIC ```python
# MAGIC # OLS: IVR ~ post_dummy + time_trend + covariates
# MAGIC import statsmodels.api as sm
# MAGIC
# MAGIC df["post"] = (df["day"] > RELEASE_DATE).astype(int)
# MAGIC df["time_trend"] = (df["day"] - df["day"].min()).dt.days
# MAGIC
# MAGIC X = df[["post", "time_trend", "spend", "impressions"]]
# MAGIC X = sm.add_constant(X)
# MAGIC y = df["ivr"]
# MAGIC
# MAGIC model = sm.OLS(y, X).fit()
# MAGIC print(model.summary())
# MAGIC # Coefficient on "post" = causal effect estimate
# MAGIC ```
# MAGIC
# MAGIC ### 3. Interrupted Time Series (ITS)
# MAGIC - Model pre-period trend
# MAGIC - Test if post-period intercept/slope changed
# MAGIC - Good for: detecting level shifts and trend changes
# MAGIC
# MAGIC ### 4. Permutation/Placebo Tests
# MAGIC Run the same analysis with **fake release dates** in the pre-period:
# MAGIC ```python
# MAGIC placebo_dates = [RELEASE_DATE - timedelta(days=14), RELEASE_DATE - timedelta(days=21)]
# MAGIC ```
# MAGIC If you find "significant" effects at placebo dates, the method may be unreliable.
# MAGIC
# MAGIC ### 5. IP-Level Analysis (from other notebook)
# MAGIC Compare KPIs directly between:
# MAGIC - `is_model_added = True` (Jaguar IPs)
# MAGIC - `is_model_added = False` (Organic IPs)
# MAGIC
# MAGIC This is a **cross-sectional** comparison vs the **time-series** approach here.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Assumptions & Limitations
# MAGIC
# MAGIC ### Key Assumptions
# MAGIC 1. **Parallel trends**: Without intervention, treated units would have followed the same trend as the synthetic control
# MAGIC 2. **No spillover**: Jaguar doesn't affect organic IPs' performance
# MAGIC 3. **Stable covariates**: Covariate relationships with the outcome are stable over time
# MAGIC
# MAGIC ### Limitations
# MAGIC 1. **Short time series**: With only ~4 weeks pre/post, estimates have wide confidence intervals
# MAGIC 2. **Seasonality**: Nov-Dec has Black Friday, holidays — may confound results
# MAGIC 3. **Single vertical**: Results may not generalize; need to test across verticals
# MAGIC 4. **Correlation ≠ Causation**: Even with controls, unmeasured confounders may exist
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Recommended Next Steps
# MAGIC
# MAGIC 1. **Run across all verticals** (`VERTICAL_IDS = None`) to see if effect is consistent
# MAGIC 2. **Try different covariates** to test sensitivity of results
# MAGIC 3. **Run placebo tests** to validate methodology
# MAGIC 4. **Cross-reference with IP-level analysis** — does model_score correlate with IVR?
# MAGIC 5. **Investigate November anomalies** — were there other changes around release date?

# COMMAND ----------

# MAGIC %md
# MAGIC # Causal Impact Analysis: Jaguar Release
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC **Objective:** Measure the incremental lift of Jaguar release on KPIs using synthetic control methodology.
# MAGIC
# MAGIC **Approach:**
# MAGIC 1. Pull daily KPI data from coredw (pre and post release)
# MAGIC 2. Use CausalImpact to model counterfactual (what KPIs would have been without intervention)
# MAGIC 3. Covariates: spend, impressions, uniques help predict expected baseline
# MAGIC 4. Primary metric: IVR (with all other KPIs available)
# MAGIC
# MAGIC **Methodology:** Google's CausalImpact uses Bayesian structural time series to estimate the causal effect of an intervention by constructing a synthetic control from covariates.
# MAGIC
# MAGIC ## Data Sources
# MAGIC
# MAGIC | Source | Tables | Purpose |
# MAGIC |--------|--------|----------|
# MAGIC | coredw (Greenplum) | `summarydata.sum_by_campaign_group_by_day`, `fpa.advertiser_verticals`, etc. | Daily KPIs by vertical |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

# =============================================================================
# INSTALL DEPENDENCIES (run once per cluster)
# =============================================================================

%pip install pycausalimpact --quiet

# COMMAND ----------

# =============================================================================
# IMPORTS
# =============================================================================

import json
from datetime import date, timedelta
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

import google.auth.transport.requests as g_request
from google.auth import compute_engine
import requests

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from causalimpact import CausalImpact

# Initialize Spark session
spark = SparkSession.builder.appName("causal_impact_jaguar").getOrCreate()

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Configuration Parameters

# COMMAND ----------

# =============================================================================
# ANALYSIS PARAMETERS
# =============================================================================

# Release date (intervention date)
RELEASE_DATE = date(2025, 11, 20)

# Vertical filter
VERTICAL_IDS: Optional[List[int]] = [113002]

# Primary metric
PRIMARY_METRIC = "ivr"

# =============================================================================
# PERIOD CONFIGURATION (WEEKLY)
# =============================================================================

# How many weeks of pre-period data to use (longer = better baseline)
PRE_PERIOD_WEEKS = 52  # 1 year

# Post-period: use all complete weeks since release
days_since_release = (date.today() - timedelta(days=1) - RELEASE_DATE).days
POST_PERIOD_WEEKS = days_since_release // 7

# Calculate date boundaries
PRE_START = RELEASE_DATE - timedelta(weeks=PRE_PERIOD_WEEKS)
PRE_END = RELEASE_DATE - timedelta(days=1)
POST_START = RELEASE_DATE + timedelta(days=1)
POST_END = RELEASE_DATE + timedelta(weeks=POST_PERIOD_WEEKS)

print(f"Release Date:       {RELEASE_DATE}")
print(f"Pre-period:         {PRE_START} to {PRE_END} ({PRE_PERIOD_WEEKS} weeks)")
print(f"Post-period:        {POST_START} to {POST_END} ({POST_PERIOD_WEEKS} weeks)")
print(f"Vertical IDs:       {VERTICAL_IDS if VERTICAL_IDS else 'All verticals'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Helper Functions

# COMMAND ----------

# =============================================================================
# VAULT & DATABASE HELPERS
# =============================================================================

def token_for_url(url: str) -> str:
    """
    Get GCP identity token for the specified URL.
    Used for Vault authentication via workload identity.
    """
    request = g_request.Request()
    credentials = compute_engine.IDTokenCredentials(
        request=request,
        target_audience=url,
        use_metadata_identity_endpoint=True,
    )
    credentials.refresh(request)
    return credentials.token


def get_secret(secret_name: str) -> Dict:
    """
    Retrieve secret from Vault using GCP workload identity authentication.
    """
    vault_address = "https://vault.prod.in.mountain.com"
    role = "gcp-workloads"
    path = "shared/global/ti"

    jwt = token_for_url(f"{vault_address}/vault/gcp-workloads")

    auth_resp = requests.post(
        f"{vault_address}/v1/auth/gcp/login",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"role": role, "jwt": jwt}),
    )
    auth_resp.raise_for_status()
    vault_token = auth_resp.json()["auth"]["client_token"]

    secret_resp = requests.get(
        f"{vault_address}/v1/secret/data/{path}/{secret_name}",
        headers={"X-Vault-Token": vault_token},
    )
    secret_resp.raise_for_status()

    return secret_resp.json().get("data", {}).get("data")


def load_postgres_query(query: str, session: SparkSession) -> DataFrame:
    """
    Execute a query against coredw (Greenplum) and return results as Spark DataFrame.
    """
    secrets = get_secret("coredw")

    return (
        session.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{secrets['hostname']}:{secrets['port']}/{secrets['database']}")
        .option("dbtable", query)
        .option("user", secrets["username"])
        .option("password", secrets["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )


print("Helper functions loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Build Daily KPI Query

# COMMAND ----------

# =============================================================================
# BUILD SQL QUERY FOR DAILY KPIs
# =============================================================================

# Build vertical filter clause
if VERTICAL_IDS:
    vertical_filter = f"and av.vertical_id in ({','.join(map(str, VERTICAL_IDS))})"
else:
    vertical_filter = "-- all verticals (no filter)"

DAILY_KPI_QUERY = f"""
(
    /* ------------------------------------------------------------------------
       Step 1: Get advertisers in target vertical(s)
       ------------------------------------------------------------------------ */
    with advertisers_in_vertical as (
        select distinct
            av.advertiser_id
          , av.vertical_id
          , av.vertical_name
        from fpa.advertiser_verticals av
        where 1 = 1
            and av.type = 1
            {vertical_filter}
    )

    /* ------------------------------------------------------------------------
       Step 2: Get Funnel 1 campaign groups
       ------------------------------------------------------------------------ */
    , qualifying_campaigns as (
        select distinct
            a.advertiser_id
          , a.vertical_id
          , a.vertical_name
          , cg.campaign_group_id
        from advertisers_in_vertical a
        inner join public.campaign_groups cg
            on cg.advertiser_id = a.advertiser_id
        inner join campaign_groups_raw cgr
            on cgr.campaign_group_id = cg.campaign_group_id
        where 1 = 1
            and cgr.objective_id = 1
    )

    /* ------------------------------------------------------------------------
       Step 3: Identify last-touch attribution advertisers
       ------------------------------------------------------------------------ */
    , last_touch_advertisers as (
        select distinct advertiser_id
        from r2.advertiser_settings
        where reporting_style = 'last_touch'
    )

    /* ------------------------------------------------------------------------
       Step 4: Aggregate daily KPIs by vertical
       ------------------------------------------------------------------------ */
    select
        qc.vertical_id
      , qc.vertical_name
      , d.day

        -- Activity counts
      , count(distinct d.campaign_group_id) as active_campaigns
      , count(distinct qc.advertiser_id) as active_advertisers

        -- Volume metrics
      , sum(d.impressions) as impressions
      , sum(d.media_spend + d.data_spend + d.platform_spend) as spend

        -- Conversions (attribution-aware)
      , sum(
            case
                when lt.advertiser_id is null
                    then d.click_conversions + d.view_conversions + coalesce(d.competing_view_conversions, 0)
                else d.click_conversions + d.view_conversions
            end
        ) as conversions

        -- Order value (attribution-aware)
      , sum(
            case
                when lt.advertiser_id is null
                    then d.click_order_value + d.view_order_value + coalesce(d.competing_view_order_value, 0)
                else d.click_order_value + d.view_order_value
            end
        ) as order_value

        -- Verified visits (attribution-aware)
      , sum(
            case
                when lt.advertiser_id is null
                    then d.clicks + d.views + coalesce(d.competing_views, 0)
                else d.clicks + d.views
            end
        ) as vv

        -- Uniques
      , sum(d.uniques) as uniques

    from qualifying_campaigns qc
    inner join summarydata.sum_by_campaign_group_by_day d
        on d.campaign_group_id = qc.campaign_group_id
    left join last_touch_advertisers lt
        on lt.advertiser_id = qc.advertiser_id
    where 1 = 1
        and d.day >= '{PRE_START}'::date
        and d.day <= '{POST_END}'::date
        and d.day <> '{RELEASE_DATE}'::date  -- exclude release date
        and d.impressions > 0
    group by 1, 2, 3
    order by 1, 3
) as daily_kpis
"""

print("Query built successfully.")
print(f"\nVertical filter: {vertical_filter}")
print(f"Date range: {PRE_START} to {POST_END} (excluding {RELEASE_DATE})")

# COMMAND ----------

WEEKLY_KPI_QUERY = f"""
(
    with advertisers_in_vertical as (
        select distinct
            av.advertiser_id
          , av.vertical_id
          , av.vertical_name
        from fpa.advertiser_verticals av
        where 1 = 1
            and av.type = 1
            {vertical_filter}
    )

    , qualifying_campaigns as (
        select distinct
            a.advertiser_id
          , a.vertical_id
          , a.vertical_name
          , cg.campaign_group_id
        from advertisers_in_vertical a
        inner join public.campaign_groups cg
            on cg.advertiser_id = a.advertiser_id
        inner join campaign_groups_raw cgr
            on cgr.campaign_group_id = cg.campaign_group_id
        where 1 = 1
            and cgr.objective_id = 1
    )

    , last_touch_advertisers as (
        select distinct advertiser_id
        from r2.advertiser_settings
        where reporting_style = 'last_touch'
    )

    /* Aggregate to WEEKLY level */
    select
        qc.vertical_id
      , qc.vertical_name
      , date_trunc('week', d.day)::date as week_start

        -- Activity counts
      , count(distinct d.campaign_group_id) as active_campaigns
      , count(distinct qc.advertiser_id) as active_advertisers

        -- Volume metrics
      , sum(d.impressions) as impressions
      , sum(d.media_spend + d.data_spend + d.platform_spend) as spend

        -- Conversions (attribution-aware)
      , sum(
            case
                when lt.advertiser_id is null
                    then d.click_conversions + d.view_conversions + coalesce(d.competing_view_conversions, 0)
                else d.click_conversions + d.view_conversions
            end
        ) as conversions

        -- Order value (attribution-aware)
      , sum(
            case
                when lt.advertiser_id is null
                    then d.click_order_value + d.view_order_value + coalesce(d.competing_view_order_value, 0)
                else d.click_order_value + d.view_order_value
            end
        ) as order_value

        -- Verified visits (attribution-aware)
      , sum(
            case
                when lt.advertiser_id is null
                    then d.clicks + d.views + coalesce(d.competing_views, 0)
                else d.clicks + d.views
            end
        ) as vv

        -- Uniques
      , sum(d.uniques) as uniques

    from qualifying_campaigns qc
    inner join summarydata.sum_by_campaign_group_by_day d
        on d.campaign_group_id = qc.campaign_group_id
    left join last_touch_advertisers lt
        on lt.advertiser_id = qc.advertiser_id
    where 1 = 1
        and d.day >= '{PRE_START}'::date
        and d.day <= '{POST_END}'::date
        and d.impressions > 0
    group by 1, 2, 3
    order by 1, 3
) as weekly_kpis
"""

print("Query built successfully.")
print(f"\\nVertical filter: {vertical_filter}")
print(f"Date range: {PRE_START} to {POST_END}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Load Data from coredw

# COMMAND ----------

# =============================================================================
# LOAD WEEKLY KPI DATA
# =============================================================================

weekly_kpis_df = load_postgres_query(WEEKLY_KPI_QUERY, spark)

# Convert to pandas
weekly_kpis_pd = weekly_kpis_df.toPandas()

# Convert week_start to datetime
weekly_kpis_pd["week_start"] = pd.to_datetime(weekly_kpis_pd["week_start"])

# Convert Decimal columns to float
numeric_cols = ["impressions", "spend", "conversions", "order_value", "vv", "uniques"]
for c in numeric_cols:
    weekly_kpis_pd[c] = weekly_kpis_pd[c].astype(float)

print(f"Loaded {len(weekly_kpis_pd):,} weekly records")
print(f"Verticals: {weekly_kpis_pd['vertical_name'].nunique()}")
print(f"Date range: {weekly_kpis_pd['week_start'].min().date()} to {weekly_kpis_pd['week_start'].max().date()}")

weekly_kpis_pd.head()

# COMMAND ----------

# =============================================================================
# CALCULATE EFFICIENCY METRICS
# =============================================================================

weekly_kpis_pd["ivr"] = weekly_kpis_pd["vv"] / weekly_kpis_pd["impressions"].replace(0, np.nan)
weekly_kpis_pd["cvr"] = weekly_kpis_pd["conversions"] / weekly_kpis_pd["vv"].replace(0, np.nan)
weekly_kpis_pd["vvr"] = weekly_kpis_pd["vv"] / weekly_kpis_pd["uniques"].replace(0, np.nan)
weekly_kpis_pd["cpa"] = weekly_kpis_pd["spend"] / weekly_kpis_pd["conversions"].replace(0, np.nan)
weekly_kpis_pd["cpv"] = weekly_kpis_pd["spend"] / weekly_kpis_pd["vv"].replace(0, np.nan)
weekly_kpis_pd["roas"] = weekly_kpis_pd["order_value"] / weekly_kpis_pd["spend"].replace(0, np.nan)
weekly_kpis_pd["aov"] = weekly_kpis_pd["order_value"] / weekly_kpis_pd["conversions"].replace(0, np.nan)

print("Efficiency metrics calculated.")

# COMMAND ----------

# =============================================================================
# VALIDATION: CHECK DATA COMPLETENESS
# =============================================================================

release_date_dt = pd.Timestamp(RELEASE_DATE)

validation = weekly_kpis_pd.groupby("vertical_name").agg(
    pre_weeks=pd.NamedAgg(column="week_start", aggfunc=lambda x: (x < release_date_dt).sum()),
    post_weeks=pd.NamedAgg(column="week_start", aggfunc=lambda x: (x >= release_date_dt).sum()),
    total_impressions=pd.NamedAgg(column="impressions", aggfunc="sum"),
    avg_ivr=pd.NamedAgg(column="ivr", aggfunc="mean"),
).reset_index()

print("Data completeness by vertical:")
display(validation)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Causal Impact Analysis Functions

# COMMAND ----------

def run_causal_impact(
    data: pd.DataFrame,
    metric: str,
    covariates: List[str],
    pre_period: list,
    post_period: list,
    vertical_name: str = "All"
) -> dict:
    """
    Run CausalImpact analysis on a single metric.
    """
    # Prepare data: metric first, then covariates
    columns = [metric] + covariates
    ci_data = data[columns].copy()
    
    # Drop rows with NaN in the target metric
    ci_data = ci_data.dropna(subset=[metric])
    
    # Fill NaN in covariates with forward fill then backward fill
    ci_data[covariates] = ci_data[covariates].ffill().bfill()
    
    # Ensure all data is float
    ci_data = ci_data.astype(float)
    
    if len(ci_data) < 10:
        print(f"  ⚠️  Insufficient data for {vertical_name} ({len(ci_data)} rows)")
        return None
    
    try:
        # Run CausalImpact
        ci = CausalImpact(ci_data, pre_period, post_period)
        
        # Extract results from inferences dataframe
        inferences = ci.inferences
        post_mask = inferences.index > pre_period[1]
        post_inferences = inferences[post_mask]
        
        # Get actual values from original data (first column is the target)
        actual_post = ci.post_data.iloc[:, 0]
        
        # Calculate summary statistics
        actual_avg = actual_post.mean()
        predicted_avg = post_inferences["preds"].mean()
        abs_effect = post_inferences["point_effects"].mean()
        rel_effect = abs_effect / predicted_avg if predicted_avg != 0 else np.nan
        
        # Confidence intervals
        ci_lower = post_inferences["point_effects_lower"].mean()
        ci_upper = post_inferences["point_effects_upper"].mean()
        
        # P-value
        p_value = ci.p_value
        
        result = {
            "vertical": vertical_name,
            "metric": metric,
            "pre_period": f"{pre_period[0].date()} to {pre_period[1].date()}",
            "post_period": f"{post_period[0].date()} to {post_period[1].date()}",
            "actual_avg": actual_avg,
            "predicted_avg": predicted_avg,
            "absolute_effect": abs_effect,
            "relative_effect": rel_effect,
            "ci_lower": ci_lower,
            "ci_upper": ci_upper,
            "p_value": p_value,
            "significant": p_value < 0.05,
            "ci_object": ci
        }
        
        return result
        
    except Exception as e:
        print(f"  ❌ Error for {vertical_name}: {str(e)}")
        print(f"      Debug info: {type(e).__name__}")
        return None

# COMMAND ----------

def summarize_results(results: List[dict]) -> pd.DataFrame:
    """
    Create summary DataFrame from list of results.
    """
    # Filter out None results and remove ci_object for display
    clean_results = []
    for r in results:
        if r is not None:
            r_copy = {k: v for k, v in r.items() if k != "ci_object"}
            clean_results.append(r_copy)
    
    if not clean_results:
        return pd.DataFrame()
    
    df = pd.DataFrame(clean_results)
    
    # Format for display
    df["relative_effect_pct"] = df["relative_effect"].apply(lambda x: f"{x:.2%}")
    df["p_value_fmt"] = df["p_value"].apply(lambda x: f"{x:.4f}")
    df["significance"] = df["significant"].apply(lambda x: "✓ Significant" if x else "Not significant")
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Run Causal Impact Analysis

# COMMAND ----------

# =============================================================================
# CONFIGURATION FOR ANALYSIS
# =============================================================================

release_date_dt = pd.Timestamp(RELEASE_DATE)

# Get actual week_start dates from the data
all_weeks = weekly_kpis_pd["week_start"].sort_values().unique()

# Split into pre and post weeks based on release date
pre_weeks = [w for w in all_weeks if w < release_date_dt]
post_weeks = [w for w in all_weeks if w >= release_date_dt]

print(f"Pre-period weeks: {len(pre_weeks)}")
print(f"Post-period weeks: {len(post_weeks)}")

if len(pre_weeks) < 2 or len(post_weeks) < 2:
    raise ValueError("Not enough weeks in pre or post period")

# Use actual data boundaries
pre_period = [pd.Timestamp(pre_weeks[0]), pd.Timestamp(pre_weeks[-1])]
post_period = [pd.Timestamp(post_weeks[0]), pd.Timestamp(post_weeks[-1])]

COVARIATES = ["spend", "impressions", "uniques"]
METRICS_TO_ANALYZE = ["ivr"]

print(f"Pre-period:  {pre_period[0].date()} to {pre_period[1].date()}")
print(f"Post-period: {post_period[0].date()} to {post_period[1].date()}")
print(f"Covariates:  {COVARIATES}")

# COMMAND ----------

# =============================================================================
# RUN ANALYSIS PER VERTICAL (WEEKLY)
# =============================================================================

all_results = []

verticals = weekly_kpis_pd["vertical_name"].unique()
print(f"Analyzing {len(verticals)} vertical(s)...\n")

for vertical in verticals:
    print(f"📊 Processing: {vertical}")
    
    # Filter data for this vertical
    vertical_data = weekly_kpis_pd[weekly_kpis_pd["vertical_name"] == vertical].copy()
    vertical_data = vertical_data.set_index("week_start").sort_index()
    
    print(f"   → {len(vertical_data)} weeks of data")
    
    for metric in METRICS_TO_ANALYZE:
        print(f"   → Metric: {metric.upper()}")
        
        result = run_causal_impact(
            data=vertical_data,
            metric=metric,
            covariates=COVARIATES,
            pre_period=pre_period,
            post_period=post_period,
            vertical_name=vertical
        )
        
        if result:
            all_results.append(result)
            print(f"      Effect: {result['relative_effect']:.2%} (p={result['p_value']:.4f})")
    
    print()

print(f"\n✅ Completed {len(all_results)} analyses.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Results Summary

# COMMAND ----------

# =============================================================================
# SUMMARY TABLE
# =============================================================================

summary_df = summarize_results(all_results)

if not summary_df.empty:
    display_cols = [
        "vertical", 
        "metric", 
        "actual_avg", 
        "predicted_avg", 
        "relative_effect_pct", 
        "p_value_fmt", 
        "significance"
    ]
    
    print("\n" + "="*80)
    print("CAUSAL IMPACT SUMMARY")
    print("="*80)
    display(summary_df[display_cols])
else:
    print("No results to display.")

# COMMAND ----------

# =============================================================================
# INTERPRETATION
# =============================================================================

if all_results:
    for result in all_results:
        if result:
            print(f"\n{'='*80}")
            print(f"INTERPRETATION: {result['vertical']} - {result['metric'].upper()}")
            print(f"{'='*80}")
            print(result["ci_object"].summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Visualizations

# COMMAND ----------

# =============================================================================
# CAUSAL IMPACT PLOTS
# =============================================================================

def plot_causal_impact(result: dict, figsize=(14, 10)):
    """
    Plot CausalImpact results.
    """
    if result is None or "ci_object" not in result:
        print("No results to plot.")
        return
    
    ci = result["ci_object"]
    
    # Print title before plot
    print(f"\n{'='*80}")
    print(f"Causal Impact: {result['metric'].upper()} - {result['vertical']}")
    print(f"Relative Effect: {result['relative_effect']:.2%} (p={result['p_value']:.4f})")
    print(f"{'='*80}\n")
    
    # ci.plot() displays directly, doesn't return a figure
    ci.plot(figsize=figsize)
    plt.show()

for result in all_results:
    if result:
        plot_causal_impact(result)

# COMMAND ----------

# =============================================================================
# RAW TIME SERIES PLOT (PRE/POST COMPARISON)
# =============================================================================

fig, ax = plt.subplots(figsize=(14, 6))

release_date_dt = pd.Timestamp(RELEASE_DATE)

for vertical in weekly_kpis_pd["vertical_name"].unique():
    v_data = weekly_kpis_pd[weekly_kpis_pd["vertical_name"] == vertical].sort_values("week_start")
    ax.plot(v_data["week_start"], v_data[PRIMARY_METRIC], marker="o", markersize=4, label=vertical)

# Add vertical line at release date
ax.axvline(x=release_date_dt, color="red", linestyle="--", linewidth=2, label="Release Date")

# Shade pre/post regions
ax.axvspan(pre_period[0], release_date_dt, alpha=0.1, color="blue", label="Pre-period")
ax.axvspan(release_date_dt, post_period[1], alpha=0.1, color="green", label="Post-period")

ax.set_xlabel("Week", fontsize=12)
ax.set_ylabel(PRIMARY_METRIC.upper(), fontsize=12)
ax.set_title(f"Weekly {PRIMARY_METRIC.upper()} - Pre/Post Comparison", fontsize=14)
ax.legend(loc="upper left", bbox_to_anchor=(1, 1))
ax.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Export Results (Optional)

# COMMAND ----------

# =============================================================================
# EXPORT TO CSV (uncomment to use)
# =============================================================================

# # Export summary
# if not summary_df.empty:
#     export_df = summary_df.drop(columns=["ci_object"], errors="ignore")
#     export_df.to_csv("/dbfs/tmp/causal_impact_summary.csv", index=False)
#     print("Summary exported to /dbfs/tmp/causal_impact_summary.csv")

# # Export daily data
# daily_kpis_pd.to_csv("/dbfs/tmp/causal_impact_daily_data.csv", index=False)
# print("Daily data exported to /dbfs/tmp/causal_impact_daily_data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Run Additional Metrics (Optional)

# COMMAND ----------

# =============================================================================
# ANALYZE ALL KPIS
# =============================================================================

ALL_METRICS = ["ivr", "cvr", "vvr", "cpa", "cpv", "roas", "aov"]

all_kpi_results = []

for vertical in weekly_kpis_pd["vertical_name"].unique():
    print(f"\n📊 Processing: {vertical}")
    vertical_data = weekly_kpis_pd[weekly_kpis_pd["vertical_name"] == vertical].copy()
    vertical_data = vertical_data.set_index("week_start").sort_index()
    
    for metric in ALL_METRICS:
        result = run_causal_impact(
            data=vertical_data,
            metric=metric,
            covariates=COVARIATES,
            pre_period=pre_period,
            post_period=post_period,
            vertical_name=vertical
        )
        if result:
            all_kpi_results.append(result)

full_summary = summarize_results(all_kpi_results)
display(full_summary[["vertical", "metric", "relative_effect_pct", "p_value_fmt", "significance"]])

# COMMAND ----------

# =============================================================================
# PLOT ALL KPI RESULTS
# =============================================================================

for result in all_kpi_results:
    if result:
        plot_causal_impact(result)