"""
TI-748: Causal Impact Analysis — Media Plan Feature (v2)
=========================================================

Measures the incremental impact of the Media Plan feature on advertiser
prospecting KPIs using Bayesian Structural Time Series (CausalImpact).

v2 changes:
  - Data source: sum_by_campaign_by_day (back to 2024-01-01, vs Sep 2025)
  - 52-week pre-period (full seasonality cycle)
  - Recommended-only filter via media_plan_publishers.badge_state
  - Within-advertiser comparison (recommended vs non-recommended campaigns)
  - Improved covariates: holidays, lagged metric, VCR, platform active advertisers
  - Outlier cleanup: winsorize, min-spend threshold, gap detection

Usage:
  python3 ti_748_causal_impact.py [--metric ivr] [--min-pre-weeks 20] [--min-post-weeks 4]
  python3 ti_748_causal_impact.py --all-metrics --save-plots --output-csv --placebo

Requirements:
  pip install google-cloud-bigquery pycausalimpact pandas numpy matplotlib db-dtypes
"""

import argparse
import logging
import warnings
from datetime import date, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from causalimpact import CausalImpact
from google.cloud import bigquery

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*frequency information.*")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

BQ_PROJECT = "dw-main-silver"

# Data quality thresholds
MIN_WEEKLY_IMPRESSIONS = 1000
MIN_TOTAL_POST_SPEND = 10_000  # exclude advertisers with <$10K post-period spend
WINSORIZE_PCTILE = (1, 99)     # winsorize rate metrics at 1st/99th percentile
RAMP_UP_WEEKS = 4              # exclude first 4 weeks post-intervention (TI-780: campaigns reach
                               # 89% of steady-state IVR by week 4, confirmed across 6,917 campaigns)

# Pre-period: 52 weeks captures full seasonality. CausalImpact best practice is
# ≥3x post-period length (Google's paper) and ≥1 full seasonal cycle. 52 weeks
# satisfies both. Data available from 2024-01-01.
DEFAULT_PRE_WEEKS = 52

# Holiday weeks (Monday dates) — major confounders for CTV advertising
HOLIDAY_WEEKS = {
    pd.Timestamp("2024-11-25"): "thanksgiving_blackfriday",
    pd.Timestamp("2024-12-23"): "christmas",
    pd.Timestamp("2024-12-30"): "newyear",
    pd.Timestamp("2025-11-24"): "thanksgiving_blackfriday",
    pd.Timestamp("2025-12-22"): "christmas",
    pd.Timestamp("2025-12-29"): "newyear",
    pd.Timestamp("2026-02-02"): "superbowl",
}

METRIC_DEFINITIONS = {
    "ivr":  {"formula": "vv / impressions",       "direction": "higher", "label": "Impression-to-Visit Rate"},
    "cvr":  {"formula": "conversions / vv",        "direction": "higher", "label": "Conversion Rate"},
    "cpa":  {"formula": "spend / conversions",     "direction": "lower",  "label": "Cost per Acquisition"},
    "cpv":  {"formula": "spend / vv",              "direction": "lower",  "label": "Cost per Visit"},
    "roas": {"formula": "order_value / spend",     "direction": "higher", "label": "Return on Ad Spend"},
}

# Covariates for CausalImpact model
# - Platform metrics control for market-wide trends/seasonality
# - Holiday flag controls for known seasonal spikes
# - Lagged metric captures advertiser-specific autocorrelation
# - VCR (video completion rate) is a CTV engagement proxy unaffected by network allocation
COVARIATES = ["platform_ivr", "platform_spend", "platform_impressions",
              "holiday", "platform_active_advertisers", "platform_vcr"]

OUTPUT_DIR = Path(__file__).parent.parent / "outputs"


# =============================================================================
# DATA LOADING
# =============================================================================

def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT)


def load_adopters(client: bigquery.Client) -> pd.DataFrame:
    """
    Identify advertisers with RECOMMENDED media plans (badge_state = 'RECOMMENDED').
    An advertiser is included if they have at least one all-recommended plan.
    Returns intervention date = first recommended plan create_time.
    """
    query = """
    WITH plan_recommendation_status AS (
        SELECT
            mp.media_plan_id,
            mp.advertiser_id,
            mp.campaign_group_id,
            mp.create_time,
            LOGICAL_AND(mpp.badge_state = 'RECOMMENDED') AS all_recommended
        FROM `dw-main-silver.core.media_plan` mp
        JOIN `dw-main-silver.core.media_plan_publishers` mpp
            ON mpp.media_plan_id = mp.media_plan_id
        WHERE mp.media_plan_status_id = 3
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        prs.advertiser_id,
        adv.company_name,
        MIN(CASE WHEN prs.all_recommended THEN prs.create_time END) AS first_recommended_plan,
        COUNT(*) AS total_plans,
        COUNTIF(prs.all_recommended) AS recommended_plans,
        COUNTIF(NOT prs.all_recommended) AS customized_plans,
        ARRAY_AGG(DISTINCT CASE WHEN prs.all_recommended THEN prs.campaign_group_id END IGNORE NULLS) AS recommended_cg_ids
    FROM plan_recommendation_status prs
    JOIN `dw-main-bronze.integrationprod.advertisers` adv
        ON adv.advertiser_id = prs.advertiser_id
    GROUP BY 1, 2
    HAVING COUNTIF(prs.all_recommended) > 0  -- must have at least one recommended plan
    ORDER BY 3
    """
    df = client.query(query).to_dataframe()
    df["first_recommended_plan"] = pd.to_datetime(df["first_recommended_plan"])
    df["intervention_date"] = df["first_recommended_plan"].dt.date
    log.info(f"Found {len(df)} advertisers with recommended media plans")
    log.info(f"  {df['recommended_plans'].sum()} recommended plans, {df['customized_plans'].sum()} customized plans")
    return df


def load_recommended_campaign_groups(client: bigquery.Client) -> set:
    """Get the set of campaign_group_ids that have all-recommended media plans."""
    query = """
    SELECT mp.campaign_group_id
    FROM `dw-main-silver.core.media_plan` mp
    JOIN `dw-main-silver.core.media_plan_publishers` mpp
        ON mpp.media_plan_id = mp.media_plan_id
    WHERE mp.media_plan_status_id = 3
    GROUP BY 1
    HAVING LOGICAL_AND(mpp.badge_state = 'RECOMMENDED')
    """
    df = client.query(query).to_dataframe()
    return set(df["campaign_group_id"].tolist())


def load_weekly_kpis(client: bigquery.Client, adopter_ids: list) -> pd.DataFrame:
    """
    Load weekly prospecting KPIs from sum_by_campaign_by_day (back to 2024-01-01).
    Includes all advertisers for platform covariate calculation.
    All adopters confirmed as industry_standard attribution.
    """
    adopter_list = ",".join(str(x) for x in adopter_ids)

    query = f"""
    WITH prospecting_campaigns AS (
        SELECT DISTINCT
            c.campaign_id,
            c.advertiser_id,
            c.campaign_group_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.funnel_level = 1
          AND c.deleted = FALSE
          AND c.is_test = FALSE
    )

    SELECT
        pc.advertiser_id,
        pc.campaign_group_id,
        DATE_TRUNC(s.day, WEEK(MONDAY)) AS week_start,
        CASE WHEN pc.advertiser_id IN ({adopter_list}) THEN TRUE ELSE FALSE END AS is_adopter,

        -- volume
        SUM(s.impressions) AS impressions,
        SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend,

        -- verified visits (industry_standard: include competing)
        SUM(s.clicks + s.views + COALESCE(s.competing_views, 0)) AS vv,

        -- conversions (industry_standard: include competing)
        SUM(s.click_conversions + s.view_conversions
            + COALESCE(s.competing_view_conversions, 0)) AS conversions,

        -- order value
        SUM(s.click_order_value + s.view_order_value
            + COALESCE(s.competing_view_order_value, 0)) AS order_value,

        -- video engagement (for VCR covariate)
        SUM(s.vast_start) AS vast_start,
        SUM(s.vast_complete) AS vast_complete

    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    INNER JOIN prospecting_campaigns pc
        ON pc.campaign_id = s.campaign_id
    WHERE s.day >= '2024-01-01'
      AND s.impressions > 0
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 3
    """
    log.info("Loading weekly KPIs from sum_by_campaign_by_day (2024-01-01 to present)...")
    df = client.query(query).to_dataframe()
    df["week_start"] = pd.to_datetime(df["week_start"])

    numeric_cols = ["impressions", "spend", "vv", "conversions", "order_value", "vast_start", "vast_complete"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)

    # filter low-impression weeks
    before = len(df)
    df = df[df["impressions"] >= MIN_WEEKLY_IMPRESSIONS].copy()
    log.info(f"Filtered {before - len(df)} low-impression weeks (<{MIN_WEEKLY_IMPRESSIONS})")

    log.info(
        f"Loaded {len(df):,} campaign-group-week records: "
        f"{df['advertiser_id'].nunique()} advertisers, "
        f"{df['week_start'].min().date()} to {df['week_start'].max().date()}"
    )
    return df


# =============================================================================
# DATA PREPARATION
# =============================================================================

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived rate metrics."""
    df = df.copy()
    df["ivr"] = df["vv"] / df["impressions"].replace(0, np.nan)
    df["cvr"] = df["conversions"] / df["vv"].replace(0, np.nan)
    df["cpa"] = df["spend"] / df["conversions"].replace(0, np.nan)
    df["cpv"] = df["spend"] / df["vv"].replace(0, np.nan)
    df["roas"] = df["order_value"] / df["spend"].replace(0, np.nan)
    df["vcr"] = df["vast_complete"] / df["vast_start"].replace(0, np.nan)
    return df


def winsorize_series(s: pd.Series, lower=1, upper=99) -> pd.Series:
    """Clip values at the given percentiles to reduce outlier influence."""
    lo = np.nanpercentile(s.dropna(), lower)
    hi = np.nanpercentile(s.dropna(), upper)
    return s.clip(lower=lo, upper=hi)


def build_platform_covariates(weekly_kpis: pd.DataFrame) -> pd.DataFrame:
    """
    Build weekly platform-wide covariates from non-adopter data.
    These are NOT affected by the media plan intervention.
    """
    # aggregate to advertiser-week first, then to platform-week
    adv_week = weekly_kpis[~weekly_kpis["is_adopter"]].groupby(
        ["advertiser_id", "week_start"]
    ).agg(
        impressions=("impressions", "sum"),
        spend=("spend", "sum"),
        vv=("vv", "sum"),
        vast_start=("vast_start", "sum"),
        vast_complete=("vast_complete", "sum"),
    ).reset_index()

    platform = adv_week.groupby("week_start").agg(
        platform_impressions=("impressions", "sum"),
        platform_spend=("spend", "sum"),
        platform_vv=("vv", "sum"),
        platform_vast_start=("vast_start", "sum"),
        platform_vast_complete=("vast_complete", "sum"),
        platform_active_advertisers=("advertiser_id", "nunique"),
    ).reset_index()

    platform["platform_ivr"] = platform["platform_vv"] / platform["platform_impressions"].replace(0, np.nan)
    platform["platform_vcr"] = platform["platform_vast_complete"] / platform["platform_vast_start"].replace(0, np.nan)

    # holiday indicator
    platform["holiday"] = platform["week_start"].map(
        lambda w: 1.0 if w in HOLIDAY_WEEKS else 0.0
    )

    # scale large values for numerical stability
    platform["platform_spend"] = platform["platform_spend"] / 1e6
    platform["platform_impressions"] = platform["platform_impressions"] / 1e9
    platform["platform_active_advertisers"] = platform["platform_active_advertisers"] / 1000.0

    log.info(f"Platform covariates: {len(platform)} weeks, "
             f"{adv_week['advertiser_id'].nunique()} non-adopter advertisers")
    return platform


def aggregate_to_advertiser_week(
    weekly_kpis: pd.DataFrame,
    campaign_group_filter: set = None,
) -> pd.DataFrame:
    """
    Aggregate campaign-group-week data to advertiser-week.
    Optionally filter to only specific campaign_group_ids.
    """
    df = weekly_kpis.copy()
    if campaign_group_filter is not None:
        df = df[df["campaign_group_id"].isin(campaign_group_filter)]

    agg = df.groupby(["advertiser_id", "week_start", "is_adopter"]).agg(
        impressions=("impressions", "sum"),
        spend=("spend", "sum"),
        vv=("vv", "sum"),
        conversions=("conversions", "sum"),
        order_value=("order_value", "sum"),
        vast_start=("vast_start", "sum"),
        vast_complete=("vast_complete", "sum"),
    ).reset_index()
    return agg


def prepare_advertiser_data(
    advertiser_id: int,
    intervention_date: date,
    pre_data: pd.DataFrame,
    post_data: pd.DataFrame,
    platform_covariates: pd.DataFrame,
    metric: str,
    min_pre_weeks: int,
    min_post_weeks: int,
) -> tuple:
    """
    Prepare CausalImpact input for a single advertiser.
    pre_data = all prospecting campaigns (historical baseline)
    post_data = recommended-only campaign groups (post-intervention)
    """
    # combine pre and post
    combined = pd.concat([pre_data, post_data], ignore_index=True)
    combined = compute_metrics(combined)

    # winsorize rate metrics
    for m in ["ivr", "cvr", "cpa", "cpv", "roas"]:
        if m in combined.columns:
            combined[m] = winsorize_series(combined[m], *WINSORIZE_PCTILE)

    # merge platform covariates
    combined = combined.merge(platform_covariates, on="week_start", how="inner")

    # add lagged metric (advertiser-specific autocorrelation)
    combined = combined.sort_values("week_start")
    combined["metric_lag1"] = combined[metric].shift(1)
    combined = combined.dropna(subset=["metric_lag1"])  # lose first row

    combined = combined.set_index("week_start").sort_index()

    # determine periods (with ramp-up exclusion)
    intervention_ts = pd.Timestamp(intervention_date)
    intervention_week = intervention_ts - pd.Timedelta(days=intervention_ts.weekday())
    post_start = intervention_week + pd.Timedelta(weeks=RAMP_UP_WEEKS)

    # pre-period ends at intervention; post-period starts AFTER ramp-up window
    # the ramp-up weeks (intervention to intervention+4wk) are excluded from both periods
    pre = combined[combined.index < intervention_week]
    post = combined[combined.index >= post_start]

    if len(post) == 0:
        log.warning(f"  {advertiser_id}: no post-period data after {RAMP_UP_WEEKS}-week ramp-up exclusion, skipping")
        return None, None, None

    if len(pre) < min_pre_weeks:
        log.warning(f"  {advertiser_id}: only {len(pre)} pre-weeks (need {min_pre_weeks}), skipping")
        return None, None, None
    if len(post) < min_post_weeks:
        log.warning(f"  {advertiser_id}: only {len(post)} post-weeks (need {min_post_weeks}), skipping")
        return None, None, None

    # check min spend threshold
    post_spend = post["spend"].sum()
    if post_spend < MIN_TOTAL_POST_SPEND:
        log.warning(f"  {advertiser_id}: post-period spend ${post_spend:,.0f} < ${MIN_TOTAL_POST_SPEND:,} threshold, skipping")
        return None, None, None

    # check for gaps (missing weeks)
    expected_weeks = pd.date_range(combined.index.min(), combined.index.max(), freq="W-MON")
    missing = len(expected_weeks) - len(combined)
    if missing > len(combined) * 0.2:  # >20% missing
        log.warning(f"  {advertiser_id}: {missing} missing weeks (>20%), skipping")
        return None, None, None

    pre_period = [pre.index[0], pre.index[-1]]
    post_period = [post.index[0], post.index[-1]]

    return combined, pre_period, post_period


# =============================================================================
# CAUSAL IMPACT ANALYSIS
# =============================================================================

def run_single_analysis(
    adv_data: pd.DataFrame,
    metric: str,
    covariates: list,
    pre_period: list,
    post_period: list,
    advertiser_id: int,
) -> dict:
    """Run CausalImpact for a single advertiser + metric."""
    # use metric_lag1 as additional covariate
    all_covariates = covariates + ["metric_lag1"]
    available = [c for c in all_covariates if c in adv_data.columns]

    columns = [metric] + available
    ci_data = adv_data[columns].copy()
    ci_data = ci_data.dropna(subset=[metric])
    ci_data[available] = ci_data[available].ffill().bfill()
    ci_data = ci_data.astype(float)

    if len(ci_data) < 10:
        log.warning(f"  {advertiser_id}: insufficient rows ({len(ci_data)})")
        return None

    post_spend = adv_data.loc[adv_data.index >= post_period[0], "spend"].sum()
    post_impressions = adv_data.loc[adv_data.index >= post_period[0], "impressions"].sum()

    try:
        ci = CausalImpact(ci_data, pre_period, post_period)
        inf = ci.inferences[ci.inferences.index >= post_period[0]]

        actual_avg = ci.post_data.iloc[:, 0].mean()
        predicted_avg = inf["preds"].mean()
        abs_effect = inf["point_effects"].mean()
        rel_effect = abs_effect / predicted_avg if predicted_avg != 0 else np.nan

        return {
            "advertiser_id": advertiser_id,
            "metric": metric,
            "pre_weeks": int((pre_period[1] - pre_period[0]).days / 7) + 1,
            "post_weeks": int((post_period[1] - post_period[0]).days / 7) + 1,
            "pre_period": f"{pre_period[0].date()} to {pre_period[1].date()}",
            "post_period": f"{post_period[0].date()} to {post_period[1].date()}",
            "actual_avg": actual_avg,
            "predicted_avg": predicted_avg,
            "absolute_effect": abs_effect,
            "relative_effect": rel_effect,
            "ci_lower": inf["point_effects_lower"].mean(),
            "ci_upper": inf["point_effects_upper"].mean(),
            "p_value": ci.p_value,
            "significant": ci.p_value < 0.05,
            "post_spend": float(post_spend),
            "post_impressions": float(post_impressions),
            "ci_object": ci,
        }
    except Exception as e:
        log.error(f"  {advertiser_id}: CausalImpact failed — {e}")
        return None


def run_placebo_test(
    adv_data: pd.DataFrame,
    metric: str,
    covariates: list,
    real_pre_period: list,
    advertiser_id: int,
) -> dict:
    """Run placebo test: fake intervention at pre-period midpoint. Should show no effect."""
    pre_only = adv_data[adv_data.index <= real_pre_period[1]]
    if len(pre_only) < 20:  # need enough data for meaningful placebo
        return None

    midpoint_idx = len(pre_only) // 2
    sorted_idx = pre_only.index.sort_values()
    placebo_pre = [sorted_idx[0], sorted_idx[midpoint_idx - 1]]
    placebo_post = [sorted_idx[midpoint_idx], sorted_idx[-1]]

    result = run_single_analysis(
        pre_only, metric, covariates, placebo_pre, placebo_post, advertiser_id
    )
    if result:
        result["test_type"] = "placebo"
    return result


# =============================================================================
# WITHIN-ADVERTISER COMPARISON
# =============================================================================

def run_within_advertiser_comparison(
    weekly_kpis: pd.DataFrame,
    recommended_cg_ids: set,
    adopters: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each adopter, compare recommended vs non-recommended campaign groups
    in the post-period (after first recommended plan). Side-by-side comparison.
    """
    results = []
    for _, row in adopters.iterrows():
        adv_id = row["advertiser_id"]
        intervention = pd.Timestamp(row["intervention_date"])
        intervention_week = intervention - pd.Timedelta(days=intervention.weekday())
        post_start = intervention_week + pd.Timedelta(weeks=RAMP_UP_WEEKS)

        # post-period data AFTER ramp-up window for this advertiser
        adv = weekly_kpis[
            (weekly_kpis["advertiser_id"] == adv_id) &
            (weekly_kpis["week_start"] >= post_start)
        ]

        rec = adv[adv["campaign_group_id"].isin(recommended_cg_ids)]
        non_rec = adv[~adv["campaign_group_id"].isin(recommended_cg_ids)]

        if rec.empty:
            continue

        rec_agg = rec.agg({
            "impressions": "sum", "spend": "sum", "vv": "sum",
            "conversions": "sum", "order_value": "sum",
        })
        non_rec_agg = non_rec.agg({
            "impressions": "sum", "spend": "sum", "vv": "sum",
            "conversions": "sum", "order_value": "sum",
        }) if not non_rec.empty else pd.Series({
            "impressions": 0, "spend": 0, "vv": 0, "conversions": 0, "order_value": 0,
        })

        rec_ivr = rec_agg["vv"] / rec_agg["impressions"] if rec_agg["impressions"] > 0 else np.nan
        non_rec_ivr = non_rec_agg["vv"] / non_rec_agg["impressions"] if non_rec_agg["impressions"] > 0 else np.nan
        rec_cvr = rec_agg["conversions"] / rec_agg["vv"] if rec_agg["vv"] > 0 else np.nan
        non_rec_cvr = non_rec_agg["conversions"] / non_rec_agg["vv"] if non_rec_agg["vv"] > 0 else np.nan
        rec_roas = rec_agg["order_value"] / rec_agg["spend"] if rec_agg["spend"] > 0 else np.nan
        non_rec_roas = non_rec_agg["order_value"] / non_rec_agg["spend"] if non_rec_agg["spend"] > 0 else np.nan

        results.append({
            "advertiser_id": adv_id,
            "company_name": row["company_name"],
            "rec_impressions": rec_agg["impressions"],
            "rec_spend": rec_agg["spend"],
            "rec_ivr": rec_ivr,
            "rec_cvr": rec_cvr,
            "rec_roas": rec_roas,
            "non_rec_impressions": non_rec_agg["impressions"],
            "non_rec_spend": non_rec_agg["spend"],
            "non_rec_ivr": non_rec_ivr,
            "non_rec_cvr": non_rec_cvr,
            "non_rec_roas": non_rec_roas,
            "ivr_diff": rec_ivr - non_rec_ivr if pd.notna(rec_ivr) and pd.notna(non_rec_ivr) else np.nan,
            "has_both": not non_rec.empty,
        })

    return pd.DataFrame(results)


# =============================================================================
# AGGREGATION & REPORTING
# =============================================================================

def aggregate_results(results: list) -> pd.DataFrame:
    clean = [{k: v for k, v in r.items() if k != "ci_object"} for r in results if r]
    if not clean:
        return pd.DataFrame()
    df = pd.DataFrame(clean)
    df["relative_effect_pct"] = df["relative_effect"].apply(lambda x: f"{x:+.2%}" if pd.notna(x) else "N/A")
    df["p_value_fmt"] = df["p_value"].apply(lambda x: f"{x:.4f}")
    df["significance"] = df["significant"].apply(lambda x: "Significant" if x else "Not significant")
    return df


def print_summary(summary_df: pd.DataFrame, metric: str):
    if summary_df.empty:
        log.warning("No results to display.")
        return

    metric_info = METRIC_DEFINITIONS[metric]
    print(f"\n{'='*100}")
    print(f"CAUSAL IMPACT SUMMARY — {metric_info['label']} ({metric.upper()})")
    print(f"{'='*100}")

    display_cols = ["advertiser_id"]
    if "company_name" in summary_df.columns:
        display_cols.append("company_name")
    display_cols += ["pre_weeks", "post_weeks", "actual_avg", "predicted_avg",
                     "relative_effect_pct", "p_value_fmt", "significance"]
    print(summary_df[display_cols].to_string(index=False))

    n = len(summary_df)
    n_sig = summary_df["significant"].sum()
    direction = metric_info["direction"]
    n_positive = (summary_df["relative_effect"] > 0).sum() if direction == "higher" else (summary_df["relative_effect"] < 0).sum()

    avg_effect = summary_df["relative_effect"].mean()
    median_effect = summary_df["relative_effect"].median()
    total_spend = summary_df["post_spend"].sum()
    weighted = (summary_df["relative_effect"] * summary_df["post_spend"]).sum() / total_spend if total_spend > 0 else avg_effect

    print(f"\n--- Aggregate ---")
    print(f"Advertisers analyzed:       {n}")
    print(f"Statistically significant:  {n_sig} ({n_sig/n:.0%})")
    print(f"Positive outcome:           {n_positive} ({n_positive/n:.0%})")
    print(f"Mean relative effect:       {avg_effect:+.2%}")
    print(f"Median relative effect:     {median_effect:+.2%}")
    print(f"Spend-weighted avg effect:  {weighted:+.2%}")
    print(f"{'='*100}\n")


def save_plots(results: list, metric: str, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    for r in results:
        if r is None or "ci_object" not in r:
            continue
        ci = r["ci_object"]
        adv_id = r["advertiser_id"]
        name = r.get("company_name", str(adv_id))

        fig = ci.plot(figsize=(14, 10))
        if fig is None:
            fig = plt.gcf()
        fig.suptitle(
            f"{name} ({adv_id}) — {metric.upper()} "
            f"(effect: {r['relative_effect']:+.2%}, p={r['p_value']:.4f})",
            fontsize=13, y=1.02,
        )
        fig.tight_layout()
        fig.savefig(output_dir / f"ci_{metric}_{adv_id}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)

    # summary bar chart
    clean = [r for r in results if r is not None]
    if not clean:
        return

    df = pd.DataFrame(clean).sort_values("relative_effect")
    direction = METRIC_DEFINITIONS[metric]["direction"]

    if direction == "higher":
        colors = ["#2ecc71" if (s and e > 0) else "#e74c3c" if (s and e < 0) else "#95a5a6"
                  for s, e in zip(df["significant"], df["relative_effect"])]
    else:
        colors = ["#2ecc71" if (s and e < 0) else "#e74c3c" if (s and e > 0) else "#95a5a6"
                  for s, e in zip(df["significant"], df["relative_effect"])]

    labels = df.get("company_name", df["advertiser_id"].astype(str))
    fig, ax = plt.subplots(figsize=(12, max(6, len(df) * 0.6)))
    bars = ax.barh(labels, df["relative_effect"] * 100, color=colors)
    ax.axvline(x=0, color="black", linewidth=0.8)
    for bar, pval in zip(bars, df["p_value"]):
        x = bar.get_width()
        ax.text(x + (0.5 if x >= 0 else -0.5), bar.get_y() + bar.get_height() / 2,
                f"p={pval:.3f}", va="center", ha="left" if x >= 0 else "right", fontsize=8)
    ax.set_xlabel("Relative Effect (%)")
    ax.set_title(f"Media Plan Causal Impact — {metric.upper()} (Recommended Plans Only)")
    plt.tight_layout()
    fig.savefig(output_dir / f"ci_{metric}_summary.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Plots saved to {output_dir}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="TI-748: Causal Impact — Media Plan (v2)")
    parser.add_argument("--metric", default="ivr", choices=list(METRIC_DEFINITIONS.keys()))
    parser.add_argument("--all-metrics", action="store_true")
    parser.add_argument("--min-pre-weeks", type=int, default=20,
                        help="Minimum pre-period weeks (default: 20; 52 available from 2024-01-01)")
    parser.add_argument("--min-post-weeks", type=int, default=4)
    parser.add_argument("--placebo", action="store_true")
    parser.add_argument("--save-plots", action="store_true")
    parser.add_argument("--output-csv", action="store_true")
    parser.add_argument("--comparison", action="store_true",
                        help="Run within-advertiser recommended vs non-recommended comparison")
    args = parser.parse_args()

    metrics_to_run = list(METRIC_DEFINITIONS.keys()) if args.all_metrics else [args.metric]

    # --- Step 1: Load data ---
    log.info("Connecting to BigQuery...")
    client = get_bq_client()

    log.info("Loading recommended media plan adopters...")
    adopters = load_adopters(client)

    log.info("Loading recommended campaign group IDs...")
    recommended_cg_ids = load_recommended_campaign_groups(client)
    log.info(f"  {len(recommended_cg_ids)} recommended campaign groups")

    log.info("Loading weekly KPIs (sum_by_campaign_by_day, 2024-01-01+)...")
    weekly_kpis = load_weekly_kpis(client, adopters["advertiser_id"].tolist())

    log.info("Building platform covariates...")
    # for platform covariates, use advertiser-level aggregation
    adv_week_all = aggregate_to_advertiser_week(weekly_kpis)
    platform_covariates = build_platform_covariates(weekly_kpis)

    # Advertiser-week: ALL prospecting campaigns (for CausalImpact pre AND post)
    # CausalImpact measures advertiser-level impact of HAVING media plan in their portfolio
    adv_week_all = aggregate_to_advertiser_week(weekly_kpis)

    # --- Step 2: Run CausalImpact per advertiser ---
    for metric in metrics_to_run:
        log.info(f"\n{'='*60}")
        log.info(f"Analyzing: {metric.upper()} ({METRIC_DEFINITIONS[metric]['label']})")
        log.info(f"{'='*60}")

        all_results = []
        placebo_results = []

        for _, adv_row in adopters.iterrows():
            adv_id = adv_row["advertiser_id"]
            adv_name = adv_row.get("company_name", "Unknown")
            intervention = adv_row["intervention_date"]
            intervention_ts = pd.Timestamp(intervention)
            intervention_week = intervention_ts - pd.Timedelta(days=intervention_ts.weekday())

            log.info(f"Processing {adv_name} ({adv_id}, intervention: {intervention})...")

            # Both pre and post use ALL prospecting campaigns (advertiser-level)
            # This measures: "did the advertiser's overall performance change after adoption?"
            pre_data = adv_week_all[
                (adv_week_all["advertiser_id"] == adv_id) &
                (adv_week_all["week_start"] < intervention_week)
            ].copy()

            post_data = adv_week_all[
                (adv_week_all["advertiser_id"] == adv_id) &
                (adv_week_all["week_start"] >= intervention_week)
            ].copy()

            adv_data, pre_period, post_period = prepare_advertiser_data(
                adv_id, intervention, pre_data, post_data,
                platform_covariates, metric,
                args.min_pre_weeks, args.min_post_weeks,
            )
            if adv_data is None:
                continue

            result = run_single_analysis(
                adv_data, metric, COVARIATES, pre_period, post_period, adv_id
            )
            if result:
                result["company_name"] = adv_name
                all_results.append(result)
                log.info(
                    f"  -> Effect: {result['relative_effect']:+.2%} "
                    f"(p={result['p_value']:.4f}) "
                    f"{'*' if result['significant'] else ''}"
                )

            if args.placebo and adv_data is not None:
                placebo = run_placebo_test(adv_data, metric, COVARIATES, pre_period, adv_id)
                if placebo:
                    placebo_results.append(placebo)

        # --- Step 3: Summarize ---
        summary_df = aggregate_results(all_results)
        print_summary(summary_df, metric)

        if args.placebo and placebo_results:
            placebo_df = aggregate_results(placebo_results)
            n_sig = placebo_df["significant"].sum() if not placebo_df.empty else 0
            print(f"\n--- PLACEBO TEST RESULTS ({metric.upper()}) ---")
            print(f"Placebo tests run:  {len(placebo_df)}")
            print(f"False positives:    {n_sig} ({n_sig/max(len(placebo_df),1):.0%})")
            if len(placebo_df) > 0 and n_sig / len(placebo_df) > 0.10:
                print("WARNING: High false positive rate")
            print()

        if args.save_plots and all_results:
            save_plots(all_results, metric, OUTPUT_DIR)

        if args.output_csv and not summary_df.empty:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            csv_path = OUTPUT_DIR / f"ci_{metric}_results.csv"
            summary_df.to_csv(csv_path, index=False)
            log.info(f"Results exported to {csv_path}")

    # --- Step 4: Within-advertiser comparison ---
    if args.comparison:
        log.info("\n" + "=" * 60)
        log.info("WITHIN-ADVERTISER COMPARISON: Recommended vs Non-Recommended")
        log.info("=" * 60)

        comparison_df = run_within_advertiser_comparison(
            weekly_kpis, recommended_cg_ids, adopters
        )
        if not comparison_df.empty:
            print(f"\n{'='*100}")
            print("WITHIN-ADVERTISER COMPARISON — Recommended vs Non-Recommended Campaign Groups (Post-Period)")
            print(f"{'='*100}")
            display_cols = ["company_name", "advertiser_id", "has_both",
                            "rec_ivr", "non_rec_ivr", "ivr_diff",
                            "rec_cvr", "non_rec_cvr",
                            "rec_roas", "non_rec_roas",
                            "rec_spend", "non_rec_spend"]
            print(comparison_df[display_cols].to_string(index=False, float_format="%.4f"))

            both = comparison_df[comparison_df["has_both"]]
            if not both.empty:
                print(f"\nAdvertisers with BOTH recommended & non-recommended: {len(both)}")
                print(f"Avg IVR diff (rec - non_rec): {both['ivr_diff'].mean():+.4f}")

            if args.output_csv:
                csv_path = OUTPUT_DIR / "within_advertiser_comparison.csv"
                comparison_df.to_csv(csv_path, index=False)
                log.info(f"Comparison exported to {csv_path}")

    log.info("Done.")


if __name__ == "__main__":
    main()
