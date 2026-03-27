"""
TI-748: Causal Impact Analysis — Media Plan Feature
====================================================

Measures the incremental lift (or decline) of the Media Plan feature on
advertiser prospecting KPIs using Bayesian Structural Time Series
(Google's CausalImpact methodology).

Design:
  - Per-advertiser analysis with staggered intervention dates
  - Intervention = first media_plan.create_time per advertiser
  - Covariates = platform-wide non-adopter prospecting metrics
  - Weekly aggregation for stability with small N
  - Prospecting campaigns only (funnel_level = 1)

Usage:
  python3 ti_748_causal_impact.py [--metric ivr] [--min-pre-weeks 8] [--min-post-weeks 4]

Requirements:
  pip install google-cloud-bigquery pycausalimpact pandas numpy matplotlib openpyxl
"""

import argparse
import logging
import os
import sys
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from causalimpact import CausalImpact
from google.cloud import bigquery

warnings.filterwarnings("ignore", category=FutureWarning)
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
DATA_FLOOR = date(2025, 1, 1)  # no BQ data before this

# Data quality thresholds
MIN_WEEKLY_IMPRESSIONS = 1000  # filter weeks with fewer impressions (attribution lag artifacts)
MAX_IVR = 1.0  # cap IVR at 100% — anything above is a data anomaly

# Beta advertiser IDs from the Excel list (27 eligible)
BETA_ADVERTISER_IDS = [
    31116, 31966, 32101, 32127, 32756, 32771, 33278, 33667,
    34094, 34114, 34437, 37056, 38363, 38563, 40597, 41426,
    41545, 45616, 45737, 47358, 48620, 48696, 48740, 48807,
    48817, 48844, 48967,
]

# Metrics configuration
METRIC_DEFINITIONS = {
    "ivr":  {"formula": "vv / impressions",       "direction": "higher", "label": "Impression-to-Visit Rate"},
    "cvr":  {"formula": "conversions / vv",        "direction": "higher", "label": "Conversion Rate"},
    "cpa":  {"formula": "spend / conversions",     "direction": "lower",  "label": "Cost per Acquisition"},
    "cpv":  {"formula": "spend / vv",              "direction": "lower",  "label": "Cost per Visit"},
    "roas": {"formula": "order_value / spend",     "direction": "higher", "label": "Return on Ad Spend"},
}
# Note: VVR (vv/uniques) excluded — uniques is unreliable at campaign-level aggregation

COVARIATES = ["platform_ivr", "platform_spend", "platform_impressions"]

OUTPUT_DIR = Path(__file__).parent.parent / "outputs"


# =============================================================================
# DATA LOADING
# =============================================================================

def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT)


def load_adopters(client: bigquery.Client) -> pd.DataFrame:
    """
    Identify advertisers actively using Media Plan (status=3).
    Returns DataFrame with advertiser_id, company_name, first_plan_created, total_plans.
    """
    query = """
    SELECT
        mp.advertiser_id,
        adv.company_name,
        MIN(mp.create_time) AS first_plan_created,
        COUNT(*) AS total_plans,
        COUNT(DISTINCT mp.campaign_group_id) AS campaign_groups_with_plan
    FROM `dw-main-silver.core.media_plan` mp
    JOIN `dw-main-bronze.integrationprod.advertisers` adv
        ON adv.advertiser_id = mp.advertiser_id
    WHERE mp.media_plan_status_id = 3
    GROUP BY 1, 2
    ORDER BY 3
    """
    df = client.query(query).to_dataframe()
    df["first_plan_created"] = pd.to_datetime(df["first_plan_created"])
    df["intervention_date"] = df["first_plan_created"].dt.date
    log.info(f"Found {len(df)} advertisers with active media plans")
    return df


def load_weekly_kpis(client: bigquery.Client, adopter_ids: list) -> pd.DataFrame:
    """
    Load weekly prospecting KPIs for ALL advertisers (adopters + non-adopters).
    Joins agg__daily_sum_by_campaign to campaigns for funnel_level filtering.
    All adopters use industry_standard attribution (include competing views).
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
        DATE_TRUNC(a.day, WEEK(MONDAY)) AS week_start,
        CASE
            WHEN pc.advertiser_id IN ({adopter_list}) THEN TRUE
            ELSE FALSE
        END AS is_adopter,

        -- volume
        SUM(a.impressions) AS impressions,
        SUM(a.media_spend + a.data_spend + a.platform_spend) AS spend,
        SUM(a.uniques) AS uniques,

        -- verified visits (industry_standard: include competing)
        SUM(a.clicks + a.views + COALESCE(a.competing_views, 0)) AS vv,

        -- conversions (industry_standard: include competing)
        SUM(
            a.click_conversions + a.view_conversions
            + COALESCE(a.competing_view_conversions, 0)
        ) AS conversions,

        -- order value (industry_standard: include competing)
        SUM(
            a.click_order_value + a.view_order_value
            + COALESCE(a.competing_view_order_value, 0)
        ) AS order_value

    FROM `dw-main-silver.aggregates.agg__daily_sum_by_campaign` a
    INNER JOIN prospecting_campaigns pc
        ON pc.campaign_id = a.campaign_id
    WHERE a.day >= '{DATA_FLOOR.isoformat()}'
      AND a.impressions > 0
    GROUP BY 1, 2, 3
    ORDER BY 1, 2
    """
    log.info("Loading weekly KPIs from BQ (this may take a minute)...")
    df = client.query(query).to_dataframe()
    df["week_start"] = pd.to_datetime(df["week_start"])

    # ensure numeric types
    numeric_cols = ["impressions", "spend", "uniques", "vv", "conversions", "order_value"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)

    # filter out low-impression weeks (attribution lag artifacts)
    before_filter = len(df)
    df = df[df["impressions"] >= MIN_WEEKLY_IMPRESSIONS].copy()
    log.info(f"Filtered {before_filter - len(df)} low-impression weeks (<{MIN_WEEKLY_IMPRESSIONS})")

    log.info(
        f"Loaded {len(df):,} weekly records: "
        f"{df['advertiser_id'].nunique()} advertisers, "
        f"{df[df['is_adopter']]['advertiser_id'].nunique()} adopters, "
        f"{df['week_start'].min().date()} to {df['week_start'].max().date()}"
    )
    return df


# =============================================================================
# DATA PREPARATION
# =============================================================================

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived rate metrics to a DataFrame with volume columns."""
    df = df.copy()
    df["ivr"] = df["vv"] / df["impressions"].replace(0, np.nan)
    df["cvr"] = df["conversions"] / df["vv"].replace(0, np.nan)
    df["cpa"] = df["spend"] / df["conversions"].replace(0, np.nan)
    df["cpv"] = df["spend"] / df["vv"].replace(0, np.nan)
    df["roas"] = df["order_value"] / df["spend"].replace(0, np.nan)

    # cap rate metrics at reasonable bounds (data quality)
    df["ivr"] = df["ivr"].clip(upper=MAX_IVR)
    df["cvr"] = df["cvr"].clip(upper=1.0)
    return df


def build_platform_covariates(weekly_kpis: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate non-adopter prospecting metrics by week as covariates.
    These are NOT affected by the media plan intervention.
    """
    non_adopter = weekly_kpis[~weekly_kpis["is_adopter"]].copy()
    platform = non_adopter.groupby("week_start").agg(
        platform_impressions=("impressions", "sum"),
        platform_spend=("spend", "sum"),
        platform_vv=("vv", "sum"),
        platform_uniques=("uniques", "sum"),
        platform_conversions=("conversions", "sum"),
        platform_order_value=("order_value", "sum"),
    ).reset_index()

    platform["platform_ivr"] = platform["platform_vv"] / platform["platform_impressions"].replace(0, np.nan)
    platform["platform_cvr"] = platform["platform_conversions"] / platform["platform_vv"].replace(0, np.nan)

    log.info(f"Platform covariates: {len(platform)} weeks, {non_adopter['advertiser_id'].nunique()} non-adopter advertisers")
    return platform


def prepare_advertiser_data(
    advertiser_id: int,
    intervention_date: date,
    weekly_kpis: pd.DataFrame,
    platform_covariates: pd.DataFrame,
    min_pre_weeks: int,
    min_post_weeks: int,
) -> tuple:
    """
    Prepare CausalImpact input for a single advertiser.
    Returns (ci_data, pre_period, post_period) or (None, None, None) if insufficient data.
    """
    # filter to this advertiser
    adv_data = weekly_kpis[weekly_kpis["advertiser_id"] == advertiser_id].copy()
    adv_data = compute_metrics(adv_data)

    # merge platform covariates
    adv_data = adv_data.merge(platform_covariates, on="week_start", how="inner")
    adv_data = adv_data.set_index("week_start").sort_index()

    # determine pre/post periods
    intervention_ts = pd.Timestamp(intervention_date)
    # intervention week = the Monday of the intervention week
    intervention_week = intervention_ts - pd.Timedelta(days=intervention_ts.weekday())

    pre_data = adv_data[adv_data.index < intervention_week]
    post_data = adv_data[adv_data.index >= intervention_week]

    if len(pre_data) < min_pre_weeks:
        log.warning(
            f"  Advertiser {advertiser_id}: only {len(pre_data)} pre-weeks "
            f"(need {min_pre_weeks}), skipping"
        )
        return None, None, None

    if len(post_data) < min_post_weeks:
        log.warning(
            f"  Advertiser {advertiser_id}: only {len(post_data)} post-weeks "
            f"(need {min_post_weeks}), skipping"
        )
        return None, None, None

    pre_period = [pre_data.index[0], pre_data.index[-1]]
    post_period = [post_data.index[0], post_data.index[-1]]

    return adv_data, pre_period, post_period


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
    """
    Run CausalImpact for a single advertiser + metric combination.
    Returns a result dict or None on failure.
    """
    columns = [metric] + covariates
    ci_data = adv_data[columns].copy()

    # drop rows where target is NaN
    ci_data = ci_data.dropna(subset=[metric])
    # fill covariate NaNs
    ci_data[covariates] = ci_data[covariates].ffill().bfill()
    ci_data = ci_data.astype(float)

    if len(ci_data) < 10:
        log.warning(f"  Advertiser {advertiser_id}: insufficient rows ({len(ci_data)})")
        return None

    # compute total post-period spend for weighting
    post_spend = adv_data.loc[adv_data.index >= post_period[0], "spend"].sum() if "spend" in adv_data.columns else 0
    post_impressions = adv_data.loc[adv_data.index >= post_period[0], "impressions"].sum() if "impressions" in adv_data.columns else 0

    try:
        ci = CausalImpact(ci_data, pre_period, post_period)

        # extract results
        inferences = ci.inferences
        post_mask = inferences.index >= post_period[0]
        post_inferences = inferences[post_mask]

        actual_avg = ci.post_data.iloc[:, 0].mean()
        predicted_avg = post_inferences["preds"].mean()
        abs_effect = post_inferences["point_effects"].mean()
        rel_effect = abs_effect / predicted_avg if predicted_avg != 0 else np.nan

        ci_lower = post_inferences["point_effects_lower"].mean()
        ci_upper = post_inferences["point_effects_upper"].mean()
        p_value = ci.p_value

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
            "ci_lower": ci_lower,
            "ci_upper": ci_upper,
            "p_value": p_value,
            "significant": p_value < 0.05,
            "post_spend": float(post_spend),
            "post_impressions": float(post_impressions),
            "ci_object": ci,
        }

    except Exception as e:
        log.error(f"  Advertiser {advertiser_id}: CausalImpact failed — {e}")
        return None


def run_placebo_test(
    adv_data: pd.DataFrame,
    metric: str,
    covariates: list,
    real_pre_period: list,
    advertiser_id: int,
) -> dict:
    """
    Run a placebo test: use the midpoint of the pre-period as a fake intervention.
    If the method is valid, placebo should show NO significant effect.
    """
    pre_only = adv_data[adv_data.index <= real_pre_period[1]]
    if len(pre_only) < 12:
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
# AGGREGATION & REPORTING
# =============================================================================

def aggregate_results(results: list) -> pd.DataFrame:
    """Create summary DataFrame from per-advertiser results."""
    clean = [{k: v for k, v in r.items() if k != "ci_object"} for r in results if r]
    if not clean:
        return pd.DataFrame()

    df = pd.DataFrame(clean)
    df["relative_effect_pct"] = df["relative_effect"].apply(lambda x: f"{x:+.2%}" if pd.notna(x) else "N/A")
    df["p_value_fmt"] = df["p_value"].apply(lambda x: f"{x:.4f}")
    df["significance"] = df["significant"].apply(lambda x: "Significant" if x else "Not significant")
    return df


def print_summary(summary_df: pd.DataFrame, metric: str):
    """Print a formatted summary table."""
    if summary_df.empty:
        log.warning("No results to display.")
        return

    metric_info = METRIC_DEFINITIONS[metric]
    print(f"\n{'='*90}")
    print(f"CAUSAL IMPACT SUMMARY — {metric_info['label']} ({metric.upper()})")
    print(f"{'='*90}")

    display_cols = ["advertiser_id"]
    if "company_name" in summary_df.columns:
        display_cols.append("company_name")
    display_cols += [
        "pre_weeks", "post_weeks",
        "actual_avg", "predicted_avg", "relative_effect_pct",
        "p_value_fmt", "significance"
    ]
    print(summary_df[display_cols].to_string(index=False))

    # aggregate stats
    n_total = len(summary_df)
    n_sig = summary_df["significant"].sum()
    n_improved = summary_df[summary_df["relative_effect"] > 0].shape[0]
    n_declined = summary_df[summary_df["relative_effect"] < 0].shape[0]
    direction = metric_info["direction"]

    if direction == "higher":
        n_positive = n_improved
    else:
        n_positive = n_declined  # for CPA/CPV, lower = better

    avg_effect = summary_df["relative_effect"].mean()
    median_effect = summary_df["relative_effect"].median()

    # spend-weighted average (more meaningful — larger advertisers count more)
    if "post_spend" in summary_df.columns:
        total_spend = summary_df["post_spend"].sum()
        if total_spend > 0:
            weighted_effect = (summary_df["relative_effect"] * summary_df["post_spend"]).sum() / total_spend
        else:
            weighted_effect = avg_effect
    else:
        weighted_effect = avg_effect

    print(f"\n--- Aggregate ---")
    print(f"Advertisers analyzed:       {n_total}")
    print(f"Statistically significant:  {n_sig} ({n_sig/n_total:.0%})")
    print(f"Positive outcome:           {n_positive} ({n_positive/n_total:.0%})")
    print(f"Mean relative effect:       {avg_effect:+.2%}")
    print(f"Median relative effect:     {median_effect:+.2%}")
    print(f"Spend-weighted avg effect:  {weighted_effect:+.2%}")
    print(f"{'='*90}\n")


def save_plots(results: list, metric: str, output_dir: Path):
    """Save per-advertiser CausalImpact plots and an aggregate summary chart."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # per-advertiser plots
    for r in results:
        if r is None or "ci_object" not in r:
            continue
        ci = r["ci_object"]
        adv_id = r["advertiser_id"]

        fig = ci.plot(figsize=(14, 10))
        if fig is None:
            fig = plt.gcf()
        fig.suptitle(
            f"Advertiser {adv_id} — {metric.upper()} "
            f"(effect: {r['relative_effect']:+.2%}, p={r['p_value']:.4f})",
            fontsize=13, y=1.02,
        )
        fig.tight_layout()
        fig.savefig(output_dir / f"ci_{metric}_{adv_id}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)

    # aggregate summary bar chart
    clean = [r for r in results if r is not None]
    if not clean:
        return

    df = pd.DataFrame(clean)
    df = df.sort_values("relative_effect")

    fig, ax = plt.subplots(figsize=(12, max(6, len(df) * 0.5)))
    colors = ["#2ecc71" if sig else "#95a5a6" for sig in df["significant"]]

    direction = METRIC_DEFINITIONS[metric]["direction"]
    if direction == "lower":
        colors = ["#2ecc71" if (sig and eff < 0) else "#e74c3c" if (sig and eff > 0) else "#95a5a6"
                  for sig, eff in zip(df["significant"], df["relative_effect"])]
    else:
        colors = ["#2ecc71" if (sig and eff > 0) else "#e74c3c" if (sig and eff < 0) else "#95a5a6"
                  for sig, eff in zip(df["significant"], df["relative_effect"])]

    bars = ax.barh(
        [str(x) for x in df["advertiser_id"]],
        df["relative_effect"] * 100,
        color=colors,
    )
    ax.axvline(x=0, color="black", linewidth=0.8)
    ax.set_xlabel("Relative Effect (%)")
    ax.set_ylabel("Advertiser ID")
    ax.set_title(f"Media Plan Causal Impact — {metric.upper()} by Advertiser")

    # add p-value annotations
    for bar, pval in zip(bars, df["p_value"]):
        x = bar.get_width()
        ax.text(
            x + (0.5 if x >= 0 else -0.5),
            bar.get_y() + bar.get_height() / 2,
            f"p={pval:.3f}",
            va="center", ha="left" if x >= 0 else "right",
            fontsize=8,
        )

    plt.tight_layout()
    fig.savefig(output_dir / f"ci_{metric}_summary.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Plots saved to {output_dir}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="TI-748: Causal Impact — Media Plan")
    parser.add_argument("--metric", default="ivr", choices=list(METRIC_DEFINITIONS.keys()),
                        help="Primary metric to analyze (default: ivr)")
    parser.add_argument("--all-metrics", action="store_true",
                        help="Run analysis for all metrics")
    parser.add_argument("--min-pre-weeks", type=int, default=6,
                        help="Minimum pre-period weeks required (default: 6; agg data starts 2025-09-01)")
    parser.add_argument("--min-post-weeks", type=int, default=4,
                        help="Minimum post-period weeks required (default: 4)")
    parser.add_argument("--placebo", action="store_true",
                        help="Run placebo tests for validation")
    parser.add_argument("--save-plots", action="store_true",
                        help="Save per-advertiser and summary plots")
    parser.add_argument("--output-csv", action="store_true",
                        help="Export results to CSV")
    args = parser.parse_args()

    metrics_to_run = list(METRIC_DEFINITIONS.keys()) if args.all_metrics else [args.metric]

    # --- Step 1: Load data ---
    log.info("Connecting to BigQuery...")
    client = get_bq_client()

    log.info("Loading adopter information from core.media_plan...")
    adopters = load_adopters(client)

    # cross-reference with beta list
    adopters_in_beta = adopters[adopters["advertiser_id"].isin(BETA_ADVERTISER_IDS)]
    adopters_not_in_beta = adopters[~adopters["advertiser_id"].isin(BETA_ADVERTISER_IDS)]
    if len(adopters_not_in_beta) > 0:
        log.info(
            f"Note: {len(adopters_not_in_beta)} adopters are NOT on the original beta list: "
            f"{adopters_not_in_beta['advertiser_id'].tolist()}"
        )

    log.info("Loading weekly KPIs...")
    weekly_kpis = load_weekly_kpis(client, adopters["advertiser_id"].tolist())

    log.info("Building platform covariates from non-adopter data...")
    platform_covariates = build_platform_covariates(weekly_kpis)

    # --- Step 2: Run analysis per advertiser ---
    for metric in metrics_to_run:
        log.info(f"\n{'='*60}")
        log.info(f"Analyzing metric: {metric.upper()} ({METRIC_DEFINITIONS[metric]['label']})")
        log.info(f"{'='*60}")

        all_results = []
        placebo_results = []

        for _, adv_row in adopters.iterrows():
            adv_id = adv_row["advertiser_id"]
            adv_name = adv_row.get("company_name", "Unknown")
            intervention = adv_row["intervention_date"]
            log.info(f"Processing {adv_name} ({adv_id}, intervention: {intervention})...")

            adv_data, pre_period, post_period = prepare_advertiser_data(
                adv_id, intervention, weekly_kpis, platform_covariates,
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

            # placebo test
            if args.placebo and adv_data is not None:
                placebo = run_placebo_test(
                    adv_data, metric, COVARIATES, pre_period, adv_id
                )
                if placebo:
                    placebo_results.append(placebo)

        # --- Step 3: Summarize ---
        summary_df = aggregate_results(all_results)
        print_summary(summary_df, metric)

        if args.placebo and placebo_results:
            placebo_df = aggregate_results(placebo_results)
            print(f"\n--- PLACEBO TEST RESULTS ({metric.upper()}) ---")
            n_placebo_sig = placebo_df["significant"].sum() if not placebo_df.empty else 0
            print(f"Placebo tests run:  {len(placebo_df)}")
            print(f"False positives:    {n_placebo_sig} ({n_placebo_sig/len(placebo_df):.0%})")
            if n_placebo_sig / max(len(placebo_df), 1) > 0.10:
                print("WARNING: High false positive rate — methodology may be unreliable for this metric")
            print()

        # --- Step 4: Export ---
        if args.save_plots and all_results:
            save_plots(all_results, metric, OUTPUT_DIR)

        if args.output_csv and not summary_df.empty:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            csv_path = OUTPUT_DIR / f"ci_{metric}_results.csv"
            summary_df.drop(columns=["ci_object"], errors="ignore").to_csv(csv_path, index=False)
            log.info(f"Results exported to {csv_path}")

    log.info("Done.")


if __name__ == "__main__":
    main()
