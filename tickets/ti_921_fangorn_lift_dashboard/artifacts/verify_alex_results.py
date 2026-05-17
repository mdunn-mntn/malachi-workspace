"""
TI-921 — Local verification of Alex's RolloutTierEvaluations notebook.

Inputs : outputs/verify_alex_daily_perf.csv  (both passes from BQ)
Outputs: outputs/verify_alex_tier_summary.csv
         outputs/verify_alex_advertiser_change.csv
         outputs/verify_alex_filter_impact.csv
         stdout summary table

Replicates Alex's Tier-1 pre/post + per-advertiser change math in pandas.
DiD vs untreated tiers is NOT replicated here — Alex's control set comes from
tpa.fangorn_advertiser_inclusion (Postgres-only). We compare 'alex' (his
filters: funnel_level=1 + objective_id=1 + mntn_matched_cgids) against
'loose' (TI-921 baseline: funnel_level=1) on the same 51 AIDs to isolate the
impact of his extra filters.
"""

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PERF_CSV = ROOT / "outputs" / "verify_alex_daily_perf.csv"
OUT_DIR = ROOT / "outputs"

LOOKBACK_DAYS = 14         # Alex's default widget value
CHANGE_THRESHOLD = 0.10    # Alex's default — ±10% per-AID visit-rate change


def load_panel() -> pd.DataFrame:
    df = pd.read_csv(PERF_CSV, parse_dates=["flip_date", "day"])
    df["impressions"] = df["impressions"].astype("int64")
    df["vv"] = df["vv"].astype("int64")
    df["conversions"] = df["conversions"].astype("int64")
    df["spend"] = df["spend"].astype(float)
    df["order_value"] = df["order_value"].astype(float)
    return df


def assign_period(df: pd.DataFrame) -> pd.DataFrame:
    pre_start = df["flip_date"] - pd.Timedelta(days=LOOKBACK_DAYS)
    period = np.where(
        (df["day"] >= pre_start) & (df["day"] < df["flip_date"]),
        "pre",
        np.where(df["day"] > df["flip_date"], "post", None),
    )
    return df.assign(period=period).dropna(subset=["period"])


def tier_summary(period_df: pd.DataFrame) -> pd.DataFrame:
    """Pooled (Alex's tier_summary_df equivalent), grouped by pass + cohort."""
    g = (
        period_df.groupby(["pass", "cohort", "period"])
        .agg(
            advertisers=("advertiser_id", "nunique"),
            days=("day", "nunique"),
            impressions=("impressions", "sum"),
            visits=("vv", "sum"),
            conversions=("conversions", "sum"),
            spend=("spend", "sum"),
            order_value=("order_value", "sum"),
        )
        .reset_index()
    )
    g["visit_rate"] = g["visits"] / g["impressions"]
    g["conv_rate"] = g["conversions"] / g["impressions"]
    g["cpv"] = g["spend"] / g["visits"].replace(0, np.nan)
    g["cpa"] = g["spend"] / g["conversions"].replace(0, np.nan)
    g["roas"] = g["order_value"] / g["spend"].replace(0, np.nan)
    return g.sort_values(["pass", "cohort", "period"])


def pivot_pre_post(tier_df: pd.DataFrame) -> pd.DataFrame:
    """Wide pivot with treated-lift = post/pre - 1 per cohort × pass."""
    metric_cols = ["impressions", "visits", "visit_rate", "spend"]
    p = tier_df.pivot_table(
        index=["pass", "cohort"],
        columns="period",
        values=metric_cols,
    )
    p.columns = [f"{period}_{metric}" for metric, period in p.columns]
    p = p.reset_index()
    p["visit_rate_lift_pct"] = p["post_visit_rate"] / p["pre_visit_rate"] - 1
    return p[
        [
            "pass", "cohort",
            "pre_impressions", "post_impressions",
            "pre_visits", "post_visits",
            "pre_visit_rate", "post_visit_rate",
            "visit_rate_lift_pct",
            "pre_spend", "post_spend",
        ]
    ].sort_values(["cohort", "pass"])


def advertiser_change(period_df: pd.DataFrame) -> pd.DataFrame:
    g = (
        period_df.groupby(["pass", "advertiser_id", "company_name", "cohort", "period"])
        .agg(
            impressions=("impressions", "sum"),
            visits=("vv", "sum"),
            spend=("spend", "sum"),
        )
        .reset_index()
    )
    g["visit_rate"] = g["visits"] / g["impressions"]
    wide = g.pivot_table(
        index=["pass", "advertiser_id", "company_name", "cohort"],
        columns="period",
        values=["impressions", "visits", "visit_rate", "spend"],
    )
    wide.columns = [f"{period}_{metric}" for metric, period in wide.columns]
    wide = wide.reset_index()
    wide["visit_rate_pct_change"] = (
        wide["post_visit_rate"] - wide["pre_visit_rate"]
    ) / wide["pre_visit_rate"]
    wide["change_bucket"] = np.select(
        [
            wide["pre_visit_rate"].isna() | (wide["pre_visit_rate"] == 0),
            wide["post_visit_rate"].isna(),
            wide["visit_rate_pct_change"] <= -CHANGE_THRESHOLD,
            wide["visit_rate_pct_change"] >= CHANGE_THRESHOLD,
        ],
        ["no_pre_data", "no_post_data", "drop_ge_threshold", "rise_ge_threshold"],
        default="within_threshold",
    )
    return wide.sort_values(
        ["pass", "cohort", "visit_rate_pct_change"],
        na_position="last",
    )


def threshold_summary(adv_df: pd.DataFrame) -> pd.DataFrame:
    eligible = adv_df[adv_df["visit_rate_pct_change"].notna()].copy()
    g = (
        eligible.groupby(["pass", "cohort"])
        .agg(
            advertisers_with_pre_post=("advertiser_id", "nunique"),
            n_drop=("change_bucket", lambda s: (s == "drop_ge_threshold").sum()),
            n_rise=("change_bucket", lambda s: (s == "rise_ge_threshold").sum()),
            median_pct_change=("visit_rate_pct_change", "median"),
        )
        .reset_index()
    )
    g["pct_drop_ge_threshold"] = g["n_drop"] / g["advertisers_with_pre_post"]
    g["pct_rise_ge_threshold"] = g["n_rise"] / g["advertisers_with_pre_post"]
    return g.sort_values(["pass", "cohort"])


def filter_impact_table(tier_pivot: pd.DataFrame) -> pd.DataFrame:
    """For each cohort: how does Alex's filter change vs the loose baseline?"""
    a = tier_pivot[tier_pivot["pass"] == "alex"].set_index("cohort")
    l = tier_pivot[tier_pivot["pass"] == "loose"].set_index("cohort")
    rows = []
    for cohort in sorted(a.index.union(l.index)):
        ar = a.loc[cohort] if cohort in a.index else None
        lr = l.loc[cohort] if cohort in l.index else None
        if ar is None or lr is None:
            continue
        rows.append({
            "cohort": cohort,
            "pre_imps_alex": int(ar["pre_impressions"]),
            "pre_imps_loose": int(lr["pre_impressions"]),
            "pre_imps_kept_pct": ar["pre_impressions"] / lr["pre_impressions"],
            "post_imps_alex": int(ar["post_impressions"]),
            "post_imps_loose": int(lr["post_impressions"]),
            "post_imps_kept_pct": ar["post_impressions"] / lr["post_impressions"],
            "visit_rate_lift_alex": ar["visit_rate_lift_pct"],
            "visit_rate_lift_loose": lr["visit_rate_lift_pct"],
            "lift_delta_pp": (ar["visit_rate_lift_pct"] - lr["visit_rate_lift_pct"]) * 100,
        })
    return pd.DataFrame(rows)


def main() -> None:
    panel = load_panel()
    print(f"loaded {len(panel):,} rows · {panel['advertiser_id'].nunique()} AIDs · passes={sorted(panel['pass'].unique())}\n")

    period_df = assign_period(panel)
    tier_df = tier_summary(period_df)
    tier_df.to_csv(OUT_DIR / "verify_alex_tier_summary.csv", index=False)

    tier_pivot = pivot_pre_post(tier_df)
    tier_pivot.to_csv(OUT_DIR / "verify_alex_tier_pivot.csv", index=False)

    adv_df = advertiser_change(period_df)
    adv_df.to_csv(OUT_DIR / "verify_alex_advertiser_change.csv", index=False)

    threshold_df = threshold_summary(adv_df)
    threshold_df.to_csv(OUT_DIR / "verify_alex_threshold_summary.csv", index=False)

    impact_df = filter_impact_table(tier_pivot)
    impact_df.to_csv(OUT_DIR / "verify_alex_filter_impact.csv", index=False)

    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 30)
    pd.set_option("display.float_format", "{:.4f}".format)

    print("=== TIER-1 POOLED PRE/POST (visit-rate lift = post/pre - 1) ===\n")
    print(tier_pivot.to_string(index=False))

    print("\n=== PER-ADVERTISER CHANGE DISTRIBUTION (threshold ±10%) ===\n")
    print(threshold_df.to_string(index=False))

    print("\n=== FILTER IMPACT: Alex's (objective_id=1 + mntn_matched) vs TI-921 baseline ===\n")
    print(impact_df.to_string(index=False))


if __name__ == "__main__":
    main()
