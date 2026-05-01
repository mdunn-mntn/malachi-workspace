"""
TI-849 Method 3 — CausalImpact synthetic control per Fangorn AID

Loads the daily covariate matrix from BQ (panel of treated + control AIDs over
90-day pre + post-period), fits a per-(AID, metric) Bayesian structural time
series model with control AIDs' time series as covariates and the treated
AID's own daily spend as a nuisance covariate, and outputs posterior effect
+ 95% credible intervals.

Adapted from TI-504 (`ti_504_ci_all_prospecting.py`) but stripped of the
experiment-validation steps (VIF, BIC, CV, sensitivity, placebo) since
TI-849 is a non-randomized phased rollout, not an RCT.

Usage:
    python ti_849_method3_causal_impact.py

Pre-requisites (one-time):
    pip install causalimpact google-cloud-bigquery pandas numpy matplotlib

Post-period must have ≥1 day of data after 2026-05-01 — re-run daily as
the post-window grows. D+7 (2026-05-07) is the May 7 review target.
"""

from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from google.cloud import bigquery

try:
    from causalimpact import CausalImpact
except ImportError:
    print("Install: pip install causalimpact", file=sys.stderr)
    raise

warnings.filterwarnings("ignore")

# --- config ---
WORKSPACE = Path("/Users/malachi/Developer/work/mntn/workspace")
TICKET_DIR = WORKSPACE / "tickets" / "ti_849_fangorn_score_monitoring"
COVARIATE_SQL = TICKET_DIR / "queries" / "ti_849_method3_covariate_pull.sql"
OUTPUT_DIR = TICKET_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

BQ_PROJECT = "dw-main-bronze"

TREATED_AIDS = {
    32320: "Biz2Credit",
    38659: "Big Blue Bubble Inc.",
    32233: "University of Northwestern Ohio",
}
TREATMENT_DATE = pd.Timestamp("2026-05-01")  # day of vertical_data_source = 46 flip

# Per-AID, fit a model for each of these metrics
METRICS = ["ivr", "vvr", "cvr"]


def load_panel() -> pd.DataFrame:
    """Pull the daily covariate matrix from BQ."""
    client = bigquery.Client(project=BQ_PROJECT)
    sql = COVARIATE_SQL.read_text()
    print(f"[load_panel] Running covariate query against {BQ_PROJECT}...")
    df = client.query(sql).to_dataframe()
    df["day"] = pd.to_datetime(df["day"])
    for col in ["impressions", "uniques", "vv", "conversions",
                "order_value", "spend", "ivr", "vvr", "cvr", "roas"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    print(f"[load_panel] Loaded {len(df):,} rows, "
          f"{df['advertiser_id'].nunique()} AIDs, "
          f"{df['day'].min().date()} → {df['day'].max().date()}")
    return df


def fit_causal_impact(panel: pd.DataFrame, treated_aid: int, metric: str) -> dict:
    """Fit CausalImpact for one (AID, metric).

    y = treated_aid's daily metric
    X = each control AID's daily same-metric + treated_aid's daily spend (nuisance)

    Pre-period: Feb 1 → Apr 29 (release excluded)
    Post-period: May 1 → today

    Returns: dict with posterior summary stats.
    """
    treated_series = (panel[panel["advertiser_id"] == treated_aid]
                      .set_index("day")
                      .sort_index())
    if treated_series.empty:
        return {"error": "no treated data"}

    # Treated vertical to scope controls
    treated_vertical = treated_series["vertical_id"].iloc[0]

    control_aids = (panel[(panel["group_name"] == "control") &
                          (panel["vertical_id"] == treated_vertical)]
                    ["advertiser_id"].unique().tolist())

    if not control_aids:
        return {"error": f"no controls for vertical {treated_vertical}"}

    # Pivot controls to wide: one column per control AID's metric
    control_wide = (panel[panel["advertiser_id"].isin(control_aids)]
                    .pivot_table(index="day", columns="advertiser_id",
                                 values=metric, aggfunc="first"))
    control_wide.columns = [f"ctrl_{c}_{metric}" for c in control_wide.columns]

    # Build the CausalImpact data frame (response + covariates)
    data = pd.DataFrame({"y": treated_series[metric]})
    data = data.join(control_wide, how="left")
    data["own_spend"] = treated_series["spend"]

    # Drop rows with too many NAs in covariates
    data = data.dropna(subset=["y"])
    data = data.dropna(thresh=int(0.5 * len(data.columns)))  # at least half non-null
    data = data.fillna(0.0)

    # Pre/post split
    pre_period = [data.index.min().strftime("%Y-%m-%d"),
                  (TREATMENT_DATE - pd.Timedelta(days=2)).strftime("%Y-%m-%d")]
    post_period = [TREATMENT_DATE.strftime("%Y-%m-%d"),
                   data.index.max().strftime("%Y-%m-%d")]

    if pd.Timestamp(post_period[0]) > pd.Timestamp(post_period[1]):
        return {"error": "no post-period data yet"}

    print(f"[fit] AID={treated_aid} metric={metric} "
          f"vertical={treated_vertical} n_controls={len(control_aids)} "
          f"pre={pre_period} post={post_period}")

    # Fit
    ci = CausalImpact(data, pre_period, post_period)

    # Extract posterior summary
    summary = ci.summary_data
    avg_effect = summary.loc["actual", "average"] - summary.loc["predicted", "average"]
    cum_effect = summary.loc["actual", "cumulative"] - summary.loc["predicted", "cumulative"]
    rel_effect = avg_effect / summary.loc["predicted", "average"] if summary.loc["predicted", "average"] else None
    p_value = ci.p_value

    return {
        "advertiser_id": treated_aid,
        "metric": metric,
        "n_controls": len(control_aids),
        "pre_n_days": (pd.Timestamp(pre_period[1]) - pd.Timestamp(pre_period[0])).days + 1,
        "post_n_days": (pd.Timestamp(post_period[1]) - pd.Timestamp(post_period[0])).days + 1,
        "actual_post_avg": summary.loc["actual", "average"],
        "predicted_post_avg": summary.loc["predicted", "average"],
        "avg_effect": avg_effect,
        "cum_effect": cum_effect,
        "rel_effect": rel_effect,
        "p_value": p_value,
        "ci_obj": ci,  # keep for plotting
    }


def main():
    panel = load_panel()
    panel.to_csv(OUTPUT_DIR / "ti_849_panel.csv", index=False)
    print(f"[main] Panel saved to {OUTPUT_DIR / 'ti_849_panel.csv'}")

    rows = []
    for aid, name in TREATED_AIDS.items():
        for metric in METRICS:
            result = fit_causal_impact(panel, aid, metric)
            if "error" in result:
                print(f"  SKIP {name} ({aid}) {metric}: {result['error']}")
                continue

            # Plot
            fig = result["ci_obj"].plot()
            fig.savefig(OUTPUT_DIR / f"ti_849_ci_{aid}_{metric}.png", dpi=150)
            plt.close()

            # Strip non-serializable for table
            r = {k: v for k, v in result.items() if k != "ci_obj"}
            r["advertiser_name"] = name
            rows.append(r)
            print(f"  {name} ({aid}) {metric}: "
                  f"effect={r['rel_effect']:.1%}, p={r['p_value']:.3f}")

    if rows:
        results = pd.DataFrame(rows)
        results.to_csv(OUTPUT_DIR / "ti_849_method3_results.csv", index=False)
        print(f"\n[main] Results table saved to {OUTPUT_DIR / 'ti_849_method3_results.csv'}")
        print(results[["advertiser_name", "metric", "rel_effect", "p_value",
                       "n_controls", "post_n_days"]].to_string(index=False))
    else:
        print("\n[main] No results — likely no post-period data yet. Re-run after 2026-05-01.")


if __name__ == "__main__":
    main()
