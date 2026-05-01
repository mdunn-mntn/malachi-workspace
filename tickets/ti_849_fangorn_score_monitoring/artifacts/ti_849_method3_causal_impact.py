"""
TI-849 Method 3 — CausalImpact synthetic control per Fangorn advertiser

Pattern: TI-748 / TI-542 / TI-803 — covariate-validated CausalImpact.

For each Fangorn-flipped advertiser, predict what their daily KPIs (IVR,
CVR, ROAS, CPA, CPV) would have been WITHOUT the flip, using:
  - Platform covariates: aggregate of non-Fangorn advertisers' daily KPIs
    (platform_ivr, platform_cvr, platform_vcr, platform_roas, platform_cpa,
     platform_impressions, platform_spend, platform_active_advertisers)
  - Holiday week indicator
  - Lagged metric (lag1, lag2)
  - Spend change pct
  - Advertiser-specific: active campaign-groups

Covariate selection: VIF (multicollinearity drop) → BIC (model fit) →
CausalImpact with the winning subset.

Granularity: DAILY. Reason: D+7 review window means weekly grain gives 1
post observation, which is too thin. Daily gives 7 post observations and
honest credible intervals.

NOT a within-AID DiD — that approach (TI-835 holdout hash + augmentor_log)
is too data-heavy to run daily. CausalImpact's covariates absorb the
spend/seasonality confounds the user flagged.

Usage:
    python ti_849_method3_causal_impact.py

Pre-requisites (one-time):
    pip install causalimpact google-cloud-bigquery pandas numpy \\
                matplotlib statsmodels
"""

from __future__ import annotations

import sys
import warnings
from itertools import combinations
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from google.cloud import bigquery
from statsmodels.stats.outliers_influence import variance_inflation_factor

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
TREATMENT_DATE = pd.Timestamp("2026-05-01")  # vertical_data_source=46 flip

# Per-AID minimums for fitting (relaxed vs TI-748 since post-period is short)
MIN_PRE_DAYS = 60
MIN_POST_DAYS = 1   # accept even D+1 reads; CrI will reflect uncertainty
MIN_DAILY_IMPRESSIONS = 1000

# Metrics to fit (one CI model per metric per treated AID)
METRIC_DEFS = {
    "ivr":  {"direction": "higher", "label": "Impression-to-Visit Rate"},
    "cvr":  {"direction": "higher", "label": "Conversion Rate"},
    "roas": {"direction": "higher", "label": "Return on Ad Spend"},
    "cpa":  {"direction": "lower",  "label": "Cost per Acquisition"},
    "cpv":  {"direction": "lower",  "label": "Cost per Visit"},
}

# US holidays / known soft weeks in the panel period (TI-748 list, extended)
HOLIDAY_DATES = pd.to_datetime([
    "2025-11-27", "2025-11-28",  # Thanksgiving
    "2025-12-24", "2025-12-25", "2025-12-26",  # Christmas
    "2025-12-31", "2026-01-01",  # New Year
    "2026-02-14",  # Valentine's
    "2026-04-20",  # Easter weekend
])

# All candidate covariates — VIF + BIC will pick the winners per (AID, metric)
ALL_CANDIDATES = [
    "platform_ivr", "platform_cvr", "platform_vcr", "platform_roas", "platform_cpa",
    "platform_impressions", "platform_spend", "platform_active_advertisers",
    "platform_avg_cgs",
    "holiday",
    "metric_lag1", "metric_lag2",
    "spend_change_pct",
    "adv_active_cgs",
]


# =============================================================================
# DATA LOADING
# =============================================================================

def load_panel() -> pd.DataFrame:
    """Pull the daily KPI panel from BQ — all active prospecting AIDs."""
    client = bigquery.Client(project=BQ_PROJECT)
    sql = COVARIATE_SQL.read_text()
    print(f"[load] Running daily-panel query against {BQ_PROJECT}...")
    df = client.query(sql).to_dataframe()
    df["day"] = pd.to_datetime(df["day"])
    for col in ["impressions", "uniques", "active_cgs", "vv", "conversions",
                "order_value", "spend", "vast_start", "vast_complete"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    print(f"[load] {len(df):,} rows | "
          f"treated AIDs: {df[df['is_treated']]['advertiser_id'].nunique()} | "
          f"non-treated: {df[~df['is_treated']]['advertiser_id'].nunique()} | "
          f"window {df['day'].min().date()} → {df['day'].max().date()}")
    return df


# =============================================================================
# COVARIATE BUILDING
# =============================================================================

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add ivr/cvr/roas/cpa/cpv/vcr columns (winsorized to 1-99 percentile)."""
    out = df.copy()
    out["ivr"]  = out["vv"]          / out["impressions"].replace(0, np.nan)
    out["cvr"]  = out["conversions"] / out["vv"].replace(0, np.nan)
    out["roas"] = out["order_value"] / out["spend"].replace(0, np.nan)
    out["cpa"]  = out["spend"]       / out["conversions"].replace(0, np.nan)
    out["cpv"]  = out["spend"]       / out["vv"].replace(0, np.nan)
    out["vcr"]  = out["vast_complete"] / out["vast_start"].replace(0, np.nan)
    for m in ["ivr", "cvr", "roas", "cpa", "cpv", "vcr"]:
        vals = out[m].dropna()
        if len(vals) > 2:
            lo, hi = np.nanpercentile(vals, [1, 99])
            out[m] = out[m].clip(lower=lo, upper=hi)
    return out


def build_platform_covariates(panel: pd.DataFrame) -> pd.DataFrame:
    """Aggregate non-treated advertisers into daily platform covariates.

    Platform covariates are the sum/mean of all non-Fangorn AIDs' daily
    KPIs — they absorb secular trends (seasonality, spend swings, supply
    shifts) that would otherwise leak into the treated-AID series.
    """
    base = panel[~panel["is_treated"]].copy()
    base = base[base["impressions"] >= MIN_DAILY_IMPRESSIONS]

    plat = base.groupby("day").agg(
        platform_impressions=("impressions", "sum"),
        platform_uniques=("uniques", "sum"),
        platform_vv=("vv", "sum"),
        platform_conversions=("conversions", "sum"),
        platform_order_value=("order_value", "sum"),
        platform_spend=("spend", "sum"),
        platform_vast_start=("vast_start", "sum"),
        platform_vast_complete=("vast_complete", "sum"),
        platform_active_advertisers=("advertiser_id", "nunique"),
        platform_avg_cgs=("active_cgs", "mean"),
    ).reset_index()

    # Derived rates
    plat["platform_ivr"]  = plat["platform_vv"]          / plat["platform_impressions"].replace(0, np.nan)
    plat["platform_cvr"]  = plat["platform_conversions"] / plat["platform_vv"].replace(0, np.nan)
    plat["platform_vcr"]  = plat["platform_vast_complete"] / plat["platform_vast_start"].replace(0, np.nan)
    plat["platform_roas"] = plat["platform_order_value"] / plat["platform_spend"].replace(0, np.nan)
    plat["platform_cpa"]  = plat["platform_spend"]       / plat["platform_conversions"].replace(0, np.nan)
    plat["holiday"] = plat["day"].isin(HOLIDAY_DATES).astype(float)

    # Numerical-stability scaling
    plat["platform_spend"] /= 1e6
    plat["platform_impressions"] /= 1e9
    plat["platform_active_advertisers"] /= 1000.0

    return plat


def prepare_advertiser(adv_id: int, panel: pd.DataFrame, plat: pd.DataFrame,
                        metric: str) -> tuple[pd.DataFrame, list, list] | tuple[None, None, None]:
    """Build the per-AID feature frame: target metric + all candidate covariates."""
    adv = panel[panel["advertiser_id"] == adv_id].copy()
    adv = compute_metrics(adv)
    adv["adv_active_cgs"] = adv["active_cgs"].astype(float)

    df = adv.merge(plat, on="day", how="inner").sort_values("day")
    df["metric_lag1"] = df[metric].shift(1)
    df["metric_lag2"] = df[metric].shift(2)
    df["spend_change_pct"] = df["spend"].pct_change().fillna(0).clip(-1, 5)
    df = df.dropna(subset=["metric_lag1", "metric_lag2"]).set_index("day").sort_index()

    pre = df[df.index < TREATMENT_DATE]
    post = df[df.index >= TREATMENT_DATE]
    if len(pre) < MIN_PRE_DAYS or len(post) < MIN_POST_DAYS:
        return None, None, None
    return df, [pre.index[0].strftime("%Y-%m-%d"), pre.index[-1].strftime("%Y-%m-%d")], \
              [post.index[0].strftime("%Y-%m-%d"), post.index[-1].strftime("%Y-%m-%d")]


# =============================================================================
# COVARIATE SELECTION (VIF + BIC)
# =============================================================================

def drop_high_vif(features: pd.DataFrame, vif_threshold: float = 10.0) -> list[str]:
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
                        max_size: int = 5) -> list[str]:
    """Search subsets of covariates up to size max_size, return the BIC-optimal one."""
    cols = list(features.columns)
    best_bic = np.inf
    best_subset: list[str] = []
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
    return best_subset


# =============================================================================
# CAUSAL IMPACT FIT
# =============================================================================

def fit_one(panel: pd.DataFrame, plat: pd.DataFrame, adv_id: int, adv_name: str,
            metric: str) -> dict | None:
    df, pre_period, post_period = prepare_advertiser(adv_id, panel, plat, metric)
    if df is None:
        return None

    candidates = [c for c in ALL_CANDIDATES if c in df.columns]
    pre_df = df.loc[pre_period[0]:pre_period[1]]
    feats = pre_df[candidates].fillna(0.0)
    target = pre_df[metric].fillna(method="ffill").fillna(method="bfill")

    keep = drop_high_vif(feats)
    winning = best_subset_by_bic(target, feats[keep])
    if not winning:
        winning = keep[:3]   # safety fallback

    ci_data = pd.DataFrame({"y": df[metric].fillna(method="ffill").fillna(method="bfill")})
    ci_data = ci_data.join(df[winning].fillna(0.0))

    print(f"[fit] {adv_name} ({adv_id}) {metric}: "
          f"pre={pre_period} post={post_period} covariates={winning}")

    ci = CausalImpact(ci_data, pre_period, post_period)
    s = ci.summary_data
    avg_actual = s.loc["actual", "average"]
    avg_pred = s.loc["predicted", "average"]
    abs_eff = avg_actual - avg_pred
    rel_eff = abs_eff / avg_pred if avg_pred else np.nan

    # 95% CrI on cumulative effect
    cum_lower = s.loc["predicted_lower", "cumulative"]
    cum_upper = s.loc["predicted_upper", "cumulative"]
    cum_actual = s.loc["actual", "cumulative"]
    cum_eff_lower = cum_actual - cum_upper
    cum_eff_upper = cum_actual - cum_lower

    fig = ci.plot()
    fig.savefig(OUTPUT_DIR / f"ti_849_ci_{adv_id}_{metric}.png", dpi=150,
                bbox_inches="tight")
    plt.close()

    return {
        "advertiser_id": adv_id,
        "advertiser_name": adv_name,
        "metric": metric,
        "pre_n_days": (pd.Timestamp(pre_period[1]) - pd.Timestamp(pre_period[0])).days + 1,
        "post_n_days": (pd.Timestamp(post_period[1]) - pd.Timestamp(post_period[0])).days + 1,
        "covariates": ",".join(winning),
        "avg_actual_post": avg_actual,
        "avg_predicted_post": avg_pred,
        "abs_effect": abs_eff,
        "rel_effect": rel_eff,
        "cum_effect_95_lower": cum_eff_lower,
        "cum_effect_95_upper": cum_eff_upper,
        "p_value": ci.p_value,
    }


# =============================================================================
# DRIVER
# =============================================================================

def main():
    panel = load_panel()
    panel.to_csv(OUTPUT_DIR / "ti_849_panel.csv", index=False)

    treated = (panel[panel["is_treated"]]
               [["advertiser_id", "company_name"]].drop_duplicates())
    if treated.empty:
        print("[main] No treated AIDs found. Has the rollout flipped any?")
        return

    plat = build_platform_covariates(panel)

    rows = []
    for aid, name in zip(treated["advertiser_id"], treated["company_name"]):
        for metric in METRIC_DEFS:
            try:
                r = fit_one(panel, plat, int(aid), name, metric)
            except Exception as e:
                print(f"  ERROR fitting {name} ({aid}) {metric}: {e}")
                continue
            if r is None:
                continue
            rows.append(r)
            print(f"  → {name} {metric}: rel_eff={r['rel_effect']:+.2%} "
                  f"p={r['p_value']:.3f} post_n_days={r['post_n_days']}")

    if rows:
        results = pd.DataFrame(rows)
        results.to_csv(OUTPUT_DIR / "ti_849_method3_results.csv", index=False)
        print(f"\nResults → {OUTPUT_DIR / 'ti_849_method3_results.csv'}")
    else:
        print("\nNo fits produced. Likely no post-period yet (post starts 2026-05-01).")


if __name__ == "__main__":
    main()
