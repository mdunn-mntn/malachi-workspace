# Databricks notebook source
# MAGIC %md
# MAGIC # Fangorn Lift Evaluation — Wave-Aware
# MAGIC
# MAGIC **Owner:** Alex Knorr (post-handoff). Original author: Malachi.
# MAGIC **Companion doc:** `alex_handoff.md` in this folder.
# MAGIC
# MAGIC This notebook produces, for every advertiser flipped to Fangorn (DS46),
# MAGIC two evaluations of KPI movement:
# MAGIC
# MAGIC 1. **Pre/post per AID** (Method 1) — descriptive movement vs the 30 days
# MAGIC    before that AID's flip date. Easy to read; not a lift claim.
# MAGIC 2. **CausalImpact synthetic control** (Method 2) — predicts what each
# MAGIC    advertiser's daily KPI WOULD have been without the flip, using
# MAGIC    non-treated advertisers as the platform covariate pool. Pattern
# MAGIC    inherited from TI-748 / TI-542 / TI-803 / TI-849.
# MAGIC
# MAGIC ## How to run
# MAGIC 1. Update `wave_config.csv` (in this folder) if new AIDs flipped.
# MAGIC 2. Attach this notebook to a cluster with the BigQuery connector or
# MAGIC    `google-cloud-bigquery` Python lib.
# MAGIC 3. Run all cells. Outputs land in `outputs/` (relative to this folder).
# MAGIC
# MAGIC ## Time / cost
# MAGIC - Daily panel pull: ~30s, ~5GB billed.
# MAGIC - CausalImpact fits: ~5s per (AID, metric). For 50 AIDs × 5 metrics, ~20 min.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------
# DBTITLE 1,Imports & config
import os
import sys
import warnings
from itertools import combinations
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

# CausalImpact: install once with `%pip install causalimpact` if not already present.
from causalimpact import CausalImpact

# BigQuery client. On Databricks, the workspace already has a BQ service
# account; locally, run `gcloud auth application-default login` once.
from google.cloud import bigquery

warnings.filterwarnings("ignore")

# Resolve paths so this notebook runs identically in Databricks and locally.
# Databricks: notebook lives in /Workspace/Repos/.../artifacts/databricks_fangorn_lift.py
# Local:      .../tickets/ti_921_fangorn_lift_dashboard/artifacts/databricks_fangorn_lift.py
NOTEBOOK_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
TICKET_DIR = NOTEBOOK_DIR.parent
WAVE_CONFIG_CSV = NOTEBOOK_DIR / "wave_config.csv"
DAILY_PANEL_SQL = TICKET_DIR / "queries" / "ti_921_daily_panel.sql"
OUTPUT_DIR = TICKET_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

BQ_PROJECT = "dw-main-bronze"

# CausalImpact fit minima — relaxed when post-period is short.
MIN_PRE_DAYS = 60
MIN_POST_DAYS = 1
MIN_DAILY_IMPRESSIONS = 1000

METRIC_DEFS = {
    "ivr":  {"direction": "higher", "label": "Impression-to-Visit Rate"},
    "cvr":  {"direction": "higher", "label": "Conversion Rate"},
    "roas": {"direction": "higher", "label": "Return on Ad Spend"},
    "cpa":  {"direction": "lower",  "label": "Cost per Acquisition"},
    "cpv":  {"direction": "lower",  "label": "Cost per Visit"},
}

# US holidays / known soft weeks in the panel period (TI-748 list).
HOLIDAY_DATES = pd.to_datetime([
    "2025-11-27", "2025-11-28",        # Thanksgiving
    "2025-12-24", "2025-12-25", "2025-12-26",   # Christmas
    "2025-12-31", "2026-01-01",        # New Year
    "2026-02-14",                       # Valentine's
    "2026-04-20",                       # Easter weekend
])

# Candidate covariates — VIF + BIC pick the winning subset per (AID, metric).
ALL_CANDIDATES = [
    "platform_ivr", "platform_cvr", "platform_vcr", "platform_roas", "platform_cpa",
    "platform_impressions", "platform_spend", "platform_active_advertisers",
    "platform_avg_cgs",
    "holiday",
    "metric_lag1", "metric_lag2",
    "spend_change_pct",
    "adv_active_cgs",
]

print(f"Notebook dir: {NOTEBOOK_DIR}")
print(f"Wave config:  {WAVE_CONFIG_CSV}")
print(f"Output dir:   {OUTPUT_DIR}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load wave config
# MAGIC
# MAGIC `wave_config.csv` is the manually-maintained source of truth for which
# MAGIC AIDs are flipped and when. Append rows there as new cohorts go live.

# COMMAND ----------
# DBTITLE 1,Load wave_config
wave = pd.read_csv(WAVE_CONFIG_CSV, parse_dates=["flip_date"])
wave["advertiser_id"] = wave["advertiser_id"].astype(int)
print(f"{len(wave)} treated AIDs loaded; {wave['cohort'].nunique()} cohort(s).")
print(wave.head(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Pull daily KPI panel from BigQuery
# MAGIC
# MAGIC We use the SQL file `queries/ti_921_daily_panel.sql` — keep this
# MAGIC notebook and that file in sync (the wave_config block in the SQL
# MAGIC mirrors the CSV).

# COMMAND ----------
# DBTITLE 1,Run BQ panel query

# Inject the wave_config CSV into the SQL at runtime so we don't have to
# keep two copies in sync. Replaces the inline `WITH wave_config AS (...)`
# block with one constructed from the CSV.
def build_wave_config_cte(wave_df: pd.DataFrame) -> str:
    rows = []
    for _, r in wave_df.iterrows():
        rows.append(
            f"  SELECT {int(r['advertiser_id'])} AS advertiser_id, "
            f"DATE '{r['flip_date'].strftime('%Y-%m-%d')}' AS flip_date, "
            f"'{r['cohort']}' AS cohort"
        )
    return "WITH wave_config AS (\n" + "\n  UNION ALL\n".join(rows) + "\n),\n"


def load_panel(wave_df: pd.DataFrame) -> pd.DataFrame:
    sql = DAILY_PANEL_SQL.read_text()
    # The SQL file's `WITH wave_config AS (...)` block goes through `,` to
    # `prospecting_campaigns AS (...)`. Replace from `WITH` to that boundary.
    new_cte = build_wave_config_cte(wave_df)
    head, sep, tail = sql.partition("prospecting_campaigns AS (")
    if not sep:
        raise RuntimeError("Couldn't find prospecting_campaigns CTE marker in SQL")
    sql_runtime = new_cte + sep + tail

    client = bigquery.Client(project=BQ_PROJECT)
    print("[load] Running daily-panel query against BigQuery...")
    df = client.query(sql_runtime).to_dataframe()
    df["day"] = pd.to_datetime(df["day"])
    df["flip_date"] = pd.to_datetime(df["flip_date"])
    for col in ["impressions", "uniques", "active_cgs", "vv", "conversions",
                "order_value", "spend", "vast_start", "vast_complete", "days_since_flip"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    print(
        f"[load] {len(df):,} rows | "
        f"treated AIDs: {df[df['aid_in_treatment_group']]['advertiser_id'].nunique()} | "
        f"non-treated: {df[~df['aid_in_treatment_group']]['advertiser_id'].nunique()} | "
        f"window {df['day'].min().date()} -> {df['day'].max().date()}"
    )
    return df


panel = load_panel(wave)
panel.to_csv(OUTPUT_DIR / "ti_921_panel.csv", index=False)
panel.head()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Method 1 — pre/post per AID
# MAGIC
# MAGIC Descriptive movement only. Anchored to each AID's `flip_date` so cohorts
# MAGIC that flipped on different days are computed correctly.

# COMMAND ----------
# DBTITLE 1,Per-AID pre/post

def compute_pre_post(panel: pd.DataFrame, wave_df: pd.DataFrame) -> pd.DataFrame:
    """For each treated AID, compute pre (30d before flip) vs post (flip+1..today) KPIs."""
    rows = []
    for _, w in wave_df.iterrows():
        aid = int(w["advertiser_id"])
        flip = w["flip_date"]
        pre_start = flip - pd.Timedelta(days=31)
        pre_end = flip - pd.Timedelta(days=1)
        post_start = flip + pd.Timedelta(days=1)
        post_end = panel["day"].max()

        adv = panel[panel["advertiser_id"] == aid]
        for label, lo, hi in [("pre", pre_start, pre_end), ("post", post_start, post_end)]:
            slice_ = adv[(adv["day"] >= lo) & (adv["day"] <= hi)]
            agg = slice_[["impressions", "uniques", "vv", "conversions",
                          "order_value", "spend"]].sum()
            rows.append({
                "advertiser_id": aid,
                "advertiser_name": w["advertiser_name"],
                "cohort": w["cohort"],
                "flip_date": flip,
                "period": label,
                "period_days": (hi - lo).days + 1,
                **agg.to_dict(),
            })

    df = pd.DataFrame(rows)
    df["ivr"]  = df["vv"] / df["impressions"].replace(0, np.nan)
    df["vvr"]  = df["vv"] / df["uniques"].replace(0, np.nan)
    df["cvr"]  = df["conversions"] / df["vv"].replace(0, np.nan)
    df["roas"] = df["order_value"] / df["spend"].replace(0, np.nan)
    df["cpv"]  = df["spend"] / df["vv"].replace(0, np.nan)
    df["cpa"]  = df["spend"] / df["conversions"].replace(0, np.nan)
    df["aov"]  = df["order_value"] / df["conversions"].replace(0, np.nan)

    # Pivot to one row per AID with pre / post / pct_change for each metric.
    out = []
    for aid, sub in df.groupby("advertiser_id"):
        pre_row = sub[sub["period"] == "pre"].iloc[0]
        post_row = sub[sub["period"] == "post"].iloc[0]
        record = {
            "advertiser_id": aid,
            "advertiser_name": pre_row["advertiser_name"],
            "cohort": pre_row["cohort"],
            "flip_date": pre_row["flip_date"],
            "pre_days": int(pre_row["period_days"]),
            "post_days": int(post_row["period_days"]),
        }
        for m in ["impressions", "vv", "conversions", "spend", "order_value",
                  "ivr", "vvr", "cvr", "roas", "cpv", "cpa", "aov"]:
            pre_val = pre_row[m]
            post_val = post_row[m]
            record[f"{m}_pre"] = pre_val
            record[f"{m}_post"] = post_val
            record[f"{m}_pct_change"] = (
                (post_val - pre_val) / pre_val if pre_val and pre_val != 0 else np.nan
            )
        out.append(record)

    return pd.DataFrame(out).sort_values(["cohort", "advertiser_id"])


pre_post = compute_pre_post(panel, wave)
pre_post.to_csv(OUTPUT_DIR / "ti_921_pre_post.csv", index=False)
print(f"Wrote {OUTPUT_DIR / 'ti_921_pre_post.csv'}")
pre_post[["advertiser_name", "cohort", "post_days",
          "ivr_pre", "ivr_post", "ivr_pct_change",
          "cvr_pre", "cvr_post", "cvr_pct_change"]]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Method 2 — CausalImpact per (AID, metric)
# MAGIC
# MAGIC For each treated AID and each metric, fit a Bayesian structural time-series
# MAGIC model on the pre-period using non-treated advertisers as platform covariates,
# MAGIC plus holiday/lag/spend covariates. Predict the post-period counterfactual
# MAGIC and compare to actuals.
# MAGIC
# MAGIC Pipeline (per (AID, metric)):
# MAGIC 1. Build feature frame (target metric + all candidate covariates).
# MAGIC 2. VIF-drop high-collinearity covariates.
# MAGIC 3. BIC-pick the best subset (max size 5).
# MAGIC 4. Fit CausalImpact, save plot + summary stats.

# COMMAND ----------
# DBTITLE 1,Helpers — metric computation, platform aggregation, VIF, BIC

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add per-row rate metrics; winsorize 1-99 percentile to dampen extremes."""
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
    Excludes any AID that's *ever* treated (aid_in_treatment_group=True),
    so platform covariates aren't contaminated by treated AIDs' pre-flip data.
    """
    base = panel[~panel["aid_in_treatment_group"]].copy()
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

    plat["platform_ivr"]  = plat["platform_vv"]          / plat["platform_impressions"].replace(0, np.nan)
    plat["platform_cvr"]  = plat["platform_conversions"] / plat["platform_vv"].replace(0, np.nan)
    plat["platform_vcr"]  = plat["platform_vast_complete"] / plat["platform_vast_start"].replace(0, np.nan)
    plat["platform_roas"] = plat["platform_order_value"] / plat["platform_spend"].replace(0, np.nan)
    plat["platform_cpa"]  = plat["platform_spend"]       / plat["platform_conversions"].replace(0, np.nan)
    plat["holiday"] = plat["day"].isin(HOLIDAY_DATES).astype(float)

    # Numerical-stability scaling.
    plat["platform_spend"] /= 1e6
    plat["platform_impressions"] /= 1e9
    plat["platform_active_advertisers"] /= 1000.0
    return plat


def drop_high_vif(features: pd.DataFrame, vif_threshold: float = 10.0) -> list:
    """Iteratively drop the highest-VIF covariate until all are below threshold."""
    keep = list(features.columns)
    while len(keep) > 1:
        X = features[keep].fillna(0.0)
        X_const = sm.add_constant(X, has_constant="add")
        try:
            vifs = [variance_inflation_factor(X_const.values, i + 1) for i in range(len(keep))]
        except Exception:
            break
        max_v = max(vifs)
        if max_v < vif_threshold:
            break
        worst = keep[vifs.index(max_v)]
        keep.remove(worst)
    return keep


def best_subset_by_bic(target: pd.Series, features: pd.DataFrame, max_size: int = 5) -> list:
    """Search subsets of covariates up to size max_size, return BIC-optimal one."""
    cols = list(features.columns)
    best_bic = np.inf
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
    return best_subset

# COMMAND ----------
# DBTITLE 1,Per-AID prep & one-fit driver

def prepare_advertiser(adv_id: int, flip_date: pd.Timestamp,
                       panel: pd.DataFrame, plat: pd.DataFrame, metric: str):
    """Build the per-(AID, metric) feature frame. Returns (df, pre_period, post_period) or (None, None, None)."""
    adv = panel[panel["advertiser_id"] == adv_id].copy()
    adv = compute_metrics(adv)
    adv["adv_active_cgs"] = adv["active_cgs"].astype(float)

    df = adv.merge(plat, on="day", how="inner").sort_values("day")
    df["metric_lag1"] = df[metric].shift(1)
    df["metric_lag2"] = df[metric].shift(2)
    df["spend_change_pct"] = df["spend"].pct_change().fillna(0).clip(-1, 5)
    df = df.dropna(subset=["metric_lag1", "metric_lag2"]).set_index("day").sort_index()

    pre = df[df.index < flip_date]
    post = df[df.index > flip_date]   # strictly > flip_date — flip day already excluded in SQL
    if len(pre) < MIN_PRE_DAYS or len(post) < MIN_POST_DAYS:
        return None, None, None
    return df, [pre.index[0].strftime("%Y-%m-%d"), pre.index[-1].strftime("%Y-%m-%d")], \
              [post.index[0].strftime("%Y-%m-%d"), post.index[-1].strftime("%Y-%m-%d")]


def fit_one(panel: pd.DataFrame, plat: pd.DataFrame, adv_id: int, adv_name: str,
            cohort: str, flip_date: pd.Timestamp, metric: str) -> Optional[dict]:
    df, pre_period, post_period = prepare_advertiser(adv_id, flip_date, panel, plat, metric)
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

    print(f"[fit] {adv_name} ({adv_id}) {metric}: pre={pre_period} post={post_period} cov={winning}")
    ci = CausalImpact(ci_data, pre_period, post_period)
    s = ci.summary_data
    avg_actual = s.loc["actual", "average"]
    avg_pred = s.loc["predicted", "average"]
    abs_eff = avg_actual - avg_pred
    rel_eff = abs_eff / avg_pred if avg_pred else np.nan

    cum_lower = s.loc["predicted_lower", "cumulative"]
    cum_upper = s.loc["predicted_upper", "cumulative"]
    cum_actual = s.loc["actual", "cumulative"]
    cum_eff_lower = cum_actual - cum_upper
    cum_eff_upper = cum_actual - cum_lower

    fig = ci.plot()
    fig.savefig(OUTPUT_DIR / f"ti_921_ci_{adv_id}_{metric}.png", dpi=150,
                bbox_inches="tight")
    plt.close()

    return {
        "advertiser_id": adv_id,
        "advertiser_name": adv_name,
        "cohort": cohort,
        "flip_date": flip_date,
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

# COMMAND ----------
# DBTITLE 1,Run CausalImpact for every (AID, metric)

plat = build_platform_covariates(panel)

ci_rows = []
for _, w in wave.iterrows():
    aid = int(w["advertiser_id"])
    name = w["advertiser_name"]
    cohort = w["cohort"]
    flip = w["flip_date"]
    for metric in METRIC_DEFS:
        try:
            r = fit_one(panel, plat, aid, name, cohort, flip, metric)
        except Exception as e:
            print(f"  ERROR fitting {name} ({aid}) {metric}: {e}")
            continue
        if r is None:
            print(f"  SKIP {name} ({aid}) {metric}: insufficient pre/post days")
            continue
        ci_rows.append(r)
        print(f"  -> {name} {metric}: rel_eff={r['rel_effect']:+.2%} "
              f"p={r['p_value']:.3f} post_n_days={r['post_n_days']}")

ci_results = pd.DataFrame(ci_rows)
if not ci_results.empty:
    ci_results.to_csv(OUTPUT_DIR / "ti_921_ci_results.csv", index=False)
    print(f"\nResults -> {OUTPUT_DIR / 'ti_921_ci_results.csv'}")
ci_results

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Combined readout

# COMMAND ----------
# DBTITLE 1,Combined view: pre/post + CausalImpact side by side

if not ci_results.empty:
    ci_pivot = ci_results.pivot_table(
        index=["advertiser_id", "advertiser_name", "cohort"],
        columns="metric",
        values=["rel_effect", "p_value", "post_n_days"],
    )
    ci_pivot.columns = ['_'.join(map(str, c)).strip() for c in ci_pivot.columns]
    ci_pivot = ci_pivot.reset_index()
else:
    ci_pivot = pd.DataFrame()

combined = pre_post.merge(
    ci_pivot,
    on=["advertiser_id", "advertiser_name", "cohort"],
    how="left",
)
combined.to_csv(OUTPUT_DIR / "ti_921_combined.csv", index=False)
print(f"Wrote {OUTPUT_DIR / 'ti_921_combined.csv'}")
combined

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. What to share
# MAGIC
# MAGIC - **Slack/Jira summary:** point at the rows in `ti_921_combined.csv` and
# MAGIC   call out anything where pre/post movement is >20% AND CausalImpact
# MAGIC   p-value < 0.10. Either condition alone is suggestive; both together
# MAGIC   is signal.
# MAGIC - **Per-advertiser plots:** the CausalImpact `*.png` files in `outputs/`
# MAGIC   show actual vs counterfactual + cumulative effect. These are what
# MAGIC   leadership wants to see.
# MAGIC - **Final readout per cohort:** when a cohort hits 4 weeks post-flip
# MAGIC   (TI-780 maturity rule), do a one-pager: pre/post table, three CI plots
# MAGIC   for the most-moved metrics, two-sentence interpretation, list of
# MAGIC   advertisers in the cohort.
