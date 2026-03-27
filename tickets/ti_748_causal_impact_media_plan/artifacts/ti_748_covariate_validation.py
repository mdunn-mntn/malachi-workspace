"""
TI-748: Covariate Validation & Model Diagnostics
==================================================

Systematic testing of covariate selection for CausalImpact.

Steps:
  1. Load all candidate covariates (platform + advertiser-specific)
  2. VIF multicollinearity check — drop covariates with VIF > 10
  3. Stepwise model comparison — AIC/BIC for each covariate combination
  4. Cross-validation — hold-out last N pre-period weeks, test prediction accuracy
  5. Run CausalImpact with winning covariate set
  6. Sensitivity analysis — vary pre-period length
  7. Placebo tests with final model
  8. All-metrics analysis

Output: prints structured results for each step.
"""

import warnings
from datetime import date
from itertools import combinations

import numpy as np
import pandas as pd
from google.cloud import bigquery
from causalimpact import CausalImpact
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

warnings.filterwarnings("ignore")

BQ_PROJECT = "dw-main-silver"
MIN_WEEKLY_IMPRESSIONS = 1000
MIN_TOTAL_POST_SPEND = 10_000
MIN_PRE_WEEKS = 20
MIN_POST_WEEKS = 4

HOLIDAY_WEEKS = {
    pd.Timestamp("2024-11-25"): 1, pd.Timestamp("2024-12-23"): 1, pd.Timestamp("2024-12-30"): 1,
    pd.Timestamp("2025-11-24"): 1, pd.Timestamp("2025-12-22"): 1, pd.Timestamp("2025-12-29"): 1,
    pd.Timestamp("2026-02-02"): 1,
}

METRIC_DEFS = {
    "ivr":  {"direction": "higher", "label": "Impression-to-Visit Rate"},
    "cvr":  {"direction": "higher", "label": "Conversion Rate"},
    "cpa":  {"direction": "lower",  "label": "Cost per Acquisition"},
    "cpv":  {"direction": "lower",  "label": "Cost per Visit"},
    "roas": {"direction": "higher", "label": "Return on Ad Spend"},
}


# =============================================================================
# DATA LOADING (same as main script but with additional covariate candidates)
# =============================================================================

def load_all_data():
    """Load adopters, weekly KPIs, and build all candidate covariates."""
    client = bigquery.Client(project=BQ_PROJECT)

    # --- Adopters ---
    adopters = client.query("""
    WITH plan_status AS (
        SELECT mp.media_plan_id, mp.advertiser_id, mp.campaign_group_id, mp.create_time,
               LOGICAL_AND(mpp.badge_state = 'RECOMMENDED') AS all_recommended
        FROM `dw-main-silver.core.media_plan` mp
        JOIN `dw-main-silver.core.media_plan_publishers` mpp ON mpp.media_plan_id = mp.media_plan_id
        WHERE mp.media_plan_status_id = 3
        GROUP BY 1, 2, 3, 4
    )
    SELECT ps.advertiser_id, adv.company_name,
        MIN(CASE WHEN ps.all_recommended THEN ps.create_time END) AS first_recommended_plan,
        COUNT(*) AS total_plans, COUNTIF(ps.all_recommended) AS recommended_plans
    FROM plan_status ps
    JOIN `dw-main-bronze.integrationprod.advertisers` adv ON adv.advertiser_id = ps.advertiser_id
    GROUP BY 1, 2 HAVING COUNTIF(ps.all_recommended) > 0 ORDER BY 3
    """).to_dataframe()
    adopters["first_recommended_plan"] = pd.to_datetime(adopters["first_recommended_plan"])
    adopters["intervention_date"] = adopters["first_recommended_plan"].dt.date
    print(f"Adopters: {len(adopters)}")

    # --- Weekly KPIs with extra fields for covariate candidates ---
    adopter_list = ",".join(str(x) for x in adopters["advertiser_id"])
    weekly_kpis = client.query(f"""
    WITH prospecting AS (
        SELECT DISTINCT c.campaign_id, c.advertiser_id, c.campaign_group_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.funnel_level = 1 AND c.deleted = FALSE AND c.is_test = FALSE
    )
    SELECT pc.advertiser_id, pc.campaign_group_id,
        DATE_TRUNC(s.day, WEEK(MONDAY)) AS week_start,
        CASE WHEN pc.advertiser_id IN ({adopter_list}) THEN TRUE ELSE FALSE END AS is_adopter,
        SUM(s.impressions) AS impressions,
        SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend,
        SUM(s.clicks + s.views + COALESCE(s.competing_views, 0)) AS vv,
        SUM(s.click_conversions + s.view_conversions + COALESCE(s.competing_view_conversions, 0)) AS conversions,
        SUM(s.click_order_value + s.view_order_value + COALESCE(s.competing_view_order_value, 0)) AS order_value,
        SUM(s.vast_start) AS vast_start, SUM(s.vast_complete) AS vast_complete
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN prospecting pc ON pc.campaign_id = s.campaign_id
    WHERE s.day >= '2024-01-01' AND s.impressions > 0
    GROUP BY 1, 2, 3, 4 ORDER BY 1, 3
    """).to_dataframe()

    weekly_kpis["week_start"] = pd.to_datetime(weekly_kpis["week_start"])
    for c in ["impressions", "spend", "vv", "conversions", "order_value", "vast_start", "vast_complete"]:
        weekly_kpis[c] = pd.to_numeric(weekly_kpis[c], errors="coerce").astype(float)
    weekly_kpis = weekly_kpis[weekly_kpis["impressions"] >= MIN_WEEKLY_IMPRESSIONS].copy()
    print(f"Weekly KPIs: {len(weekly_kpis):,} rows, {weekly_kpis['advertiser_id'].nunique()} advertisers")
    print(f"Date range: {weekly_kpis['week_start'].min().date()} to {weekly_kpis['week_start'].max().date()}")

    return adopters, weekly_kpis


def build_all_covariates(weekly_kpis):
    """Build comprehensive set of candidate covariates."""
    # Advertiser-week aggregation
    adv_week = weekly_kpis.groupby(["advertiser_id", "week_start", "is_adopter"]).agg(
        impressions=("impressions", "sum"), spend=("spend", "sum"), vv=("vv", "sum"),
        conversions=("conversions", "sum"), order_value=("order_value", "sum"),
        vast_start=("vast_start", "sum"), vast_complete=("vast_complete", "sum"),
        active_campaign_groups=("campaign_group_id", "nunique"),
    ).reset_index()

    # Platform covariates from non-adopters
    non_adopter = adv_week[~adv_week["is_adopter"]]
    platform = non_adopter.groupby("week_start").agg(
        platform_impressions=("impressions", "sum"),
        platform_spend=("spend", "sum"),
        platform_vv=("vv", "sum"),
        platform_conversions=("conversions", "sum"),
        platform_order_value=("order_value", "sum"),
        platform_vast_start=("vast_start", "sum"),
        platform_vast_complete=("vast_complete", "sum"),
        platform_active_advertisers=("advertiser_id", "nunique"),
        platform_avg_campaign_groups=("active_campaign_groups", "mean"),
    ).reset_index()

    # Derived platform metrics
    platform["platform_ivr"] = platform["platform_vv"] / platform["platform_impressions"].replace(0, np.nan)
    platform["platform_cvr"] = platform["platform_conversions"] / platform["platform_vv"].replace(0, np.nan)
    platform["platform_vcr"] = platform["platform_vast_complete"] / platform["platform_vast_start"].replace(0, np.nan)
    platform["platform_roas"] = platform["platform_order_value"] / platform["platform_spend"].replace(0, np.nan)
    platform["platform_cpa"] = platform["platform_spend"] / platform["platform_conversions"].replace(0, np.nan)
    platform["holiday"] = platform["week_start"].map(lambda w: HOLIDAY_WEEKS.get(w, 0.0))

    # Scale for numerical stability
    platform["platform_spend"] /= 1e6
    platform["platform_impressions"] /= 1e9
    platform["platform_active_advertisers"] /= 1000.0
    platform["platform_avg_campaign_groups"] /= 10.0

    return adv_week, platform


def compute_metrics(df):
    df = df.copy()
    df["ivr"] = df["vv"] / df["impressions"].replace(0, np.nan)
    df["cvr"] = df["conversions"] / df["vv"].replace(0, np.nan)
    df["cpa"] = df["spend"] / df["conversions"].replace(0, np.nan)
    df["cpv"] = df["spend"] / df["vv"].replace(0, np.nan)
    df["roas"] = df["order_value"] / df["spend"].replace(0, np.nan)
    for m in ["ivr", "cvr", "cpa", "cpv", "roas"]:
        vals = df[m].dropna()
        if len(vals) > 2:
            pcts = np.nanpercentile(vals, [1, 99])
            df[m] = df[m].clip(lower=pcts[0], upper=pcts[1])
    return df


def prepare_advertiser(adv_id, intervention_date, adv_week, platform, metric):
    """Prepare single advertiser data with ALL candidate covariates."""
    data = adv_week[adv_week["advertiser_id"] == adv_id].copy()
    data = compute_metrics(data)

    # advertiser-specific covariates
    data["adv_active_cgs"] = data["active_campaign_groups"].astype(float)

    data = data.merge(platform, on="week_start", how="inner")
    data = data.sort_values("week_start")

    # lagged metrics
    data["metric_lag1"] = data[metric].shift(1)
    data["metric_lag2"] = data[metric].shift(2)
    if len(data) > 1:
        data["spend_change_pct"] = data["spend"].pct_change().fillna(0).clip(-1, 5)
    else:
        data["spend_change_pct"] = 0.0
    data = data.dropna(subset=["metric_lag1", "metric_lag2"]).set_index("week_start").sort_index()

    int_ts = pd.Timestamp(intervention_date)
    int_week = int_ts - pd.Timedelta(days=int_ts.weekday())
    pre = data[data.index < int_week]
    post = data[data.index >= int_week]

    if len(pre) < MIN_PRE_WEEKS or len(post) < MIN_POST_WEEKS:
        return None, None, None
    if post["spend"].sum() < MIN_TOTAL_POST_SPEND:
        return None, None, None

    expected_weeks = pd.date_range(data.index.min(), data.index.max(), freq="W-MON")
    if len(expected_weeks) - len(data) > len(data) * 0.2:
        return None, None, None

    return data, [pre.index[0], pre.index[-1]], [post.index[0], post.index[-1]]


# =============================================================================
# STEP 1: VIF MULTICOLLINEARITY CHECK
# =============================================================================

def run_vif_check(data, candidates):
    """Check VIF for each candidate covariate. VIF > 10 = multicollinearity problem."""
    print("\n" + "=" * 80)
    print("STEP 1: VIF MULTICOLLINEARITY CHECK")
    print("=" * 80)

    available = [c for c in candidates if c in data.columns]
    clean = data[available].dropna().astype(float)

    if len(clean) < 10:
        print("Insufficient data for VIF check")
        return available

    results = []
    for i, col in enumerate(available):
        try:
            vif = variance_inflation_factor(clean.values, i)
            results.append({"covariate": col, "VIF": vif})
        except Exception:
            results.append({"covariate": col, "VIF": np.nan})

    vif_df = pd.DataFrame(results).sort_values("VIF", ascending=False)
    print(vif_df.to_string(index=False))

    # flag high VIF
    high_vif = vif_df[vif_df["VIF"] > 10]["covariate"].tolist()
    if high_vif:
        print(f"\nHigh VIF (>10): {high_vif}")
        print("These covariates are collinear — will iteratively remove worst offender.")
    else:
        print("\nAll covariates pass VIF check (<10)")

    # iteratively remove highest VIF until all < 10
    keep = available.copy()
    while True:
        clean = data[keep].dropna().astype(float)
        if len(clean) < 10 or len(keep) <= 1:
            break
        vifs = []
        for i, col in enumerate(keep):
            try:
                vifs.append(variance_inflation_factor(clean.values, i))
            except Exception:
                vifs.append(0)
        max_vif = max(vifs)
        if max_vif <= 10:
            break
        worst = keep[vifs.index(max_vif)]
        print(f"  Removing {worst} (VIF={max_vif:.1f})")
        keep.remove(worst)

    print(f"\nFinal VIF-clean covariates: {keep}")
    return keep


# =============================================================================
# STEP 2: STEPWISE MODEL COMPARISON (AIC/BIC)
# =============================================================================

def run_stepwise_selection(data, metric, vif_clean_covariates, pre_period):
    """Compare model fits with different covariate subsets using AIC/BIC."""
    print("\n" + "=" * 80)
    print("STEP 2: STEPWISE COVARIATE SELECTION (AIC/BIC)")
    print("=" * 80)

    pre_data = data.loc[pre_period[0]:pre_period[1]].copy()
    y = pre_data[metric].dropna()

    results = []

    # test all subsets of size 1 to len(covariates)
    for size in range(1, min(len(vif_clean_covariates) + 1, 8)):
        for combo in combinations(vif_clean_covariates, size):
            combo_list = list(combo)
            X = pre_data[combo_list].reindex(y.index).dropna()
            common_idx = y.index.intersection(X.index)
            if len(common_idx) < 20:
                continue

            y_sub = y.loc[common_idx]
            X_sub = sm.add_constant(X.loc[common_idx])

            try:
                model = sm.OLS(y_sub, X_sub).fit()
                results.append({
                    "covariates": ", ".join(combo_list),
                    "n_covariates": len(combo_list),
                    "AIC": model.aic,
                    "BIC": model.bic,
                    "R2_adj": model.rsquared_adj,
                    "n_obs": len(common_idx),
                })
            except Exception:
                pass

    if not results:
        print("No valid models found")
        return vif_clean_covariates

    results_df = pd.DataFrame(results).sort_values("BIC")
    print("\nTop 10 models by BIC:")
    print(results_df.head(10).to_string(index=False))

    best = results_df.iloc[0]
    best_covariates = best["covariates"].split(", ")
    print(f"\nBest BIC model: {best_covariates}")
    print(f"  AIC={best['AIC']:.2f}, BIC={best['BIC']:.2f}, R2_adj={best['R2_adj']:.4f}")

    # also show best AIC if different
    best_aic = results_df.sort_values("AIC").iloc[0]
    if best_aic["covariates"] != best["covariates"]:
        print(f"Best AIC model (different): {best_aic['covariates'].split(', ')}")
        print(f"  AIC={best_aic['AIC']:.2f}, BIC={best_aic['BIC']:.2f}, R2_adj={best_aic['R2_adj']:.4f}")

    return best_covariates


# =============================================================================
# STEP 3: CROSS-VALIDATION
# =============================================================================

def run_cross_validation(data, metric, covariate_sets, pre_period, n_holdout=8):
    """Hold out last N weeks of pre-period, test prediction accuracy."""
    print("\n" + "=" * 80)
    print(f"STEP 3: CROSS-VALIDATION (hold out last {n_holdout} pre-period weeks)")
    print("=" * 80)

    pre_data = data.loc[pre_period[0]:pre_period[1]]
    if len(pre_data) < n_holdout + 20:
        print("Insufficient pre-period data for cross-validation")
        return

    train = pre_data.iloc[:-n_holdout]
    test = pre_data.iloc[-n_holdout:]

    results = []
    for name, covs in covariate_sets.items():
        available_covs = [c for c in covs if c in data.columns]
        cols = [metric] + available_covs
        cv_data = pre_data[cols].dropna().astype(float)

        if len(cv_data) < 20:
            continue

        train_period = [train.index[0], train.index[-1]]
        test_period = [test.index[0], test.index[-1]]

        try:
            ci = CausalImpact(cv_data, train_period, test_period)
            preds = ci.inferences.loc[test_period[0]:test_period[1], "preds"]
            actuals = cv_data.loc[test_period[0]:test_period[1], metric]
            common = actuals.index.intersection(preds.index)
            if len(common) == 0:
                continue

            mae = np.abs(actuals.loc[common] - preds.loc[common]).mean()
            mape = (np.abs(actuals.loc[common] - preds.loc[common]) / actuals.loc[common].replace(0, np.nan)).dropna().mean()
            rmse = np.sqrt(((actuals.loc[common] - preds.loc[common]) ** 2).mean())

            results.append({
                "model": name,
                "MAE": mae,
                "MAPE": mape,
                "RMSE": rmse,
                "covariates": ", ".join(available_covs),
            })
        except Exception as e:
            print(f"  {name}: failed — {e}")

    if results:
        cv_df = pd.DataFrame(results).sort_values("MAE")
        print(cv_df.to_string(index=False))
        print(f"\nBest model by MAE: {cv_df.iloc[0]['model']}")
    else:
        print("No valid cross-validation results")


# =============================================================================
# STEP 4: SENSITIVITY ANALYSIS
# =============================================================================

def run_sensitivity_analysis(data, metric, covariates, pre_period, post_period, adv_id):
    """Test sensitivity to pre-period length and covariate subsets."""
    print("\n" + "=" * 80)
    print("STEP 4: SENSITIVITY ANALYSIS")
    print("=" * 80)

    results = []

    # vary pre-period length
    pre_data = data.loc[:pre_period[1]]
    for n_weeks in [26, 39, 52, 65, 78, len(pre_data)]:
        if n_weeks > len(pre_data) or n_weeks < 20:
            continue
        trimmed_pre = [pre_data.index[-n_weeks], pre_period[1]]
        try:
            available = [c for c in covariates if c in data.columns]
            ci_data = data[[metric] + available].dropna(subset=[metric]).astype(float)
            ci_data[available] = ci_data[available].ffill().bfill()
            ci = CausalImpact(ci_data, trimmed_pre, post_period)
            inf = ci.inferences[ci.inferences.index >= post_period[0]]
            predicted = inf["preds"].mean()
            abs_eff = inf["point_effects"].mean()
            results.append({
                "pre_weeks": n_weeks,
                "relative_effect": abs_eff / predicted if predicted != 0 else np.nan,
                "p_value": ci.p_value,
                "significant": ci.p_value < 0.05,
            })
        except Exception:
            pass

    if results:
        sens_df = pd.DataFrame(results)
        sens_df["effect_pct"] = sens_df["relative_effect"].apply(lambda x: f"{x:+.2%}")
        sens_df["p_fmt"] = sens_df["p_value"].apply(lambda x: f"{x:.4f}")
        print(f"\nAdvertiser {adv_id} — Pre-period length sensitivity:")
        print(sens_df[["pre_weeks", "effect_pct", "p_fmt", "significant"]].to_string(index=False))

        # check directional consistency
        signs = sens_df["relative_effect"].apply(lambda x: 1 if x > 0 else -1)
        if signs.nunique() == 1:
            print("  Direction: CONSISTENT across all pre-period lengths")
        else:
            print("  Direction: INCONSISTENT — result is sensitive to pre-period choice")


# =============================================================================
# STEP 5: PLACEBO TESTS
# =============================================================================

def run_placebo_tests(data, metric, covariates, pre_period, adv_id, n_placebos=5):
    """Run multiple placebo tests at different fake intervention points."""
    print(f"\n--- Placebo tests for advertiser {adv_id} ---")

    pre_data = data.loc[pre_period[0]:pre_period[1]]
    if len(pre_data) < 30:
        print("  Insufficient pre-data for placebo tests")
        return []

    results = []
    step = max(len(pre_data) // (n_placebos + 1), 4)

    for i in range(1, n_placebos + 1):
        split_idx = i * step
        if split_idx >= len(pre_data) - 4 or split_idx < 15:
            continue

        placebo_pre = [pre_data.index[0], pre_data.index[split_idx - 1]]
        placebo_post = [pre_data.index[split_idx], pre_data.index[-1]]

        try:
            available = [c for c in covariates if c in data.columns]
            ci_data = pre_data[[metric] + available].dropna(subset=[metric]).astype(float)
            ci_data[available] = ci_data[available].ffill().bfill()
            ci = CausalImpact(ci_data, placebo_pre, placebo_post)
            results.append({
                "placebo_split": f"week {split_idx}",
                "p_value": ci.p_value,
                "significant": ci.p_value < 0.05,
            })
        except Exception:
            pass

    if results:
        n_sig = sum(r["significant"] for r in results)
        fpr = n_sig / len(results)
        print(f"  Placebos: {len(results)}, False positives: {n_sig} ({fpr:.0%})")
        if fpr > 0.20:
            print("  WARNING: High false positive rate — model may be unreliable for this advertiser")
        return results
    return []


# =============================================================================
# STEP 6: FINAL CAUSAL IMPACT WITH OPTIMIZED MODEL
# =============================================================================

def run_final_analysis(data, metric, covariates, pre_period, post_period, adv_id, adv_name):
    """Run CausalImpact with the optimized covariate set."""
    available = [c for c in covariates if c in data.columns]
    ci_data = data[[metric] + available].dropna(subset=[metric]).astype(float)
    ci_data[available] = ci_data[available].ffill().bfill()

    if len(ci_data) < 10:
        return None

    try:
        ci = CausalImpact(ci_data, pre_period, post_period)
        inf = ci.inferences[ci.inferences.index >= post_period[0]]
        predicted = inf["preds"].mean()
        abs_eff = inf["point_effects"].mean()
        post_spend = data.loc[data.index >= post_period[0], "spend"].sum()

        return {
            "advertiser_id": adv_id,
            "company_name": adv_name,
            "metric": metric,
            "pre_weeks": int((pre_period[1] - pre_period[0]).days / 7) + 1,
            "post_weeks": int((post_period[1] - post_period[0]).days / 7) + 1,
            "actual_avg": ci.post_data.iloc[:, 0].mean(),
            "predicted_avg": predicted,
            "relative_effect": abs_eff / predicted if predicted != 0 else np.nan,
            "ci_lower": inf["point_effects_lower"].mean(),
            "ci_upper": inf["point_effects_upper"].mean(),
            "p_value": ci.p_value,
            "significant": ci.p_value < 0.05,
            "post_spend": float(post_spend),
        }
    except Exception as e:
        print(f"  Error: {e}")
        return None


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 80)
    print("TI-748: COVARIATE VALIDATION & MODEL DIAGNOSTICS")
    print("=" * 80)

    # --- Load data ---
    adopters, weekly_kpis = load_all_data()
    adv_week, platform = build_all_covariates(weekly_kpis)

    # All candidate covariates
    ALL_CANDIDATES = [
        "platform_ivr", "platform_cvr", "platform_vcr", "platform_roas", "platform_cpa",
        "platform_spend", "platform_impressions",
        "platform_active_advertisers", "platform_avg_campaign_groups",
        "holiday",
        "metric_lag1", "metric_lag2",
        "adv_active_cgs", "spend_change_pct",
    ]

    PRIMARY_METRIC = "ivr"

    # --- Process each advertiser ---
    all_final_results = {m: [] for m in METRIC_DEFS}
    all_placebo_results = []
    best_covariates_per_adv = {}

    for _, adv_row in adopters.iterrows():
        adv_id = adv_row["advertiser_id"]
        adv_name = adv_row["company_name"]
        intervention = adv_row["intervention_date"]

        data, pre_period, post_period = prepare_advertiser(
            adv_id, intervention, adv_week, platform, PRIMARY_METRIC
        )
        if data is None:
            continue

        print(f"\n{'#' * 80}")
        print(f"# ADVERTISER: {adv_name} ({adv_id})")
        print(f"# Intervention: {intervention}, Pre: {pre_period[0].date()} to {pre_period[1].date()}")
        print(f"# Post: {post_period[0].date()} to {post_period[1].date()}")
        print(f"{'#' * 80}")

        # STEP 1: VIF check
        vif_clean = run_vif_check(data, ALL_CANDIDATES)

        # STEP 2: Stepwise selection
        best_covs = run_stepwise_selection(data, PRIMARY_METRIC, vif_clean, pre_period)
        best_covariates_per_adv[adv_id] = best_covs

        # STEP 3: Cross-validation
        covariate_sets = {
            "minimal": ["platform_ivr", "platform_spend"],
            "v2_current": ["platform_ivr", "platform_spend", "platform_impressions",
                          "holiday", "platform_active_advertisers", "platform_vcr", "metric_lag1"],
            "bic_best": best_covs,
            "kitchen_sink": vif_clean,
        }
        run_cross_validation(data, PRIMARY_METRIC, covariate_sets, pre_period)

        # STEP 4: Sensitivity analysis
        run_sensitivity_analysis(data, PRIMARY_METRIC, best_covs, pre_period, post_period, adv_id)

        # STEP 5: Placebo tests
        placebo = run_placebo_tests(data, PRIMARY_METRIC, best_covs, pre_period, adv_id)
        all_placebo_results.extend(placebo)

        # STEP 6: Final analysis for all metrics
        for metric in METRIC_DEFS:
            # re-prepare with this metric's lag
            m_data, m_pre, m_post = prepare_advertiser(
                adv_id, intervention, adv_week, platform, metric
            )
            if m_data is None:
                continue
            result = run_final_analysis(m_data, metric, best_covs, m_pre, m_post, adv_id, adv_name)
            if result:
                all_final_results[metric].append(result)

    # ==========================================================================
    # AGGREGATE RESULTS
    # ==========================================================================

    print("\n" + "=" * 80)
    print("FINAL RESULTS — ALL METRICS (BIC-optimized covariates per advertiser)")
    print("=" * 80)

    for metric, results in all_final_results.items():
        if not results:
            continue
        df = pd.DataFrame(results)
        direction = METRIC_DEFS[metric]["direction"]
        n = len(df)
        n_sig = df["significant"].sum()
        n_pos = (df["relative_effect"] > 0).sum() if direction == "higher" else (df["relative_effect"] < 0).sum()
        ts = df["post_spend"].sum()
        weighted = (df["relative_effect"] * df["post_spend"]).sum() / ts if ts > 0 else 0

        df["effect_pct"] = df["relative_effect"].apply(lambda x: f"{x:+.2%}")
        df["p_fmt"] = df["p_value"].apply(lambda x: f"{x:.4f}")

        print(f"\n--- {metric.upper()} ({METRIC_DEFS[metric]['label']}) ---")
        print(df[["company_name", "advertiser_id", "pre_weeks", "post_weeks",
                   "effect_pct", "p_fmt", "significant"]].to_string(index=False))
        print(f"  N={n}, Significant={n_sig}/{n}, Positive={n_pos}/{n}")
        print(f"  Mean={df['relative_effect'].mean():+.2%}, Median={df['relative_effect'].median():+.2%}")
        print(f"  Spend-weighted={weighted:+.2%}")

    # Placebo summary
    if all_placebo_results:
        n_plac = len(all_placebo_results)
        n_fp = sum(r["significant"] for r in all_placebo_results)
        print(f"\n--- PLACEBO SUMMARY ---")
        print(f"Total placebo tests: {n_plac}")
        print(f"False positives: {n_fp} ({n_fp/n_plac:.0%})")

    # Covariate summary
    print(f"\n--- COVARIATE SELECTION SUMMARY ---")
    for adv_id, covs in best_covariates_per_adv.items():
        print(f"  {adv_id}: {covs}")

    print("\nDone.")


if __name__ == "__main__":
    main()
