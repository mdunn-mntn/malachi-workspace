"""
TI-504: CausalImpact with Full Covariate Validation (TI-748 Methodology)
==========================================================================

Applies the full TI-748 methodology to the Fangorn experiment:
  1. VIF multicollinearity check — drop covariates with VIF > 10
  2. BIC stepwise covariate selection — per-advertiser optimal set
  3. Cross-validation — hold-out last N pre-period weeks
  4. Run CausalImpact with winning covariate set
  5. Sensitivity analysis — vary pre-period length
  6. Placebo tests — validate model reliability
  7. Generate 3-panel CausalImpact plots

Pre-period: parent campaign weekly IVR (before experiment)
Post-period: treatment arm weekly IVR (during experiment)
Covariates: platform-level metrics ONLY (no self-referencing)
"""

import warnings
from itertools import combinations

import matplotlib
matplotlib.use("Agg")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from causalimpact import CausalImpact
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

warnings.filterwarnings("ignore")

BQ_PROJECT = "dw-main-silver"
MIN_PRE_WEEKS = 12

ADVERTISER_NAMES = {
    36420: "Zumba Fitness",
    40956: "Edward Martin",
    42273: "Reedsy",
    42692: "Collector Store",
    46920: "G-Shock",
}

PARENT_CAMPAIGN_GROUPS = {
    42692: [94979],
    40956: [84250],
    46920: [101662],
    42273: [86917],
    36420: [57130],
}

TREATMENT_CAMPAIGNS = {
    42692: [553902, 553878, 553929, 553955],
    40956: [552928, 552911, 552948, 552960],
    46920: [553722, 553698, 553754, 553769],
    42273: [554027, 553980, 554052, 554068],
    36420: [552767, 552755, 552779, 552800],
}

HOLIDAY_WEEKS = {
    pd.Timestamp("2025-11-24"): 1, pd.Timestamp("2025-12-22"): 1, pd.Timestamp("2025-12-29"): 1,
    pd.Timestamp("2026-02-02"): 1,
}

EXPERIMENT_START = pd.Timestamp("2026-03-02")  # monday before first data day


# =============================================================================
# DATA LOADING
# =============================================================================

def load_all_data():
    client = bigquery.Client(project=BQ_PROJECT)

    # Parent campaign weekly data
    all_cg_ids = []
    for cg_list in PARENT_CAMPAIGN_GROUPS.values():
        all_cg_ids.extend(cg_list)
    cg_str = ",".join(str(x) for x in all_cg_ids)

    print("Loading parent campaign weekly data...")
    parent = client.query(f"""
    WITH prospecting AS (
        SELECT c.campaign_id, c.advertiser_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.campaign_group_id IN ({cg_str})
        AND c.funnel_level = 1 AND c.deleted = FALSE
    )
    SELECT pc.advertiser_id,
        DATE_TRUNC(s.day, WEEK(MONDAY)) AS week_start,
        SUM(s.impressions) AS impressions,
        SUM(s.clicks + s.views + COALESCE(s.competing_views, 0)) AS vv,
        SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend,
        SUM(s.vast_start) AS vast_start,
        SUM(s.vast_complete) AS vast_complete,
        COUNT(DISTINCT s.campaign_id) AS active_campaigns
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN prospecting pc ON pc.campaign_id = s.campaign_id
    WHERE s.day >= '2025-01-01' AND s.impressions > 0
    GROUP BY 1, 2 ORDER BY 1, 2
    """).to_dataframe()
    parent["week_start"] = pd.to_datetime(parent["week_start"])
    for c in ["impressions", "spend", "vv", "vast_start", "vast_complete"]:
        parent[c] = pd.to_numeric(parent[c], errors="coerce").astype(float)
    print(f"  {len(parent)} rows, {parent['advertiser_id'].nunique()} advertisers")

    # Experiment treatment weekly data
    all_treat = []
    for cids in TREATMENT_CAMPAIGNS.values():
        all_treat.extend(cids)
    cid_str = ",".join(str(x) for x in all_treat)

    print("Loading experiment treatment weekly data...")
    treat_imps = client.query(f"""
    SELECT campaign_id, DATE_TRUNC(DATE(time), WEEK(MONDAY)) AS week_start,
        COUNT(*) AS impressions, SUM(media_cost) AS spend
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE campaign_id IN ({cid_str})
    AND DATE(time) BETWEEN '2026-03-04' AND '2026-03-24'
    GROUP BY 1, 2
    """).to_dataframe()

    treat_vv = client.query(f"""
    SELECT campaign_id, DATE_TRUNC(DATE(time), WEEK(MONDAY)) AS week_start,
        COUNT(*) AS vv
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE campaign_id IN ({cid_str})
    AND DATE(time) BETWEEN '2026-03-04' AND '2026-03-24'
    GROUP BY 1, 2
    """).to_dataframe()

    # Map campaign_id to advertiser_id
    cid_to_adv = {}
    for adv_id, cids in TREATMENT_CAMPAIGNS.items():
        for cid in cids:
            cid_to_adv[cid] = adv_id

    treat = treat_imps.merge(treat_vv, on=["campaign_id", "week_start"], how="left")
    treat["vv"] = treat["vv"].fillna(0)
    treat["advertiser_id"] = treat["campaign_id"].map(cid_to_adv)
    treat["week_start"] = pd.to_datetime(treat["week_start"])

    treat_weekly = treat.groupby(["advertiser_id", "week_start"]).agg(
        impressions=("impressions", "sum"), vv=("vv", "sum"), spend=("spend", "sum")
    ).reset_index()
    for c in ["impressions", "vv", "spend"]:
        treat_weekly[c] = treat_weekly[c].astype(float)
    print(f"  {len(treat_weekly)} rows")

    # Platform covariates (all non-test prospecting campaigns)
    print("Loading platform covariates...")
    platform = client.query("""
    WITH prospecting AS (
        SELECT c.campaign_id, c.advertiser_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.funnel_level = 1 AND c.deleted = FALSE AND c.is_test = FALSE
    )
    SELECT DATE_TRUNC(s.day, WEEK(MONDAY)) AS week_start,
        SUM(s.impressions) AS platform_impressions,
        SUM(s.clicks + s.views + COALESCE(s.competing_views, 0)) AS platform_vv,
        SUM(s.media_spend + s.data_spend + s.platform_spend) AS platform_spend,
        SUM(s.click_conversions + s.view_conversions + COALESCE(s.competing_view_conversions, 0)) AS platform_conversions,
        SUM(s.vast_start) AS platform_vast_start,
        SUM(s.vast_complete) AS platform_vast_complete,
        COUNT(DISTINCT pc.advertiser_id) AS platform_active_advertisers
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN prospecting pc ON pc.campaign_id = s.campaign_id
    WHERE s.day >= '2025-01-01' AND s.impressions > 0
    GROUP BY 1 ORDER BY 1
    """).to_dataframe()
    platform["week_start"] = pd.to_datetime(platform["week_start"])
    for c in platform.columns:
        if c != "week_start":
            platform[c] = pd.to_numeric(platform[c], errors="coerce").astype(float)

    # Derived platform metrics
    platform["platform_ivr"] = platform["platform_vv"] / platform["platform_impressions"]
    platform["platform_cvr"] = platform["platform_conversions"] / platform["platform_vv"].replace(0, np.nan)
    platform["platform_vcr"] = platform["platform_vast_complete"] / platform["platform_vast_start"].replace(0, np.nan)
    platform["holiday"] = platform["week_start"].map(lambda w: HOLIDAY_WEEKS.get(w, 0.0))

    # Scale for numerical stability
    platform["platform_spend"] /= 1e6
    platform["platform_impressions"] /= 1e9
    platform["platform_active_advertisers"] /= 1000.0

    print(f"  {len(platform)} weeks")
    return parent, treat_weekly, platform


def prepare_advertiser(adv_id, parent, treat_weekly, platform):
    """Build combined time series with all candidate covariates for one advertiser."""
    # Pre-period from parent
    pre = parent[parent["advertiser_id"] == adv_id].copy()
    pre["ivr"] = pre["vv"] / pre["impressions"]
    pre = pre[pre["week_start"] < EXPERIMENT_START]

    # Post-period from treatment
    post = treat_weekly[treat_weekly["advertiser_id"] == adv_id].copy()
    post["ivr"] = post["vv"] / post["impressions"]

    if len(pre) < MIN_PRE_WEEKS or post.empty:
        return None, None, None

    # Combine into single time series
    combined = pd.concat([
        pre[["week_start", "ivr", "impressions", "spend"]],
        post[["week_start", "ivr", "impressions", "spend"]]
    ]).set_index("week_start").sort_index()

    # Merge platform covariates
    combined = combined.merge(platform, left_index=True, right_on="week_start", how="inner").set_index("week_start")

    # Advertiser-specific covariates
    combined["metric_lag1"] = combined["ivr"].shift(1)
    combined["metric_lag2"] = combined["ivr"].shift(2)
    combined["spend_change_pct"] = combined["spend"].pct_change().fillna(0).clip(-1, 5)
    combined["adv_active_campaigns"] = combined.get("active_campaigns", 1)

    combined = combined.dropna(subset=["metric_lag1", "metric_lag2"])

    pre_period = [combined.index[0], combined[combined.index < EXPERIMENT_START].index[-1]]
    post_period = [combined[combined.index >= EXPERIMENT_START].index[0],
                   combined[combined.index >= EXPERIMENT_START].index[-1]]

    return combined, pre_period, post_period


# =============================================================================
# STEP 1: VIF CHECK
# =============================================================================

ALL_CANDIDATES = [
    "platform_ivr", "platform_cvr", "platform_vcr",
    "platform_spend", "platform_impressions",
    "platform_active_advertisers",
    "holiday",
    "metric_lag1", "metric_lag2",
    "spend_change_pct",
]


def run_vif_check(data, candidates):
    """VIF multicollinearity check — iteratively remove VIF > 10."""
    available = [c for c in candidates if c in data.columns]
    clean = data[available].dropna().astype(float)

    if len(clean) < 10:
        return available

    # Iteratively remove highest VIF
    keep = available.copy()
    removed = []
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
        removed.append((worst, max_vif))
        keep.remove(worst)

    if removed:
        print(f"    VIF removed: {[(r, f'{v:.0f}') for r, v in removed]}")
    print(f"    VIF-clean: {keep}")
    return keep


# =============================================================================
# STEP 2: BIC STEPWISE SELECTION
# =============================================================================

def run_bic_selection(data, metric, vif_clean, pre_period):
    """BIC stepwise covariate selection on pre-period only."""
    pre_data = data.loc[pre_period[0]:pre_period[1]].copy()
    y = pre_data[metric].dropna()

    results = []
    max_size = min(len(vif_clean), 6)

    for size in range(1, max_size + 1):
        for combo in combinations(vif_clean, size):
            combo_list = list(combo)
            X = pre_data[combo_list].reindex(y.index).dropna()
            common = y.index.intersection(X.index)
            if len(common) < 15:
                continue

            try:
                model = sm.OLS(y.loc[common], sm.add_constant(X.loc[common])).fit()
                results.append({
                    "covariates": combo_list,
                    "n_cov": len(combo_list),
                    "BIC": model.bic,
                    "AIC": model.aic,
                    "R2_adj": model.rsquared_adj,
                })
            except Exception:
                pass

    if not results:
        return vif_clean[:2]

    results_df = pd.DataFrame(results).sort_values("BIC")
    best = results_df.iloc[0]
    print(f"    BIC best: {best['covariates']} (BIC={best['BIC']:.1f}, R²={best['R2_adj']:.3f})")

    # Show top 3 for comparison
    for i, row in results_df.head(3).iterrows():
        print(f"      #{results_df.index.get_loc(i)+1}: {row['covariates']} BIC={row['BIC']:.1f} R²={row['R2_adj']:.3f}")

    return best["covariates"]


# =============================================================================
# STEP 3: CROSS-VALIDATION
# =============================================================================

def run_cross_validation(data, metric, covariate_sets, pre_period, n_holdout=6):
    """Hold out last N weeks of pre-period, compare prediction accuracy."""
    pre_data = data.loc[pre_period[0]:pre_period[1]]
    if len(pre_data) < n_holdout + 12:
        print(f"    CV: insufficient data ({len(pre_data)} weeks)")
        return

    train = pre_data.iloc[:-n_holdout]
    test = pre_data.iloc[-n_holdout:]

    results = []
    for name, covs in covariate_sets.items():
        available = [c for c in covs if c in data.columns]
        cols = [metric] + available
        cv_data = pre_data[cols].dropna().astype(float)

        if len(cv_data) < 15:
            continue

        try:
            ci = CausalImpact(cv_data, [train.index[0], train.index[-1]],
                              [test.index[0], test.index[-1]])
            preds = ci.inferences.loc[test.index[0]:test.index[-1], "preds"]
            actuals = cv_data.loc[test.index[0]:test.index[-1], metric]
            common = actuals.index.intersection(preds.index)
            if len(common) == 0:
                continue

            mae = np.abs(actuals.loc[common] - preds.loc[common]).mean()
            mape = (np.abs(actuals.loc[common] - preds.loc[common]) / actuals.loc[common].replace(0, np.nan)).dropna().mean()

            results.append({"model": name, "MAE": mae, "MAPE": f"{mape:.1%}", "covariates": available})
        except Exception as e:
            pass

    if results:
        cv_df = pd.DataFrame(results).sort_values("MAE")
        for _, r in cv_df.iterrows():
            print(f"    CV {r['model']:15s}: MAE={r['MAE']:.6f}  MAPE={r['MAPE']}")
        print(f"    Best: {cv_df.iloc[0]['model']}")


# =============================================================================
# STEP 4: SENSITIVITY ANALYSIS
# =============================================================================

def run_sensitivity(data, metric, covariates, pre_period, post_period):
    """Vary pre-period length, check directional consistency."""
    pre_data = data.loc[:pre_period[1]]
    results = []

    for n_weeks in [15, 20, 26, 32, 39, len(pre_data)]:
        if n_weeks > len(pre_data) or n_weeks < 12:
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
                "effect": abs_eff / predicted if predicted != 0 else np.nan,
                "p_value": ci.p_value,
            })
        except Exception:
            pass

    if results:
        sens_df = pd.DataFrame(results)
        signs = sens_df["effect"].apply(lambda x: "+" if x > 0 else "-")
        consistent = signs.nunique() == 1
        for _, r in sens_df.iterrows():
            sig = "*" if r["p_value"] < 0.05 else " "
            print(f"    {int(r['pre_weeks']):3d} weeks: {r['effect']:+.2%}  p={r['p_value']:.4f} {sig}")
        print(f"    Direction: {'CONSISTENT' if consistent else 'INCONSISTENT'}")
        return consistent
    return None


# =============================================================================
# STEP 5: PLACEBO TESTS
# =============================================================================

def run_placebo_tests(data, metric, covariates, pre_period, n_placebos=5):
    """Run placebo tests at fake intervention points within pre-period."""
    pre_data = data.loc[pre_period[0]:pre_period[1]]
    if len(pre_data) < 25:
        print(f"    Placebo: insufficient data ({len(pre_data)} weeks)")
        return None

    results = []
    step = max(len(pre_data) // (n_placebos + 1), 4)

    for i in range(1, n_placebos + 1):
        split_idx = i * step
        if split_idx >= len(pre_data) - 4 or split_idx < 12:
            continue

        placebo_pre = [pre_data.index[0], pre_data.index[split_idx - 1]]
        placebo_post = [pre_data.index[split_idx], pre_data.index[-1]]

        try:
            available = [c for c in covariates if c in data.columns]
            ci_data = pre_data[[metric] + available].dropna(subset=[metric]).astype(float)
            ci_data[available] = ci_data[available].ffill().bfill()
            ci = CausalImpact(ci_data, placebo_pre, placebo_post)
            results.append({
                "split": f"week {split_idx}",
                "p_value": ci.p_value,
                "significant": ci.p_value < 0.05,
            })
        except Exception:
            pass

    if results:
        n_sig = sum(r["significant"] for r in results)
        fpr = n_sig / len(results)
        print(f"    Placebo: {len(results)} tests, {n_sig} false positives ({fpr:.0%})")
        if fpr > 0.20:
            print(f"    WARNING: High FPR — model may be unreliable")
        return fpr
    return None


# =============================================================================
# STEP 6: FINAL CAUSAL IMPACT + PLOT
# =============================================================================

def run_final_and_plot(data, metric, covariates, pre_period, post_period, adv_id, adv_name):
    """Run CausalImpact with optimized covariates and generate 3-panel plot."""
    available = [c for c in covariates if c in data.columns]
    ci_data = data[[metric] + available].dropna(subset=[metric]).astype(float)
    ci_data[available] = ci_data[available].ffill().bfill()

    try:
        ci = CausalImpact(ci_data, pre_period, post_period)
        inf = ci.inferences[ci.inferences.index >= post_period[0]]
        predicted = inf["preds"].mean()
        actual = ci.post_data.iloc[:, 0].mean()
        abs_eff = inf["point_effects"].mean()
        rel_eff = abs_eff / predicted if predicted != 0 else np.nan

        print(f"\n    FINAL RESULT:")
        print(f"    Predicted IVR: {predicted:.6f}")
        print(f"    Actual IVR:    {actual:.6f}")
        print(f"    Effect: {rel_eff:+.2%}  (p={ci.p_value:.4f})")
        print(f"    Significant: {'YES' if ci.p_value < 0.05 else 'no'}")

        # Generate 3-panel plot
        plt.close("all")
        ci.plot(figsize=(14, 10))
        fig = plt.gcf()
        fig.suptitle(f"CausalImpact: {adv_name} (BIC-Optimized Covariates)\n"
                     f"Pre: {pre_period[0].date()} – {pre_period[1].date()} | "
                     f"Post: {post_period[0].date()} – {post_period[1].date()} | "
                     f"Effect: {rel_eff:+.2%} (p={ci.p_value:.4f})\n"
                     f"Covariates: {', '.join(available)}",
                     fontsize=11, fontweight="bold")
        fig.subplots_adjust(top=0.85)
        fname = f"outputs/ti_504_ci_validated_{adv_name.lower().replace(' ', '_')}.png"
        fig.savefig(fname, dpi=150)
        plt.close("all")
        print(f"    Plot: {fname}")

        return {
            "advertiser_id": adv_id,
            "advertiser_name": adv_name,
            "pre_weeks": len(data.loc[pre_period[0]:pre_period[1]]),
            "post_weeks": len(data.loc[post_period[0]:post_period[1]]),
            "predicted_ivr": predicted,
            "actual_ivr": actual,
            "relative_effect": rel_eff,
            "p_value": ci.p_value,
            "significant": ci.p_value < 0.05,
            "covariates": available,
        }
    except Exception as e:
        print(f"    Error: {e}")
        return None


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 90)
    print("TI-504: CAUSAL IMPACT — FULL COVARIATE VALIDATION (TI-748 METHODOLOGY)")
    print("=" * 90)

    parent, treat_weekly, platform = load_all_data()

    results = []
    all_placebo_fpr = []
    all_sensitivity = []

    for adv_id, adv_name in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
        print(f"\n{'#' * 90}")
        print(f"# {adv_name} ({adv_id})")
        print(f"{'#' * 90}")

        data, pre_period, post_period = prepare_advertiser(adv_id, parent, treat_weekly, platform)
        if data is None:
            print("  Insufficient data — skipping")
            continue

        pre_len = len(data.loc[pre_period[0]:pre_period[1]])
        post_len = len(data.loc[post_period[0]:post_period[1]])
        print(f"  Pre: {pre_period[0].date()} to {pre_period[1].date()} ({pre_len} weeks)")
        print(f"  Post: {post_period[0].date()} to {post_period[1].date()} ({post_len} weeks)")

        # Step 1: VIF
        print(f"\n  --- STEP 1: VIF CHECK ---")
        vif_clean = run_vif_check(data, ALL_CANDIDATES)

        # Step 2: BIC
        print(f"\n  --- STEP 2: BIC SELECTION ---")
        bic_best = run_bic_selection(data, "ivr", vif_clean, pre_period)

        # Step 3: Cross-validation
        print(f"\n  --- STEP 3: CROSS-VALIDATION ---")
        covariate_sets = {
            "minimal": ["platform_ivr"],
            "platform": ["platform_ivr", "platform_spend"],
            "bic_best": bic_best,
            "vif_clean": vif_clean,
        }
        run_cross_validation(data, "ivr", covariate_sets, pre_period)

        # Step 4: Sensitivity
        print(f"\n  --- STEP 4: SENSITIVITY ---")
        consistent = run_sensitivity(data, "ivr", bic_best, pre_period, post_period)
        all_sensitivity.append({"advertiser": adv_name, "consistent": consistent})

        # Step 5: Placebo
        print(f"\n  --- STEP 5: PLACEBO TESTS ---")
        fpr = run_placebo_tests(data, "ivr", bic_best, pre_period)
        if fpr is not None:
            all_placebo_fpr.append({"advertiser": adv_name, "fpr": fpr})

        # Step 6: Final + Plot
        print(f"\n  --- STEP 6: FINAL CAUSAL IMPACT ---")
        result = run_final_and_plot(data, "ivr", bic_best, pre_period, post_period, adv_id, adv_name)
        if result:
            results.append(result)

    # Summary
    print(f"\n{'=' * 90}")
    print("SUMMARY — BIC-OPTIMIZED CAUSAL IMPACT")
    print(f"{'=' * 90}")

    if results:
        rdf = pd.DataFrame(results)
        for _, r in rdf.iterrows():
            sig = "***" if r["significant"] else "   "
            print(f"  {r['advertiser_name']:20s}: {r['relative_effect']:+.2%}  p={r['p_value']:.4f} {sig}  covs={r['covariates']}")

        n_sig = rdf["significant"].sum()
        n_pos = (rdf["relative_effect"] > 0).sum()
        print(f"\n  Significant: {n_sig}/{len(rdf)}")
        print(f"  Positive: {n_pos}/{len(rdf)}")
        print(f"  Median effect: {rdf['relative_effect'].median():+.2%}")

    if all_placebo_fpr:
        avg_fpr = np.mean([x["fpr"] for x in all_placebo_fpr])
        print(f"\n  Avg placebo FPR: {avg_fpr:.0%}")
        for p in all_placebo_fpr:
            print(f"    {p['advertiser']}: {p['fpr']:.0%}")

    if all_sensitivity:
        n_consistent = sum(1 for s in all_sensitivity if s["consistent"])
        print(f"\n  Sensitivity consistent: {n_consistent}/{len(all_sensitivity)}")

    # Save results
    if results:
        pd.DataFrame(results).to_csv("outputs/ti_504_ci_validated_results.csv", index=False)
        print("\nResults saved to outputs/ti_504_ci_validated_results.csv")

    print("\nDone.")


if __name__ == "__main__":
    main()
