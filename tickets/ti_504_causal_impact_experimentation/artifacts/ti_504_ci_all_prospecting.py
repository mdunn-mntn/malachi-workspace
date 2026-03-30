"""
TI-504: CausalImpact — All Prospecting Campaigns as Baseline
==============================================================

Uses ALL of an advertiser's prospecting campaigns (not just the parent campaign
group) as the pre-period baseline, then compares to the treatment arm's IVR
during the experiment period.

Full TI-748 methodology: VIF → BIC → CV → sensitivity → placebo → plot.
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

ADVERTISER_NAMES = {
    36420: "Zumba Fitness",
    40956: "Edward Martin",
    42273: "Reedsy",
    42692: "Collector Store",
    46920: "G-Shock",
}

TREATMENT_CAMPAIGNS = {
    42692: [553902, 553878, 553929, 553955],
    40956: [552928, 552911, 552948, 552960],
    46920: [553722, 553698, 553754, 553769],
    42273: [554027, 553980, 554052, 554068],
    36420: [552767, 552755, 552779, 552800],
}

EXPERIMENT_START = pd.Timestamp("2026-03-02")

HOLIDAY_WEEKS = {
    pd.Timestamp("2025-11-24"): 1, pd.Timestamp("2025-12-22"): 1,
    pd.Timestamp("2025-12-29"): 1, pd.Timestamp("2026-02-02"): 1,
}

ALL_CANDIDATES = [
    "platform_ivr", "platform_cvr", "platform_vcr",
    "platform_spend", "platform_impressions",
    "platform_active_advertisers",
    "holiday",
    "metric_lag1", "metric_lag2",
    "spend_change_pct",
]


def load_all_data():
    client = bigquery.Client(project=BQ_PROJECT)
    adv_list = ",".join(str(x) for x in ADVERTISER_NAMES.keys())

    # ALL prospecting campaigns for these advertisers (weekly)
    print("Loading ALL prospecting campaign weekly data...")
    adv_weekly = client.query(f"""
    WITH prospecting AS (
        SELECT c.campaign_id, c.advertiser_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.advertiser_id IN ({adv_list})
        AND c.funnel_level = 1 AND c.deleted = FALSE AND c.is_test = FALSE
    )
    SELECT pc.advertiser_id,
        DATE_TRUNC(s.day, WEEK(MONDAY)) AS week_start,
        SUM(s.impressions) AS impressions,
        SUM(s.clicks + s.views + COALESCE(s.competing_views, 0)) AS vv,
        SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend,
        SUM(s.vast_start) AS vast_start,
        SUM(s.vast_complete) AS vast_complete,
        COUNT(DISTINCT pc.campaign_id) AS active_campaigns
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN prospecting pc ON pc.campaign_id = s.campaign_id
    WHERE s.day >= '2025-01-01' AND s.impressions > 0
    GROUP BY 1, 2 ORDER BY 1, 2
    """).to_dataframe()
    adv_weekly["week_start"] = pd.to_datetime(adv_weekly["week_start"])
    for c in ["impressions", "spend", "vv", "vast_start", "vast_complete"]:
        adv_weekly[c] = pd.to_numeric(adv_weekly[c], errors="coerce").astype(float)
    print(f"  {len(adv_weekly)} rows, {adv_weekly['advertiser_id'].nunique()} advertisers")
    for aid, aname in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
        sub = adv_weekly[adv_weekly["advertiser_id"] == aid]
        avg_ivr = sub["vv"].sum() / sub["impressions"].sum()
        print(f"    {aname}: {len(sub)} weeks, avg IVR={avg_ivr:.4f}")

    # Experiment treatment weekly data
    all_treat = []
    cid_to_adv = {}
    for adv_id, cids in TREATMENT_CAMPAIGNS.items():
        for cid in cids:
            all_treat.append(cid)
            cid_to_adv[cid] = adv_id
    cid_str = ",".join(str(x) for x in all_treat)

    print("\nLoading experiment treatment weekly data...")
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
    for aid, aname in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
        sub = treat_weekly[treat_weekly["advertiser_id"] == aid]
        if not sub.empty:
            avg_ivr = sub["vv"].sum() / sub["impressions"].sum()
            print(f"    {aname}: {len(sub)} weeks, avg IVR={avg_ivr:.4f}")

    # Platform covariates
    print("\nLoading platform covariates...")
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
    platform["platform_ivr"] = platform["platform_vv"] / platform["platform_impressions"]
    platform["platform_cvr"] = platform["platform_conversions"] / platform["platform_vv"].replace(0, np.nan)
    platform["platform_vcr"] = platform["platform_vast_complete"] / platform["platform_vast_start"].replace(0, np.nan)
    platform["holiday"] = platform["week_start"].map(lambda w: HOLIDAY_WEEKS.get(w, 0.0))
    platform["platform_spend"] /= 1e6
    platform["platform_impressions"] /= 1e9
    platform["platform_active_advertisers"] /= 1000.0
    print(f"  {len(platform)} weeks")

    return adv_weekly, treat_weekly, platform


def prepare_advertiser(adv_id, adv_weekly, treat_weekly, platform):
    pre = adv_weekly[adv_weekly["advertiser_id"] == adv_id].copy()
    pre["ivr"] = pre["vv"] / pre["impressions"]
    pre = pre[pre["week_start"] < EXPERIMENT_START]

    post = treat_weekly[treat_weekly["advertiser_id"] == adv_id].copy()
    post["ivr"] = post["vv"] / post["impressions"]

    if len(pre) < 12 or post.empty:
        return None, None, None

    combined = pd.concat([
        pre[["week_start", "ivr", "impressions", "spend"]],
        post[["week_start", "ivr", "impressions", "spend"]]
    ]).set_index("week_start").sort_index()

    combined = combined.merge(platform, left_index=True, right_on="week_start", how="inner").set_index("week_start")
    combined["metric_lag1"] = combined["ivr"].shift(1)
    combined["metric_lag2"] = combined["ivr"].shift(2)
    combined["spend_change_pct"] = combined["spend"].pct_change().fillna(0).clip(-1, 5)
    combined = combined.dropna(subset=["metric_lag1", "metric_lag2"])

    pre_idx = combined[combined.index < EXPERIMENT_START].index
    post_idx = combined[combined.index >= EXPERIMENT_START].index
    if len(pre_idx) < 12 or len(post_idx) == 0:
        return None, None, None

    return combined, [pre_idx[0], pre_idx[-1]], [post_idx[0], post_idx[-1]]


def run_vif_check(data, candidates):
    keep = [c for c in candidates if c in data.columns]
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
        if max(vifs) <= 10:
            break
        worst = keep[vifs.index(max(vifs))]
        print(f"    VIF drop: {worst} ({max(vifs):.0f})")
        keep.remove(worst)
    print(f"    VIF-clean: {keep}")
    return keep


def run_bic_selection(data, vif_clean, pre_period):
    pre_data = data.loc[pre_period[0]:pre_period[1]]
    y = pre_data["ivr"].dropna()
    results = []
    for size in range(1, min(len(vif_clean) + 1, 6)):
        for combo in combinations(vif_clean, size):
            combo_list = list(combo)
            X = pre_data[combo_list].reindex(y.index).dropna()
            common = y.index.intersection(X.index)
            if len(common) < 15:
                continue
            try:
                model = sm.OLS(y.loc[common], sm.add_constant(X.loc[common])).fit()
                results.append({"covariates": combo_list, "BIC": model.bic, "R2": model.rsquared_adj})
            except Exception:
                pass
    if not results:
        return vif_clean[:2]
    best = pd.DataFrame(results).sort_values("BIC").iloc[0]
    print(f"    BIC best: {best['covariates']} (BIC={best['BIC']:.1f}, R²={best['R2']:.3f})")
    return best["covariates"]


def run_sensitivity(data, covariates, pre_period, post_period):
    pre_data = data.loc[:pre_period[1]]
    results = []
    for n_weeks in [15, 20, 26, 32, 39, 52, len(pre_data)]:
        if n_weeks > len(pre_data) or n_weeks < 12:
            continue
        trimmed = [pre_data.index[-n_weeks], pre_period[1]]
        try:
            avail = [c for c in covariates if c in data.columns]
            ci_data = data[["ivr"] + avail].dropna(subset=["ivr"]).astype(float)
            ci_data[avail] = ci_data[avail].ffill().bfill()
            ci = CausalImpact(ci_data, trimmed, post_period)
            inf = ci.inferences[ci.inferences.index >= post_period[0]]
            pred = inf["preds"].mean()
            eff = inf["point_effects"].mean() / pred if pred != 0 else np.nan
            results.append({"pre_weeks": n_weeks, "effect": eff, "p": ci.p_value})
        except Exception:
            pass
    if results:
        signs = [r["effect"] > 0 for r in results]
        consistent = len(set(signs)) == 1
        for r in results:
            sig = "*" if r["p"] < 0.05 else " "
            print(f"    {int(r['pre_weeks']):3d} wks: {r['effect']:+.2%}  p={r['p']:.4f} {sig}")
        print(f"    Direction: {'CONSISTENT' if consistent else 'INCONSISTENT'}")
        return consistent
    return None


def run_placebo(data, covariates, pre_period, n_placebos=5):
    pre_data = data.loc[pre_period[0]:pre_period[1]]
    if len(pre_data) < 25:
        print(f"    Placebo: insufficient data ({len(pre_data)} weeks)")
        return None
    results = []
    step = max(len(pre_data) // (n_placebos + 1), 4)
    for i in range(1, n_placebos + 1):
        idx = i * step
        if idx >= len(pre_data) - 4 or idx < 12:
            continue
        try:
            avail = [c for c in covariates if c in data.columns]
            ci_data = pre_data[["ivr"] + avail].dropna(subset=["ivr"]).astype(float)
            ci_data[avail] = ci_data[avail].ffill().bfill()
            ci = CausalImpact(ci_data,
                              [pre_data.index[0], pre_data.index[idx - 1]],
                              [pre_data.index[idx], pre_data.index[-1]])
            results.append({"split": idx, "p": ci.p_value, "sig": ci.p_value < 0.05})
        except Exception:
            pass
    if results:
        n_sig = sum(r["sig"] for r in results)
        fpr = n_sig / len(results)
        print(f"    Placebo: {len(results)} tests, {n_sig} FP ({fpr:.0%})")
        return fpr
    return None


def run_final(data, covariates, pre_period, post_period, adv_id, adv_name):
    avail = [c for c in covariates if c in data.columns]
    ci_data = data[["ivr"] + avail].dropna(subset=["ivr"]).astype(float)
    ci_data[avail] = ci_data[avail].ffill().bfill()
    try:
        ci = CausalImpact(ci_data, pre_period, post_period)
        inf = ci.inferences[ci.inferences.index >= post_period[0]]
        pred = inf["preds"].mean()
        actual = ci.post_data.iloc[:, 0].mean()
        eff = inf["point_effects"].mean() / pred if pred != 0 else np.nan

        print(f"    Predicted: {pred:.6f}  Actual: {actual:.6f}")
        print(f"    Effect: {eff:+.2%}  p={ci.p_value:.4f}  {'SIG' if ci.p_value < 0.05 else 'n.s.'}")

        plt.close("all")
        ci.plot(figsize=(14, 10))
        fig = plt.gcf()
        fig.suptitle(
            f"CausalImpact: {adv_name} (All Prospecting Baseline, BIC Covariates)\n"
            f"Pre: {pre_period[0].date()} – {pre_period[1].date()} | "
            f"Post: {post_period[0].date()} – {post_period[1].date()} | "
            f"Effect: {eff:+.2%} (p={ci.p_value:.4f})\n"
            f"Covariates: {', '.join(avail)}",
            fontsize=11, fontweight="bold")
        fig.subplots_adjust(top=0.85)
        fname = f"outputs/ti_504_ci_allprosp_{adv_name.lower().replace(' ', '_')}.png"
        fig.savefig(fname, dpi=150)
        plt.close("all")
        print(f"    Plot: {fname}")

        return {
            "advertiser_id": adv_id, "advertiser_name": adv_name,
            "pre_weeks": len(data.loc[pre_period[0]:pre_period[1]]),
            "post_weeks": len(data.loc[post_period[0]:post_period[1]]),
            "predicted_ivr": pred, "actual_ivr": actual,
            "relative_effect": eff, "p_value": ci.p_value,
            "significant": ci.p_value < 0.05, "covariates": avail,
        }
    except Exception as e:
        print(f"    Error: {e}")
        return None


def main():
    print("=" * 90)
    print("TI-504: CAUSAL IMPACT — ALL PROSPECTING AS BASELINE")
    print("=" * 90)

    adv_weekly, treat_weekly, platform = load_all_data()

    results = []
    for adv_id, adv_name in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
        print(f"\n{'#' * 90}")
        print(f"# {adv_name} ({adv_id})")
        print(f"{'#' * 90}")

        data, pre_period, post_period = prepare_advertiser(adv_id, adv_weekly, treat_weekly, platform)
        if data is None:
            print("  Skipping — insufficient data")
            continue

        pre_len = len(data.loc[pre_period[0]:pre_period[1]])
        post_len = len(data.loc[post_period[0]:post_period[1]])
        pre_ivr = data.loc[pre_period[0]:pre_period[1], "ivr"].mean()
        post_ivr = data.loc[post_period[0]:post_period[1], "ivr"].mean()
        print(f"  Pre: {pre_period[0].date()} to {pre_period[1].date()} ({pre_len} wks, avg IVR={pre_ivr:.4f})")
        print(f"  Post: {post_period[0].date()} to {post_period[1].date()} ({post_len} wks, avg IVR={post_ivr:.4f})")
        print(f"  IVR ratio (post/pre): {post_ivr/pre_ivr:.2f}x")

        print(f"\n  STEP 1: VIF")
        vif_clean = run_vif_check(data, ALL_CANDIDATES)

        print(f"\n  STEP 2: BIC")
        bic_best = run_bic_selection(data, vif_clean, pre_period)

        print(f"\n  STEP 3: SENSITIVITY")
        run_sensitivity(data, bic_best, pre_period, post_period)

        print(f"\n  STEP 4: PLACEBO")
        run_placebo(data, bic_best, pre_period)

        print(f"\n  STEP 5: FINAL")
        r = run_final(data, bic_best, pre_period, post_period, adv_id, adv_name)
        if r:
            results.append(r)

    print(f"\n{'=' * 90}")
    print("SUMMARY")
    print(f"{'=' * 90}")
    if results:
        for r in results:
            sig = "***" if r["significant"] else "   "
            print(f"  {r['advertiser_name']:20s}: {r['relative_effect']:+.2%}  p={r['p_value']:.4f} {sig}")
        rdf = pd.DataFrame(results)
        print(f"\n  Significant: {rdf['significant'].sum()}/{len(rdf)}")
        print(f"  Positive: {(rdf['relative_effect'] > 0).sum()}/{len(rdf)}")
        rdf.to_csv("outputs/ti_504_ci_allprosp_results.csv", index=False)

    print("\nDone.")


if __name__ == "__main__":
    main()
