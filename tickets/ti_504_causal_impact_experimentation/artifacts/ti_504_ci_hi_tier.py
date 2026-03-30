"""
TI-504: CausalImpact — HI-Tier Only (Apples-to-Apples)
========================================================

Segments historical prospecting impressions by HHST >= 6666 (HI tier)
and computes HI-only weekly IVR as the pre-period baseline. Compares to
experiment HI campaign IVR during post-period.

This is apples-to-apples: same intent tier, same type of IPs, just
before vs after Fangorn enablement.

Full TI-748 methodology: VIF → BIC → sensitivity → placebo → plot.
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

# HI experiment campaign IDs (HHST 10000)
HI_TREATMENT = {
    42692: 553878,
    40956: 552911,
    46920: 553698,
    42273: 553980,
    36420: 552755,
}

HI_CONTROL = {
    42692: 553860,
    40956: 552896,
    46920: 553680,
    42273: 553970,
    36420: 552732,
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

    # Historical HI-tier weekly IVR from old prospecting campaigns
    # Segment by advertiser_household_score >= 6666
    print("Loading historical HI-tier weekly IVR (HHST >= 6666)...")
    hi_historical = client.query(f"""
    WITH old_prospecting AS (
        SELECT c.campaign_id, c.advertiser_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.advertiser_id IN ({adv_list})
        AND c.funnel_level = 1 AND c.deleted = FALSE AND c.is_test = FALSE
    ),
    hi_impressions AS (
        SELECT cil.campaign_id, cil.advertiser_id, cil.ip,
            DATE_TRUNC(DATE(cil.time), WEEK(MONDAY)) AS week_start,
            cil.impression_id
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN old_prospecting op ON op.campaign_id = cil.campaign_id
        WHERE cil.advertiser_household_score >= 6666
        AND DATE(cil.time) >= '2025-01-01'
        AND DATE(cil.time) < '2026-03-04'
    ),
    weekly_imps AS (
        SELECT advertiser_id, week_start, COUNT(*) AS impressions
        FROM hi_impressions
        GROUP BY 1, 2
    ),
    hi_visits AS (
        SELECT cl.campaign_id, op.advertiser_id,
            DATE_TRUNC(DATE(cl.time), WEEK(MONDAY)) AS week_start,
            COUNT(*) AS vv
        FROM `dw-main-silver.logdata.clickpass_log` cl
        JOIN old_prospecting op ON op.campaign_id = cl.campaign_id
        WHERE DATE(cl.time) >= '2025-01-01'
        AND DATE(cl.time) < '2026-03-04'
        GROUP BY 1, 2, 3
    ),
    weekly_vv AS (
        SELECT advertiser_id, week_start, SUM(vv) AS vv
        FROM hi_visits
        GROUP BY 1, 2
    )
    SELECT wi.advertiser_id, wi.week_start, wi.impressions,
        COALESCE(wv.vv, 0) AS vv
    FROM weekly_imps wi
    LEFT JOIN weekly_vv wv ON wv.advertiser_id = wi.advertiser_id AND wv.week_start = wi.week_start
    ORDER BY 1, 2
    """).to_dataframe()
    hi_historical["week_start"] = pd.to_datetime(hi_historical["week_start"])
    for c in ["impressions", "vv"]:
        hi_historical[c] = pd.to_numeric(hi_historical[c], errors="coerce").astype(float)
    hi_historical["ivr"] = hi_historical["vv"] / hi_historical["impressions"]
    print(f"  {len(hi_historical)} rows")
    for aid, aname in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
        sub = hi_historical[hi_historical["advertiser_id"] == aid]
        if not sub.empty:
            avg = sub["vv"].sum() / sub["impressions"].sum()
            print(f"    {aname}: {len(sub)} weeks, HI-tier avg IVR={avg:.4f}")

    # Experiment HI campaign weekly data (treatment + control)
    all_exp_cids = list(HI_TREATMENT.values()) + list(HI_CONTROL.values())
    cid_str = ",".join(str(x) for x in all_exp_cids)

    print("\nLoading experiment HI campaign weekly data...")
    exp_imps = client.query(f"""
    SELECT campaign_id, DATE_TRUNC(DATE(time), WEEK(MONDAY)) AS week_start,
        COUNT(*) AS impressions
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE campaign_id IN ({cid_str})
    AND DATE(time) BETWEEN '2026-03-04' AND '2026-03-24'
    GROUP BY 1, 2
    """).to_dataframe()
    exp_vv = client.query(f"""
    SELECT campaign_id, DATE_TRUNC(DATE(time), WEEK(MONDAY)) AS week_start,
        COUNT(*) AS vv
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE campaign_id IN ({cid_str})
    AND DATE(time) BETWEEN '2026-03-04' AND '2026-03-24'
    GROUP BY 1, 2
    """).to_dataframe()

    exp = exp_imps.merge(exp_vv, on=["campaign_id", "week_start"], how="left")
    exp["vv"] = exp["vv"].fillna(0)
    exp["week_start"] = pd.to_datetime(exp["week_start"])

    # Map to advertiser and arm
    treat_map = {v: k for k, v in HI_TREATMENT.items()}
    ctrl_map = {v: k for k, v in HI_CONTROL.items()}
    exp["advertiser_id"] = exp["campaign_id"].map(lambda x: treat_map.get(x, ctrl_map.get(x)))
    exp["arm"] = exp["campaign_id"].map(lambda x: "treatment" if x in treat_map else "control")

    for c in ["impressions", "vv"]:
        exp[c] = exp[c].astype(float)

    print(f"  {len(exp)} rows")
    for aid, aname in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
        for arm in ["treatment", "control"]:
            sub = exp[(exp["advertiser_id"] == aid) & (exp["arm"] == arm)]
            if not sub.empty:
                avg = sub["vv"].sum() / sub["impressions"].sum()
                print(f"    {aname} ({arm}): {len(sub)} weeks, HI avg IVR={avg:.4f}")

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

    return hi_historical, exp, platform


def prepare_advertiser(adv_id, hi_historical, exp, platform):
    pre = hi_historical[hi_historical["advertiser_id"] == adv_id][["week_start", "ivr", "impressions"]].copy()
    pre["spend"] = pre["impressions"] * 0.01  # proxy for spend_change_pct
    pre = pre.set_index("week_start").sort_index()
    pre = pre[pre.index < EXPERIMENT_START]

    # Treatment HI post-period
    treat = exp[(exp["advertiser_id"] == adv_id) & (exp["arm"] == "treatment")].copy()
    if treat.empty or len(pre) < 12:
        return None, None, None
    treat["ivr"] = treat["vv"] / treat["impressions"]
    treat["spend"] = treat["impressions"] * 0.01
    treat = treat[["week_start", "ivr", "impressions", "spend"]].set_index("week_start").sort_index()

    combined = pd.concat([pre[["ivr", "impressions", "spend"]], treat[["ivr", "impressions", "spend"]]]).sort_index()
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


def run_vif(data, candidates):
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


def run_bic(data, vif_clean, pre_period):
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
    rdf = pd.DataFrame(results).sort_values("BIC")
    best = rdf.iloc[0]
    print(f"    BIC best: {best['covariates']} (BIC={best['BIC']:.1f}, R²={best['R2']:.3f})")
    for i, row in rdf.head(3).iterrows():
        print(f"      #{rdf.index.get_loc(i)+1}: {row['covariates']} BIC={row['BIC']:.1f}")
    return best["covariates"]


def run_sensitivity(data, covariates, pre_period, post_period):
    pre_data = data.loc[:pre_period[1]]
    results = []
    for n in [15, 20, 26, 32, 39, 52, len(pre_data)]:
        if n > len(pre_data) or n < 12:
            continue
        trimmed = [pre_data.index[-n], pre_period[1]]
        try:
            avail = [c for c in covariates if c in data.columns]
            ci_data = data[["ivr"] + avail].dropna(subset=["ivr"]).astype(float)
            ci_data[avail] = ci_data[avail].ffill().bfill()
            ci = CausalImpact(ci_data, trimmed, post_period)
            inf = ci.inferences[ci.inferences.index >= post_period[0]]
            pred = inf["preds"].mean()
            eff = inf["point_effects"].mean() / pred if pred != 0 else np.nan
            results.append({"wks": n, "effect": eff, "p": ci.p_value})
        except Exception:
            pass
    if results:
        signs = [r["effect"] > 0 for r in results]
        consistent = len(set(signs)) == 1
        for r in results:
            sig = "*" if r["p"] < 0.05 else " "
            print(f"    {int(r['wks']):3d} wks: {r['effect']:+.2%}  p={r['p']:.4f} {sig}")
        print(f"    Direction: {'CONSISTENT' if consistent else 'INCONSISTENT'}")
        return consistent
    return None


def run_placebo(data, covariates, pre_period, n_placebos=5):
    pre_data = data.loc[pre_period[0]:pre_period[1]]
    if len(pre_data) < 25:
        print(f"    Placebo: insufficient data ({len(pre_data)} wks)")
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

        print(f"    Predicted HI IVR: {pred:.6f}")
        print(f"    Actual HI IVR:    {actual:.6f}")
        print(f"    Effect: {eff:+.2%}  p={ci.p_value:.4f}  {'SIG' if ci.p_value < 0.05 else 'n.s.'}")

        plt.close("all")
        ci.plot(figsize=(14, 10))
        fig = plt.gcf()
        fig.suptitle(
            f"CausalImpact: {adv_name} — HI Tier Only (HHST ≥ 6666)\n"
            f"Pre: {pre_period[0].date()} – {pre_period[1].date()} | "
            f"Post: {post_period[0].date()} – {post_period[1].date()} | "
            f"Effect: {eff:+.2%} (p={ci.p_value:.4f})\n"
            f"Covariates: {', '.join(avail)}",
            fontsize=11, fontweight="bold")
        fig.subplots_adjust(top=0.85)
        fname = f"outputs/ti_504_ci_hi_tier_{adv_name.lower().replace(' ', '_')}.png"
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
    print("TI-504: CAUSAL IMPACT — HI TIER ONLY (APPLES-TO-APPLES)")
    print("=" * 90)

    hi_historical, exp, platform = load_all_data()

    results = []
    for adv_id, adv_name in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
        print(f"\n{'#' * 90}")
        print(f"# {adv_name} ({adv_id})")
        print(f"{'#' * 90}")

        data, pre_period, post_period = prepare_advertiser(adv_id, hi_historical, exp, platform)
        if data is None:
            print("  Skipping — insufficient data")
            continue

        pre_len = len(data.loc[pre_period[0]:pre_period[1]])
        post_len = len(data.loc[post_period[0]:post_period[1]])
        pre_ivr = data.loc[pre_period[0]:pre_period[1], "ivr"].mean()
        post_ivr = data.loc[post_period[0]:post_period[1], "ivr"].mean()
        print(f"  Pre: {pre_period[0].date()} to {pre_period[1].date()} ({pre_len} wks, HI avg IVR={pre_ivr:.4f})")
        print(f"  Post: {post_period[0].date()} to {post_period[1].date()} ({post_len} wks, HI avg IVR={post_ivr:.4f})")
        print(f"  IVR ratio (post/pre): {post_ivr/pre_ivr:.2f}x")

        print(f"\n  VIF:")
        vif_clean = run_vif(data, ALL_CANDIDATES)

        print(f"\n  BIC:")
        bic_best = run_bic(data, vif_clean, pre_period)

        print(f"\n  SENSITIVITY:")
        run_sensitivity(data, bic_best, pre_period, post_period)

        print(f"\n  PLACEBO:")
        run_placebo(data, bic_best, pre_period)

        print(f"\n  FINAL:")
        r = run_final(data, bic_best, pre_period, post_period, adv_id, adv_name)
        if r:
            results.append(r)

    print(f"\n{'=' * 90}")
    print("SUMMARY — HI TIER CAUSAL IMPACT")
    print(f"{'=' * 90}")
    if results:
        for r in results:
            sig = "***" if r["significant"] else "   "
            print(f"  {r['advertiser_name']:20s}: {r['relative_effect']:+.2%}  p={r['p_value']:.4f} {sig}  covs={r['covariates']}")
        rdf = pd.DataFrame(results)
        print(f"\n  Significant: {rdf['significant'].sum()}/{len(rdf)}")
        print(f"  Positive: {(rdf['relative_effect'] > 0).sum()}/{len(rdf)}")
        print(f"  Median effect: {rdf['relative_effect'].median():+.2%}")
        rdf.to_csv("outputs/ti_504_ci_hi_tier_results.csv", index=False)

    print("\nDone.")


if __name__ == "__main__":
    main()
