"""
TI-504: CausalImpact Visualizations for Fangorn Experiment
============================================================

Generates the standard CausalImpact 3-panel plots (observed vs predicted,
pointwise effect, cumulative effect) for each advertiser.

Approach:
  - Pre-period: parent campaign weekly IVR (before experiment)
  - Post-period: treatment arm weekly IVR (during experiment)
  - Covariates: control arm IVR during experiment, platform-level metrics
"""

import warnings

import matplotlib
matplotlib.use("Agg")  # non-interactive backend, no popups

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from causalimpact import CausalImpact

warnings.filterwarnings("ignore")

BQ_PROJECT = "dw-main-silver"

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

# Treatment campaign IDs (funnel_level=1, prospecting)
TREATMENT_CAMPAIGNS = {
    42692: [553902, 553878, 553929, 553955],
    40956: [552928, 552911, 552948, 552960],
    46920: [553722, 553698, 553754, 553769],
    42273: [554027, 553980, 554052, 554068],
    36420: [552767, 552755, 552779, 552800],
}

CONTROL_CAMPAIGNS = {
    42692: [553884, 553860, 553911, 553935],
    40956: [552917, 552896, 552936, 552949],
    46920: [553710, 553680, 553746, 553760],
    42273: [554015, 553970, 554043, 554061],
    36420: [552762, 552732, 552770, 552784],
}


def load_parent_weekly(client):
    """Load parent campaign weekly IVR from sum_by_campaign_by_day."""
    all_cg_ids = []
    for cg_list in PARENT_CAMPAIGN_GROUPS.values():
        all_cg_ids.extend(cg_list)
    cg_str = ",".join(str(x) for x in all_cg_ids)

    df = client.query(f"""
    WITH prospecting AS (
        SELECT c.campaign_id, c.advertiser_id, c.campaign_group_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.campaign_group_id IN ({cg_str})
        AND c.funnel_level = 1 AND c.deleted = FALSE
    )
    SELECT pc.advertiser_id,
        DATE_TRUNC(s.day, WEEK(MONDAY)) AS week_start,
        SUM(s.impressions) AS impressions,
        SUM(s.clicks + s.views + COALESCE(s.competing_views, 0)) AS vv,
        SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN prospecting pc ON pc.campaign_id = s.campaign_id
    WHERE s.day >= '2025-06-01'
    AND s.impressions > 0
    GROUP BY 1, 2
    ORDER BY 1, 2
    """).to_dataframe()

    df["week_start"] = pd.to_datetime(df["week_start"])
    df["ivr"] = df["vv"].astype(float) / df["impressions"].astype(float)
    return df


def load_experiment_weekly(client):
    """Load experiment campaign weekly IVR from cost_impression_log + clickpass_log."""
    all_campaign_ids = []
    campaign_to_arm = {}
    campaign_to_adv = {}

    for adv_id in ADVERTISER_NAMES:
        for cid in TREATMENT_CAMPAIGNS[adv_id]:
            all_campaign_ids.append(cid)
            campaign_to_arm[cid] = "treatment"
            campaign_to_adv[cid] = adv_id
        for cid in CONTROL_CAMPAIGNS[adv_id]:
            all_campaign_ids.append(cid)
            campaign_to_arm[cid] = "control"
            campaign_to_adv[cid] = adv_id

    cid_str = ",".join(str(x) for x in all_campaign_ids)

    imps = client.query(f"""
    SELECT campaign_id,
        DATE_TRUNC(DATE(time), WEEK(MONDAY)) AS week_start,
        COUNT(*) AS impressions
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE campaign_id IN ({cid_str})
    AND DATE(time) BETWEEN '2026-03-04' AND '2026-03-24'
    GROUP BY 1, 2
    """).to_dataframe()

    vvs = client.query(f"""
    SELECT campaign_id,
        DATE_TRUNC(DATE(time), WEEK(MONDAY)) AS week_start,
        COUNT(*) AS vv
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE campaign_id IN ({cid_str})
    AND DATE(time) BETWEEN '2026-03-04' AND '2026-03-24'
    GROUP BY 1, 2
    """).to_dataframe()

    merged = imps.merge(vvs, on=["campaign_id", "week_start"], how="left")
    merged["vv"] = merged["vv"].fillna(0)
    merged["week_start"] = pd.to_datetime(merged["week_start"])
    merged["arm"] = merged["campaign_id"].map(campaign_to_arm)
    merged["advertiser_id"] = merged["campaign_id"].map(campaign_to_adv)

    # Aggregate by advertiser, arm, week
    weekly = merged.groupby(["advertiser_id", "arm", "week_start"]).agg(
        impressions=("impressions", "sum"), vv=("vv", "sum")
    ).reset_index()
    weekly["ivr"] = weekly["vv"].astype(float) / weekly["impressions"].astype(float)

    return weekly


def load_platform_covariates(client):
    """Load platform-level weekly IVR from non-experiment campaigns."""
    df = client.query("""
    WITH prospecting AS (
        SELECT c.campaign_id, c.advertiser_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.funnel_level = 1 AND c.deleted = FALSE AND c.is_test = FALSE
    )
    SELECT DATE_TRUNC(s.day, WEEK(MONDAY)) AS week_start,
        SUM(s.impressions) AS impressions,
        SUM(s.clicks + s.views + COALESCE(s.competing_views, 0)) AS vv,
        SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN prospecting pc ON pc.campaign_id = s.campaign_id
    WHERE s.day >= '2025-06-01' AND s.impressions > 0
    GROUP BY 1
    ORDER BY 1
    """).to_dataframe()

    df["week_start"] = pd.to_datetime(df["week_start"])
    df["platform_ivr"] = df["vv"].astype(float) / df["impressions"].astype(float)
    df["platform_spend"] = df["spend"].astype(float) / 1e6
    return df[["week_start", "platform_ivr", "platform_spend"]]


def run_causal_impact_with_plots(adv_id, adv_name, parent_weekly, experiment_weekly, platform):
    """Run CausalImpact and generate the 3-panel plot for one advertiser."""
    # Pre-period: parent campaign weekly IVR
    parent = parent_weekly[parent_weekly["advertiser_id"] == adv_id][["week_start", "ivr"]].copy()
    parent = parent.rename(columns={"ivr": "y"}).set_index("week_start").sort_index()

    # Post-period: treatment arm weekly IVR
    treat = experiment_weekly[
        (experiment_weekly["advertiser_id"] == adv_id) & (experiment_weekly["arm"] == "treatment")
    ][["week_start", "ivr"]].copy()
    treat = treat.groupby("week_start").agg(
        ivr=("ivr", "mean")
    ).rename(columns={"ivr": "y"})

    # Control arm as covariate during post-period
    ctrl = experiment_weekly[
        (experiment_weekly["advertiser_id"] == adv_id) & (experiment_weekly["arm"] == "control")
    ][["week_start", "ivr"]].copy()
    ctrl = ctrl.groupby("week_start").agg(
        ivr=("ivr", "mean")
    ).rename(columns={"ivr": "control_ivr"})

    if len(parent) < 10 or treat.empty:
        print(f"  {adv_name}: insufficient data (parent={len(parent)}, treatment={len(treat)})")
        return None

    # Cut pre-period before experiment start (week of March 2 is the last pre week)
    experiment_start = pd.Timestamp("2026-03-02")
    parent = parent[parent.index < experiment_start]

    if len(parent) < 10:
        print(f"  {adv_name}: insufficient pre-period data after cutoff ({len(parent)} weeks)")
        return None

    # Build combined dataframe
    # Pre-period: y from parent, covariates from platform
    pre = parent.merge(platform.set_index("week_start"), left_index=True, right_index=True, how="inner")

    # Post-period: y from treatment, covariates from platform + control
    post = treat.merge(platform.set_index("week_start"), left_index=True, right_index=True, how="inner")
    post = post.merge(ctrl, left_index=True, right_index=True, how="left")

    # For pre-period, we don't have control_ivr — fill with parent IVR as proxy
    pre["control_ivr"] = pre["y"]

    # Combine
    combined = pd.concat([pre, post]).sort_index()
    combined = combined[["y", "platform_ivr", "platform_spend", "control_ivr"]].astype(float)

    # Drop any NaN rows
    combined = combined.dropna()

    if len(combined) < 15:
        print(f"  {adv_name}: insufficient combined data ({len(combined)} weeks)")
        return None

    # Define periods
    pre_period = [pre.index[0], pre.index[-1]]
    post_period = [post.index[0], post.index[-1]]

    print(f"\n  {adv_name} ({adv_id}):")
    print(f"    Pre:  {pre_period[0].date()} to {pre_period[1].date()} ({len(pre)} weeks)")
    print(f"    Post: {post_period[0].date()} to {post_period[1].date()} ({len(post)} weeks)")

    try:
        ci = CausalImpact(combined, pre_period, post_period)

        # Print summary
        inf = ci.inferences[ci.inferences.index >= post_period[0]]
        predicted = inf["preds"].mean()
        actual = ci.post_data.iloc[:, 0].mean()
        abs_eff = inf["point_effects"].mean()
        rel_eff = abs_eff / predicted if predicted != 0 else np.nan

        print(f"    Predicted IVR (counterfactual): {predicted:.6f}")
        print(f"    Actual IVR (treatment):         {actual:.6f}")
        print(f"    Relative effect: {rel_eff:+.2%}")
        print(f"    p-value: {ci.p_value:.4f}")
        print(f"    Significant: {'YES' if ci.p_value < 0.05 else 'no'}")

        # Generate the 3-panel plot
        plt.close("all")
        ci.plot(figsize=(14, 10))
        fig = plt.gcf()
        fig.suptitle(f"CausalImpact: {adv_name}\n"
                     f"Pre: {pre_period[0].date()} – {pre_period[1].date()} | "
                     f"Post: {post_period[0].date()} – {post_period[1].date()} | "
                     f"Effect: {rel_eff:+.2%} (p={ci.p_value:.4f})",
                     fontsize=12, fontweight="bold")
        fig.subplots_adjust(top=0.88)
        fname = f"outputs/ti_504_causal_impact_{adv_name.lower().replace(' ', '_')}.png"
        fig.savefig(fname, dpi=150)
        plt.close("all")
        print(f"    Saved: {fname}")

        return {
            "advertiser_id": adv_id,
            "advertiser_name": adv_name,
            "pre_weeks": len(pre),
            "post_weeks": len(post),
            "predicted_ivr": predicted,
            "actual_ivr": actual,
            "relative_effect": rel_eff,
            "p_value": ci.p_value,
            "significant": ci.p_value < 0.05,
        }
    except Exception as e:
        print(f"    Error: {e}")
        return None


def main():
    print("=" * 80)
    print("TI-504: CAUSAL IMPACT VISUALIZATIONS")
    print("=" * 80)

    client = bigquery.Client(project=BQ_PROJECT)

    print("\nLoading parent campaign data...")
    parent_weekly = load_parent_weekly(client)
    print(f"  {len(parent_weekly)} rows, {parent_weekly['advertiser_id'].nunique()} advertisers")

    print("Loading experiment campaign data...")
    experiment_weekly = load_experiment_weekly(client)
    print(f"  {len(experiment_weekly)} rows")

    print("Loading platform covariates...")
    platform = load_platform_covariates(client)
    print(f"  {len(platform)} weeks")

    print("\n" + "=" * 80)
    print("RUNNING CAUSAL IMPACT PER ADVERTISER")
    print("=" * 80)

    results = []
    for adv_id, adv_name in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
        r = run_causal_impact_with_plots(adv_id, adv_name, parent_weekly, experiment_weekly, platform)
        if r:
            results.append(r)

    if results:
        print("\n" + "=" * 80)
        print("CAUSAL IMPACT SUMMARY")
        print("=" * 80)
        rdf = pd.DataFrame(results)
        rdf["effect_pct"] = rdf["relative_effect"].apply(lambda x: f"{x:+.2%}")
        rdf["p_fmt"] = rdf["p_value"].apply(lambda x: f"{x:.4f}")
        print(rdf[["advertiser_name", "pre_weeks", "post_weeks",
                    "effect_pct", "p_fmt", "significant"]].to_string(index=False))

    print("\nDone.")


if __name__ == "__main__":
    main()
