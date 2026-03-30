"""
TI-504: Fangorn AIS Experiment — RCT Analysis
===============================================

Two analysis tracks:
  1. Direct RCT — head-to-head comparison of control vs treatment by intent group
     (t-tests, bootstrap CIs, daily time series)
  2. Synthetic Control — CausalImpact using parent campaigns' pre-period data

Experiment: 5 advertisers × 4 intent groups × 2 arms (control/treatment)
Run period: 2026-03-04 to 2026-03-24 (21 days)
Primary metric: IVR (impression-to-visit rate = VV / impressions)
"""

import warnings
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

warnings.filterwarnings("ignore")

# =============================================================================
# CAMPAIGN MAPPING
# =============================================================================

ADVERTISER_NAMES = {
    36420: "Zumba Fitness",
    40956: "Edward Martin",
    42273: "Reedsy",
    42692: "Collector Store",
    46920: "G-Shock",
}

CAMPAIGN_MAP = {
    # Collector Store (42692) — Control
    553884: {"advertiser_id": 42692, "arm": "control", "intent_group": "PP"},
    553860: {"advertiser_id": 42692, "arm": "control", "intent_group": "HI"},
    553911: {"advertiser_id": 42692, "arm": "control", "intent_group": "MI"},
    553935: {"advertiser_id": 42692, "arm": "control", "intent_group": "MI_PP"},
    # Collector Store (42692) — Treatment
    553902: {"advertiser_id": 42692, "arm": "treatment", "intent_group": "PP"},
    553878: {"advertiser_id": 42692, "arm": "treatment", "intent_group": "HI"},
    553929: {"advertiser_id": 42692, "arm": "treatment", "intent_group": "MI"},
    553955: {"advertiser_id": 42692, "arm": "treatment", "intent_group": "MI_PP"},
    # Edward Martin (40956) — Control
    552917: {"advertiser_id": 40956, "arm": "control", "intent_group": "PP"},
    552896: {"advertiser_id": 40956, "arm": "control", "intent_group": "HI"},
    552936: {"advertiser_id": 40956, "arm": "control", "intent_group": "MI"},
    552949: {"advertiser_id": 40956, "arm": "control", "intent_group": "MI_PP"},
    # Edward Martin (40956) — Treatment
    552928: {"advertiser_id": 40956, "arm": "treatment", "intent_group": "PP"},
    552911: {"advertiser_id": 40956, "arm": "treatment", "intent_group": "HI"},
    552948: {"advertiser_id": 40956, "arm": "treatment", "intent_group": "MI"},
    552960: {"advertiser_id": 40956, "arm": "treatment", "intent_group": "MI_PP"},
    # G-Shock (46920) — Control
    553710: {"advertiser_id": 46920, "arm": "control", "intent_group": "PP"},
    553680: {"advertiser_id": 46920, "arm": "control", "intent_group": "HI"},
    553746: {"advertiser_id": 46920, "arm": "control", "intent_group": "MI"},
    553760: {"advertiser_id": 46920, "arm": "control", "intent_group": "MI_PP"},
    # G-Shock (46920) — Treatment
    553722: {"advertiser_id": 46920, "arm": "treatment", "intent_group": "PP"},
    553698: {"advertiser_id": 46920, "arm": "treatment", "intent_group": "HI"},
    553754: {"advertiser_id": 46920, "arm": "treatment", "intent_group": "MI"},
    553769: {"advertiser_id": 46920, "arm": "treatment", "intent_group": "MI_PP"},
    # Reedsy (42273) — Control
    554015: {"advertiser_id": 42273, "arm": "control", "intent_group": "PP"},
    553970: {"advertiser_id": 42273, "arm": "control", "intent_group": "HI"},
    554043: {"advertiser_id": 42273, "arm": "control", "intent_group": "MI"},
    554061: {"advertiser_id": 42273, "arm": "control", "intent_group": "MI_PP"},
    # Reedsy (42273) — Treatment
    554027: {"advertiser_id": 42273, "arm": "treatment", "intent_group": "PP"},
    553980: {"advertiser_id": 42273, "arm": "treatment", "intent_group": "HI"},
    554052: {"advertiser_id": 42273, "arm": "treatment", "intent_group": "MI"},
    554068: {"advertiser_id": 42273, "arm": "treatment", "intent_group": "MI_PP"},
    # Zumba Fitness (36420) — Control
    552762: {"advertiser_id": 36420, "arm": "control", "intent_group": "PP"},
    552732: {"advertiser_id": 36420, "arm": "control", "intent_group": "HI"},
    552770: {"advertiser_id": 36420, "arm": "control", "intent_group": "MI"},
    552784: {"advertiser_id": 36420, "arm": "control", "intent_group": "MI_PP"},
    # Zumba Fitness (36420) — Treatment
    552767: {"advertiser_id": 36420, "arm": "treatment", "intent_group": "PP"},
    552755: {"advertiser_id": 36420, "arm": "treatment", "intent_group": "HI"},
    552779: {"advertiser_id": 36420, "arm": "treatment", "intent_group": "MI"},
    552800: {"advertiser_id": 36420, "arm": "treatment", "intent_group": "MI_PP"},
}

# Parent prospecting campaigns (from "Changes for Live Campaigns" in Nick's spreadsheet)
PARENT_CAMPAIGNS = {
    42692: [94979],   # Collector Store — LP Prospecting - KW Audience
    40956: [84250],   # Edward Martin — MNTN_EM_PR_CTV_ROAS
    46920: [101662],  # G-Shock — G-SHOCK TRUE TO THE CORE
    42273: [86917],   # Reedsy — TELEVISION_PROSPECTING Campaign Group
    36420: [57130],   # Zumba Fitness — CTV Prospecting
}


# =============================================================================
# DATA LOADING
# =============================================================================

def load_experiment_data(csv_path="outputs/ti_504_experiment_daily_metrics.csv"):
    """Load the pre-pulled experiment daily metrics."""
    df = pd.read_csv(csv_path)
    df["day"] = pd.to_datetime(df["day"])
    df["ivr"] = df["vv"] / df["impressions"]
    df["advertiser_name"] = df["advertiser_id"].map(ADVERTISER_NAMES)
    return df


# =============================================================================
# TRACK 1: DIRECT RCT ANALYSIS
# =============================================================================

def bootstrap_ci(control_vals, treatment_vals, n_boot=10000, alpha=0.05):
    """Bootstrap confidence interval for difference in means."""
    rng = np.random.default_rng(42)
    diffs = []
    for _ in range(n_boot):
        c_sample = rng.choice(control_vals, size=len(control_vals), replace=True)
        t_sample = rng.choice(treatment_vals, size=len(treatment_vals), replace=True)
        diffs.append(t_sample.mean() - c_sample.mean())
    diffs = np.array(diffs)
    return np.percentile(diffs, [alpha / 2 * 100, (1 - alpha / 2) * 100])


def run_rct_comparison(df, level="intent_group"):
    """
    Direct head-to-head comparison of control vs treatment.
    level: "intent_group" for per-group analysis, "advertiser" for aggregate.
    """
    print("\n" + "=" * 90)
    print(f"TRACK 1: DIRECT RCT COMPARISON — by {level}")
    print("=" * 90)

    if level == "intent_group":
        group_cols = ["advertiser_id", "advertiser_name", "intent_group"]
    else:
        group_cols = ["advertiser_id", "advertiser_name"]

    results = []

    # Get unique groups
    groups = df[group_cols].drop_duplicates().values

    for group_vals in groups:
        if level == "intent_group":
            adv_id, adv_name, ig = group_vals
            mask_base = (df["advertiser_id"] == adv_id) & (df["intent_group"] == ig)
            label = f"{adv_name} — {ig}"
        else:
            adv_id, adv_name = group_vals
            mask_base = df["advertiser_id"] == adv_id
            label = adv_name

        control = df[mask_base & (df["arm"] == "control")]
        treatment = df[mask_base & (df["arm"] == "treatment")]

        if len(control) == 0 or len(treatment) == 0:
            continue

        # Aggregate totals for rate calculation
        c_imps = control["impressions"].sum()
        c_vv = control["vv"].sum()
        t_imps = treatment["impressions"].sum()
        t_vv = treatment["vv"].sum()

        c_ivr = c_vv / c_imps if c_imps > 0 else 0
        t_ivr = t_vv / t_imps if t_imps > 0 else 0

        # Daily IVR for statistical tests
        c_daily_ivr = control.groupby("day").agg(
            impressions=("impressions", "sum"), vv=("vv", "sum")
        )
        c_daily_ivr["ivr"] = c_daily_ivr["vv"] / c_daily_ivr["impressions"]

        t_daily_ivr = treatment.groupby("day").agg(
            impressions=("impressions", "sum"), vv=("vv", "sum")
        )
        t_daily_ivr["ivr"] = t_daily_ivr["vv"] / t_daily_ivr["impressions"]

        # Welch's t-test on daily IVRs
        t_stat, p_value = stats.ttest_ind(
            t_daily_ivr["ivr"].values, c_daily_ivr["ivr"].values, equal_var=False
        )

        # Mann-Whitney U test (non-parametric)
        u_stat, u_pvalue = stats.mannwhitneyu(
            t_daily_ivr["ivr"].values, c_daily_ivr["ivr"].values, alternative="two-sided"
        )

        # Bootstrap CI for difference
        boot_ci = bootstrap_ci(c_daily_ivr["ivr"].values, t_daily_ivr["ivr"].values)

        # Relative lift
        lift = (t_ivr - c_ivr) / c_ivr if c_ivr > 0 else np.nan

        results.append({
            "group": label,
            "advertiser_id": adv_id,
            "intent_group": ig if level == "intent_group" else "ALL",
            "control_impressions": c_imps,
            "treatment_impressions": t_imps,
            "control_ivr": c_ivr,
            "treatment_ivr": t_ivr,
            "ivr_lift": lift,
            "t_stat": t_stat,
            "t_pvalue": p_value,
            "u_pvalue": u_pvalue,
            "boot_ci_lower": boot_ci[0],
            "boot_ci_upper": boot_ci[1],
            "significant_t": p_value < 0.05,
            "significant_u": u_pvalue < 0.05,
        })

    results_df = pd.DataFrame(results)

    # Print results
    for _, r in results_df.iterrows():
        sig_marker = " ***" if r["significant_t"] else ""
        print(f"\n  {r['group']}")
        print(f"    Control IVR:   {r['control_ivr']:.6f}  ({r['control_impressions']:>10,} imps)")
        print(f"    Treatment IVR: {r['treatment_ivr']:.6f}  ({r['treatment_impressions']:>10,} imps)")
        print(f"    Lift: {r['ivr_lift']:+.2%}{sig_marker}")
        print(f"    t-test p={r['t_pvalue']:.4f}, Mann-Whitney p={r['u_pvalue']:.4f}")
        print(f"    Bootstrap 95% CI for diff: [{r['boot_ci_lower']:.6f}, {r['boot_ci_upper']:.6f}]")

    # Summary table
    print("\n" + "-" * 90)
    print("SUMMARY TABLE:")
    print("-" * 90)
    summary = results_df[["group", "control_ivr", "treatment_ivr", "ivr_lift",
                           "t_pvalue", "significant_t"]].copy()
    summary["control_ivr"] = summary["control_ivr"].apply(lambda x: f"{x:.6f}")
    summary["treatment_ivr"] = summary["treatment_ivr"].apply(lambda x: f"{x:.6f}")
    summary["ivr_lift"] = summary["ivr_lift"].apply(lambda x: f"{x:+.2%}")
    summary["t_pvalue"] = summary["t_pvalue"].apply(lambda x: f"{x:.4f}")
    print(summary.to_string(index=False))

    # Overall significance count
    n_sig = results_df["significant_t"].sum()
    n_total = len(results_df)
    n_pos = (results_df["ivr_lift"] > 0).sum()
    print(f"\n  Significant: {n_sig}/{n_total}")
    print(f"  Positive lift: {n_pos}/{n_total}")

    return results_df


def run_pooled_analysis(df):
    """Pool all advertisers for an overall treatment effect estimate."""
    print("\n" + "=" * 90)
    print("POOLED ANALYSIS — All advertisers combined")
    print("=" * 90)

    for ig in ["PP", "HI", "MI", "MI_PP", "ALL"]:
        if ig == "ALL":
            subset = df
        else:
            subset = df[df["intent_group"] == ig]

        c = subset[subset["arm"] == "control"]
        t = subset[subset["arm"] == "treatment"]

        c_daily = c.groupby("day").agg(impressions=("impressions", "sum"), vv=("vv", "sum"))
        t_daily = t.groupby("day").agg(impressions=("impressions", "sum"), vv=("vv", "sum"))
        c_daily["ivr"] = c_daily["vv"] / c_daily["impressions"]
        t_daily["ivr"] = t_daily["vv"] / t_daily["impressions"]

        c_ivr = c["vv"].sum() / c["impressions"].sum()
        t_ivr = t["vv"].sum() / t["impressions"].sum()
        lift = (t_ivr - c_ivr) / c_ivr

        t_stat, p_val = stats.ttest_ind(t_daily["ivr"].values, c_daily["ivr"].values, equal_var=False)

        sig = "***" if p_val < 0.05 else ""
        label = ig if ig != "ALL" else "ALL GROUPS"
        print(f"  {label:12s}: C={c_ivr:.6f}  T={t_ivr:.6f}  Lift={lift:+.2%}  p={p_val:.4f} {sig}")


# =============================================================================
# TRACK 2: SYNTHETIC CONTROL (CausalImpact)
# =============================================================================

def run_synthetic_control(experiment_df):
    """
    Use CausalImpact with parent campaigns as pre-period donors.
    Requires parent campaign data to be loaded separately.
    """
    try:
        from causalimpact import CausalImpact
    except ImportError:
        print("\n[SKIP] causalimpact not installed — run: pip install causalimpact")
        return None

    from google.cloud import bigquery

    print("\n" + "=" * 90)
    print("TRACK 2: SYNTHETIC CONTROL (CausalImpact)")
    print("=" * 90)

    # Pull parent campaign weekly data for pre-period
    client = bigquery.Client(project="dw-main-silver")

    parent_cg_ids = []
    for adv_id, cg_ids in PARENT_CAMPAIGNS.items():
        parent_cg_ids.extend(cg_ids)

    parent_cg_list = ",".join(str(x) for x in parent_cg_ids)

    print("Loading parent campaign pre-period data...")
    parent_data = client.query(f"""
    WITH prospecting AS (
        SELECT c.campaign_id, c.advertiser_id, c.campaign_group_id
        FROM `dw-main-bronze.integrationprod.campaigns` c
        WHERE c.campaign_group_id IN ({parent_cg_list})
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

    if parent_data.empty:
        print("  No parent campaign data found in sum_by_campaign_by_day")
        return None

    parent_data["week_start"] = pd.to_datetime(parent_data["week_start"])
    parent_data["ivr"] = parent_data["vv"] / parent_data["impressions"]

    # Experiment weekly data
    exp_weekly = experiment_df.groupby(["advertiser_id", "arm",
                                         pd.Grouper(key="day", freq="W-MON")]).agg(
        impressions=("impressions", "sum"), vv=("vv", "sum")
    ).reset_index()
    exp_weekly["ivr"] = exp_weekly["vv"] / exp_weekly["impressions"]

    results = []
    intervention_date = pd.Timestamp("2026-03-03")  # week before experiment

    for adv_id, adv_name in ADVERTISER_NAMES.items():
        parent = parent_data[parent_data["advertiser_id"] == adv_id].copy()
        if len(parent) < 10:
            print(f"\n  {adv_name}: insufficient pre-period data ({len(parent)} weeks)")
            continue

        # Treatment arm weekly IVR during experiment
        treat_exp = exp_weekly[
            (exp_weekly["advertiser_id"] == adv_id) & (exp_weekly["arm"] == "treatment")
        ].copy()

        if treat_exp.empty:
            continue

        # Build time series: parent pre-period + treatment post-period
        parent_ts = parent[["week_start", "ivr"]].set_index("week_start").sort_index()
        parent_ts = parent_ts[parent_ts.index < intervention_date]

        treat_ts = treat_exp.groupby("day").agg(
            impressions=("impressions", "sum"), vv=("vv", "sum")
        )
        treat_ts["ivr"] = treat_ts["vv"] / treat_ts["impressions"]

        # Combine into single series
        combined = pd.concat([
            parent_ts[["ivr"]],
            treat_ts[["ivr"]]
        ]).sort_index()

        if len(combined) < 15:
            print(f"\n  {adv_name}: insufficient combined data ({len(combined)} points)")
            continue

        pre_period = [combined.index[0], parent_ts.index[-1]]
        post_period = [treat_ts.index[0], treat_ts.index[-1]]

        try:
            ci = CausalImpact(combined.astype(float), pre_period, post_period)
            inf = ci.inferences[ci.inferences.index >= post_period[0]]
            predicted = inf["preds"].mean()
            abs_eff = inf["point_effects"].mean()
            rel_eff = abs_eff / predicted if predicted != 0 else np.nan

            print(f"\n  {adv_name} ({adv_id}):")
            print(f"    Pre-period: {pre_period[0].date()} to {pre_period[1].date()} ({len(parent_ts)} weeks)")
            print(f"    Post-period: {post_period[0].date()} to {post_period[1].date()}")
            print(f"    Predicted IVR: {predicted:.6f}")
            print(f"    Actual IVR:    {ci.post_data.iloc[:, 0].mean():.6f}")
            print(f"    Effect: {rel_eff:+.2%} (p={ci.p_value:.4f})")
            sig = "***" if ci.p_value < 0.05 else ""
            print(f"    Significant: {sig if sig else 'No'}")

            results.append({
                "advertiser_id": adv_id,
                "advertiser_name": adv_name,
                "pre_weeks": len(parent_ts),
                "predicted_ivr": predicted,
                "actual_ivr": ci.post_data.iloc[:, 0].mean(),
                "relative_effect": rel_eff,
                "p_value": ci.p_value,
                "significant": ci.p_value < 0.05,
            })
        except Exception as e:
            print(f"\n  {adv_name}: CausalImpact failed — {e}")

    if results:
        print("\n" + "-" * 90)
        print("SYNTHETIC CONTROL SUMMARY:")
        print("-" * 90)
        rdf = pd.DataFrame(results)
        rdf["effect_pct"] = rdf["relative_effect"].apply(lambda x: f"{x:+.2%}")
        rdf["p_fmt"] = rdf["p_value"].apply(lambda x: f"{x:.4f}")
        print(rdf[["advertiser_name", "pre_weeks", "effect_pct", "p_fmt", "significant"]].to_string(index=False))

    return results


# =============================================================================
# VISUALIZATIONS
# =============================================================================

OUTPUT_DIR = Path("outputs")

def plot_daily_ivr_by_advertiser(df):
    """Daily IVR time series for control vs treatment, one subplot per advertiser."""
    fig, axes = plt.subplots(2, 3, figsize=(18, 10), sharey=False)
    axes = axes.flatten()

    for idx, (adv_id, adv_name) in enumerate(sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1])):
        ax = axes[idx]
        adv_data = df[df["advertiser_id"] == adv_id]

        for arm, color, ls in [("control", "#2196F3", "-"), ("treatment", "#FF5722", "-")]:
            arm_data = adv_data[adv_data["arm"] == arm].groupby("day").agg(
                impressions=("impressions", "sum"), vv=("vv", "sum")
            )
            arm_data["ivr"] = arm_data["vv"] / arm_data["impressions"]
            ax.plot(arm_data.index, arm_data["ivr"] * 100, color=color, ls=ls,
                    marker="o", markersize=3, linewidth=1.5, label=arm.title(), alpha=0.85)

        ax.set_title(adv_name, fontsize=12, fontweight="bold")
        ax.set_ylabel("IVR (%)")
        ax.legend(fontsize=9)
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.grid(axis="y", alpha=0.3)

    axes[5].set_visible(False)  # hide 6th subplot
    fig.suptitle("Daily IVR: Control vs Treatment by Advertiser", fontsize=14, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "ti_504_daily_ivr_by_advertiser.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / 'ti_504_daily_ivr_by_advertiser.png'}")


def plot_daily_ivr_by_intent_group(df):
    """Daily IVR time series, one subplot per intent group, all advertisers overlaid."""
    intent_groups = ["PP", "HI", "MI", "MI_PP"]
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    axes = axes.flatten()

    colors = {"Collector Store": "#E91E63", "Edward Martin": "#9C27B0",
              "G-Shock": "#00BCD4", "Reedsy": "#4CAF50", "Zumba Fitness": "#FF9800"}

    for idx, ig in enumerate(intent_groups):
        ax = axes[idx]
        ig_data = df[df["intent_group"] == ig]

        for adv_id, adv_name in sorted(ADVERTISER_NAMES.items(), key=lambda x: x[1]):
            adv_ig = ig_data[ig_data["advertiser_id"] == adv_id]
            for arm, ls in [("control", "--"), ("treatment", "-")]:
                arm_data = adv_ig[adv_ig["arm"] == arm]
                if arm_data.empty:
                    continue
                label = f"{adv_name} ({arm[0].upper()})" if arm == "treatment" else None
                ax.plot(arm_data["day"], arm_data["ivr"] * 100,
                        color=colors[adv_name], ls=ls, linewidth=1.2,
                        marker="o" if arm == "treatment" else None, markersize=2,
                        alpha=0.7 if arm == "control" else 0.9, label=label)

        ig_labels = {"PP": "Peak Performance", "HI": "High Intent",
                     "MI": "Mid Intent", "MI_PP": "Mid Intent + Peak Performance"}
        ax.set_title(ig_labels[ig], fontsize=12, fontweight="bold")
        ax.set_ylabel("IVR (%)")
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        ax.grid(axis="y", alpha=0.3)
        if idx == 0:
            ax.legend(fontsize=7, loc="upper right")

    fig.suptitle("Daily IVR by Intent Group (solid=treatment, dashed=control)", fontsize=14, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "ti_504_daily_ivr_by_intent_group.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / 'ti_504_daily_ivr_by_intent_group.png'}")


def plot_lift_heatmap(rct_ig_results):
    """Heatmap of IVR lift (%) by advertiser × intent group. Stars for significance."""
    pivot_lift = rct_ig_results.pivot_table(
        index="group", columns="intent_group", values="ivr_lift", aggfunc="first"
    )
    pivot_sig = rct_ig_results.pivot_table(
        index="group", columns="intent_group", values="significant_t", aggfunc="first"
    )

    # Rebuild with advertiser names as index
    adv_names = [ADVERTISER_NAMES[aid] for aid in sorted(ADVERTISER_NAMES.keys(), key=lambda x: ADVERTISER_NAMES[x])]
    intent_order = ["PP", "HI", "MI", "MI_PP"]

    lift_matrix = np.zeros((len(adv_names), len(intent_order)))
    sig_matrix = np.zeros((len(adv_names), len(intent_order)), dtype=bool)

    for i, adv_name in enumerate(adv_names):
        for j, ig in enumerate(intent_order):
            row = rct_ig_results[
                (rct_ig_results["group"] == f"{adv_name} — {ig}")
            ]
            if not row.empty:
                lift_matrix[i, j] = row["ivr_lift"].values[0] * 100
                sig_matrix[i, j] = row["significant_t"].values[0]

    fig, ax = plt.subplots(figsize=(10, 6))
    im = ax.imshow(lift_matrix, cmap="RdYlGn", aspect="auto",
                   vmin=-30, vmax=130)

    ax.set_xticks(range(len(intent_order)))
    ax.set_xticklabels(["Peak Perf", "High Intent", "Mid Intent", "MI + PP"], fontsize=11)
    ax.set_yticks(range(len(adv_names)))
    ax.set_yticklabels(adv_names, fontsize=11)

    for i in range(len(adv_names)):
        for j in range(len(intent_order)):
            val = lift_matrix[i, j]
            sig = "**" if sig_matrix[i, j] else ""
            color = "white" if abs(val) > 60 else "black"
            ax.text(j, i, f"{val:+.1f}%{sig}", ha="center", va="center",
                    fontsize=10, fontweight="bold", color=color)

    plt.colorbar(im, ax=ax, label="IVR Lift (%)", shrink=0.8)
    ax.set_title("Fangorn Treatment Effect: IVR Lift by Advertiser × Intent Group\n(** = p < 0.05)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "ti_504_lift_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / 'ti_504_lift_heatmap.png'}")


def plot_advertiser_level_bars(rct_adv_results):
    """Bar chart of advertiser-level IVR with control vs treatment side by side."""
    rct_adv_results = rct_adv_results.sort_values("ivr_lift", ascending=True)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6), gridspec_kw={"width_ratios": [2, 1]})

    # Left: grouped bar chart
    advs = [g.replace("Zumba Fitness", "Zumba").replace("Collector Store", "Collector St.")
            for g in rct_adv_results["group"].values]
    x = np.arange(len(advs))
    width = 0.35

    c_ivr = rct_adv_results["control_ivr"].values * 100
    t_ivr = rct_adv_results["treatment_ivr"].values * 100

    bars_c = ax1.barh(x - width / 2, c_ivr, width, label="Control", color="#2196F3", alpha=0.85)
    bars_t = ax1.barh(x + width / 2, t_ivr, width, label="Treatment", color="#FF5722", alpha=0.85)

    for i, (sig, lift) in enumerate(zip(rct_adv_results["significant_t"].values, rct_adv_results["ivr_lift"].values)):
        marker = f"  {lift:+.0%} **" if sig else f"  {lift:+.0%}"
        ax1.text(max(c_ivr[i], t_ivr[i]) + 0.02, x[i], marker, va="center", fontsize=10,
                 fontweight="bold" if sig else "normal")

    ax1.set_yticks(x)
    ax1.set_yticklabels(advs, fontsize=11)
    ax1.set_xlabel("IVR (%)", fontsize=11)
    ax1.set_title("IVR by Advertiser", fontsize=13, fontweight="bold")
    ax1.legend(fontsize=10)
    ax1.grid(axis="x", alpha=0.3)

    # Right: lift bar chart
    lifts = rct_adv_results["ivr_lift"].values * 100
    colors = ["#4CAF50" if l > 0 else "#F44336" for l in lifts]
    sig_flags = rct_adv_results["significant_t"].values
    edge_colors = ["black" if s else "none" for s in sig_flags]

    ax2.barh(x, lifts, color=colors, alpha=0.85, edgecolor=edge_colors, linewidth=2)
    ax2.axvline(0, color="black", linewidth=0.8)
    ax2.set_yticks(x)
    ax2.set_yticklabels([])
    ax2.set_xlabel("IVR Lift (%)", fontsize=11)
    ax2.set_title("Treatment Lift (** = sig)", fontsize=13, fontweight="bold")
    ax2.grid(axis="x", alpha=0.3)

    for i, (l, s) in enumerate(zip(lifts, sig_flags)):
        label = f"{l:+.1f}%**" if s else f"{l:+.1f}%"
        ax2.text(l + (2 if l > 0 else -2), i, label, va="center", ha="left" if l > 0 else "right",
                 fontsize=10, fontweight="bold" if s else "normal")

    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "ti_504_advertiser_bars.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / 'ti_504_advertiser_bars.png'}")


def plot_pooled_intent_bars(df):
    """Pooled control vs treatment IVR by intent group across all advertisers."""
    intent_groups = ["PP", "HI", "MI", "MI_PP"]
    ig_labels = ["Peak Perf", "High Intent", "Mid Intent", "MI + PP"]

    c_ivrs, t_ivrs, lifts, p_vals = [], [], [], []
    for ig in intent_groups:
        subset = df[df["intent_group"] == ig]
        c = subset[subset["arm"] == "control"]
        t = subset[subset["arm"] == "treatment"]
        c_ivr = c["vv"].sum() / c["impressions"].sum()
        t_ivr = t["vv"].sum() / t["impressions"].sum()
        c_ivrs.append(c_ivr * 100)
        t_ivrs.append(t_ivr * 100)
        lifts.append((t_ivr - c_ivr) / c_ivr * 100)

        c_daily = c.groupby("day").agg(impressions=("impressions", "sum"), vv=("vv", "sum"))
        t_daily = t.groupby("day").agg(impressions=("impressions", "sum"), vv=("vv", "sum"))
        c_daily["ivr"] = c_daily["vv"] / c_daily["impressions"]
        t_daily["ivr"] = t_daily["vv"] / t_daily["impressions"]
        _, p = stats.ttest_ind(t_daily["ivr"].values, c_daily["ivr"].values, equal_var=False)
        p_vals.append(p)

    x = np.arange(len(ig_labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width / 2, c_ivrs, width, label="Control", color="#2196F3", alpha=0.85)
    ax.bar(x + width / 2, t_ivrs, width, label="Treatment", color="#FF5722", alpha=0.85)

    for i, (lift, p) in enumerate(zip(lifts, p_vals)):
        sig = "**" if p < 0.05 else ""
        ax.text(i, max(c_ivrs[i], t_ivrs[i]) + 0.02, f"{lift:+.1f}%{sig}",
                ha="center", fontsize=10, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(ig_labels, fontsize=11)
    ax.set_ylabel("IVR (%)", fontsize=11)
    ax.set_title("Pooled IVR: Control vs Treatment by Intent Group\n(all 5 advertisers combined)",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    fig.savefig(OUTPUT_DIR / "ti_504_pooled_intent_bars.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {OUTPUT_DIR / 'ti_504_pooled_intent_bars.png'}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 90)
    print("TI-504: FANGORN AIS EXPERIMENT — RCT ANALYSIS")
    print("=" * 90)

    df = load_experiment_data()
    print(f"\nLoaded {len(df):,} rows")
    print(f"Date range: {df['day'].min().date()} to {df['day'].max().date()}")
    print(f"Advertisers: {df['advertiser_id'].nunique()}")
    print(f"Campaigns: {df['campaign_id'].nunique()}")
    print(f"Total impressions: {df['impressions'].sum():,.0f}")
    print(f"Total VVs: {df['vv'].sum():,.0f}")
    print(f"Overall IVR: {df['vv'].sum() / df['impressions'].sum():.6f}")

    # Track 1a: Intent-group level head-to-head
    rct_ig = run_rct_comparison(df, level="intent_group")

    # Track 1b: Advertiser-level aggregate
    rct_adv = run_rct_comparison(df, level="advertiser")

    # Track 1c: Pooled across all advertisers
    run_pooled_analysis(df)

    # Track 2: Synthetic control
    sc_results = run_synthetic_control(df)

    # Save results
    rct_ig.to_csv("outputs/ti_504_rct_intent_group_results.csv", index=False)
    rct_adv.to_csv("outputs/ti_504_rct_advertiser_results.csv", index=False)
    print("\nResults saved to outputs/")

    # Visualizations
    print("\n" + "=" * 90)
    print("GENERATING VISUALIZATIONS")
    print("=" * 90)

    plot_daily_ivr_by_advertiser(df)
    plot_daily_ivr_by_intent_group(df)
    plot_lift_heatmap(rct_ig)
    plot_advertiser_level_bars(rct_adv)
    plot_pooled_intent_bars(df)

    print("\n" + "=" * 90)
    print("ANALYSIS COMPLETE")
    print("=" * 90)


if __name__ == "__main__":
    main()
