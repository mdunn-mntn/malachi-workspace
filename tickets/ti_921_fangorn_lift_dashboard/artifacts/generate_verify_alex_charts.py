"""
TI-921 — Tufte-style charts of the local verification of Alex's notebook.

Outputs:
  artifacts/ti_921_verify_alex_wave2_per_aid.png
  artifacts/ti_921_verify_alex_tier_lift.png

Run after verify_alex_results.py has produced the outputs CSVs.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ADV_CSV = ROOT / "outputs" / "verify_alex_advertiser_change.csv"
PIVOT_CSV = ROOT / "outputs" / "verify_alex_tier_pivot.csv"

ACCENT = "#b91c1c"      # drops
POSITIVE = "#1a7f37"    # rises
NEUTRAL = "#6b7280"
BG = "#FAFAFA"
FONT = "Helvetica Neue"

plt.rcParams.update({
    "font.family": FONT,
    "figure.facecolor": BG,
    "axes.facecolor": BG,
    "savefig.facecolor": BG,
    "axes.edgecolor": "#888",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.spines.left": False,
    "axes.grid": False,
})


def chart_per_aid_wave2() -> None:
    df = pd.read_csv(ADV_CSV)
    w2 = (
        df[(df["pass"] == "alex") & (df["cohort"] == "Tier1-Wave2") & df["visit_rate_pct_change"].notna()]
        .copy()
        .sort_values("visit_rate_pct_change", ascending=True)
        .reset_index(drop=True)
    )
    w2["pct"] = w2["visit_rate_pct_change"] * 100
    w2["color"] = np.where(w2["pct"] >= 10, POSITIVE, np.where(w2["pct"] <= -10, ACCENT, NEUTRAL))

    n = len(w2)
    fig, ax = plt.subplots(figsize=(10, 0.22 * n + 1.5), dpi=200)
    bars = ax.barh(np.arange(n), w2["pct"], color=w2["color"], height=0.7)

    for i, (pct, name) in enumerate(zip(w2["pct"], w2["company_name"])):
        if pct >= 0:
            ax.text(pct + 4, i, f"{pct:+.0f}%", va="center", ha="left",
                    fontsize=8, color="#222")
        else:
            ax.text(pct - 4, i, f"{pct:+.0f}%", va="center", ha="right",
                    fontsize=8, color="#222")

    ax.set_yticks(np.arange(n))
    ax.set_yticklabels(w2["company_name"], fontsize=8.5)
    ax.axvline(0, color="#444", linewidth=0.8)
    ax.axvline(10, color=POSITIVE, linewidth=0.5, linestyle=":", alpha=0.5)
    ax.axvline(-10, color=ACCENT, linewidth=0.5, linestyle=":", alpha=0.5)
    ax.set_xlim(left=w2["pct"].min() - 20, right=w2["pct"].max() + 20)
    ax.set_xlabel("visit-rate change pre to post (%)", fontsize=9, color=NEUTRAL)
    ax.tick_params(axis="x", colors=NEUTRAL, labelsize=8)
    ax.tick_params(axis="y", length=0)

    n_rise = int((w2["pct"] >= 10).sum())
    n_drop = int((w2["pct"] <= -10).sum())
    n_flat = n - n_rise - n_drop

    fig.suptitle(
        "Tier-1 Wave 2 - visit rate rose for 32 of 41 evaluable advertisers",
        x=0.02, y=0.985, ha="left", fontsize=13, fontweight="semibold", color="#111",
    )
    fig.text(
        0.02, 0.955,
        f"Pre vs post Fangorn flip  ·  May 5-6 cohort  ·  +/-10% threshold  ({n_rise} up, {n_flat} flat, {n_drop} down)",
        ha="left", fontsize=9.5, color=NEUTRAL,
    )

    plt.tight_layout(rect=(0, 0, 1, 0.93))
    out = ROOT / "artifacts" / "ti_921_verify_alex_wave2_per_aid.png"
    plt.savefig(out, bbox_inches="tight", dpi=200)
    print(f"wrote {out}")
    plt.close()


def chart_tier_lift_alex_vs_loose() -> None:
    df = pd.read_csv(PIVOT_CSV)
    df["lift_pct"] = df["visit_rate_lift_pct"] * 100

    cohorts = sorted(df["cohort"].unique())
    passes = ["loose", "alex"]
    pass_labels = {"loose": "TI-921 baseline\n(funnel_level=1 only)",
                   "alex":  "Alex's filter\n(+ obj=1 + mntn_matched)"}
    pass_colors = {"loose": "#9ca3af", "alex": "#1f2937"}

    fig, ax = plt.subplots(figsize=(8, 5), dpi=200)
    width = 0.36
    x = np.arange(len(cohorts))

    for i, p in enumerate(passes):
        sub = df[df["pass"] == p].set_index("cohort").reindex(cohorts)
        bars = ax.bar(x + (i - 0.5) * width, sub["lift_pct"], width,
                      color=pass_colors[p], label=pass_labels[p])
        for bar, val in zip(bars, sub["lift_pct"]):
            ax.text(bar.get_x() + bar.get_width() / 2, val + 0.5,
                    f"{val:+.1f}%", ha="center", va="bottom",
                    fontsize=10, color="#111", fontweight="semibold")

    ax.set_xticks(x)
    ax.set_xticklabels(cohorts, fontsize=10)
    ax.set_ylabel("pre/post visit-rate lift (%)", fontsize=10, color=NEUTRAL)
    ax.tick_params(axis="y", colors=NEUTRAL, labelsize=9)
    ax.axhline(0, color="#444", linewidth=0.8)
    ax.legend(loc="upper left", frameon=False, fontsize=9)
    ax.set_ylim(top=max(df["lift_pct"]) + 6)

    fig.suptitle(
        "Alex's filter shows ~6-11pp larger lift than the loose baseline",
        x=0.02, y=0.98, ha="left", fontsize=13, fontweight="semibold", color="#111",
    )
    fig.text(
        0.02, 0.93,
        "Mostly because his obj=1 filter excludes retargeting that was contaminating the panel",
        ha="left", fontsize=10, color=NEUTRAL,
    )

    plt.tight_layout(rect=(0, 0, 1, 0.88))
    out = ROOT / "artifacts" / "ti_921_verify_alex_tier_lift.png"
    plt.savefig(out, bbox_inches="tight", dpi=200)
    print(f"wrote {out}")
    plt.close()


if __name__ == "__main__":
    chart_per_aid_wave2()
    chart_tier_lift_alex_vs_loose()
