"""TI-917 — Tufte-compliant revenue/iROAS MDE charts.

Inputs:
  outputs/ti_917_revenue_mde_per_advertiser.csv

Outputs:
  artifacts/ti_917_chart_iroas_mde_vs_spend.png
  artifacts/ti_917_chart_tier_breakdown.png
  artifacts/ti_917_chart_revenue_reporting_gap.png
"""
import csv
import math
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib as mpl

THIS_DIR = Path(__file__).resolve().parent
TI917_ROOT = THIS_DIR.parent
CSV_PATH = TI917_ROOT / "outputs" / "ti_917_revenue_mde_per_advertiser.csv"

# Tufte palette + Helvetica Neue base (matches presentation_playbook visualization standards).
NAVY = "#1B2A4A"
RED = "#D63B2F"
GRAY = "#888888"
LIGHT_GRAY = "#C8CDD4"
BG = "#FAFAFA"

mpl.rcParams.update({
    "font.family": ["Helvetica Neue", "Helvetica", "Arial", "sans-serif"],
    "font.size": 11,
    "axes.edgecolor": "#444",
    "axes.linewidth": 0.7,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.labelcolor": "#222",
    "xtick.color": "#444",
    "ytick.color": "#444",
    "axes.facecolor": BG,
    "figure.facecolor": BG,
    "savefig.facecolor": BG,
    "savefig.dpi": 200,
})


def load_rows():
    rows = []
    with open(CSV_PATH) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def to_float(s):
    if s == "" or s is None:
        return None
    try:
        v = float(s)
        return None if math.isinf(v) or math.isnan(v) else v
    except ValueError:
        return None


def chart_iroas_mde_vs_spend(rows):
    fig, ax = plt.subplots(figsize=(10.5, 5.6))
    spends = [float(r["monthly_spend"]) / 1000 for r in rows]  # $k
    mdes = [to_float(r["mde_iroas_post_stack"]) for r in rows]
    tiers = [r["tier_iroas_post_stack"] for r in rows]

    # No-data points: plot at the bottom of the y-axis as gray X markers
    nodata_x = [s for s, m in zip(spends, mdes) if m is None]
    measured = [(s, m, t) for s, m, t in zip(spends, mdes, tiers) if m is not None]
    well_x = [s for s, m, t in measured if t == "well_powered"]
    well_y = [m for s, m, t in measured if t == "well_powered"]
    border_x = [s for s, m, t in measured if t == "borderline"]
    border_y = [m for s, m, t in measured if t == "borderline"]
    under_x = [s for s, m, t in measured if t == "underpowered"]
    under_y = [m for s, m, t in measured if t == "underpowered"]

    # 1.0 = "must double total return to be detectable" — sane upper plotting cap.
    ax.set_yscale("log")
    ax.scatter(under_x, under_y, s=40, color=GRAY, alpha=0.75, edgecolors="none", label=None)
    ax.scatter(border_x, border_y, s=55, color=NAVY, alpha=0.85, edgecolors="none", label=None)
    ax.scatter(well_x, well_y, s=80, color=RED, edgecolors="none", label=None)

    # No-data band along the bottom of the chart
    if nodata_x:
        ax.scatter(nodata_x, [0.005] * len(nodata_x), s=40, marker="x", color=LIGHT_GRAY, label=None)

    # Reference: iROAS = 1.0 line ("can detect 1× return")
    ax.axhline(1.0, color=GRAY, linestyle="--", linewidth=0.7, alpha=0.6)
    ax.text(max(spends) * 1.02, 1.0, "iROAS = 1.0", color=GRAY, fontsize=8, va="center")

    # Direct labels on the well-powered points
    for s, m, t in zip(spends, mdes, tiers):
        if t == "well_powered" and m is not None:
            aid = next(r["advertiser_id"] for r in rows if abs(float(r["monthly_spend"]) / 1000 - s) < 1e-6)
            ax.annotate(f"AID {aid}\n{m:.2f}", (s, m), textcoords="offset points", xytext=(8, 6),
                        fontsize=8, color=RED, fontweight="bold")

    ax.set_xscale("log")
    ax.set_xlabel("April 2026 Stage 1 spend ($k/month)", fontsize=11)
    ax.set_ylabel("Min detectable iROAS (post-stack)", fontsize=11)
    fig.suptitle(
        "Only 2 of 50 top advertisers are well-powered for iROAS measurement",
        fontsize=14, color=NAVY, fontweight="bold", x=0.02, y=0.98, ha="left",
    )
    fig.text(
        0.02, 0.91,
        "Each dot = one top-50 advertiser. Y = smallest detectable iROAS at α=0.05, power=0.8 (post-stack: CUPED + ghost-ad + strat).\n"
        "Gray ×s along bottom = 18 advertisers with $0 measured order revenue (no `order_amt` in conversion pixel) — iROAS unmeasurable at any spend.",
        ha="left", va="top", fontsize=8.5, color=GRAY,
    )

    # Annotation for the no-data band
    if nodata_x:
        ax.text(min(spends) * 0.95, 0.005, "no revenue reported  ", color=GRAY,
                fontsize=8, ha="right", va="center", fontstyle="italic")

    plt.tight_layout(rect=(0, 0, 1, 0.86))
    out = THIS_DIR / "ti_917_chart_iroas_mde_vs_spend.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[OK] {out.name}")


def chart_tier_breakdown(rows):
    """Stacked bar showing visit / CVR / iROAS measurability tiers across the cohort."""
    # Read the TI-884 tier CSV for visit + cvr tiers
    cohort_csv = TI917_ROOT.parent / "ti_884_power_sample_size_analysis" / "outputs" / "ti_884_top50_mde_tiers.csv"
    visit_tiers = {}
    cvr_tiers = {}
    with open(cohort_csv) as f:
        for r in csv.DictReader(f):
            aid = int(r["advertiser_id"])
            visit_tiers[aid] = r["tier_visits_post_stack"]
            cvr_tiers[aid] = r["tier_cvr_post_stack"]
    iroas_tiers = {int(r["advertiser_id"]): r["tier_iroas_post_stack"] for r in rows}

    # Restrict to the 50 advertisers we actually have revenue data for
    cohort_aids = sorted(iroas_tiers.keys())
    # Counts per outcome
    def counts(by_aid):
        out = {"well_powered": 0, "borderline": 0, "underpowered": 0, "no_data": 0}
        for a in cohort_aids:
            t = by_aid.get(a, "no_data")
            out[t] = out.get(t, 0) + 1
        return out

    visit_c = counts(visit_tiers)
    cvr_c = counts(cvr_tiers)
    iroas_c = counts(iroas_tiers)

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    outcomes = ["Visit rate", "Conversion rate", "Revenue / iROAS"]
    well = [visit_c["well_powered"], cvr_c["well_powered"], iroas_c["well_powered"]]
    border = [visit_c["borderline"], cvr_c["borderline"], iroas_c["borderline"]]
    under = [visit_c["underpowered"], cvr_c["underpowered"], iroas_c["underpowered"]]
    nodata = [visit_c["no_data"], cvr_c["no_data"], iroas_c["no_data"]]

    y = list(range(len(outcomes)))[::-1]
    left = [0, 0, 0]
    bars_w = ax.barh(y, well, color=RED, label="Well powered (<5% MDE)")
    left = list(well)
    bars_b = ax.barh(y, border, left=left, color=NAVY, label="Borderline (5-10%)")
    left = [a + b for a, b in zip(left, border)]
    bars_u = ax.barh(y, under, left=left, color=GRAY, label="Underpowered (>10%)")
    left = [a + b for a, b in zip(left, under)]
    bars_n = ax.barh(y, nodata, left=left, color=LIGHT_GRAY, label="No data")

    # Direct labels: count inside each segment
    for i, (w_, b_, u_, n_) in enumerate(zip(well, border, under, nodata)):
        cur = 0
        for label, val, color in [("well", w_, RED), ("bord", b_, NAVY), ("under", u_, GRAY), ("nd", n_, LIGHT_GRAY)]:
            if val > 0:
                ax.text(cur + val / 2, y[i], str(val), ha="center", va="center",
                        color="white" if color in (RED, NAVY, GRAY) else "#333",
                        fontsize=11, fontweight="bold")
            cur += val

    ax.set_yticks(y)
    ax.set_yticklabels(outcomes, fontsize=12, color=NAVY)
    ax.set_xlim(0, 50)
    ax.set_xlabel("Top-50 advertisers (count)", fontsize=11)
    fig.suptitle(
        "Measurability collapses as the outcome moves from visits to revenue",
        fontsize=14, color=NAVY, fontweight="bold", x=0.02, y=0.98, ha="left",
    )
    fig.text(
        0.02, 0.91,
        "All 50 top advertisers, post-stack power. Tier thresholds: <5% rel MDE = well-powered; 5-10% = borderline; >10% = underpowered.",
        ha="left", va="top", fontsize=8.5, color=GRAY,
    )
    ax.legend(loc="lower right", frameon=False, fontsize=9, ncol=2)
    plt.tight_layout(rect=(0, 0, 1, 0.86))
    out = THIS_DIR / "ti_917_chart_tier_breakdown.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[OK] {out.name}")


def chart_sigma_over_mu(rows):
    """Per-advertiser σ/μ ratio — shows why iROAS is hard even with revenue data."""
    pts = []
    for r in rows:
        mu = float(r["mu_rev_per_ip"])
        sigma = float(r["sigma_rev_per_ip"])
        spend = float(r["monthly_spend"]) / 1000
        if mu > 0 and sigma > 0:
            pts.append((spend, sigma / mu, r["advertiser_id"], r["tier_iroas_post_stack"]))
    pts.sort()

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    colors = [RED if p[3] == "well_powered" else NAVY if p[3] == "borderline" else GRAY for p in pts]
    ax.scatter(xs, ys, s=55, c=colors, alpha=0.85, edgecolors="none")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("April 2026 Stage 1 spend ($k/month)", fontsize=11)
    ax.set_ylabel("Revenue σ / μ (per IP)", fontsize=11)
    fig.suptitle(
        "Revenue per IP is heavy-tailed for almost every advertiser",
        fontsize=14, color=NAVY, fontweight="bold", x=0.02, y=0.98, ha="left",
    )
    fig.text(
        0.02, 0.91,
        "σ/μ measures the noise floor relative to mean revenue per IP. Higher = harder to measure. Most MNTN advertisers cluster at σ/μ between 30 and 200 — orders of magnitude harder than visit rates.",
        ha="left", va="top", fontsize=8.5, color=GRAY,
    )
    plt.tight_layout(rect=(0, 0, 1, 0.86))
    out = THIS_DIR / "ti_917_chart_sigma_over_mu.png"
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[OK] {out.name}")


def main():
    rows = load_rows()
    chart_iroas_mde_vs_spend(rows)
    chart_tier_breakdown(rows)
    chart_sigma_over_mu(rows)


if __name__ == "__main__":
    main()
