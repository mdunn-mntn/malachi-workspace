"""TI-933 — Tufte-compliant Select lift charts.

Reads:
  outputs/ti_933_select_volume_by_advertiser.csv
  outputs/ti_933_select_lift_pooled_7d.json
  outputs/ti_933_xlsx_vs_our_cohort.csv

Writes:
  artifacts/ti_933_chart_volume_by_advertiser.png
  artifacts/ti_933_chart_pooled_lift.png
  artifacts/ti_933_chart_per_advertiser_power.png
  artifacts/ti_933_chart_conversions.png
"""
import csv, json, math
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib as mpl

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"
ART = ROOT / "artifacts"

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
    "axes.facecolor": BG,
    "figure.facecolor": BG,
    "savefig.facecolor": BG,
    "savefig.dpi": 200,
})


def load_volume_rows():
    rows = []
    with open(OUT / "ti_933_select_volume_by_advertiser.csv") as f:
        for r in csv.DictReader(f):
            r["impressions_30d"] = int(r["impressions_30d"])
            r["spend_30d"] = float(r["spend_30d"])
            r["monthly_equiv_spend"] = float(r["monthly_equiv_spend"])
            rows.append(r)
    return rows


def chart_volume_by_advertiser():
    rows = load_volume_rows()
    rows.sort(key=lambda r: -r["impressions_30d"])
    top = rows[:20]  # show top 20 advertisers

    names = [r["advertiser_name"] or f"AID {r['advertiser_id']}" for r in top]
    imps = [r["impressions_30d"] / 1_000_000 for r in top]
    monthly = [r["monthly_equiv_spend"] / 1_000 for r in top]

    fig, ax = plt.subplots(figsize=(11, 8.5))
    y = list(range(len(top)))
    ax.barh(y, imps, color=NAVY, height=0.7)
    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Impressions, last 30 days (millions)", fontsize=10, color="#444")
    ax.set_title("Top 20 active MNTN Select advertisers by impression volume",
                 fontsize=14, fontweight="bold", color=NAVY, loc="left", pad=14)
    ax.text(0, 1.04, f"Last 30 days · 38 active advertisers · all prospecting",
            transform=ax.transAxes, fontsize=9, color=GRAY)
    # Direct labels: impressions in M and monthly $ on the right
    for i, (im, m) in enumerate(zip(imps, monthly)):
        ax.text(im + max(imps) * 0.012, i,
                f"{im:.1f}M  ·  ${m:,.0f}k/mo",
                va="center", fontsize=9, color="#222")
    ax.set_xlim(0, max(imps) * 1.32)
    # Hide y-axis line for cleanliness
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", which="both", left=False)
    ax.grid(axis="x", linestyle=":", linewidth=0.5, alpha=0.4)
    fig.tight_layout()
    fig.savefig(ART / "ti_933_chart_volume_by_advertiser.png", bbox_inches="tight")
    plt.close(fig)


def load_lift():
    """Return (per_adv_rows, pooled_dict) parsed from the lift JSON."""
    with open(OUT / "ti_933_select_lift_pooled_7d.json") as f:
        rows = json.load(f)
    pooled = {r["arm"]: r for r in rows if r["is_pooled"]}
    per_adv = [r for r in rows if not r["is_pooled"]]
    return per_adv, pooled


def lift_with_ci(p_t, n_t, p_h, n_h):
    """Return (lift_pp, ci_low_pp, ci_high_pp) using Wald 95% CI for diff of two proportions."""
    if n_t == 0 or n_h == 0 or p_t is None or p_h is None:
        return None, None, None
    se = math.sqrt(p_t * (1 - p_t) / n_t + p_h * (1 - p_h) / n_h)
    diff = p_t - p_h
    return diff * 100, (diff - 1.96 * se) * 100, (diff + 1.96 * se) * 100


def chart_pooled_lift():
    """Headline: Select pooled lift vs TI-917's all/prosp/rtg baselines, with 95% CIs."""
    per_adv, pooled = load_lift()
    treated = pooled["treated_served"]
    holdout = pooled["holdout_biddable"]

    # Build the lift values for each metric
    metrics = []
    for label, treated_count_key, holdout_count_key, rate_key in [
        ("Visit rate (clickpass)", "clickpass_visitors", "clickpass_visitors", "clickpass_rate"),
        ("Visit rate (guid)",      "guid_visitors",      "guid_visitors",      "guid_rate"),
        ("Conversion rate",        "ui_converters",      "ui_converters",      "ui_conv_rate"),
    ]:
        n_t = int(treated["n_ips"])
        n_h = int(holdout["n_ips"])
        p_t = float(treated[rate_key]) if treated[rate_key] is not None else 0
        p_h = float(holdout[rate_key]) if holdout[rate_key] is not None else 0
        lift, lo, hi = lift_with_ci(p_t, n_t, p_h, n_h)
        metrics.append((label, lift, lo, hi, p_t, p_h, n_t, n_h))

    # Compare to TI-917 baselines (visit-rate clickpass, guid)
    ti917_visit_baselines = {
        "all (TI-917)": 3.12,
        "prospecting (TI-917)": 0.78,
        "retargeting (TI-917)": 21.07,
        "stage1 (TI-917)": -0.06,
    }

    # Headline chart: bar chart with TI-917 segments + Select for guid visit rate
    fig, ax = plt.subplots(figsize=(11, 5.8))
    labels = list(ti917_visit_baselines.keys()) + ["MNTN Select (TI-933)"]
    select_lift = metrics[1][1]  # guid lift
    select_lo = metrics[1][2]
    select_hi = metrics[1][3]
    values = list(ti917_visit_baselines.values()) + [select_lift if select_lift is not None else 0]
    colors = [LIGHT_GRAY, GRAY, NAVY, GRAY, RED]
    bars = ax.bar(labels, values, color=colors, edgecolor="none", width=0.65)
    # CI on Select bar only
    if select_lift is not None and select_lo is not None and select_hi is not None:
        ax.errorbar([labels[-1]], [select_lift], yerr=[[select_lift - select_lo], [select_hi - select_lift]],
                    fmt="none", ecolor="#222", capsize=6, capthick=1.2, elinewidth=1.2)
    # Direct labels on each bar
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, v + (0.6 if v >= 0 else -1.2),
                f"{v:+.2f}pp" if v != 0 else "n/a",
                ha="center", va="bottom" if v >= 0 else "top",
                fontsize=10, fontweight="bold", color=bar.get_facecolor() if v != 0 else GRAY)
    ax.axhline(0, color="#444", linewidth=0.7)
    ax.set_ylabel("Visit-rate lift, percentage points", fontsize=10, color="#444")
    ax.set_title("MNTN Select lift vs prior segments (visit rate, 7 days)",
                 fontsize=14, fontweight="bold", color=NAVY, loc="left", pad=14)
    ax.text(0, 1.04,
            f"Pooled across 38 Select advertisers · 2026-04-29 to 2026-05-05 · 95% CI on Select bar",
            transform=ax.transAxes, fontsize=9, color=GRAY)
    ax.grid(axis="y", linestyle=":", linewidth=0.5, alpha=0.4)
    ax.spines["bottom"].set_visible(True)
    fig.tight_layout()
    fig.savefig(ART / "ti_933_chart_pooled_lift.png", bbox_inches="tight")
    plt.close(fig)
    return metrics  # for downstream use


def chart_per_advertiser_power(metrics_pooled):
    """Per-advertiser visit-rate lift (guid), sized by n_ips, color-coded by 95% CI sign."""
    per_adv, pooled = load_lift()
    # Build per-advertiser arms map
    per_aid = {}
    for r in per_adv:
        aid = int(r["advertiser_id"])
        per_aid.setdefault(aid, {})[r["arm"]] = r

    # Load monthly spend
    vol = {int(r["advertiser_id"]): r for r in load_volume_rows()}

    rows_out = []
    for aid, arms in per_aid.items():
        if "treated_served" not in arms or "holdout_biddable" not in arms:
            continue
        t = arms["treated_served"]; h = arms["holdout_biddable"]
        n_t = int(t["n_ips"]); n_h = int(h["n_ips"])
        p_t = float(t["guid_rate"]) if t["guid_rate"] is not None else 0
        p_h = float(h["guid_rate"]) if h["guid_rate"] is not None else 0
        lift, lo, hi = lift_with_ci(p_t, n_t, p_h, n_h)
        v = vol.get(aid, {})
        rows_out.append({
            "advertiser_id": aid,
            "advertiser_name": v.get("advertiser_name", f"AID {aid}"),
            "monthly_equiv_spend": float(v.get("monthly_equiv_spend", 0)),
            "n_treated": n_t, "n_holdout": n_h,
            "lift_pp": lift, "lo_pp": lo, "hi_pp": hi,
        })

    # Save per-advertiser results to CSV for the deck table slide
    with open(OUT / "ti_933_per_advertiser_lift.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["advertiser_id", "advertiser_name", "monthly_equiv_spend",
                    "n_treated", "n_holdout", "lift_pp", "ci_low_pp", "ci_high_pp", "significant"])
        for r in sorted(rows_out, key=lambda x: -x["monthly_equiv_spend"]):
            sig = "Y" if r["lo_pp"] is not None and r["lo_pp"] > 0 else (
                "Y(-)" if r["hi_pp"] is not None and r["hi_pp"] < 0 else "N"
            )
            w.writerow([r["advertiser_id"], r["advertiser_name"], f"{r['monthly_equiv_spend']:.2f}",
                        r["n_treated"], r["n_holdout"],
                        f"{r['lift_pp']:.3f}" if r["lift_pp"] is not None else "",
                        f"{r['lo_pp']:.3f}" if r["lo_pp"] is not None else "",
                        f"{r['hi_pp']:.3f}" if r["hi_pp"] is not None else "",
                        sig])

    # Plot: x = monthly spend ($k), y = lift (pp) with error bars
    fig, ax = plt.subplots(figsize=(11, 6.2))
    xs = [r["monthly_equiv_spend"] / 1000 for r in rows_out]
    ys = [r["lift_pp"] for r in rows_out]
    los = [r["lift_pp"] - r["lo_pp"] if r["lo_pp"] is not None else 0 for r in rows_out]
    his = [r["hi_pp"] - r["lift_pp"] if r["hi_pp"] is not None else 0 for r in rows_out]
    sig_mask = [r["lo_pp"] is not None and (r["lo_pp"] > 0 or r["hi_pp"] < 0) for r in rows_out]

    for x, y, lo, hi, s, r in zip(xs, ys, los, his, sig_mask, rows_out):
        color = RED if s else GRAY
        ax.errorbar([x], [y], yerr=[[lo], [hi]], fmt="o", color=color,
                    ecolor=color, capsize=3, alpha=0.85, markersize=7)
        if s:
            ax.annotate(r["advertiser_name"], (x, y), textcoords="offset points", xytext=(7, 3),
                        fontsize=9, color=RED, fontweight="bold")

    ax.axhline(0, color="#444", linewidth=0.7)
    # Pooled line
    pooled_lift = metrics_pooled[1][1]
    if pooled_lift is not None:
        ax.axhline(pooled_lift, color=NAVY, linestyle="--", linewidth=1.2, alpha=0.8)
        ax.text(max(xs) * 0.98, pooled_lift, f"  pooled = {pooled_lift:+.2f}pp", color=NAVY, fontsize=9, va="bottom", ha="right")
    ax.set_xlabel("Monthly equivalent spend, $k", fontsize=10, color="#444")
    ax.set_ylabel("Visit-rate lift, percentage points (guid)", fontsize=10, color="#444")
    ax.set_title("Per-advertiser Select lift — none individually powered, pooled is",
                 fontsize=14, fontweight="bold", color=NAVY, loc="left", pad=14)
    ax.text(0, 1.04, f"38 active Select advertisers · 7-day window · red = 95% CI excludes zero",
            transform=ax.transAxes, fontsize=9, color=GRAY)
    ax.grid(axis="both", linestyle=":", linewidth=0.5, alpha=0.4)
    fig.tight_layout()
    fig.savefig(ART / "ti_933_chart_per_advertiser_power.png", bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    chart_volume_by_advertiser()
    metrics = chart_pooled_lift()
    chart_per_advertiser_power(metrics)
    print("=== Pooled metrics ===")
    for label, lift, lo, hi, p_t, p_h, n_t, n_h in metrics:
        if lift is None:
            print(f"  {label:24s}  insufficient data")
        else:
            print(f"  {label:24s}  treated={p_t:.4%}  holdout={p_h:.4%}  lift={lift:+.3f}pp  95%CI=[{lo:+.3f}, {hi:+.3f}]  n_t={n_t:,}  n_h={n_h:,}")
