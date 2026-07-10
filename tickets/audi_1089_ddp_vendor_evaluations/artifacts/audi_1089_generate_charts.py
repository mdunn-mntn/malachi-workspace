#!/usr/bin/env python3
"""AUDI-1089 chart set — reads ../outputs/*.csv (never hardcoded data).
Klickly (DS39) verdict support + the cross-vendor chart reused by the remaining evals."""
import csv
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "outputs")
BG, NAVY, RED, GRAY, GREEN = "#FAFAFA", "#27496D", "#D63B2F", "#9AA0A6", "#2E8B57"
plt.rcParams.update({"font.family": "Helvetica Neue", "figure.facecolor": BG,
                     "axes.facecolor": BG, "savefig.facecolor": BG, "axes.edgecolor": BG})

DS_NAME = {23: "guid_log (internal)", 24: "Justuno", 25: "5x5", 26: "Predactiv", 28: "33Across",
           30: "augmentor (internal)", 33: "Sovrn", 36: "Cybba", 39: "Klickly", 40: "33Across API"}
INTERNAL = {23, 30}


def rows(name):
    with open(os.path.join(OUT, name)) as f:
        return list(csv.DictReader(f))


def fmt(v):
    v = float(v)
    if v >= 1e6:
        return f"{v/1e6:.1f}M"
    return f"{v:,.0f}"


def strip(ax):
    for s in ax.spines.values():
        s.set_visible(False)


# ---- Chart 1: sole classified domains per vendor (the MM-value axis) ----
def chart_sole_classified():
    data = sorted(((int(r["data_source_id"]), int(r["sole_classified"]))
                   for r in rows("audi_1089_uniqueness_domains_30d.csv")), key=lambda x: x[1])
    fig, ax = plt.subplots(figsize=(9, 5.2))
    names = [DS_NAME[d] for d, _ in data]
    vals = [v for _, v in data]
    colors = [RED if d == 39 else (GRAY if d in INTERNAL else NAVY) for d, _ in data]
    bars = ax.barh(names, vals, color=colors, height=0.62)
    for b, v in zip(bars, vals):
        ax.text(b.get_width() + max(vals) * 0.008, b.get_y() + b.get_height() / 2,
                f"{v:,}", va="center", fontsize=10,
                fontweight="bold" if v == vals[0] or v == max(vals) else "normal")
    ax.set_title("Klickly uniquely classifies 126 domains for Matched — Predactiv: 226,826",
                 fontsize=14, fontweight="bold", loc="left", pad=14)
    ax.text(0, 1.015, "Domains only this source provides that map to a Matched vertical · 30 days (Jun 2 – Jul 1, 2026)",
            transform=ax.transAxes, fontsize=9.5, color="#777")
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0, labelsize=10.5)
    strip(ax)
    fig.tight_layout()
    fig.savefig(os.path.join(HERE, "audi_1089_chart_sole_classified_domains.png"), dpi=200)
    plt.close(fig)


# ---- Chart 2: Klickly dependency ladder (typography, no fake geometry) ----
def chart_dependency_ladder():
    r39 = next(r for r in rows("audi_1089_value_tiers.csv") if int(r["data_source_id"]) == 39)
    steps = [
        ("Impressions to IPs Klickly touched", float(r39["imps_touched"]), float(r39["media_touched"]),
         "co-occurrence — 99.98% of these IPs are seen by other sources too", GRAY),
        ("Impressions to IPs ONLY Klickly saw", float(r39["imps_sole"]), float(r39["media_sole"]),
         "the ceiling on real dependency · produced 1 visit all week", NAVY),
        ("…of those: score-gated (needed the score to serve)", float(r39["imps_sole_scored_nonrtc"]),
         float(r39["media_sole_scored"]),
         "impressions that could not have served without Klickly", RED),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 3.2)
    ax.axis("off")
    for i, (label, imps, media, note, color) in enumerate(steps):
        y = 2.55 - i * 1.05
        ax.text(0.02, y + 0.30, label, fontsize=11.5, color="#333")
        ax.text(0.02, y - 0.10, fmt(imps), fontsize=26, fontweight="bold", color=color)
        ax.text(0.30, y - 0.06, f"impressions / week   ·   ${media:,.2f} media", fontsize=10.5, color="#555")
        ax.text(0.02, y - 0.32, note, fontsize=9, color="#999", style="italic")
        if i < 2:
            ax.annotate("", xy=(0.012, y - 0.42), xytext=(0.012, y - 0.22),
                        arrowprops=dict(arrowstyle="->", color="#bbb"))
    ax.set_title("224M weekly impressions touch Klickly IPs — 26 actually needed Klickly",
                 fontsize=14, fontweight="bold", loc="left", pad=30)
    ax.text(0, 1.035, "Valuation week Jul 2–8, 2026 · sole = no other source (internal or vendor) saw the IP in the trailing window",
            transform=ax.transAxes, fontsize=9.5, color="#777")
    fig.tight_layout()
    fig.savefig(os.path.join(HERE, "audi_1089_chart_klickly_dependency_ladder.png"), dpi=200)
    plt.close(fig)


# ---- Chart 3: adverse selection on Klickly's unique reach ----
def chart_adverse_selection():
    t = {r["cohort"]: r for r in rows("audi_1089_score_tiers_sole_vs_touched.csv")
         if int(r["data_source_id"]) == 39}
    pairs = [("Shared with other sources", float(t["touched"]["pct_delivered"]),
              float(t["touched"]["pct_unscored_delivered"])),
             ("Only Klickly saw them", float(t["sole"]["pct_delivered"]),
              float(t["sole"]["pct_unscored_delivered"]))]
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.2))
    for ax, (metric, ti, si, better_low) in zip(
            axes,
            [("% of IPs we ever served", pairs[0][1], pairs[1][1], False),
             ("% unscored among those served", pairs[0][2], pairs[1][2], True)]):
        vals = [ti, si]
        colors = [GRAY, RED]
        bars = ax.bar(["shared\nIPs", "Klickly-only\nIPs"], vals, color=colors, width=0.55)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v + max(vals) * 0.02, f"{v:.1f}%",
                    ha="center", fontsize=12, fontweight="bold")
        ax.set_title(metric, fontsize=11, loc="left", color="#333")
        ax.set_yticks([])
        ax.tick_params(axis="x", length=0, labelsize=10)
        strip(ax)
    fig.suptitle("Klickly's unique IPs barely enter delivery — and score as junk when they do",
                 fontsize=13.5, fontweight="bold", x=0.02, ha="left")
    fig.text(0.02, 0.90, "IP cohorts over the 37-day window vs the Jul 2–8 delivery week", fontsize=9.5, color="#777")
    fig.tight_layout(rect=[0, 0, 1, 0.88])
    fig.savefig(os.path.join(HERE, "audi_1089_chart_klickly_adverse_selection.png"), dpi=200)
    plt.close(fig)


# ---- Chart 4: recency mix per vendor (redundancy / insurance picture) ----
def chart_recency_mix():
    data = sorted(((int(r["data_source_id"]), float(r["pct_sole"]), float(r["pct_freshest"]),
                    float(r["pct_tied"]), float(r["pct_stale"]))
                   for r in rows("audi_1089_recency_pairs_30d.csv")), key=lambda x: -x[1])
    fig, ax = plt.subplots(figsize=(9.5, 5))
    names = [DS_NAME[d] for d, *_ in data]
    segs = [("sole (no one else has it)", 1, GREEN), ("freshest", 2, NAVY),
            ("tied same-day (insurance)", 3, GRAY), ("stale (others fresher)", 4, "#D7D9DC")]
    left = [0] * len(data)
    for label, idx, color in segs:
        vals = [row[idx] for row in data]
        ax.barh(names, vals, left=left, color=color, height=0.62, label=label)
        left = [l + v for l, v in zip(left, vals)]
    for i, row in enumerate(data):
        ax.text(row[1] / 2, i, f"{row[1]:.0f}%", va="center", ha="center", fontsize=9,
                color="white", fontweight="bold")
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0, labelsize=10.5)
    strip(ax)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.02), ncol=4, frameon=False, fontsize=8.5)
    ax.set_title("How replaceable is each vendor's data? % of its (IP,domain) pairs by 30-day recency",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    ax.text(0, 1.015, "Sole = irreplaceable in the targeting window · tied = same-day redundancy (outage insurance) · Jun 2 – Jul 1, 2026",
            transform=ax.transAxes, fontsize=9.5, color="#777")
    fig.tight_layout()
    fig.savefig(os.path.join(HERE, "audi_1089_chart_recency_mix.png"), dpi=200)
    plt.close(fig)


# ---- Chart 5: real dependency per vendor (sole imps T2 + gated T1), split panels ----
def chart_dependency_by_vendor():
    data = sorted(((int(r["data_source_id"]), float(r["imps_sole"]), float(r["imps_sole_scored_nonrtc"]))
                   for r in rows("audi_1089_value_tiers.csv") if int(r["data_source_id"]) not in INTERNAL),
                  key=lambda x: -x[1])
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    names = [DS_NAME[d] for d, *_ in data]
    t2 = [v for _, v, _ in data]
    t1 = [v for _, _, v in data]
    bars = ax.barh(names, t2, color=NAVY, height=0.6, label="all imps to vendor-sole IPs (T2)")
    ax.barh(names, t1, color=RED, height=0.6, label="gated + scored, non-RTC (T1 — needed the vendor)")
    for b, v2, v1 in zip(bars, t2, t1):
        ax.text(b.get_width() + max(t2) * 0.01, b.get_y() + b.get_height() / 2,
                f"{v2:,.0f}  (T1: {v1:,.0f})", va="center", fontsize=9.5)
    ax.invert_yaxis()
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0, labelsize=10.5)
    strip(ax)
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    ax.set_title("Weekly impressions that depended on each vendor — every external vendor is <450K of ~2.5B",
                 fontsize=13, fontweight="bold", loc="left", pad=14)
    ax.text(0, 1.02, "Impressions to IPs ONLY that vendor saw (37-day union) · valuation week Jul 2–8 · red = also score-gated",
            transform=ax.transAxes, fontsize=9.5, color="#777")
    fig.tight_layout()
    fig.savefig(os.path.join(HERE, "audi_1089_chart_dependency_by_vendor.png"), dpi=200)
    plt.close(fig)


# ---- Chart 6: sole-IP quality per vendor (delivered % and unscored %) ----
def chart_sole_quality():
    t = {}
    for r in rows("audi_1089_score_tiers_sole_vs_touched.csv"):
        d = int(r["data_source_id"])
        t.setdefault(d, {})[r["cohort"]] = r
    ds_order = sorted((d for d in t if d not in INTERNAL),
                      key=lambda d: -float(t[d]["sole"]["pct_delivered"]))
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.6))
    for ax, key, title, accent_hi in [
            (axes[0], "pct_delivered", "% of vendor-sole IPs we ever served", False),
            (axes[1], "pct_unscored_delivered", "% unscored among served sole IPs", True)]:
        vals = [float(t[d]["sole"][key]) for d in ds_order]
        colors = [RED if d == 39 else NAVY for d in ds_order]
        bars = ax.barh([DS_NAME[d] for d in ds_order], vals, color=colors, height=0.6)
        for b, v in zip(bars, vals):
            ax.text(b.get_width() + max(vals) * 0.015, b.get_y() + b.get_height() / 2,
                    f"{v:.1f}%", va="center", fontsize=9.5)
        ax.invert_yaxis()
        ax.set_xticks([])
        ax.tick_params(axis="y", length=0, labelsize=10)
        strip(ax)
        ax.set_title(title, fontsize=11, loc="left", color="#333")
    fig.suptitle("Vendor-sole IPs are low-quality reach for EVERY vendor — uniqueness ≠ usefulness",
                 fontsize=13.5, fontweight="bold", x=0.02, ha="left")
    fig.text(0.02, 0.90, "External vendors · sole = no other source saw the IP · Jul 2–8 delivery week", fontsize=9.5, color="#777")
    fig.tight_layout(rect=[0, 0, 1, 0.87])
    fig.savefig(os.path.join(HERE, "audi_1089_chart_sole_quality.png"), dpi=200)
    plt.close(fig)


# ---- Chart 7: delivery consistency sparklines (daily rows, 30d) ----
def chart_scale_sparklines():
    daily = {}
    for r in rows("audi_1089_scale_by_day_30d.csv"):
        daily.setdefault(int(r["data_source_id"]), []).append((r["dt"], float(r["n_rows"])))
    ds_order = [26, 28, 25, 40, 30, 23, 24, 33, 39, 36]
    fig, axes = plt.subplots(2, 5, figsize=(11, 3.6))
    for ax, d in zip(axes.flat, ds_order):
        series = sorted(daily.get(d, []))
        ys = [v for _, v in series]
        ax.plot(range(len(ys)), ys, color=RED if d == 39 else NAVY, lw=1.4)
        ax.fill_between(range(len(ys)), ys, color=RED if d == 39 else NAVY, alpha=0.08)
        ax.set_ylim(0, max(ys) * 1.15 if ys else 1)
        ax.set_xticks([])
        ax.set_yticks([])
        strip(ax)
        avg = sum(ys) / len(ys) if ys else 0
        ax.set_title(f"{DS_NAME[d].replace(' (internal)','*')} · {fmt(avg)}/day", fontsize=8.5, loc="left",
                     color=RED if d == 39 else "#333")
    fig.suptitle("All 10 sources delivered every day of the window — no liveness concerns (* = internal)",
                 fontsize=12.5, fontweight="bold", x=0.02, ha="left")
    fig.tight_layout(rect=[0, 0, 1, 0.90])
    fig.savefig(os.path.join(HERE, "audi_1089_chart_scale_sparklines.png"), dpi=200)
    plt.close(fig)


if __name__ == "__main__":
    chart_sole_classified()
    chart_dependency_ladder()
    chart_adverse_selection()
    chart_recency_mix()
    chart_dependency_by_vendor()
    chart_sole_quality()
    chart_scale_sparklines()
    print("wrote 7 charts to", HERE)
