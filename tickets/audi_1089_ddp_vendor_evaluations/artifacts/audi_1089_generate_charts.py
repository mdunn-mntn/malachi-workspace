#!/usr/bin/env python3
"""AUDI-1089 chart set. Reads ../outputs/*.csv, writes PNGs for doc assembly.
Style: plain descriptive titles, one-line caption below each chart, no annotations."""
import csv
import math
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "outputs")
BG, NAVY, RED, GRAY, GREEN, AMBER = "#FAFAFA", "#27496D", "#D63B2F", "#9AA0A6", "#2E8B57", "#C77B30"
plt.rcParams.update({"font.family": "Helvetica Neue", "figure.facecolor": BG,
                     "axes.facecolor": BG, "savefig.facecolor": BG, "axes.edgecolor": BG})

DS_NAME = {23: "guid_log (internal)", 24: "Justuno", 25: "5x5", 26: "Predactiv", 28: "33Across",
           30: "augmentor (internal)", 33: "Sovrn", 36: "Cybba", 39: "Klickly", 40: "33Across API"}
INTERNAL = {23, 30}
EXTERNAL = [26, 25, 28, 40, 24, 33, 36, 39]

SIGNAL_WINDOW = "Jun 2 to Jul 1, 2026"
VALUE_WEEK = "Jul 2 to Jul 8, 2026"


def rows(name):
    with open(os.path.join(OUT, name)) as f:
        return list(csv.DictReader(f))


def by_ds(name):
    return {int(r["data_source_id"]): r for r in rows(name)}


def fmt(v):
    v = float(v)
    if v >= 1e6:
        return f"{v/1e6:.1f}M"
    return f"{v:,.0f}"


def strip(ax):
    for s in ax.spines.values():
        s.set_visible(False)


def save(fig, fname, caption):
    fig.text(0.01, 0.015, caption, fontsize=9, color="#888")
    fig.savefig(os.path.join(HERE, fname), dpi=200)
    plt.close(fig)


def bills_annual():
    b = {}
    for r in rows("audi_1089_metered_usage_by_month.csv"):
        if r["mo"] == "2026-06":
            b[int(r["data_source_id"])] = float(r["usage_dollars"]) * 12
    return b


def fee_bands():
    dom = by_ds("audi_1089_uniqueness_domains_30d.csv")
    val = by_ds("audi_1089_value_tiers.csv")
    bands = {}
    for d in EXTERNAL:
        sc = float(dom[d]["sole_classified"])
        t2_yr = float(val[d]["imps_sole"]) * 52 * 0.0005
        bands[d] = (sc * 3, sc * 13 + t2_yr)
    return bands


def quality_scores():
    dom = by_ds("audi_1089_uniqueness_domains_30d.csv")
    rec = by_ds("audi_1089_recency_pairs_30d.csv")
    val = by_ds("audi_1089_value_tiers.csv")
    vr = by_ds("audi_1089_vr_sole_by_ds.csv")
    tier = {}
    for r in rows("audi_1089_score_tiers_sole_vs_touched.csv"):
        tier.setdefault(int(r["data_source_id"]), {})[r["cohort"]] = r
    max_sc = max(float(dom[d]["sole_classified"]) for d in EXTERNAL)
    max_t1 = max(float(val[d]["imps_sole_scored_nonrtc"]) for d in EXTERNAL)
    out = {}
    for d in EXTERNAL:
        V = math.log10(float(dom[d]["sole_classified"]) + 1) / math.log10(max_sc + 1)
        R = (float(rec[d]["pct_sole"]) + float(rec[d]["pct_freshest"])) / 100
        Q = 0.5 * float(dom[d]["pct_classified"]) / 100 \
            + 0.5 * (1 - float(tier[d]["sole"]["pct_unscored_delivered"]) / 100)
        D = math.log10(float(val[d]["imps_sole_scored_nonrtc"]) + 1) / math.log10(max_t1 + 1)
        if float(vr[d]["sole_imps"]) >= 5000:
            P = min(float(vr[d]["vr_overall_pct"]) / 0.0223, 2) / 2
        else:
            P = 0.5
        out[d] = round(100 * (0.40 * V + 0.15 * R + 0.15 * Q + 0.10 * D + 0.20 * P), 1)
    return out


# ---- 1. Daily delivery table ----
def table_daily_delivery():
    agg = {}
    for r in rows("audi_1089_scale_by_day_30d.csv"):
        d = int(r["data_source_id"])
        a = agg.setdefault(d, {"rows": 0.0, "ips": 0.0, "domains": 0.0, "days": 0})
        a["rows"] += float(r["n_rows"])
        a["ips"] += float(r["ips"])
        a["domains"] += float(r["domains"])
        a["days"] += 1 if float(r["n_rows"]) > 0 else 0
    order = sorted(agg, key=lambda d: -agg[d]["ips"] / max(agg[d]["days"], 1))
    cells, row_colors = [], []
    for d in order:
        a = agg[d]
        n = a["days"] or 1
        cells.append([DS_NAME[d], fmt(a["ips"] / n), fmt(a["rows"] / n),
                      fmt(a["domains"] / n), f"{a['days']}/30"])
        row_colors.append("#eef1f4" if d in INTERNAL else "white")
    cols = ["Source", "Avg IPs / Day", "Avg Rows / Day", "Avg Domains / Day", "Days Delivered"]
    fig, ax = plt.subplots(figsize=(8.6, 4.6))
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1, 1.5)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor(row_colors[r - 1])
            if c == 0:
                cell.set_text_props(ha="left")
    ax.set_title("Daily Delivery by Source", fontsize=13.5, fontweight="bold", loc="left", pad=16)
    fig.tight_layout(rect=[0, 0.06, 1, 1])
    save(fig, "audi_1089_table_daily_delivery.png",
         f"Average per-day volumes in site_visit_signal, ranked by IPs per day, {SIGNAL_WINDOW}.")


# ---- 2. Window reach ----
def chart_window_reach():
    reach = by_ds("audi_1089_window_reach_30d.csv")
    order = sorted(EXTERNAL, key=lambda d: -float(reach[d]["ips_30d"]))
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.8))
    for ax, col, title, scale in [(axes[0], "ips_30d", "Distinct IPs", 1e6),
                                  (axes[1], "domains_30d", "Distinct Domains", 1)]:
        vals = [float(reach[d][col]) / scale for d in order]
        bars = ax.barh([DS_NAME[d] for d in order], vals, color=NAVY, height=0.6)
        for b, v in zip(bars, vals):
            lab = f"{v:,.1f}M" if scale == 1e6 else f"{v:,.0f}"
            ax.text(b.get_width() + max(vals) * 0.015, b.get_y() + b.get_height() / 2, lab,
                    va="center", fontsize=9.5)
        ax.invert_yaxis()
        ax.set_xticks([])
        ax.tick_params(axis="y", length=0, labelsize=10)
        strip(ax)
        ax.set_title(title, fontsize=11, loc="left", color="#333")
    fig.suptitle("30-Day Reach by Vendor", fontsize=13.5, fontweight="bold", x=0.02, ha="left")
    fig.tight_layout(rect=[0, 0.07, 1, 0.92])
    save(fig, "audi_1089_chart_window_reach.png",
         f"Distinct IPs and registered domains observed per vendor in site_visit_signal, {SIGNAL_WINDOW}.")


# ---- 3. Recency mix ----
def chart_recency_mix():
    data = sorted(((int(r["data_source_id"]), float(r["pct_sole"]), float(r["pct_freshest"]),
                    float(r["pct_tied"]), float(r["pct_stale"]))
                   for r in rows("audi_1089_recency_pairs_30d.csv")), key=lambda x: -x[1])
    fig, ax = plt.subplots(figsize=(9.5, 5.4))
    names = [DS_NAME[d] for d, *_ in data]
    segs = [("sole", 1, GREEN), ("freshest", 2, NAVY), ("tied same day", 3, GRAY), ("stale", 4, "#D7D9DC")]
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
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.01), ncol=4, frameon=False, fontsize=9)
    ax.set_title("30-Day Pair Recency Mix by Source", fontsize=13.5, fontweight="bold", loc="left", pad=12)
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    save(fig, "audi_1089_chart_recency_mix.png",
         f"Share of each source's (IP, domain) pairs by recency vs all other sources, {SIGNAL_WINDOW}. "
         "Sole: no other source has the pair. Tied: another source delivered it the same day.")


# ---- 4. Sole classified domains ----
def chart_sole_classified():
    data = sorted(((int(r["data_source_id"]), int(r["sole_classified"]))
                   for r in rows("audi_1089_uniqueness_domains_30d.csv")), key=lambda x: x[1])
    fig, ax = plt.subplots(figsize=(9, 5.4))
    names = [DS_NAME[d] for d, _ in data]
    vals = [v for _, v in data]
    colors = [GRAY if d in INTERNAL else NAVY for d, _ in data]
    bars = ax.barh(names, vals, color=colors, height=0.62)
    for b, v in zip(bars, vals):
        ax.text(b.get_width() + max(vals) * 0.008, b.get_y() + b.get_height() / 2,
                f"{v:,}", va="center", fontsize=10)
    ax.set_title("Sole Classified Domains by Source, 30 Days", fontsize=13.5, fontweight="bold",
                 loc="left", pad=12)
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0, labelsize=10.5)
    strip(ax)
    fig.tight_layout(rect=[0, 0.06, 1, 1])
    save(fig, "audi_1089_chart_sole_classified_domains.png",
         f"Domains only that source provides which map to a Matched vertical, {SIGNAL_WINDOW}.")


# ---- 5. Sole-IP quality ----
def chart_sole_quality():
    tier = {}
    for r in rows("audi_1089_score_tiers_sole_vs_touched.csv"):
        tier.setdefault(int(r["data_source_id"]), {})[r["cohort"]] = r
    order = sorted(EXTERNAL, key=lambda d: -float(tier[d]["sole"]["pct_delivered"]))
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.8))
    for ax, key, title in [(axes[0], "pct_delivered", "Share of Sole IPs Served"),
                           (axes[1], "pct_unscored_delivered", "Unscored Share of Served Sole IPs")]:
        vals = [float(tier[d]["sole"][key]) for d in order]
        bars = ax.barh([DS_NAME[d] for d in order], vals, color=NAVY, height=0.6)
        for b, v in zip(bars, vals):
            ax.text(b.get_width() + max(vals) * 0.015, b.get_y() + b.get_height() / 2,
                    f"{v:.1f}%", va="center", fontsize=9.5)
        ax.invert_yaxis()
        ax.set_xticks([])
        ax.tick_params(axis="y", length=0, labelsize=10)
        strip(ax)
        ax.set_title(title, fontsize=11, loc="left", color="#333")
    fig.suptitle("Delivery and Scoring of Vendor-Sole IPs", fontsize=13.5, fontweight="bold", x=0.02, ha="left")
    fig.tight_layout(rect=[0, 0.08, 1, 0.92])
    save(fig, "audi_1089_chart_sole_quality.png",
         f"IPs seen only by that vendor in the 37-day window, measured against the {VALUE_WEEK} delivery week.")


# ---- 6. Dependency by vendor ----
def chart_dependency_by_vendor():
    data = sorted(((int(r["data_source_id"]), float(r["imps_sole"]), float(r["imps_sole_scored_nonrtc"]))
                   for r in rows("audi_1089_value_tiers.csv") if int(r["data_source_id"]) not in INTERNAL),
                  key=lambda x: -x[1])
    fig, ax = plt.subplots(figsize=(9.5, 5.0))
    names = [DS_NAME[d] for d, *_ in data]
    t2 = [v for _, v, _ in data]
    t1 = [v for _, _, v in data]
    bars = ax.barh(names, t2, color=NAVY, height=0.6, label="all impressions to sole IPs")
    ax.barh(names, t1, color=RED, height=0.6, label="score-gated impressions to sole IPs")
    for b, v2, v1 in zip(bars, t2, t1):
        ax.text(b.get_width() + max(t2) * 0.01, b.get_y() + b.get_height() / 2,
                f"{v2:,.0f}  /  {v1:,.0f}", va="center", fontsize=9.5)
    ax.invert_yaxis()
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0, labelsize=10.5)
    strip(ax)
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    ax.set_title("Weekly Impressions to Vendor-Sole IPs", fontsize=13.5, fontweight="bold", loc="left", pad=12)
    fig.tight_layout(rect=[0, 0.07, 1, 1])
    save(fig, "audi_1089_chart_dependency_by_vendor.png",
         f"Impressions in the {VALUE_WEEK} week to IPs only that vendor observed. "
         "Labels: all sole impressions / score-gated non-RTC subset.")


# ---- 7. Sole-IP visit rate ----
def chart_sole_vr_baseline():
    vr = by_ds("audi_1089_vr_sole_by_ds.csv")
    base_no_svs = 0.0223
    guid_ref = float(vr[23]["vr_overall_pct"])
    order = sorted(EXTERNAL, key=lambda d: -float(vr[d]["vr_overall_pct"]))
    fig, ax = plt.subplots(figsize=(9.5, 5.0))
    names, vals, ok = [], [], []
    for d in order:
        names.append(DS_NAME[d])
        vals.append(float(vr[d]["vr_overall_pct"]))
        ok.append(float(vr[d]["sole_imps"]) >= 5000)
    colors = [NAVY if o else "#C8CCD0" for o in ok]
    bars = ax.barh(names, vals, color=colors, height=0.6)
    for b, v, d, o in zip(bars, vals, order, ok):
        note = "" if o else ", under 5K impressions"
        ax.text(b.get_width() + 0.001, b.get_y() + b.get_height() / 2,
                f"{v:.3f}% ({float(vr[d]['sole_imps']):,.0f} imps{note})", va="center", fontsize=9)
    ax.axvline(base_no_svs, color=RED, lw=1.2, ls="--")
    ax.text(base_no_svs + 0.0005, len(names) - 0.4, f"no-signal baseline {base_no_svs}%", color=RED, fontsize=9)
    ax.axvline(guid_ref, color=GREEN, lw=1.2, ls=":")
    ax.text(guid_ref + 0.0005, -0.45, f"guid_log sole {guid_ref:.2f}%", color=GREEN, fontsize=9)
    ax.invert_yaxis()
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0, labelsize=10.5)
    strip(ax)
    ax.set_title("Visit Rate on Impressions to Vendor-Sole IPs", fontsize=13.5, fontweight="bold",
                 loc="left", pad=12)
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    save(fig, "audi_1089_chart_sole_vr_baseline.png",
         f"Visit rate for the {VALUE_WEEK} week. Grey bars have under 5,000 impressions and are treated "
         "as neutral in the quality score.")


# ---- 8. Fee bands vs bills ----
def chart_fee_bands_vs_bills():
    bands = fee_bands()
    bills = bills_annual()
    order = [26, 25, 28, 40, 33, 24, 36, 39]
    fig, axes = plt.subplots(len(order), 1, figsize=(9.5, 7.6))
    for ax, d in zip(axes, order):
        lo, hi = bands[d]
        bill = bills.get(d)
        xmax = max(hi * 1.35, (bill or 0) * 1.15, hi + 1)
        ax.axvspan(0, hi, color=GREEN, alpha=0.22)
        ax.axvspan(hi, hi * 3, color=AMBER, alpha=0.20)
        ax.axvspan(hi * 3, xmax, color=RED, alpha=0.14)
        if bill:
            over = bill / hi
            col = GREEN if bill <= hi else (AMBER if bill <= 3 * hi else RED)
            ax.plot([bill], [0.42], marker="v", color=col, markersize=12, zorder=5)
            txt = f"bill ${bill/1e3:,.0f}K/yr" + (f", {over:.1f}x band" if over > 1 else "")
            ax.text(min(bill, xmax * 0.98), 0.80, txt, fontsize=9, fontweight="bold", color=col,
                    ha="right" if bill > xmax * 0.6 else "left")
        else:
            ax.text(xmax * 0.98, 0.72, "flat fee, amount pending", fontsize=9, style="italic",
                    color="#888", ha="right")
        ax.text(hi, 0.10, f"band top ${hi/1e3:,.0f}K/yr", fontsize=8.5, color=GREEN, ha="right")
        ax.set_xlim(0, xmax)
        ax.set_ylim(0, 1)
        ax.set_yticks([])
        ax.set_xticks([])
        strip(ax)
        ax.set_ylabel(DS_NAME[d], rotation=0, ha="right", va="center", fontsize=10.5)
    fig.suptitle("Defensible Fee Band vs Actual Annualized Bill", fontsize=13.5, fontweight="bold",
                 x=0.02, ha="left")
    fig.tight_layout(rect=[0, 0.06, 1, 0.95])
    save(fig, "audi_1089_chart_fee_bands_vs_bills.png",
         "Band: sole classified domains x $3 to $13 per year plus sole impressions at $0.50 CPM. "
         "Bill: June 2026 metered usage x 12. Each row has its own scale.")


# ---- 9. Quality scores ----
def chart_quality_scores():
    scores = quality_scores()
    order = sorted(EXTERNAL, key=lambda d: -scores[d])
    fig, ax = plt.subplots(figsize=(9, 4.8))
    vals = [scores[d] for d in order]
    bars = ax.barh([DS_NAME[d] for d in order], vals, color=NAVY, height=0.6)
    for b, v in zip(bars, vals):
        ax.text(b.get_width() + 1, b.get_y() + b.get_height() / 2, f"{v:.0f}", va="center",
                fontsize=11, fontweight="bold")
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0, labelsize=10.5)
    strip(ax)
    ax.set_title("Quality Score by Vendor, v1", fontsize=13.5, fontweight="bold", loc="left", pad=12)
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    save(fig, "audi_1089_chart_quality_scores.png",
         "Composite of unique value 40%, performance 20%, non-redundancy 15%, signal quality 15%, "
         "dependency 10%. Baseline run July 2026.")


# ---- 10. Score vs bill ----
def chart_score_vs_bill_quadrant():
    scores = quality_scores()
    bills = bills_annual()
    fig, ax = plt.subplots(figsize=(8.6, 5.6))
    for d in EXTERNAL:
        s = scores[d]
        if d in bills:
            b = bills[d] / 1e3
            ax.plot([b], [s], "o", color=NAVY, markersize=9)
            ax.annotate(f"{DS_NAME[d]} (${b:,.0f}K/yr)", (b, s), textcoords="offset points",
                        xytext=(8, 4), fontsize=9.5)
        else:
            ax.plot([2], [s], "s", color=GRAY, markersize=8)
            ax.annotate(f"{DS_NAME[d]}, fee pending", (2, s), textcoords="offset points",
                        xytext=(8, -12), fontsize=9, color="#888")
    ax.set_xlim(0, 440)
    ax.set_ylim(0, 100)
    ax.set_xlabel("annualized bill, $K per year", fontsize=9.5, color="#555")
    ax.set_ylabel("quality score, v1", fontsize=9.5, color="#555")
    ax.tick_params(labelsize=9)
    strip(ax)
    ax.set_title("Quality Score vs Annualized Bill", fontsize=13.5, fontweight="bold", loc="left", pad=12)
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    save(fig, "audi_1089_chart_score_vs_bill_quadrant.png",
         "Circles: June 2026 metered bill x 12. Squares: flat-fee vendors, amount pending from the renewal schedule.")


# ---- 11. Klickly dependency ladder ----
def chart_dependency_ladder():
    r39 = next(r for r in rows("audi_1089_value_tiers.csv") if int(r["data_source_id"]) == 39)
    steps = [
        ("Impressions to IPs Klickly touched", float(r39["imps_touched"]), float(r39["media_touched"]), GRAY),
        ("Impressions to IPs only Klickly saw", float(r39["imps_sole"]), float(r39["media_sole"]), NAVY),
        ("Score-gated impressions to IPs only Klickly saw", float(r39["imps_sole_scored_nonrtc"]),
         float(r39["media_sole_scored"]), RED),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 3.1)
    ax.axis("off")
    for i, (label, imps, media, color) in enumerate(steps):
        y = 2.5 - i * 1.0
        ax.text(0.02, y + 0.28, label, fontsize=11.5, color="#333")
        ax.text(0.02, y - 0.12, fmt(imps), fontsize=25, fontweight="bold", color=color)
        ax.text(0.30, y - 0.08, f"impressions per week,  ${media:,.2f} media", fontsize=10.5, color="#555")
    ax.set_title("Klickly Weekly Impression Dependency", fontsize=13.5, fontweight="bold", loc="left", pad=14)
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    save(fig, "audi_1089_chart_klickly_dependency_ladder.png",
         f"Valuation week {VALUE_WEEK}. Sole means no other source, internal or vendor, saw the IP in the window.")


# ---- 12. Klickly cohorts ----
def chart_adverse_selection():
    t = {r["cohort"]: r for r in rows("audi_1089_score_tiers_sole_vs_touched.csv")
         if int(r["data_source_id"]) == 39}
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.4))
    for ax, (key, title) in zip(axes, [("pct_delivered", "Share of IPs Served"),
                                       ("pct_unscored_delivered", "Unscored Share of Served IPs")]):
        vals = [float(t["touched"][key]), float(t["sole"][key])]
        bars = ax.bar(["shared IPs", "Klickly-only IPs"], vals, color=[GRAY, NAVY], width=0.55)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v + max(vals) * 0.02, f"{v:.1f}%",
                    ha="center", fontsize=12, fontweight="bold")
        ax.set_title(title, fontsize=11, loc="left", color="#333")
        ax.set_yticks([])
        ax.tick_params(axis="x", length=0, labelsize=10)
        strip(ax)
    fig.suptitle("Klickly IP Cohorts: Delivery and Scoring", fontsize=13.5, fontweight="bold", x=0.02, ha="left")
    fig.tight_layout(rect=[0, 0.09, 1, 0.92])
    save(fig, "audi_1089_chart_klickly_adverse_selection.png",
         f"IP cohorts over the 37-day window measured against the {VALUE_WEEK} delivery week.")


if __name__ == "__main__":
    table_daily_delivery()
    chart_window_reach()
    chart_recency_mix()
    chart_sole_classified()
    chart_sole_quality()
    chart_dependency_by_vendor()
    chart_sole_vr_baseline()
    chart_fee_bands_vs_bills()
    chart_quality_scores()
    chart_score_vs_bill_quadrant()
    chart_dependency_ladder()
    chart_adverse_selection()
    print("wrote 12 images to", HERE)
