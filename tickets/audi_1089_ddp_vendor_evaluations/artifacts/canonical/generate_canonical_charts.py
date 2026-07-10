#!/usr/bin/env python3
"""Canonical DDP quality-score pipeline visuals, one function per runbook step.
Reads ../../outputs/run_<date>/*.csv (latest run by default, or --run run_YYYY_MM_DD),
writes PNGs alongside this script. Style matches audi_1089_generate_charts.py:
plain descriptive titles, one-line caption below, no annotations, no em-dashes."""
import argparse
import csv
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
OUTROOT = os.path.join(HERE, "..", "..", "outputs")
BG, NAVY, RED, GRAY, GREEN, AMBER = "#FAFAFA", "#27496D", "#D63B2F", "#9AA0A6", "#2E8B57", "#C77B30"
HILITE = "#dbe4ee"
plt.rcParams.update({"font.family": "Helvetica Neue", "figure.facecolor": BG,
                     "axes.facecolor": BG, "savefig.facecolor": BG, "axes.edgecolor": BG})


def run_dir(name=None):
    if name:
        return os.path.join(OUTROOT, name)
    runs = sorted(d for d in os.listdir(OUTROOT) if d.startswith("run_"))
    if not runs:
        raise SystemExit("no run_* directory under outputs/")
    return os.path.join(OUTROOT, runs[-1])


def save(fig, fname, caption):
    fig.text(0.01, 0.015, caption, fontsize=9, color="#888", wrap=True)
    fig.savefig(os.path.join(HERE, fname), dpi=200)
    plt.close(fig)
    print("wrote", fname)


def money(v):
    return f"${v/1e3:,.1f}K" if v < 1e6 else f"${v/1e6:,.2f}M"


# ---- Step 0: roster + actual cost ----
def q0(rdir):
    vendors = {}
    with open(os.path.join(rdir, "q0_roster_cost.csv")) as f:
        for r in csv.DictReader(f):
            d = int(r["data_source_id"])
            v = vendors.setdefault(d, {"name": r["data_partner_name"], "billing": r["billing_type"],
                                       "cpm": r["fixed_cpm"], "enabled": r["enabled"] == "true",
                                       "mm": r["used_in_mntn_match"] == "true", "type": r["type"],
                                       "months": {}, "checks": []})
            if r["reporting_month"]:
                v["months"][r["reporting_month"]] = float(r["usage_dollars"])
                if r["meter_check_ok"]:
                    v["checks"].append(r["meter_check_ok"] == "true")
    months = sorted({m for v in vendors.values() for m in v["months"]})
    latest = months[-1]
    MON = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    latest_lbl = f"{MON[int(latest[5:7])]} {latest[:4]} bill"

    def bill_row(d):
        v = vendors[d]
        rate = f"${float(v['cpm']):.2f} CPM" if v["cpm"] else \
               ("variable CPM" if v["billing"] == "variable_cpm" else "flat fee")
        b = v["months"].get(latest)
        if b is not None:
            bill, ann = money(b), money(b * 12) + "/yr"
        elif v["billing"] == "flat_fee":
            bill, ann = "unknown", "renewal schedule"
        else:
            bill, ann = "$0 (no delivery)", "$0/yr"
        chk = f"{float(v['cpm']):.2f} ok" if v["checks"] and all(v["checks"]) else \
              ("variable" if b is not None and v["billing"] == "variable_cpm" else "n/a")
        name = v["name"] + ("" if v["enabled"] else " (disabled)")
        return [name, f"DS{d}", v["billing"] or "none", rate, bill, ann, chk]

    mm = [d for d, v in vendors.items() if v["mm"]]
    ctx = [d for d, v in vendors.items() if not v["mm"]]
    key = lambda d: -(vendors[d]["months"].get(latest) or -1)
    mm = sorted([d for d in mm if vendors[d]["months"]], key=key) \
        + sorted([d for d in mm if not vendors[d]["months"] and vendors[d]["enabled"]]) \
        + sorted([d for d in mm if not vendors[d]["months"] and not vendors[d]["enabled"]])
    ctx = sorted(ctx, key=key)

    cells = [bill_row(d) for d in mm]
    sep_at = len(cells)
    cells.append(["Metered but outside MM site-visit scope (interests / CRM)", "", "", "", "", "", ""])
    cells += [bill_row(d) for d in ctx]
    cols = ["Source", "DS", "Billing", "Rate", latest_lbl, "Annualized", "Meter check"]

    mm_metered = [d for d in mm if vendors[d]["months"]]
    hi_row = 1 + mm.index(max(mm_metered, key=lambda d: vendors[d]["months"][latest]))

    fig = plt.figure(figsize=(12.6, 9.4))
    gs = fig.add_gridspec(2, 1, height_ratios=[1.05, 0.62], hspace=0.16,
                          left=0.04, right=0.97, top=0.90, bottom=0.13)
    ax = fig.add_subplot(gs[0])
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1, 1.45)
    widths = [0.26, 0.06, 0.12, 0.12, 0.13, 0.15, 0.12]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        elif r == sep_at + 1:
            cell.set_facecolor("#eef1f4")
            cell.set_text_props(color="#666", fontsize=9, fontweight="bold", ha="left")
        else:
            cell.set_facecolor("white" if r <= sep_at else "#f4f5f7")
            if c == 0:
                cell.set_text_props(ha="left")
            if c in (1, 2, 6):
                cell.set_text_props(color="#666")
            if r > sep_at + 1:
                cell.set_text_props(color="#888")
            if r == hi_row and c in (4, 5):
                cell.set_facecolor(HILITE)
                cell.set_text_props(fontweight="bold", color=NAVY)
    ax.set_title("DDP Roster and Actual Metered Bills, Last 6 Months",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)

    trend = [d for d in mm if vendors[d]["months"]] + [d for d in ctx if vendors[d]["months"]]
    sub = fig.add_subplot(gs[1])
    sub.axis("off")
    inner = gs[1].subgridspec(1, len(trend), wspace=0.35)
    xt = [m[5:7].lstrip("0") for m in months]
    for i, d in enumerate(trend):
        v = vendors[d]
        axi = fig.add_subplot(inner[i])
        ys = [v["months"].get(m) for m in months]
        color = NAVY if v["mm"] else GRAY
        axi.plot(range(len(months)), ys, color=color, lw=1.8)
        axi.plot(len(months) - 1, ys[-1], "o", color=color, ms=4)
        axi.set_title(v["name"], fontsize=9, color="#333")
        axi.text(len(months) - 1, ys[-1], "  " + money(ys[-1]), fontsize=8, color=color,
                 va="center", fontweight="bold")
        axi.set_ylim(0, max(ys) * 1.35)
        axi.set_xlim(-0.4, len(months) + 1.1)
        axi.set_xticks(range(len(months)))
        axi.set_xticklabels(xt, fontsize=7, color="#999")
        axi.set_yticks([])
        for s in axi.spines.values():
            s.set_visible(False)
        axi.spines["bottom"].set_visible(True)
        axi.spines["bottom"].set_color("#ddd")
    save(fig, "q0_roster_cost.png",
         "Registry roster (tpa.direct_data_partners, CDC deduped) joined to the billing meter "
         "(coredw.usage_reporting_data), Jan to Jun 2026. Metered usage equals impressions x contract CPM, "
         "credited 1/N across vendors sharing an IP. Flat fee amounts are not in our data and come from the "
         "renewal schedule. Grey sources share the meter but are interests or CRM vendors, outside this "
         "evaluation; monthly bill scales differ per panel. ShareThis rate dropped from $1.20 to $0.95 CPM "
         "with May usage per registry notes.")


STEPS = {"0": q0}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default=None, help="run_YYYY_MM_DD under outputs/ (default: latest)")
    ap.add_argument("--step", default=None, help="single step number (default: all built so far)")
    a = ap.parse_args()
    rdir = run_dir(a.run)
    print("run dir:", os.path.relpath(rdir, HERE))
    for k, fn in STEPS.items():
        if a.step in (None, k):
            fn(rdir)
