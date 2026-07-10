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
                                       "months": {}})
            if r["reporting_month"]:
                v["months"][r["reporting_month"]] = float(r["usage_dollars"])
    months = sorted({m for v in vendors.values() for m in v["months"]})
    MON = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    mlbl = [f"{MON[int(m[5:7])]} {m[:4]}" for m in months]
    BILLING = {"fixed_cpm": "fixed CPM", "flat_fee": "flat fee", "variable_cpm": "variable CPM"}

    def row(d):
        v = vendors[d]
        rate = f"${float(v['cpm']):.2f}" if v["cpm"] else "-"
        name = v["name"] + ("" if v["enabled"] else " (disabled)")
        cells = [name, f"DS{d}", BILLING.get(v["billing"], v["billing"] or "-"), rate]
        if v["months"]:
            cells += [f"${v['months'][m]:,.0f}" if m in v["months"] else "-" for m in months]
            cells.append(f"${sum(v['months'].values()):,.0f}")
        else:
            cells += ["-"] * len(months)
            cells.append("not metered" if v["billing"] == "flat_fee" else "$0")
        return cells

    tot = lambda d: sum(vendors[d]["months"].values())
    mm = [d for d, v in vendors.items() if v["mm"]]
    ctx = sorted([d for d, v in vendors.items() if not v["mm"]], key=lambda d: -tot(d))
    mm = sorted([d for d in mm if vendors[d]["months"]], key=lambda d: -tot(d)) \
        + sorted([d for d in mm if not vendors[d]["months"] and vendors[d]["enabled"]]) \
        + sorted([d for d in mm if not vendors[d]["months"] and not vendors[d]["enabled"]])

    cells = [row(d) for d in mm]
    sep_at = len(cells)
    cells.append(["Outside MM scope (interests / CRM)"] + [""] * (4 + len(months)))
    cells += [row(d) for d in ctx]
    cols = ["Source", "DS", "Billing", "Rate", *mlbl, "6-mo total"]

    fig = plt.figure(figsize=(13.4, 5.8))
    fig.subplots_adjust(left=0.02, right=0.98, top=0.86, bottom=0.12)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9.5)
    tbl.scale(1, 1.5)
    widths = [0.17, 0.05, 0.09, 0.06] + [0.085] * len(months) + [0.115]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        elif r == sep_at + 1:
            cell.set_facecolor("#eef1f4")
            cell.set_text_props(color="#666", fontsize=8.5, fontweight="bold", ha="left")
        else:
            cell.set_facecolor("white" if r <= sep_at else "#f4f5f7")
            if c == 0:
                cell.set_text_props(ha="left")
            if c in (1, 2):
                cell.set_text_props(color="#666")
            if c == len(widths) - 1:
                cell.set_text_props(fontweight="bold")
            if r > sep_at + 1:
                cell.set_text_props(color="#888")
    ax.set_title("DDP Roster and Actual Metered Bills by Month, Jan to Jun 2026",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q0_roster_cost.png",
         "Registry roster (tpa.direct_data_partners, CDC deduped) joined to the billing meter "
         "(coredw.usage_reporting_data). Metered usage equals impressions x contract CPM, credited 1/N "
         "across vendors sharing an IP; meter check passes against each source's registry rate. Flat fee "
         "amounts are not in our data and come from the renewal schedule. Grey sources share the meter but "
         "are interests or CRM vendors, outside this evaluation. ShareThis rate dropped from $1.20 to "
         "$0.95 CPM with May usage per registry notes.")


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
