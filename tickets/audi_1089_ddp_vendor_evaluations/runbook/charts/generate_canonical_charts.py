#!/usr/bin/env python3
"""Canonical DDP quality-score pipeline visuals, one function per runbook step.
Reads ../../outputs/run_<date>/*.csv (latest run by default, or --run run_YYYY_MM_DD),
writes PNGs alongside this script. Style matches audi_1089_generate_charts.py:
plain descriptive titles, one-line caption below, no annotations, no em-dashes."""
import argparse
import csv
import re
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


def save(fig, fname, caption=""):
    if caption:
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

    cells = [row(d) for d in mm] + [row(d) for d in ctx]
    sep_at = len(mm)
    cols = ["Source", "DS", "Billing", "Rate", *mlbl, "6-mo total"]

    fig = plt.figure(figsize=(11.2, 4.4))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.86, bottom=0.05)
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
        else:
            cell.set_facecolor("white" if r <= sep_at else "#f4f5f7")
            if c == 0:
                cell.set_text_props(ha="left")
            if c in (1, 2):
                cell.set_text_props(color="#666")
            if c == len(widths) - 1:
                cell.set_text_props(fontweight="bold")
            if r > sep_at:
                cell.set_text_props(color="#888")
    ax.set_title("DDP Roster and Actual Metered Bills by Month, Jan to Jun 2026",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q0_roster_cost.png")


# ---- Step 1: scale & liveness ----
NAMES = {23: "guid_log (internal)", 24: "Justuno", 25: "5x5", 26: "Predactiv", 28: "33Across",
         30: "augmentor (internal)", 33: "Sovrn", 36: "Cybba", 39: "Klickly", 40: "33Across API"}
MON = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def q1(rdir):
    by_ds, days = {}, set()
    with open(os.path.join(rdir, "q1_scale_by_day.csv")) as f:
        for r in csv.DictReader(f):
            days.add(r["dt"])
            by_ds.setdefault(int(r["data_source_id"]), {})[r["dt"]] = \
                (int(r["n_rows"]), float(r["pct_ipv6"]))
    days = sorted(days)
    dlbl = lambda dt: f"{MON[int(dt[5:7])]} {int(dt[8:10])}"

    med = {}
    for d, m in by_ds.items():
        vals = sorted(v[0] for v in m.values())
        med[d] = vals[len(vals) // 2]
    ext = sorted((d for d in by_ds if d not in (23, 30)), key=lambda d: -med[d])
    order = ext + sorted((d for d in (23, 30) if d in by_ds), key=lambda d: -med[d])
    sep_at = len(ext)
    cells, flags = [], []
    for d in order:
        m = by_ds[d]
        partial = sorted(x for x, v in m.items() if v[0] < 0.5 * med[d])
        mind, minv = min(m.items(), key=lambda kv: kv[1][0])
        ipv6 = sum(v[1] for v in m.values()) / len(m)
        gate = "PASS" if len(m) >= 0.95 * len(days) else "FAIL"
        plbl = f"{len(partial)}:  " + ", ".join(dlbl(x) for x in partial) if partial else "0"
        cells.append([NAMES.get(d, f"DS{d}"), f"DS{d}", f"{len(m)}/{len(days)}", plbl,
                      f"{med[d]:,}", f"{100 * minv[0] / med[d]:.0f}%  ({dlbl(mind)})",
                      f"{ipv6:.1f}%", gate])
        flags.append((bool(partial), gate))
    cols = ["Source", "DS", "Days", "Partial days (<50% med)",
            "Median rows/day", "Weakest (% of med)", "IPv6", "Gate"]

    fig = plt.figure(figsize=(10.2, 3.4))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.82, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9.5)
    tbl.scale(1, 1.5)
    widths = [0.19, 0.06, 0.075, 0.22, 0.155, 0.15, 0.075, 0.075]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white" if r <= sep_at else "#f4f5f7")
            has_partial, gate = flags[r - 1]
            if c == 0:
                cell.set_text_props(ha="left")
            if c == 1:
                cell.set_text_props(color="#666")
            if c == 3 and has_partial:
                cell.set_text_props(color=AMBER, fontweight="bold")
            if c == 5 and has_partial:
                cell.set_text_props(color=AMBER)
            if c == 7:
                cell.set_text_props(color=GREEN if gate == "PASS" else RED, fontweight="bold")
            if r > sep_at:
                cell.set_text_props(color="#888")
    ax.set_title(f"DDP Feed Liveness and Daily Scale, {dlbl(days[0])} to {dlbl(days[-1])} {days[-1][:4]}",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q1_scale_by_day.png")


# ---- Step 1b: column richness of each vendor's drop ----
SHORT = {23: "guid_log", 24: "Justuno", 25: "5x5", 26: "Predactiv", 28: "33Across",
         30: "augmentor", 33: "Sovrn", 36: "Cybba", 39: "Klickly", 40: "33A API"}


def q1b(rdir):
    prof, nrows = {}, {}
    with open(os.path.join(rdir, "q1b_column_richness.csv")) as f:
        for r in csv.DictReader(f):
            d = int(r["data_source_id"])
            prof[(d, r["field"])] = (float(r["pct_populated"]), r["example_modal"])
            nrows[d] = int(r["n_rows"])
    path_pct = {}
    with open(os.path.join(rdir, "q1_scale_by_day.csv")) as f:
        for r in csv.DictReader(f):
            path_pct.setdefault(int(r["data_source_id"]), []).append(float(r["pct_with_path"]))
    path_med = {d: sorted(v)[len(v) // 2] for d, v in path_pct.items()}

    ext = sorted((d for d in nrows if d not in (23, 30)), key=lambda d: -nrows[d])
    order = ext + sorted((d for d in (23, 30) if d in nrows), key=lambda d: -nrows[d])
    n_ext = len(ext)

    def pfmt(p):
        return "-" if p == 0 else (f"{p:.0f}" if round(p, 1).is_integer() else f"{p:.1f}")

    FIELDS = [
        ("ip", "50.148.233.82"),
        ("time", "2026-07-01 12:00:30.901+00"),
        ("uid", "01KWF42SNX0H1CZ7V22A3JXY44  (ULID)"),
        ("url", "varies by source, see the URL richness chart"),
        ("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit..."),
        ("advertiser_id", "49489  (internal guid_log only)"),
        ("query_parameters", "never populated by any source"),
    ]
    PART = [("dt", "2026-07-01  (partition key)"), ("hh", "12  (partition key)"),
            ("data_source_id", "39  (partition key)")]

    cells_top = [[f, ex] + [pfmt(prof[(d, f)][0]) for d in order] for f, ex in FIELDS]
    cells_top += [[f, ex] + ["100"] * len(order) for f, ex in PART]
    sep_top = len(FIELDS)
    cols_top = ["Field", "Example"] + [SHORT[d] for d in order]

    def strip_url(u):
        u = re.sub(r"^https?://", "", u)
        return u[:58] + ("..." if len(u) > 58 else "")

    b_ext = sorted(ext, key=lambda d: -path_med[d])
    b_int = sorted((d for d in (23, 30) if d in nrows), key=lambda d: -path_med[d])
    cells_bot = [[SHORT[d], f"{path_med[d]:.0f}%", strip_url(prof[(d, 'url')][1]) or "-"]
                 for d in b_ext + b_int]
    cols_bot = ["Source", "URLs w/ path", "Modal URL in the hour slice"]

    fig = plt.figure(figsize=(12.2, 3.3))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.85, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells_top, colLabels=cols_top, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.4)
    widths = [0.115, 0.325] + [0.056] * len(order)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", fontsize=8)
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white" if r <= sep_top else "#f4f5f7")
            if c == 0:
                cell.set_text_props(ha="left", fontweight="bold")
            if c == 1:
                cell.set_text_props(ha="left", color="#666")
            if c >= 2 + n_ext or r > sep_top:
                cell.set_text_props(color="#888")
    ax.set_title("Drop Schema: Every Source Ships the Same 10 Columns, Populated Differently",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q1b_schema_fields.png")

    fig2 = plt.figure(figsize=(7.6, 3.3))
    fig2.subplots_adjust(left=0.03, right=0.97, top=0.85, bottom=0.05)
    ax2 = fig2.add_subplot(111)
    ax2.axis("off")
    tbl2 = ax2.table(cellText=cells_bot, colLabels=cols_bot, loc="center", cellLoc="right")
    tbl2.auto_set_font_size(False)
    tbl2.set_fontsize(9)
    tbl2.scale(1, 1.4)
    widths2 = [0.16, 0.14, 0.68]
    for (r, c), cell in tbl2.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths2[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white" if r <= len(b_ext) else "#f4f5f7")
            if c in (0, 2):
                cell.set_text_props(ha="left")
            if r > len(b_ext):
                cell.set_text_props(color="#888")
    ax2.set_title("URL Richness: % of Rows with a Path (30d median) + Modal URL",
                  fontsize=12, fontweight="bold", loc="left", pad=14)
    save(fig2, "q1b_url_richness.png")


# ---- Step 1c: content quality — junk markers ----
def q1c(rdir):
    rows = []
    with open(os.path.join(rdir, "q1c_content_quality.csv")) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    data = {int(r["ds"]): r for r in rows}
    ext = sorted((d for d in data if d not in (23, 30)), key=lambda d: -int(data[d]["n"]))
    order = ext + sorted((d for d in (23, 30) if d in data), key=lambda d: -int(data[d]["n"]))

    def pct(v, nd=1):
        if v in ("", None):
            return "-"
        x = max(0.0, float(v))
        return "0" if x < 0.05 else f"{x:.{nd}f}%"

    cells, marks = [], []
    for d in order:
        r = data[d]
        top_dom = r["top_domain"] or "(unparsed)" if float(r["url_parse_fail_pct"] or 0) > 5 \
            else (r["top_domain"] or "(empty)")
        dom_cell = f"{top_dom}  ({float(r['top_domain_share']):.0f}%)"
        cells.append([SHORT[d], f"{int(r['n']):,}", pct(r["pct_googlebot_ip"]),
                      pct(r["ua_bot_pct"]), pct(r["top_ip_share"]),
                      pct(r["url_parse_fail_pct"]), pct(r["url_malformed_pct"]),
                      dom_cell, f"{float(r['top5_domain_share']):.0f}%",
                      f"{int(r['dom_distinct']):,}", f"{int(r['host_distinct']):,}"])
        junk = [max(0.0, float(r[k] or 0)) for k in
                ("pct_googlebot_ip", "ua_bot_pct", "top_ip_share",
                 "url_parse_fail_pct", "url_malformed_pct")]
        conc = [float(r["top_domain_share"]), float(r["top5_domain_share"])]
        marks.append((junk, conc))
    cols = ["Source", "Rows (hr)", "Googlebot IP", "Bot UA", "Top IP", "URL parse fail",
            "URL malformed", "Top domain (share)", "Top-5 share", "Domains (hr)", "Hosts (hr)"]

    fig = plt.figure(figsize=(12.4, 3.4))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.83, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.5)
    widths = [0.105, 0.09, 0.082, 0.062, 0.062, 0.088, 0.088, 0.175, 0.072, 0.088, 0.088]
    JCOL = {2: 0, 3: 1, 4: 2, 5: 3, 6: 4}
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", fontsize=8.5)
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white" if r <= len(ext) else "#f4f5f7")
            junk, conc = marks[r - 1]
            if c == 0:
                cell.set_text_props(ha="left")
            if c in JCOL and junk[JCOL[c]] >= 3:
                cell.set_text_props(color=RED if junk[JCOL[c]] >= 25 else AMBER, fontweight="bold")
            if c == 7 and conc[0] >= 40:
                cell.set_text_props(color=RED if conc[0] >= 70 else AMBER, fontweight="bold")
            if c == 8 and conc[1] >= 40:
                cell.set_text_props(color=RED if conc[1] >= 70 else AMBER, fontweight="bold")
            if r > len(ext):
                cell.set_text_props(color="#888")
    ax.set_title("Content Quality: Junk and Concentration Markers by Source, One-Hour Slice",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q1c_content_quality.png")

    bad = [d for d in order if float(data[d]["url_parse_fail_pct"] or 0) >= 0.5]
    bad.sort(key=lambda d: -float(data[d]["url_parse_fail_pct"]))
    cells_ex = [[SHORT[d], f"{float(data[d]['url_parse_fail_pct']):.1f}%",
                 f"{max(0.0, float(data[d]['url_malformed_pct'] or 0)):.1f}%",
                 (data[d]["unparsed_example"] or "-")[:80]] for d in bad]
    cols_ex = ["Source", "Parse fail", "Malformed", "Example URL that fails domain parsing"]
    fig2 = plt.figure(figsize=(9.6, 0.85 + 0.34 * len(bad)))
    fig2.subplots_adjust(left=0.03, right=0.97, top=0.72, bottom=0.06)
    ax2 = fig2.add_subplot(111)
    ax2.axis("off")
    tbl2 = ax2.table(cellText=cells_ex, colLabels=cols_ex, loc="center", cellLoc="right")
    tbl2.auto_set_font_size(False)
    tbl2.set_fontsize(9)
    tbl2.scale(1, 1.5)
    widths2 = [0.11, 0.09, 0.10, 0.68]
    for (r, c), cell in tbl2.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths2[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white" if bad[r - 1] not in (23, 30) else "#f4f5f7")
            if c in (0, 3):
                cell.set_text_props(ha="left")
            if c == 1:
                v = float(data[bad[r - 1]]["url_parse_fail_pct"])
                cell.set_text_props(color=RED if v >= 25 else AMBER, fontweight="bold")
            if bad[r - 1] in (23, 30):
                cell.set_text_props(color="#888")
    ax2.set_title("Unparseable URLs: Sources Over 0.5% Parse Fail, With a Live Example",
                  fontsize=12.5, fontweight="bold", loc="left", pad=12)
    save(fig2, "q1c_unparsed_examples.png")


# ---- Step 1d: what we actually paid for (billed usage vs delivered) ----
def fmtn(v):
    v = float(v)
    if v >= 1e9:
        return f"{v/1e9:.1f}B"
    if v >= 1e6:
        return f"{v/1e6:.1f}M"
    return f"{v:,.0f}"


def q1d(rdir):
    billed = {}
    with open(os.path.join(rdir, "q1d_billed_usage.csv")) as f:
        for r in csv.DictReader(f):
            billed[int(r["ds"])] = r
    raw = {}
    with open(os.path.join(rdir, "q1_scale_by_day.csv")) as f:
        for r in csv.DictReader(f):
            raw[int(r["data_source_id"])] = raw.get(int(r["data_source_id"]), 0) + int(r["n_rows"])
    reach = {}
    with open(os.path.join(rdir, "q2_window_reach.csv")) as f:
        for r in csv.DictReader(f):
            reach[int(r["data_source_id"])] = int(r["domains_30d"])

    ext = sorted((d for d in raw if d not in (23, 30)), key=lambda d: -raw[d])
    cells = []
    for d in ext:
        b = billed.get(d)
        dom = reach.get(d, 0)
        if b:
            bi, bd = float(b["billed_imps"]), int(b["billed_domains"] or 0)
            cells.append([SHORT[d], fmtn(raw[d]), fmtn(dom), fmtn(bi),
                          f"{100 * bi / raw[d]:.2f}%", f"{bd:,}",
                          f"{100 * bd / dom:.2f}%" if dom else "-",
                          money(float(b["billed_usd"])), money(float(b["billed_usd"]) * 12) + "/yr"])
        else:
            cells.append([SHORT[d], fmtn(raw[d]), fmtn(dom), "-", "-", "-", "-", "flat fee", "renewal sched."])
    cols = ["Source", "Rows delivered (30d)", "Domains (30d)", "Imps billed (Jun)",
            "% rows billed", "Domains billed", "% domains billed", "Jun bill", "Run rate"]

    fig = plt.figure(figsize=(12.0, 3.1))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.82, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.5)
    widths = [0.10, 0.125, 0.095, 0.115, 0.095, 0.10, 0.11, 0.085, 0.115]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", fontsize=8.5)
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white")
            if c == 0:
                cell.set_text_props(ha="left")
            if c in (4, 6):
                cell.set_text_props(fontweight="bold", color=NAVY)
            if cells[r - 1][7] == "flat fee" and c >= 3:
                cell.set_text_props(color="#888")
            if c == 8:
                cell.set_text_props(fontweight="bold")
    ax.set_title("What We Actually Pay For: Delivered Feed vs Billed Usage, June 2026",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q1d_used_vs_delivered.png")

    metered = [d for d in ext if d in billed]
    cells2 = [[SHORT[d], f"{float(billed[d]['pct_imps_domain_attributed']):.0f}%",
               f"{int(billed[d]['billed_domains'] or 0):,}",
               billed[d]["top5_billed_domains"][:95]] for d in metered]
    cols2 = ["Source", "Imps w/ domain", "Billed domains", "Top billed domains (share of attributed imps)"]
    fig2 = plt.figure(figsize=(11.6, 0.9 + 0.34 * len(metered)))
    fig2.subplots_adjust(left=0.03, right=0.97, top=0.76, bottom=0.06)
    ax2 = fig2.add_subplot(111)
    ax2.axis("off")
    tbl2 = ax2.table(cellText=cells2, colLabels=cols2, loc="center", cellLoc="right")
    tbl2.auto_set_font_size(False)
    tbl2.set_fontsize(8.5)
    tbl2.scale(1, 1.5)
    widths2 = [0.09, 0.10, 0.10, 0.69]
    BADDOM = re.compile(r"https |https,|https$|cookies\.|sync\.|csync\.|cs\.|cs-server|\.ai ")
    for (r, c), cell in tbl2.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths2[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white")
            if c in (0, 3):
                cell.set_text_props(ha="left")
            if c == 3 and BADDOM.search(cells2[r - 1][3]):
                cell.set_text_props(color=RED)
    ax2.set_title("Billed Domains Include Junk: Malformed Hosts and Cookie-Sync Endpoints Get Paid",
                  fontsize=12.5, fontweight="bold", loc="left", pad=12)
    save(fig2, "q1d_billed_domains.png")


# ---- Step 1e: columns MM consumes today vs latent value (synthesis, no query) ----
# Sources: airflow-ti code audit 2026-07-10 (site_visit_signal_advertiser_id_dsc_id.py,
# distinct_site_visit_signal_domains.py, AP-3779 targeted_signal) + q1b/q1c population stats.
def q1e(rdir):
    rows = [
        ("ip", "used",
         "THE household key: feature rollups, MM scoring unit,\nbilling credit key (first reporter of ip+url+day)",
         "", "all sources, 100%"),
        ("url - domain", "used",
         "DS13: domain to wcv verticals; DS19: domain to product\ncategories; the billing credit key with ip",
         "", "all sources, ~100%"),
        ("time", "used",
         "First-reporter-wins ordering; day-grain recency",
         "Hour-grain recency decay, dayparting features", "all sources, 100%"),
        ("uid", "used",
         "Row identity: dedup + lineage into targeted_signal",
         "", "all sources, 100%"),
        ("dt / hh / data_source_id", "used",
         "Partitioning, ingestion lag config, payout attribution",
         "", "all sources (partition keys)"),
        ("url - path + query", "STRIPPED",
         "",
         "BUK/DS38 keyword extraction: product + search paths are\nhigh-intent tokens; page-type (checkout vs content)",
         "Klickly 100, Sovrn 92, Justuno 91,\nCybba 79, Predactiv 75, 33Across 68 (% w/ path)"),
        ("user_agent", "unused",
         "",
         "Bot filtering BEFORE credit (33Across: 6.4% Googlebot IPs\n+ 5.7% bot UAs get paid today); device/OS features",
         "33Across, Sovrn, 33A API + internal (~100%)"),
        ("query_parameters", "empty",
         "",
         "Search terms / SKUs / UTM = highest-intent BUK input;\nvendor ask (Klickly checkout params would be gold)",
         "nobody today - a vendor ask"),
        ("advertiser_id", "internal",
         "guid_log path: advertiser-pixel features",
         "External vendors cannot supply it", "guid_log only (100%)"),
    ]
    cells = [[f, u, t if t else b, p] for f, u, t, b, p in rows]
    cols = ["Field", "Status", "MM today / latent benefit", "Populated by"]

    fig = plt.figure(figsize=(12.6, 4.6))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.87, bottom=0.04)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="left")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8.5)
    tbl.scale(1, 2.35)
    widths = [0.135, 0.06, 0.46, 0.275]
    used_flags = [r[1] for r in rows]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", ha="left")
            cell.set_facecolor(NAVY)
        else:
            flag = used_flags[r - 1]
            cell.set_facecolor("white" if flag == "used" else "#fdf6ee")
            if c == 0:
                cell.set_text_props(fontweight="bold")
            if c == 1:
                cell.set_text_props(color=GREEN if flag == "used" else AMBER, fontweight="bold")
            if c == 2 and flag not in ("used", "internal"):
                cell.set_text_props(color="#7a5a1e")
    ax.set_title("svs Columns: What MM Consumes Today vs What the Rest Could Unlock",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q1e_column_value.png")


# ---- Step 2: window reach — ranked raw counts ----
def q2(rdir):
    reach = {}
    with open(os.path.join(rdir, "q2_window_reach.csv")) as f:
        for r in csv.DictReader(f):
            reach[int(r["data_source_id"])] = r
    rows_day = {}
    with open(os.path.join(rdir, "q1_scale_by_day.csv")) as f:
        for r in csv.DictReader(f):
            d = int(r["data_source_id"])
            rows_day[d] = rows_day.get(d, 0) + int(r["n_rows"])
    days = 30

    key = lambda d: -int(reach[d]["ips_30d"])
    ext = sorted((d for d in reach if d not in (23, 30)), key=key)
    order = ext + sorted((d for d in (23, 30) if d in reach), key=key)
    cells = [[SHORT[d], fmtn(rows_day.get(d, 0) / days), fmtn(reach[d]["ips_30d"]),
              fmtn(reach[d]["domains_30d"]), fmtn(reach[d]["ip_domain_pairs_30d"])]
             for d in order]
    cols = ["Source", "Avg rows/day (total)", "Unique IPs (30d)", "Unique domains (30d)", "Unique IP x domain pairs (30d)"]

    fig = plt.figure(figsize=(8.6, 3.4))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.82, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9.5)
    tbl.scale(1, 1.5)
    widths = [0.15, 0.16, 0.14, 0.16, 0.23]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white" if r <= len(ext) else "#f4f5f7")
            if c == 0:
                cell.set_text_props(ha="left")
            if c == 2:
                cell.set_text_props(fontweight="bold")
            if r > len(ext):
                cell.set_text_props(color="#888")
    ax.set_title("Raw Reach Over the 30-Day Targeting Window, Ranked by Distinct IPs",
                 fontsize=13, fontweight="bold", loc="left", pad=14)
    save(fig, "q2_window_reach.png")


# ---- Step 2b: rows/IPs dropped per day ----
def q2b(rdir):
    data = {}
    with open(os.path.join(rdir, "q2b_daily_drops.csv")) as f:
        for r in csv.DictReader(f):
            data[int(r["ds"])] = r
    key = lambda d: -int(data[d]["rows_day"])
    ext = sorted((d for d in data if d not in (23, 30)), key=key)
    order = ext + sorted((d for d in (23, 30) if d in data), key=key)

    cells, hard, soft = [], [], []
    for d in order:
        r = data[d]
        bot = int(r["rows_bot_ua"])
        cells.append([SHORT[d], fmtn(r["rows_day"]), fmtn(r["rows_hard_dropped"]),
                      f"{float(r['pct_hard_dropped']):.2f}%", fmtn(r["ips_day"]),
                      fmtn(r["ips_hard_dropped"]),
                      f"{fmtn(r['rows_blocked_ds13'])}  ({float(r['pct_blocked_ds13']):.0f}%)"
                      if int(r["rows_blocked_ds13"]) > 0 else "0",
                      fmtn(bot) if bot else "0"])
        hard.append(float(r["pct_hard_dropped"]))
        soft.append((float(r["pct_blocked_ds13"]), bot / int(r["rows_day"]) * 100))
    cols = ["Source", "Rows/day", "Hard-dropped", "% hard", "Unique IPs/day",
            "IPs dropped", "DS13-blocked rows", "Bot-UA rows"]

    fig = plt.figure(figsize=(10.6, 3.4))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.82, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.5)
    widths = [0.115, 0.10, 0.105, 0.085, 0.095, 0.105, 0.155, 0.10]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", fontsize=8.5)
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white" if r <= len(ext) else "#f4f5f7")
            if c == 0:
                cell.set_text_props(ha="left")
            if c in (2, 3) and hard[r - 1] >= 3:
                cell.set_text_props(color=RED if hard[r - 1] >= 25 else AMBER, fontweight="bold")
            if c == 6 and soft[r - 1][0] >= 10:
                cell.set_text_props(color=AMBER, fontweight="bold")
            if c == 7 and soft[r - 1][1] >= 3:
                cell.set_text_props(color=AMBER, fontweight="bold")
            if r > len(ext):
                cell.set_text_props(color="#888")
    ax.set_title("Rows and IPs Dropped Per Day by Consumer Filters, Full Day Jul 1 2026",
                 fontsize=13, fontweight="bold", loc="left", pad=14)
    save(fig, "q2b_daily_drops.png")


# ---- Step 2c: the survival funnel pivot — raw feed to DS13/DS19 to billed ----
def q2c(rdir):
    fun = {}
    with open(os.path.join(rdir, "q2c_funnel.csv")) as f:
        for r in csv.DictReader(f):
            fun[int(r["ds"])] = r
    billed = {}
    with open(os.path.join(rdir, "q1d_billed_usage.csv")) as f:
        for r in csv.DictReader(f):
            billed[int(r["ds"])] = r
    rows30 = {}
    with open(os.path.join(rdir, "q1_scale_by_day.csv")) as f:
        for r in csv.DictReader(f):
            d = int(r["data_source_id"])
            rows30[d] = rows30.get(d, 0) + int(r["n_rows"])

    ext = sorted((d for d in fun if d not in (23, 30)), key=lambda d: -int(fun[d]["rows_raw"]))

    def cp(d, key, base_key):
        v, b = int(fun[d][key]), int(fun[d][base_key])
        return f"{fmtn(v)}\n{100 * v / b:.1f}%"

    STAGES = [
        ("Raw rows/day", lambda d: fmtn(fun[d]["rows_raw"])),
        ("Kept: url parses,\nnot empty/infra", lambda d: cp(d, "rows_kept", "rows_raw")),
        ("DS13 input\n(after blocklist)", lambda d: cp(d, "rows_ds13_input", "rows_raw")),
        ("DS13 classified\n(domain in wcv)", lambda d: cp(d, "rows_ds13_class", "rows_raw")),
        ("DS19 categorized\n(url in product cat)", lambda d: cp(d, "rows_ds19_cat", "rows_raw")),
        ("USED: DS13 or DS19\n(creditable)", lambda d: cp(d, "rows_used", "rows_raw")),
        ("Unique IPs/day", lambda d: fmtn(fun[d]["ips_raw"])),
        ("IPs on used rows", lambda d: cp(d, "ips_used", "ips_raw")),
        ("Unique domains/day", lambda d: fmtn(fun[d]["domains_raw"])),
        ("Domains classified", lambda d: cp(d, "domains_classified", "domains_raw")),
        ("Billed imps (June,\nserve grain)", lambda d:
            f"{fmtn(float(billed[d]['billed_imps']))}\n{100 * float(billed[d]['billed_imps']) / rows30[d]:.2f}% of 30d rows"
            if d in billed else "flat fee\n-"),
        ("June bill / run rate", lambda d:
            f"{money(float(billed[d]['billed_usd']))}\n{money(float(billed[d]['billed_usd']) * 12)}/yr"
            if d in billed else "not metered\nrenewal sched."),
    ]
    cells = [[label] + [fn(d) for d in ext] for label, fn in STAGES]
    cols = ["Funnel stage"] + [SHORT[d] for d in ext]

    fig = plt.figure(figsize=(13.0, 7.2))
    fig.subplots_adjust(left=0.02, right=0.98, top=0.90, bottom=0.03)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8.8)
    tbl.scale(1, 2.6)
    widths = [0.155] + [0.1056] * len(ext)
    USED_ROW, BILL_ROWS = 6, (11, 12)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white")
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white")
            if c == 0:
                cell.set_text_props(ha="left", fontweight="bold", color="#444", fontsize=8.2)
            if r == USED_ROW:
                cell.set_facecolor(HILITE)
                if c > 0:
                    cell.set_text_props(fontweight="bold", color=NAVY)
            if r in BILL_ROWS:
                cell.set_facecolor("#f4f5f7")
                if c > 0 and r == BILL_ROWS[1]:
                    cell.set_text_props(fontweight="bold")
    ax.set_title("The Survival Funnel: What Each Source Delivers, What MM Can Use, What We Pay",
                 fontsize=13.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q2c_funnel.png")


# ---- Step 2d: share of the usable pool by source (chart-only view over q2c) ----
def q2d(rdir):
    fun = {}
    with open(os.path.join(rdir, "q2c_funnel.csv")) as f:
        for r in csv.DictReader(f):
            fun[int(r["ds"])] = r
    tot = {k: sum(int(fun[d][k]) for d in fun)
           for k in ("rows_used", "ips_used", "domains_classified")}
    order = sorted(fun, key=lambda d: -int(fun[d]["rows_used"]))

    def share(v, t):
        p = 100 * v / t
        return f"{p:.2f}%" if p < 0.1 else f"{p:.1f}%"

    cells = []
    for d in order:
        r = fun[d]
        cells.append([SHORT[d],
                      fmtn(r["rows_used"]), share(int(r["rows_used"]), tot["rows_used"]),
                      fmtn(r["ips_used"]), share(int(r["ips_used"]), tot["ips_used"]),
                      fmtn(r["domains_classified"]), share(int(r["domains_classified"]), tot["domains_classified"])])
    cells.append(["TOTAL usable", fmtn(tot["rows_used"]), "100%",
                  fmtn(tot["ips_used"]), "100%", fmtn(tot["domains_classified"]), "100%"])
    cols = ["Source", "Used rows/day", "% of usable", "Used IPs/day", "% of usable",
            "Classified domains/day", "% of usable"]

    fig = plt.figure(figsize=(9.8, 3.7))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.83, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.5)
    widths = [0.13, 0.115, 0.095, 0.105, 0.095, 0.155, 0.095]
    n = len(order)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", fontsize=8.5)
            cell.set_facecolor(NAVY)
        elif r == n + 1:
            cell.set_facecolor(HILITE)
            cell.set_text_props(fontweight="bold", color=NAVY)
            if c == 0:
                cell.set_text_props(ha="left", fontweight="bold", color=NAVY)
        else:
            internal = order[r - 1] in (23, 30)
            cell.set_facecolor("#f4f5f7" if internal else "white")
            if c == 0:
                cell.set_text_props(ha="left")
            if c in (2, 4, 6):
                cell.set_text_props(fontweight="bold", color="#888" if internal else NAVY)
            if internal and c not in (2, 4, 6):
                cell.set_text_props(color="#888")
    ax.set_title("Who Supplies the Usable Pool: Source Share of Used Rows, IPs, Classified Domains",
                 fontsize=12.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q2d_usable_share.png")


# ---- Step 5: score-tier quality of delivered IPs (sole vs touched) ----
def q5(rdir):
    data = {}
    with open(os.path.join(rdir, "q5_score_tiers.csv")) as f:
        for r in csv.DictReader(f):
            data[(int(r["data_source_id"]), r["cohort"])] = r
    dss = sorted({d for d, _ in data})
    ext = sorted((d for d in dss if d not in (23, 30)),
                 key=lambda d: -float(data[(d, "touched")]["pct_of_delivered_high"]))
    order = ext + sorted((d for d in dss if d in (23, 30)),
                         key=lambda d: -float(data[(d, "touched")]["pct_of_delivered_high"]))

    cells = []
    for d in order:
        t, s = data[(d, "touched")], data[(d, "sole")]
        cells.append([SHORT[d], fmtn(t["vendor_ips"]),
                      f"{fmtn(t['delivered_ips'])}  ({t['pct_delivered']}%)",
                      f"{t['pct_hi']}%", f"{t['pct_pp']}%", f"{t['pct_high_grad']}%",
                      f"{t['pct_mid']}%", f"{t['pct_maxreach']}%", f"{t['pct_unscored_delivered']}%",
                      f"{t['pct_of_delivered_high']}%",
                      f"{fmtn(s['delivered_ips'])}  ({s['pct_delivered']}%)",
                      f"{s['pct_of_delivered_high']}%"])
    cols = ["Source", "IPs touched", "Delivered (pct)", "HI 10000", "PP 8000", "High grad",
            "Mid", "Max reach", "Unscored", "HIGH total", "Sole delivered (pct)", "Sole HIGH"]

    fig = plt.figure(figsize=(12.8, 3.4))
    fig.subplots_adjust(left=0.02, right=0.98, top=0.82, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8.6)
    tbl.scale(1, 1.5)
    widths = [0.09, 0.085, 0.135, 0.065, 0.065, 0.072, 0.055, 0.072, 0.07, 0.075, 0.135, 0.068]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", fontsize=8)
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white" if r <= len(ext) else "#f4f5f7")
            if c == 0:
                cell.set_text_props(ha="left")
            if c == 3:
                cell.set_text_props(fontweight="bold", color=GREEN)
            if c == 9:
                cell.set_text_props(fontweight="bold", color=NAVY)
            if c == 11:
                v = float(cells[r - 1][11].rstrip("%"))
                cell.set_text_props(color=RED if v < 10 else "#444", fontweight="bold")
            if r > len(ext):
                cell.set_text_props(color="#888")
    ax.set_title("Score Quality of Delivered IPs: Tier Mix for Touched vs Sole, Valuation Week Jul 2-8",
                 fontsize=12.5, fontweight="bold", loc="left", pad=14)
    save(fig, "q5_score_tiers.png")


# ---- Step 9 (v1): per-vendor scorecard — usable, money, quality, worth, verdict, asks ----
# Synthesis of q2c (usable), q5/q6 (score + money, valuation week), q1d (bill), and the
# AUDI-1089 eval verdicts/fee bands (root summary section 4). q3 usable-uniqueness refresh
# and flat-fee amounts (renewal schedule / Maya Triman) slot in when available.
def q9(rdir):
    fun, billed, vt = {}, {}, {}
    with open(os.path.join(rdir, "q2c_funnel.csv")) as f:
        for r in csv.DictReader(f):
            fun[int(r["ds"])] = r
    with open(os.path.join(rdir, "q1d_billed_usage.csv")) as f:
        for r in csv.DictReader(f):
            billed[int(r["ds"])] = r
    with open(os.path.join(rdir, "q6_value_tiers.csv")) as f:
        for r in csv.DictReader(f):
            vt[int(r["data_source_id"])] = r
    st = {}
    with open(os.path.join(rdir, "q5_score_tiers.csv")) as f:
        for r in csv.DictReader(f):
            st[(int(r["data_source_id"]), r["cohort"])] = r

    META = {  # worth $/mo band, verdict, key ask — from eval section 4 + q1b-q2c findings
        28: ("$2.5-8.3K", "NEGOTIATE\ncap or drop", "strip webmail/bot rows;\njustify vs our Magnite feed"),
        40: ("$0.8-3.3K", "DROP /\nrenegotiate", "remove cookie-sync urls;\nsend real page paths"),
        33: ("$40-200", "DROP", "fix doubled-protocol\nurls (77% of feed)"),
        24: ("$1.2-5K", "KEEP-trim", "grow scale; add\nuser_agent"),
        36: ("$90-390", "DROP", "fix truncated urls;\nscale too small"),
        26: ("$58-250K", "KEEP\n(lock price)", "restore dropped metadata;\nkeep HEM feed"),
        25: ("see TI-1027", "KEEP", "add url paths + UA\n(domain-only today)"),
        39: ("$10-125", "DROP unless\n~free", "populate query_params\n(checkout); non-Shopify"),
    }
    order = [28, 40, 33, 24, 36, 26, 25, 39]

    cells = []
    for d in order:
        u = 100 * int(fun[d]["rows_used"]) / int(fun[d]["rows_raw"])
        t = st[(d, "touched")]
        b = billed.get(d)
        bill = f"{money(float(b['billed_usd']))}\n{money(float(b['billed_usd']) * 12)}/yr" if b \
            else "flat fee\nrenewal sched."
        w, verdict, ask = META[d]
        cells.append([SHORT[d], f"{u:.0f}%", fmtn(st[(d, 'sole')]['vendor_ips']),
                      money(float(vt[d]["media_touched"])), money(float(vt[d]["media_sole"])),
                      f"{t['pct_of_delivered_high']}%", bill, w, verdict, ask])
    cols = ["Source", "Usable", "Sole IPs", "Media $/wk\ntouched", "Media $/wk\nsole",
            "HIGH score\n(of delivered)", "Jun bill /\nrun rate", "Worth $/mo", "Verdict", "Key ask"]

    fig = plt.figure(figsize=(13.2, 4.6))
    fig.subplots_adjust(left=0.02, right=0.98, top=0.86, bottom=0.04)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8.4)
    tbl.scale(1, 2.35)
    widths = [0.085, 0.055, 0.075, 0.085, 0.08, 0.095, 0.095, 0.085, 0.10, 0.20]
    VCOL = {"KEEP": GREEN, "KEEP-trim": GREEN, "KEEP\n(lock price)": GREEN,
            "NEGOTIATE\ncap or drop": AMBER}
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", fontsize=8)
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white")
            if c == 0:
                cell.set_text_props(ha="left", fontweight="bold")
            if c == 8:
                v = cells[r - 1][8]
                cell.set_text_props(fontweight="bold", color=VCOL.get(v, RED))
            if c == 9:
                cell.set_text_props(ha="left", fontsize=7.8, color="#555")
    ax.set_title("Vendor Scorecard v1: Usable Share, Money, Score Quality, Worth, Verdict",
                 fontsize=13, fontweight="bold", loc="left", pad=14)
    save(fig, "q9_vendor_scorecard.png")


# ---- Step 9b: composite quality score + ranking (weights per runbook) ----
def q9b(rdir):
    import math
    def by_ds(f):
        return {int(r["data_source_id"]): r for r in csv.DictReader(open(os.path.join(rdir, f)))}
    dom, rec = by_ds("q4_domain_value.csv"), by_ds("q3_pair_recency.csv")
    val, vr = by_ds("q6_value_tiers.csv"), by_ds("q7_sole_vr.csv")
    tier = {}
    with open(os.path.join(rdir, "q5_score_tiers.csv")) as f:
        for r in csv.DictReader(f):
            tier.setdefault(int(r["data_source_id"]), {})[r["cohort"]] = r
    billed = by_ds_safe = {}
    with open(os.path.join(rdir, "q1d_billed_usage.csv")) as f:
        for r in csv.DictReader(f):
            billed[int(r["ds"])] = r
    EXT = [24, 25, 26, 28, 33, 36, 39, 40]
    NO_SVS_BASELINE_VR = 0.0223
    max_sc = max(float(dom[d]["sole_classified"]) for d in EXT)
    max_t1 = max(float(val[d]["imps_sole_scored_nonrtc"]) for d in EXT)
    comp = {}
    for d in EXT:
        V = math.log10(float(dom[d]["sole_classified"]) + 1) / math.log10(max_sc + 1)
        R = (float(rec[d]["pct_sole"]) + float(rec[d]["pct_freshest"])) / 100
        Q = 0.5 * float(dom[d]["pct_classified"]) / 100 \
            + 0.5 * (1 - float(tier[d]["sole"]["pct_unscored_delivered"]) / 100)
        D = math.log10(float(val[d]["imps_sole_scored_nonrtc"]) + 1) / math.log10(max_t1 + 1)
        P = min(float(vr[d]["vr_overall_pct"]) / NO_SVS_BASELINE_VR, 2) / 2 \
            if float(vr[d]["sole_imps"]) >= 5000 else 0.5
        comp[d] = (V, R, Q, D, P, 100 * (0.40 * V + 0.15 * R + 0.15 * Q + 0.10 * D + 0.20 * P))
    order = sorted(EXT, key=lambda d: -comp[d][5])

    WTP = {25: ("$150K-600K", 600e3), 26: ("$0.7M-3M", 3e6), 28: ("$30K-100K", 100e3),
           24: ("$14K-60K", 60e3), 39: ("$0.1K-1.5K", 1.5e3), 40: ("$10K-40K", 40e3),
           36: ("$1.1K-4.7K", 4.7e3), 33: ("$0.5K-2.4K", 2.4e3)}
    cells, billcol = [], []
    for i, d in enumerate(order):
        V, R, Q, D, P, S = comp[d]
        b = billed.get(d)
        wtp_lbl, wtp_top = WTP[d]
        if b:
            ann = float(b["billed_usd"]) * 12
            bill = money(ann) + "/yr"
            billcol.append(GREEN if ann <= wtp_top else (AMBER if ann <= 3 * wtp_top else RED))
        else:
            bill = "flat fee"
            billcol.append("#888")
        cells.append([f"#{i + 1}", SHORT[d], f"{V:.2f}", f"{R:.2f}", f"{Q:.2f}",
                      f"{D:.2f}", f"{P:.2f}", f"{S:.1f}", wtp_lbl, bill])
    cols = ["Rank", "Source", "V unique\nvalue (40%)", "R non-\nredund (15%)", "Q signal\nqual (15%)",
            "D depend-\nency (10%)", "P perform-\nance (20%)", "SCORE\n(0-100)",
            "WTP $/yr\n(pay up to)", "Bill\nrun rate"]

    fig = plt.figure(figsize=(10.8, 3.5))
    fig.subplots_adjust(left=0.03, right=0.97, top=0.82, bottom=0.05)
    ax = fig.add_subplot(111)
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=cols, loc="center", cellLoc="right")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)
    widths = [0.05, 0.105, 0.088, 0.088, 0.088, 0.088, 0.088, 0.082, 0.115, 0.10]
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#e2e2e2")
        cell.set_width(widths[c])
        if r == 0:
            cell.set_text_props(fontweight="bold", color="white", fontsize=7.8)
            cell.set_facecolor(NAVY)
        else:
            cell.set_facecolor("white")
            s = float(cells[r - 1][7])
            if c == 1:
                cell.set_text_props(ha="left", fontweight="bold")
            if c == 7:
                cell.set_facecolor(HILITE)
                cell.set_text_props(fontweight="bold",
                                    color=GREEN if s >= 60 else (AMBER if s >= 45 else RED))
            if c == 8:
                cell.set_text_props(fontweight="bold", color=NAVY)
            if c == 9:
                cell.set_text_props(fontweight="bold", color=billcol[r - 1])
    ax.set_title("Composite Quality Score: Components and Ranking (score = data quality; verdict = score x cost)",
                 fontsize=12, fontweight="bold", loc="left", pad=14)
    save(fig, "q9b_quality_ranking.png")


STEPS = {"0": q0, "1": q1, "1b": q1b, "1c": q1c, "1d": q1d, "1e": q1e,
         "2": q2, "2b": q2b, "2c": q2c, "2d": q2d, "5": q5, "9": q9, "9b": q9b}

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
