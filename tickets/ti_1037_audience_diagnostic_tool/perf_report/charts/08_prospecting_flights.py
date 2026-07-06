"""Module 08 render — scheduled-flight timeline (from core_flights), 3 length-tiers.

One band per campaign_group split into 3 fixed tiers by flight length — short (<=3d) on top,
4-7d middle, 8+ long on bottom — so all short flights line up on one row and the timeline is
de-crowded. Dormant gaps (no active flight in any tier) are grayed. P1/P2 bands shaded; the
short-flight count is broken out P1 vs P2.

Reads  outputs/<adv>/08_prospecting_flights.csv   (one row per flight)
Writes outputs/<adv>/08_prospecting_flights.png
"""
import argparse
import csv
from datetime import datetime, timedelta
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
NAVY, AMBER, RED, GRAY = "#27496D", "#C77B30", "#D63B2F", "#C8CCD0"
SUBH = 0.42
# tier index by length: 0=short(top) 1=medium 2=long(bottom); color per tier
TIER = [RED, AMBER, NAVY]


def tier(days):
    return 0 if days <= 3 else (1 if days <= 7 else 2)


def d(s):
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def prospecting_spend_by_group(csv_path):
    """Per-campaign_group prospecting spend from the sibling 00_campaign_enum.csv (obj=1 rows)."""
    import os
    enum_path = os.path.join(os.path.dirname(csv_path), "00_campaign_enum.csv")
    out = {}
    if os.path.exists(enum_path):
        for r in csv.DictReader(open(enum_path)):
            if r.get("obj") == "1":
                out[r["grp"]] = out.get(r["grp"], 0) + float(r["spend"] or 0)
    return out


def pctfmt(share):
    return "<1%" if 0 < share < 0.01 else f"{share*100:.0f}%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/08_prospecting_flights.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/08_prospecting_flights.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--win-start", default="2025-01-01")
    ap.add_argument("--win-end", default="2026-06-01")
    ap.add_argument("--p1", nargs=2, default=["2025-01-01", "2025-06-01"])
    ap.add_argument("--p2", nargs=2, default=["2026-01-01", "2026-06-01"])
    a = ap.parse_args()

    ws, we = d(a.win_start), d(a.win_end)
    p1s, p1e = d(a.p1[0]), d(a.p1[1])
    p2s, p2e = d(a.p2[0]), d(a.p2[1])
    rows = list(csv.DictReader(open(a.csv)))
    groups = {}
    for r in rows:
        g = r["campaign_group_id"]
        groups.setdefault(g, {"name": r["group_name"], "fl": []})
        groups[g]["fl"].append((d(r["flight_start"]), d(r["flight_end"]), int(r["flight_days"])))
    pspend = prospecting_spend_by_group(a.csv)  # rank groups by % of prospecting spend (most -> least)
    tot_ps = sum(pspend.get(g, 0) for g in groups) or 1
    for g in groups:
        groups[g]["sh"] = pspend.get(g, 0) / tot_ps
    order = sorted(groups, key=lambda g: -groups[g]["sh"])
    order = [g for g in order if groups[g]["sh"] > 0] or order  # omit zero-spend groups (guard)

    GH = 3 * SUBH          # group height (3 tiers)
    fig, ax = plt.subplots(figsize=(14.5, 1.6 + len(order) * (GH + 0.34) * 1.05))
    for s, e in (a.p1, a.p2):
        ax.axvspan(mdates.date2num(d(s)), mdates.date2num(d(e)), color=NAVY, alpha=0.05, zorder=0)

    y = 0.0
    layout = []
    for g in reversed(order):     # bottom-up so first group is on top
        layout.append((g, y))
        y += GH + 0.34
    ytop = y

    for g, ybase in layout:
        fl = sorted(groups[g]["fl"])
        # dormant gaps: merge intervals, gray any gap within the active span
        merged = []
        for s, e, dd in fl:
            if merged and s <= merged[-1][1] + timedelta(days=1):
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e])
        for i in range(len(merged) - 1):
            gs, ge = max(ws, merged[i][1]), min(we, merged[i + 1][0])
            if mdates.date2num(ge) > mdates.date2num(gs):
                ax.barh(ybase + GH / 2, mdates.date2num(ge) - mdates.date2num(gs),
                        left=mdates.date2num(gs), height=GH, color=GRAY, alpha=0.5, zorder=1)
        # flights in length-tiers
        for s, e, dd in fl:
            bs, be = max(s, ws), min(e, we)
            if mdates.date2num(be) <= mdates.date2num(bs):
                continue
            t = tier(dd)
            yy = ybase + (2 - t) * SUBH        # short(0) on top, long(2) at bottom
            ax.barh(yy + SUBH / 2, mdates.date2num(be) - mdates.date2num(bs), left=mdates.date2num(bs),
                    height=SUBH * 0.82, color=TIER[t], edgecolor="white", linewidth=0.4, zorder=3)
        n = len(fl)
        p1_short = sum(1 for s, e, dd in fl if p1s <= s < p1e and dd <= 3)
        p2_short = sum(1 for s, e, dd in fl if p2s <= s < p2e and dd <= 3)
        nm = (groups[g]["name"] or "").replace("CTV Prospecting", "").strip()
        ax.text(mdates.date2num(ws) - 14, ybase + GH / 2, f"{g} · {pctfmt(groups[g]['sh'])}\n{nm[:14]}",
                ha="right", va="center", fontsize=9, color="#333")
        ax.text(mdates.date2num(we) + 4, ybase + GH / 2,
                f"{n} flights\nshort  P1:{p1_short}  P2:{p2_short}", ha="left", va="center",
                fontsize=8, color="#666")

    ax.set_ylim(-0.2, ytop)
    ax.set_xlim(mdates.date2num(ws) - 20, mdates.date2num(we) + 62)
    ax.set_yticks([])
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n'%y"))
    ax.tick_params(axis="x", labelsize=8)
    for sp in ["top", "right", "left"]:
        ax.spines[sp].set_visible(False)
    leg = [Patch(fc=RED, label="short (≤3d, auto-ungates)"), Patch(fc=AMBER, label="4–7 days"),
           Patch(fc=NAVY, label="8+ days"), Patch(fc=GRAY, label="dormant (no active flight)")]
    ax.legend(handles=leg, frameon=False, ncol=4, fontsize=9, loc="lower center",
              bbox_to_anchor=(0.5, -0.11))
    ax.set_title(f"{a.adv} — Scheduled flights per campaign (short / 4-7d / long tiers)",
                 fontsize=14, fontweight="bold", loc="left", color="#222", pad=12)
    plt.tight_layout(rect=[0, 0.03, 1, 1])
    plt.savefig(a.out, dpi=190, bbox_inches="tight")
    print(f"wrote {a.out}")
    tot = sum(len(v["fl"]) for v in groups.values())
    p1 = sum(1 for v in groups.values() for s, e, dd in v["fl"] if p1s <= s < p1e and dd <= 3)
    p2 = sum(1 for v in groups.values() for s, e, dd in v["fl"] if p2s <= s < p2e and dd <= 3)
    print(f"FINDING: {tot} flights; short flights P1={p1} vs P2={p2}. Continuous delivery built from "
          f"many back-to-back manual flights; short (<=3d) flights auto-ungate (HHST=0).")


if __name__ == "__main__":
    main()
