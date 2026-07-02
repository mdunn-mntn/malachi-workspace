"""Module 08 render — scheduled-flight timeline (from core_flights).

One band per campaign_group; every scheduled flight drawn as a bar, colored by length
(short <=3d = red, 4-7d = amber, >7d = navy) so you can see how short/fragmented the flights are.
Flights are packed into non-overlapping sub-lanes per group; the group's active span backdrop is
gray so dormant gaps read as "not running". P1/P2 comparison bands shaded.

Reads  outputs/<adv>/08_prospecting_flights.csv   (one row per flight)
Writes outputs/<adv>/08_prospecting_flights.png
"""
import argparse
import csv
from datetime import datetime, date
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


def d(s):
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def color(days):
    return RED if days <= 3 else (AMBER if days <= 7 else NAVY)


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
    rows = list(csv.DictReader(open(a.csv)))
    groups = {}
    for r in rows:
        g = r["campaign_group_id"]
        groups.setdefault(g, {"name": r["group_name"], "fl": []})
        groups[g]["fl"].append((d(r["flight_start"]), d(r["flight_end"]), int(r["flight_days"])))
    order = sorted(groups, key=lambda g: min(f[0] for f in groups[g]["fl"]))

    # greedy pack each group's flights into non-overlapping sub-lanes; lay out top→bottom
    SUBH = 0.44
    layout = []          # (group, sublane_assignments, n_sub, y_base)
    y = 0.0
    for g in reversed(order):     # build bottom-up so first group ends on top
        fl = sorted(groups[g]["fl"])
        lane_end = []
        assign = []
        for s, e, dd in fl:
            for li, le in enumerate(lane_end):
                if s >= le:      # touching (start == prior end) counts as sequential, not overlap
                    lane_end[li] = e
                    assign.append((s, e, dd, li))
                    break
            else:
                assign.append((s, e, dd, len(lane_end)))
                lane_end.append(e)
        n_sub = max(1, len(lane_end))
        layout.append((g, assign, n_sub, y))
        y += n_sub * SUBH + 0.32
    ytop = y

    fig, ax = plt.subplots(figsize=(14.5, 1.4 + ytop * 0.9))
    for s, e in (a.p1, a.p2):
        ax.axvspan(mdates.date2num(d(s)), mdates.date2num(d(e)), color=NAVY, alpha=0.05, zorder=0)

    for g, assign, n_sub, ybase in layout:
        span_lo = max(ws, min(f[0] for f in assign))
        span_hi = min(we, max(f[1] for f in assign))
        # dormant backdrop across the group's active span
        ax.barh(ybase + n_sub * SUBH / 2, mdates.date2num(span_hi) - mdates.date2num(span_lo),
                left=mdates.date2num(span_lo), height=n_sub * SUBH, color=GRAY, alpha=0.35, zorder=1)
        n_short = sum(1 for s, e, dd, li in assign if dd <= 3)
        for s, e, dd, li in assign:
            bs, be = max(s, ws), min(e, we)
            if mdates.date2num(be) <= mdates.date2num(bs):
                continue
            yy = ybase + li * SUBH
            ax.barh(yy + SUBH / 2, mdates.date2num(be) - mdates.date2num(bs), left=mdates.date2num(bs),
                    height=SUBH * 0.82, color=color(dd), edgecolor="white", linewidth=0.4, zorder=3)
        nm = (groups[g]["name"] or "").replace("CTV Prospecting", "").strip()
        ax.text(mdates.date2num(ws) - 14, ybase + n_sub * SUBH / 2, f"{g}\n{nm[:14]}",
                ha="right", va="center", fontsize=9, color="#333")
        ax.text(mdates.date2num(we) + 4, ybase + n_sub * SUBH / 2,
                f"{len(assign)} flights\n{n_short} short", ha="left", va="center", fontsize=8, color="#666")

    ax.set_ylim(-0.2, ytop)
    ax.set_xlim(mdates.date2num(ws) - 20, mdates.date2num(we) + 60)
    ax.set_yticks([])
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n'%y"))
    ax.tick_params(axis="x", labelsize=8)
    for sp in ["top", "right", "left"]:
        ax.spines[sp].set_visible(False)
    leg = [Patch(fc=RED, label="short flight (≤3d, auto-ungates)"),
           Patch(fc=AMBER, label="4–7 days"), Patch(fc=NAVY, label="8+ days"),
           Patch(fc=GRAY, label="dormant (no active flight)")]
    ax.legend(handles=leg, frameon=False, ncol=4, fontsize=9, loc="lower center",
              bbox_to_anchor=(0.5, -0.13))
    ax.set_title(f"{a.adv} — Scheduled flights per campaign (Start-End, core_flights)",
                 fontsize=14, fontweight="bold", loc="left", color="#222", pad=12)
    plt.tight_layout(rect=[0, 0.03, 1, 1])
    plt.savefig(a.out, dpi=190, bbox_inches="tight")
    print(f"wrote {a.out}")
    tot = sum(len(v["fl"]) for v in groups.values())
    short = sum(1 for v in groups.values() for f in v["fl"] if f[2] <= 3)
    print(f"FINDING: {tot} scheduled flights across {len(groups)} groups in-window; {short} are <=3d "
          f"(auto-ungate). Delivery is continuous but composed of many short back-to-back flights "
          f"(High Pop the most). Flights set manually per launch (Tofer) — coverage may be partial pre-2025.")


if __name__ == "__main__":
    main()
