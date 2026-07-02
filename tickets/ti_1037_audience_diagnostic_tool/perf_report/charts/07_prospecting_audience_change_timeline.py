"""Module 07 render — audience-expression change timeline (data-source presence over time).

For one prospecting campaign, a Gantt of WHEN each data_source_id was present in the audience
expression, plus vertical markers where the audience_id was swapped. Shows the audience mutating
under a fixed campaign_id — e.g. DS19 (keyword MM) present across both periods, DS13 (vertical)
only added later, audience_id 22666→31114, etc. Defaults to the campaign with the most changes.

Reads  outputs/<adv>/07_prospecting_audience_change_history.csv
Writes outputs/<adv>/07_prospecting_audience_change_timeline.png
"""
import argparse
import csv
from datetime import date, datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
NAVY, GREEN, MAROON, AMBER, GRAY, ORANGE, PURPLE, BLUE, RED = \
    "#27496D", "#2E8B57", "#8C3B3B", "#C77B30", "#9AA0A6", "#D98C4A", "#5B4B8A", "#3A6EA5", "#D63B2F"

# ds_id -> (label, role-color, order). MM core (13/19) first.
DS = {
    19: ("DS19 · Keyword (MM)", GREEN, 0), 13: ("DS13 · Vertical (MM)", GREEN, 1),
    35: ("DS35 · LiveRamp 3P", BLUE, 2), 11: ("DS11 · LiveRamp legacy", BLUE, 3),
    46: ("DS46 · Fangorn", PURPLE, 4), 16: ("DS16 · Funnel tags", GRAY, 5),
    14: ("DS14 · Availability gate", AMBER, 6),
    2: ("DS2 · MNTN 1P (excl)", MAROON, 7), 4: ("DS4 · CRM (excl)", MAROON, 8),
    9: ("DS9 · 1P (excl)", MAROON, 9), 47: ("DS47 · CRM idgraph (excl)", MAROON, 10),
    21: ("DS21 · Retgt PV (excl)", ORANGE, 11), 34: ("DS34 · Retgt Conv (excl)", ORANGE, 12),
    1: ("DS1 · (excl)", MAROON, 13),
}


def d(s):
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/07_prospecting_audience_change_history.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/07_prospecting_audience_change_timeline.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--campaign", type=int, default=0)   # 0 = auto (most changes)
    ap.add_argument("--end", default="2026-06-01")
    ap.add_argument("--p1", nargs=2, default=["2025-01-01", "2025-06-01"])
    ap.add_argument("--p2", nargs=2, default=["2026-01-01", "2026-06-01"])
    a = ap.parse_args()

    rows = list(csv.DictReader(open(a.csv)))
    by_camp = {}
    for r in rows:
        by_camp.setdefault(int(r["campaign_id"]), []).append(r)
    target = a.campaign or max(by_camp, key=lambda c: len(by_camp[c]))
    evs = sorted(by_camp[target], key=lambda r: r["changed_on"])
    grp = evs[0]["campaign_group_id"]

    end = d(a.end)
    # forward-fill: build (start,end) presence intervals per ds; track audience_id switches
    points = [(d(e["changed_on"]), set(int(x) for x in e["ds_ids"].split(",") if x), int(e["audience_id"]))
              for e in evs]
    ds_intervals = {ds: [] for ds in DS}
    aud_changes = []
    prev_aud = None
    for i, (dt, dsset, aud) in enumerate(points):
        nxt = points[i + 1][0] if i + 1 < len(points) else end
        for ds in dsset:
            if ds in ds_intervals:
                ds_intervals[ds].append((dt, nxt))
        if aud != prev_aud:
            aud_changes.append((dt, aud))
            prev_aud = aud

    present = sorted([ds for ds in DS if ds_intervals[ds]], key=lambda ds: DS[ds][2])
    n = len(present)
    x0 = min(p[0] for p in points)
    fig, ax = plt.subplots(figsize=(14, 1.4 + 0.5 * n))

    # period bands
    for s, e, lab, col in [(a.p1[0], a.p1[1], "P1", NAVY), (a.p2[0], a.p2[1], "P2", NAVY)]:
        ax.axvspan(mdates.date2num(d(s)), mdates.date2num(d(e)), color=col, alpha=0.06, zorder=0)
        ax.text(mdates.date2num(d(s) + (d(e) - d(s)) / 2), n - 0.3, lab, ha="center", fontsize=8, color=NAVY, alpha=0.8)

    for yi, ds in enumerate(present):
        y = n - 1 - yi
        label, color, _ = DS[ds]
        ax.text(mdates.date2num(x0) - 8, y, label, ha="right", va="center", fontsize=8.5, color="#333")
        for s, e in ds_intervals[ds]:
            ax.barh(y, mdates.date2num(e) - mdates.date2num(s), left=mdates.date2num(s),
                    height=0.6, color=color, alpha=0.9, zorder=3)

    # audience_id change markers
    for dt, aud in aud_changes:
        ax.axvline(mdates.date2num(dt), color=RED, ls="--", lw=1.2, zorder=4)
        ax.text(mdates.date2num(dt) + 4, n - 0.75, f"audience_id -> {aud}", rotation=90,
                va="top", ha="left", fontsize=7.5, color=RED)

    ax.set_ylim(-0.6, n - 0.05)
    ax.set_xlim(mdates.date2num(x0) - 30, mdates.date2num(end) + 5)
    ax.set_yticks([])
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n'%y"))
    ax.tick_params(axis="x", labelsize=8)
    for sp in ["top", "right", "left"]:
        ax.spines[sp].set_visible(False)
    ax.set_title(f"{a.adv} — Audience change timeline · campaign {target} (group {grp})",
                 fontsize=13.5, fontweight="bold", loc="left", color="#222", pad=12)
    plt.tight_layout()
    plt.savefig(a.out, dpi=190, bbox_inches="tight")
    print(f"wrote {a.out}")
    mm = "DS19 present entire span" if 19 in present else "no DS19"
    print(f"FINDING: campaign {target} audience changed {len(points)}x under one campaign_id; "
          f"audience_id swaps: {[a for _,a in aud_changes]}. MM: {mm}; "
          f"DS13 intervals={len(ds_intervals[13])}, DS35 intervals={len(ds_intervals[35])}.")


if __name__ == "__main__":
    main()
