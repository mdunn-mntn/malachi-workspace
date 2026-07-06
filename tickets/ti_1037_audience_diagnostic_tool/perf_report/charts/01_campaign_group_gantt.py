"""Module 01 chart — Campaign-group Gantt.

One horizontal bar per campaign_group_id = the calendar span it delivered
(first -> last active day). Bars ordered by start date (earliest on top).
Prospecting vs Retargeting colored distinctly; spend + active-day count direct-labeled.
Left '<' marker = active at the window's left edge (likely started earlier / clipped);
right '> live' marker = still active at the window's right edge.

Reads  outputs/<adv>/01_campaign_group_gantt.csv
Writes outputs/<adv>/01_campaign_group_gantt.png
Also prints a one-line FINDING: for the eventual assembled report.
"""
import argparse
from datetime import datetime, date
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager

# ---- MNTN / Tufte house style ------------------------------------------------
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
NAVY, AMBER, GRAY, RED = "#27496D", "#C77B30", "#9AA0A6", "#D63B2F"


def d(s):  # "YYYY-MM-DD" -> date
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def role_color(name):
    n = (name or "").lower()
    if "retarget" in n:
        return AMBER, "Retargeting"
    return NAVY, "Prospecting"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/01_campaign_group_gantt.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/01_campaign_group_gantt.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--win-start", default="2025-01-01")
    ap.add_argument("--win-end", default="2026-06-01")   # exclusive
    ap.add_argument("--p1", nargs=2, default=["2025-01-01", "2025-06-01"])
    ap.add_argument("--p2", nargs=2, default=["2026-01-01", "2026-06-01"])
    a = ap.parse_args()

    # ---- load ----
    import csv
    rows = []
    with open(a.csv) as f:
        for r in csv.DictReader(f):
            if not r.get("campaign_group_id"):
                continue
            rows.append(r)
    # omit groups with no spend in the period (guard: never empty), then highest spend on top
    rows = [r for r in rows if float(r["total_spend"] or 0) > 0] or rows
    rows.sort(key=lambda r: (-float(r["total_spend"] or 0), r["first_active_day"]))

    ws, we = d(a.win_start), d(a.win_end)
    p1s, p1e = d(a.p1[0]), d(a.p1[1])
    p2s, p2e = d(a.p2[0]), d(a.p2[1])
    n = len(rows)

    fig, ax = plt.subplots(figsize=(13, 1.7 + 0.62 * n))

    # ---- comparison-period bands (ties the Gantt to the two YoY windows) ----
    for (s, e, lab) in [(p1s, p1e, "P1  Jan–May '25"), (p2s, p2e, "P2  Jan–May '26")]:
        ax.axvspan(mdates.date2num(s), mdates.date2num(e), color=NAVY, alpha=0.055, zorder=0)
        ax.text(mdates.date2num(s + (e - s) / 2), n - 0.32, lab, ha="center", va="bottom",
                fontsize=8, color=NAVY, alpha=0.8)

    # ---- faint quarter guides ----
    q = date(ws.year, 1, 1)
    while q < we:
        if ws <= q <= we:
            ax.axvline(mdates.date2num(q), color=GRAY, alpha=0.18, lw=0.7, zorder=0)
        m = q.month + 3
        q = date(q.year + (m - 1) // 12, (m - 1) % 12 + 1, 1)

    ylabels, seen_roles = [], {}
    for i, r in enumerate(rows):
        y = n - 1 - i  # invert: first row at top
        s = max(d(r["first_active_day"]), ws)
        e = min(d(r["last_active_day"]), we)
        color, role = role_color(r["group_name"])
        seen_roles[role] = color
        x0, w = mdates.date2num(s), mdates.date2num(e) - mdates.date2num(s)
        ax.barh(y, w, left=x0, height=0.56, color=color, alpha=0.9, zorder=3)

        # edge markers (sit in the padded margins so they're not clipped)
        if d(r["first_active_day"]) <= ws:  # active at/at-before window start -> may predate it
            ax.text(mdates.date2num(ws) - 4, y, "<", ha="right", va="center",
                    fontsize=12, color=RED, fontweight="bold", zorder=4)
        # "live" = still active at (or within a day of) the window's right edge
        is_live = (we - d(r["last_active_day"])).days <= 1
        if is_live:
            ax.text(mdates.date2num(we) + 4, y, ">", ha="left", va="center",
                    fontsize=12, color="#2E8B57", fontweight="bold", zorder=4)

        # direct label: spend + active days, placed after the bar end (or inside if near right edge)
        spend = float(r["total_spend"] or 0)
        lab = f"${spend/1000:.0f}k · {r['active_days']}d"
        label_x = mdates.date2num(e) + 5
        if mdates.date2num(e) > mdates.date2num(we) - 95:  # near right edge -> put inside bar
            ax.text(mdates.date2num(e) - 5, y, lab, ha="right", va="center", fontsize=7.5,
                    color="white", zorder=5, fontweight="bold")
        else:
            ax.text(label_x, y, lab, ha="left", va="center", fontsize=7.5, color="#555", zorder=4)

        gid = r["campaign_group_id"]
        nm = r["group_name"] or ""
        nm = (nm[:34] + "…") if len(nm) > 35 else nm
        ylabels.append(f"{gid}  {nm}")

    ax.set_yticks(range(n))
    ax.set_yticklabels(list(reversed(ylabels)), fontsize=8)
    ax.set_ylim(-0.7, n - 0.05)
    ax.set_xlim(mdates.date2num(ws) - 18, mdates.date2num(we) + 18)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%y"))
    ax.tick_params(axis="x", labelsize=7.5)
    for s in ["top", "right", "left"]:
        ax.spines[s].set_visible(False)
    ax.tick_params(axis="y", length=0)

    ax.set_title(f"{a.adv} — Campaign-Group Running Spans", fontsize=14,
                 fontweight="bold", loc="left", color="#222", pad=10)

    # legend (direct, minimal)
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in seen_roles.values()]
    ax.legend(handles, list(seen_roles.keys()), loc="lower right", frameon=False,
              fontsize=8.5, ncol=len(seen_roles))

    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    # ---- one-line finding for the report ----
    live_n = sum(1 for r in rows if (we - d(r["last_active_day"])).days <= 1)
    y2026 = sum(1 for r in rows if d(r["first_active_day"]) >= date(2026, 1, 1))
    top = max(rows, key=lambda r: float(r["total_spend"] or 0))
    print(f"FINDING: {n} campaign-groups; {live_n} still live at window end; {y2026} launched in 2026. "
          f"Flagship {top['campaign_group_id']} '{top['group_name']}' = ${float(top['total_spend'])/1000:.0f}k "
          f"({top['active_days']} active days).")


if __name__ == "__main__":
    main()
