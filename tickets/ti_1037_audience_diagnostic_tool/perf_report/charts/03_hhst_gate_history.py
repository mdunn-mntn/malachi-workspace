"""Module 03 chart — HHST gate trajectory (prospecting campaigns).

One small-multiple panel per prospecting campaign: a step-line of the HHST threshold over time
(forward-filled from the change events, clipped to the window). Green band = HI zone (>=8000);
red band = NO gate (<=0). Reveals gate thrash, holiday gate-off, and graduated auto-pacing.

Reads  outputs/<adv>/03_hhst_gate_history.csv   (one row per gate-change event)
Writes outputs/<adv>/03_hhst_gate_history.png
Prints a one-line FINDING: for the assembled report.
"""
import argparse
import csv
import os
from datetime import datetime, date
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
NAVY, GREEN, RED, GRAY = "#27496D", "#2E8B57", "#D63B2F", "#9AA0A6"


def ts(s):
    return datetime.strptime(s.strip()[:19], "%Y-%m-%d %H:%M:%S")


def gate_label(thr):
    if thr <= 0:
        return "no gate"
    if thr >= 10000:
        return "HI-only"
    if thr == 6666:
        return "HI+PP"
    return str(thr)


def prospecting_spend_by_group(outdir):
    """Read <outdir>/00_campaign_enum.csv and return {campaign_group_id: prospecting spend}.
    Prospecting = obj==1; a group's prospecting spend = SUM of spend over its obj==1 rows.
    Advertiser-agnostic; a group absent here gets 0 (sorts last)."""
    path = os.path.join(outdir, "00_campaign_enum.csv")
    spend = {}
    if not os.path.exists(path):
        return spend
    csv.field_size_limit(10 ** 7)
    for r in csv.DictReader(open(path)):
        if str(r.get("obj")) != "1":
            continue
        g = r.get("grp")
        try:
            s = float(r.get("spend") or 0)
        except ValueError:
            s = 0.0
        spend[g] = spend.get(g, 0.0) + s
    return spend


def pctfmt(share):
    return "<1%" if 0 < share < 0.01 else f"{share*100:.0f}%"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/03_hhst_gate_history.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/03_hhst_gate_history.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--win-start", default="2025-01-01")
    ap.add_argument("--win-end", default="2026-06-01")
    ap.add_argument("--p1", nargs=2, default=["2025-01-01", "2025-06-01"])
    ap.add_argument("--p2", nargs=2, default=["2026-01-01", "2026-06-01"])
    a = ap.parse_args()

    rows = list(csv.DictReader(open(a.csv)))
    # group change events by campaign_group_id
    groups = {}
    for r in rows:
        g = r["campaign_group_id"]
        groups.setdefault(g, {"name": r["group_name"], "ev": []})
        groups[g]["ev"].append((ts(r["update_time"]), int(r["threshold"])))

    # prospecting-spend share per group (advertiser-agnostic; from 00_campaign_enum.csv next to --csv).
    # Total = sum over the groups this chart actually shows; groups absent from the enum -> spend 0 -> share 0.
    pspend = prospecting_spend_by_group(os.path.dirname(a.csv))
    for g in groups:
        groups[g]["spend"] = pspend.get(g, 0.0)
    tot_ps = sum(groups[g]["spend"] for g in groups) or 1.0
    for g in groups:
        groups[g]["sshare"] = groups[g]["spend"] / tot_ps
    # panels ordered by prospecting-spend share DESC (biggest spender on top); tie-break: earliest change.
    order = sorted(groups, key=lambda g: (-groups[g]["sshare"], min(t for t, _ in groups[g]["ev"])))
    order = [g for g in order if groups[g]["sshare"] > 0] or order  # omit zero-spend campaigns (guard)
    n = len(order)

    ws = mdates.date2num(datetime.strptime(a.win_start, "%Y-%m-%d"))
    we = mdates.date2num(datetime.strptime(a.win_end, "%Y-%m-%d"))
    p1 = [mdates.date2num(datetime.strptime(x, "%Y-%m-%d")) for x in a.p1]
    p2 = [mdates.date2num(datetime.strptime(x, "%Y-%m-%d")) for x in a.p2]

    fig, axes = plt.subplots(n, 1, sharex=True, figsize=(13, 1.15 * n + 1.1))
    if n == 1:
        axes = [axes]

    for i, g in enumerate(order):
        ax = axes[i]
        ev = sorted(groups[g]["ev"])
        xs = [mdates.date2num(t) for t, _ in ev] + [we]
        ys = [v for _, v in ev] + [ev[-1][1]]

        ax.axhspan(8000, 10600, color=GREEN, alpha=0.10, zorder=0)   # HI zone
        ax.axhspan(-600, 0, color=RED, alpha=0.09, zorder=0)          # no-gate zone
        ax.axhline(6666, color=GRAY, ls=":", lw=0.7, alpha=0.6, zorder=1)
        for s, e in (p1, p2):
            ax.axvspan(s, e, color=NAVY, alpha=0.05, zorder=0)
        ax.step(xs, ys, where="post", color=NAVY, lw=1.5, zorder=3)

        ax.set_ylim(-600, 10800)
        ax.set_yticks([0, 10000])
        ax.set_yticklabels(["0", "10k"], fontsize=7)
        ax.set_xlim(ws, we)
        for sp in ["top", "right"]:
            ax.spines[sp].set_visible(False)
        # label + spend share + change-count in the LEFT MARGIN (no lines there → nothing gets cut off)
        nm = (groups[g]["name"] or "").replace("CTV Prospecting", "").strip()
        sp = groups[g]["sshare"]
        sp_txt = f"· {pctfmt(sp)} spend"
        ax.text(-0.013, 0.66, f"{g} {sp_txt}\n{nm[:14]}", transform=ax.transAxes, ha="right",
                va="center", fontsize=8, color="#333")
        ax.text(-0.013, 0.24, f"{len(ev)} chg · last {gate_label(ev[-1][1])}",
                transform=ax.transAxes, ha="right", va="center", fontsize=6.8, color="#999")
        if i == 0:  # band legend on the top panel only (no gray subtitle)
            ax.text(ws + 6, 9200, "HI zone (≥ 8000)", fontsize=7, color=GREEN, va="center")
            ax.text(ws + 6, -300, "no gate (≤ 0)", fontsize=7, color=RED, va="center")

    axes[-1].xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%b\n%y"))
    axes[-1].tick_params(axis="x", labelsize=7.5)
    axes[0].set_title(f"{a.adv} — HHST Gate Trajectory (prospecting campaigns)",
                      fontsize=14, fontweight="bold", loc="left", color="#222", pad=10)

    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    top = max(order, key=lambda g: len(groups[g]["ev"]))
    tot = sum(len(v["ev"]) for v in groups.values())
    print(f"FINDING: {n} prospecting campaigns, {tot} total HHST changes. Gate is auto-paced (graduated "
          f"thresholds, not on/off). Flagship {top} thrashed {len(groups[top]['ev'])}x; last gate values: "
          + "; ".join(f"{g}={gate_label(groups[g]['ev'][-1][1])}" for g in order) + ".")


if __name__ == "__main__":
    main()
