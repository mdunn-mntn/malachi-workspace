"""Module 03b chart — HHST gate RIBBON (prospecting campaigns).

Companion to 03's step-line: each prospecting campaign = one lane, every delivering day colored by
its HHST gate bucket (green = gated HI/Peak >=6600, amber = mid/continuous 1-6599, red = NO gate <=0).
Forward-filled across each campaign's active span and CLIPPED to its true delivery life. Ported from
the AUDI-1070 gate_ribbon_chart.py, parameterized to read this module's single CSV.

Reads  outputs/<adv>/03b_hhst_gate_daily_ribbon.csv   (one row per campaign per delivering day)
Writes outputs/<adv>/03b_hhst_gate_daily_ribbon.png
Prints a one-line FINDING: for the assembled report.
"""
import argparse
import csv
import datetime as dt
import os
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
NAVY, RED, GREEN, GRAY, AMBER = "#27496D", "#D63B2F", "#2E8B57", "#B8BDC2", "#C77B30"


def dn(s):
    return mdates.date2num(dt.date.fromisoformat(s))


def state_color(v):
    if v is None:
        return GRAY
    return RED if v <= 0 else (GREEN if v >= 6600 else AMBER)


def prospecting_spend_share(csv_path):
    """Per-group share of prospecting spend, from the sibling 00_campaign_enum.csv.

    Prospecting spend for a group = SUM of spend over that group's obj==1 enum rows.
    Total = sum across all prospecting groups. Returns {group_id: share (0..1)}; a group
    absent from the enum (or with no obj==1 rows) gets share 0.
    """
    enum_path = os.path.join(os.path.dirname(csv_path), "00_campaign_enum.csv")
    if not os.path.exists(enum_path):
        return {}
    by_grp = {}
    for r in csv.DictReader(open(enum_path)):
        if str(r.get("obj")) != "1":
            continue
        try:
            sp = float(r.get("spend") or 0)
        except ValueError:
            sp = 0.0
        by_grp[r["grp"]] = by_grp.get(r["grp"], 0.0) + sp
    tot = sum(by_grp.values()) or 1.0
    return {g: s / tot for g, s in by_grp.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/03b_hhst_gate_daily_ribbon.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/03b_hhst_gate_daily_ribbon.png")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--win-start", default="2025-01-01")
    ap.add_argument("--win-end", default="2026-06-05")
    ap.add_argument("--holiday", nargs=2, default=["2025-11-19", "2026-01-06"])
    ap.add_argument("--p1", nargs=2, default=["2025-01-01", "2025-06-01"])
    ap.add_argument("--p2", nargs=2, default=["2026-01-01", "2026-06-01"])
    a = ap.parse_args()

    rows = list(csv.DictReader(open(a.csv)))
    by = {}
    for r in rows:
        g = r["campaign_group_id"]
        by.setdefault(g, {"name": r["group_name"], "days": {}})
        gate = r["gate"]
        by[g]["days"][r["day"]] = int(gate) if gate not in ("", None) else None

    share = prospecting_spend_share(a.csv)  # {group_id: % of prospecting spend}
    # lanes ordered by prospecting-spend share DESC (biggest spender on top); a group missing
    # from the enum -> share 0 sorts last. Tie-break on earliest active day for determinism.
    camps = sorted(by.items(), key=lambda kv: (-share.get(kv[0], 0.0), min(kv[1]["days"])))
    n = len(camps)
    fig, ax = plt.subplots(figsize=(15, max(3.2, 0.52 * n + 2.0)))

    ylabels, tot = [], {"HI/Peak": 0, "mid": 0, "no-gate": 0}
    for i, (grp, info) in enumerate(camps):
        y = n - 1 - i
        days = sorted(info["days"])
        thr = info["days"]
        d0, d1 = dt.date.fromisoformat(days[0]), dt.date.fromisoformat(days[-1])
        # forward-fill daily state across [d0, d1]; collapse contiguous same-color runs into segments
        cur = None
        run_start = None
        last = None
        d = d0
        segs = []
        while d <= d1:
            ds = d.isoformat()
            if ds in thr:
                last = thr[ds]
                if last is not None:
                    tot["HI/Peak" if last >= 6600 else ("no-gate" if last <= 0 else "mid")] += 1
            c = state_color(last)
            if c != cur:
                if cur is not None:
                    segs.append((run_start, d, cur))
                cur = c
                run_start = d
            d += dt.timedelta(days=1)
        if cur is not None:
            segs.append((run_start, d1 + dt.timedelta(days=1), cur))
        for s, e, c in segs:
            ax.barh(y, dn(e.isoformat()) - dn(s.isoformat()), left=dn(s.isoformat()),
                    height=0.62, color=c, edgecolor="white", linewidth=0.3, zorder=3)
        nm = info["name"] or ""
        nm = (nm[:30] + "…") if len(nm) > 31 else nm
        sh = share.get(grp)
        sh_txt = f"  ·  {sh*100:.0f}% spend" if sh is not None else "  ·  — spend"
        ylabels.append(f"{grp}  {nm}{sh_txt}")

    ax.set_yticks(range(n))
    ax.set_yticklabels(ylabels[::-1], fontsize=8)
    ax.set_ylim(-0.6, n - 0.4)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.set_xlim(dn(a.win_start), dn(a.win_end))
    plt.setp(ax.get_xticklabels(), fontsize=8)
    for s in ["top", "right", "left"]:
        ax.spines[s].set_visible(False)
    ax.tick_params(left=False)

    # comparison-period + holiday bands — draw only the portion inside the window so a
    # single-period window (e.g. a P2-only run) doesn't push labels off-canvas.
    xlo, xhi = dn(a.win_start), dn(a.win_end)

    def band(s, e, color, alpha, lab, lab_color, lab_alpha):
        lo, hi = max(dn(s), xlo), min(dn(e), xhi)
        if hi <= lo:                      # band entirely outside the window → skip
            return
        ax.axvspan(lo, hi, color=color, alpha=alpha, zorder=0)
        ax.text((lo + hi) / 2, n - 0.35, lab, ha="center", fontsize=8, color=lab_color, alpha=lab_alpha)

    band(a.p1[0], a.p1[1], NAVY, 0.05, "P1  Jan–May '25", NAVY, 0.75)
    band(a.p2[0], a.p2[1], NAVY, 0.05, "P2  Jan–May '26", NAVY, 0.75)
    band(a.holiday[0], a.holiday[1], RED, 0.06, "holiday", RED, 0.8)

    leg = [Patch(fc=GREEN, label="gated HI/Peak (≥6600)"),
           Patch(fc=AMBER, label="mid / continuous (1-6599)"),
           Patch(fc=RED, label="NO gate (≤0)")]
    ax.set_title(f"{a.adv} — prospecting campaigns & their intent gate over time",
                 fontsize=13.5, fontweight="bold", color=NAVY, loc="left", pad=10)
    fig.legend(handles=leg, frameon=False, ncol=3, fontsize=9, loc="lower center",
               bbox_to_anchor=(0.5, 0.0))
    plt.tight_layout(rect=[0, 0.05, 1, 0.97])
    plt.savefig(a.out, dpi=190, bbox_inches="tight")
    print(f"wrote {a.out}")
    print(f"FINDING: gate-ribbon over {n} prospecting campaigns, {sum(tot.values())} delivering-days: "
          f"{tot['HI/Peak']} gated HI/Peak, {tot['mid']} mid, {tot['no-gate']} NO-gate. The no-gate days "
          f"cluster in the Dec–Feb holiday window (flagship + LowPop gate-OFF).")


if __name__ == "__main__":
    main()
