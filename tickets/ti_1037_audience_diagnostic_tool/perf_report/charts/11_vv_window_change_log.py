"""Module 11 render — VV lookback window change log (measurement-change flag).

Step-lines of the PRO (prospecting) and RT (retargeting) verified-visit lookback windows over time,
change points marked, P1/P2 comparison bands shaded. If the window differs between P1 and P2 it's
flagged in red: P1's visits/conversions were measured on a different-length window than P2's, so part
of any P1-vs-P2 gap is a MEASUREMENT change (a shorter VV window shrinks connectable visits AND
conversions — the latter on a ~window-length lag). Also emits a committable change-log markdown table.

Reads  outputs/<adv>/11_vv_window_change_log.csv   (one row per change event)
Writes outputs/<adv>/11_vv_window_change_log.png
       outputs/<adv>/11_vv_window_change_log.md
"""
import argparse
import csv
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
NAVY, AMBER, RED, GRAY = "#27496D", "#C77B30", "#D63B2F", "#9AA0A6"


def d(s):
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def val_at(changes, key, when):
    v = None
    for c in changes:
        if c["date"] <= when:
            v = c[key]
    return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/11_vv_window_change_log.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/11_vv_window_change_log.png")
    ap.add_argument("--md",  default="outputs/kindred_35094/11_vv_window_change_log.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--win-start", default="2025-01-01")
    ap.add_argument("--win-end", default="2026-06-01")
    ap.add_argument("--p1", nargs=2, default=["2025-01-01", "2025-06-01"])
    ap.add_argument("--p2", nargs=2, default=["2026-01-01", "2026-06-01"])
    a = ap.parse_args()

    rows = list(csv.DictReader(open(a.csv)))
    changes = [{"date": d(r["change_date"]), "pro": int(r["pro_window"]), "rt": int(r["rt_window"]),
                "conv": int(r["conversion_window"])} for r in rows]
    ws, we = d(a.win_start), d(a.win_end)
    p1m = d(a.p1[0]) + (d(a.p1[1]) - d(a.p1[0])) / 2
    p2m = d(a.p2[0]) + (d(a.p2[1]) - d(a.p2[0])) / 2

    # adaptive y-scale — headroom above the largest window across PRO/RT/conv (handles
    # advertisers with windows > the Kindred 45d ceiling); floor at 52 to keep Kindred output identical.
    ymax = max([52] + [max(c["pro"], c["rt"], c["conv"]) for c in changes]) + 5
    y_flag = ymax - 5          # change-marker label baseline (was 47 for the 52 ceiling)
    y_period = ymax * 2 / 52   # P1/P2 label baseline (was 2 for the 52 ceiling)

    fig, ax = plt.subplots(figsize=(13, 5.2))
    for s, e in (a.p1, a.p2):
        ax.axvspan(mdates.date2num(d(s)), mdates.date2num(d(e)), color=NAVY, alpha=0.055, zorder=0)

    # step lines: forward-fill each window over [ws, we]
    for key, color, lab in [("pro", NAVY, "PRO window (prospecting VV)"), ("rt", AMBER, "RT window (retargeting VV)")]:
        pts = [(ws, val_at(changes, key, ws))]
        for c in changes:
            if ws < c["date"] <= we:
                pts.append((c["date"], c[key]))
        pts.append((we, pts[-1][1]))
        xs = [mdates.date2num(p[0]) for p in pts]
        ys = [p[1] for p in pts]
        ax.step(xs, ys, where="post", color=color, lw=2.6, marker="o", ms=5, label=lab, zorder=3)

    # change markers + labels
    for c in changes:
        if ws < c["date"] <= we:
            ax.axvline(mdates.date2num(c["date"]), color=RED, ls="--", lw=1.3, zorder=2)
            ax.text(mdates.date2num(c["date"]) + 5, y_flag, f"{c['date']}\nPRO->{c['pro']}d  RT->{c['rt']}d",
                    fontsize=8.5, color=RED, va="top", fontweight="bold")

    # P1/P2 flag — PRO window at each period's start & end (P2 may straddle a change)
    p1s, p1e_ = val_at(changes, "pro", d(a.p1[0])), val_at(changes, "pro", d(a.p1[1]))
    p2s, p2e_ = val_at(changes, "pro", d(a.p2[0])), val_at(changes, "pro", d(a.p2[1]))
    p1lbl = f"{p1s}d" if p1s == p1e_ else f"{p1s}-{p1e_}d"
    p2lbl = f"{p2s}d" if p2s == p2e_ else f"{p2s}-{p2e_}d"
    ax.text(mdates.date2num(p1m), y_period, f"P1: PRO {p1lbl}", ha="center", fontsize=9, color=NAVY, fontweight="bold")
    ax.text(mdates.date2num(p2m), y_period, f"P2: PRO {p2lbl}", ha="center", fontsize=9, color=NAVY, fontweight="bold")
    if p1e_ != p2e_ or p1s != p1e_ or p2s != p2e_:
        ax.text(0.5, 1.04, f"FLAG: prospecting VV window shortened  P1 {p1lbl}  ->  P2 {p2lbl}  (progressive) — P1 "
                f"visits/conversions were measured on a LONGER window; each shortening mechanically reduces measured "
                f"visits & conversions, so part of the P1-vs-P2 gap is measurement.", transform=ax.transAxes,
                ha="center", fontsize=9, color=RED, fontweight="bold")

    ax.set_ylim(0, ymax)
    ax.set_xlim(mdates.date2num(ws) - 10, mdates.date2num(we) + 10)
    ax.set_ylabel("lookback window (days)", fontsize=10, color="#555")
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n'%y"))
    ax.tick_params(axis="x", labelsize=8)
    for sp in ["top", "right"]:
        ax.spines[sp].set_visible(False)
    ax.legend(frameon=False, fontsize=9.5, loc="center left")
    ax.set_title(f"{a.adv} — Verified-Visit lookback window over time", fontsize=14,
                 fontweight="bold", loc="left", color="#222", pad=26)
    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    # ---- markdown change log ----
    md = [f"# {a.adv} — VV lookback window change log",
          "Source: `archives_advertiser_archives` (PRO=`clickpass_acquisition_ttl`, RT=`clickpass_click_ttl`, "
          "conv=`conversion_window`). A shorter VV window shrinks connectable visits AND conversions "
          "(conversions ride on verified-visit impressions) — check any performance change against these.", "",
          "| change_date | PRO window | RT window | conversion window | change |", "|---|---:|---:|---:|---|"]
    for i, r in enumerate(rows):
        chg = "initial" if not r["prev_pro"] else \
            f"PRO {r['prev_pro']}→{r['pro_window']}, RT {r['prev_rt']}→{r['rt_window']}"
        md.append(f"| {r['change_date']} | {r['pro_window']}d | {r['rt_window']}d | {r['conversion_window']}d | {chg} |")
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")

    # ---- data-derived finding (no hardcoded advertiser specifics) ----
    pro_traj = "->".join(str(c["pro"]) for c in changes) if changes else "n/a"
    conv_vals = {c["conv"] for c in changes}
    conv_note = (f"Conversion window constant {next(iter(conv_vals))}d."
                 if len(conv_vals) == 1 else
                 f"Conversion window varies ({'->'.join(str(c['conv']) for c in changes)}d).")
    # does P2 straddle a change (window differs at P2 start vs end)?
    straddle = " P2 straddles a mid-period change." if p2s != p2e_ else ""
    if p1lbl == p2lbl and p1s == p1e_ and p2s == p2e_:
        confound = ("No measurement confound: prospecting VV window identical across P1 and P2 — "
                    "any P1-vs-P2 visits/conv/CVR gap is NOT a window artifact.")
    else:
        confound = (f"Measurement confound on the P1-vs-P2 visits/conv/CVR gap: prospecting VV window "
                    f"P1={p1lbl} -> P2={p2lbl}.{straddle}")
    print(f"FINDING: {len(changes)} VV-window change events. Prospecting VV window trajectory {pro_traj}d. "
          f"{confound} {conv_note}")


if __name__ == "__main__":
    main()
