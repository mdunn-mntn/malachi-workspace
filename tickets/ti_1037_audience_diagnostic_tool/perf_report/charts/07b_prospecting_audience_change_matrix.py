"""Module 07b render — audience change-log MATRIX (campaigns × change-dates).

Columns = campaign_group_id, rows = each change date, cells = what changed that day for that campaign:
data sources ADDED (green +) / REMOVED (red −), each tagged incl/excl/gate, plus audience_id swaps.
DS-level only (the sources you care about); segment/category detail is kept out to a separate file.
Shared dates across columns reveal platform-wide vs campaign-specific changes. No re-query — DS role
(incl/excl/gate) comes from the known classification; reads the module-07 history CSV.

Reads  outputs/<adv>/07_prospecting_audience_change_history.csv
Writes outputs/<adv>/07b_prospecting_audience_change_matrix.png
       outputs/<adv>/07b_prospecting_audience_change_matrix.md
"""
import argparse
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
GREEN, RED, NAVY, GRAY = "#1E7A46", "#C0392B", "#27496D", "#888888"

# DS -> role (incl / excl / gate / funnel). The sources that matter; stable per DS.
ROLE = {19: "incl", 13: "incl", 35: "incl", 11: "incl", 46: "incl", 38: "incl", 16: "funnel",
        2: "excl", 4: "excl", 9: "excl", 47: "excl", 1: "excl", 14: "gate", 21: "excl", 34: "excl"}


def role(ds):
    return ROLE.get(ds, "?")


def dsfmt(dss):
    return ", ".join(f"DS{ds} {role(ds)}" for ds in sorted(dss))


def dsfmt_grouped(dss):   # compact for the verbose initial set: "incl 19,35 · excl 2,4 · gate 14"
    by = {}
    for ds in sorted(dss):
        by.setdefault(role(ds), []).append(str(ds))
    return " · ".join(f"{r} {','.join(by[r])}" for r in ["incl", "excl", "gate", "funnel", "?"] if r in by)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/07_prospecting_audience_change_history.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/07b_prospecting_audience_change_matrix.png")
    ap.add_argument("--md",  default="outputs/kindred_35094/07b_prospecting_audience_change_matrix.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()

    rows = list(csv.DictReader(open(a.csv)))
    # per campaign_group: ordered change events
    by_grp = {}
    gname = {}
    for r in rows:
        g = r["campaign_group_id"]
        by_grp.setdefault(g, []).append(r)
        gname[g] = r["camp_name"]
    cols = sorted(by_grp, key=lambda g: min(e["changed_on"] for e in by_grp[g]))

    # cell[(date, grp)] = list of (text, color); collect all change dates
    cell = {}
    dates = set()
    for g in cols:
        evs = sorted(by_grp[g], key=lambda e: e["changed_on"])
        prev_ds, prev_aud = None, None
        for e in evs:
            dt = e["changed_on"]
            dates.add(dt)
            cur = set(int(x) for x in e["ds_ids"].split(",") if x)
            aud = int(e["audience_id"])
            lines = []
            if prev_ds is None:
                lines.append((f"start: {dsfmt_grouped(cur)}", GRAY))
            else:
                if aud != prev_aud:
                    lines.append((f"audience_id -> {aud}", NAVY))
                added, removed = cur - prev_ds, prev_ds - cur
                if added:
                    lines.append((f"+ {dsfmt(added)}", GREEN))
                if removed:
                    lines.append((f"− {dsfmt(removed)}", RED))
                if not added and not removed and aud == prev_aud:
                    lines.append(("(minor)", GRAY))
            cell[(dt, g)] = lines
            prev_ds, prev_aud = cur, aud

    dates = sorted(dates)
    # row heights = max lines across the row (min 1)
    rheight = [max(1, max((len(cell.get((dt, g), [])) for g in cols), default=1)) for dt in dates]
    total_lines = sum(rheight)
    fig, ax = plt.subplots(figsize=(3.0 + 2.15 * len(cols), 1.6 + 0.34 * total_lines))
    ax.axis("off")
    ncol = len(cols)
    colx = [0.0] + [1.0 + i for i in range(ncol)]   # date col width 1.0, each grp width 1.0 (units)
    xmax = 1.0 + ncol

    # header
    ytop = total_lines + 1.0
    ax.text(0.05, ytop - 0.4, "date", fontsize=10, fontweight="bold", color=NAVY)
    for i, g in enumerate(cols):
        nm = (gname[g] or "").replace("Beeswax Television Prospecting", "").strip()
        ax.text(colx[i + 1] + 0.5, ytop - 0.4, g, ha="center", fontsize=10, fontweight="bold", color=NAVY)
    ax.plot([0, xmax], [total_lines + 0.55, total_lines + 0.55], color=NAVY, lw=1.3)

    # rows
    y = total_lines
    for ri, dt in enumerate(dates):
        h = rheight[ri]
        if ri % 2 == 0:
            ax.axhspan(y - h, y, xmin=0, xmax=1, color="#000", alpha=0.03, zorder=0)
        ax.text(0.05, y - 0.5, dt, fontsize=8.5, va="center", color="#333")
        for i, g in enumerate(cols):
            lines = cell.get((dt, g), [])
            for li, (txt, col) in enumerate(lines):
                ax.text(colx[i + 1] + 0.04, y - 0.5 - li, txt, fontsize=7.6, va="center", color=col,
                        fontweight="bold" if col in (GREEN, RED, NAVY) else "normal")
        y -= h
    ax.set_xlim(0, xmax)
    ax.set_ylim(0, ytop + 0.2)
    ax.set_title(f"{a.adv} — Prospecting audience change-log (DS-level)  ·  green = added, red = removed",
                 fontsize=13, fontweight="bold", loc="left", color="#222", pad=12)
    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    # ---- markdown ----
    md = [f"# {a.adv} — Prospecting audience change-log (DS-level)",
          "Columns = campaign_group_id. `+` added, `−` removed. incl/excl/gate = the DS role. "
          "Segment (category_id) detail intentionally omitted.", "",
          "| date | " + " | ".join(cols) + " |", "|" + "---|" * (len(cols) + 1)]
    for dt in dates:
        cells = []
        for g in cols:
            txt = " <br> ".join(t for t, _ in cell.get((dt, g), []))
            cells.append(txt or "")
        md.append(f"| {dt} | " + " | ".join(cells) + " |")
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")


if __name__ == "__main__":
    main()
