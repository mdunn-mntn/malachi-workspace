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
import os
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


_ORDER = ["incl", "excl", "gate", "funnel", "?"]


def dsg(dss):             # delta form, same-role grouped: "DS21,DS34 excl" / "DS19 incl"
    by = {}
    for ds in sorted(dss):
        by.setdefault(role(ds), []).append(f"DS{ds}")
    return " · ".join(f"{','.join(by[r])} {r}" for r in _ORDER if r in by)


def dsfmt_grouped(dss):   # compact for the verbose initial set: "incl 19,35 · excl 2,4 · gate 14"
    by = {}
    for ds in sorted(dss):
        by.setdefault(role(ds), []).append(str(ds))
    return " · ".join(f"{r} {','.join(by[r])}" for r in _ORDER if r in by)


def prospecting_spend_by_group(outdir):
    """Per-group prospecting spend (share of total) from the shared campaign enum.
    Group prospecting spend = SUM of spend over that group's obj==1 rows; share = grp/total.
    Groups absent from the enum get 0 (share 0 -> sort last)."""
    path = os.path.join(outdir, "00_campaign_enum.csv")
    spend = {}
    if os.path.exists(path):
        for r in csv.DictReader(open(path)):
            if int(r.get("obj") or 0) == 1:
                spend[r["grp"]] = spend.get(r["grp"], 0.0) + float(r.get("spend") or 0)
    total = sum(spend.values()) or 1.0
    return spend, {g: s / total for g, s in spend.items()}


def pctfmt(share):
    return "<1%" if 0 < share < 0.01 else f"{share*100:.0f}%"

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
    # % of PROSPECTING SPEND per group (from the shared enum) — orders columns by materiality.
    _, gshare = prospecting_spend_by_group(os.path.dirname(a.csv))
    # ordered by prospecting-spend share DESC (biggest spender first); earliest-change tie-break.
    cols = sorted(by_grp, key=lambda g: (-gshare.get(g, 0.0), min(e["changed_on"] for e in by_grp[g])))
    cols = [g for g in cols if gshare.get(g, 0.0) > 0] or cols  # omit zero-spend groups (guard)

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
                added, removed = cur - prev_ds, prev_ds - cur
                if aud != prev_aud:
                    tag = "" if (added or removed) else "  (same DS)"
                    lines.append((f"audience_id -> {aud}{tag}", NAVY))
                if added:
                    lines.append((f"+ {dsg(added)}", GREEN))
                if removed:
                    lines.append((f"− {dsg(removed)}", RED))
            # accumulate (a campaign can have >1 version on the same date — never overwrite)
            cell.setdefault((dt, g), []).extend(lines)
            prev_ds, prev_aud = cur, aud

    # group names from the shared enum (obj=1 rows) for readable section headers
    enum_path = os.path.join(os.path.dirname(a.csv), "00_campaign_enum.csv")
    gname_enum = {}
    if os.path.exists(enum_path):
        for r in csv.DictReader(open(enum_path)):
            if int(r.get("obj") or 0) == 1 and r.get("group_name"):
                gname_enum[r["grp"]] = r["group_name"]

    def shortname(g):
        s = (gname_enum.get(g) or gname.get(g) or "").replace("CTV Prospecting", "").replace("CTV", "")
        s = s.replace("Prospecting", "").replace("Frequency", "Freq").replace("Subscriptions", "Subs")
        return " ".join(s.split()).strip()

    # ---- flatten to a tall CHANGELOG TABLE: campaign sections (spend-ranked) -> chronological rows ----
    group_events = {}
    for (dt, g), lines in cell.items():
        group_events.setdefault(g, []).append((dt, lines))
    for g in group_events:
        group_events[g].sort()

    render = []  # ("hdr", grp, share, name) | ("row", date, txt, color) | ("gap",)
    for g in cols:
        render.append(("hdr", g, gshare.get(g, 0.0), shortname(g)))
        for dt, lines in group_events.get(g, []):
            for i, (txt, col) in enumerate(lines):
                render.append(("row", dt if i == 0 else "", txt, col))
        render.append(("gap",))
    n = len(render)

    fig, ax = plt.subplots(figsize=(12, 1.4 + 0.31 * n))
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(-1, n + 2)
    ax.text(0.0, n + 1.3, f"{a.adv} — prospecting audience change-log (DS-level)", fontsize=15,
            fontweight="bold", color="#222")
    ax.text(0.0, n + 0.5, "By campaign, most spend first.  green = added, red = removed.  incl / excl / gate / funnel = the DS role.",
            fontsize=11, color="#555")
    xDATE, xCHG = 0.185, 0.31
    for i, item in enumerate(render):
        y = n - i
        if item[0] == "hdr":
            _, g, sh, gn = item
            ax.axhspan(y - 0.5, y + 0.5, color=NAVY, alpha=0.08, zorder=0)
            ax.text(0.008, y, f"{g}  ·  {gn[:34]}" if gn else f"{g}", fontsize=12, fontweight="bold",
                    color=NAVY, va="center")
            ax.text(0.992, y, f"{pctfmt(sh)} spend", fontsize=11, fontweight="bold",
                    color=NAVY, va="center", ha="right")
        elif item[0] == "row":
            _, dt, txt, col = item
            ax.text(xDATE, y, dt, fontsize=10.5, va="center", color="#666")
            ax.text(xCHG, y, txt, fontsize=10.5, va="center", color=col,
                    fontweight="bold" if col in (GREEN, RED, NAVY) else "normal")
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    # ---- markdown (linear changelog) ----
    md = [f"# {a.adv} — Prospecting audience change-log (DS-level)",
          "By campaign (most spend first). `+` added, `−` removed. incl/excl/gate/funnel = the DS role. "
          "Segment (category_id) detail intentionally omitted.", "",
          "| Campaign (group) | % spend | Date | Change |", "|---|--:|---|---|"]
    for g in cols:
        sh = gshare.get(g, 0.0)
        spend_s = pctfmt(sh)
        evs = group_events.get(g, [])
        first_row = True
        for dt, lines in evs:
            for i, (txt, _) in enumerate(lines):
                camp = f"{g} {shortname(g)}" if first_row else ""
                sp = spend_s if first_row else ""
                md.append(f"| {camp} | {sp} | {dt if i == 0 else ''} | {txt} |")
                first_row = False
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")


if __name__ == "__main__":
    main()
