"""Module 02 chart — Prospecting audience-expression fingerprint (anomaly view).

Parses each prospecting group's v2 audience expression with diag/expr.py, then lays out a
targeting-layer x campaign_group matrix. Every cell that DEVIATES from the flagship (left-most,
earliest-start) column is outlined in red — so config drift across the prospecting fleet pops.

Reads  outputs/<adv>/02_prospecting_audience_expressions.csv   (has the raw `expression` JSON)
Writes outputs/<adv>/02_prospecting_audience_expressions.png
       outputs/<adv>/02_prospecting_audience_decomposition.md   (readable per-layer table)
Prints a one-line FINDING: for the assembled report.
"""
import argparse
import csv
import json
import os
import sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Rectangle

# reuse the TI-1037 audience-expression parser
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "artifacts"))
from diag.expr import parse_expression  # noqa: E402

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA"})
NAVY, MAROON, AMBER, GREEN, PURPLE, RED, GRAY = \
    "#27496D", "#8C3B3B", "#C77B30", "#2E8B57", "#5B4B8A", "#D63B2F", "#9AA0A6"
ROLE_COLOR = {"incl": NAVY, "excl": MAROON, "gate": AMBER, "holdout": GREEN, "rtc": PURPLE}


def ds_count(leaves, ds):
    return sum(l.n for l in leaves if l.data_source_id == ds)


# (label, role, extractor) -> extractor(parsed) returns (display_str, compare_value|None)
LAYERS = [
    ("DS19 · MM keywords (incl)",        "incl",
     lambda p: (str(ds_count(p.includes, 19)), ds_count(p.includes, 19))),
    ("DS35 · LiveRamp 3P (incl)",        "incl",
     lambda p: (str(ds_count(p.includes, 35)), ds_count(p.includes, 35))),
    ("DS16 · CampaignGroupID tag (incl)", "incl",
     lambda p: ((str(ds_count(p.includes, 16)), ds_count(p.includes, 16))
                if ds_count(p.includes, 16) else (None, 0))),
    ("DS16 · Impressions+Wins (excl)",   "excl",
     lambda p: ((str(ds_count(p.excludes, 16)), ds_count(p.excludes, 16))
                if ds_count(p.excludes, 16) else (None, 0))),
    ("DS2 · MNTN 1P (excl)",             "excl",
     lambda p: (str(ds_count(p.excludes, 2)), ds_count(p.excludes, 2))),
    ("DS47 · CRM idgraph (excl)",        "excl",
     lambda p: (str(ds_count(p.excludes, 47)), ds_count(p.excludes, 47))),
    ("DS14 · availability gate",         "gate",
     lambda p: (("on", 1) if p.availability_gate else (None, 0))),
    ("DS21/34 · own-site retgt (excl)",  "excl",
     lambda p: (("180d", 1) if p.retargeting else (None, 0))),
    ("Holdout",                          "holdout",
     lambda p: ((f"{p.holdout.pct:.0f}%", p.holdout.pct) if p.holdout else (None, 0))),
    ("RTC score directive",              "rtc",
     lambda p: (("on", p.score.id) if p.score else (None, 0))),
]


def short(gid, name):
    """Advertiser-agnostic column label: strip boilerplate 'Prospecting'/'CTV' noise,
    then keep the distinguishing tail so near-identical group names stay distinct."""
    import re
    n = (name or "").strip()
    n = re.sub(r"\bCTV\b", "", n)
    n = re.sub(r"\bProspecting\b", "", n, flags=re.I)
    n = re.sub(r"\bProspect\b", "", n, flags=re.I)
    n = re.sub(r"\b20\d\d\b", "", n)          # drop bare year tokens
    n = re.sub(r"\bQ[1-4]-?", "", n)          # drop quarter tokens
    n = re.sub(r"\s+", " ", n).strip(" -")
    if len(n) > 15:                            # keep the distinguishing tail, not the head
        n = "…" + n[-14:]
    return f"{gid}\n{n or '(unnamed)'}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="outputs/kindred_35094/02_prospecting_audience_expressions.csv")
    ap.add_argument("--out", default="outputs/kindred_35094/02_prospecting_audience_expressions.png")
    ap.add_argument("--md",  default="outputs/kindred_35094/02_prospecting_audience_decomposition.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()

    csv.field_size_limit(10 ** 7)
    rows = [r for r in csv.DictReader(open(a.csv)) if r.get("expression")]
    parsed = [parse_expression(json.loads(r["expression"])) for r in rows]
    cols = [short(r["campaign_group_id"], r["group_name"]) for r in rows]
    ncol, nrow = len(rows), len(LAYERS)

    # value matrix: disp[row][col], cmp[row][col]
    disp = [[None] * ncol for _ in range(nrow)]
    cmp = [[0] * ncol for _ in range(nrow)]
    for ci, p in enumerate(parsed):
        for ri, (_, _, ex) in enumerate(LAYERS):
            d, v = ex(p)
            disp[ri][ci], cmp[ri][ci] = d, v

    fig, ax = plt.subplots(figsize=(1.9 + 1.35 * ncol, 1.6 + 0.52 * nrow))
    cw, chh = 1.0, 1.0
    for ri, (label, role, _) in enumerate(LAYERS):
        y = nrow - 1 - ri
        ax.text(-0.12, y + 0.5, label, ha="right", va="center", fontsize=9, color="#333")
        for ci in range(ncol):
            present = disp[ri][ci] is not None
            face = ROLE_COLOR[role] if present else "#ECECEC"
            anom = cmp[ri][ci] != cmp[ri][0]           # deviates from flagship (col 0)
            ax.add_patch(Rectangle((ci, y), cw, chh, facecolor=face,
                                   edgecolor="white", lw=1.5, zorder=2))
            if anom:
                ax.add_patch(Rectangle((ci + 0.04, y + 0.04), cw - 0.08, chh - 0.08,
                                       facecolor="none", edgecolor=RED, lw=2.4, zorder=4))
            txt = disp[ri][ci] if present else "–"
            ax.text(ci + 0.5, y + 0.5, txt, ha="center", va="center", fontsize=9.5,
                    color="white" if present else "#AAA",
                    fontweight="bold" if present else "normal", zorder=5)

    # column headers (flagship marked)
    for ci in range(ncol):
        ax.text(ci + 0.5, nrow + 0.12, cols[ci] + ("\n(flagship)" if ci == 0 else ""),
                ha="center", va="bottom", fontsize=8.3, color="#333")
    ax.set_xlim(-3.0, ncol + 0.15)
    ax.set_ylim(-0.9, nrow + 0.95)
    ax.axis("off")
    ax.set_title(f"{a.adv} — Prospecting Audience Fingerprint",
                 fontsize=14, fontweight="bold", loc="left", color="#222", x=-0.0, y=1.06)
    # anomaly legend
    ax.add_patch(Rectangle((-3.0, -0.72), 0.34, 0.34, facecolor="none", edgecolor=RED, lw=2.4))
    ax.text(-2.55, -0.55, "red outline = differs from flagship template   ·   "
            "'–' = layer absent", fontsize=8.5, color="#777", va="center")

    plt.tight_layout()
    plt.savefig(a.out, dpi=200, bbox_inches="tight")
    print(f"wrote {a.out}")

    # ---- decomposition markdown ----
    hdr = "| Targeting layer | " + " | ".join(r["campaign_group_id"] for r in rows) + " |"
    sep = "|" + "---|" * (ncol + 1)
    lines = [f"# {a.adv} — Prospecting audience decomposition", "",
             "Group names: " + "; ".join(f"{r['campaign_group_id']}={r['group_name']}" for r in rows),
             "", hdr, sep]
    for ri, (label, _, _) in enumerate(LAYERS):
        lines.append("| " + label + " | " +
                     " | ".join((disp[ri][ci] if disp[ri][ci] is not None else "–") for ci in range(ncol)) + " |")
    open(a.md, "w").write("\n".join(lines) + "\n")
    print(f"wrote {a.md}")

    # ---- finding (data-driven; no advertiser-specific narrative) ----
    gids = [rows[ci]["campaign_group_id"] for ci in range(ncol)]
    # a layer is SHARED when every column matches the flagship AND the flagship has it present
    shared, drift = [], []
    for ri, (label, _, _) in enumerate(LAYERS):
        present0 = disp[ri][0] is not None
        uniform = all(cmp[ri][ci] == cmp[ri][0] for ci in range(ncol))
        if uniform and present0:
            shared.append(label)
        elif not uniform:
            dev = [gids[ci] for ci in range(ncol) if cmp[ri][ci] != cmp[ri][0]]
            drift.append(f"{label} differs on {dev}")
    shared_txt = "; ".join(shared) if shared else "none"
    drift_txt = " | ".join(drift) if drift else "none — fully uniform template"
    print(f"FINDING: {ncol} active prospecting groups. Shared across all: {shared_txt}. "
          f"Drift: {drift_txt}.")


if __name__ == "__main__":
    main()
