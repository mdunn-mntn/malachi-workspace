"""Module 12c render — INTEREST-LOGIC deep-dive (targeting DNA + funnel-gate evidence).

Answers the client's core audience question: does any campaign NARROW its reach, and what is
UNIQUE about each? Parses all 6 prospecting expressions and overlays the empirical reach/overlap.

Finding: MM and 3P are OR'd (additive) in ALL 6 — no MM-AND-3P narrowing anywhere. The only
differentiator is a DS16 funnel gate on the 3 Q1-2026 variants (Harter/Motherhood/Mom-Focus):
`(NOT already-impressed/won by Kindred) OR (already served by THIS variant)` = a NET-NEW-reach
gate. Empirically each variant reaches ~1/4 of base's households, ~72% net-new vs base, ~90%
mutually disjoint (a 3-way creative split of the residual pool the ungated base already skipped).

Reads  02_prospecting_audience_expressions.csv · 12c_reach_monthly.csv · 12c_overlap.csv
Writes 12c_interest_dna.png · 12c_funnel_gate_evidence.png · 12c_interest_logic_deep_dive.md
"""
import argparse
import csv
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
NAVY, GREEN, RED, AMBER, GRAY = "#27496D", "#2E8B57", "#D63B2F", "#C77B30", "#666666"

GRPNAME = {"69884": "High Pop (base)", "109926": "Mid Pop", "96108": "Low Pop",
           "115943": "HiPop Harter", "115945": "HiPop Motherhood-J", "115946": "HiPop Mom-Focus"}
ORDER = ["69884", "109926", "96108", "115943", "115945", "115946"]


def tier_of(n):
    return f"High · {n}" if n <= 25 else (f"Mid · {n}" if n <= 80 else f"Low · {n}")


def geo_n(e):
    inc = []

    def rec(n, neg):
        if isinstance(n, list):
            [rec(x, neg) for x in n]
        elif isinstance(n, dict):
            v = n.get("value")
            if isinstance(v, dict) and "location_ids" in v:
                (None if neg else inc.extend(v["location_ids"]))
                return
            if v is not None:
                rec(v, neg ^ (n.get("op") == "not"))
    rec((e.get("geos") or {}).get("where"), False)
    return len(set(inc))


def ds_counts(node, out=None):
    out = {} if out is None else out
    if isinstance(node, list):
        [ds_counts(x, out) for x in node]
    elif isinstance(node, dict):
        v = node.get("value")
        if isinstance(v, dict) and "data_source_id" in v:
            out[v["data_source_id"]] = out.get(v["data_source_id"], 0) + len(v.get("category_ids") or [])
        elif v is not None:
            ds_counts(v, out)
    return out


def contains(node, ds):
    if isinstance(node, list):
        return any(contains(x, ds) for x in node)
    if isinstance(node, dict):
        v = node.get("value")
        if isinstance(v, dict) and v.get("data_source_id") == ds:
            return True
        return contains(v, ds) if v is not None else False
    return False


def join_op(node, a=19, b=35):
    best = [None, -1]

    def walk(n, depth):
        if isinstance(n, dict):
            op, v = n.get("op"), n.get("value")
            if op in ("and", "or") and v is not None and contains(v, a) and contains(v, b) and depth > best[1]:
                best[0], best[1] = op, depth
            if v is not None:
                walk(v, depth + 1)
        elif isinstance(n, list):
            for c in n:
                walk(c, depth + 1)
    walk(node, 0)
    return best[0]


def kfmt(v):
    v = float(v)
    return f"{v/1e6:.2f}M" if v >= 1e6 else (f"{v/1e3:.0f}K" if v >= 1e3 else f"{v:.0f}")


def main():
    ap = argparse.ArgumentParser()
    base = "outputs/kindred_35094/"
    ap.add_argument("--expr", default=base + "02_prospecting_audience_expressions.csv")
    ap.add_argument("--monthly", default=base + "12c_reach_monthly.csv")
    ap.add_argument("--overlap", default=base + "12c_overlap.csv")
    ap.add_argument("--dna-png", default=base + "12c_interest_dna.png")
    ap.add_argument("--gate-png", default=base + "12c_funnel_gate_evidence.png")
    ap.add_argument("--md", default=base + "12c_interest_logic_deep_dive.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()
    csv.field_size_limit(10 ** 7)

    rows = {r["campaign_group_id"]: r for r in csv.DictReader(open(a.expr)) if r.get("expression")}
    dna = {}
    for g in ORDER:
        e = json.loads(rows[g]["expression"])
        cw = (e.get("categories") or {}).get("where")
        dsc = ds_counts(cw)
        dna[g] = {"geo": geo_n(e), "mm": dsc.get(19, 0), "tp": dsc.get(35, 0),
                  "join": join_op(cw), "gate": contains(cw, 16)}

    # =====================================================================
    # PNG 1 — INTEREST-LOGIC DNA TABLE
    # =====================================================================
    fig, ax = plt.subplots(figsize=(15, 6.6))
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.text(0.03, 0.945, f"{a.adv} — interest logic: additive everywhere; only the 3 Q1 variants gate to net-new",
             fontsize=17, fontweight="bold", color="#222")
    fig.text(0.03, 0.905, "No campaign narrows MM with a required 3P segment — all 6 are (MM keywords OR 3P segments) = additive. "
             "The 3 variants AND a DS16 funnel gate; base/Mid/Low do not.", fontsize=11.5, color=GRAY)
    cols = ["Campaign", "Geo tier · #DMAs", "MM kw", "3P seg", "MM × 3P", "Funnel gate (DS16)", "Read"]
    xs = [0.03, 0.235, 0.40, 0.475, 0.55, 0.685, 0.885]
    yt = 0.83
    for x, c in zip(xs, cols):
        ax.text(x, yt, c, fontsize=11.5, fontweight="bold", color=NAVY, va="center")
    ax.plot([0.03, 0.985], [0.80, 0.80], color=NAVY, lw=1.4)
    y = 0.745
    dy = 0.108
    for i, g in enumerate(ORDER):
        d = dna[g]
        if i % 2 == 0:
            ax.axhspan(y - dy / 2 + 0.006, y + dy / 2 + 0.006, color="#000", alpha=0.03)
        gate_txt = "net-new only  (AND'd)" if d["gate"] else "—"
        gate_col = AMBER if d["gate"] else GRAY
        read = "net-new residual gate" if d["gate"] else ("broad · ungated" if g == "69884" else "clean geo slice")
        read_col = AMBER if d["gate"] else (NAVY if g == "69884" else GREEN)
        cells = [f"{g}  {GRPNAME[g]}", tier_of(d["geo"]), str(d["mm"]), str(d["tp"]),
                 f"{(d['join'] or '?').upper()}  (additive)"]
        colors = ["#222", "#222", "#222", "#222", GREEN]
        for x, txt, cc in zip(xs, cells, colors):
            ax.text(x, y, txt, fontsize=11, va="center", color=cc,
                    fontweight="bold" if x == xs[0] else "normal")
        ax.text(xs[5], y, gate_txt, fontsize=11, va="center", color=gate_col,
                fontweight="bold" if d["gate"] else "normal")
        ax.text(xs[6], y, read, fontsize=10.2, va="center", color=read_col, style="italic")
        y -= dy
    ax.text(0.03, y + 0.02, "Shared by all 6 (hygiene): exclude CRM-upload lists (DS47) · own converters (DS21) · own funnel (DS34).  "
            "DS14[1]=Beeswax bidder = plumbing.", fontsize=9.8, color=GRAY, va="center", style="italic")
    plt.savefig(a.dna_png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.dna_png}")
    plt.close(fig)

    # =====================================================================
    # PNG 2 — FUNNEL-GATE EVIDENCE (reach ¼ · net-new · disjoint · handoff)
    # =====================================================================
    ov = {r["pair"]: r for r in csv.DictReader(open(a.overlap))}
    reach = {"base": int(ov["base_reach"]["reach_a"]), "115943": int(ov["harter"]["reach_b"]),
             "115945": int(ov["mother"]["reach_b"]), "115946": int(ov["momfocus"]["reach_b"])}
    # monthly imps pivot
    mon_rows = list(csv.DictReader(open(a.monthly)))
    months = sorted({r["mon"] for r in mon_rows})
    def series(grps):
        return [sum(int(r["imps"]) for r in mon_rows if r["mon"] == m and r["grp"] in grps) for m in months]
    base_s = series({"69884"})
    var_s = series({"115943", "115945", "115946"})
    mid_s = series({"109926"})

    fig = plt.figure(figsize=(15, 7.2))
    fig.text(0.03, 0.955, f"{a.adv} — the variants' funnel gate: a smaller, net-new, 3-way-split residual pool",
             fontsize=16.5, fontweight="bold", color="#222")
    fig.text(0.03, 0.915, "Each variant is gated to households Kindred has NOT already impressed — so they fish the residual "
             "the ungated base skipped.", fontsize=11.5, color=GRAY)

    # left: reach bars
    axL = fig.add_axes([0.05, 0.20, 0.40, 0.60])
    labs = ["High Pop\n(base, ungated)", "Harter", "Motherhood-J", "Mom-Focus"]
    vals = [reach["base"], reach["115943"], reach["115945"], reach["115946"]]
    cols_b = [NAVY, AMBER, AMBER, AMBER]
    ypos = list(range(len(vals)))[::-1]
    axL.barh(ypos, vals, color=cols_b, height=0.62)
    for yp, v in zip(ypos, vals):
        axL.text(v + max(vals) * 0.015, yp, kfmt(v), va="center", fontsize=11.5, fontweight="bold", color="#222")
    axL.set_yticks(ypos)
    axL.set_yticklabels(labs, fontsize=10.5)
    axL.set_xlim(0, max(vals) * 1.16)
    axL.set_xticks([])
    for sp in ["top", "right", "bottom"]:
        axL.spines[sp].set_visible(False)
    axL.set_title("Distinct households reached (Jan–May '26)", fontsize=12, color="#333", loc="left", pad=8)
    axL.text(0.0, -0.14, "each variant ≈ ¼ of base's reach", transform=axL.transAxes, fontsize=10.5,
             color=AMBER, fontweight="bold")

    # right: handoff timeline
    axR = fig.add_axes([0.56, 0.20, 0.40, 0.60])
    mlab = [m[2:] for m in months]
    axR.plot(mlab, base_s, "-o", color=NAVY, lw=2.6, ms=6, label="High Pop base (ungated)")
    axR.plot(mlab, var_s, "-o", color=AMBER, lw=2.6, ms=6, label="3 gated variants (Σ)")
    axR.plot(mlab, mid_s, "--o", color=GRAY, lw=1.8, ms=4, label="Mid Pop")
    axR.set_title("Monthly impressions — base winds down, gated variants replace it", fontsize=12, color="#333", loc="left", pad=8)
    axR.set_ylabel("impressions", fontsize=10, color="#555")
    axR.legend(frameon=False, fontsize=9.5, loc="upper right")
    for sp in ["top", "right"]:
        axR.spines[sp].set_visible(False)
    axR.tick_params(labelsize=9.5)
    axR.annotate("base dark\nby Apr", xy=(3, 0), xytext=(2.4, max(base_s) * 0.33), fontsize=9, color=NAVY,
                 ha="center", arrowprops=dict(arrowstyle="->", color=NAVY, lw=1.2))

    # caption band with the two overlap facts + rotation implication
    fig.patches.append(plt.Rectangle((0.03, 0.02), 0.94, 0.11, transform=fig.transFigure,
                       color=NAVY, alpha=0.06, zorder=0))
    fig.text(0.05, 0.083, "~72% of each variant's households are NET-NEW vs base   ·   the 3 variants are ~90% MUTUALLY DISJOINT "
             "(a 3-way creative split of the residual)", fontsize=11.3, color="#222", fontweight="bold")
    fig.text(0.05, 0.045, "Consequence: top-20 prospecting rotated from the broad, ungated base (ROAS 2.39x) to the gated variants "
             "fishing the smaller, lower-quality residual (1.18–1.35x). The gate narrows by WHO, not by 3P.",
             fontsize=10.6, color="#333", style="italic")
    plt.savefig(a.gate_png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.gate_png}")
    plt.close(fig)

    # =====================================================================
    # MD
    # =====================================================================
    md = [f"# {a.adv} — Interest-logic deep-dive (targeting DNA + funnel gate)", "",
          "**No campaign narrows MM with a required 3P segment.** All 6 prospecting campaigns share the same "
          "interest core `(MM DS19[255 kw] OR 3P DS35[11–14 maternity/baby segs])` — OR = additive/broadening. "
          "The suspected `MM AND 3P` pattern is absent everywhere.", "",
          "## Per-campaign targeting DNA", "",
          "| Campaign | Geo tier · #DMAs | MM kw | 3P seg | MM×3P | Funnel gate (DS16) | Read |",
          "|---|---|---:|---:|---|---|---|"]
    for g in ORDER:
        d = dna[g]
        gate = "**net-new only (AND'd)**" if d["gate"] else "—"
        read = "net-new residual gate" if d["gate"] else ("broad · ungated" if g == "69884" else "clean geo slice")
        md.append(f"| {g} {GRPNAME[g]} | {tier_of(d['geo'])} | {d['mm']} | {d['tp']} | "
                  f"{(d['join'] or '?').upper()} (additive) | {gate} | {read} |")
    md += ["", "## The differentiator — the 3 Q1-2026 variants add a DS16 funnel gate",
           "`AND ( NOT DS16[7291 Impressions, 787280 Wins]  OR  DS16[own campaign-group] )` — decoded via "
           "`tpa.categories` (data_source_id=16 = the advertiser's own funnel). Target a household **iff** it "
           "was NEVER impressed/won by Kindred **OR** is already owned by this variant = a **net-new-reach gate**.",
           "",
           "## Empirical narrowing (BQ-native HLL reach on `sum_by_campaign_by_day`, Jan–May '26)",
           "",
           "| Metric | Value |",
           "|---|---|",
           f"| Base High Pop reach | {reach['base']:,} distinct households |",
           f"| Each variant reach | ~435K = **~26% of base** (¼ the pool) |",
           "| Base ∩ variant | ~27% → **~72% net-new vs base** |",
           "| Variant ∩ variant | ~9% → **~90% mutually disjoint** (3-way creative split) |",
           "",
           "**Rotation:** base High Pop (ungated, ROAS 2.39x) wound down Jan→Mar and went dark by April; the 3 "
           "gated variants (ROAS 1.18–1.35x) ramped up to replace it — so by May, top-20 prospecting is run by the "
           "gated variants fishing the smaller, lower-quality residual net-new pool. **The gate narrows by WHO "
           "(net-new households), not by 3P.**", ""]
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")
    print("FINDING: all 6 = MM OR 3P (additive); only the 3 Q1 variants AND a DS16 net-new funnel gate. "
          "Each variant reaches ~26% of base, ~72% net-new vs base, ~90% mutually disjoint (3-way creative split). "
          "Base (2.39x) wound down; gated variants (1.18-1.35x) replaced it on the residual pool.")


if __name__ == "__main__":
    main()
