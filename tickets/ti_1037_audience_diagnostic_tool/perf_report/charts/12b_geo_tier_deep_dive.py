"""Module 12b render — GEO / DMA tier deep-dive.

Opens up module 12's "geo-sliced by DMA" finding into something readable:
  (A) 12b_geo_tier_reference.png  — the 210 US DMAs NAMED, grouped by population tier
      (High 20 / Mid 38 / Low 152), each annotated with recent (in-TTL month) delivery.
  (B) 12b_geo_tier_performance.png — the finding: P1 ran the top-20 (High Pop) ONLY;
      P2 fragmented into 6 campaigns across all 210 DMAs, and the new tiers/variants
      deliver at materially lower ROAS. Flagship YoY collapse shown up top.
  (C) 12b_geo_tier_deep_dive.md — committable; full named lists for all three tiers.

Reads  02_prospecting_audience_expressions.csv (per-campaign geo location_ids)
       12b_geo_dma_decode.csv        (location_id -> name + Nielsen code)
       12b_geo_tier_metrics.csv      (per-tier P1/P2 performance)
       12b_per_dma_delivery_may26.csv(per-DMA recent delivery; metro_id = Nielsen)
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

# 6 prospecting campaigns -> 3 geo footprints (by include-set size). Names carry the tier.
TIER_ORDER = ["HIGH POP", "MID POP", "LOW POP"]
TIER_COLOR = {"HIGH POP": NAVY, "MID POP": AMBER, "LOW POP": GRAY}


def tier_of(n_inc):
    return "HIGH POP" if n_inc <= 25 else ("MID POP" if n_inc <= 80 else "LOW POP")


def geo_sets(e):
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
    return sorted(set(inc))


def shorten(nm):
    """Reduce noisy multi-city Nielsen names to 'Primary City, ST' so columns read cleanly."""
    state = ""
    if "(" not in nm and "," in nm:                       # skip parenthetical names (e.g. Washington DC)
        head, tail = nm.rsplit(",", 1)
        t = tail.strip()
        if 2 <= len(t) <= 6 and t.replace(".", "").isalpha():
            state, nm = ", " + t, head
    nm = nm.split("(")[0].split("-")[0].strip().rstrip(",").strip()
    return (nm + state)[:24]


def kfmt(v):
    v = float(v)
    return f"{v/1e6:.1f}M" if v >= 1e6 else (f"{v/1e3:.0f}K" if v >= 1e3 else f"{v:.0f}")


def main():
    ap = argparse.ArgumentParser()
    base = "outputs/kindred_35094/"
    ap.add_argument("--expr", default=base + "02_prospecting_audience_expressions.csv")
    ap.add_argument("--geo", default=base + "12b_geo_dma_decode.csv")
    ap.add_argument("--metrics", default=base + "12b_geo_tier_metrics.csv")
    ap.add_argument("--deliv", default=base + "12b_per_dma_delivery_may26.csv")
    ap.add_argument("--ref-png", default=base + "12b_geo_tier_reference.png")
    ap.add_argument("--perf-png", default=base + "12b_geo_tier_performance.png")
    ap.add_argument("--md", default=base + "12b_geo_tier_deep_dive.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--deliv-label", default="May '26")
    a = ap.parse_args()
    csv.field_size_limit(10 ** 7)

    geo = {r["location_id"]: (r["dma_name"], r["nielsen_code"]) for r in csv.DictReader(open(a.geo))}
    # delivery by Nielsen code (metro_id), summed across tiers for the recent month
    deliv = {}
    for r in csv.DictReader(open(a.deliv)):
        deliv[r["metro_id"]] = deliv.get(r["metro_id"], 0) + int(r["imps"])
    metrics = {(r["campaign_group_id"], r["period"]): r for r in csv.DictReader(open(a.metrics))}
    rows = [r for r in csv.DictReader(open(a.expr)) if r.get("expression")]

    # per campaign: grp, name, inc location_ids, tier
    camps = []
    tier_incs = {}
    for r in rows:
        inc = geo_sets(json.loads(r["expression"]))
        t = tier_of(len(inc))
        camps.append({"grp": r["campaign_group_id"], "name": (r["group_name"] or "").strip(),
                      "n": len(inc), "inc": inc, "tier": t})
        tier_incs.setdefault(t, inc)  # all campaigns in a tier share the same DMA set

    # named+delivery DMA list per tier (sorted by recent delivery desc)
    def named(tier):
        out = []
        for lid in tier_incs.get(tier, []):
            nm, ncode = geo.get(str(lid), (str(lid), ""))
            out.append((shorten(nm), deliv.get(ncode, 0)))
        return sorted(out, key=lambda x: -x[1])

    tier_named = {t: named(t) for t in TIER_ORDER}

    # =====================================================================
    # (A) TIER REFERENCE — named DMAs, readable grid
    # =====================================================================
    fig = plt.figure(figsize=(15, 8.7))
    ax = fig.add_axes([0.03, 0.02, 0.94, 0.90])
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.text(0.03, 0.975, f"{a.adv} — the 210 US DMAs, sliced into 3 population tiers",
             fontsize=19, fontweight="bold", color="#222")
    fig.text(0.03, 0.945, f"Prospecting geo-slices ALL 210 Nielsen DMAs into High/Mid/Low population buckets. "
             f"\"High Pop\" = the top-20 markets, NOT all-of-US.  ({a.deliv_label} delivery in parens)",
             fontsize=11.5, color=GRAY)
    RH, HB = 0.038, 0.040  # row height, header-bar height

    def header(y_top, tier, extra=""):
        # bar occupies [y_top-HB, y_top]; content flows DOWN; returns cursor just below the bar
        col = TIER_COLOR[tier]
        n = len(tier_named[tier])
        ax.add_patch(plt.Rectangle((0.0, y_top - HB), 1.0, HB, color=col, alpha=0.92, zorder=1))
        ax.text(0.008, y_top - HB / 2, f"{tier}  ·  {n} DMAs{extra}", fontsize=13.5,
                fontweight="bold", color="white", va="center", zorder=2)
        return y_top - HB - 0.020

    def grid(names, ytop, ncols):
        rows_per = -(-len(names) // ncols)
        cw = 1.0 / ncols
        for i, (nm, d) in enumerate(names):
            cidx, ridx = divmod(i, rows_per)
            x = 0.010 + cidx * cw
            y = ytop - ridx * RH
            ax.text(x, y, f"{i+1}. {nm}", fontsize=10.6, color="#222", va="top")
            if d:
                ax.text(x + cw - 0.022, y, f"({kfmt(d)})", fontsize=9.4, color=GRAY, va="top", ha="right")
        return ytop - rows_per * RH

    cur = header(0.905, "HIGH POP", "   ·   the only tier that ran in P1 (Jan–May '25)")
    cur = grid(tier_named["HIGH POP"], cur, 4) - 0.014
    cur = header(cur, "MID POP", "   ·   launched Feb '26")
    cur = grid(tier_named["MID POP"], cur, 4) - 0.014
    cur = header(cur, "LOW POP", "   ·   the long tail — launched Aug '25")
    low = tier_named["LOW POP"]
    top12 = ", ".join(f"{nm} ({kfmt(d)})" for nm, d in low[:12])
    ax.text(0.010, cur - 0.010, "Largest by delivery:  " + top12, fontsize=10.4, color="#222", va="top")
    ax.text(0.010, cur - 0.046, f"+ {len(low) - 12} more small-market DMAs — full ranked list in "
            f"12b_geo_tier_deep_dive.md", fontsize=10, color=GRAY, va="top", style="italic")
    plt.savefig(a.ref_png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.ref_png}")
    plt.close(fig)

    # =====================================================================
    # (B) TIER PERFORMANCE — flagship YoY collapse + P2 footprint gradient
    # =====================================================================
    # blended P2 across all 6
    P2 = [metrics[(c["grp"], "P2")] for c in camps if (c["grp"], "P2") in metrics]
    b_imps = sum(int(r["imps"]) for r in P2)
    b_vis = sum(int(r["visits"]) for r in P2)
    b_conv = sum(int(r["conv"]) for r in P2)
    b_spend = sum(float(r["spend"]) for r in P2)
    b_rev = sum(float(r["revenue"]) for r in P2)
    b_vr, b_cvr, b_roas = 1000 * b_vis / b_imps, 100 * b_conv / b_vis, b_rev / b_spend
    hp1 = metrics[("69884", "P1")]
    hp2 = metrics[("69884", "P2")]

    fig, ax = plt.subplots(figsize=(15, 9.2))
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.text(0.03, 0.955, f"{a.adv} — geo footprint fragmented, and the new pieces convert worse",
             fontsize=18.5, fontweight="bold", color="#222")
    fig.text(0.03, 0.921, "P1 (Jan–May '25) ran the top-20 (High Pop) ONLY. P2 (Jan–May '26) split into 6 campaigns "
             "across all 210 DMAs — the flagship halved and the new tiers/variants deliver at 1.2–1.7 ROAS.",
             fontsize=11.5, color=GRAY)

    # --- Band 1: flagship YoY collapse (High Pop 69884) ---
    fig.text(0.03, 0.865, "FLAGSHIP (69884 High Pop, same top-20 DMAs) — YoY collapse",
             fontsize=13, fontweight="bold", color=NAVY)
    cols1 = ["", "Impressions", "Visits", "Conv", "Spend", "Visit rate", "CVR", "ROAS"]
    xf = [0.03, 0.20, 0.33, 0.44, 0.53, 0.66, 0.79, 0.90]
    yr = 0.828
    for x, c in zip(xf, cols1):
        ax.text(x, yr, c, fontsize=11, fontweight="bold", color="#444")
    def frow(y, lab, r, labcol):
        vals = [lab, kfmt(r["imps"]), kfmt(r["visits"]), kfmt(r["conv"]), "$" + kfmt(r["spend"]),
                f"{float(r['vr_permille']):.1f}‰", f"{float(r['cvr_pct']):.1f}%", f"{float(r['roas']):.2f}x"]
        for x, v in zip(xf, vals):
            ax.text(x, y, v, fontsize=11.5, color=labcol if x == xf[0] else "#222",
                    fontweight="bold" if x == xf[0] else "normal")
    frow(0.798, "P1  '25", hp1, NAVY)
    frow(0.770, "P2  '26", hp2, NAVY)
    # delta row
    d_vr = 100 * (float(hp2["vr_permille"]) / float(hp1["vr_permille"]) - 1)
    d_cvr = 100 * (float(hp2["cvr_pct"]) / float(hp1["cvr_pct"]) - 1)
    d_roas = 100 * (float(hp2["roas"]) / float(hp1["roas"]) - 1)
    d_imp = 100 * (float(hp2["imps"]) / float(hp1["imps"]) - 1)
    dvals = ["change", f"{d_imp:+.0f}%", "", "", "", f"{d_vr:+.0f}%", f"{d_cvr:+.0f}%", f"{d_roas:+.0f}%"]
    for x, v in zip(xf, dvals):
        ax.text(x, 0.742, v, fontsize=11, color=RED, fontweight="bold")
    ax.plot([0.03, 0.97], [0.755, 0.755], color="#ddd", lw=1)

    # --- Band 2: P2 footprint by tier (expansion + ROAS gradient) ---
    fig.text(0.03, 0.688, "P2 FOOTPRINT — 1 campaign -> 6, top-20 -> 210 DMAs (ROAS gradient by tier)",
             fontsize=13, fontweight="bold", color=NAVY)
    cols2 = ["Campaign", "Tier", "#DMAs", "Launched", "P2 imps", "% mix", "VR", "CVR", "ROAS"]
    xg = [0.03, 0.255, 0.35, 0.435, 0.56, 0.66, 0.74, 0.82, 0.90]
    yr2 = 0.651
    for x, c in zip(xg, cols2):
        ax.text(x, yr2, c, fontsize=11, fontweight="bold", color="#444")
    ax.plot([0.03, 0.97], [0.637, 0.637], color=NAVY, lw=1.3)
    order = sorted(camps, key=lambda c: (TIER_ORDER.index(c["tier"]), -int(metrics[(c["grp"], "P2")]["imps"])))
    y = 0.610
    for c in order:
        r = metrics[(c["grp"], "P2")]
        roas = float(r["roas"])
        rc = GREEN if roas >= 2.0 else (AMBER if roas >= 1.5 else RED)
        nm = c["name"].replace("CTV Prospecting", "").strip() or c["grp"]
        share = 100 * int(r["imps"]) / b_imps
        launch = r["first_day"][:7]
        cells = [f"{c['grp']} {nm[:20]}", c["tier"].title(), str(c["n"]), launch,
                 kfmt(r["imps"]), f"{share:.0f}%", f"{float(r['vr_permille']):.1f}‰",
                 f"{float(r['cvr_pct']):.1f}%", f"{roas:.2f}x"]
        for x, v, in zip(xg, cells):
            col = rc if v.endswith("x") else "#222"
            ax.text(x, y, v, fontsize=10.8, color=col, va="center",
                    fontweight="bold" if v.endswith("x") else "normal")
        ax.add_patch(plt.Rectangle((0.232, y - 0.009), 0.016, 0.018,
                     color=TIER_COLOR[c["tier"]], alpha=0.85))
        y -= 0.0335
    # blended row
    ax.plot([0.03, 0.97], [y + 0.008, y + 0.008], color="#ddd", lw=1)
    y -= 0.018
    bl = ["BLENDED P2 (all 6)", "210", "", "", kfmt(b_imps), "100%",
          f"{b_vr:.1f}‰", f"{b_cvr:.1f}%", f"{b_roas:.2f}x"]
    xg_bl = [0.03, 0.35, 0.435, 0.56, 0.56, 0.66, 0.74, 0.82, 0.90]
    for x, v in zip(xg_bl, bl):
        ax.text(x, y, v, fontsize=11.2, color=NAVY, fontweight="bold", va="center")

    # takeaway strip
    ax.add_patch(plt.Rectangle((0.03, y - 0.085), 0.94, 0.058, color=NAVY, alpha=0.06))
    ax.text(0.045, y - 0.056,
            f"Prospecting ROAS {float(hp1['roas']):.2f}x (P1, top-20 only)  ->  {b_roas:.2f}x blended (P2).  "
            f"~90% is the flagship collapse ({float(hp1['roas']):.2f}->{float(hp2['roas']):.2f}); the rest is "
            f"fragmentation into Mid/Low tiers (1.3-1.7x) and 3 new "
            f"top-20 interest-variants (1.2–1.4x) that dilute the blend.",
            fontsize=10.6, color="#222", va="center", style="italic")
    plt.savefig(a.perf_png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.perf_png}")
    plt.close(fig)

    # =====================================================================
    # (C) markdown — full named lists + tier metrics
    # =====================================================================
    md = [f"# {a.adv} — Geo / DMA tier deep-dive", "",
          "**Prospecting slices all 210 US Nielsen DMAs into three population tiers.** "
          "\"High Pop\" is the **top-20 markets, not all-of-US.** P1 (Jan–May '25) ran the High Pop "
          "tier ONLY; P2 (Jan–May '26) fragmented into 6 campaigns spanning all 210 DMAs.", "",
          "## Footprint & performance by tier", "",
          "| Campaign | Tier | #DMAs | Launched | P2 imps | P2 VR | P2 CVR | P2 ROAS |",
          "|---|---|---:|---|---:|---:|---:|---:|"]
    for c in order:
        r = metrics[(c["grp"], "P2")]
        nm = c["name"].replace("CTV Prospecting", "").strip()
        md.append(f"| {c['grp']} {nm} | {c['tier'].title()} | {c['n']} | {r['first_day'][:7]} | "
                  f"{kfmt(r['imps'])} | {float(r['vr_permille']):.1f}‰ | {float(r['cvr_pct']):.1f}% | "
                  f"{float(r['roas']):.2f}x |")
    md += ["", f"**Flagship (69884 High Pop) YoY:** ROAS {float(hp1['roas']):.2f}x → {float(hp2['roas']):.2f}x, "
           f"VR {float(hp1['vr_permille']):.1f}→{float(hp2['vr_permille']):.1f}‰, "
           f"CVR {float(hp1['cvr_pct']):.1f}→{float(hp2['cvr_pct']):.1f}%. ",
           f"**Blended P2 ROAS {b_roas:.2f}x** vs **P1 {float(hp1['roas']):.2f}x** (High Pop only). "
           f"~90% of the drop is the flagship collapse; the remainder is fragmentation into "
           f"lower-ROAS tiers/variants.", ""]
    for t in TIER_ORDER:
        names = tier_named[t]
        md.append(f"## {t} — {len(names)} DMAs  (ranked by {a.deliv_label} delivery)")
        md.append(", ".join(f"{nm} ({kfmt(d)})" if d else nm for nm, d in names))
        md.append("")
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")
    print(f"FINDING: geo footprint expanded 1→6 campaigns, top-20→210 DMAs. P1 = High Pop (top-20) only. "
          f"P2 blended ROAS {b_roas:.2f}x vs P1 {float(hp1['roas']):.2f}x; flagship collapse "
          f"({float(hp1['roas']):.2f}→{float(hp2['roas']):.2f}x) ~90%, tier/variant fragmentation the rest "
          f"(Mid {float(metrics[('109926','P2')]['roas']):.2f}x, Low {float(metrics[('96108','P2')]['roas']):.2f}x).")


if __name__ == "__main__":
    main()
