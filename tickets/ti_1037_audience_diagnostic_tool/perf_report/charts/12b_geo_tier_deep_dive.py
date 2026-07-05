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

# Geo footprints by include-set size. NATIONAL (location_id 237 = all-of-US, or empty geo) is its
# own tier — for national advertisers (e.g. Bouqs) every campaign lands here and geo-tiering is N/A.
NATIONAL_LOC = "237"
TIER_ORDER = ["NATIONAL", "HIGH POP", "MID POP", "LOW POP"]
TIER_COLOR = {"NATIONAL": NAVY, "HIGH POP": NAVY, "MID POP": AMBER, "LOW POP": GRAY}


def is_national(inc):
    return len(inc) == 0 or [str(i) for i in inc] == [NATIONAL_LOC]


def tier_of(inc):
    if is_national(inc):
        return "NATIONAL"
    n = len(inc)
    return "HIGH POP" if n <= 25 else ("MID POP" if n <= 80 else "LOW POP")


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


    # per campaign: grp, name, inc location_ids, tier (national collapses to a single tier)
    camps = []
    tier_incs = {}
    for r in rows:
        inc = geo_sets(json.loads(r["expression"]))
        t = tier_of(inc)
        camps.append({"grp": r["campaign_group_id"], "name": (r["group_name"] or "").strip(),
                      "n": (0 if is_national(inc) else len(inc)), "inc": inc, "tier": t,
                      "natl": is_national(inc)})
        tier_incs.setdefault(t, inc)  # campaigns sharing a tier share the same geo set
    present_tiers = [t for t in TIER_ORDER if any(c["tier"] == t for c in camps)]
    # Account is "national" when the majority of prospecting campaigns target all-of-US: geo-tiering
    # is then N/A even if one campaign (e.g. a seasonal VDay push) carries a DMA slice.
    national_acct = camps and (sum(1 for c in camps if c["natl"]) / len(camps)) >= 0.5

    # named+delivery DMA list per tier (national tier has none; sorted by recent delivery desc)
    def named(tier):
        out = []
        for lid in tier_incs.get(tier, []):
            if str(lid) == NATIONAL_LOC:
                continue
            nm, ncode = geo.get(str(lid), (str(lid), ""))
            out.append((shorten(nm), deliv.get(ncode, 0)))
        return sorted(out, key=lambda x: -x[1])

    tier_named = {t: named(t) for t in TIER_ORDER}

    # flagship = the campaign with BOTH P1 and P2 metrics and the largest P1 impressions.
    # (Kindred: the High Pop base 69884; Bouqs: the one YoY-comparable group.) None if no group ran in P1.
    yoy = [c for c in camps if (c["grp"], "P1") in metrics and (c["grp"], "P2") in metrics]
    flag = max(yoy, key=lambda c: int(metrics[(c["grp"], "P1")]["imps"])) if yoy else None
    hp1 = metrics[(flag["grp"], "P1")] if flag else None
    hp2 = metrics[(flag["grp"], "P2")] if flag else None

    # =====================================================================
    # (A) TIER / GEO REFERENCE
    # =====================================================================
    fig = plt.figure(figsize=(15, 8.7))
    ax = fig.add_axes([0.03, 0.02, 0.94, 0.90])
    ax.axis("off"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    if national_acct:
        # National advertiser: geo-tiering is N/A. Emit a clear "no DMA slicing" reference panel.
        fig.text(0.03, 0.955, f"{a.adv} — national targeting (no DMA slicing)",
                 fontsize=19, fontweight="bold", color="#222")
        fig.text(0.03, 0.915, "Prospecting targets location_id 237 = United States (all-of-US) nationally. "
                 "There is NO population-tier / DMA slice here, so the per-tier DMA reference does not apply. "
                 f"Delivery still spreads geographically ({a.deliv_label}); the top markets by delivery are shown below.",
                 fontsize=11.5, color=GRAY)
        ax.add_patch(plt.Rectangle((0.0, 0.86), 1.0, 0.040, color=NAVY, alpha=0.92))
        ax.text(0.008, 0.88, f"NATIONAL (US)  ·  {len(camps)} prospecting campaigns  ·  geo-tiering N/A",
                fontsize=13.5, fontweight="bold", color="white", va="center")
        # top DMAs by actual delivery (not targeting) — context that generalizes to national accounts
        top_deliv = sorted(deliv.items(), key=lambda x: -x[1])[:40]
        metroname = {r["nielsen_code"]: r["dma_name"] for r in csv.DictReader(open(a.geo))}
        RH = 0.036
        ax.text(0.010, 0.815, f"Top markets by delivered impressions ({a.deliv_label}):",
                fontsize=11.5, fontweight="bold", color="#222", va="top")
        for i, (code, v) in enumerate(top_deliv):
            cidx, ridx = divmod(i, 20)
            x = 0.010 + cidx * 0.50
            y = 0.775 - ridx * RH
            nm = shorten(metroname.get(code, code))
            ax.text(x, y, f"{i+1}. {nm}", fontsize=10.4, color="#222", va="top")
            ax.text(x + 0.46, y, f"({kfmt(v)})", fontsize=9.4, color=GRAY, va="top", ha="right")
        if not top_deliv:
            ax.text(0.010, 0.77, "(no in-TTL delivery rows for this month)", fontsize=11, color=GRAY, va="top", style="italic")
    else:
        fig.text(0.03, 0.975, f"{a.adv} — the 210 US DMAs, sliced into population tiers",
                 fontsize=19, fontweight="bold", color="#222")
        fig.text(0.03, 0.945, "Prospecting geo-slices Nielsen DMAs into High/Mid/Low population buckets. "
                 f"\"High Pop\" = the top markets, NOT all-of-US.  ({a.deliv_label} delivery in parens)",
                 fontsize=11.5, color=GRAY)
        RH, HB = 0.038, 0.040

        def header(y_top, tier, extra=""):
            col = TIER_COLOR[tier]
            nn = len(tier_named[tier])
            ax.add_patch(plt.Rectangle((0.0, y_top - HB), 1.0, HB, color=col, alpha=0.92, zorder=1))
            ax.text(0.008, y_top - HB / 2, f"{tier}  ·  {nn} DMAs{extra}", fontsize=13.5,
                    fontweight="bold", color="white", va="center", zorder=2)
            return y_top - HB - 0.020

        def grid(names, ytop, ncols):
            rows_per = -(-len(names) // ncols) if names else 1
            cw = 1.0 / ncols
            for i, (nm, d) in enumerate(names):
                cidx, ridx = divmod(i, rows_per)
                x = 0.010 + cidx * cw
                y = ytop - ridx * RH
                ax.text(x, y, f"{i+1}. {nm}", fontsize=10.6, color="#222", va="top")
                if d:
                    ax.text(x + cw - 0.022, y, f"({kfmt(d)})", fontsize=9.4, color=GRAY, va="top", ha="right")
            return ytop - rows_per * RH

        cur = 0.905
        geo_tiers = [t for t in present_tiers if t != "NATIONAL"]
        for t in geo_tiers:
            cur = header(cur, t) - 0.006
            nm_list = tier_named[t]
            if len(nm_list) > 40:  # long tail — summarize
                top12 = ", ".join(f"{nm} ({kfmt(d)})" for nm, d in nm_list[:12])
                ax.text(0.010, cur - 0.006, "Largest by delivery:  " + top12, fontsize=10.2, color="#222", va="top")
                ax.text(0.010, cur - 0.040, f"+ {len(nm_list) - 12} more small-market DMAs — full list in the .md",
                        fontsize=9.6, color=GRAY, va="top", style="italic")
                cur -= 0.070
            else:
                cur = grid(nm_list, cur, 4) - 0.014
    plt.savefig(a.ref_png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.ref_png}")
    plt.close(fig)

    # =====================================================================
    # (B) PERFORMANCE — flagship YoY (if any) + P2 footprint by campaign
    # =====================================================================
    P2 = [metrics[(c["grp"], "P2")] for c in camps if (c["grp"], "P2") in metrics]
    b_imps = sum(int(r["imps"]) for r in P2) or 1
    b_vis = sum(int(r["visits"]) for r in P2)
    b_conv = sum(int(r["conv"]) for r in P2)
    b_spend = sum(float(r["spend"]) for r in P2) or 1
    b_rev = sum(float(r["revenue"]) for r in P2)
    b_vr = 1000 * b_vis / b_imps
    b_cvr = 100 * b_conv / b_vis if b_vis else 0
    b_roas = b_rev / b_spend

    fig, ax = plt.subplots(figsize=(15, 9.2))
    ax.axis("off"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    if national_acct:
        subtitle = ("Prospecting targets all-of-US nationally (no DMA tiers). P2 spread across "
                    f"{len(camps)} campaigns; the flagship YoY and each campaign's P2 performance are shown.")
    else:
        subtitle = ("P1 ran the top-tier markets; P2 fragmented across more DMAs/campaigns. "
                    "Flagship YoY and per-campaign P2 performance shown below.")
    fig.text(0.03, 0.955, f"{a.adv} — prospecting footprint & performance", fontsize=18.5, fontweight="bold", color="#222")
    fig.text(0.03, 0.921, subtitle, fontsize=11.5, color=GRAY)

    # --- Band 1: flagship YoY (dynamic; skipped if no YoY-comparable group) ---
    if flag:
        tlabel = "national" if flag["natl"] else f"{flag['tier'].title()}, {flag['n']} DMAs"
        fig.text(0.03, 0.865, f"FLAGSHIP ({flag['grp']} {flag['name'][:32]}, {tlabel}) — YoY",
                 fontsize=13, fontweight="bold", color=NAVY)
        cols1 = ["", "Impressions", "Visits", "Conv", "Spend", "Visit rate", "CVR", "ROAS"]
        xf = [0.03, 0.20, 0.33, 0.44, 0.53, 0.66, 0.79, 0.90]
        for x, c in zip(xf, cols1):
            ax.text(x, 0.828, c, fontsize=11, fontweight="bold", color="#444")

        def frow(y, lab, r):
            vals = [lab, kfmt(r["imps"]), kfmt(r["visits"]), kfmt(r["conv"]), "$" + kfmt(r["spend"]),
                    f"{float(r['vr_permille'] or 0):.1f}‰", f"{float(r['cvr_pct'] or 0):.1f}%",
                    f"{float(r['roas'] or 0):.2f}x"]
            for x, v in zip(xf, vals):
                ax.text(x, y, v, fontsize=11.5, color=NAVY if x == xf[0] else "#222",
                        fontweight="bold" if x == xf[0] else "normal")
        frow(0.798, "P1", hp1)
        frow(0.770, "P2", hp2)

        def pct(a2, b2):
            fa, fb = float(a2 or 0), float(b2 or 0)
            return f"{100*(fa/fb - 1):+.0f}%" if fb else "—"
        dvals = ["change", pct(hp2["imps"], hp1["imps"]), "", "", "",
                 pct(hp2["vr_permille"], hp1["vr_permille"]), pct(hp2["cvr_pct"], hp1["cvr_pct"]),
                 pct(hp2["roas"], hp1["roas"])]
        for x, v in zip(xf, dvals):
            ax.text(x, 0.742, v, fontsize=11, color=RED, fontweight="bold")
        ax.plot([0.03, 0.97], [0.755, 0.755], color="#ddd", lw=1)
        band2_top = 0.688
    else:
        fig.text(0.03, 0.855, "FLAGSHIP YoY — N/A: no prospecting campaign ran in BOTH P1 and P2 "
                 "(all active groups launched in the current period).", fontsize=12, color=GRAY, style="italic")
        band2_top = 0.815

    # --- Band 2: P2 footprint by campaign (ROAS gradient) ---
    footprint_hdr = ("P2 FOOTPRINT — per-campaign delivery & ROAS (national; no geo tiers)"
                     if national_acct else "P2 FOOTPRINT — per-campaign delivery & ROAS by tier")
    fig.text(0.03, band2_top, footprint_hdr, fontsize=13, fontweight="bold", color=NAVY)
    cols2 = ["Campaign", "Tier", "#DMAs", "Launched", "P2 imps", "% mix", "VR", "CVR", "ROAS"]
    xg = [0.03, 0.30, 0.42, 0.50, 0.60, 0.68, 0.755, 0.83, 0.905]
    yr2 = band2_top - 0.037
    for x, c in zip(xg, cols2):
        ax.text(x, yr2, c, fontsize=11, fontweight="bold", color="#444")
    ax.plot([0.03, 0.97], [yr2 - 0.014, yr2 - 0.014], color=NAVY, lw=1.3)
    order = sorted([c for c in camps if (c["grp"], "P2") in metrics],
                   key=lambda c: (TIER_ORDER.index(c["tier"]), -int(metrics[(c["grp"], "P2")]["imps"])))
    # adaptive row pitch for a variable number of campaigns
    rowdy = min(0.0335, (yr2 - 0.14) / max(len(order) + 2, 1))
    y = yr2 - 0.030
    for c in order:
        r = metrics[(c["grp"], "P2")]
        roas = float(r["roas"] or 0)
        rc = GREEN if roas >= 2.0 else (AMBER if roas >= 1.5 else RED)
        nm = c["name"] or c["grp"]
        share = 100 * int(r["imps"]) / b_imps
        cells = [f"{c['grp']} {nm[:22]}", "National" if c["natl"] else c["tier"].title(),
                 "US" if c["natl"] else str(c["n"]), r["first_day"][:7],
                 kfmt(r["imps"]), f"{share:.0f}%", f"{float(r['vr_permille'] or 0):.1f}‰",
                 f"{float(r['cvr_pct'] or 0):.1f}%", f"{roas:.2f}x"]
        for x, v in zip(xg, cells):
            col = rc if v.endswith("x") else "#222"
            ax.text(x, y, v, fontsize=10.4, color=col, va="center",
                    fontweight="bold" if v.endswith("x") else "normal")
        ax.add_patch(plt.Rectangle((0.278, y - 0.009), 0.014, 0.018, color=TIER_COLOR[c["tier"]], alpha=0.85))
        y -= rowdy
    ax.plot([0.03, 0.97], [y + 0.008, y + 0.008], color="#ddd", lw=1)
    y -= 0.016
    bl = [f"BLENDED P2 (all {len(order)})", "", "", "", kfmt(b_imps), "100%",
          f"{b_vr:.1f}‰", f"{b_cvr:.1f}%", f"{b_roas:.2f}x"]
    for x, v in zip(xg, bl):
        ax.text(x, y, v, fontsize=11.0, color=NAVY, fontweight="bold", va="center")

    # takeaway strip
    if flag:
        take = (f"Flagship ROAS {float(hp1['roas'] or 0):.2f}x (P1) -> {float(hp2['roas'] or 0):.2f}x (P2); "
                f"blended P2 across all {len(order)} campaigns = {b_roas:.2f}x. "
                + ("National account — the decline is delivery/gate-driven, not geo fragmentation."
                   if national_acct else "Remainder is geo/variant fragmentation into lower-ROAS pieces."))
    else:
        take = (f"No YoY-comparable flagship; blended P2 ROAS across all {len(order)} campaigns = {b_roas:.2f}x. "
                + ("National account — geo-tiering N/A." if national_acct else ""))
    ax.add_patch(plt.Rectangle((0.03, y - 0.075), 0.94, 0.050, color=NAVY, alpha=0.06))
    ax.text(0.045, y - 0.050, take, fontsize=10.5, color="#222", va="center", style="italic")
    plt.savefig(a.perf_png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.perf_png}")
    plt.close(fig)

    # =====================================================================
    # (C) markdown
    # =====================================================================
    if national_acct:
        lede = ("**Prospecting targets the US nationally (location_id 237) — there is NO population-tier / DMA "
                "slice.** Geo-tiering does not apply to this account; the audience differentiation is by "
                "frequency/variant and a net-new funnel gate, not by geography.")
    else:
        lede = ("**Prospecting slices US Nielsen DMAs into population tiers.** \"High Pop\" is the top markets, "
                "not all-of-US. P1 ran the top tier; P2 fragmented across more DMAs/campaigns.")
    md = [f"# {a.adv} — Geo / DMA tier deep-dive", "", lede, "",
          "## Footprint & performance by campaign", "",
          "| Campaign | Tier | #DMAs | Launched | P2 imps | P2 VR | P2 CVR | P2 ROAS |",
          "|---|---|---:|---|---:|---:|---:|---:|"]
    for c in order:
        r = metrics[(c["grp"], "P2")]
        md.append(f"| {c['grp']} {c['name']} | {'National' if c['natl'] else c['tier'].title()} | "
                  f"{'US' if c['natl'] else c['n']} | {r['first_day'][:7]} | {kfmt(r['imps'])} | "
                  f"{float(r['vr_permille'] or 0):.1f}‰ | {float(r['cvr_pct'] or 0):.1f}% | {float(r['roas'] or 0):.2f}x |")
    if flag:
        md += ["", f"**Flagship ({flag['grp']} {flag['name']}) YoY:** ROAS {float(hp1['roas'] or 0):.2f}x -> "
               f"{float(hp2['roas'] or 0):.2f}x, VR {float(hp1['vr_permille'] or 0):.1f}->{float(hp2['vr_permille'] or 0):.1f}‰, "
               f"CVR {float(hp1['cvr_pct'] or 0):.1f}->{float(hp2['cvr_pct'] or 0):.1f}%.",
               f"**Blended P2 ROAS {b_roas:.2f}x** across all {len(order)} active campaigns.", ""]
    else:
        md += ["", f"**No YoY-comparable flagship** (all active groups launched in P2). "
               f"Blended P2 ROAS {b_roas:.2f}x across {len(order)} campaigns.", ""]
    geo_tiers = [t for t in present_tiers if t != "NATIONAL"]
    if geo_tiers:
        for t in geo_tiers:
            names = tier_named[t]
            md.append(f"## {t} — {len(names)} DMAs  (ranked by {a.deliv_label} delivery)")
            md.append(", ".join(f"{nm} ({kfmt(d)})" if d else nm for nm, d in names) or "(none)")
            md.append("")
    else:
        md += ["## Geo tiers", "N/A — national targeting (all-of-US, location_id 237). No DMA tiers exist.", ""]
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")
    if national_acct:
        n_natl = sum(1 for c in camps if c["natl"])
        print(f"FINDING: NATIONAL account — geo-tiering N/A ({n_natl}/{len(camps)} prospecting campaigns target "
              f"location_id 237 = all-of-US). Blended P2 ROAS {b_roas:.2f}x across {len(order)} campaigns"
              + (f"; flagship {flag['grp']} YoY ROAS {float(hp1['roas'] or 0):.2f}->{float(hp2['roas'] or 0):.2f}x." if flag else "."))
    else:
        print(f"FINDING: geo footprint across {len(order)} campaigns / tiers {geo_tiers}. "
              f"Blended P2 ROAS {b_roas:.2f}x"
              + (f" vs flagship P1 {float(hp1['roas'] or 0):.2f}x." if flag else "."))


if __name__ == "__main__":
    main()
