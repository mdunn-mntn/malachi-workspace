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
import os
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

# GRPNAME / ORDER are derived per-advertiser from group_name at runtime (no hardcoded dicts).
NATIONAL_LOC = "237"


def is_national(inc_ids):
    return len(inc_ids) == 0 or [str(i) for i in inc_ids] == [NATIONAL_LOC]


def geo_incs(e):
    """Return the set of included geo location_ids (for national detection + DMA count)."""
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


def tier_of(inc_ids):
    if is_national(inc_ids):
        return "National · US"
    n = len(inc_ids)
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


def prospecting_spend_by_group(outdir):
    """Prospecting (obj==1) spend per campaign_group_id from <outdir>/00_campaign_enum.csv.
    Advertiser-agnostic: sums each obj==1 campaign's already-summed spend into its group. Groups
    absent from the enum simply won't appear (caller treats missing -> 0 share, sort last)."""
    path = os.path.join(outdir, "00_campaign_enum.csv")
    grp_spend = {}
    if not os.path.exists(path):
        return grp_spend
    for r in csv.DictReader(open(path)):
        try:
            obj = int(r.get("obj") or 0)
        except (TypeError, ValueError):
            obj = 0
        if obj != 1:
            continue
        try:
            sp = float(r.get("spend") or 0)
        except (TypeError, ValueError):
            sp = 0.0
        grp_spend[r["grp"]] = grp_spend.get(r["grp"], 0.0) + sp
    return grp_spend


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


    # ---- derive group order + labels dynamically (no hardcoded dicts) --------
    exprs = {r["campaign_group_id"]: r for r in csv.DictReader(open(a.expr)) if r.get("expression")}

    # overlap CSV (general long format): per-group reach + union_with_base; base = largest-reach group.
    ov = list(csv.DictReader(open(a.overlap)))
    reach = {r["campaign_group_id"]: int(r["reach"]) for r in ov}
    base_grp = ov[0]["base_grp"] if ov else (max(exprs, key=lambda g: reach.get(g, 0)) if exprs else None)
    union_base = {r["campaign_group_id"]: int(r["union_with_base"]) for r in ov}
    # net-new-vs-base per group: intersection = reach_g + reach_base - union; net_new = 1 - inter/reach_g
    base_reach = reach.get(base_grp, 0)

    def net_new_frac(g):
        rg = reach.get(g, 0)
        if not rg or g == base_grp:
            return 0.0
        inter = rg + base_reach - union_base.get(g, rg + base_reach)
        return max(0.0, min(1.0, 1 - inter / rg))

    def short_name(nm):
        return (nm or "").replace("CTV", "").replace("Prospecting", "").replace("2026", "").strip(" -") or nm

    # prospecting-spend share per group (advertiser-agnostic, from the enum next to --expr)
    outdir = os.path.dirname(a.expr) or "."
    grp_spend = prospecting_spend_by_group(outdir)
    tot_spend = sum(grp_spend.get(g, 0.0) for g in exprs) or 1.0  # denom = groups this chart shows

    dna = {}
    for g, r in exprs.items():
        e = json.loads(r["expression"])
        cw = (e.get("categories") or {}).get("where")
        dsc = ds_counts(cw)
        inc = geo_incs(e)
        sp = grp_spend.get(g, 0.0)  # missing from enum -> 0 spend -> 0 share, sorts last
        dna[g] = {"name": short_name(r["group_name"]), "full": (r["group_name"] or "").strip(),
                  "inc": inc, "natl": is_national(inc), "n_dma": (0 if is_national(inc) else len(inc)),
                  "mm": dsc.get(19, 0), "tp": dsc.get(35, 0), "join": join_op(cw),
                  "gate": contains(cw, 16), "reach": reach.get(g, 0),
                  "spend": sp, "sshare": sp / tot_spend}
    # order: by prospecting-spend share desc (biggest spender first; missing-from-enum -> 0, last)
    order = sorted(dna, key=lambda g: (-dna[g]["sshare"], -dna[g]["reach"]))
    any_gate = any(dna[g]["gate"] for g in order)
    all_or = all((dna[g]["join"] in (None, "or")) for g in order)

    # =====================================================================
    # PNG 1 — INTEREST-LOGIC DNA TABLE (adaptive rows)
    # =====================================================================
    n = len(order)
    fig, ax = plt.subplots(figsize=(13.5, 1.2 + 0.55 * max(n, 4)))
    ax.axis("off"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    fig.text(0.03, 0.95, f"{a.adv} — how narrow is each prospecting audience?",
             fontsize=20, fontweight="bold", color="#222")
    sub = ("Interest is additive everywhere (MM OR 3P). " if all_or else "")
    sub += ("Narrowing comes from a net-new funnel gate (DS16), not geography (national account)."
            if any(dna[g]["natl"] for g in order) else
            "Narrowing comes from GEO and a net-new funnel gate.")
    fig.text(0.03, 0.90, sub, fontsize=12.5, color="#444")
    cols = ["Campaign", "% spend", "Geo tier", "Interest", "Funnel gate", "Narrowing flag"]
    xs = [0.03, 0.235, 0.345, 0.505, 0.635, 0.79]
    yt = 0.82
    for x, c in zip(xs, cols):
        ax.text(x, yt, c, fontsize=13.5, fontweight="bold", color=NAVY, va="center")
    top = yt - 0.045
    ax.plot([0.03, 0.985], [top, top], color=NAVY, lw=1.6)
    dy = (top - 0.05) / n
    y = top - dy * 0.6
    maxshare = max((dna[g]["sshare"] for g in order), default=1) or 1
    for i, g in enumerate(order):
        d = dna[g]
        if i % 2 == 0:
            ax.axhspan(y - dy / 2, y + dy / 2, color="#000", alpha=0.035)
        natl = d["natl"]
        geo_col = "#222" if natl else (AMBER if d["n_dma"] <= 25 else (RED if d["n_dma"] >= 120 else "#222"))
        gate_txt = "net-new  (AND'd)" if d["gate"] else "—"
        gate_col = RED if d["gate"] else "#999"
        if d["gate"]:
            flag, fcol = "narrow · net-new funnel gate", RED
        elif not natl and d["n_dma"] <= 25:
            flag, fcol = f"narrow geo · {d['n_dma']}/210", AMBER
        elif not natl and d["n_dma"] >= 120:
            flag, fcol = f"thin · long-tail {d['n_dma']}/210", RED
        else:
            flag, fcol = "clean · ungated", GREEN
        ax.text(xs[0], y, f"{g}  {d['name'][:14]}", fontsize=12.5, va="center", color="#222", fontweight="bold")
        # % of prospecting spend: the number + a proportional weight bar so materiality reads at a glance
        ax.text(xs[1], y, f"{d['sshare']*100:.0f}%", fontsize=12.5, va="center", color=NAVY, fontweight="bold")
        bar_w = 0.055 * d["sshare"] / maxshare
        ax.add_patch(plt.Rectangle((xs[1] + 0.04, y - 0.006), bar_w, 0.012, color=NAVY, alpha=0.5, zorder=1))
        ax.text(xs[2], y, tier_of(d["inc"]), fontsize=12.5, va="center", color=geo_col,
                fontweight="bold" if geo_col != "#222" else "normal")
        ax.text(xs[3], y, f"MM {(d['join'] or '?').upper()} 3P", fontsize=12.5, va="center", color=GREEN)
        ax.text(xs[4], y, gate_txt, fontsize=12.5, va="center", color=gate_col,
                fontweight="bold" if d["gate"] else "normal")
        ax.text(xs[5], y, flag, fontsize=12, va="center", color=fcol,
                fontweight="bold" if fcol in (RED, AMBER) else "normal")
        y -= dy
    plt.savefig(a.dna_png, dpi=200, bbox_inches="tight")
    print(f"wrote {a.dna_png}")
    plt.close(fig)

    # =====================================================================
    # PNG 2 — FUNNEL-GATE EVIDENCE (base reach vs gated variants + shift timeline)
    # =====================================================================
    gated = [g for g in order if dna[g]["gate"] and g != base_grp]
    # show the base plus up to 6 largest-reach gated variants
    show_vars = sorted(gated, key=lambda g: -dna[g]["reach"])[:6]

    mon_rows = list(csv.DictReader(open(a.monthly)))
    months = sorted({r["mon"] for r in mon_rows})

    def series(grps):
        gs = set(grps)
        return [sum(int(r["imps"]) for r in mon_rows if r["mon"] == m and r["grp"] in gs) for m in months]
    base_s = series({base_grp})
    var_s = series(set(gated)) if gated else [0] * len(months)

    fig = plt.figure(figsize=(15, 7.6))
    fig.text(0.03, 0.955, f"{a.adv} — prospecting shifted impressions onto narrower gated audiences",
             fontsize=16.5, fontweight="bold", color="#222")
    if show_vars:
        avg_nn = sum(net_new_frac(g) for g in show_vars) / len(show_vars)
        base_desc = "gated" if dna[base_grp]["gate"] else "ungated"
        fig.text(0.03, 0.912, f"The broad base ({kfmt(base_reach)} households) is {base_desc}. The other gated variants each "
                 f"reach a smaller pool and run ~{avg_nn*100:.0f}% NET-NEW vs the base — the residual the base already skipped.",
                 fontsize=11.5, color="#444")
    else:
        fig.text(0.03, 0.912, "No gated variants distinct from the base — interest is additive and ungated across the account.",
                 fontsize=11.5, color="#444")

    # left: reach bars — base vs gated variants
    axL = fig.add_axes([0.075, 0.28, 0.36, 0.52])
    bar_g = [base_grp] + show_vars
    base_tag = " (broad, gated)" if dna[base_grp]["gate"] else " (broad, ungated)"
    labs = [(dna[base_grp]["name"][:18] + "\n" + base_tag.strip())] + \
           [dna[g]["name"][:18] + "\n(gated)" for g in show_vars]
    vals = [dna[g]["reach"] for g in bar_g]
    cols_b = [NAVY] + [RED] * len(show_vars)
    ypos = list(range(len(vals)))[::-1]
    axL.barh(ypos, vals, color=cols_b, height=0.66)
    for yp, v in zip(ypos, vals):
        axL.text(v + max(vals) * 0.015, yp, kfmt(v), va="center", fontsize=11, fontweight="bold", color="#222")
    axL.set_yticks(ypos); axL.set_yticklabels(labs, fontsize=9.5)
    axL.set_xlim(0, max(vals) * 1.20); axL.set_xticks([])
    for sp in ["top", "right", "bottom"]:
        axL.spines[sp].set_visible(False)
    axL.set_title(f"Households each audience reaches ({a.adv.split('(')[0].strip()})",
                  fontsize=12, color="#222", loc="left", pad=10, fontweight="bold")

    # right: shift timeline (base vs Σ gated)
    axR = fig.add_axes([0.57, 0.28, 0.39, 0.52])
    mlab = [m[2:] for m in months]
    axR.plot(mlab, base_s, "-o", color=NAVY, lw=2.8, ms=6,
             label="broad base (" + ("gated)" if dna[base_grp]["gate"] else "ungated)"))
    if gated:
        axR.plot(mlab, var_s, "-o", color=RED, lw=2.8, ms=6, label="gated variants (Σ)")
    axR.set_title("Monthly impressions: broad base vs gated variants", fontsize=12, color="#222",
                  loc="left", pad=8, fontweight="bold")
    axR.set_ylabel("impressions", fontsize=10, color="#444")
    axR.legend(frameon=False, fontsize=9.5, loc="upper center")
    for sp in ["top", "right"]:
        axR.spines[sp].set_visible(False)
    axR.tick_params(labelsize=9.5)

    # bottom explanation
    if show_vars:
        fig.text(0.05, 0.10, f"The switch: delivery moved from the broad base onto {len(gated)} gated variant(s), "
                 f"each ~{avg_nn*100:.0f}% NET-NEW households (the leftover pool the broad base already skipped).",
                 fontsize=11.2, color="#222", fontweight="bold")
        fig.text(0.05, 0.05, "So more impressions land on narrower, lower-quality audiences — the residual the base "
                 "skipped. The narrowing is by WHO (net-new households), not by 3P.", fontsize=10.6, color="#333")
    plt.savefig(a.gate_png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.gate_png}")
    plt.close(fig)

    # =====================================================================
    # MD
    # =====================================================================
    join_set = "/".join(sorted(set((dna[g]["join"] or "?") for g in order)))
    lede = ("**No campaign narrows MM with a required 3P segment.** " if all_or else
            "**Some campaigns AND a required 3P onto MM — investigate.** ")
    lede += (f"MM×3P join across campaigns: {join_set.upper()} (OR = additive/broadening). "
             + ("The differentiator is a DS16 net-new funnel gate on the gated campaigns, not 3P."
                if any_gate else "No net-new funnel gate present."))
    md = [f"# {a.adv} — Interest-logic deep-dive (targeting DNA + funnel gate)", "", lede, "",
          "## Per-campaign targeting DNA (ranked by % of prospecting spend)", "",
          "| Campaign | % spend | Geo tier | MM kw | 3P seg | MM×3P | Funnel gate (DS16) | Reach | Net-new vs base | Read |",
          "|---|--:|---|---:|---:|---|---|---:|---:|---|"]
    for g in order:
        d = dna[g]
        gate = "**net-new (AND'd)**" if d["gate"] else "—"
        if d["gate"]:
            read = "net-new residual gate"
        elif g == base_grp:
            read = "broad · ungated (base)"
        elif d["natl"]:
            read = "national · ungated"
        else:
            read = "geo slice"
        nn = f"{net_new_frac(g)*100:.0f}%" if g != base_grp and d["reach"] else "—"
        md.append(f"| {g} {d['full']} | {d['sshare']*100:.0f}% | {tier_of(d['inc'])} | {d['mm']} | {d['tp']} | "
                  f"{(d['join'] or '?').upper()} | {gate} | {kfmt(d['reach'])} | {nn} | {read} |")
    md += ["", "## The differentiator — DS16 net-new funnel gate",
           "`AND ( NOT DS16[own Impressions/Wins]  OR  DS16[own campaign-group tag] )` — decoded via "
           "`tpa.categories` (data_source_id=16 = the advertiser's own funnel). Target a household **iff** it was "
           "NEVER impressed/won by this advertiser **OR** is already owned by this campaign = a **net-new-reach gate**.",
           "",
           "## Empirical reach & net-new (BQ-native HLL on `sum_by_campaign_by_day`)",
           "",
           "| Metric | Value |",
           "|---|---|",
           f"| Base ({base_grp} {dna.get(base_grp,{}).get('name','')}) reach | {base_reach:,} distinct households |"]
    if gated:
        nn_list = [net_new_frac(g) for g in gated if dna[g]["reach"]]
        avg_nn = sum(nn_list) / len(nn_list) if nn_list else 0
        sm = min((dna[g]["reach"] for g in gated), default=0)
        lg = max((dna[g]["reach"] for g in gated), default=0)
        md += [f"| Gated-variant reach | {kfmt(sm)}–{kfmt(lg)} ({len(gated)} gated campaigns) |",
               f"| Base ∩ gated variant | **~{avg_nn*100:.0f}% net-new vs base** (avg across gated) |"]
    md += ["",
           "**Read:** the gate narrows by WHO (net-new households), not by 3P — gated campaigns fish the "
           "residual net-new pool the ungated base already skipped.", ""]
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")
    n_gated = len(gated)
    gate_note = (f"{n_gated}/{n} carry a DS16 net-new funnel gate" if any_gate else "no net-new gate")
    print(f"FINDING: interest join = {join_set.upper()} across {n} campaigns "
          f"({'all additive (OR)' if all_or else 'AND-narrowing present'}); {gate_note}. "
          + (f"Base {base_grp} reaches {kfmt(base_reach)}; gated variants run net-new vs base — "
             "narrowing is by WHO (net-new households), not by 3P." if any_gate else ""))


if __name__ == "__main__":
    main()
