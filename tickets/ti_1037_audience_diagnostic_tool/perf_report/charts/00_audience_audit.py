"""Module 00 — SYSTEMATIC AUDIENCE AUDIT (report front matter — ONE coherent artifact).

Runs FIRST. Advertiser-agnostic: inventories EVERY active campaign, classifies stage by
objective_id, decodes every audience expression, folds the prospecting audience FUNNEL
(addressable -> reached -> HI) + flags into ONE front-matter figure with an adaptive layout.
  · TOP    — STAGE MAP: where impressions / spend / conversions / revenue go.
  · BOTTOM — PROSPECTING AUDIENCE: every obj=1 campaign — targeting DNA + funnel
             (Reached · HI-share · Coverage) + flags (narrow/thin geo · net-new gate ·
             MM-AND-3P · low-HI-share · high-unscored · dark).

Reads  <outdir>/00_campaign_enum.csv · 00_all_expressions.csv · 00_funnel_sizes.csv · 00_funnel_hishare.csv
Writes <outdir>/00_audience_audit.png · 00_audience_audit.md
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
NAVY, GREEN, RED, AMBER, GRAY = "#27496D", "#2E8B57", "#D63B2F", "#C77B30", "#888888"

# Multi-Touch S2+S3 grouped together (obj 5 & 6); the stage-1 expression is what the audit reads.
STAGE = {1: "Prospecting", 4: "Retargeting", 5: "Multi-Touch", 6: "Multi-Touch", 7: "Ego"}
STAGE_ORDER = ["Prospecting", "Retargeting", "Multi-Touch", "Ego"]


US_LOC = 237  # geo.location_data location_id 237 = "United States" (country/national, type 2)


def geo_class(e):
    """Classify the geo whitelist: national (US, type 2) vs a DMA slice (type 4, ids ~461-672)."""
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
    ids = set(inc)
    n_dma = len([i for i in ids if 461 <= i <= 672])
    return {"n_dma": n_dma, "national": (US_LOC in ids and n_dma == 0), "n_ids": len(ids)}


def walk_ds(node, neg, out):
    if isinstance(node, list):
        [walk_ds(x, neg, out) for x in node]
    elif isinstance(node, dict):
        op, v = node.get("op"), node.get("value")
        if isinstance(v, dict) and "data_source_id" in v:
            ds = v["data_source_id"]
            out.setdefault(ds, {"inc": 0, "exc": 0})["exc" if neg else "inc"] += max(1, len(v.get("category_ids") or []))
        elif v is not None:
            walk_ds(v, neg ^ (op == "not"), out)


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

    def walk(n, d):
        if isinstance(n, dict):
            op, v = n.get("op"), n.get("value")
            if op in ("and", "or") and v is not None and contains(v, a) and contains(v, b) and d > best[1]:
                best[0], best[1] = op, d
            if v is not None:
                walk(v, d + 1)
        elif isinstance(n, list):
            [walk(c, d + 1) for c in n]
    walk(node, 0)
    return best[0]


def kfmt(v):
    if v is None:
        return "—"
    v = float(v)
    if abs(v) >= 1e6:
        return f"{v/1e6:.1f}M"
    return f"{v/1e3:.0f}K" if abs(v) >= 1e3 else f"{v:.0f}"


def dollar(v):
    return "$" + kfmt(v)


def tier(n):
    if not n:
        return "—"
    return "top-20" if n <= 25 else ("mid" if n <= 80 else f"long-{n}")


def glabel(gname, cid):
    s = (gname or "").replace("CTV ", "").replace("Prospecting", "").replace(" 2026", "").replace("-old", " (old)")
    s = " ".join(s.split()).strip()
    return f"{cid} {s}" if s else f"{cid}"


def geo_txt(p):
    return "national" if p["national"] else (f"{p['ngeo']}/210" if p["ngeo"] else "—")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="outputs/kindred_35094")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()
    o = a.outdir.rstrip("/")
    csv.field_size_limit(10 ** 7)

    enum = list(csv.DictReader(open(f"{o}/00_campaign_enum.csv")))
    expr = {r["campaign_id"]: r["expression"] for r in csv.DictReader(open(f"{o}/00_all_expressions.csv")) if r.get("expression")}
    sizes = {r["campaign_id"]: r for r in csv.DictReader(open(f"{o}/00_funnel_sizes.csv"))} if os.path.exists(f"{o}/00_funnel_sizes.csv") else {}
    hishare = {r["campaign_id"]: r for r in csv.DictReader(open(f"{o}/00_funnel_hishare.csv"))} if os.path.exists(f"{o}/00_funnel_hishare.csv") else {}
    for r in enum:
        r["obj"] = int(r["obj"])
        r["stage"] = STAGE.get(r["obj"], f"obj{r['obj']}")
        for k in ("imps", "conv", "spend", "revenue"):
            r[k] = float(r[k] or 0)

    smap = {}
    for r in enum:
        s = smap.setdefault(r["stage"], {"imps": 0, "spend": 0, "conv": 0, "revenue": 0, "n": 0})
        for k in ("imps", "spend", "conv", "revenue"):
            s[k] += r[k]
        s["n"] += 1
    tot = {k: sum(s[k] for s in smap.values()) for k in ("imps", "spend", "conv", "revenue")}

    prosp = []
    for r in sorted([x for x in enum if x["obj"] == 1], key=lambda x: -x["imps"]):
        cid = r["campaign_id"]
        e = json.loads(expr[cid]) if cid in expr else {}
        cw = (e.get("categories") or {}).get("where")
        ds = {}
        walk_ds(cw, False, ds)
        gc = geo_class(e)
        ngeo, national = gc["n_dma"], gc["national"]
        jop = join_op(cw) if (19 in ds and 35 in ds) else None
        gate = contains(cw, 16)
        h = hishare.get(cid)
        reach = int(h["reach_ip"]) if h else None
        hi = int(h["hi_ip"]) if h else None
        unsc = int(h["unscored_ip"]) if h else None
        hs = (hi / reach) if (reach and h) else None
        us = (unsc / reach) if (reach and h) else None
        addr = int(sizes[cid]["med_total"]) if cid in sizes else None
        cov = (reach / (addr / 5)) if (reach and addr) else None
        roas = r["revenue"] / r["spend"] if r["spend"] else 0
        interest = "MM " + ((jop or "?").upper()) + " 3P" if (19 in ds and 35 in ds) else \
            ("MM only" if 19 in ds else ("3P only" if 35 in ds else "—"))
        flags = []
        if jop == "and":
            flags.append(("MM AND 3P", RED))
        if gate:
            flags.append(("net-new gate", RED))
        if 35 in ds and 19 not in ds:
            flags.append(("3P-only", AMBER))
        if 2 <= ngeo <= 25:
            flags.append((f"narrow geo {ngeo}/210", AMBER))
        if ngeo >= 120:
            flags.append((f"thin geo {ngeo}/210", AMBER))
        if hs is not None and hs < 0.70:
            flags.append((f"low HI {hs*100:.0f}%", RED))
        if us is not None and us > 0.30:
            flags.append((f"unscored {us*100:.0f}%", AMBER))
        if h is None:
            flags.append(("dark (F1 stopped)", GRAY))
        prosp.append({"cid": cid, "grp": r["grp"], "label": glabel(r.get("group_name"), cid),
                      "ngeo": ngeo, "national": national, "interest": interest, "gate": gate, "reach": reach,
                      "hs": hs, "us": us, "cov": cov, "roas": roas, "addr": addr, "flags": flags})

    # =====================================================================
    # ONE FIGURE — adaptive height (stage map + prospecting audit)
    # =====================================================================
    stages = [s for s in STAGE_ORDER if s in smap]
    RH = 0.34
    H = 0.55 + 0.42 + 0.60 + 0.34 + 0.34 + (len(stages) + 1) * RH + 0.85 + 0.34 + 0.34 + len(prosp) * RH + 0.55
    fig = plt.figure(figsize=(15, H))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    def Y(inch):
        return 1 - inch / H
    cy = 0.55
    fig.text(0.03, Y(cy), f"{a.adv} — audience audit", fontsize=21, fontweight="bold", color="#222")
    cy += 0.42
    rt = smap.get("Retargeting", {})
    rroas = rt["revenue"] / rt["spend"] if rt.get("spend") else 0
    fig.text(0.03, Y(cy), f"Retargeting is the revenue engine ({rroas:.0f}x, {rt.get('revenue',0)/max(tot['revenue'],1)*100:.0f}% "
             f"of revenue). Prospecting — the focus below — audited per campaign with its audience funnel.",
             fontsize=12.5, color="#444")
    cy += 0.60

    # STAGE MAP
    ax.text(0.03, Y(cy), "WHERE THE MONEY GOES  ·  all stages (stage = objective_id; each group is a full funnel)",
            fontsize=13, fontweight="bold", color=NAVY)
    cy += 0.34
    c1 = ["Stage", "Camps", "Impressions", "Spend", "Conversions", "Revenue", "ROAS"]
    x1 = [0.03, 0.25, 0.35, 0.50, 0.61, 0.75, 0.90]
    for x, c in zip(x1, c1):
        ax.text(x, Y(cy), c, fontsize=12, fontweight="bold", color="#555", va="center")
    cy += 0.12
    ax.plot([0.03, 0.97], [Y(cy), Y(cy)], color=NAVY, lw=1.3)
    cy += 0.22
    for st in stages:
        s = smap[st]
        roas = s["revenue"] / s["spend"] if s["spend"] else 0
        share = s["revenue"] / tot["revenue"] * 100 if tot["revenue"] else 0
        rc = GREEN if roas >= 5 else (NAVY if roas >= 1.8 else RED)
        hot = st == "Retargeting"
        cells = [st, str(s["n"]), kfmt(s["imps"]), dollar(s["spend"]), f"{s['conv']:,.0f}",
                 f"{dollar(s['revenue'])} ({share:.0f}%)", f"{roas:.1f}x"]
        for x, t in zip(x1, cells):
            ax.text(x, Y(cy), t, fontsize=12, va="center",
                    color=(rc if x == x1[6] else (NAVY if hot else "#222")),
                    fontweight="bold" if (x == x1[0] or x == x1[6] or hot) else "normal")
        cy += RH
    ax.plot([0.03, 0.97], [Y(cy - 0.14), Y(cy - 0.14)], color="#ccc", lw=1)
    troas = tot["revenue"] / tot["spend"] if tot["spend"] else 0
    for x, t in zip(x1, ["TOTAL", str(len(enum)), kfmt(tot["imps"]), dollar(tot["spend"]),
                         f"{tot['conv']:,.0f}", dollar(tot["revenue"]), f"{troas:.1f}x"]):
        ax.text(x, Y(cy), t, fontsize=12, va="center", color=NAVY, fontweight="bold")
    cy += RH + 0.55

    # PROSPECTING AUDIT
    ax.text(0.03, Y(cy), f"PROSPECTING AUDIENCE  ·  all {len(prosp)} obj=1 campaigns — targeting + funnel + flags",
            fontsize=13, fontweight="bold", color=NAVY)
    cy += 0.34
    c2 = ["Campaign", "Geo", "Interest", "Gate", "Reached", "HI-share", "ROAS", "Flags"]
    x2 = [0.03, 0.235, 0.315, 0.42, 0.50, 0.575, 0.665, 0.735]
    for x, c in zip(x2, c2):
        ax.text(x, Y(cy), c, fontsize=12, fontweight="bold", color="#555", va="center")
    cy += 0.12
    ax.plot([0.03, 0.985], [Y(cy), Y(cy)], color=NAVY, lw=1.3)
    cy += 0.22
    for i, p in enumerate(prosp):
        if i % 2 == 0:
            ax.axhspan(Y(cy + RH / 2 - 0.03), Y(cy - RH / 2 + 0.03), color="#000", alpha=0.03)
        gcol = AMBER if (2 <= p["ngeo"] <= 25 or p["ngeo"] >= 120) else "#222"
        gate_txt, gate_col = ("net-new", RED) if p["gate"] else ("—", GRAY)
        roas_col = GREEN if p["roas"] >= 2.2 else (NAVY if p["roas"] >= 1.5 else (RED if p["roas"] else GRAY))
        if p["hs"] is None:
            hs_txt, hs_col = "dark", GRAY
        else:
            hs_txt = f"{p['hs']*100:.0f}%"
            hs_col = GREEN if p["hs"] >= 0.80 else (AMBER if p["hs"] >= 0.60 else RED)
        ax.text(x2[0], Y(cy), p["label"][:30], fontsize=10.5, va="center", color="#222", fontweight="bold")
        ax.text(x2[1], Y(cy), geo_txt(p), fontsize=10.5, va="center", color=gcol,
                fontweight="bold" if gcol == AMBER else "normal")
        ax.text(x2[2], Y(cy), p["interest"], fontsize=10, va="center",
                color=GREEN if "MM" in p["interest"] else (AMBER if "3P only" in p["interest"] else GRAY))
        ax.text(x2[3], Y(cy), gate_txt, fontsize=10.5, va="center", color=gate_col,
                fontweight="bold" if p["gate"] else "normal")
        ax.text(x2[4], Y(cy), kfmt(p["reach"]) if p["reach"] else "dark", fontsize=10.5, va="center",
                color="#222" if p["reach"] else GRAY)
        ax.text(x2[5], Y(cy), hs_txt, fontsize=10.5, va="center", color=hs_col, fontweight="bold")
        ax.text(x2[6], Y(cy), f"{p['roas']:.2f}x" if p["roas"] else "—", fontsize=10.5, va="center",
                color=roas_col, fontweight="bold")
        fl = ", ".join(f[0] for f in p["flags"]) if p["flags"] else "—"
        fcol = RED if any(f[1] == RED for f in p["flags"]) else (AMBER if p["flags"] else GRAY)
        ax.text(x2[7], Y(cy), fl[:44], fontsize=9, va="center", color=fcol)
        cy += RH
    ax.text(0.03, Y(cy + 0.05), "Reached / HI-share exact (CIL, recent in-TTL month); HI-share = of reached households, "
            "score ≥ 8001. Low HI-share or high-unscored = reaching low-intent supply.", fontsize=9.3, color=GRAY, va="center")
    plt.savefig(f"{o}/00_audience_audit.png", dpi=200, bbox_inches="tight")
    print(f"wrote {o}/00_audience_audit.png")
    plt.close(fig)

    # ---- MD ----
    md = [f"# {a.adv} — systematic audience audit", "",
          "## Where delivery & revenue go, by stage", "",
          "| Stage | Camps | Impressions | Spend | Conv | Revenue | ROAS |", "|---|--:|--:|--:|--:|--:|--:|"]
    for st in stages:
        s = smap[st]
        roas = s["revenue"] / s["spend"] if s["spend"] else 0
        md.append(f"| {st} | {s['n']} | {kfmt(s['imps'])} | {dollar(s['spend'])} | {s['conv']:,.0f} | "
                  f"{dollar(s['revenue'])} | {roas:.1f}x |")
    md += ["", "**Structural:** each campaign group is a full funnel (stage = `objective_id`); group-level metrics conflate "
           "stages. Retargeting is the engine.", "",
           "## Prospecting audience — targeting + funnel + flags", "",
           "| Campaign | Geo | Interest | Gate | Reached | HI-share | Coverage | ROAS | Flags |",
           "|---|---|---|---|--:|--:|--:|--:|---|"]
    for p in prosp:
        fl = ", ".join(f[0] for f in p["flags"]) or "—"
        reach_s = kfmt(p["reach"]) if p["reach"] else "— dark"
        hs_s = f"{p['hs']*100:.0f}%" if p["hs"] is not None else "—"
        cov_s = f"{p['cov']*100:.0f}%" if p["cov"] is not None else "—"
        geo_s = "national" if p["national"] else (f"{p['ngeo']}/210 ({tier(p['ngeo'])})" if p["ngeo"] else "—")
        md.append(f"| {p['label']} | {geo_s} | {p['interest']} | "
                  f"{'net-new' if p['gate'] else '—'} | {reach_s} | {hs_s} | {cov_s} | {p['roas']:.2f}x | {fl} |")
    open(f"{o}/00_audience_audit.md", "w").write("\n".join(md) + "\n")
    print(f"wrote {o}/00_audience_audit.md")
    lohi = sum(1 for p in prosp if p["hs"] is not None and p["hs"] < 0.70)
    print(f"FINDING: {len(enum)} campaigns / {len(stages)} stages. Retargeting {rroas:.0f}x "
          f"({rt.get('revenue',0)/max(tot['revenue'],1)*100:.0f}% rev). {len(prosp)} prospecting campaigns audited; "
          f"{lohi} flagged low-HI (<70%).")


if __name__ == "__main__":
    main()
