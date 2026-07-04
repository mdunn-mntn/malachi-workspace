"""Module 00 — SYSTEMATIC AUDIENCE AUDIT (report front matter — ONE coherent artifact).

Runs FIRST. Inventories EVERY active campaign, classifies stage by objective_id, decodes every
audience expression, and folds the prospecting audience FUNNEL (addressable -> reached -> HI) and
its flags into one front-matter figure:
  00_audience_audit.png
    · TOP  — STAGE MAP: where impressions / spend / conversions / revenue go (retargeting = engine).
    · BOTTOM — PROSPECTING AUDIENCE: every obj=1 campaign, its targeting DNA + funnel (reached,
      HI-share, coverage) + flags (narrow/thin geo · net-new gate · MM-AND-3P · low HI-share · dark).

Reads  00_campaign_enum.csv · 00_all_expressions.csv · 00_funnel_sizes.csv · 00_funnel_hishare.csv
Writes 00_audience_audit.png · 00_audience_audit.md
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
NAVY, GREEN, RED, AMBER, GRAY = "#27496D", "#2E8B57", "#D63B2F", "#C77B30", "#888888"

STAGE = {1: "Prospecting", 4: "Retargeting", 5: "Multi-Touch S2", 6: "Multi-Touch S3", 7: "Ego"}
STAGE_ORDER = ["Prospecting", "Retargeting", "Multi-Touch S2", "Multi-Touch S3", "Ego"]
DSROLE = {19: ("MM", "interest"), 13: ("MM-vert", "interest"), 35: ("3P", "interest"),
          8: ("1P/IP", "interest"), 4: ("CRM", "interest"), 16: ("funnel-tag", "gate"),
          21: ("conv", "gate"), 34: ("funnel", "gate"), 2: ("seg", "gate"),
          14: ("bidder", "plumbing"), 47: ("CRM-excl", "exclusion")}


def geo_inc(e):
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
    return "top-20" if n <= 25 else ("mid-38" if n <= 80 else f"low-{n}")


def main():
    ap = argparse.ArgumentParser()
    base = "outputs/kindred_35094/"
    ap.add_argument("--enum", default=base + "00_campaign_enum.csv")
    ap.add_argument("--expr", default=base + "00_all_expressions.csv")
    ap.add_argument("--sizes", default=base + "00_funnel_sizes.csv")
    ap.add_argument("--hishare", default=base + "00_funnel_hishare.csv")
    ap.add_argument("--png", default=base + "00_audience_audit.png")
    ap.add_argument("--md", default=base + "00_audience_audit.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()
    csv.field_size_limit(10 ** 7)

    enum = list(csv.DictReader(open(a.enum)))
    expr = {r["campaign_id"]: r["expression"] for r in csv.DictReader(open(a.expr)) if r.get("expression")}
    sizes = {r["campaign_id"]: r for r in csv.DictReader(open(a.sizes))}
    hishare = {r["campaign_id"]: r for r in csv.DictReader(open(a.hishare))}
    for r in enum:
        r["obj"] = int(r["obj"])
        r["stage"] = STAGE.get(r["obj"], f"obj{r['obj']}")
        for k in ("imps", "conv", "spend", "revenue"):
            r[k] = float(r[k] or 0)

    # ---- stage aggregates ----
    smap = {}
    for r in enum:
        s = smap.setdefault(r["stage"], {"imps": 0, "spend": 0, "conv": 0, "revenue": 0, "n": 0})
        for k in ("imps", "spend", "conv", "revenue"):
            s[k] += r[k]
        s["n"] += 1
    tot = {k: sum(s[k] for s in smap.values()) for k in ("imps", "spend", "conv", "revenue")}

    # ---- prospecting (obj=1) audit rows, with funnel folded in ----
    prosp = []
    for r in sorted([x for x in enum if x["obj"] == 1], key=lambda x: -x["imps"]):
        cid = r["campaign_id"]
        e = json.loads(expr[cid]) if cid in expr else {}
        cw = (e.get("categories") or {}).get("where")
        ds = {}
        walk_ds(cw, False, ds)
        ngeo = geo_inc(e)
        jop = join_op(cw) if (19 in ds and 35 in ds) else None
        gate = contains(cw, 16)
        h = hishare.get(cid)
        reach = int(h["reach_ip"]) if h else None
        hi = int(h["hi_ip"]) if h else None
        hs = (hi / reach) if (reach and h) else None
        addr = int(sizes[cid]["med_total"]) if cid in sizes else None
        cov = (reach / (addr / 5)) if (reach and addr) else None
        roas = r["revenue"] / r["spend"] if r["spend"] else 0
        flags = []
        if jop == "and":
            flags.append(("MM AND 3P", RED))
        if gate:
            flags.append(("net-new gate", RED))
        if 35 in ds and 19 not in ds:
            flags.append(("3P-only", AMBER))
        if ngeo and ngeo <= 25:
            flags.append((f"narrow geo {ngeo}/210", AMBER))
        if ngeo >= 120:
            flags.append((f"thin geo {ngeo}/210", AMBER))
        if hs is not None and hs < 0.70:
            flags.append((f"low HI {hs*100:.0f}%", RED))
        if h is None:
            flags.append(("dark (F1 stopped)", GRAY))
        prosp.append({"cid": cid, "grp": r["grp"], "ngeo": ngeo, "tier": tier(ngeo) if ngeo else "—",
                      "jop": jop, "gate": gate, "reach": reach, "hs": hs, "cov": cov, "roas": roas,
                      "imps": r["imps"], "addr": addr, "flags": flags})

    # =====================================================================
    # ONE FIGURE — stage map (top) + prospecting audience audit (bottom)
    # =====================================================================
    fig, ax = plt.subplots(figsize=(14.5, 9.0))
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.text(0.03, 0.965, f"{a.adv} — audience audit", fontsize=21, fontweight="bold", color="#222")
    rt = smap.get("Retargeting", {})
    rroas = rt["revenue"] / rt["spend"] if rt.get("spend") else 0
    fig.text(0.03, 0.930, f"Retargeting is the revenue engine ({rroas:.0f}x, {rt.get('revenue',0)/tot['revenue']*100:.0f}% of "
             f"revenue). Prospecting — the focus below — reaches ~85% High-Intent but converts down YoY.",
             fontsize=12.5, color="#444")

    # ---------- SECTION 1: STAGE MAP ----------
    ax.text(0.03, 0.882, "WHERE THE MONEY GOES  ·  all stages (stage = objective_id; each group is a full funnel)",
            fontsize=13, fontweight="bold", color=NAVY)
    c1 = ["Stage", "Camps", "Impressions", "Spend", "Conversions", "Revenue", "ROAS"]
    x1 = [0.03, 0.25, 0.35, 0.50, 0.61, 0.75, 0.90]
    for x, c in zip(x1, c1):
        ax.text(x, 0.852, c, fontsize=12, fontweight="bold", color="#555", va="center")
    ax.plot([0.03, 0.97], [0.836, 0.836], color=NAVY, lw=1.3)
    y = 0.806
    for st in STAGE_ORDER:
        if st not in smap:
            continue
        s = smap[st]
        roas = s["revenue"] / s["spend"] if s["spend"] else 0
        share = s["revenue"] / tot["revenue"] * 100 if tot["revenue"] else 0
        rc = GREEN if roas >= 5 else (NAVY if roas >= 1.8 else RED)
        hot = st == "Retargeting"
        cells = [st, str(s["n"]), kfmt(s["imps"]), dollar(s["spend"]), f"{s['conv']:,.0f}",
                 f"{dollar(s['revenue'])} ({share:.0f}%)", f"{roas:.1f}x"]
        for x, t in zip(x1, cells):
            ax.text(x, y, t, fontsize=12, va="center",
                    color=(rc if x == x1[6] else ("#222" if not hot else NAVY)),
                    fontweight="bold" if (x == x1[0] or x == x1[6] or hot) else "normal")
        y -= 0.036
    ax.plot([0.03, 0.97], [y + 0.016, y + 0.016], color="#ccc", lw=1)
    troas = tot["revenue"] / tot["spend"] if tot["spend"] else 0
    for x, t in zip(x1, ["TOTAL", str(len(enum)), kfmt(tot["imps"]), dollar(tot["spend"]),
                         f"{tot['conv']:,.0f}", dollar(tot["revenue"]), f"{troas:.1f}x"]):
        ax.text(x, y - 0.008, t, fontsize=12, va="center", color=NAVY, fontweight="bold")

    # ---------- SECTION 2: PROSPECTING AUDIENCE AUDIT + FUNNEL ----------
    ax.text(0.03, 0.545, "PROSPECTING AUDIENCE  ·  every obj=1 campaign — targeting + funnel + flags",
            fontsize=13, fontweight="bold", color=NAVY)
    c2 = ["Campaign", "Geo", "Interest", "Gate", "Reached", "HI-share", "ROAS", "Flags"]
    x2 = [0.03, 0.175, 0.275, 0.40, 0.50, 0.585, 0.68, 0.755]
    for x, c in zip(x2, c2):
        ax.text(x, 0.513, c, fontsize=12, fontweight="bold", color="#555", va="center")
    ax.plot([0.03, 0.97], [0.497, 0.497], color=NAVY, lw=1.3)
    y = 0.463
    dy = 0.047
    NAMES = {"261318": "High Pop (base)", "540723": "Mid Pop", "463188": "Low Pop",
             "576256": "HiPop Harter", "576267": "HiPop Motherhood", "576276": "HiPop Mom-Focus"}
    for i, p in enumerate(prosp):
        if i % 2 == 0:
            ax.axhspan(y - dy / 2 + 0.004, y + dy / 2 + 0.004, color="#000", alpha=0.03)
        nm = NAMES.get(p["cid"], p["cid"])
        gcol = AMBER if (p["ngeo"] and p["ngeo"] <= 25) else (AMBER if p["ngeo"] >= 120 else "#222")
        gate_txt, gate_col = ("net-new", RED) if p["gate"] else ("—", GRAY)
        roas_col = GREEN if p["roas"] >= 2.2 else (NAVY if p["roas"] >= 1.5 else (RED if p["roas"] else GRAY))
        if p["hs"] is None:
            hs_txt, hs_col = "dark", GRAY
        else:
            hs_txt = f"{p['hs']*100:.0f}%"
            hs_col = GREEN if p["hs"] >= 0.80 else (AMBER if p["hs"] >= 0.70 else RED)
        ax.text(x2[0], y, f"{p['grp']} {nm}", fontsize=11, va="center", color="#222", fontweight="bold")
        ax.text(x2[1], y, f"{p['ngeo']}/210", fontsize=11, va="center", color=gcol,
                fontweight="bold" if gcol == AMBER else "normal")
        ax.text(x2[2], y, f"MM {(p['jop'] or '?').upper()} 3P", fontsize=11, va="center", color=GREEN)
        ax.text(x2[3], y, gate_txt, fontsize=11, va="center", color=gate_col,
                fontweight="bold" if p["gate"] else "normal")
        ax.text(x2[4], y, kfmt(p["reach"]) if p["reach"] else "— dark", fontsize=11, va="center",
                color="#222" if p["reach"] else GRAY)
        ax.text(x2[5], y, hs_txt, fontsize=11, va="center", color=hs_col, fontweight="bold")
        ax.text(x2[6], y, f"{p['roas']:.2f}x" if p["roas"] else "—", fontsize=11, va="center",
                color=roas_col, fontweight="bold")
        fl = ", ".join(f[0] for f in p["flags"]) if p["flags"] else "—"
        fcol = RED if any(f[1] == RED for f in p["flags"]) else (AMBER if p["flags"] else GRAY)
        ax.text(x2[7], y, fl[:42], fontsize=9.5, va="center", color=fcol)
        y -= dy
    ax.text(0.03, y - 0.004, "Reached / HI-share exact (CIL, Apr15–May31); HI-share = of reached households, score ≥ 8001. "
            "Prospecting reaches ~85% HI at low coverage — the decline is conversion efficiency on net-new HI, not audience quality.",
            fontsize=9.5, color=GRAY, va="center")
    plt.savefig(a.png, dpi=200, bbox_inches="tight")
    print(f"wrote {a.png}")
    plt.close(fig)

    # ---- MD (full detail: stage map + all-campaign audit + prospecting funnel) ----
    md = [f"# {a.adv} — systematic audience audit", "",
          "## Where delivery & revenue go, by stage", "",
          "| Stage | Camps | Impressions | Spend | Conv | Revenue | ROAS |", "|---|--:|--:|--:|--:|--:|--:|"]
    for st in STAGE_ORDER:
        if st not in smap:
            continue
        s = smap[st]
        roas = s["revenue"] / s["spend"] if s["spend"] else 0
        md.append(f"| {st} | {s['n']} | {kfmt(s['imps'])} | {dollar(s['spend'])} | {s['conv']:,.0f} | "
                  f"{dollar(s['revenue'])} | {roas:.1f}x |")
    md += ["", "**Structural:** each campaign group is a full funnel (stage = `objective_id`); group-level metrics conflate "
           "stages. Retargeting (89071) is the engine; prospecting = 62% of spend / 13% of revenue.", "",
           "## Prospecting audience — targeting + funnel + flags", "",
           "| Campaign | Geo | Interest | Gate | Reached | HI-share | Coverage | ROAS | Flags |",
           "|---|---|---|---|--:|--:|--:|--:|---|"]
    for p in prosp:
        nm = NAMES.get(p["cid"], p["cid"])
        fl = ", ".join(f[0] for f in p["flags"]) or "—"
        reach_s = kfmt(p["reach"]) if p["reach"] else "— dark"
        hs_s = f"{p['hs']*100:.0f}%" if p["hs"] is not None else "—"
        cov_s = f"{p['cov']*100:.0f}%" if p["cov"] is not None else "—"
        md.append(f"| {p['grp']} {nm} | {p['ngeo']}/210 ({p['tier']}) | MM {(p['jop'] or '?').upper()} 3P | "
                  f"{'net-new' if p['gate'] else '—'} | {reach_s} | {hs_s} | {cov_s} | {p['roas']:.2f}x | {fl} |")
    md += ["", "**Read:** prospecting reaches ~80–88% HI at ~4–12% coverage of the (inflated) addressable — no hard HI "
           "ceiling, not scraping low-score users. Variants' worse ROAS = net-new HI converting worse. Base 261318 dark "
           "since ~Mar (F1 prospecting stopped; group's later delivery = retargeting)."]
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")
    print(f"FINDING: unified front matter — stage map (retargeting {rroas:.0f}x/{rt.get('revenue',0)/tot['revenue']*100:.0f}% rev) "
          f"+ prospecting audit w/ funnel folded in (HI-share ~85%, base dark). {len(prosp)} prospecting campaigns audited.")


if __name__ == "__main__":
    main()
