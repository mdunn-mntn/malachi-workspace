"""Module 00 — SYSTEMATIC AUDIENCE AUDIT (report front matter).

Runs FIRST. Inventories EVERY active campaign, classifies its funnel stage (by objective_id,
which is authoritative for stage here — funnel_level is reused as a sub-tier within retargeting),
decodes its audience expression (DS inventory + roles), assigns an archetype, and raises narrowing/
structure flags. Two views:
  (A) 00_stage_map.png  — where impressions / spend / conversions / revenue actually go, by stage.
      (Surfaces that RETARGETING, not prospecting, is Kindred's revenue engine; and that each
       "campaign group" is a full funnel, so group-level metrics conflate stages.)
  (B) 00_audience_audit.png — per campaign: stage · channel · archetype · DS roles · flags.

Reads  00_campaign_enum.csv (campaign-grain delivery + obj/funnel/chan)
       00_all_expressions.csv (audience expression per campaign)
Writes 00_stage_map.png · 00_audience_audit.png · 00_audience_audit.md
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

# objective_id -> stage (authoritative here). funnel_level is a sub-tier within retargeting.
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
    """Collect {ds: {'inc':n, 'exc':n}} category counts by include/exclude position."""
    if isinstance(node, list):
        [walk_ds(x, neg, out) for x in node]
    elif isinstance(node, dict):
        op, v = node.get("op"), node.get("value")
        if isinstance(v, dict) and "data_source_id" in v:
            ds = v["data_source_id"]
            k = "exc" if neg else "inc"
            out.setdefault(ds, {"inc": 0, "exc": 0})[k] += max(1, len(v.get("category_ids") or []))
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
    v = float(v)
    return f"{v/1e6:.1f}M" if abs(v) >= 1e6 else (f"{v/1e3:.0f}K" if abs(v) >= 1e3 else f"{v:.0f}")


def dollar(v):
    return "$" + kfmt(v)


def main():
    ap = argparse.ArgumentParser()
    base = "outputs/kindred_35094/"
    ap.add_argument("--enum", default=base + "00_campaign_enum.csv")
    ap.add_argument("--expr", default=base + "00_all_expressions.csv")
    ap.add_argument("--map-png", default=base + "00_stage_map.png")
    ap.add_argument("--audit-png", default=base + "00_audience_audit.png")
    ap.add_argument("--md", default=base + "00_audience_audit.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    ap.add_argument("--min-imps", type=int, default=5000)
    a = ap.parse_args()
    csv.field_size_limit(10 ** 7)

    enum = list(csv.DictReader(open(a.enum)))
    expr = {r["campaign_id"]: r["expression"] for r in csv.DictReader(open(a.expr)) if r.get("expression")}
    for r in enum:
        r["obj"] = int(r["obj"])
        r["stage"] = STAGE.get(r["obj"], f"obj{r['obj']}")
        for k in ("imps", "conv", "spend", "revenue", "reach"):
            r[k] = float(r[k] or 0)

    # ---- (A) STAGE MAP: aggregate delivery by stage ----
    smap = {}
    for r in enum:
        s = smap.setdefault(r["stage"], {"imps": 0, "spend": 0, "conv": 0, "revenue": 0, "n": 0})
        for k in ("imps", "spend", "conv", "revenue"):
            s[k] += r[k]
        s["n"] += 1
    tot = {k: sum(s[k] for s in smap.values()) for k in ("imps", "spend", "conv", "revenue")}

    fig, ax = plt.subplots(figsize=(13.5, 6.4))
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.text(0.03, 0.945, f"{a.adv} — where do the impressions and the revenue actually go?",
             fontsize=19.5, fontweight="bold", color="#222")
    fig.text(0.03, 0.90, "Every \"campaign group\" is a full funnel. Classified by stage, RETARGETING is the revenue engine — "
             "prospecting is top-funnel reach.", fontsize=12.5, color="#444")
    cols = ["Stage", "Camps", "Impressions", "Spend", "Conversions", "Revenue", "ROAS", "CPA"]
    xs = [0.03, 0.24, 0.34, 0.50, 0.60, 0.73, 0.855, 0.94]
    yt = 0.80
    for x, c in zip(xs, cols):
        ax.text(x, yt, c, fontsize=13.5, fontweight="bold", color=NAVY, va="center")
    ax.plot([0.03, 0.985], [0.765, 0.765], color=NAVY, lw=1.6)
    y = 0.68
    dy = 0.107
    for st in STAGE_ORDER:
        if st not in smap:
            continue
        s = smap[st]
        roas = s["revenue"] / s["spend"] if s["spend"] else 0
        cpa = s["spend"] / s["conv"] if s["conv"] else 0
        rc = GREEN if roas >= 5 else (NAVY if roas >= 1.8 else RED)
        share = s["revenue"] / tot["revenue"] * 100 if tot["revenue"] else 0
        cells = [st, str(s["n"]), kfmt(s["imps"]), dollar(s["spend"]), f"{s['conv']:,.0f}",
                 f"{dollar(s['revenue'])}  ({share:.0f}%)", f"{roas:.1f}x", (dollar(cpa) if cpa else "—")]
        cols_c = ["#222", "#222", "#222", "#222", "#222", "#222", rc, "#222"]
        for x, txt, cc in zip(xs, cells, cols_c):
            ax.text(x, y, txt, fontsize=13, va="center", color=cc,
                    fontweight="bold" if (x == xs[0] or cc == rc) else "normal")
        y -= dy
    ax.plot([0.03, 0.985], [y + 0.045, y + 0.045], color="#ccc", lw=1)
    troas = tot["revenue"] / tot["spend"] if tot["spend"] else 0
    for x, txt in zip(xs, ["TOTAL", str(len(enum)), kfmt(tot["imps"]), dollar(tot["spend"]),
                           f"{tot['conv']:,.0f}", dollar(tot["revenue"]), f"{troas:.1f}x", ""]):
        ax.text(x, y, txt, fontsize=13, va="center", color=NAVY, fontweight="bold")
    rt = smap.get("Retargeting", {})
    if rt:
        rroas = rt["revenue"] / rt["spend"] if rt.get("spend") else 0
        rshare = rt["revenue"] / tot["revenue"] * 100 if tot["revenue"] else 0
        ax.text(0.03, y - 0.09, f"Retargeting = {rroas:.0f}x ROAS and {rshare:.0f}% of revenue on "
                f"{rt['spend']/tot['spend']*100:.0f}% of spend. Prospecting's YoY decline (modules 12b/12c) is a "
                f"top-funnel-reach story, not where the money is.", fontsize=11, color=RED, fontweight="bold")
    plt.savefig(a.map_png, dpi=200, bbox_inches="tight")
    print(f"wrote {a.map_png}")
    plt.close(fig)

    # ---- (B) PER-CAMPAIGN AUDIENCE AUDIT ----
    rows = []
    for r in sorted(enum, key=lambda x: (STAGE_ORDER.index(x["stage"]) if x["stage"] in STAGE_ORDER else 9, -x["imps"])):
        if r["imps"] < a.min_imps:
            continue
        e = json.loads(expr[r["campaign_id"]]) if r["campaign_id"] in expr else {}
        cw = (e.get("categories") or {}).get("where")
        ds = {}
        walk_ds(cw, False, ds)
        ngeo = geo_inc(e)
        interest = [DSROLE[d][0] for d in (19, 13, 35, 8, 4) if d in ds and ds[d]["inc"]]
        jop = join_op(cw) if (19 in ds and 35 in ds) else None
        gate = contains(cw, 16)
        exc_ds = [DSROLE.get(d, (f"DS{d}",))[0] for d, v in ds.items() if v["exc"] and d not in (16,)]
        # archetype
        if r["obj"] == 1:
            arche = (("MM " + (jop.upper() if jop else "+") + " 3P") if interest else "?")
            if gate:
                arche += " · net-new gate"
        elif r["obj"] == 4:
            arche = "retargeting (site/cart)"
        elif r["obj"] in (5, 6):
            arche = "multi-touch pool"
        else:
            arche = "ego"
        # flags
        flags = []
        if r["obj"] == 1 and jop == "and":
            flags.append(("MM AND 3P", RED))
        if r["obj"] == 1 and gate:
            flags.append(("net-new gate", RED))
        if r["obj"] == 1 and ngeo and ngeo <= 25:
            flags.append((f"narrow geo {ngeo}/210", AMBER))
        if r["obj"] == 1 and ngeo >= 120:
            flags.append((f"thin geo {ngeo}/210", RED))
        if r["obj"] == 1 and 35 in ds and 19 not in ds:
            flags.append(("3P-only (no MM)", AMBER))
        if int(r["chan"]) == 1 and "CTV" in (r["group_name"] or ""):
            flags.append(("display (not CTV)", AMBER))
        rows.append({"r": r, "stage": r["stage"], "arche": arche, "interest": interest,
                     "ngeo": ngeo, "ds": ds, "flags": flags})

    n = len(rows)
    fig, ax = plt.subplots(figsize=(15.5, 1.3 + 0.42 * n))
    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, n + 1.3)
    fig.text(0.03, 0.965, f"{a.adv} — audience audit: every active campaign, its targeting, and its flags",
             fontsize=17, fontweight="bold", color="#222")
    cols = ["Stage", "Campaign", "Ch", "Audience archetype", "Interest DS", "Geo", "Imps", "ROAS", "Flags"]
    xs = [0.03, 0.135, 0.335, 0.375, 0.585, 0.70, 0.76, 0.825, 0.885]
    yt = n + 0.55
    for x, c in zip(xs, cols):
        ax.text(x, yt, c, fontsize=11.5, fontweight="bold", color=NAVY, va="center")
    ax.plot([0.03, 0.99], [n + 0.25, n + 0.25], color=NAVY, lw=1.4)
    last_stage = None
    for i, d in enumerate(rows):
        y = n - 0.2 - i
        r = d["r"]
        if i % 2 == 0:
            ax.axhspan(y - 0.5, y + 0.5, color="#000", alpha=0.028)
        st = d["stage"] if d["stage"] != last_stage else ""
        last_stage = d["stage"]
        roas = r["revenue"] / r["spend"] if r["spend"] else 0
        rc = GREEN if roas >= 5 else (NAVY if roas >= 1.8 else (RED if roas > 0 else GRAY))
        ch = "CTV" if int(r["chan"]) == 8 else "disp"
        nm = (r["camp_name"] or "")[:26]
        interest = "+".join(d["interest"]) or "—"
        geo = f"{d['ngeo']}/210" if d["ngeo"] else "—"
        ax.text(xs[0], y, st, fontsize=10.5, va="center", color=NAVY, fontweight="bold")
        ax.text(xs[1], y, f"{r['grp']} {nm}", fontsize=9.8, va="center", color="#222")
        ax.text(xs[2], y, ch, fontsize=10, va="center", color=(AMBER if ch == "disp" else "#444"))
        ax.text(xs[3], y, d["arche"], fontsize=10, va="center",
                color=(RED if "gate" in d["arche"] or "AND" in d["arche"] else "#222"))
        ax.text(xs[4], y, interest, fontsize=9.8, va="center", color=GREEN if interest != "—" else GRAY)
        ax.text(xs[5], y, geo, fontsize=9.8, va="center",
                color=(AMBER if d["ngeo"] and d["ngeo"] <= 25 else (RED if d["ngeo"] >= 120 else "#444")))
        ax.text(xs[6], y, kfmt(r["imps"]), fontsize=9.8, va="center", color="#222")
        ax.text(xs[7], y, f"{roas:.1f}x" if roas else "—", fontsize=10, va="center", color=rc, fontweight="bold")
        fl = ", ".join(f[0] for f in d["flags"]) if d["flags"] else "—"
        ax.text(xs[8], y, fl[:38], fontsize=9, va="center",
                color=RED if any(f[1] == RED for f in d["flags"]) else (AMBER if d["flags"] else GRAY))
    plt.savefig(a.audit_png, dpi=185, bbox_inches="tight")
    print(f"wrote {a.audit_png}")
    plt.close(fig)

    # ---- MD ----
    md = [f"# {a.adv} — systematic audience audit", "",
          "## Where delivery & revenue go, by stage", "",
          "| Stage | Camps | Impressions | Spend | Conv | Revenue | ROAS | CPA |",
          "|---|--:|--:|--:|--:|--:|--:|--:|"]
    for st in STAGE_ORDER:
        if st not in smap:
            continue
        s = smap[st]
        roas = s["revenue"] / s["spend"] if s["spend"] else 0
        cpa = s["spend"] / s["conv"] if s["conv"] else 0
        md.append(f"| {st} | {s['n']} | {kfmt(s['imps'])} | {dollar(s['spend'])} | {s['conv']:,.0f} | "
                  f"{dollar(s['revenue'])} | {roas:.1f}x | {dollar(cpa) if cpa else '—'} |")
    md += ["", "**Key structural findings:**",
           "- Each *campaign group* is a full funnel (Prospecting F1 + Multi-Touch S2/S3 + Ego + a separate Retargeting group). "
           "Group-level metrics conflate stages — classify by `objective_id` (1=Prospect, 4=Retarget, 5=MT-S2, 6=MT-S3, 7=Ego).",
           f"- **Retargeting (89071) is the revenue engine** — ~{(smap.get('Retargeting',{}).get('revenue',0)/smap.get('Retargeting',{}).get('spend',1)):.0f}x ROAS, "
           f"{smap.get('Retargeting',{}).get('revenue',0)/tot['revenue']*100:.0f}% of revenue. Prospecting's YoY decline is a top-funnel-reach story.",
           "- Prospecting runs on CTV; the Multi-Touch stages run on **display** (channel mix within a 'CTV' group).", "",
           "## Per-campaign audience audit", "",
           "| Stage | Campaign | Ch | Archetype | Interest | Geo | Imps | ROAS | Flags |",
           "|---|---|--|---|---|--|--:|--:|---|"]
    for d in rows:
        r = d["r"]
        roas = r["revenue"] / r["spend"] if r["spend"] else 0
        ch = "CTV" if int(r["chan"]) == 8 else "disp"
        fl = ", ".join(f[0] for f in d["flags"]) or "—"
        md.append(f"| {d['stage']} | {r['grp']} {(r['camp_name'] or '')[:28]} | {ch} | {d['arche']} | "
                  f"{'+'.join(d['interest']) or '—'} | {d['ngeo'] or '—'} | {kfmt(r['imps'])} | {roas:.1f}x | {fl} |")
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")
    rt = smap.get("Retargeting", {})
    print(f"FINDING: {len(enum)} active campaigns across {len(smap)} stages. Retargeting = "
          f"{rt.get('revenue',0)/rt.get('spend',1):.0f}x ROAS / {rt.get('revenue',0)/tot['revenue']*100:.0f}% of revenue "
          f"(the engine); Prospecting {smap.get('Prospecting',{}).get('revenue',0)/smap.get('Prospecting',{}).get('spend',1):.1f}x. "
          f"Groups are full funnels -> classify by objective_id, not group.")


if __name__ == "__main__":
    main()
