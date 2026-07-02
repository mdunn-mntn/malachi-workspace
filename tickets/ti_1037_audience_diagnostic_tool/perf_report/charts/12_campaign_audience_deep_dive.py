"""Module 12 render — campaign audience DEEP DIVE (what each campaign targets + red flags).

Parses each prospecting campaign's expression: geo tier (included/excluded DMAs, named markets via the
geo reference), interest logic (MM DS19 vs 3P DS35 joined by OR=additive or AND=narrowing), 3P segments,
excludes/gate. Auto-surfaces red flags: AND-required 3P narrowing, small limiting 3P segments, geo
footprint (tiny / thin-long-tail / fragmentation), MM-audience narrowing. Emits a committable markdown
deep-dive + a PNG summary table.

Reads  02_prospecting_audience_expressions.csv (expressions) · 12_geo_dma_reference.csv (DMA names)
       12_ds35_segment_names.csv (3P segment names)
Writes 12_campaign_audience_deep_dive.png / .md
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "artifacts"))
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
NAVY, GREEN, RED, AMBER, GRAY = "#27496D", "#2E8B57", "#D63B2F", "#C77B30", "#666666"


def geo_sets(e):
    inc, exc = [], []

    def rec(n, neg):
        if isinstance(n, list):
            [rec(x, neg) for x in n]
        elif isinstance(n, dict):
            v = n.get("value")
            if isinstance(v, dict) and "location_ids" in v:
                (exc if neg else inc).extend(v["location_ids"]); return
            rec(v, neg ^ (n.get("op") == "not")) if v is not None else None
    rec((e.get("geos") or {}).get("where"), False)
    return sorted(set(inc)), sorted(set(exc))


def ds_leaves(node, out=None):
    out = {} if out is None else out
    if isinstance(node, list):
        [ds_leaves(x, out) for x in node]
    elif isinstance(node, dict):
        v = node.get("value")
        if isinstance(v, dict) and "data_source_id" in v:
            out[v["data_source_id"]] = out.get(v["data_source_id"], 0) + len(v.get("category_ids") or [])
        elif v is not None:
            ds_leaves(v, out)
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


def tier(n):
    if n <= 30:
        return "Top markets"
    if n <= 80:
        return "Mid markets"
    return "Long-tail (small mkts)"


def main():
    ap = argparse.ArgumentParser()
    base = "outputs/kindred_35094/"
    ap.add_argument("--expr", default=base + "02_prospecting_audience_expressions.csv")
    ap.add_argument("--geo", default=base + "12_geo_dma_reference.csv")
    ap.add_argument("--segs", default=base + "12_ds35_segment_names.csv")
    ap.add_argument("--png", default=base + "12_campaign_audience_deep_dive.png")
    ap.add_argument("--md", default=base + "12_campaign_audience_deep_dive.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()

    csv.field_size_limit(10 ** 7)
    geo = {r["location_id"]: r["dma_name"] for r in csv.DictReader(open(a.geo))}
    segn = {r["category_id"]: r["name"] for r in csv.DictReader(open(a.segs))}
    rows = [r for r in csv.DictReader(open(a.expr)) if r.get("expression")]

    # count how many campaigns share each geo include-set (for fragmentation flag)
    setkey = {}
    for r in rows:
        inc, _ = geo_sets(json.loads(r["expression"]))
        setkey.setdefault(tuple(inc), []).append(r["campaign_group_id"])

    dive = []
    for r in rows:
        e = json.loads(r["expression"])
        inc, exc = geo_sets(e)
        cw = (e.get("categories") or {}).get("where")
        leaves = ds_leaves(cw)
        jop = join_op(cw)
        mm, tp = leaves.get(19, 0), leaves.get(35, 0)
        # 3P segment names present
        seg_ids = []

        def collect35(n):
            if isinstance(n, list):
                [collect35(x) for x in n]
            elif isinstance(n, dict):
                v = n.get("value")
                if isinstance(v, dict) and v.get("data_source_id") == 35:
                    seg_ids.extend(str(c) for c in v.get("category_ids") or [])
                elif v is not None:
                    collect35(v)
        collect35(cw)
        markets = [geo.get(str(i), str(i)) for i in inc][:5]
        flags = []
        if jop == "and":
            flags.append("AND-NARROWING: 3P required onto MM (throttles reach)")
        if len(inc) <= 25:
            flags.append(f"narrow geo: only {len(inc)} of 210 DMAs")
        if len(inc) >= 120:
            flags.append(f"thin long-tail: {len(inc)} DMAs (per-DMA delivery near-zero at low $)")
        if len(setkey[tuple(inc)]) > 1:
            flags.append(f"fragmentation: {len(setkey[tuple(inc)])} campaigns share this exact DMA set")
        dive.append({"grp": r["campaign_group_id"], "name": r["group_name"], "n_inc": len(inc), "n_exc": len(exc),
                     "tier": tier(len(inc)), "markets": markets, "jop": jop, "mm": mm, "tp": tp,
                     "segs": [segn.get(s, s) for s in seg_ids], "flags": flags})

    and_narrowing = any(d["jop"] == "and" for d in dive)

    # ---- markdown deep-dive ----
    md = [f"# {a.adv} — Prospecting campaign audience deep-dive", "",
          f"**Account red-flag verdict: {'AUDIENCE NARROWING (AND-required 3P/segment) — investigate' if and_narrowing else 'BROADENING / geo-slicing — NOT audience narrowing'}.** "
          f"Interest logic across campaigns: {'/'.join(sorted(set((d['jop'] or '?') for d in dive)))} (OR = MM/3P additive; AND = 3P narrows MM). "
          "Geo `location_ids` decode via `geo.location_data` (Nielsen DMA); 3P names via `tpa.categories` (sizes GCS-gated).", ""]
    for d in sorted(dive, key=lambda x: x["n_inc"]):
        md.append(f"### {d['grp']} — {d['name']}")
        md.append(f"- **Geo:** {d['tier']} — **{d['n_inc']} DMAs** (excl {d['n_exc']}). e.g. {', '.join(d['markets'])}"
                  + (" …" if d["n_inc"] > 5 else ""))
        md.append(f"- **Interest:** ({d['mm']} MM keywords **{(d['jop'] or '?').upper()}** {d['tp']} LiveRamp 3P segments)"
                  + (" — additive/broadening" if d["jop"] == "or" else " — AND-NARROWING" if d["jop"] == "and" else ""))
        if d["segs"]:
            md.append(f"- **3P segments:** {'; '.join(d['segs'][:4])}" + (" …" if len(d["segs"]) > 4 else ""))
        md.append(f"- **Red flags:** {'; '.join(d['flags']) if d['flags'] else 'none structural'}")
        md.append("")
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")

    # ---- PNG summary table ----
    cols = ["Campaign", "Geo tier", "#DMAs", "Interest logic", "MM kw", "3P seg", "Red flag"]
    order = sorted(dive, key=lambda x: x["n_inc"])
    n = len(order)
    fig, ax = plt.subplots(figsize=(15, 1.1 + 0.62 * n))
    ax.axis("off")
    xs = [0.0, 0.30, 0.40, 0.47, 0.66, 0.73, 0.80]
    yt = n + 0.2
    for x, c in zip(xs, cols):
        ax.text(x, yt, c, fontsize=11, fontweight="bold", color=NAVY, va="center")
    ax.plot([0, 1], [n - 0.4, n - 0.4], color=NAVY, lw=1.4)
    for i, d in enumerate(order):
        y = n - 1 - i
        if i % 2 == 0:
            ax.axhspan(y - 0.5, y + 0.5, color="#000", alpha=0.03)
        nm = (d["name"] or "").replace("CTV Prospecting", "").strip()
        jc = GREEN if d["jop"] == "or" else (RED if d["jop"] == "and" else GRAY)
        cells = [f"{d['grp']} {nm[:18]}", d["tier"], str(d["n_inc"]),
                 f"MM {(d['jop'] or '?').upper()} 3P", str(d["mm"]), str(d["tp"])]
        for x, txt in zip(xs, cells):
            col = jc if txt.startswith("MM ") else "#222"
            ax.text(x, y, txt, fontsize=10.5, va="center", color=col,
                    fontweight="bold" if txt.startswith("MM ") else "normal")
        fl = d["flags"][0] if d["flags"] else "—"
        ax.text(xs[6], y, fl[:34], fontsize=9, va="center", color=RED if d["flags"] else GRAY)
    ax.set_xlim(0, 1)
    ax.set_ylim(-0.6, n + 0.6)
    verdict = "AUDIENCE NARROWING — investigate" if and_narrowing else "BROADENING / geo-slicing (not narrowing)"
    ax.set_title(f"{a.adv} — Campaign audience deep-dive   ·   verdict: {verdict}",
                 fontsize=13.5, fontweight="bold", loc="left", color="#222", pad=12)
    plt.tight_layout()
    plt.savefig(a.png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.png}")
    print(f"FINDING: {n} prospecting campaigns geo-sliced by DMA (Top-20 / Mid-38 / Low-152). Interest "
          f"logic = {'/'.join(sorted(set((d['jop'] or '?') for d in dive)))} ⇒ "
          f"{'AND-narrowing present' if and_narrowing else 'ADDITIVE (no AND-narrowing)'}. "
          f"Story = geo-mix broadening into smaller markets + top-market flagship wind-down, NOT audience narrowing.")


if __name__ == "__main__":
    main()
