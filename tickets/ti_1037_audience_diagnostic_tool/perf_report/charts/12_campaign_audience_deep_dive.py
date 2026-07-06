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


# location_id 237 = "United States" (a national target, NOT a Nielsen DMA). When a campaign's
# geo include-set is national, geo-tiering is N/A — the advertiser targets all-of-US, not a DMA slice.
NATIONAL_LOC = "237"


def is_national(inc):
    """True if the include-set is a national target (US as a whole), so geo-tiering does not apply."""
    return len(inc) == 0 or [str(i) for i in inc] == [NATIONAL_LOC]


def tier(inc):
    if is_national(inc):
        return "National (US)"
    n = len(inc)
    if n <= 30:
        return "Top markets"
    if n <= 80:
        return "Mid markets"
    return "Long-tail (small mkts)"


def prospecting_spend_by_group(enum_path):
    """grp (campaign_group_id) -> summed prospecting (obj==1) spend from 00_campaign_enum.csv.
    Advertiser-agnostic: reads the already-summed `spend` column; groups absent -> not in map (spend 0)."""
    out = {}
    if not (enum_path and os.path.exists(enum_path)):
        return out
    for r in csv.DictReader(open(enum_path)):
        try:
            if int(r["obj"]) != 1:
                continue
        except (KeyError, ValueError, TypeError):
            continue
        try:
            sp = float(r.get("spend") or 0)
        except (ValueError, TypeError):
            sp = 0.0
        out[r["grp"]] = out.get(r["grp"], 0.0) + sp
    return out


def pctfmt(share):
    return "<1%" if 0 < share < 0.01 else f"{share*100:.0f}%"


# compact tier labels for the fixed-width table (full names stay in the .md)
SHORT_TIER = {"National (US)": "National", "Top markets": "Top mkts",
              "Mid markets": "Mid mkts", "Long-tail (small mkts)": "Long-tail"}


def main():
    ap = argparse.ArgumentParser()
    base = "outputs/kindred_35094/"
    ap.add_argument("--expr", default=base + "02_prospecting_audience_expressions.csv")
    ap.add_argument("--geo", default=base + "12_geo_dma_reference.csv")
    ap.add_argument("--segs", default=base + "12_ds35_segment_names.csv")
    ap.add_argument("--png", default=base + "12_campaign_audience_deep_dive.png")
    ap.add_argument("--md", default=base + "12_campaign_audience_deep_dive.md")
    ap.add_argument("--enum", default=None, help="00_campaign_enum.csv (spend source); default = alongside --expr")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()

    csv.field_size_limit(10 ** 7)
    # spend source (advertiser-agnostic): 00_campaign_enum.csv sits in the same <outdir> as --expr
    enum_path = a.enum or os.path.join(os.path.dirname(a.expr), "00_campaign_enum.csv")
    gspend = prospecting_spend_by_group(enum_path)
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
        gate = contains(cw, 16)  # DS16 = advertiser's own funnel = net-new-reach gate
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
        natl = is_national(inc)
        markets = ["United States (national)"] if natl else [geo.get(str(i), str(i)) for i in inc][:5]
        flags = []
        if jop == "and":
            flags.append("AND-NARROWING: 3P required onto MM (throttles reach)")
        # geo flags only apply to DMA-sliced campaigns; national targets skip them entirely
        if not natl:
            if len(inc) <= 25:
                flags.append(f"narrow geo: only {len(inc)} of 210 DMAs")
            if len(inc) >= 120:
                flags.append(f"thin long-tail: {len(inc)} DMAs (per-DMA delivery near-zero at low $)")
            if len(setkey[tuple(inc)]) > 1:
                flags.append(f"fragmentation: {len(setkey[tuple(inc)])} campaigns share this exact DMA set")
        if gate:
            flags.append("net-new gate (net-new households only)")
        dive.append({"grp": r["campaign_group_id"], "name": r["group_name"], "n_inc": len(inc), "n_exc": len(exc),
                     "natl": natl, "tier": tier(inc), "markets": markets, "jop": jop, "mm": mm, "tp": tp,
                     "gate": gate, "segs": [segn.get(s, s) for s in seg_ids], "flags": flags,
                     "spend": gspend.get(r["campaign_group_id"], 0.0)})

    # rank by % of prospecting spend (materiality); groups absent from the enum -> 0 -> sort last
    tot_ps = sum(d["spend"] for d in dive) or 1
    for d in dive:
        d["sshare"] = d["spend"] / tot_ps
    _ranked = sorted(dive, key=lambda x: (-x["spend"], x["n_inc"]))
    dive_by_spend = [d for d in _ranked if d["spend"] > 0] or _ranked  # omit zero-spend groups (guard)

    and_narrowing = any(d["jop"] == "and" for d in dive)
    n_dive = len(dive) or 1
    natl_frac = sum(1 for d in dive if d["natl"]) / n_dive
    mostly_national = natl_frac >= 0.5      # account targets US as a whole (geo-tiering N/A)
    all_national = natl_frac == 1.0
    any_gate = any(d["gate"] for d in dive)
    # geo story only applies when the account actually geo-slices; national accounts get a gate/interest read.
    if and_narrowing:
        verdict_md = "AUDIENCE NARROWING (AND-required 3P/segment) — investigate"
    elif mostly_national:
        verdict_md = ("NATIONAL — no meaningful geo slicing; " + ("net-new funnel gate present (narrows by WHO, not 3P)"
                      if any_gate else "additive interest, no structural narrowing"))
    else:
        verdict_md = "BROADENING / geo-slicing — NOT audience narrowing"

    # ---- markdown deep-dive ----
    md = [f"# {a.adv} — Prospecting campaign audience deep-dive", "",
          f"**Account red-flag verdict: {verdict_md}.** "
          f"Interest logic across campaigns: {'/'.join(sorted(set((d['jop'] or '?') for d in dive)))} (OR = MM/3P additive; AND = 3P narrows MM). "
          "Geo `location_ids` decode via `geo.location_data` (Nielsen DMA; location 237 = national US); 3P names via `tpa.categories` (sizes GCS-gated).", ""]
    for d in dive_by_spend:
        md.append(f"### {d['grp']} — {d['name']}  ·  {pctfmt(d['sshare'])} of prospecting spend")
        geo_line = ("National (US) — targets all-of-US (no DMA slice)" if d["natl"]
                    else f"{d['tier']} — **{d['n_inc']} DMAs** (excl {d['n_exc']}). e.g. {', '.join(d['markets'])}"
                    + (" …" if d["n_inc"] > 5 else ""))
        md.append(f"- **Geo:** {geo_line}")
        md.append(f"- **Interest:** ({d['mm']} MM keywords **{(d['jop'] or '?').upper()}** {d['tp']} LiveRamp 3P segments)"
                  + (" — additive/broadening" if d["jop"] == "or" else " — AND-NARROWING" if d["jop"] == "and" else ""))
        if d["segs"]:
            md.append(f"- **3P segments:** {'; '.join(d['segs'][:4])}" + (" …" if len(d["segs"]) > 4 else ""))
        md.append(f"- **Red flags:** {'; '.join(d['flags']) if d['flags'] else 'none structural'}")
        md.append("")
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")

    # ---- PNG summary table (adaptive to a variable number of campaigns) ----
    # ranked by % of prospecting spend (materiality) — biggest spender on top; enum-absent groups (0) last
    cols = ["Campaign", "% spend", "Geo tier", "#DMAs", "Interest logic", "MM kw", "3P seg", "Gate", "Red flag"]
    order = dive_by_spend
    n = len(order)
    # name width shrinks the more campaigns there are so long frequency-variant names still fit
    namew = 26 if n <= 6 else (22 if n <= 10 else 18)
    maxshare = max((d["sshare"] for d in order), default=1) or 1
    fig, ax = plt.subplots(figsize=(15.5, 1.1 + 0.58 * n))
    ax.axis("off")
    #      Campaign  %spend  Geo    #DMAs  Interest  MM kw  3P seg  Gate   Red flag
    xs = [0.0, 0.245, 0.37, 0.455, 0.525, 0.655, 0.715, 0.765, 0.825]
    yt = n + 0.2
    for x, c in zip(xs, cols):
        ax.text(x, yt, c, fontsize=11, fontweight="bold", color=NAVY, va="center")
    ax.plot([0, 1], [n - 0.4, n - 0.4], color=NAVY, lw=1.4)
    for i, d in enumerate(order):
        y = n - 1 - i
        if i % 2 == 0:
            ax.axhspan(y - 0.5, y + 0.5, color="#000", alpha=0.03)
        nm = (d["name"] or "").strip()
        jc = GREEN if d["jop"] == "or" else (RED if d["jop"] == "and" else GRAY)
        dma_cell = "US" if d["natl"] else str(d["n_inc"])
        # % spend: number + tiny proportional weight bar (materiality at a glance, like module 00)
        ax.text(xs[1], y, f"{pctfmt(d['sshare'])}", fontsize=10.3, va="center", color=NAVY, fontweight="bold")
        bar_w = 0.05 * d["sshare"] / maxshare
        ax.add_patch(Rectangle((xs[1] + 0.038, y - 0.10), bar_w, 0.20, color=NAVY, alpha=0.5, zorder=1))
        cells = [f"{d['grp']} {nm[:namew]}", SHORT_TIER.get(d["tier"], d["tier"]), dma_cell,
                 f"MM {(d['jop'] or '?').upper()} 3P", str(d["mm"]), str(d["tp"])]
        cell_x = [xs[0], xs[2], xs[3], xs[4], xs[5], xs[6]]
        for x, txt in zip(cell_x, cells):
            col = jc if txt.startswith("MM ") else "#222"
            ax.text(x, y, txt, fontsize=10.3, va="center", color=col,
                    fontweight="bold" if txt.startswith("MM ") else "normal")
        ax.text(xs[7], y, "gated" if d["gate"] else "—", fontsize=10, va="center",
                color=RED if d["gate"] else GRAY, fontweight="bold" if d["gate"] else "normal")
        fl = d["flags"][0] if d["flags"] else "—"
        ax.text(xs[8], y, fl[:40], fontsize=9, va="center", color=RED if d["flags"] else GRAY)
    ax.set_xlim(0, 1)
    ax.set_ylim(-0.6, n + 0.6)
    if and_narrowing:
        verdict = "AUDIENCE NARROWING — investigate"
    elif mostly_national:
        verdict = "NATIONAL · net-new gate" if any_gate else "NATIONAL · additive (no narrowing)"
    else:
        verdict = "BROADENING / geo-slicing (not narrowing)"
    ax.set_title(f"{a.adv} — Campaign audience deep-dive   ·   verdict: {verdict}",
                 fontsize=13.5, fontweight="bold", loc="left", color="#222", pad=12)
    plt.tight_layout()
    plt.savefig(a.png, dpi=190, bbox_inches="tight")
    print(f"wrote {a.png}")
    n_natl = sum(1 for d in dive if d["natl"])
    n_gated = sum(1 for d in dive if d["gate"])
    geo_desc = (f"{n_natl}/{n} national (US), {n - n_natl} DMA-sliced" if n_natl else
                f"{n} prospecting campaigns geo-sliced by DMA")
    print(f"FINDING: {geo_desc}. Interest logic = "
          f"{'/'.join(sorted(set((d['jop'] or '?') for d in dive)))} ⇒ "
          f"{'AND-narrowing present' if and_narrowing else 'ADDITIVE (no AND-narrowing)'}; "
          f"{n_gated}/{n} carry a DS16 net-new funnel gate. "
          f"{'Narrowing is by WHO (net-new households), not by 3P.' if not and_narrowing else ''}")


if __name__ == "__main__":
    main()
