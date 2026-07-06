"""Module 00b render — PROSPECTING REACH COMPOSITION by score bucket (funnel detail).

Advertiser-agnostic companion to module 00: for each prospecting (obj=1) campaign, the distinct
households reached, split by score bucket (unscored · Mid/MaxReach · Peak Performance ·
High-Intent ≥8001, drawn low->high so High-Intent lands on the right end). Shows WHO prospecting
actually reaches — the reliable, exact read behind the audit's HI-share column. Adaptive height.

Reads  <outdir>/00_campaign_enum.csv · 00_funnel_sizes.csv · 00_funnel_hishare.csv
Writes <outdir>/00b_prospecting_funnel.png · 00b_prospecting_funnel.md
"""
import argparse
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam
        break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
NAVY, AMBER, GRAY, LGRAY = "#27496D", "#C77B30", "#888888", "#D7D9DC"


def kfmt(v):
    v = float(v)
    if abs(v) >= 1e6:
        return f"{v/1e6:.1f}M"
    return f"{v/1e3:.0f}K" if abs(v) >= 1e3 else f"{v:.0f}"


def glabel(grp, gname, cid, multi=False):
    s = (gname or "").replace("CTV ", "").replace("Prospecting", "").replace(" 2026", "").replace("-old", " (old)")
    s = s.replace("Frequency", "Freq").replace("Subscriptions", "Subs")
    s = " ".join(s.split()).strip()
    base = f"{grp} {s}" if s else f"{grp}"
    return base + (f" (c{cid})" if multi else "")


def pctfmt(share):
    return "<1%" if 0 < share < 0.01 else f"{share*100:.0f}%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="outputs/kindred_35094")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()
    o = a.outdir.rstrip("/")

    enum = {r["campaign_id"]: r for r in csv.DictReader(open(f"{o}/00_campaign_enum.csv"))}
    sizes = {r["campaign_id"]: r for r in csv.DictReader(open(f"{o}/00_funnel_sizes.csv"))}
    hishare = {r["campaign_id"]: r for r in csv.DictReader(open(f"{o}/00_funnel_hishare.csv"))}

    grp_obj1 = {}  # prospecting campaigns per group -> disambiguate multi-campaign groups
    for x in enum.values():
        if x.get("obj") == "1":
            grp_obj1[x.get("grp")] = grp_obj1.get(x.get("grp"), 0) + 1
    rows = []
    for cid, h in hishare.items():
        reach = int(h["reach_ip"])
        if reach <= 0:
            continue
        b = {k: int(h[k + "_ip"]) for k in ("hi", "pp", "mid", "unscored")}
        en = enum.get(cid, {})
        gname = en.get("group_name")
        grp = en.get("grp") or cid
        # prospecting spend for this campaign_id (obj==1 row in the enum); missing/non-prospecting -> 0
        spend = float(en["spend"] or 0) if (en.get("obj") == "1" and en.get("spend")) else 0.0
        addr = int(sizes[cid]["med_total"]) if cid in sizes else None
        rows.append({"cid": cid, "label": glabel(grp, gname, cid, grp_obj1.get(grp, 1) > 1), "reach": reach,
                     "b": b, "hs": b["hi"] / reach, "addr": addr, "spend": spend})
    tot_ps = sum(r["spend"] for r in rows) or 1  # rank/label bars by % of prospecting spend
    for r in rows:
        r["sshare"] = r["spend"] / tot_ps
    rows.sort(key=lambda r: -r["spend"])  # biggest spender first (top bar)
    n = len(rows)

    H = 1.9 + 0.42 * n
    fig = plt.figure(figsize=(13.5, H))
    fig.text(0.03, 1 - 0.55 / H, f"{a.adv} — prospecting reach by score bucket",
             fontsize=17, fontweight="bold", color="#222")
    fig.text(0.03, 1 - 0.92 / H, "Distinct households each prospecting campaign reached (recent in-TTL month), split by score. "
             "High HI-share = reaching high-intent supply; low HI-share / high unscored = reaching low-intent supply.",
             fontsize=11, color="#444")

    axb = fig.add_axes([0.30, 0.09, 0.56, 1 - (1.55 / H) - 0.09])
    ypos = list(range(n))[::-1]
    lefts = [0.0] * n
    # stack low->high so High-Intent (blue) lands on the RIGHT end of every bar
    for key, col, lab in [("unscored", LGRAY, "unscored"), ("mid", GRAY, "Mid / MaxReach"),
                          ("pp", AMBER, "Peak Perf"), ("hi", NAVY, "High-Intent (≥8001)")]:
        vals = [r["b"][key] for r in rows]
        axb.barh(ypos, vals, left=lefts, color=col, height=0.66, label=lab)
        lefts = [l + v for l, v in zip(lefts, vals)]
    maxr = max(r["reach"] for r in rows)
    for i, r in enumerate(rows):
        axb.text(r["reach"] + maxr * 0.022, ypos[i],
                 f"{kfmt(r['reach'])} · {r['hs']*100:.0f}% HI · {pctfmt(r['sshare'])} spend",
                 va="center", fontsize=9.8, color=NAVY, fontweight="bold")
    axb.set_yticks(ypos)
    axb.set_yticklabels([f"{r['label'][:34]}  ·  {pctfmt(r['sshare'])}" for r in rows], fontsize=9.8)
    axb.set_xlim(0, maxr * 1.30)
    axb.set_xticks([])
    for sp in ["top", "right", "bottom"]:
        axb.spines[sp].set_visible(False)
    # legend as a single horizontal row ABOVE the bars — never overlaps a data row
    axb.legend(frameon=False, fontsize=9.5, loc="lower center", bbox_to_anchor=(0.5, 1.005), ncol=4)
    plt.savefig(f"{o}/00b_prospecting_funnel.png", dpi=195, bbox_inches="tight")
    print(f"wrote {o}/00b_prospecting_funnel.png")
    plt.close(fig)

    md = [f"# {a.adv} — prospecting reach by score bucket", "",
          "Distinct households reached per obj=1 campaign (recent in-TTL month), split by score bucket, "
          "ranked by % of prospecting spend. Addressable = UI interest size (`total_audience_size`, ~5x-inflated).", "",
          "| Campaign | % spend | Addressable | Reached | HI | HI-share | unscored |",
          "|---|--:|--:|--:|--:|--:|--:|"]
    for r in rows:
        md.append(f"| {r['label']} | {pctfmt(r['sshare'])} | {kfmt(r['addr']) if r['addr'] else '—'} | {kfmt(r['reach'])} | "
                  f"{kfmt(r['b']['hi'])} | {r['hs']*100:.0f}% | {r['b']['unscored']/r['reach']*100:.0f}% |")
    open(f"{o}/00b_prospecting_funnel.md", "w").write("\n".join(md) + "\n")
    print(f"wrote {o}/00b_prospecting_funnel.md")
    lo = sum(1 for r in rows if r["hs"] < 0.7)
    print(f"FINDING: {n} prospecting campaigns; median HI-share {sorted(r['hs'] for r in rows)[n//2]*100:.0f}%; "
          f"{lo} reach mostly low-intent (<70% HI).")


if __name__ == "__main__":
    main()
