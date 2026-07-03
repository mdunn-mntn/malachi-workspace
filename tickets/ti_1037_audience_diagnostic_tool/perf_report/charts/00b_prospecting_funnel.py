"""Module 00b render — PROSPECTING AUDIENCE FUNNEL (max addressable -> HI-eligible -> reached).

For each prospecting (obj=1) campaign: the audience funnel backing the audit.
  Addressable (UI interest size, ~5x-inflated, national)  ->  ~Deliverable (÷5)  ->
  Reached (distinct households served)  ->  of which High-Intent (household_score>=8001).

Key read: prospecting REACHES mostly HI (~80–88%) at low coverage of the addressable pool —
so the YoY decline is conversion EFFICIENCY on net-new HI, not audience-quality erosion.
Base High Pop (261318) went dark ~March (out of the 90d CIL window) — shown addressable-only.

Reads  00_campaign_enum.csv · 00_funnel_sizes.csv · 00_funnel_hishare.csv
Writes 00b_prospecting_funnel.png · 00b_prospecting_funnel.md
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
NAVY, GREEN, RED, AMBER, GRAY, LGRAY = "#27496D", "#2E8B57", "#D63B2F", "#C77B30", "#888888", "#D7D9DC"

# prospecting F1 campaign_id -> (label, tier)
CAMP = {"261318": ("High Pop (base)", "top-20"), "540723": ("Mid Pop", "mid-38"),
        "463188": ("Low Pop", "low-152"), "576256": ("HiPop Harter", "top-20"),
        "576267": ("HiPop Motherhood", "top-20"), "576276": ("HiPop Mom-Focus", "top-20")}
ORDER = ["261318", "540723", "463188", "576256", "576267", "576276"]


def kfmt(v):
    v = float(v)
    if abs(v) >= 1e6:
        return f"{v/1e6:.1f}M"
    return f"{v/1e3:.0f}K" if abs(v) >= 1e3 else f"{v:.0f}"


def main():
    ap = argparse.ArgumentParser()
    base = "outputs/kindred_35094/"
    ap.add_argument("--sizes", default=base + "00_funnel_sizes.csv")
    ap.add_argument("--hishare", default=base + "00_funnel_hishare.csv")
    ap.add_argument("--png", default=base + "00b_prospecting_funnel.png")
    ap.add_argument("--md", default=base + "00b_prospecting_funnel.md")
    ap.add_argument("--adv", default="Kindred Bravely (35094)")
    a = ap.parse_args()

    sizes = {r["campaign_id"]: r for r in csv.DictReader(open(a.sizes))}
    hi = {r["campaign_id"]: r for r in csv.DictReader(open(a.hishare))}
    rows = []
    for cid in ORDER:
        lab, tier = CAMP[cid]
        addr = int(sizes[cid]["med_total"]) if cid in sizes else None
        deliver = addr / 5 if addr else None
        h = hi.get(cid)
        reach = int(h["reach_ip"]) if h else None
        buckets = {b: (int(h[b + "_ip"]) if h else 0) for b in ("hi", "pp", "mid", "unscored")} if h else None
        hishare = (buckets["hi"] / reach) if (reach and buckets) else None
        cov = (reach / deliver) if (reach and deliver) else None
        rows.append({"cid": cid, "lab": lab, "tier": tier, "addr": addr, "deliver": deliver,
                     "reach": reach, "buckets": buckets, "hishare": hishare, "cov": cov, "dark": h is None})

    live = [r for r in rows if not r["dark"]]

    fig = plt.figure(figsize=(14, 8.6))
    fig.text(0.03, 0.955, f"{a.adv} — prospecting audience funnel: reaches mostly High-Intent, at low coverage",
             fontsize=17.5, fontweight="bold", color="#222")
    fig.text(0.03, 0.917, "Max addressable (UI interest size, national) -> ~deliverable (÷5) -> reached -> of which High-Intent "
             "(score ≥ 8001). ~85% of who we reach is HI — so the decline is conversion efficiency, not audience quality.",
             fontsize=12, color="#444")

    # ---- top: funnel table ----
    axT = fig.add_axes([0.03, 0.52, 0.94, 0.34])
    axT.axis("off")
    axT.set_xlim(0, 1)
    axT.set_ylim(0, 1)
    cols = ["Prospecting campaign", "Geo", "Addressable*", "~Deliverable", "Reached", "HI reached", "HI-share", "Coverage*"]
    xs = [0.0, 0.20, 0.31, 0.45, 0.585, 0.70, 0.82, 0.92]
    axT.text(0, 0.94, "", fontsize=1)
    for x, c in zip(xs, cols):
        axT.text(x, 0.9, c, fontsize=12, fontweight="bold", color=NAVY, va="center")
    axT.plot([0, 1], [0.83, 0.83], color=NAVY, lw=1.4)
    y = 0.72
    for r in rows:
        if r["dark"]:
            cells = [f"{r['cid']} {r['lab']}", r["tier"], kfmt(r["addr"]), kfmt(r["deliver"]),
                     "— dark ~Mar", "—", "—", "—"]
            for x, t in zip(xs, cells):
                axT.text(x, y, t, fontsize=11.5, va="center",
                         color=GRAY if x != xs[0] else "#222", fontweight="bold" if x == xs[0] else "normal",
                         style="italic" if "dark" in str(t) else "normal")
        else:
            hs = r["hishare"]
            hs_col = GREEN if hs >= 0.83 else (AMBER if hs >= 0.7 else RED)
            cells = [f"{r['cid']} {r['lab']}", r["tier"], kfmt(r["addr"]), kfmt(r["deliver"]),
                     kfmt(r["reach"]), kfmt(r["buckets"]["hi"]), f"{hs*100:.0f}%", f"{r['cov']*100:.0f}%"]
            for x, t in zip(xs, cells):
                col = "#222"
                if x == xs[6]:
                    col = hs_col
                if x == xs[7]:
                    col = GRAY
                axT.text(x, y, t, fontsize=11.5, va="center", color=col,
                         fontweight="bold" if x in (xs[0], xs[6]) else "normal")
        y -= 0.115
    axT.text(0, y + 0.02, "*Addressable = UI interest size (MM OR 3P match), NATIONAL and ~5x-inflated — so Coverage is a loose "
             "floor (real in-geo coverage is higher). Reached / HI reached are exact (CIL, Apr15–May31).",
             fontsize=9.3, color=GRAY, va="center")

    # ---- bottom: reached composition by score bucket (the exact, reliable read) ----
    axB = fig.add_axes([0.07, 0.09, 0.62, 0.33])
    labs = [f"{r['cid']} {r['lab']}" for r in live]
    ypos = list(range(len(live)))[::-1]
    lefts = [0] * len(live)
    order_b = [("hi", NAVY, "High-Intent"), ("pp", AMBER, "Purchase-Prone"),
               ("mid", GRAY, "Mid/MaxReach"), ("unscored", LGRAY, "unscored")]
    for key, col, lab in order_b:
        vals = [r["buckets"][key] for r in live]
        axB.barh(ypos, vals, left=lefts, color=col, height=0.66, label=lab)
        lefts = [l + v for l, v in zip(lefts, vals)]
    for i, r in enumerate(live):
        yp = ypos[i]
        axB.text(r["reach"] * 1.01, yp, f"{r['hishare']*100:.0f}% HI", va="center", fontsize=10.5,
                 color=NAVY, fontweight="bold")
    axB.set_yticks(ypos)
    axB.set_yticklabels(labs, fontsize=10)
    axB.set_xlim(0, max(r["reach"] for r in live) * 1.16)
    axB.set_xticks([])
    for sp in ["top", "right", "bottom"]:
        axB.spines[sp].set_visible(False)
    axB.set_title("Who we actually reached, by score bucket (Apr15–May31)", fontsize=12.5, color="#222",
                  loc="left", pad=8, fontweight="bold")
    axB.legend(frameon=False, fontsize=9.5, loc="lower right", ncol=2)

    fig.text(0.72, 0.40, "The read", fontsize=12.5, fontweight="bold", color=NAVY)
    for i, line in enumerate([
        "• Prospecting reaches ~80–88% HI —",
        "  it is NOT scraping low-score users.",
        "• Reach is a small slice of the (inflated)",
        "  addressable pool — no hard ceiling.",
        "• So the variants' worse ROAS is net-new",
        "  HI converting worse, not lower quality.",
        "• Base prospecting (261318) is dark since",
        "  ~March — its group's May delivery was",
        "  retargeting, not prospecting.",
    ]):
        fig.text(0.72, 0.355 - i * 0.033, line, fontsize=10.3, color="#333")

    plt.savefig(a.png, dpi=195, bbox_inches="tight")
    print(f"wrote {a.png}")
    plt.close(fig)

    # ---- md ----
    md = [f"# {a.adv} — prospecting audience funnel", "",
          "Max addressable (UI interest size, national, ~5x-inflated) → ~deliverable (÷5) → reached → of which "
          "High-Intent (`household_score` ≥ 8001). Reached / HI exact from CIL (Apr15–May31); base 261318 dark since ~Mar.", "",
          "| Campaign | Geo | Addressable* | ~Deliverable | Reached | HI reached | HI-share | Coverage* |",
          "|---|---|--:|--:|--:|--:|--:|--:|"]
    for r in rows:
        if r["dark"]:
            md.append(f"| {r['cid']} {r['lab']} | {r['tier']} | {kfmt(r['addr'])} | {kfmt(r['deliver'])} | "
                      f"— (dark ~Mar) | — | — | — |")
        else:
            md.append(f"| {r['cid']} {r['lab']} | {r['tier']} | {kfmt(r['addr'])} | {kfmt(r['deliver'])} | "
                      f"{kfmt(r['reach'])} | {kfmt(r['buckets']['hi'])} | {r['hishare']*100:.0f}% | {r['cov']*100:.0f}% |")
    md += ["", "*Addressable = national UI interest size (~5x-inflated); Coverage is a loose floor.", "",
           "**Read:** prospecting reaches ~80–88% High-Intent households at low coverage of the addressable pool — "
           "no hard HI ceiling, and NOT scraping low-score users. The variants' worse ROAS is net-new HI converting "
           "worse, not audience-quality erosion. Base prospecting (261318) went dark ~March; its group's later "
           "delivery is retargeting."]
    open(a.md, "w").write("\n".join(md) + "\n")
    print(f"wrote {a.md}")
    print("FINDING: prospecting reaches ~80-88% HI at low coverage of the (inflated) addressable pool. Decline = "
          "conversion efficiency on net-new HI, not audience-quality erosion. Base 261318 dark since ~Mar.")


if __name__ == "__main__":
    main()
