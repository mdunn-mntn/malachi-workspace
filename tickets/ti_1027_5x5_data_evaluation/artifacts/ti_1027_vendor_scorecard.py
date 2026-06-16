#!/usr/bin/env python3
"""TI-1027 — MM site-visit DDP quality scorecard. Joins scale + uniqueness + cost; rates each provider.
Data: outputs/ti_1027_scale_per_ds_2026-06-15.csv (1d), ti_1027_vendor_uniqueness_comparison_7d.csv (7d),
      ti_1027_vendor_cost.csv (from tpa.direct_data_partners)."""
import csv, math, os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

OUT = os.path.join(os.path.dirname(__file__), "..", "outputs")
ART = os.path.dirname(__file__)
for f in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(f.lower() in n.lower() for n in {fp.name for fp in font_manager.fontManager.ttflist}):
        plt.rcParams["font.family"] = f; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#CCCCCC", "axes.grid": False})
RED, NAVY, GRAY, TEAL = "#D1495B", "#1B3A5B", "#9AA5B1", "#5B8A72"

def read(name): return {r["data_source_id"]: r for r in csv.DictReader(open(os.path.join(OUT, name)))}
scale, uniq, cost = (read(n) for n in ["ti_1027_scale_per_ds_2026-06-15.csv",
                                       "ti_1027_vendor_uniqueness_comparison_7d.csv", "ti_1027_vendor_cost.csv"])

rows = []
for ds, c in cost.items():
    u, s = uniq[ds], scale[ds]
    total = int(u["total_domains"]); classified = int(u["classified_domains"])
    rows.append({
        "ds": ds, "partner": c["partner"], "internal": c["internal"] == "1",
        "billing": c["billing_type"], "cpm": c["fixed_cpm"],
        "rows_day": int(s["n_rows"]), "ips_day": int(s["distinct_ips"]),
        "pct_path": float(s["pct_with_path"]),
        "total_domains": total, "classified": classified,
        "class_rate": round(100*classified/total, 1),
        "pct_unique": float(u["pct_unique"]), "unique_classified": int(u["unique_classified"]),
    })

ext = [r for r in rows if not r["internal"]]
lo, hi = min(math.log10(r["unique_classified"]+1) for r in ext), max(math.log10(r["unique_classified"]+1) for r in ext)
for r in rows:
    vnorm = (math.log10(r["unique_classified"]+1) - lo) / (hi - lo) if hi > lo else 0
    r["score"] = round(100*(0.55*vnorm + 0.25*r["pct_unique"]/100 + 0.20*r["class_rate"]/100), 1)

def verdict(r):
    if r["internal"]: return "BASELINE (internal, $0)"
    uc, pu, rd = r["unique_classified"], r["pct_unique"], r["rows_day"]
    if r["billing"] == "flat_fee":
        if uc >= 20000: return "KEEP — high unique value, fixed cost"
        if uc >= 2000:  return "KEEP — efficient (fixed cost)"
        return "REVIEW — negligible contribution"
    if pu < 10 and rd > 5e7: return "DROP-CANDIDATE — pay-per-use, ~fully redundant"
    if pu < 10:              return "REVIEW — low uniqueness"
    if rd > 2e8 and pu < 40: return "REVIEW — high CPM volume for modest uniqueness"
    if uc >= 4000:           return "KEEP — unique, low volume"
    return "REVIEW"
for r in rows: r["verdict"] = verdict(r)

rows.sort(key=lambda r: (-r["score"]))
# write scorecard csv
cols = ["ds","partner","internal","billing","cpm","rows_day","ips_day","pct_path",
        "total_domains","classified","class_rate","pct_unique","unique_classified","score","verdict"]
with open(os.path.join(OUT, "ti_1027_vendor_scorecard.csv"), "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=cols); w.writeheader()
    for r in rows: w.writerow({k: r[k] for k in cols})
print("scorecard rows (by score):")
for r in rows:
    print(f'  {r["partner"]:<14} score={r["score"]:>5}  unique_classified={r["unique_classified"]:>7,}  '
          f'unique%={r["pct_unique"]:>5}  classRate={r["class_rate"]:>5}  {r["billing"]:<10} {r["verdict"]}')

# --- quadrant: value (y, log) vs non-redundancy (x), color=cost structure, size=scale ---
fig, ax = plt.subplots(figsize=(9.5, 6))
def col(r):
    if r["internal"]: return TEAL
    if r["ds"] == "25": return RED
    return NAVY if r["billing"] == "flat_fee" else GRAY
for r in rows:
    x, y = r["pct_unique"], r["unique_classified"]+1
    ax.scatter(x, y, s=40+ (r["rows_day"]/1e6)*1.1, color=col(r), alpha=0.85, edgecolor="white", linewidth=0.8, zorder=3)
    dy = 1.18 if r["partner"] not in ("Cybba","Sovrn") else (1.6 if r["partner"]=="Cybba" else 0.62)
    ax.annotate(r["partner"], (x, y*dy), ha="center", fontsize=9,
                color=col(r), fontweight="bold" if r["ds"]=="25" else "normal", zorder=4)
ax.set_yscale("log"); ax.set_ylim(60, 400000)
ax.set_xlabel("Non-redundancy — % of vendor's domains unique to it", fontsize=10)
ax.set_ylabel("Net value — unique MM-usable domains (log)", fontsize=10)
for sp in ("top","right"): ax.spines[sp].set_visible(False)
ax.axhline(2000, color="#DDD", lw=1, ls="--", zorder=1)
ax.text(99, 2300, "value floor", ha="right", color="#999", fontsize=8)
ax.set_title("Provider scorecard — keep the unique flat-fee feeds, review the redundant per-use ones",
             fontsize=13, fontweight="bold", loc="left", pad=26)
ax.text(0, 1.012, "MM site-visit DDPs · 7-day window · red = 5x5, navy = flat-fee, gray = $0.50 CPM (per-use), green = internal · bubble = daily volume",
        transform=ax.transAxes, color="#666", fontsize=9, va="top")
fig.tight_layout(); fig.savefig(os.path.join(ART, "ti_1027_chart_scorecard.png"), dpi=200); plt.close(fig)
print("\nchart: ti_1027_chart_scorecard.png")
