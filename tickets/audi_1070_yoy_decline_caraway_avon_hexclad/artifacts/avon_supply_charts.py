"""AUDI-1070 Avon supply-side charts: (1) budget-vs-spend pacing, (2) targetable
audience-size over time. Reads outputs/avon_pacing.csv + q_inv1_audsize_monthly.csv."""
import numpy as np, pandas as pd
from io import StringIO
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager, dates as mdates
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY, GRAY, RED, GREEN = "#27496D", "#9AA0A6", "#D63B2F", "#2E8B57"

def load(path, hdr_key):
    raw = open(path).read().splitlines()
    h = next(i for i, l in enumerate(raw) if l.lower().startswith(hdr_key))
    rows = []
    for l in raw[h:]:
        if l.strip() == "" or l.startswith(("---", "Waiting", "Bytes", "Time:", "Cache", "Reservation", "Index", "Logged")): break
        rows.append(l)
    return pd.read_csv(StringIO("\n".join(rows)))

# ---- Chart 1: pacing (budget vs spend) ----
p = load(D + "outputs/avon_pacing.csv", "mo")
for c in ["budget", "spend"]: p[c] = pd.to_numeric(p[c], errors="coerce")
p["mo"] = pd.to_datetime(p["mo"]); p = p.dropna(subset=["budget", "spend"])
p["pacing"] = p.spend / p.budget * 100
fig, ax = plt.subplots(figsize=(11, 5))
x = np.arange(len(p)); wd = 0.4
ax.bar(x - wd/2, p.budget/1000, wd, color=GRAY, label="Budget")
ax.bar(x + wd/2, p.spend/1000, wd, color=NAVY, label="Delivered spend")
ax2 = ax.twinx()
ax2.plot(x, p.pacing, "-o", color=RED, lw=2, ms=4, label="Pacing %")
ax2.set_ylim(0, 160); ax2.set_ylabel("Pacing (spend ÷ budget)", color=RED)
ax2.axhline(100, color=RED, ls=":", lw=1, alpha=.5)
ax.set_xticks(x); ax.set_xticklabels([d.strftime("%b'%y") for d in p.mo], rotation=60, fontsize=8)
ax.set_ylabel("$ thousands / month"); ax.legend(loc="upper left", frameon=False, fontsize=9)
ax.set_title("Avon delivers only ~40–60% of its budget — and the gap widened in 2026",
             fontsize=13.5, fontweight="bold", loc="left", y=1.08)
ax.text(0, 1.02, "Feb–May 2026: budget +8% vs 2025 but spend −14% — pacing fell from 59% to 47%. A deliverability gap, not a performance one.",
        transform=ax.transAxes, color="#666", fontsize=9.5)
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_pacing.png", dpi=200, bbox_inches="tight"); plt.close()

# ---- Chart 2: audience size over time ----
a = load(D + "outputs/q_inv1_audsize_monthly.csv", "advertiser_id")
a = a[a.advertiser_id == 31921].copy()
a["mo"] = pd.to_datetime(a["mo"]); a["pool"] = pd.to_numeric(a["avg_pool"], errors="coerce") / 1e6
a = a.sort_values("mo")
fig, ax = plt.subplots(figsize=(11, 5))
ax.plot(a.mo, a.pool, "-o", color=NAVY, lw=2.2, ms=5)
ax.fill_between(a.mo, 0, a.pool, color=NAVY, alpha=.06)
feb, jul = a.iloc[0], a[a.mo == "2025-07-01"].iloc[0]
ax.annotate(f"{feb.pool:.0f}M", (feb.mo, feb.pool), textcoords="offset points", xytext=(0, 8), fontsize=10, fontweight="bold", color=NAVY)
ax.annotate(f"{jul.pool:.0f}M", (jul.mo, jul.pool), textcoords="offset points", xytext=(0, -16), fontsize=10, fontweight="bold", color=RED)
ax.annotate("−26% (May to Jul 2025)", (pd.Timestamp("2025-06-15"), 70), color=RED, fontsize=10, fontweight="bold", ha="center")
ax.set_ylim(0, 100); ax.set_ylabel("Targetable audience pool (M IPs)")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y")); plt.xticks(rotation=60, fontsize=8)
ax.set_title("Avon's targetable audience shrank −26% in mid-2025 and never recovered",
             fontsize=13.5, fontweight="bold", loc="left", y=1.08)
ax.text(0, 1.02, "Supply-side contraction (perml audience size). Fewer high-intent IPs to buy + higher CPM means we can't fill the budget.",
        transform=ax.transAxes, color="#666", fontsize=9.5)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_audience_size.png", dpi=200, bbox_inches="tight"); plt.close()
print("wrote avon_pacing.png + avon_audience_size.png")
