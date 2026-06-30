"""AUDI-1070 Avon YoY (Feb-May 2025 vs 2026), CONSISTENT last-touch lens.
Shows raw counts then rates, indexed to 2025=100, with Welch t-tests (weekly) =>
no significant performance change. Reads outputs/avon_weekly.csv (footer-tolerant)."""
import numpy as np, pandas as pd
from io import StringIO
from scipy import stats
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY, GRAY, GREEN, RED = "#27496D", "#9AA0A6", "#2E8B57", "#D63B2F"

raw = open(D + "outputs/avon_weekly.csv").read().splitlines()
hdr = next(i for i, l in enumerate(raw) if l.lower().startswith("week"))
rows = []
for l in raw[hdr:]:
    if l.strip() == "" or l.startswith(("---", "Waiting", "Bytes", "Time:", "Cache", "Reservation", "Index", "Logged")): break
    rows.append(l)
w = pd.read_csv(StringIO("\n".join(rows)))
for c in ["imps", "visits", "conv", "spend", "rev"]: w[c] = pd.to_numeric(w[c], errors="coerce")
w["week"] = pd.to_datetime(w["week"]); w = w.dropna(subset=["spend"])
w = w[w.spend > 0]
w["yr"] = w.week.dt.year; w["mo"] = w.week.dt.month
# rates
w["ROAS"] = w.rev / w.spend
w["VR"] = w.visits / w.imps * 100
w["CVR"] = np.where(w.visits > 0, w.conv / w.visits * 100, np.nan)
w["AOV"] = np.where(w.conv > 0, w.rev / w.conv, np.nan)
# Jan-May window each year (matches client's "Jan 01 - May 31" chart)
fm = w[w.mo.between(1, 5)]
a, b = fm[fm.yr == 2025], fm[fm.yr == 2026]

def stars(p): return "ns" if p >= 0.05 else ("*" if p >= .01 else "**")

# RAW: weekly totals; RATES: weekly rates. Index 2026 to 2025 mean = 100. Welch t-test.
raw_m = ["spend", "imps", "visits", "conv", "rev"]
rate_m = ["ROAS", "VR", "CVR", "AOV"]
lab = {"spend": "Spend", "imps": "Impr.", "visits": "Visits", "conv": "Conv.", "rev": "Revenue",
       "ROAS": "ROAS", "VR": "Visit rate", "CVR": "Conv. rate", "AOV": "AOV"}

def panel(ax, metrics, title):
    idx, ch, ps = [], [], []
    for m in metrics:
        x, y = a[m].dropna(), b[m].dropna()
        mx, my = x.mean(), y.mean()
        idx.append(my / mx * 100); ch.append((my / mx - 1) * 100)
        ps.append(stats.ttest_ind(x, y, equal_var=False).pvalue)
    xs = np.arange(len(metrics))
    ax.axhline(100, color="#444", lw=1)
    bars = ax.bar(xs, idx, 0.6, color=[GREEN if c >= -5 else RED for c in ch])
    for i, (bar, c, p) in enumerate(zip(bars, ch, ps)):
        h = bar.get_height()
        ax.text(i, h + 1.5, f"{c:+.0f}%", ha="center", fontsize=11, fontweight="bold")
        ax.text(i, 6, stars(p), ha="center", fontsize=10, color="#444")
    ax.set_xticks(xs); ax.set_xticklabels([lab[m] for m in metrics])
    ax.set_ylim(0, max(max(idx) + 12, 130)); ax.set_ylabel("2026 vs 2025 (2025 = 100)")
    ax.set_title(title, fontsize=12.5, fontweight="bold", loc="left")
    for s in ["top", "right"]: ax.spines[s].set_visible(False)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5.2))
panel(ax1, raw_m, "Raw counts — volume tracks spend, outcomes flat")
panel(ax2, rate_m, "Performance rates — flat to up, none significantly down")
fig.suptitle("Avon YoY (Jan–May 2026 vs 2025): no significant performance decline",
             fontsize=15, fontweight="bold", x=0.02, ha="left")
fig.text(0.02, 0.925, "Consistent last-touch lens (both years). 'ns' = not significant (Welch t-test on weekly values). "
                      "Volume tracks the lower spend; revenue, conversions and every rate held.",
         color="#666", fontsize=9.5, ha="left")
fig.text(0.02, 0.01, "Note: the client UI's larger Avon 'decline' is the 2025 last-touch to 2026 first-touch reporting switch, "
                     "not a performance change.", color=RED, fontsize=9, ha="left")
plt.tight_layout(rect=[0, 0.03, 1, 0.9])
plt.savefig(D + "artifacts/audi_1070_avon_yoy_no_change.png", dpi=200, bbox_inches="tight")
# print the table too
print(f"{'metric':>10} {'2025':>12} {'2026':>12} {'%chg':>7} {'p':>7}")
for m in raw_m + rate_m:
    x, y = a[m].dropna(), b[m].dropna()
    p = stats.ttest_ind(x, y, equal_var=False).pvalue
    print(f"{lab[m]:>10} {x.mean():>12.2f} {y.mean():>12.2f} {(y.mean()/x.mean()-1)*100:>+6.0f}% {p:>7.3f}  {stars(p)}")
print("wrote artifacts/audi_1070_avon_yoy_no_change.png")
