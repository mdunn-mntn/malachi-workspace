"""AUDI-1070 Avon chart: ROAS vs monthly spend, colored by year. Shows ROAS is driven
by spend (saturation), and 2026 sits ON/ABOVE the same curve -> no real YoY decline.
Reads outputs/avon_monthly.csv (bq_run.sh, footer-tolerant)."""
import numpy as np, pandas as pd
from io import StringIO
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
raw = open(D + "outputs/avon_monthly.csv").read().splitlines()
hdr = next(i for i, l in enumerate(raw) if l.lower().startswith("month"))
rows = []
for l in raw[hdr:]:
    if l.strip() == "" or l.startswith(("---", "Waiting", "Bytes")): break
    rows.append(l)
df = pd.read_csv(StringIO("\n".join(rows)))
for c in ["spend", "roas", "yr"]: df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["spend", "roas"])

fig, ax = plt.subplots(figsize=(9, 5.4))
colors = {2024: "#9AA0A6", 2025: "#27496D", 2026: "#D63B2F"}
# fitted saturation curve (log-log)
b, a = np.polyfit(np.log(df.spend), np.log(df.roas), 1)
xs = np.linspace(df.spend.min(), df.spend.max(), 100)
ax.plot(xs, np.exp(a) * xs ** b, color="#888", lw=1.5, ls="--", zorder=1,
        label=f"saturation curve (ROAS ∝ spend^{b:.2f})")
for yr in [2024, 2025, 2026]:
    s = df[df.yr == yr]
    ax.scatter(s.spend, s.roas, s=90, color=colors[yr], label=str(yr), zorder=3,
               edgecolor="white", lw=1)
ax.set_xscale("log"); ax.set_xticks([9000, 12000, 18000, 27000, 37000])
ax.get_xaxis().set_major_formatter(plt.FuncFormatter(lambda v, _: f"${v/1000:.0f}k"))
ax.set_xlabel("Avon monthly spend (log)"); ax.set_ylabel("ROAS (×)")
ax.set_title("Avon's ROAS is driven by spend, not by year", fontsize=14, fontweight="bold", loc="left", y=1.10)
ax.text(0, 1.02, "2026 (red) sits on/above the same curve as 2024–25 → no real YoY decline; spend explains it",
        transform=ax.transAxes, color="#666", fontsize=10.5)
ax.legend(frameon=False, fontsize=10, loc="upper right")
for sp in ["top", "right"]: ax.spines[sp].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_roas_vs_spend.png", dpi=200, bbox_inches="tight")
print("wrote artifacts/audi_1070_avon_roas_vs_spend.png")
