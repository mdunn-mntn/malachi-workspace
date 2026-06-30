"""AUDI-1070 — Why the MoM chart (prospecting) reads low while the account ROAS is up.
Two panels: (A) ROAS by scope x year — every scope UP YoY; (B) retargeting is the hidden
revenue engine (22% of spend -> 64% of revenue). Data: outputs/avon_prospecting_vs_retargeting_split.csv.
Tufte: no gridlines/borders, direct labels, color encodes meaning, finding-as-title."""
import csv, os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

plt.rcParams["font.family"] = "Helvetica Neue"
plt.rcParams["font.size"] = 12

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "..", "outputs", "avon_prospecting_vs_retargeting_split.csv")

rows = list(csv.DictReader(open(CSV)))
def get(scope, yr, col):
    for r in rows:
        if r["scope"] == scope and r["yr"] == yr:
            return float(r[col])
    return None

NAVY, RED, GRAY, PINK = "#1f3a5f", "#e8243c", "#9aa0a6", "#d63a8f"
BG = "#FAFAFA"

fig, (axA, axB) = plt.subplots(1, 2, figsize=(15, 6.2), facecolor=BG)

# ---- Panel A: ROAS by scope x year (LT rollup; account also shows CHAPI/UI level) ----
axA.set_facecolor(BG)
scopes = ["prospecting", "retargeting", "account_total"]
labels = ["Prospecting\n(the MoM chart)", "Retargeting\n(hidden from chart)", "Account total\n(the UI cards)"]
x = range(len(scopes))
w = 0.38
roas25 = [get(s, "2025", "roas_lt") for s in scopes]
roas26 = [get(s, "2026", "roas_lt") for s in scopes]
b1 = axA.bar([i - w/2 for i in x], roas25, w, color=GRAY, label="2025")
b2 = axA.bar([i + w/2 for i in x], roas26, w, color=RED, label="2026")
for bars in (b1, b2):
    for bar in bars:
        axA.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.8,
                 f"{bar.get_height():.1f}x", ha="center", va="bottom",
                 fontsize=11, fontweight="bold", color="#222")
# annotate the UI/CHAPI account number
axA.annotate("UI cards show 22.1x to 26.4x\n(CHAPI attribution level)",
             xy=(2 + w/2, get("account_total","2026","roas_lt")),
             xytext=(1.55, 44), fontsize=10, color=NAVY,
             arrowprops=dict(arrowstyle="->", color=NAVY, lw=1.2))
axA.set_xticks(list(x)); axA.set_xticklabels(labels, fontsize=10.5)
axA.set_ylabel("ROAS (revenue ÷ spend, pooled)", fontsize=11, color="#444")
axA.set_ylim(0, 60)
axA.legend(frameon=False, loc="upper left", fontsize=11)
for s in ("top", "right", "left"): axA.spines[s].set_visible(False)
axA.tick_params(left=False)
axA.set_yticks([])
axA.set_title("Every scope's ROAS rose YoY — including the prospecting line the chart shows",
              fontsize=12.5, fontweight="bold", color="#111", loc="left", pad=12)

# ---- Panel B: spend vs revenue share, 2025 (retargeting = hidden engine) ----
axB.set_facecolor(BG)
cats = ["Share of\nSPEND", "Share of\nREVENUE"]
prosp = [get("prospecting","2025","spend_share_pct"), get("prospecting","2025","revenue_share_pct")]
retar = [get("retargeting","2025","spend_share_pct"), get("retargeting","2025","revenue_share_pct")]
y = range(len(cats))
axB.barh(y, prosp, color=PINK, label="Prospecting (the chart)")
axB.barh(y, retar, left=prosp, color=NAVY, label="Retargeting (hidden)")
for i in range(len(cats)):
    axB.text(prosp[i]/2, i, f"{prosp[i]:.0f}%", ha="center", va="center",
             color="white", fontweight="bold", fontsize=12)
    axB.text(prosp[i] + retar[i]/2, i, f"{retar[i]:.0f}%", ha="center", va="center",
             color="white", fontweight="bold", fontsize=12)
axB.set_yticks(list(y)); axB.set_yticklabels(cats, fontsize=11)
axB.set_xlim(0, 100); axB.set_xticks([])
axB.invert_yaxis()
axB.legend(frameon=False, loc="lower center", bbox_to_anchor=(0.5, -0.18),
           ncol=2, fontsize=10.5)
for s in ("top", "right", "bottom", "left"): axB.spines[s].set_visible(False)
axB.tick_params(left=False)
axB.set_title("Retargeting: 22% of spend, 64% of revenue (50× ROAS) — the chart hides it",
              fontsize=12.5, fontweight="bold", color="#111", loc="left", pad=12)

fig.suptitle("The chart looks flat because it's prospecting-only — the account ROAS is up +19%",
             fontsize=15, fontweight="bold", color="#111", x=0.5, y=0.99)
fig.text(0.5, 0.005,
         "Avon (31921), Jan–May 2025 vs 2026 · last-touch BigQuery rollup · same CHAPI source as the UI, prospecting filter removed",
         ha="center", fontsize=9.5, color=GRAY)
fig.tight_layout(rect=[0, 0.02, 1, 0.95])
OUT = os.path.join(HERE, "audi_1070_avon_scope_decomposition.png")
fig.savefig(OUT, dpi=200, facecolor=BG)
print("wrote", OUT)
