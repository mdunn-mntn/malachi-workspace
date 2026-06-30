"""AUDI-1070 — Avon (31921) verified-case deck charts. WINDOW = Jan-May 2025 vs Jan-May 2026
(five complete months both years; excludes partial June). Tufte: no chartjunk, direct labels,
color encodes meaning, NO embedded title (RevealJS slide H2 carries the finding).
Reads outputs/avon_case_*.csv. 200 DPI, #FAFAFA. Verified by workflow wf_733743cd-c9c + Jan-May re-pull."""
import numpy as np, pandas as pd
from io import StringIO
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
                     "savefig.facecolor": "#FAFAFA", "axes.edgecolor": "#888"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
ART = D + "artifacts/"
NAVY, GRAY, GREEN, RED, LIGHT = "#27496D", "#9AA0A6", "#2E8B57", "#D63B2F", "#CBD2D9"

def read_csv(name):
    raw = open(D + "outputs/" + name).read().splitlines()
    rows = [l for l in raw if l.strip() and not l.startswith(
        ("---", "Waiting", "Bytes", "Time:", "Cache", "Reservation", "Index", "Logged", "Optimiz", "  S0"))]
    return pd.read_csv(StringIO("\n".join(rows)))

def finish(ax):
    for s in ["top", "right"]: ax.spines[s].set_visible(False)

# ===================================================== Chart 1 — HEADLINE (Jan-May)
y = read_csv("avon_case_janmay_yearly.csv").set_index("yr")
def d(col): return (y.loc[2026, col] / y.loc[2025, col] - 1) * 100
metrics = [("Reach (users)", d("reach")), ("Visits", d("visits")), ("Impressions", d("impr")),
           ("Spend", d("spend")), ("Conversions", d("conv")), ("Revenue", d("rev")),
           ("ROAS", d("roas")), ("Conv. rate", (y.loc[2026,"cvr"]/y.loc[2025,"cvr"]-1)*100)]
metrics.sort(key=lambda t: t[1])
labels = [m[0] for m in metrics]; vals = [m[1] for m in metrics]
colors = [GRAY if v < 0 else GREEN for v in vals]
fig, ax = plt.subplots(figsize=(11, 5.6))
ypos = np.arange(len(vals))
ax.barh(ypos, vals, color=colors, height=0.66)
ax.axvline(0, color="#444", lw=1.1)
for i, v in enumerate(vals):
    ax.text(v + (1.1 if v >= 0 else -1.1), i, f"{v:+.0f}%", va="center",
            ha="left" if v >= 0 else "right", fontsize=12.5, fontweight="bold",
            color=GREEN if v >= 0 else "#5f6368")
ax.set_yticks(ypos); ax.set_yticklabels(labels, fontsize=12)
ax.set_xlim(-32, 30); ax.set_xticks([])
ax.text(-26, len(vals)-0.3, "VOLUME  (down)", fontsize=11, color="#5f6368", fontweight="bold")
ax.text(18, 0.0, "MONEY  (up)", fontsize=11, color=GREEN, fontweight="bold", ha="center")
finish(ax); ax.spines["left"].set_visible(False); ax.tick_params(length=0)
fig.text(0.5, 0.015, "Avon Jan-May 2025 vs Jan-May 2026 (last-touch). Spend -12%; fewer impressions/users, but more conversions, revenue, ROAS. Visit rate flat (+0.4%).",
         color="#666", fontsize=9.3, ha="center")
plt.tight_layout(rect=[0, 0.04, 1, 1])
plt.savefig(ART + "avon_case_1_headline.png", dpi=200, bbox_inches="tight"); plt.close()

# ===================================================== Chart 2 — TREND (full timeline, Jan-May ROAS callouts)
m = read_csv("avon_case_raw_counts_monthly.csv")
m["dt"] = pd.to_datetime(m["month"] + "-01")
m["yint"] = m["month"].str[:4].astype(int); m["moint"] = m["month"].str[5:7].astype(int)
jm = m[m["moint"].between(1, 5)]
roas_jm = jm.groupby("yint").apply(lambda g: g["revenue"].sum() / g["spend"].sum())
fig, ax = plt.subplots(figsize=(11.5, 5.4))
ax.bar(m["dt"], m["spend"], width=22, color=LIGHT, label="Monthly spend")
ax.set_ylabel("Monthly spend ($)", color="#888", fontsize=11)
ax.tick_params(axis="y", colors="#888"); ax.set_ylim(0, m["spend"].max()*1.15)
ax2 = ax.twinx()
ax2.plot(m["dt"], m["roas"], color=GREEN, lw=2.6, marker="o", ms=4, label="ROAS")
ax2.set_ylabel("ROAS (revenue / spend)", color=GREEN, fontsize=11)
ax2.tick_params(axis="y", colors=GREEN); ax2.set_ylim(0, m["roas"].max()*1.12)
for yr, x in [(2024,"2024-03-15"),(2025,"2025-03-15"),(2026,"2026-03-15")]:
    if yr in roas_jm.index:
        ax2.text(pd.to_datetime(x), m["roas"].max()*1.07, f"Jan-May {roas_jm[yr]:.1f}×",
                 ha="center", fontsize=9.5, color=NAVY, fontweight="bold")
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
fig.text(0.5, 0.015, "ROAS stays in a healthy 10-38x band the entire period; its Jan-May average rises 17.3x (2025) to 20.7x (2026). Volume swings with budget; performance does not erode.",
         color="#666", fontsize=9.2, ha="center")
plt.tight_layout(rect=[0, 0.04, 1, 1])
plt.savefig(ART + "avon_case_2_trend.png", dpi=200, bbox_inches="tight"); plt.close()

# ===================================================== Chart 3 — INFLECTION (quarterly, window-independent)
q = read_csv("avon_case_quarterly.csv")
q["reach_per_1k"] = q["reach"] / (q["spend"] / 1000)
fig, ax = plt.subplots(figsize=(11.5, 5.4))
bar_c = [RED if yq == "2026-Q2" else LIGHT for yq in q["yq"]]
ax.bar(q["yq"], q["reach_per_1k"]/1000, color=bar_c, width=0.62)
for i, v in enumerate(q["reach_per_1k"]/1000):
    ax.text(i, v+0.6, f"{v:.0f}k", ha="center", fontsize=9.5,
            color=RED if q["yq"].iloc[i]=="2026-Q2" else "#5f6368",
            fontweight="bold" if q["yq"].iloc[i]=="2026-Q2" else "normal")
ax.set_ylabel("Unique users reached per $1k spend", fontsize=11)
ax.set_ylim(0, (q["reach_per_1k"].max()/1000)*1.2)
ax2 = ax.twinx()
ax2.plot(q["yq"], q["freq"], color=NAVY, lw=2.4, marker="s", ms=5, label="Frequency")
ax2.set_ylabel("Frequency (impressions / user)", color=NAVY, fontsize=11)
ax2.tick_params(axis="y", colors=NAVY); ax2.set_ylim(0, q["freq"].max()*1.35)
ax.annotate("2026: reach/$ falls,\nfrequency rises",
            xy=(9, q["reach_per_1k"].iloc[9]/1000), xytext=(5.4, 12),
            fontsize=10, color=RED, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=RED, lw=1.4))
for s in ["top"]: ax.spines[s].set_visible(False); ax2.spines[s].set_visible(False)
ax.tick_params(axis="x", labelsize=9.5)
fig.text(0.5, 0.015, "Jan-May 2025 each $1k reached ~36k users; Jan-May 2026 the same dollars reach ~31k as frequency rises (+13%). Extra delivery now buys repetition, not new users - a reach ceiling, not a performance loss.",
         color="#666", fontsize=9.0, ha="center")
plt.tight_layout(rect=[0, 0.045, 1, 1])
plt.savefig(ART + "avon_case_3_inflection.png", dpi=200, bbox_inches="tight"); plt.close()

# ===================================================== Chart 4 — NO EXPANSION (Jan-May)
fl = read_csv("avon_case_flagship_janmay.csv").iloc[0]
pc = read_csv("avon_case_prospecting_change.csv")
fig, (axA, axB) = plt.subplots(1, 2, figsize=(12, 5.2), gridspec_kw={"width_ratios":[1, 1.25]})
axA.bar(["Jan-May\n2025","Jan-May\n2026"], [fl["imp_25"]/1e6, fl["imp_26"]/1e6], color=[LIGHT, NAVY], width=0.55)
axA.text(0, fl["imp_25"]/1e6+0.04, f"{fl['imp_25']/1e6:.2f}M\nROAS {fl['roas_25']:.2f}x", ha="center", fontsize=10, color="#5f6368")
axA.text(1, fl["imp_26"]/1e6+0.04, f"{fl['imp_26']/1e6:.2f}M\nROAS {fl['roas_26']:.2f}x", ha="center", fontsize=10, color=NAVY, fontweight="bold")
axA.set_ylim(0, fl["imp_25"]/1e6*1.3); axA.set_ylabel("Prospecting impressions (M)", fontsize=10.5)
axA.set_title("Avon's only prospecting campaign\ncontracted -26%, ROAS rose", fontsize=11, color=NAVY, loc="center", pad=8)
finish(axA); axA.tick_params(length=0)
order = pc.sort_values("prospecting_impr_chg_millions")
cols = [GREEN if v < 1 else GRAY for v in order["prospecting_impr_chg_millions"]]
axB.barh(order["advertiser"], order["prospecting_impr_chg_millions"], color=cols, height=0.6)
for i, v in enumerate(order["prospecting_impr_chg_millions"]):
    axB.text(v + (0.5 if v >= 0 else -0.5), i, f"{v:+.1f}M", va="center",
             ha="left" if v >= 0 else "right", fontsize=11.5, fontweight="bold",
             color=GREEN if v < 1 else "#5f6368")
axB.axvline(0, color="#444", lw=1)
axB.set_xlim(-4, 24); axB.set_xticks([])
axB.set_title("Change in prospecting impressions, Jan-May 25 to 26\nAvon did NOT expand; the others added 9-19M", fontsize=11, color=NAVY, loc="center", pad=8)
finish(axB); axB.spines["left"].set_visible(False); axB.tick_params(length=0)
fig.text(0.5, 0.015, "100% of Avon's 2026 impressions came from campaigns also active in 2025; zero from new campaigns. Caraway/HexClad shown for contrast.",
         color="#666", fontsize=9.2, ha="center")
plt.tight_layout(rect=[0, 0.04, 1, 1])
plt.savefig(ART + "avon_case_4_no_expansion.png", dpi=200, bbox_inches="tight"); plt.close()

# ===================================================== Chart 5 — AUDIENCE TIMELINE (window-independent)
fig, ax = plt.subplots(figsize=(11.5, 4.6))
ax.axhline(0, color="#888", lw=2, zorder=1)
events = [
    ("2024-06", "DS13 Vertical\n+ CRM/1P suppress", GRAY, 1),
    ("2024-10", "DS19 MNTN Matched\nadded (DS13 dropped)", NAVY, -1),
    ("2025-04", "New targeting\nschema (v2)", GRAY, 1),
    ("2025-09", "RTC conquest\nscoring ON", GREEN, -1),
    ("2026-06", "Latest:\nMNTN-Matched + RTC", NAVY, 1),
]
xs = [pd.to_datetime(e[0]+"-01") for e in events]
ax.set_xlim(pd.to_datetime("2024-03-01"), pd.to_datetime("2026-09-01"))
for (lab, x, col, side) in [(e[1], xs[i], e[2], e[3]) for i, e in enumerate(events)]:
    ax.scatter([x], [0], s=130, color=col, zorder=3, edgecolor="white", linewidth=1.5)
    ax.annotate(lab, xy=(x, 0), xytext=(x, side*0.7), ha="center", va="center",
                fontsize=10, color=col, fontweight="bold",
                arrowprops=dict(arrowstyle="-", color=col, lw=1.2))
ax.set_ylim(-1.25, 1.25); ax.set_yticks([])
for s in ["top","right","left"]: ax.spines[s].set_visible(False)
ax.text(pd.to_datetime("2024-03-01"), -1.18, "Fangorn (DS46): never used  ·  LiveRamp DS35: only on $0-spend campaigns",
        fontsize=10, color=RED, fontweight="bold")
fig.text(0.5, 0.015, "Honest disclosure: Avon's prospecting audience is MNTN-derived but NOT static - refined toward higher intent. Any YoY spans two audience/scoring regimes.",
         color="#666", fontsize=9.2, ha="center")
plt.tight_layout(rect=[0, 0.05, 1, 1])
plt.savefig(ART + "avon_case_5_audience_timeline.png", dpi=200, bbox_inches="tight"); plt.close()

# ===================================================== Chart 6 — TRIANGULATION (Jan-May)
t = read_csv("avon_case_triangulation.csv")
fig, ax = plt.subplots(figsize=(10.5, 5.2))
xpos = np.arange(len(t)); w = 0.36
ax.bar(xpos - w/2, t["rollup_yoy"], w, color=NAVY, label="Rollup (sum_by_advertiser_by_day)")
ax.bar(xpos + w/2, t["independent_yoy"], w, color=GREEN, label="Independent log table")
for i, r in t.iterrows():
    ax.text(i - w/2, r["rollup_yoy"] + (0.4 if r["rollup_yoy"]>=0 else -0.4), f"{r['rollup_yoy']:+.1f}%",
            ha="center", va="bottom" if r["rollup_yoy"]>=0 else "top", fontsize=10.5, color=NAVY, fontweight="bold")
    ax.text(i + w/2, r["independent_yoy"] + (0.4 if r["independent_yoy"]>=0 else -0.4), f"{r['independent_yoy']:+.1f}%",
            ha="center", va="bottom" if r["independent_yoy"]>=0 else "top", fontsize=10.5, color=GREEN, fontweight="bold")
ax.axhline(0, color="#444", lw=1)
ax.set_xticks(xpos); ax.set_xticklabels([f"{r['metric']}\n(vs {r['independent_source']})" for _, r in t.iterrows()], fontsize=10.5)
ax.set_ylabel("YoY change Jan-May 25 to 26 (%)", fontsize=11)
ax.set_ylim(-19, 11); ax.legend(frameon=False, fontsize=10, loc="lower right")
finish(ax); ax.tick_params(length=0)
fig.text(0.5, 0.015, "Three independent tables, one signature: visits down double-digit, revenue & conversions up low-single-digit. The case does not depend on one source.",
         color="#666", fontsize=9.3, ha="center")
plt.tight_layout(rect=[0, 0.04, 1, 1])
plt.savefig(ART + "avon_case_6_triangulation.png", dpi=200, bbox_inches="tight"); plt.close()

print("wrote 6 Jan-May charts: avon_case_{1_headline,2_trend,3_inflection,4_no_expansion,5_audience_timeline,6_triangulation}.png")
