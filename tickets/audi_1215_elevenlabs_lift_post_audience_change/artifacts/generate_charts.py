"""Charts for the AUDI-1215 ElevenLabs deck, reading outputs/*.csv|json. Deck versions: no chart titles (slide H2 carries the finding)."""
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

T = "tickets/audi_1215_elevenlabs_lift_post_audience_change"
NAVY, BLUE, GRAY, RED, BG = "#1B2A4A", "#2E5090", "#999999", "#D63B2F", "#FAFAFA"
plt.rcParams.update({"font.family": "Helvetica Neue", "figure.facecolor": BG, "axes.facecolor": BG,
                     "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
                     "text.color": "#222222", "axes.edgecolor": "#CCCCCC"})

def save(fig, name):
    fig.savefig(f"{T}/artifacts/{name}", dpi=200, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(name)

d = pd.read_csv(f"{T}/outputs/audi_1215_daily_lift_series_raw.csv", parse_dates=["dt"])
wide = d.pivot(index="dt", columns="arm", values=["n_ip", "visited"]).sort_index()
roll = wide.rolling(7, center=True, min_periods=5).sum()
lift = (roll[("visited", "submitted")] / roll[("n_ip", "submitted")]) / (roll[("visited", "ghost")] / roll[("n_ip", "ghost")]) - 1

fig, ax = plt.subplots(figsize=(11, 4.6))
ax.axvspan(pd.Timestamp("2026-07-01"), pd.Timestamp("2026-07-10"), color="#E8E8E8", zorder=0)
ax.axhline(0, color="#CCCCCC", lw=0.8)
pre_m, post_m = lift[:"2026-06-30"], lift["2026-07-11":]
ax.plot(lift.index, lift * 100, color=NAVY, lw=2.4, solid_capstyle="round")
for x, lab, dy in [("2026-06-30", "audience swap", 4), ("2026-07-16", "custom segments", 4), ("2026-07-24", "segments added", 20), ("2026-07-29", "targeting rewrite", 4)]:
    ax.axvline(pd.Timestamp(x), color=GRAY, lw=0.9, ls=(0, (3, 3)), zorder=1)
    ax.annotate(lab, (pd.Timestamp(x), ax.get_ylim()[1]), xytext=(4, -dy - 4), textcoords="offset points",
                fontsize=8.5, color="#666666", va="top")
ax.annotate("pre avg +11.1%", (pd.Timestamp("2026-06-26"), 11.1), xytext=(0, -26), textcoords="offset points",
            fontsize=10.5, color=RED, fontweight="bold", ha="center")
ax.annotate("post avg +16.5%", (pd.Timestamp("2026-08-01"), 16.5), xytext=(0, 16), textcoords="offset points",
            fontsize=10.5, color=RED, fontweight="bold", ha="center")
ax.set_ylabel("visit lift vs holdout (7-day window)", fontsize=9.5, color="#666666")
ax.yaxis.set_major_formatter(lambda v, _: f"{v:+.0f}%")
ax.tick_params(length=0, labelsize=9)
save(fig, "audi_1215_chart_daily_lift.png")

fig, ax = plt.subplots(figsize=(8.2, 4.4))
bars = [("Visits\npre", 11.14, 3.42, NAVY, "+11.1%"), ("Visits\npost", 16.46, 6.86, NAVY, "+16.5%"),
        ("Conversions\npre", 11.25, 24.6, GRAY, "+11%"), ("Conversions\npost", 34.65, 46.7, GRAY, "+35%")]
xs = [0, 1, 2.4, 3.4]
for x, (lab, v, ci, c, txt) in zip(xs, bars):
    ax.bar(x, v, width=0.72, color=c)
    ax.errorbar(x, v, yerr=ci, color="#555555", capsize=4, lw=1.2)
    ax.text(x, v + ci + 3, txt, ha="center", fontsize=12, fontweight="bold",
            color=NAVY if c == NAVY else "#666666")
ax.axhline(0, color="#CCCCCC", lw=0.8)
ax.set_xticks(xs, [b[0] for b in bars], fontsize=10)
ax.text(0.5, -26, "significant, p < 0.000003", ha="center", fontsize=9, color=NAVY)
ax.text(2.9, -26, "not yet significant (low base rate)", ha="center", fontsize=9, color="#888888")
ax.set_yticks([])
ax.set_ylim(-32, 95)
save(fig, "audi_1215_chart_prepost_lift.png")

g = json.load(open(f"{T}/outputs/audi_1215_gold_strata.json"))
freq = [r for r in g["results_strata_cg_122748"] if r["stratum_type"] == "bid_count"]
order = {"1": 0, "2-3": 1, "4-10": 2, "11+": 3}
freq.sort(key=lambda r: order[r["stratum_value"]])
fig, ax = plt.subplots(figsize=(8.2, 4.4))
for i, r in enumerate(freq):
    v = float(r["rel_itt"]) * 100
    ci = 1.96 * float(r["se"]) / float(r["rate_holdout"]) * 100
    c = RED if v < 0 else NAVY
    ax.bar(i, v, width=0.72, color=c)
    ax.errorbar(i, v, yerr=ci, color="#555555", capsize=4, lw=1.2)
    ax.text(i, v + (ci + 2 if v > 0 else -ci - 6), f"{v:+.0f}%", ha="center", fontsize=12, fontweight="bold", color=c)
ax.axhline(0, color="#CCCCCC", lw=0.8)
ax.set_xticks(range(4), [f"{r['stratum_value']}\nexposures" for r in freq], fontsize=10)
ax.set_yticks([])
ax.set_ylim(-32, 30)
save(fig, "audi_1215_chart_frequency_lift.png")

fig, ax = plt.subplots(figsize=(7.6, 4.2))
for x, (lab, v, txt, c) in enumerate([("Conversions", 2000, "$2M/month", GRAY), ("Visits", 36, "$36K/month", NAVY)]):
    ax.bar(x, v, width=0.6, color=c)
    ax.text(x, v + 60, txt, ha="center", fontsize=13, fontweight="bold", color=c if c == NAVY else "#666666")
ax.set_xticks([0, 1], ["Conversions\n(0.06% base rate)", "Visits\n(0.9% base rate)"], fontsize=10)
ax.set_yticks([])
ax.set_ylim(0, 2400)
ax.text(0.5, 2280, "spend needed to detect a 5% lift", ha="center", fontsize=10, color="#666666")
save(fig, "audi_1215_chart_power_cost.png")

fig, ax = plt.subplots(figsize=(8.6, 4.2))
groups = [("Pre", 0.916, 0.824, "+11.1% lift"), ("Post", 0.158, 0.136, "+16.5% lift")]
for i, (lab, r, h, gap) in enumerate(groups):
    x0 = i * 1.6
    ax.bar(x0, r, width=0.55, color=NAVY)
    ax.bar(x0 + 0.62, h, width=0.55, color=GRAY)
    ax.text(x0, r + 0.03, f"{r:.3f}%", ha="center", fontsize=10.5, fontweight="bold", color=NAVY)
    ax.text(x0 + 0.62, h + 0.03, f"{h:.3f}%", ha="center", fontsize=10.5, fontweight="bold", color="#666666")
    ax.text(x0 + 0.31, max(r, h) + 0.12, gap, ha="center", fontsize=11, fontweight="bold", color=RED)
ax.text(0, -0.09, "reached", ha="center", fontsize=9.5, color=NAVY)
ax.text(0.62, -0.09, "holdout", ha="center", fontsize=9.5, color="#666666")
ax.text(1.6, -0.09, "reached", ha="center", fontsize=9.5, color=NAVY)
ax.text(2.22, -0.09, "holdout", ha="center", fontsize=9.5, color="#666666")
ax.set_xticks([0.31, 1.91], ["Pre: old audience", "Post: precision audience"], fontsize=10)
ax.tick_params(axis="x", pad=18, length=0)
ax.set_yticks([])
ax.set_ylim(-0.12, 1.15)
ax.text(1.11, 1.05, "both groups fell ~6x; the lift gap persisted", ha="center", fontsize=10, color="#666666")
save(fig, "audi_1215_chart_baseline_collapse.png")

import numpy as np
pw = pd.read_csv(f"{T}/outputs/audi_1215_power_table.csv")
spend = np.linspace(200_000, 3_000_000, 200)
fig, ax = plt.subplots(figsize=(9.6, 4.6))
for _, r in pw.iterrows():
    mde = r["mde_raw_pct"] * np.sqrt(r["actual_spend"] / spend)
    is_conv = "Conversion" in r["regime"]
    c = GRAY if is_conv else NAVY
    ax.plot(spend / 1e6, mde, color=c, lw=2.4)
    ax.text(3.05, mde[-1], "conversions" if is_conv else "visits", fontsize=10.5, fontweight="bold", color=c, va="center")
ax.axvline(1.0, color="#CCCCCC", lw=1.0, ls=(0, (3, 3)))
ax.text(1.0, 26, "current spend", fontsize=9.5, color="#666666", ha="center")
ax.annotate("~1% lift detectable", (1.0, 1.04), xytext=(28, 16), textcoords="offset points", fontsize=10.5, color=NAVY, fontweight="bold")
ax.annotate("only 7%+ detectable", (1.0, 7.4), xytext=(28, 12), textcoords="offset points", fontsize=10.5, color="#666666", fontweight="bold")
ax.set_xlabel("monthly spend ($M)", fontsize=9.5, color="#666666")
ax.set_ylabel("smallest detectable lift", fontsize=9.5, color="#666666")
ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
ax.set_xlim(0.2, 3.45)
ax.set_ylim(0, 28)
ax.tick_params(length=0, labelsize=9)
save(fig, "audi_1215_chart_mde_curve.png")
