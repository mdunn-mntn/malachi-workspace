"""TI-1044 charts — Tufte-style. Reads outputs/ CSVs, writes PNGs to artifacts/.
Charts:
  1. power_contrast  — spend-to-detect-5% : visits vs CVR vs actual spend (the killer chart)
  2. visit_vs_cvr    — daily visit rate (clean, measurable) vs CVR (noisy, flat) as spend scaled
  3. mde_curve       — MDE vs monthly budget for CVR + visits, current ~$1M position marked
"""
import csv, importlib.util
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

plt.rcParams.update({
    "font.family": "Helvetica Neue, Helvetica, Arial, sans-serif",
    "figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA",
    "axes.edgecolor": "#888", "axes.linewidth": 0.8, "savefig.dpi": 200,
    "axes.spines.top": False, "axes.spines.right": False,
})
NAVY, RED, GRAY, MINT = "#1f3a5f", "#c0392b", "#9aa0a6", "#16a085"
BASE = "/Users/malachi/Developer/work/mntn/workspace/tickets/ti_1044_elevenlabs_ctv_incrementality"

# ---- load power table + calculator
spec = importlib.util.spec_from_file_location("mde",
    "/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts/ti_884_mde_calculator.py")
mde = importlib.util.module_from_spec(spec); spec.loader.exec_module(mde)
pt = {r["regime"]: r for r in csv.DictReader(open(f"{BASE}/outputs/ti_1044_power_table.csv"))}
ACTUAL = 1.01e6

# ============ Chart 1: spend-to-detect-5% contrast ============
fig, ax = plt.subplots(figsize=(9, 5.2))
labels = ["Visit rate\n(IVR, 3.07%)", "Conversion rate\n(CVR, 0.062%)"]
vals = [float(pt["Visit rate (IVR)"]["spend_5pct"]), float(pt["Conversion rate (CVR)"]["spend_5pct"])]
bars = ax.bar(labels, vals, color=[MINT, RED], width=0.55, zorder=3)
ax.axhline(ACTUAL, color=NAVY, ls="--", lw=1.6, zorder=2)
ax.text(1.46, ACTUAL, f"  ElevenLabs actual spend ≈ ${ACTUAL/1e6:.1f}M/mo", va="center", ha="left",
        color=NAVY, fontsize=10, fontweight="bold")
for b, v in zip(bars, vals):
    ax.text(b.get_x()+b.get_width()/2, v*1.04, f"${v/1e3:,.0f}K" if v < 1e6 else f"${v/1e6:.2f}M",
            ha="center", va="bottom", fontsize=13, fontweight="bold",
            color=MINT if v < 1e6 else RED)
ax.set_ylabel("Monthly spend required to detect a 5% lift (80% power)")
ax.set_ylim(0, max(vals)*1.22)
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x/1e6:.1f}M" if x else "0"))
ax.set_title("Visits are measurable. Conversions are not.", fontsize=15, fontweight="bold",
             loc="left", pad=34)
ax.text(0, 1.05, "Same 5% lift, same ~$1M spend — well within reach for visits, ~half of what CVR needs.",
        transform=ax.transAxes, fontsize=10, color="#555")
fig.tight_layout(); fig.savefig(f"{BASE}/artifacts/ti_1044_chart_power_contrast.png"); plt.close(fig)

# ============ Chart 2: daily visit rate vs CVR as spend scaled ============
rows = [r for r in csv.DictReader(open(f"{BASE}/outputs/ti_1044_daily_ctv_panel.csv")) if r.get("dt")]
dts = [datetime.strptime(r["dt"], "%Y-%m-%d") for r in rows]
imps = [float(r["ctv_imps"]) for r in rows]
uniq = [float(r["adv_uniques"]) for r in rows]
vis = [float(r["site_visitors"]) for r in rows]
conv = [float(r["view_conv"]) + float(r["click_conv"]) for r in rows]
vr = [100*v/u if u else 0 for v, u in zip(vis, uniq)]
cvr = [100*c/u if u else 0 for c, u in zip(conv, uniq)]
spend_day = [float(r["ctv_spend_raw"]) for r in rows]

fig, axes = plt.subplots(3, 1, figsize=(10, 8.4), sharex=True,
                         gridspec_kw={"height_ratios": [1, 1.2, 1.2], "hspace": 0.18})
ax0, ax1, ax2 = axes
ax0.fill_between(dts, spend_day, color=NAVY, alpha=0.25, zorder=2)
ax0.plot(dts, spend_day, color=NAVY, lw=1.1)
ax0.set_ylabel("CTV spend/day", fontsize=9)
ax0.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
ax0.set_title("Spend scaled ~10×. Visit rate is a clean signal; CVR is noise.",
              fontsize=14, fontweight="bold", loc="left")
ax1.plot(dts, vr, color=MINT, lw=1.6)
ax1.set_ylabel("Visit rate %\n(measurable)", fontsize=9, color=MINT)
ax2.plot(dts, cvr, color=RED, lw=1.0, alpha=0.85)
ax2.set_ylabel("CVR %\n(noise, flat)", fontsize=9, color=RED)
NATIONAL = datetime(2026, 5, 7)
for ax in axes:
    ax.axvline(NATIONAL, color=GRAY, ls=":", lw=1.2)
ax0.text(NATIONAL, ax0.get_ylim()[1]*0.92, " national scale →", color="#555", fontsize=9)
ax2.set_xlabel("")
fig.tight_layout(); fig.savefig(f"{BASE}/artifacts/ti_1044_chart_visit_vs_cvr.png"); plt.close(fig)

# ============ Chart 3: MDE vs monthly budget (CVR + visits), current marked ============
budgets = [b*1e3 for b in range(10, 10001, 10)]  # $10k .. $10M
def mde_at_budget(p, budget):
    imp = budget / CPM_ * 1000.0; n_treated = imp / IMPS_; n_total = n_treated / 0.9
    n_t, n_c = n_total*0.9, n_total*0.1
    _, rel = mde.mde_binomial(n_t, n_c, p)
    return rel*100
CPM_, IMPS_ = 8.58, 4.22
fig, ax = plt.subplots(figsize=(9, 5.4))
for p, c, lbl in [(0.0307, MINT, "Visit rate (3.07%)"), (0.00062, RED, "CVR (0.062%)")]:
    ax.plot(budgets, [mde_at_budget(p, b) for b in budgets], color=c, lw=2, label=lbl)
ax.axhline(5, color=GRAY, ls="--", lw=1); ax.text(1.1e4, 5.3, "5% target MDE", color="#555", fontsize=9)
ax.axvline(ACTUAL, color=NAVY, ls="--", lw=1.4)
ax.text(ACTUAL*1.05, 26, "ElevenLabs ≈ $1M/mo", color=NAVY, fontsize=9, fontweight="bold")
# mark CVR at $1M
ax.scatter([ACTUAL], [mde_at_budget(0.00062, ACTUAL)], color=RED, zorder=5, s=40)
ax.annotate(f"{mde_at_budget(0.00062, ACTUAL):.1f}% CVR floor", (ACTUAL, mde_at_budget(0.00062, ACTUAL)),
            textcoords="offset points", xytext=(8, 6), color=RED, fontsize=10, fontweight="bold")
ax.set_xscale("log"); ax.set_xlim(1e4, 1e7); ax.set_ylim(0, 30)
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x/1e6:.0f}M" if x>=1e6 else f"${x/1e3:.0f}K"))
ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.0f}%"))
ax.set_xlabel("Monthly budget"); ax.set_ylabel("Minimum detectable lift (relative %)")
ax.set_title("At $1M, CVR can only resolve a ~7% lift — visits resolve ~1%.",
             fontsize=14, fontweight="bold", loc="left")
ax.legend(frameon=False, loc="upper right")
fig.tight_layout(); fig.savefig(f"{BASE}/artifacts/ti_1044_chart_mde_curve.png"); plt.close(fig)

print("charts written:")
for c in ["power_contrast", "visit_vs_cvr", "mde_curve"]:
    print(f"  artifacts/ti_1044_chart_{c}.png")
