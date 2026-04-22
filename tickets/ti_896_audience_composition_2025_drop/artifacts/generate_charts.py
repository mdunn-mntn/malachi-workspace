"""TI-896 — chart generation for audience composition analysis.

Reads outputs/ti_896_composition_by_week.csv and produces Tufte-standard PNGs
for the presentation deck. Matplotlib, Helvetica Neue, 200 DPI, #FAFAFA bg.
"""

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

TICKET_DIR = Path(__file__).resolve().parent.parent
OUTPUTS = TICKET_DIR / "outputs"
ARTIFACTS = TICKET_DIR / "artifacts"

# --- Tufte-aligned rcParams ---
mpl.rcParams.update({
    "font.family": "Helvetica Neue",
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.labelsize": 11,
    "axes.edgecolor": "#333333",
    "axes.linewidth": 0.8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.facecolor": "#FAFAFA",
    "figure.facecolor": "#FAFAFA",
    "savefig.facecolor": "#FAFAFA",
    "xtick.color": "#555555",
    "ytick.color": "#555555",
    "axes.grid": False,
})

ACCENT = "#C0392B"       # red — key insight
BASELINE = "#1F3A5F"     # navy — supporting
GRAY = "#888888"         # context
MUTED = "#C8D0D8"

# Event annotations
EVENT_PP = pd.Timestamp("2025-10-06")      # Peak Performance launch (early Oct per Mike)
EVENT_MAX_REACH = pd.Timestamp("2025-11-19")  # Max Reach scoring off (Ryan)

CATS = ["mm", "3p", "crm", "pp", "keywords"]
LABELS = {"mm": "MM", "3p": "3P", "crm": "CRM", "pp": "Peak Performance", "keywords": "Keywords"}
COLORS = {"mm": BASELINE, "3p": GRAY, "crm": "#4C72B0", "pp": ACCENT, "keywords": "#8E8E8E"}


def load() -> pd.DataFrame:
    df = pd.read_csv(OUTPUTS / "ti_896_composition_by_week.csv", parse_dates=["week_start"])
    return df


def annotate_events(ax, y_lo=None, y_hi=None):
    """Vertical guide lines + labels for the two regime-change events (staggered)."""
    ylim = ax.get_ylim()
    if y_lo is None:
        y_lo, y_hi = ylim
    for date, label, color, y_frac in [
        (EVENT_PP,        "Peak Performance launch\n(early Oct 2025)", ACCENT,   0.97),
        (EVENT_MAX_REACH, "Max Reach scoring off\n(Nov 19 2025)",      BASELINE, 0.85),
    ]:
        ax.axvline(date, color=color, linestyle="--", linewidth=0.8, alpha=0.7)
        y_pos = y_lo + (y_hi - y_lo) * y_frac
        ax.text(date, y_pos, " " + label, fontsize=8, color=color,
                ha="left", va="top")


def save(fig, name):
    out = ARTIFACTS / name
    fig.savefig(out, dpi=200, bbox_inches="tight")
    print(f"wrote {out}")
    plt.close(fig)


def chart_01_interest_jump(df):
    """Headline: Peak Performance adoption tripled Sep-Dec 2025."""
    fig, ax = plt.subplots(figsize=(11, 5.5))
    x = df["week_start"]
    y = df["pct_adv_pp"] * 100
    ax.plot(x, y, color=ACCENT, linewidth=2.4)

    ax.set_title("21% of 2025-active advertisers have adopted Peak Performance",
                 loc="left", color="#222")
    ax.set_ylabel("% of 2025-active advertisers with >=1 Peak Performance audience")
    ax.set_xlabel("")
    ax.set_ylim(0, max(35, y.max() * 1.1))

    # Direct label: peak
    peak_y = y.iloc[-1]
    ax.annotate(f"{peak_y:.0f}%",
                xy=(x.iloc[-1], peak_y),
                xytext=(6, 0), textcoords="offset points",
                fontsize=12, color=ACCENT, weight="bold", va="center")

    # Baseline callout — sit well below the line so we don't overprint
    baseline_idx = (df["week_start"] == pd.Timestamp("2025-09-29")).idxmax()
    base_y = y.iloc[baseline_idx]
    ax.scatter([x.iloc[baseline_idx]], [base_y], color=GRAY, zorder=3, s=25)
    ax.annotate(f"Sep 29: {base_y:.0f}%",
                xy=(x.iloc[baseline_idx], base_y),
                xytext=(-12, -20), textcoords="offset points",
                fontsize=9, color=GRAY, ha="right",
                arrowprops=dict(arrowstyle="-", color=GRAY, lw=0.6))

    annotate_events(ax)

    ax.text(0.01, -0.18,
            "PP detector: expression carries score_type=rtc + DS13 + DS19 together. "
            "~1% pre-Oct baseline = early-access / legacy RTC+DS13+DS19 configs, not formal PP. "
            "Cohort: advertisers with >=1 2025 impression. Source: TI-896.",
            transform=ax.transAxes, fontsize=8, color="#777")

    save(fig, "ti_896_chart_01_pp_jump.png")


def chart_02_cohort_composition(df):
    """All four buckets over time — shows which moved and which stayed flat."""
    fig, ax = plt.subplots(figsize=(11, 5.5))
    for cat in CATS:
        y = df[f"pct_adv_{cat}"] * 100
        ax.plot(df["week_start"], y, color=COLORS[cat], linewidth=2,
                label=LABELS[cat])
        # Direct label at right edge
        ax.text(df["week_start"].iloc[-1], y.iloc[-1], f"  {LABELS[cat]}  {y.iloc[-1]:.0f}%",
                color=COLORS[cat], fontsize=10, weight="bold", va="center")

    ax.set_title("Audience-type usage across 2025-active advertisers",
                 loc="left", color="#222")
    ax.set_ylabel("% of advertisers using audience type")
    ax.set_xlabel("")
    ax.set_ylim(0, 105)

    annotate_events(ax)

    ax.text(0.01, -0.18,
            "MM stays near-universal; Keywords, 3P, CRM all flat in the drop window. "
            "Peak Performance is the only line above noise.",
            transform=ax.transAxes, fontsize=8, color="#555")

    save(fig, "ti_896_chart_02_cohort_composition.png")


def chart_03_retargeting(df):
    """Retargeting-campaign share — Alex Knorr's hypothesis."""
    fig, ax = plt.subplots(figsize=(11, 5.5))
    y_ret = df["pct_camp_retargeting"] * 100
    y_pro = df["pct_camp_prospecting"] * 100

    ax.plot(df["week_start"], y_pro, color=BASELINE, linewidth=2, label="Prospecting (obj 1/5/6)")
    ax.plot(df["week_start"], y_ret, color=ACCENT, linewidth=2, label="Retargeting (obj 4)")

    ax.text(df["week_start"].iloc[-1], y_ret.iloc[-1],
            f"  Retarg  {y_ret.iloc[-1]:.0f}%", color=ACCENT, fontsize=10, weight="bold", va="center")
    ax.text(df["week_start"].iloc[-1], y_pro.iloc[-1],
            f"  Prospect  {y_pro.iloc[-1]:.0f}%", color=BASELINE, fontsize=10, weight="bold", va="center")
    # Mark starting point
    ax.text(df["week_start"].iloc[0], y_ret.iloc[0],
            f"{y_ret.iloc[0]:.0f}%  ", color=ACCENT, fontsize=9, ha="right", va="center")
    ax.text(df["week_start"].iloc[0], y_pro.iloc[0],
            f"{y_pro.iloc[0]:.0f}%  ", color=BASELINE, fontsize=9, ha="right", va="center")

    ax.set_title("Retargeting share of active campaigns has fallen ~13pp over 18 months",
                 loc="left", color="#222")
    ax.set_ylabel("% of active campaigns")
    ax.set_xlabel("")
    ax.set_ylim(0, 70)

    annotate_events(ax)

    ax.text(0.01, -0.18,
            "Gotcha: objective_id unreliable post-TV-migration. funnel_level ≥ 2 (retarget equivalent) "
            "trends inversely — composition shift is real but absolute levels may differ from dashboards.",
            transform=ax.transAxes, fontsize=8, color="#555")

    save(fig, "ti_896_chart_03_retargeting.png")


def chart_04_shift_magnitudes(df):
    """Bar chart: Sep 2025 → Dec 2025 delta (pp) per category — the drop window."""
    sep = df[df["week_start"] == pd.Timestamp("2025-09-29")].iloc[0]
    dec = df[df["week_start"] == pd.Timestamp("2025-12-29")].iloc[0]

    categories = [
        ("Peak Performance", "pct_adv_pp"),
        ("Keywords",         "pct_adv_keywords"),
        ("3P",               "pct_adv_3p"),
        ("CRM",              "pct_adv_crm"),
        ("MM",               "pct_adv_mm"),
        ("Retargeting",      "pct_camp_retargeting"),
        ("Prospecting",      "pct_camp_prospecting"),
    ]
    deltas = []
    for label, col in categories:
        d = (dec[col] - sep[col]) * 100
        deltas.append((label, d))
    deltas.sort(key=lambda t: t[1], reverse=True)

    fig, ax = plt.subplots(figsize=(9, 5.5))
    labels = [d[0] for d in deltas]
    values = [d[1] for d in deltas]
    colors = [ACCENT if v > 5 or v < -5 else GRAY for v in values]
    bars = ax.barh(labels, values, color=colors, edgecolor="none")
    ax.axvline(0, color="#333", linewidth=0.6)
    ax.invert_yaxis()

    for bar, v in zip(bars, values):
        ha = "left" if v >= 0 else "right"
        off = 0.3 if v >= 0 else -0.3
        ax.text(v + off, bar.get_y() + bar.get_height() / 2, f"{v:+.1f}pp",
                va="center", ha=ha, color=bar.get_facecolor(), weight="bold", fontsize=10)

    ax.set_title("Peak Performance was the only material shift Sep-Dec 2025",
                 loc="left", color="#222")
    ax.set_xlabel("Change in cohort share (percentage points)")
    ax.set_ylabel("")
    xmax = max(abs(v) for v in values) * 1.35
    ax.set_xlim(-xmax, xmax)

    ax.text(0.01, -0.15,
            "Advertiser-level share deltas for Sep 29 2025 -> Dec 29 2025. "
            "Campaign-grain retargeting/prospecting mix shown for reference.",
            transform=ax.transAxes, fontsize=8, color="#777")

    save(fig, "ti_896_chart_04_shift_magnitudes.png")


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    df = load()
    chart_01_interest_jump(df)
    chart_02_cohort_composition(df)
    chart_03_retargeting(df)
    chart_04_shift_magnitudes(df)
    print(f"Wrote {len(list(ARTIFACTS.glob('ti_896_chart_*.png')))} chart PNGs")


if __name__ == "__main__":
    main()
