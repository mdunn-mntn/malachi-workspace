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
    """Headline: Peak Performance adoption — currently active campaigns only (Fix M10).
    Trims trailing partial-data week."""
    df_p = df.copy().sort_values("week_start").reset_index(drop=True)
    while len(df_p) > 1 and df_p.iloc[-1]["n_advertisers"] < 0.7 * df_p.iloc[-2]["n_advertisers"]:
        df_p = df_p.iloc[:-1]

    fig, ax = plt.subplots(figsize=(11, 5.5))
    x = df_p["week_start"]
    y = df_p["pct_adv_pp"] * 100
    ax.plot(x, y, color=ACCENT, linewidth=2.4)

    peak_y = y.iloc[-1]
    ax.set_title(f"Peak Performance presence — % of currently-active advertisers (Nov 2024 - Apr 2026)",
                 loc="left", color="#222")
    ax.set_ylabel("% of cohort advertisers running an active PP campaign")
    ax.set_xlabel("")
    ax.set_ylim(0, max(20, y.max() * 1.1))

    ax.annotate(f"{peak_y:.0f}%",
                xy=(x.iloc[-1], peak_y),
                xytext=(6, 0), textcoords="offset points",
                fontsize=12, color=ACCENT, weight="bold", va="center")

    baseline_idx = (df_p["week_start"] == pd.Timestamp("2025-09-29")).idxmax()
    base_y = y.iloc[baseline_idx]
    ax.scatter([x.iloc[baseline_idx]], [base_y], color=GRAY, zorder=3, s=25)
    ax.annotate(f"Sep 29: {base_y:.0f}%",
                xy=(x.iloc[baseline_idx], base_y),
                xytext=(-12, -20), textcoords="offset points",
                fontsize=9, color=GRAY, ha="right",
                arrowprops=dict(arrowstyle="-", color=GRAY, lw=0.6))

    annotate_events(ax)

    ax.text(0.01, -0.18,
            "PP detector: expression carries score_type=rtc + DS13 + DS19 together. Effective windows capped at "
            "campaign last-active day (Fix M10) so paused-but-not-deleted campaigns no longer inflate adoption. "
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

    ax.set_title("Cohort-share deltas Sep 29 -> Dec 29 2025",
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


def chart_05_pp_spend_share(df):
    """Track A — PP presence share vs PP spend share on same axes.
    Trims trailing weeks below 50% of prior-week spend (Fix M5 — incomplete data tail)."""
    try:
        df_spend = pd.read_csv(OUTPUTS / "ti_896_composition_spend_weighted.csv",
                               parse_dates=["week_start"])
    except FileNotFoundError:
        print("ti_896_composition_spend_weighted.csv not found; skipping chart 05.")
        return

    # Fix M5: drop trailing weeks where total_spend < 50% of prior week
    df_spend = df_spend.copy().sort_values("week_start").reset_index(drop=True)
    df_spend["spend_prev"] = df_spend["total_spend"].shift(1)
    while len(df_spend) > 1 and df_spend.iloc[-1]["total_spend"] < 0.5 * df_spend.iloc[-1]["spend_prev"]:
        df_spend = df_spend.iloc[:-1]
    df_spend = df_spend.drop(columns=["spend_prev"])

    df_pres = df.copy().sort_values("week_start").reset_index(drop=True)
    # Also drop the trailing partial week from presence df (last week n_advertisers fell sharply)
    while len(df_pres) > 1 and df_pres.iloc[-1]["n_advertisers"] < 0.7 * df_pres.iloc[-2]["n_advertisers"]:
        df_pres = df_pres.iloc[:-1]

    fig, ax = plt.subplots(figsize=(11, 5.5))
    x1, y1 = df_pres["week_start"], df_pres["pct_adv_pp"] * 100
    x2, y2 = df_spend["week_start"], df_spend["pct_spend_pp"] * 100

    ax.plot(x1, y1, color=ACCENT, linewidth=2.4, label="% of advertisers with PP")
    ax.plot(x2, y2, color=BASELINE, linewidth=2.4, label="% of MNTN spend on PP campaigns")

    ax.text(x1.iloc[-1], y1.iloc[-1], f"  {y1.iloc[-1]:.0f}%  advertisers",
            color=ACCENT, fontsize=10, weight="bold", va="center")
    ax.text(x2.iloc[-1], y2.iloc[-1], f"  {y2.iloc[-1]:.0f}%  spend",
            color=BASELINE, fontsize=10, weight="bold", va="center")

    ax.set_title("Peak Performance presence vs spend share over time",
                 loc="left", color="#222")
    ax.set_ylabel("% (of advertisers, or of MNTN spend)")
    ax.set_ylim(0, max(20, max(y1.max(), y2.max()) * 1.15))
    annotate_events(ax)

    ax.text(0.01, -0.18,
            "Presence: >=1 active PP campaign that delivered. Spend-weighted: share of cohort media_cost on "
            "campaigns running PP. Both metrics now AGREE at ~12% (Fix M10 corrected prior 21% presence "
            "overcount from paused-campaign attribution). Trailing partial-data weeks trimmed.",
            transform=ax.transAxes, fontsize=8, color="#777")

    save(fig, "ti_896_chart_05_pp_spend_share.png")


def chart_05b_mm_spend_cliff(df):
    """Track A sidebar promoted to co-finding (Fix M6) — MM spend cliff Oct 27 2025.
    MM was 73-79% of cohort spend pre-Oct, dropped to 56% on Oct 27,
    then 42-46% sustained Nov-Apr 2026."""
    try:
        df_spend = pd.read_csv(OUTPUTS / "ti_896_composition_spend_weighted.csv",
                               parse_dates=["week_start"])
    except FileNotFoundError:
        print("ti_896_composition_spend_weighted.csv not found; skipping chart 05b.")
        return

    # Trim incomplete trailing weeks
    df_spend = df_spend.copy().sort_values("week_start").reset_index(drop=True)
    df_spend["spend_prev"] = df_spend["total_spend"].shift(1)
    while len(df_spend) > 1 and df_spend.iloc[-1]["total_spend"] < 0.5 * df_spend.iloc[-1]["spend_prev"]:
        df_spend = df_spend.iloc[:-1]
    df_spend = df_spend.drop(columns=["spend_prev"])

    fig, ax = plt.subplots(figsize=(11, 5.5))
    x = df_spend["week_start"]
    y_mm = df_spend["pct_spend_mm"] * 100
    y_pp = df_spend["pct_spend_pp"] * 100
    y_kw = df_spend["pct_spend_keywords"] * 100
    y_3p = df_spend["pct_spend_3p"] * 100
    y_crm = df_spend["pct_spend_crm"] * 100

    ax.plot(x, y_mm, color=ACCENT, linewidth=2.6, label="DS2 (OPM)")
    ax.plot(x, y_kw, color=BASELINE, linewidth=2.0, label="Keywords")
    ax.plot(x, y_3p, color=GRAY, linewidth=1.6, label="3P")
    ax.plot(x, y_crm, color="#888", linewidth=1.6, label="CRM")
    ax.plot(x, y_pp, color="#4C72B0", linewidth=2.0, label="PP")

    # Direct labels at right
    for series, label, color in [(y_mm,"DS2 (OPM)",ACCENT),(y_kw,"Keywords",BASELINE),(y_3p,"3P",GRAY),(y_crm,"CRM","#888"),(y_pp,"PP",  "#4C72B0")]:
        ax.text(x.iloc[-1], series.iloc[-1], f"  {label}  {series.iloc[-1]:.0f}%",
                color=color, fontsize=10, weight="bold", va="center")

    # Annotate the Oct 27 cliff specifically
    cliff_date = pd.Timestamp("2025-10-27")
    cliff_idx_arr = (df_spend["week_start"] == cliff_date)
    if cliff_idx_arr.any():
        cliff_idx = cliff_idx_arr.idxmax()
        cliff_y = y_mm.iloc[cliff_idx]
        prev_y = y_mm.iloc[cliff_idx - 1] if cliff_idx > 0 else cliff_y
        ax.annotate(f"Oct 27: MM spend\nfell {prev_y:.0f}% -> {cliff_y:.0f}%",
                    xy=(cliff_date, cliff_y),
                    xytext=(20, -40), textcoords="offset points",
                    fontsize=9, color=ACCENT, weight="bold",
                    arrowprops=dict(arrowstyle="->", color=ACCENT, lw=1.0))

    ax.set_title("DS2 (OPM) spend share fell from 73-79% to 42-46% in 2 weeks (Oct 27 - Nov 10 2025)",
                 loc="left", color="#222")
    ax.set_ylabel("% of cohort media spend")
    ax.set_ylim(0, 90)
    annotate_events(ax)

    ax.text(0.01, -0.18,
            "DS2 (OPM segments per Alyson + Zach 2026-04-22) held 73-79% of cohort spend through Oct 20 2025. "
            "On Oct 27 it dropped to 56%; the next two weeks fell to 41-44% and stayed there through April 2026 "
            "(~30pp below the pre-Oct baseline). PP launched Oct 6. ~99.7% of DS2 usage is in inclusion clauses (real targeting).",
            transform=ax.transAxes, fontsize=8, color="#777")

    save(fig, "ti_896_chart_05b_mm_spend_cliff.png")


def chart_06_pp_default_vs_custom(df):
    """Track B — of PP adopters, share default / custom / both / unclassified."""
    try:
        df_dc = pd.read_csv(OUTPUTS / "ti_896_pp_default_custom_weekly.csv",
                            parse_dates=["week_start"])
    except FileNotFoundError:
        print("ti_896_pp_default_custom_weekly.csv not found; skipping chart 06.")
        return

    # Restrict to post-launch window where adopter counts are meaningful (n>=50)
    dfw = df_dc[df_dc["n_pp_adopters"] >= 50].copy()
    if dfw.empty:
        print("No weeks with >=50 adopters; skipping chart 06.")
        return

    fig, ax = plt.subplots(figsize=(11, 5.5))

    x = dfw["week_start"]
    s_def = dfw["share_default_of_adopters"].fillna(0) * 100
    s_cust = dfw["share_custom_of_adopters"].fillna(0) * 100
    s_both = dfw["share_both_of_adopters"].fillna(0) * 100
    s_unclass = 100 - s_def - s_cust - s_both

    # Stacked area
    ax.stackplot(x,
                 s_def, s_cust, s_both, s_unclass,
                 colors=[BASELINE, ACCENT, "#888", "#D0D0D0"])

    # In-band labels — put at a middle x position so they sit cleanly inside the bands
    mid_idx = len(dfw) // 2
    mid_x = x.iloc[mid_idx]

    y_def_mid  = float(s_def.iloc[mid_idx]) / 2
    y_cust_mid = float(s_def.iloc[mid_idx]) + float(s_cust.iloc[mid_idx]) / 2
    ax.text(mid_x, y_def_mid,  f"Default  {s_def.iloc[-1]:.0f}%",
            color="white", fontsize=12, weight="bold", ha="center", va="center")
    ax.text(mid_x, y_cust_mid, f"Custom  {s_cust.iloc[-1]:.0f}%",
            color="white", fontsize=12, weight="bold", ha="center", va="center")

    # "Both" and "unclassified" as small tick labels on the right
    top = float(s_def.iloc[-1]) + float(s_cust.iloc[-1])
    ax.text(x.iloc[-1], top + float(s_both.iloc[-1]) / 2,
            f"  Both {s_both.iloc[-1]:.0f}%", color="#666", fontsize=8, va="center", ha="left")
    ax.text(x.iloc[-1], top + float(s_both.iloc[-1]) + float(s_unclass.iloc[-1]) / 2,
            f"  Unclassified {s_unclass.iloc[-1]:.0f}%", color="#999", fontsize=8, va="center", ha="left")

    ax.set_title("Default vs custom Peak Performance, share of weekly adopters",
                 loc="left", color="#222")
    ax.set_ylabel("Share of Peak Performance adopters (%)")
    ax.set_ylim(0, 100)
    ax.set_xlim(x.iloc[0], x.iloc[-1])

    ax.text(0.01, -0.18,
            "Default = audience template has only DS13 + DS19 (minimal PP pattern). Custom = template layers "
            "additional DS clauses (exclusions, overlays, extra keywords). Split has been stable since launch. "
            "'Both' = advertiser runs a mix. 'Template not yet archived' = CDC lag on the template table.",
            transform=ax.transAxes, fontsize=8, color="#777")

    save(fig, "ti_896_chart_06_pp_default_vs_custom.png")


def chart_07_pp_vs_conv_scatter(df):
    """Track C — per-advertiser Δ(PP share) vs Δ(ROAS). Audience-side cross-check.
    Adds bootstrap 95% CIs (Fix M3) and reports n_valid_roas explicitly (Fix M1)."""
    try:
        df_sc = pd.read_csv(OUTPUTS / "ti_896_pp_vs_conv_scatter.csv")
    except FileNotFoundError:
        print("ti_896_pp_vs_conv_scatter.csv not found; skipping chart 07.")
        return

    # Cohort sizes BEFORE dropping NaN delta_roas_rel (true cohort sizes)
    is_new_full = df_sc["is_pp_new_adopter"].astype(str).str.lower() == "true"
    is_non_full = df_sc["is_non_adopter"].astype(str).str.lower() == "true"
    n_new_total = int(is_new_full.sum())
    n_non_total = int(is_non_full.sum())

    df_sc = df_sc.dropna(subset=["delta_pp_share", "delta_roas_rel"])
    df_sc = df_sc[(df_sc["delta_roas_rel"] > -1) & (df_sc["delta_roas_rel"] < 5)]

    is_new = df_sc["is_pp_new_adopter"].astype(str).str.lower() == "true"
    is_non = df_sc["is_non_adopter"].astype(str).str.lower() == "true"
    n_new_valid = int(is_new.sum())
    n_non_valid = int(is_non.sum())

    # Bootstrap medians (load from JSON if present, else compute inline)
    rng = np.random.default_rng(20260422)
    def boot_ci(vals, n_boot=1000):
        arr = np.array(vals, dtype=float)
        if len(arr) == 0:
            return (float("nan"), float("nan"), float("nan"))
        meds = np.array([np.median(rng.choice(arr, len(arr), replace=True)) for _ in range(n_boot)])
        return (float(np.median(arr)), float(np.percentile(meds, 2.5)), float(np.percentile(meds, 97.5)))

    new_vals = df_sc.loc[is_new, "delta_roas_rel"].to_numpy()
    non_vals = df_sc.loc[is_non, "delta_roas_rel"].to_numpy()
    med_new, lo_new, hi_new = boot_ci(new_vals)
    med_non, lo_non, hi_non = boot_ci(non_vals)
    overlap = not (hi_new < lo_non or hi_non < lo_new)

    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.axhline(0, color="#333", linewidth=0.5)
    ax.axvline(0, color="#333", linewidth=0.5)

    ax.scatter(df_sc.loc[is_non, "delta_pp_share"] * 100,
               df_sc.loc[is_non, "delta_roas_rel"] * 100,
               color=GRAY, s=14, alpha=0.45,
               label=f"Non-adopter (n={n_non_total} cohort, {n_non_valid} with valid ROAS)")
    ax.scatter(df_sc.loc[is_new, "delta_pp_share"] * 100,
               df_sc.loc[is_new, "delta_roas_rel"] * 100,
               color=ACCENT, s=22, alpha=0.75,
               label=f"New PP adopter (n={n_new_total} cohort, {n_new_valid} with valid ROAS)")

    # Median bands with CI shading
    ax.axhspan(lo_new*100, hi_new*100, color=ACCENT, alpha=0.10)
    ax.axhspan(lo_non*100, hi_non*100, color="#333", alpha=0.08)
    ax.axhline(med_new*100, color=ACCENT, linestyle="--", linewidth=1.0, alpha=0.85)
    ax.axhline(med_non*100, color="#333", linestyle="--", linewidth=1.0, alpha=0.85)
    ax.text(80, med_new*100 + 18,
            f"PP adopter median +{med_new*100:.0f}%\nCI95 [{lo_new*100:+.0f}%, {hi_new*100:+.0f}%]",
            color=ACCENT, fontsize=10, weight="bold", ha="center")
    ax.text(80, med_non*100 + 18,
            f"Non-adopter median +{med_non*100:.0f}%\nCI95 [{lo_non*100:+.0f}%, {hi_non*100:+.0f}%]",
            color="#333", fontsize=10, weight="bold", ha="center")

    ax.set_title("Δ ROAS distribution by cohort, Aug-Sep -> Dec 2025",
                 loc="left", color="#222")
    ax.set_xlabel("Δ PP delivery share (Aug-Sep to Dec 2025), percentage points")
    ax.set_ylabel("Δ ROAS (Aug-Sep to Dec 2025), relative %")
    ax.set_xlim(-10, 100)
    ax.set_ylim(-100, 400)

    ax.legend(loc="upper right", fontsize=9, frameon=False)

    overlap_msg = "CIs OVERLAP — gap is directional, not statistically robust at 95%." if overlap else \
                  "CIs do NOT overlap — gap survives 1,000-resample bootstrap."
    pct_valid_new = n_new_valid / n_new_total * 100 if n_new_total else 0
    pct_valid_non = n_non_valid / n_non_total * 100 if n_non_total else 0
    ax.text(0.01, -0.20,
            f"~{pct_valid_new:.0f}% of adopters and ~{pct_valid_non:.0f}% of non-adopters have non-zero order value "
            f"(rest are lead-gen / no-pixel — ROAS undefined). Medians are computed only on advertisers "
            f"with valid ROAS in both windows. {overlap_msg} Audience-side cross-check, not canonical conv analysis.",
            transform=ax.transAxes, fontsize=8, color="#777")

    save(fig, "ti_896_chart_07_pp_vs_conv_scatter.png")


def chart_08_default_vs_custom_roas(df):
    """Fix Section-4 #2 — default-PP vs custom-PP ROAS deltas."""
    try:
        df_dc = pd.read_csv(OUTPUTS / "ti_896_pp_default_custom_roas.csv")
    except FileNotFoundError:
        print("ti_896_pp_default_custom_roas.csv not found; skipping chart 08.")
        return

    adopters = df_dc[df_dc["is_pp_new_adopter"].astype(str).str.lower() == "true"]
    classes = ["default_dominant", "custom_dominant", "mixed"]
    rng = np.random.default_rng(20260422)
    def boot_ci(vals, n_boot=1000):
        arr = np.array(vals, dtype=float)
        arr = arr[np.isfinite(arr)]
        if len(arr) == 0:
            return (float("nan"), float("nan"), float("nan"), 0)
        meds = np.array([np.median(rng.choice(arr, len(arr), replace=True)) for _ in range(n_boot)])
        return (float(np.median(arr)), float(np.percentile(meds, 2.5)), float(np.percentile(meds, 97.5)), len(arr))

    rows = []
    for cls in classes:
        sub = adopters[adopters["pp_template_dominant_post"] == cls]
        n_total = len(sub)
        med, lo, hi, n_valid = boot_ci(sub["delta_roas_rel"].to_numpy())
        rows.append((cls, n_total, n_valid, med, lo, hi))

    fig, ax = plt.subplots(figsize=(10, 5.0))
    labels = [f"{r[0].replace('_dominant','').title()}\n(n={r[1]} cohort, {r[2]} valid ROAS)" for r in rows]
    medians = [r[3] * 100 for r in rows]
    los = [(r[3] - r[4]) * 100 for r in rows]
    his = [(r[5] - r[3]) * 100 for r in rows]
    colors = [BASELINE, ACCENT, GRAY]

    y = np.arange(len(rows))
    ax.barh(y, medians, color=colors, alpha=0.85, edgecolor="none", height=0.55)
    ax.errorbar(medians, y, xerr=[los, his], fmt="none", ecolor="#333", elinewidth=1.4, capsize=6)

    for i, (med, lo, hi) in enumerate(zip(medians, [r[4]*100 for r in rows], [r[5]*100 for r in rows])):
        ax.text(med + (15 if med >= 0 else -15), y[i],
                f"{med:+.0f}% (CI {lo:+.0f}% to {hi:+.0f}%)",
                va="center", ha="left" if med >= 0 else "right",
                color="#222", fontsize=10, weight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.axvline(0, color="#333", linewidth=0.6)
    ax.set_xlabel("Median Δ ROAS (Aug-Sep -> Dec 2025), relative %")
    ax.set_title("Default vs custom Peak Performance adopters: ROAS lift comparison",
                 loc="left", color="#222")

    ax.text(0.01, -0.20,
            "Among new PP adopters (post-window dominant template class). 'Default' = PP segment templates "
            "carrying only DS13+DS19. 'Custom' = layered with additional DS clauses. Mixed: both. "
            "Bootstrap 95% CIs from 1,000 resamples. Tiny cohorts mean wide CIs; differences are directional.",
            transform=ax.transAxes, fontsize=8, color="#777")

    save(fig, "ti_896_chart_08_default_vs_custom_roas.png")


def chart_09_weekly_cohort_roas(df):
    """Fix Section-4 #3 — weekly spend-weighted ROAS time series, adopters vs non.
    (Median is uninformative — most cohort advertisers have $0 order value.
    Spend-weighted ROAS captures the dynamics that matter.)"""
    try:
        df_w = pd.read_csv(OUTPUTS / "ti_896_weekly_roas_adopters_vs_non.csv",
                           parse_dates=["week_start"])
    except FileNotFoundError:
        print("ti_896_weekly_roas_adopters_vs_non.csv not found; skipping chart 09.")
        return

    piv = df_w.pivot(index="week_start", columns="cohort", values="spend_weighted_roas")
    if "new_adopter" not in piv.columns or "non_adopter" not in piv.columns:
        print("expected cohorts missing; skipping chart 09.")
        return

    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.plot(piv.index, piv["new_adopter"], color=ACCENT, linewidth=2.2, label="New PP adopter cohort")
    ax.plot(piv.index, piv["non_adopter"], color=BASELINE, linewidth=2.2, label="Non-adopter cohort")

    # Direct labels at right
    last = piv.iloc[-1]
    ax.text(piv.index[-1], last["new_adopter"], f"  Adopter  {last['new_adopter']:.0f}",
            color=ACCENT, fontsize=10, weight="bold", va="center")
    ax.text(piv.index[-1], last["non_adopter"], f"  Non-adopter  {last['non_adopter']:.0f}",
            color=BASELINE, fontsize=10, weight="bold", va="center")

    # Annotate baseline window endpoint
    ax.axvspan(pd.Timestamp("2025-08-01"), pd.Timestamp("2025-09-28"),
               color="#dddddd", alpha=0.4)
    ax.text(pd.Timestamp("2025-08-15"), ax.get_ylim()[1]*0.95,
            "  baseline\n  Aug-Sep", fontsize=9, color="#666", va="top")
    ax.axvspan(pd.Timestamp("2025-12-01"), pd.Timestamp("2025-12-31"),
               color="#dddddd", alpha=0.4)
    ax.text(pd.Timestamp("2025-12-10"), ax.get_ylim()[1]*0.95,
            "  post\n  Dec", fontsize=9, color="#666", va="top")

    annotate_events(ax)

    ax.set_title("Spend-weighted ROAS by cohort, weekly (Aug 2025 - Apr 2026)",
                 loc="left", color="#222")
    ax.set_ylabel("Spend-weighted ROAS (order value / media spend)")
    ax.set_xlabel("")
    ax.set_ylim(0, max(piv.values.max() * 1.15, 70))

    ax.legend(loc="upper left", fontsize=9, frameon=False)

    ax.text(0.01, -0.18,
            "Spend-weighted ROAS per cohort per week (median is uninformative because ~50% of cohort "
            "advertisers have zero order value). Adopter baseline ROAS was ~28-31; non-adopter ~17-25 — a "
            "~1.5x baseline gap that warns against direct lift comparison without propensity matching. "
            "Both cohorts lifted Q4 in absolute terms.",
            transform=ax.transAxes, fontsize=8, color="#777")

    save(fig, "ti_896_chart_09_weekly_cohort_roas.png")


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    df = load()
    chart_01_interest_jump(df)
    chart_02_cohort_composition(df)
    chart_03_retargeting(df)
    chart_04_shift_magnitudes(df)
    chart_05_pp_spend_share(df)
    chart_05b_mm_spend_cliff(df)
    chart_06_pp_default_vs_custom(df)
    chart_07_pp_vs_conv_scatter(df)
    chart_08_default_vs_custom_roas(df)
    chart_09_weekly_cohort_roas(df)
    print(f"Wrote {len(list(ARTIFACTS.glob('ti_896_chart_*.png')))} chart PNGs")


if __name__ == "__main__":
    main()
