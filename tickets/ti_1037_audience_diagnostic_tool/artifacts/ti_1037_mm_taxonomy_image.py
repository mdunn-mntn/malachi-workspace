#!/usr/bin/env python3
"""TI-1037 — MM taxonomy reference card (PNG for Slack).

Renders the settled MM component/config taxonomy (Matt Brorby, 2026-07-08) as a
single shareable image: components, IP score tiers, the six live configs with
footprint, and the generating rules. Data source: ti_1037_mm_ds_cooccurrence.sql
(live prospecting obj=1/funnel=1, delivered in trailing 45d, 4,610 campaigns).
"""
import textwrap

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

NAVY, RED, GREEN, GRAY, BG = "#27496D", "#D63B2F", "#2E8B57", "#777777", "#FAFAFA"
plt.rcParams["font.family"] = ["Helvetica Neue", "Helvetica", "Arial"]

FIG_W = 12.4
fig = plt.figure(figsize=(FIG_W, 11.6), dpi=200, facecolor=BG)

# chars-per-fraction-of-width for wrapping estimates (tuned for fontsize ~10)
CPF = FIG_W * 15.5


def band(y0, y1, color, alpha=1.0):
    fig.patches.append(Rectangle((0.02, y0), 0.96, y1 - y0, transform=fig.transFigure,
                                 facecolor=color, alpha=alpha, edgecolor="none", zorder=0))


def hline(y, color=NAVY, lw=1.6, x0=0.03, x1=0.97):
    fig.add_artist(plt.Line2D([x0, x1], [y, y], transform=fig.transFigure,
                              color=color, linewidth=lw, zorder=1))


y = 0.975
fig.text(0.03, y, "MNTN Matched (MM) — components, configs & score tiers",
         fontsize=17, fontweight="bold", color="#222222", va="top")
y -= 0.023
fig.text(0.03, y, "Taxonomy settled with Matt Brorby (TI), 2026-07-08  ·  footprint = live prospecting "
                  "(objective 1, funnel 1) delivering in the trailing 45 days — 4,610 campaigns",
         fontsize=9.5, color=GRAY, va="top")
y -= 0.030


def section(title):
    global y
    fig.text(0.03, y, title, fontsize=12.5, fontweight="bold", color=NAVY, va="top")
    y -= 0.021


def table(cols, headers, rows, fs=10.0, pad=0.0035, line_h=0.0128, row_colors=None):
    """cols: list of (x, width_fraction). Draws header + wrapped rows; advances y."""
    global y
    for (x, w), h in zip(cols, headers):
        fig.text(x, y, h, fontsize=fs - 0.5, fontweight="bold", color=NAVY, va="top")
    y -= line_h + pad
    hline(y + 0.004)
    for ri, row in enumerate(rows):
        wrapped = [textwrap.wrap(str(cell), max(10, int(w * CPF))) or [""]
                   for (x, w), cell in zip(cols, row)]
        n_lines = max(len(wl) for wl in wrapped)
        row_h = n_lines * line_h + 2 * pad
        if ri % 2 == 0:
            band(y - row_h + 0.004, y + 0.004, "#000000", alpha=0.03)
        color = (row_colors or {}).get(ri, "#222222")
        for (x, w), wl, bold in zip(cols, wrapped, [True] + [False] * (len(cols) - 1)):
            fig.text(x, y - pad, "\n".join(wl), fontsize=fs, color=color, va="top",
                     fontweight="bold" if bold else "normal", linespacing=1.35)
        y -= row_h
    y -= 0.012


# ---- 1. components ----
section("1 · Components — what each DS leaf in the audience expression is")
table(
    cols=[(0.03, 0.105), (0.14, 0.215), (0.36, 0.345), (0.715, 0.255)],
    headers=["Leaf", "Official name", "Matches which IPs", "Tier it unlocks"],
    rows=[
        ["DS19", "MM Core  /  Keyword-Only", "IPs associated to the advertiser's keywords",
         "Max Reach (also reaches keyword-matching MI & HI IPs)"],
        ["DS13", "Peak Performance (v1)", "In-vertical IPs — leaf holds the 6-digit vertical id (= the RTC id)",
         "PP 8000 (also reaches HI vertical members)"],
        ["DS46", "Peak Performance v2 (Fangorn)", "Same as DS13 — same leaf shape & vertical id, Fangorn-scored",
         "Same, with continuous scores"],
        ["DS13 + bucket ids", "Expanded Peak Performance", "Bucket (industry) members",
         "MI — named but never shipped; zero live campaigns"],
    ],
)

# ---- 2. tiers ----
section("2 · IP score tiers — per-IP, independent of the campaign config")
table(
    cols=[(0.03, 0.16), (0.20, 0.115), (0.325, 0.36), (0.70, 0.27)],
    headers=["Tier", "Score", "IP membership", ""],
    rows=[
        ["High Intent", "10000", "in vertical AND matches keywords", ""],
        ["Peak Performance", "8000", "in vertical, no keyword match", ""],
        ["Mid Intent", "3333–6665", "in bucket but not vertical", ""],
        ["Max Reach", "1–3332", "keyword match only, outside the bucket",
         "(under PP v2 / Fangorn the fixed points become continuous model scores)"],
    ],
)

# ---- 3. configs ----
section("3 · The six live configs — what we call it")
table(
    cols=[(0.03, 0.115), (0.15, 0.28), (0.44, 0.155), (0.60, 0.165), (0.77, 0.20)],
    headers=["Expression", "What we call it", "Biddable IPs", "Tiers reachable", "Footprint (45d)"],
    rows=[
        ["DS19 only", "Keyword-Only  /  MM Core", "keyword matchers",
         "HI · MI · MaxReach  (no PP tier)", "42.7% spend · 859 adv · 1,559 camps"],
        ["DS19 + DS46", "MM Core + PP v2  —  the flagship (Fangorn)", "keywords + vertical",
         "all four", "18.9% · 606 adv · 1,314 camps"],
        ["DS13 + DS19", "MM Core + PP v1 (shipped Oct '25 as the “Peak Performance” product)",
         "keywords + vertical", "all four", "4.0% · 286 adv · 403 camps"],
        ["DS46 only", "PP v2 only  (“vertical only”)", "vertical members",
         "HI + PP only", "6.5% · 115 adv · 235 camps"],
        ["DS13 only", "PP v1 only  (“vertical only”)", "vertical members",
         "HI + PP only", "1.1% · 42 adv · 57 camps"],
        ["none", "non-MM  (3P / 1P / CRM / run-of-network)", "per its other leaves",
         "unscored (HS = −1)", "26.8% · 450 adv · 1,042 camps"],
        ["DS13 + DS46 (±19)", "impossible — same slot, two generations; the Fangorn flip swaps 13→46",
         "—", "—", "0 campaigns"],
    ],
    row_colors={6: RED},
)

# ---- 4. rules ----
section("4 · The rules that generate all of the above")
rules = [
    "Include leaves OR-join — adding a leaf always broadens the audience, never narrows it.",
    "Biddability: an IP must match ≥1 include leaf. Scores don't gate biddability — HHST gates delivery by score "
    "(10000 → HI only  ·  6666 → HI+PP  ·  0/unset → every matched IP serves).",
    "DS13 and DS46 are one slot (identical leaf, same vertical id = the RTC id) — never co-occur.",
    "MM = any of DS19 / DS13 / DS46 present. A “has DS19” definition misses the vertical-only cells "
    "≈ 7.6% of prospecting spend.",
    "Product names ARE the tier names, applied to the component that unlocks the tier "
    "(keywords → Max Reach · vertical → Peak Performance · bucket → Expanded PP, unshipped · "
    "HI needs no component — both reach it).",
]
for i, r in enumerate(rules, 1):
    for j, ln in enumerate(textwrap.wrap(r, 128)):
        fig.text(0.03 if j else 0.03, y, (f"{i}.  " if j == 0 else "     ") + ln,
                 fontsize=10, color="#222222", va="top")
        y -= 0.0128
    y -= 0.004

y -= 0.006
fig.text(0.03, y, "TI-1037  ·  query: ti_1037_mm_ds_cooccurrence.sql  ·  segment level = audience.audience_segments "
                  "(type 2, targeted) — the bidder-facing expression", fontsize=8.5, color=GRAY, va="top")

out = "/Users/malachi/Developer/work/mntn/workspace/tickets/ti_1037_audience_diagnostic_tool/artifacts/ti_1037_mm_taxonomy.png"
fig.savefig(out, dpi=200, facecolor=BG, bbox_inches="tight", pad_inches=0.25)
print(out)
