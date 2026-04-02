"""
TI-804: Executive-quality visualization charts for keyword visit rate analysis.
Generates 4 charts from CSV data in outputs/.

Usage: python3 artifacts/generate_charts.py
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import csv
import os

# ── Global Style ──────────────────────────────────────────────────────────────

FONT = 'Helvetica Neue'
BG = '#FAFAFA'

# MNTN-inspired palette
NAVY    = '#1B2A4A'
BLUE    = '#2E5090'
MID     = '#5A7DB5'
LIGHT   = '#A8BDD9'
PALE    = '#D1DDED'
MUTED   = '#C8CDD4'
RED     = '#D63B2F'
RED_SOFT = '#E8685A'

plt.rcParams.update({
    'font.family': FONT,
    'font.size': 13,
    'axes.facecolor': BG,
    'figure.facecolor': BG,
    'axes.edgecolor': '#CCCCCC',
    'axes.linewidth': 0.5,
    'xtick.color': '#666666',
    'ytick.color': '#666666',
    'text.color': '#222222',
    'axes.spines.top': False,
    'axes.spines.right': False,
})

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(OUT_DIR), 'outputs')


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_rank_buckets():
    rows = []
    with open(os.path.join(DATA_DIR, 'ti_804_rank_bucket_visit_rates.csv')) as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows

def load_per_advertiser():
    rows = []
    with open(os.path.join(DATA_DIR, 'ti_804_per_advertiser_rank_lift.csv')) as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return sorted(rows, key=lambda r: float(r['lift_top_vs_bottom']), reverse=True)

def load_per_vertical():
    rows = []
    with open(os.path.join(DATA_DIR, 'ti_804_per_vertical_rank_lift.csv')) as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return sorted(rows, key=lambda r: float(r['lift_top_vs_bottom']), reverse=True)


# ── Chart 1: The Cliff ───────────────────────────────────────────────────────

def chart_rank_buckets():
    data = load_rank_buckets()
    labels = ['1–5', '6–10', '11–20', '21–30', '31–50', '51+']
    lifts = [float(r['lift_vs_worst']) for r in data]

    # Color gradient: bold accent for top, progressively muted
    colors = [RED, NAVY, BLUE, MID, LIGHT, MUTED]

    fig, ax = plt.subplots(figsize=(14, 7))

    bars = ax.bar(range(len(labels)), lifts, width=0.65, color=colors,
                  edgecolor='white', linewidth=1.5, zorder=3)

    # Labels on bars
    for i, (bar, lift) in enumerate(zip(bars, lifts)):
        label = f'{lift:.0f}x' if lift >= 10 else f'{lift:.1f}x'
        color = RED if i == 0 else '#333333'
        weight = 'bold' if i == 0 else 'medium'
        size = 22 if i == 0 else 16
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + lifts[0] * 0.02,
                label, ha='center', va='bottom', fontsize=size, fontweight=weight, color=color)

    # Title and subtitle
    ax.text(0.0, 1.15, 'Top-Ranked Keywords Drive 184x More Visits',
            transform=ax.transAxes, fontsize=26, fontweight='bold', color='#111111')
    ax.text(0.0, 1.07, 'IPs matched to an advertiser\'s top-5 BUK keywords visit at 184x the rate of those matched to rank 51+',
            transform=ax.transAxes, fontsize=13, color='#666666')

    # Annotation — positioned to the right of the first bar, not overlapping
    ax.annotate('Top-5 keywords carry\nthe vast majority of signal',
                xy=(0.65, lifts[0] * 0.7), fontsize=11, color='#555555',
                style='italic', ha='left')

    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=14)
    ax.set_xlabel('Best Matched Keyword Rank (per advertiser)', fontsize=14, labelpad=12, color='#555555')
    ax.set_ylabel('')
    ax.set_ylim(0, lifts[0] * 1.22)
    ax.yaxis.set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_color('#CCCCCC')

    # Light horizontal reference lines
    for y in [50, 100, 150]:
        ax.axhline(y=y, color='#E0E0E0', linewidth=0.5, zorder=1)

    fig.subplots_adjust(top=0.82, bottom=0.12, left=0.05, right=0.95)
    fig.savefig(os.path.join(OUT_DIR, 'ti_804_chart_rank_bucket_visit_rates.png'),
                dpi=200, facecolor=BG)
    plt.close(fig)
    print('  [1/4] Rank bucket chart saved')


# ── Chart 2: Per-Advertiser Consistency ──────────────────────────────────────

def chart_per_advertiser():
    data = load_per_advertiser()
    # Shorten long advertiser names for readability
    name_map = {
        'BISJ - SJSE Subscriptions': 'BISJ - SJSE',
        'S - APG - Wyoming - Renewal by Andersen': 'Renewal by Andersen',
        'Drive - OTF Royal Palm Beach FL & Westlake FL': 'OTF Royal Palm Beach',
    }
    names = [name_map.get(r['advertiser_name'], r['advertiser_name']) for r in data]
    lifts = [float(r['lift_top_vs_bottom']) for r in data]
    median_lift = np.median(lifts)

    # Color: red for >100x, navy for >10x, muted for <10x
    colors = []
    for l in lifts:
        if l >= 100:
            colors.append(RED)
        elif l >= 10:
            colors.append(NAVY)
        else:
            colors.append(MUTED)

    fig, ax = plt.subplots(figsize=(14, 8))

    y_pos = range(len(names) - 1, -1, -1)
    bars = ax.barh(y_pos, lifts, height=0.65, color=colors,
                   edgecolor='white', linewidth=1, zorder=3)

    # Labels
    for i, (yp, lift) in enumerate(zip(y_pos, lifts)):
        label = f'{lift:.0f}x' if lift >= 10 else f'{lift:.0f}x'
        ax.text(lift + max(lifts) * 0.015, yp, label,
                va='center', fontsize=12, fontweight='medium', color='#333333')

    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, fontsize=12)
    ax.set_xlim(0, max(lifts) * 1.18)
    ax.xaxis.set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_color('#CCCCCC')

    # Median line
    ax.axvline(x=median_lift, color=RED_SOFT, linewidth=1.5, linestyle='--', zorder=2, alpha=0.7)
    ax.text(median_lift + max(lifts) * 0.01, len(names) - 0.3,
            f'Median: {median_lift:.0f}x', fontsize=11, color=RED_SOFT, fontweight='medium')

    # 10x threshold line
    ax.axvline(x=10, color='#AAAAAA', linewidth=1, linestyle=':', zorder=2)
    ax.text(10 + max(lifts) * 0.01, -0.8, '10x threshold',
            fontsize=10, color='#999999')

    # Title and subtitle
    ax.text(0.0, 1.10, '93% of Advertisers Show >10x Keyword Lift',
            transform=ax.transAxes, fontsize=24, fontweight='bold', color='#111111')
    ax.text(0.0, 1.04, '14 of 15 advertisers above 10x — signal is consistent, not driven by outliers',
            transform=ax.transAxes, fontsize=13, color='#666666')

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=RED, label='>100x lift'),
        Patch(facecolor=NAVY, label='10–100x lift'),
        Patch(facecolor=MUTED, label='<10x lift'),
    ]
    ax.legend(handles=legend_elements, loc='lower right', fontsize=11,
              frameon=True, facecolor=BG, edgecolor='#CCCCCC')

    fig.subplots_adjust(top=0.88, bottom=0.06, left=0.22, right=0.95)
    fig.savefig(os.path.join(OUT_DIR, 'ti_804_chart_per_advertiser_lift.png'),
                dpi=200, facecolor=BG)
    plt.close(fig)
    print('  [2/4] Per-advertiser chart saved')


# ── Chart 3: The Punchline — Global vs Per-Advertiser ────────────────────────

def chart_contrast():
    fig, (ax_global, ax_per) = plt.subplots(1, 2, figsize=(16, 8),
                                             gridspec_kw={'width_ratios': [1, 2.2]})

    # ── Left panel: Global (muted, flat) ──
    global_labels = ['Best', 'Median', 'Worst']
    global_rates = [1.48e-2, 9.5e-3, 5.54e-3]  # from CSV
    global_lifts = [r / global_rates[-1] for r in global_rates]  # normalize to worst

    global_colors = [MUTED, '#D8D8D8', '#E8E8E8']
    bars_g = ax_global.bar(range(3), global_lifts, width=0.55, color=global_colors,
                           edgecolor='white', linewidth=1.5, zorder=3)

    for bar, lift in zip(bars_g, global_lifts):
        ax_global.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.08,
                       f'{lift:.1f}x', ha='center', fontsize=14, color='#888888', fontweight='medium')

    ax_global.set_xticks(range(3))
    ax_global.set_xticklabels(global_labels, fontsize=12, color='#888888')
    ax_global.set_ylim(0, 4.5)
    ax_global.yaxis.set_visible(False)
    ax_global.spines['left'].set_visible(False)
    ax_global.spines['bottom'].set_color('#DDDDDD')

    # Panel label — inside the axes area to avoid crowding the supertitle
    ax_global.text(0.5, 0.97, 'Global Ranking', transform=ax_global.transAxes,
                   ha='center', fontsize=16, fontweight='medium', color='#999999')
    ax_global.text(0.5, 0.85, '3x range', transform=ax_global.transAxes,
                   ha='center', fontsize=28, fontweight='bold', color='#AAAAAA')

    # ── Right panel: Per-Advertiser (bold, dramatic) ──
    data = load_rank_buckets()
    labels = ['1–5', '6–10', '11–20', '21–30', '31–50', '51+']
    lifts = [float(r['lift_vs_worst']) for r in data]
    per_colors = [RED, NAVY, BLUE, MID, LIGHT, MUTED]

    bars_p = ax_per.bar(range(6), lifts, width=0.65, color=per_colors,
                        edgecolor='white', linewidth=1.5, zorder=3)

    for i, (bar, lift) in enumerate(zip(bars_p, lifts)):
        label = f'{lift:.0f}x' if lift >= 10 else f'{lift:.1f}x'
        color = RED if i == 0 else '#333333'
        weight = 'bold' if i == 0 else 'medium'
        size = 18 if i == 0 else 13
        ax_per.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + lifts[0] * 0.02,
                    label, ha='center', fontsize=size, fontweight=weight, color=color)

    ax_per.set_xticks(range(6))
    ax_per.set_xticklabels(labels, fontsize=12)
    ax_per.set_xlabel('BUK Keyword Rank', fontsize=12, labelpad=10, color='#555555')
    ax_per.set_ylim(0, lifts[0] * 1.2)
    ax_per.yaxis.set_visible(False)
    ax_per.spines['left'].set_visible(False)
    ax_per.spines['bottom'].set_color('#CCCCCC')

    # Panel label — inside the axes area to avoid crowding the supertitle
    ax_per.text(0.5, 0.97, 'Per-Advertiser Ranking', transform=ax_per.transAxes,
                ha='center', fontsize=16, fontweight='bold', color=NAVY)
    ax_per.text(0.5, 0.85, '184x range', transform=ax_per.transAxes,
                ha='center', fontsize=28, fontweight='bold', color=RED)

    # Reference lines on right panel
    for y in [50, 100, 150]:
        ax_per.axhline(y=y, color='#E0E0E0', linewidth=0.5, zorder=1)

    # ── Supertitle ──
    fig.text(0.5, 0.96, 'Keyword Value is Advertiser-Specific, Not Universal',
             ha='center', fontsize=26, fontweight='bold', color='#111111')
    fig.text(0.5, 0.91, 'Global keyword ranking captures 3x differentiation. Per-advertiser ranking captures 184x.',
             ha='center', fontsize=13, color='#666666')

    # Separator line between panels
    from matplotlib.lines import Line2D
    line = Line2D([0.375, 0.375], [0.15, 0.82], transform=fig.transFigure,
                  color='#E0E0E0', linewidth=1, linestyle='-')
    fig.add_artist(line)

    # "60x more signal" as a bottom-center callout below both charts
    fig.text(0.5, 0.04, 'Per-advertiser ranking captures 60x more signal than global ranking',
             ha='center', fontsize=13, color=RED, fontweight='bold')

    fig.subplots_adjust(top=0.85, bottom=0.14, left=0.04, right=0.96, wspace=0.22)
    fig.savefig(os.path.join(OUT_DIR, 'ti_804_chart_global_vs_per_advertiser.png'),
                dpi=200, facecolor=BG)
    plt.close(fig)
    print('  [3/4] Contrast chart saved')


# ── Chart 4: Per-Vertical Lift (Lollipop) ────────────────────────────────────

def chart_per_vertical():
    data = load_per_vertical()
    names = [r['vertical_name'].replace(' & ', ' &\n') if len(r['vertical_name']) > 25
             else r['vertical_name'] for r in data]
    # Clean up long names
    names = [r['vertical_name'] for r in data]
    lifts = [float(r['lift_top_vs_bottom']) for r in data]
    median_lift = np.median(lifts)

    # Color by magnitude
    colors = []
    for l in lifts:
        if l >= 100:
            colors.append(RED)
        elif l >= 10:
            colors.append(NAVY)
        else:
            colors.append(MUTED)

    fig, ax = plt.subplots(figsize=(14, 8))

    y_pos = list(range(len(names) - 1, -1, -1))

    # Lollipop: stem + dot
    for yp, lift, c in zip(y_pos, lifts, colors):
        ax.plot([0, lift], [yp, yp], color=c, linewidth=2, zorder=2, alpha=0.6)
        ax.scatter(lift, yp, color=c, s=100, zorder=3, edgecolors='white', linewidth=1.5)

    # Labels on dots
    for yp, lift in zip(y_pos, lifts):
        label = f'{lift:.0f}x' if lift >= 10 else f'{lift:.0f}x'
        ax.text(lift + max(lifts) * 0.02, yp, label,
                va='center', fontsize=12, fontweight='medium', color='#333333')

    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, fontsize=12)
    ax.set_xlim(0, max(lifts) * 1.18)
    ax.xaxis.set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_color('#CCCCCC')

    # Median line
    ax.axvline(x=median_lift, color=RED_SOFT, linewidth=1.5, linestyle='--', zorder=1, alpha=0.7)
    ax.text(median_lift + max(lifts) * 0.01, len(names) - 0.3,
            f'Median: {median_lift:.0f}x', fontsize=11, color=RED_SOFT, fontweight='medium')

    # 10x threshold
    ax.axvline(x=10, color='#AAAAAA', linewidth=1, linestyle=':', zorder=1)

    # Title and subtitle
    ax.text(0.0, 1.10, 'All 15 Verticals Show Positive Keyword Lift',
            transform=ax.transAxes, fontsize=24, fontweight='bold', color='#111111')
    ax.text(0.0, 1.04, 'Signal works across every industry — not limited to specific verticals',
            transform=ax.transAxes, fontsize=13, color='#666666')

    # Legend — upper right to avoid bottom annotation overlap
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='o', color=RED, markerfacecolor=RED, markersize=8, label='>100x', linewidth=0),
        Line2D([0], [0], marker='o', color=NAVY, markerfacecolor=NAVY, markersize=8, label='10–100x', linewidth=0),
        Line2D([0], [0], marker='o', color=MUTED, markerfacecolor=MUTED, markersize=8, label='<10x', linewidth=0),
    ]
    ax.legend(handles=legend_elements, loc='center right', fontsize=11,
              frameon=True, facecolor=BG, edgecolor='#CCCCCC')

    # Annotation at bottom
    ax.text(0.98, 0.02, 'Strongest in product verticals · Weakest in local services · All positive',
            transform=ax.transAxes, ha='right', fontsize=11, color='#888888', style='italic')

    fig.subplots_adjust(top=0.88, bottom=0.06, left=0.25, right=0.95)
    fig.savefig(os.path.join(OUT_DIR, 'ti_804_chart_per_vertical_lift.png'),
                dpi=200, facecolor=BG)
    plt.close(fig)
    print('  [4/4] Per-vertical chart saved')


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print('Generating TI-804 executive charts...')
    chart_rank_buckets()
    chart_per_advertiser()
    chart_contrast()
    chart_per_vertical()
    print('Done. All charts in artifacts/')
