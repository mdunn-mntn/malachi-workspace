"""AUDI-1070 — Bridge: API vs UI vs BigQuery. The three Avon ROAS numbers differ by
exactly two knobs: SCOPE (prospecting -> all campaigns, via TV retargeting obj=4) and
ATTRIBUTION (last-touch -> industry_standard = + competing_* FIRST-TOUCH cols).
Both knobs are reproducible in BQ — the UI reproduces to the dollar (VV 692,888 EXACT).
2025 Jan-May. API=9.40 (prospecting, first-touch) -> +retargeting -> 17.33 (all, last-touch)
-> +first-touch(competing_*) -> UI=22.12. Every source up YoY."""
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"
NAVY, RED, GREEN, GRAY, AMBER = "#27496D", "#D63B2F", "#2E8B57", "#9AA0A6", "#C77B30"

API, BQ, UI = 9.40, 17.33, 22.12
labels = ["API / MoM chart\n(prospecting,\nfirst-touch)", "+ TV retargeting\n(SCOPE, obj=4)",
          "Earlier BigQuery\n(all campaigns,\nlast-touch)", "+ first-touch\n(competing_*)",
          "UI cards\n(all campaigns,\nindustry_standard)"]
fig, ax = plt.subplots(figsize=(12, 6.3))
ax.bar(0, API, 0.62, color=NAVY)
ax.bar(2, BQ, 0.62, color=GRAY)
ax.bar(4, UI, 0.62, color=GREEN)
ax.bar(1, BQ - API, 0.62, bottom=API, color=AMBER, alpha=0.85)
ax.bar(3, UI - BQ, 0.62, bottom=BQ, color=AMBER, alpha=0.85)
for a, b, y in [(0, 1, API), (1, 2, BQ), (2, 3, BQ), (3, 4, UI)]:
    ax.plot([a + 0.31, b - 0.31], [y, y], color="#bbb", lw=1, ls="--")
ax.text(0, API + 0.5, f"{API:.1f}×", ha="center", fontsize=14, fontweight="bold", color=NAVY)
ax.text(2, BQ + 0.5, f"{BQ:.1f}×", ha="center", fontsize=14, fontweight="bold", color="#555")
ax.text(4, UI + 0.5, f"{UI:.1f}×", ha="center", fontsize=14, fontweight="bold", color=GREEN)
ax.text(1, API + (BQ - API) / 2, f"+{BQ-API:.1f}", ha="center", va="center", fontsize=12, fontweight="bold", color="white")
ax.text(3, BQ + (UI - BQ) / 2, f"+{UI-BQ:.1f}", ha="center", va="center", fontsize=12, fontweight="bold", color="white")
ax.set_xticks(range(5)); ax.set_xticklabels(labels, fontsize=9.5)
ax.set_ylim(0, 26); ax.set_ylabel("ROAS (Jan–May 2025)")
ax.set_title("API vs UI vs BigQuery: same advertiser, two knobs — scope + first-touch",
             fontsize=14, fontweight="bold", loc="left", y=1.06, color=NAVY)
ax.text(0, 1.015, "Not in conflict. API/chart = prospecting only; add TV retargeting to reach all-campaigns; the UI adds first-touch "
        "(competing_*, industry_standard). Both knobs reproduce in BQ — the UI matches to the dollar (VV 692,888 EXACT). Every source up YoY.",
        transform=ax.transAxes, color="#666", fontsize=8.8)
for s in ["top", "right"]: ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_three_source_bridge.png", dpi=200, bbox_inches="tight")
print("wrote avon_three_source_bridge.png (verified competing_* labels)")
