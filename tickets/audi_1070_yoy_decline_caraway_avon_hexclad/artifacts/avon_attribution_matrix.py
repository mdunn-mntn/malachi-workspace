"""AUDI-1070 Avon attribution 2x2: same window (Jan-May), vary the lens per year.
Proves the client's apparent decline is the 2025-LT -> 2026-FT switch, not performance.
Visits from clickpass_log: LT=all VVs, FT=VVs with resolvable first_touch."""
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Rectangle
for fam in ["Helvetica Neue", "Helvetica", "Arial"]:
    if any(fam in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = fam; break
plt.rcParams.update({"figure.facecolor": "#FAFAFA", "axes.facecolor": "#FAFAFA", "savefig.facecolor": "#FAFAFA"})
D = "tickets/audi_1070_yoy_decline_caraway_avon_hexclad/"

LT25, LT26, FT25, FT26 = 686963, 591016, 250129, 167761
# cell = (row 2025 lens, col 2026 lens): value, from, to, tag, color
cells = {
 (0, 0): (LT26/LT25 - 1, LT25, LT26, "consistent (LT)", "#cfe3d4"),     # top-left
 (0, 1): (FT26/LT25 - 1, LT25, FT26, "WHAT THE CLIENT SEES\nLT-2025 to FT-2026", "#f1b4ad"),  # top-right
 (1, 0): (LT26/FT25 - 1, FT25, LT26, "switched (FT to LT)", "#bcd3ea"),     # bottom-left
 (1, 1): (FT26/FT25 - 1, FT25, FT26, "consistent (FT)", "#f5dcae"),      # bottom-right
}
fig, ax = plt.subplots(figsize=(10.5, 6.2))
for (r, c), (val, frm, to, tag, col) in cells.items():
    x, y = c, 1 - r
    edge = "#D63B2F" if (r, c) == (0, 1) else "#bbb"
    lw = 3.2 if (r, c) == (0, 1) else 1
    ax.add_patch(Rectangle((x, y), 1, 1, facecolor=col, edgecolor=edge, lw=lw))
    ax.text(x + 0.5, y + 0.66, f"{val*100:+.0f}%", ha="center", va="center", fontsize=30, fontweight="bold", color="#1B2A4A")
    ax.text(x + 0.5, y + 0.40, f"{frm:,} to {to:,}", ha="center", va="center", fontsize=10.5, color="#444")
    ax.text(x + 0.5, y + 0.20, tag, ha="center", va="center", fontsize=10, fontweight="bold",
            color="#D63B2F" if (r, c) == (0, 1) else "#666")
# headers
ax.text(0.5, 2.14, "2026 = Last-touch", ha="center", fontsize=12.5, fontweight="bold", color="#27496D")
ax.text(1.5, 2.14, "2026 = First-touch", ha="center", fontsize=12.5, fontweight="bold", color="#27496D")
ax.text(-0.10, 1.5, "2025 =\nLast-touch", ha="center", va="center", fontsize=12.5, fontweight="bold", color="#27496D", rotation=90)
ax.text(-0.10, 0.5, "2025 =\nFirst-touch", ha="center", va="center", fontsize=12.5, fontweight="bold", color="#27496D", rotation=90)
ax.set_xlim(-0.25, 2.05); ax.set_ylim(-0.15, 2.4); ax.axis("off")
ax.text(-0.25, 2.34, "Avon visits YoY (Jan–May) — same window, attribution varied per year",
        fontsize=14, fontweight="bold", color="#1B2A4A")
ax.text(-0.25, -0.12, "Diagonal = consistent lens (the true change). Off-diagonal = a lens SWITCH between years. "
        "Avon's reporting flipped LT(2025) to FT(2026), so the client's UI compares the top-right cell.",
        fontsize=9.3, color="#666")
plt.tight_layout(); plt.savefig(D + "artifacts/audi_1070_avon_attribution_matrix.png", dpi=200, bbox_inches="tight")
print("wrote avon_attribution_matrix.png")
print(f"LT-LT {LT26/LT25-1:+.1%} | FT-FT {FT26/FT25-1:+.1%} | LT->FT {FT26/LT25-1:+.1%} | FT->LT {LT26/FT25-1:+.1%}")
print(f"FT-resolvable rate: 2025 {FT25/LT25:.1%} -> 2026 {FT26/LT26:.1%} (first-touch null worsened -> inflates consistent-FT drop)")
