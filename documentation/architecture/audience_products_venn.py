"""
Render MM 2.0 audience-products Venn diagram.

Layout:
- Bucket (DS13 industry) = large outer ellipse
- Vertical (DS13 subindustry) = nested inside Bucket (strict subset)
- Keywords (DS19) = overlaps Vertical and extends past Bucket boundary

Regions:
- PP = Vertical \ Keywords           score 8000
- HI = Vertical ∩ Keywords           score 10000
- MI = (Bucket \ Vertical) ∩ Keywords  score 3333-6665
- Max Reach = Keywords \ Bucket      score 1-3332 (random)

Output: audience_products_venn.png (200 DPI, off-white background).
Source: MM 2.0 state table (Ryan Kleck / Sean Yang, 2026-05-29). TI-897 / TI-999.
"""

import os
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse

plt.rcParams["font.family"] = ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"]

BUCKET_FILL = "#EDEDED"
VERTICAL_FILL = "#BFD4E5"
KEYWORDS_FILL = "#F2D7B6"
BUCKET_EDGE = "#8C8C8C"
VERTICAL_EDGE = "#456B8C"
KEYWORDS_EDGE = "#A8693F"

HI_ACCENT = "#9F2A1F"
VERTICAL_TEXT = "#2C5282"
KEYWORDS_TEXT = "#8B4513"
MUTED = "#6B6B6B"
TEXT = "#2C2C2C"
BG = "#FAFAFA"

fig, ax = plt.subplots(figsize=(12, 7.5), facecolor=BG)
ax.set_facecolor(BG)

# Bucket: large outer ellipse, x in [0.5, 8.5]
bucket = Ellipse(
    (4.5, 4.0), width=8.0, height=5.5,
    facecolor=BUCKET_FILL, edgecolor=BUCKET_EDGE, linewidth=1.4, alpha=0.5,
)
ax.add_patch(bucket)

# Vertical: nested inside Bucket, x in [1.75, 6.25]
vertical = Ellipse(
    (4.0, 4.0), width=4.5, height=4.0,
    facecolor=VERTICAL_FILL, edgecolor=VERTICAL_EDGE, linewidth=1.4, alpha=0.7,
)
ax.add_patch(vertical)

# Keywords: overlaps right side of Vertical, extends past Bucket. x in [4.5, 9.5]
keywords = Ellipse(
    (7.0, 4.0), width=5.0, height=4.0,
    facecolor=KEYWORDS_FILL, edgecolor=KEYWORDS_EDGE, linewidth=1.4, alpha=0.55,
)
ax.add_patch(keywords)

# Outer labels for each circle (positioned at the top of each shape)
ax.text(0.7, 6.85, "Bucket — DS13 industry",
        fontsize=12.5, fontweight="bold", color="#555555")
ax.text(2.4, 5.85, "Vertical",
        fontsize=12, fontweight="bold", color=VERTICAL_TEXT, ha="center")
ax.text(2.4, 5.55, "(DS13 subindustry)",
        fontsize=9, color=VERTICAL_TEXT, ha="center")
ax.text(7.3, 5.85, "Keywords",
        fontsize=12, fontweight="bold", color=KEYWORDS_TEXT, ha="center")
ax.text(7.3, 5.55, "(DS19)",
        fontsize=9, color=KEYWORDS_TEXT, ha="center")

# Tier labels at the centroid of each region.
# PP = Vertical-only (left part of vertical, before Keywords overlap)
ax.text(3.0, 4.25, "PP", fontsize=22, fontweight="bold", color=VERTICAL_TEXT, ha="center")
ax.text(3.0, 3.75, "score 8000", fontsize=10, color=VERTICAL_TEXT, ha="center")

# HI = Vertical ∩ Keywords (the small lens region around x=5.4, well inside both)
ax.text(5.35, 4.25, "HI", fontsize=24, fontweight="bold", color=HI_ACCENT, ha="center")
ax.text(5.35, 3.75, "score 10000", fontsize=10, color=HI_ACCENT, ha="center")

# MI = (Bucket \ Vertical) ∩ Keywords — inside Bucket and Keywords, outside Vertical
ax.text(7.5, 4.25, "MI", fontsize=20, fontweight="bold", color=KEYWORDS_TEXT, ha="center")
ax.text(7.5, 3.75, "3333–6665", fontsize=10, color=KEYWORDS_TEXT, ha="center")

# Max Reach = Keywords \ Bucket — the small slice of Keywords past the Bucket boundary
# Plus a callout note for the random-fallback role
ax.annotate(
    "Max Reach\n1–3332 (random)",
    xy=(9.35, 4.0), xytext=(10.4, 2.5),
    fontsize=10.5, fontweight="bold", color=MUTED, ha="center",
    arrowprops=dict(arrowstyle="->", color="#9A9A9A", lw=1.2),
)
ax.text(10.4, 1.7,
        "Keywords outside Bucket /\nfallback for everything else",
        fontsize=8.5, style="italic", color=MUTED, ha="center")

# Title block (figure-level, left aligned, two lines)
fig.text(0.05, 0.95, "MM 2.0 Scoring Tiers",
         fontsize=15.5, fontweight="bold", color=TEXT, ha="left")
fig.text(0.05, 0.915,
         "Mountain Match evaluates Bucket, Vertical, and Keywords per IP — "
         "the combination determines the tier and score.",
         fontsize=10, color=MUTED, ha="left")

# Caption / source line
fig.text(0.05, 0.045,
         "HI = Vertical ∩ Keywords  ·  PP = Vertical only  ·  "
         "MI = Bucket ∩ Keywords (no Vertical)  ·  Max Reach = outside Bucket / random fallback",
         fontsize=9, color=MUTED, ha="left", style="italic")
fig.text(0.05, 0.02,
         "Source: MM 2.0 state table (Ryan Kleck / Sean Yang, 2026-05-29). TI-897 / TI-999.",
         fontsize=8, color=MUTED, ha="left")

ax.set_xlim(0, 12)
ax.set_ylim(0.8, 7.4)
ax.set_aspect("equal")
ax.axis("off")

out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "audience_products_venn.png")
plt.savefig(out_path, dpi=200, facecolor=BG, bbox_inches="tight")
plt.close(fig)
print(f"wrote {out_path}")
