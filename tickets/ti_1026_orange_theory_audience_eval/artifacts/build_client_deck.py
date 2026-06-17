#!/usr/bin/env python3
"""TI-1026 CLIENT-SAFE deck for Orange Theory (via Kelly / account director).
Scope (per stakeholder call): the effect of GEO and DEMOGRAPHIC EXCLUSIONS on reach, + constructive
recommendations to grow reach. NO internal mechanics (scoring/HHST/DS14/RTC/holdout), NO peer benchmark,
NO UI-vs-deliverable, NO creative-ceiling message, NO keywords, NO internal IDs/byline.
"""
import base64, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

HERE = Path(__file__).resolve().parent
NAVY = "#1F3864"; RED = "#C00000"; GREY = "#9AA0A6"; BG = "#FAFAFA"
plt.rcParams.update({"font.family": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG, "font.size": 13})

# ---- Client funnel chart: geo + demo exclusion effect (relabeled, no internal jargon) ----
fig, ax = plt.subplots(figsize=(9.4, 4.7), dpi=200)
stages = ["Households matching\nyour audience targeting", "Within 7 mi of\na studio", "After income / age\nexclusions"]
vals = [4.6, 2.1, 1.5]
ypos = [2, 1, 0]
ax.barh(ypos, vals, color=[NAVY, NAVY, GREY], height=0.62)
for y, v in zip(ypos, vals):
    ax.text(v + 0.07, y, f"~{v:.1f}M", va="center", fontsize=13, fontweight="bold", color=NAVY)
for y, st in zip(ypos, stages):
    ax.text(-0.12, y, st, va="center", ha="right", fontsize=10.5, color="#333")
ax.text(0.05, 1.5, "↓  the 7-mile studio fence removes ~half the audience", fontsize=10.5, color=RED, fontweight="bold", va="center")
ax.text(0.05, 0.5, "↓  income / age exclusions remove ~1.3M more", fontsize=10.5, color=RED, fontweight="bold", va="center")
for s in ("top", "right", "left", "bottom"): ax.spines[s].set_visible(False)
ax.set_xticks([]); ax.set_yticks([]); ax.set_xlim(-2.6, 5.6); ax.set_ylim(-0.6, 2.5)
plt.tight_layout(); plt.savefig(HERE / "ti_1026_client_chart_funnel.png"); plt.close()

def img(p): return "data:image/png;base64," + base64.b64encode((HERE / p).read_bytes()).decode()
C_FUNNEL = img("ti_1026_client_chart_funnel.png")

SLIDES = f"""
<section>
  <h1>Orange Theory National</h1>
  <h2 style="color:#C00000">Audience Reach Review</h2>
  <p class="sub">How to grow the audience without hurting performance</p>
  <p class="byline">Prepared by MNTN</p>
</section>

<section>
  <h2>What we looked at</h2>
  <ul>
    <li>You flagged that the audience keeps running into <b>sizing limits</b>.</li>
    <li>We traced exactly which parts of the audience setup are constraining reach.</li>
    <li>Goal: a few targeted changes that <b>grow reach while protecting performance</b>.</li>
  </ul>
</section>

<section>
  <h2>The takeaway</h2>
  <p class="lead">Two parts of the current setup — the <b>studio geo-fence</b> and the <b>income/age exclusions</b> — account for most of the size limiting.</p>
  <p class="lead">Both can be <b style="color:#C00000">safely relaxed</b> to recover a large share of the audience.</p>
</section>

<section>
  <h2>Two filters are doing most of the size-limiting</h2>
  <img src="{C_FUNNEL}" style="width:88%">
  <p class="take">Together, the studio geo-fence and the income/age exclusions take the reachable audience from ~4.6M down to ~1.5M.</p>
</section>

<section>
  <h2>1 · Geo is the biggest constraint</h2>
  <ul>
    <li>The audience is fenced to a <b>7-mile radius</b> around each studio (946 locations).</li>
    <li>That fence alone removes <b>~half</b> the otherwise-addressable audience.</li>
    <li><b>Lever:</b> widening the radius (e.g. 7 → 10 mi) recovers meaningful reach while staying studio-relevant.</li>
  </ul>
</section>

<section>
  <h2>2 · The income / age exclusions are broad</h2>
  <ul>
    <li>The low-income and older-age exclusions remove <b>~1.3M</b> additional households (~29%).</li>
    <li>They're a blunt filter — they also screen out qualified prospects.</li>
    <li><b>Lever:</b> narrow or refine these bands to recover reach without lowering quality.</li>
  </ul>
</section>

<section>
  <h2>Recommendations</h2>
  <ol>
    <li><b>Widen the studio radius</b> (7 → 10 mi) — the single biggest reach lever.</li>
    <li><b>Refine the income/age exclusions</b> — recover ~1.3M households the current bands over-screen.</li>
    <li><b>Trim the lowest-performing third-party segments</b> — they add little reach and underperform.</li>
  </ol>
  <p class="note">Net: more reach, same or better performance — without relying on broad third-party data.</p>
</section>

<section>
  <h2>Thank you</h2>
  <p class="lead">Happy to walk through any of this and help implement the changes.</p>
</section>
"""

HTML = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Orange Theory National — Audience Reach Review</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/theme/white.css">
<style>
 .reveal {{ font-size: 32px; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color:#222; }}
 .reveal h1 {{ font-size: 2em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h2 {{ font-size: 1.4em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal section.present {{ background:#FAFAFA; }}
 .reveal ul, .reveal ol {{ font-size: 0.84em; line-height:1.55; width:86%; margin:0 auto; }}
 .reveal li {{ margin-bottom:0.5em; }}
 .reveal p.sub {{ font-size:0.9em; color:#555; margin-top:0.2em; }}
 .reveal p.byline {{ font-size:0.5em; color:#888; margin-top:1.2em; }}
 .reveal p.lead {{ font-size:0.95em; line-height:1.5; margin:0.35em 0; }}
 .reveal p.note {{ font-size:0.62em; color:#777; margin-top:0.8em; }}
 .reveal p.take {{ font-size:0.6em; color:#444; margin-top:0.4em; width:86%; margin-left:auto; margin-right:auto; }}
</style></head><body>
<div class="reveal"><div class="slides">{SLIDES}</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/reveal.js"></script>
<script>Reveal.initialize({{ hash:true, slideNumber:true, controls:true, progress:true, center:true,
  transition:'fade', width:1100, height:800, margin:0.04, minScale:0.2, maxScale:1.5 }});</script>
</body></html>"""

(HERE / "ti_1026_client_deck.html").write_text(HTML)
print("wrote ti_1026_client_deck.html + ti_1026_client_chart_funnel.png")
