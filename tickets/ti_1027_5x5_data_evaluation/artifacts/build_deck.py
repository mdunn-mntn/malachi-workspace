#!/usr/bin/env python3
"""Build the TI-1027 RevealJS deck (self-contained HTML, charts embedded base64, CDN reveal.js).
House config per documentation/docs/revealjs_guide.md. Author on title slide; no other names."""
import base64
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "ti_1027_presentation_deck.html"

def img(name):
    b = base64.b64encode((HERE / name).read_bytes()).decode()
    return f"data:image/png;base64,{b}"

C_LEV = img("ti_1027_chart_leverage.png")
C_VENDOR = img("ti_1027_chart_vendor_comparison.png")
C_SCORE = img("ti_1027_chart_scorecard.png")
C_VERT = img("ti_1027_chart_vertical_dependence.png")
C_TIERS = img("ti_1027_chart_score_tiers.png")
C_LAYERS = img("ti_1027_chart_layered_uniqueness.png")
C_WTP = img("ti_1027_chart_wtp_scale.png")
C_RECENCY = img("ti_1027_chart_recency.png")
C_SPEND = img("ti_1027_chart_spend_comparison.png")
C_PRICING = img("ti_1027_chart_pricing.png")

SLIDES = f"""
<section>
  <h1>5x5 — Data Provider Evaluation</h1>
  <h2 style="color:#C00000">Is it worth renewing?</h2>
  <p class="sub">An estimation exercise — what 5x5 brings to MNTN Matched, and how it rates vs our other partners</p>
  <p class="byline">Malachi Dunn · Targeting Infrastructure · TI-1027</p>
</section>

<section>
  <h2>The question</h2>
  <ul>
    <li>5x5's contract ends <b>end of June</b>. Renew, drop, or renegotiate?</li>
    <li>It's a flat-fee data partner feeding <b>MNTN Matched</b> — one of ~8 external + 2 internal site-visit sources.</li>
    <li>The real question: is its impact on MNTN Matched <b>outsized relative to its scale, or in line?</b></li>
  </ul>
</section>

<section>
  <h2>Bottom line</h2>
  <p class="lead"><b style="color:#C00000">KEEP (renew).</b></p>
  <p class="lead">5x5 is only <b>3.6%</b> of raw data but contributes <b>~12%</b> of unique MNTN-Matched domain signal — <b>~3.4× outsized.</b></p>
  <p class="lead">It's the <b>#2 most-unique</b> of all our data partners, concentrated in <b>B2B</b> — our #1 Q2 growth theme — and it's <b>flat-fee</b>, so the cost doesn't scale.</p>
</section>

<section>
  <h2>How 5x5 feeds MNTN Matched</h2>
  <p class="big">IP → website visit → domain → vertical → MNTN&nbsp;Matched score</p>
  <p class="note">5x5 sends which households visited which sites. We classify those domains into industries; that signal helps score who to target. Its value is in the <b>domains</b> it sees — not new reach (we already see 74% of its households).</p>
</section>

<section>
  <img src="{C_LEV}" style="width:80%">
  <p class="take">3.6% of the raw data, ~12% of the unique usable signal — 5x5 punches ~3.4× above its weight.</p>
</section>

<section>
  <img src="{C_VENDOR}" style="width:88%">
  <p class="take">5x5 is the #2 unique-domain partner. The per-use $0.50-CPM vendors (33Across API, Sovrn, Cybba) add almost nothing unique.</p>
</section>

<section>
  <img src="{C_SPEND}" style="width:88%">
  <p class="take">On raw "touched spend" every big vendor looks the same (~$350–400K/day) — because they all see the same households. Touched volume can't tell vendors apart; only unique contribution can.</p>
</section>

<section>
  <img src="{C_SCORE}" style="width:86%">
  <p class="take">Rated on value × uniqueness × quality, with cost. The flat-fee feeds (Predactiv, 5x5) are the best deals; the redundant per-use feeds are the waste.</p>
</section>

<section>
  <img src="{C_VERT}" style="width:80%">
  <p class="take">If we drop 5x5, B2B verticals lose the most fresh domain coverage — exactly the area we're investing to grow.</p>
</section>

<section>
  <img src="{C_TIERS}" style="width:88%">
  <p class="take">Scored ≠ high-value — so we checked. 5x5's households are top-tier: 39% land in High Intent, the highest of any high-volume partner. It's not bringing low-value traffic.</p>
</section>

<section>
  <img src="{C_LAYERS}" style="width:74%">
  <p class="take">The "unique data" question, answered: only 20% of its IPs are unique, but 77% of the specific household→site visits are 5x5-only. The value is the data, not the reach.</p>
</section>

<section>
  <h2>"But don't other vendors already have it?" — No.</h2>
  <img src="{C_RECENCY}" style="width:90%">
  <p class="take">We only target the last 30 days. Within that window, vendors deliver on irregular cadences and their copies expire — so 5x5 is the SOLE source for 70% of its data and the freshest for 95%. Snapshot "overlap" overstated redundancy.</p>
</section>

<section>
  <img src="{C_WTP}" style="width:92%">
  <p class="take">$0.50 CPM = per 1,000 impressions. 5x5 touches ~34M impr/day → ~$6.3M/yr if billed like a CPM peer (the ceiling). Fair value ~$150–600K/yr. Place the flat fee here.</p>
</section>

<section>
  <h2>What we should pay — the definitive answer</h2>
  <img src="{C_PRICING}" style="width:96%">
  <p class="take">Anchor on a monthly rate (~$15–50K/mo) with a volume minimum. If they bill CPM, insist it's on matched impressions ($0.50 fair) — not all touched traffic. The recency finding makes the floor firmer: 70% of 5x5's data has no in-window substitute.</p>
</section>

<section>
  <h2>Is it worth the fee?</h2>
  <ul>
    <li>MNTN Matched touches <b>~$210–385M/yr</b> of media and drives a measured <b>~10–36% visit-rate lift</b>.</li>
    <li>Its value (via advertiser retention) is conservatively <b>tens of $M/yr</b>.</li>
    <li>5x5 uniquely supplies <b>~12%</b> of MM's domain signal (much more in B2B) — that clears a typical data-partner flat fee with margin.</li>
    <li><b>Keep</b> unless the fee is unusually large; then <b>renegotiate</b> (ask for full URLs, or a lower fee).</li>
  </ul>
</section>

<section>
  <h2>The counter-intuitive part</h2>
  <div class="cols">
    <div class="col">
      <h3 style="color:#375623">Flat-fee feeds = the good deals</h3>
      <p>5x5 &amp; Predactiv: most unique signal, fixed cost. The line item looks fixed, but you're getting the most non-redundant data per dollar.</p>
    </div>
    <div class="col">
      <h3 style="color:#C00000">Per-use feeds = the waste</h3>
      <p>33Across API (3% unique), Sovrn (2%), Cybba (6%) bill <b>per impression</b> for data we mostly already have. These — not 5x5 — are the cost-review targets.</p>
    </div>
  </div>
</section>

<section>
  <h2>Recommendations</h2>
  <ol>
    <li><b>Keep 5x5</b> (and Predactiv, Justuno) — high unique value. <b>Renew ≤ ~$50K/mo ($600K/yr)</b> with a volume minimum; or $0.50 CPM on matched impressions.</li>
    <li><b>Review the redundant per-use partners</b> (33Across API, Sovrn, Cybba) for savings — a separate exercise.</li>
    <li><b>Confirm the 5x5 flat fee</b> with billing to finalize keep vs renegotiate.</li>
    <li><b>Re-rate quarterly</b> — uniqueness shifts as our own bidstream coverage grows.</li>
  </ol>
</section>

<section>
  <h2>Keep the unique flat-fee feeds. Review the redundant per-use ones.</h2>
  <p class="byline">TI-1027 · Targeting Infrastructure</p>
</section>
"""

HTML = f"""<!doctype html><html><head><meta charset="utf-8">
<title>5x5 Data Provider Evaluation (TI-1027)</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/theme/white.css">
<style>
 .reveal {{ font-size: 32px; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color:#222; }}
 .reveal h1 {{ font-size: 1.9em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h2 {{ font-size: 1.4em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h3 {{ font-size: 1em; margin-top:0; text-transform:none; }}
 .reveal section.present {{ background:#FAFAFA; }}
 .reveal ul, .reveal ol {{ font-size: 0.82em; line-height:1.5; width:88%; margin:0 auto; }}
 .reveal li {{ margin-bottom:0.45em; }}
 .reveal p.sub {{ font-size:0.9em; color:#555; margin-top:0.2em; }}
 .reveal p.byline {{ font-size:0.5em; color:#888; margin-top:1.2em; }}
 .reveal p.big {{ font-size:1.0em; font-weight:bold; color:#1F3864; margin:0.2em 0; }}
 .reveal p.note {{ font-size:0.62em; color:#777; margin-top:0.8em; width:84%; margin-left:auto; margin-right:auto; }}
 .reveal p.take {{ font-size:0.58em; color:#444; margin-top:0.4em; width:88%; margin-left:auto; margin-right:auto; }}
 .reveal p.lead {{ font-size:0.95em; line-height:1.45; margin:0.3em 0; }}
 .reveal .cols {{ display:flex; gap:1.4em; width:90%; margin:0.4em auto; }}
 .reveal .col {{ flex:1; background:#fff; border:1px solid #E0E0E0; border-radius:8px; padding:0.7em 0.9em; }}
 .reveal .col p {{ font-size:0.6em; line-height:1.45; }}
</style></head><body>
<div class="reveal"><div class="slides">{SLIDES}</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/reveal.js"></script>
<script>
Reveal.initialize({{ hash:true, slideNumber:true, controls:true, progress:true, center:true,
  transition:'fade', transitionSpeed:'slow', width:1100, height:800, margin:0.01, minScale:0.2, maxScale:1.5 }});
</script></body></html>"""

OUT.write_text(HTML)
print(f"Wrote {OUT} ({len(HTML)//1024} KB)")
