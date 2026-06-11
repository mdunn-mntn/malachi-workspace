#!/usr/bin/env python3
"""Build the TI-1026 RevealJS deck (self-contained HTML, charts embedded base64, CDN reveal.js).
House config per documentation/docs/revealjs_guide.md. Author on title slide; no other names."""
import base64
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "ti_1026_presentation_deck.html"

def img(name):
    b = base64.b64encode((HERE / name).read_bytes()).decode()
    return f"data:image/png;base64,{b}"

C_SCORE = img("ti_1026_chart_score_vr.png")
C_BENCH = img("ti_1026_chart_benchmark.png")
C_HHST = img("ti_1026_chart_hhst_delivery.png")

SLIDES = f"""
<section>
  <h1>Orange Theory National</h1>
  <h2 style="color:#C00000">Audience Evaluation</h2>
  <p class="sub">Why the visit rate is low — and what to change</p>
  <p class="byline">Malachi Dunn · Targeting Infrastructure · TI-1026</p>
</section>

<section>
  <h2>The situation</h2>
  <ul>
    <li>Sales reports recurring <b>audience-sizing</b> concerns on the national CTV campaign.</li>
    <li>The agency says the <b>non-MNTN-matched (3P) audiences run 8–10× worse</b> on visit rate.</li>
    <li>Goal: keep size while dropping weak 3P — or grow size — <b>without hurting visit rate</b>.</li>
  </ul>
</section>

<section>
  <h2>How the audience is built</h2>
  <p class="big">( MNTN&nbsp;Matched&nbsp;keywords <span style="color:#C00000">OR</span> 11&nbsp;bought&nbsp;3P&nbsp;segments )</p>
  <p class="big2">AND within 7 miles of a studio (946 fences)</p>
  <p class="note">Only the MNTN&nbsp;Matched households get a quality score. The 3P-only households do not.</p>
</section>

<section>
  <h2>Bottom line</h2>
  <p class="lead">We're targeting the <b>right</b> people, and <b>enough</b> of them.</p>
  <p class="lead">The low visit rate is a <b style="color:#C00000">creative / offer ceiling</b> — not a targeting gap.</p>
  <p class="lead">And the <b>3P segments can't fix it</b> — by design.</p>
</section>

<section>
  <img src="{C_SCORE}" style="width:92%">
  <p class="take">MNTN's score rank-orders responsiveness — the model is finding the right households.</p>
</section>

<section>
  <img src="{C_BENCH}" style="width:92%">
  <p class="take">Same platform, same audience quality — yet OTF lands far below peers. That gap is creative/offer, not targeting.</p>
</section>

<section>
  <h2>Two levers — only one is ours</h2>
  <div class="cols">
    <div class="col">
      <h3 style="color:#1F3864">Ours (mix)</h3>
      <p>Concentrate delivery on the top (10000 / High-Intent) tier — it visits at <b>1.35%</b> vs ~0.2% for the rest. Modest reach cost, real VR lift.</p>
    </div>
    <div class="col">
      <h3 style="color:#C00000">Theirs (ceiling)</h3>
      <p>Even our <b>best</b>-targeted households only reach the peer <b>median</b>. Creative, offer, landing page, brand pull — targeting can't move this.</p>
    </div>
  </div>
</section>

<section>
  <img src="{C_HHST}" style="width:92%">
  <p class="take">Gate ON (main campaign): 3P-only households are filtered out — 3P does ~nothing. Gate OFF: 3P delivers unscored junk = the 8–10× traffic.</p>
</section>

<section>
  <h2>Are we running out of MNTN&nbsp;Matched households?</h2>
  <ul>
    <li><b>Not at the current budget.</b> The main campaign paces to its ~$2,000/day budget most days in June on the scored, in-fence audience <i>alone</i> — no 3P needed.</li>
    <li><b>But headroom is thin.</b> It underdelivered in late May (35–60% of budget). The scored × 7-mi-geo pool is adequate, not deep — so scaling spend will hit a ceiling.</li>
    <li>The binding constraint is the <b>scored-household × studio-geo</b> intersection — <b>not</b> the 3P segments.</li>
  </ul>
</section>

<section>
  <h2>To grow size, expand MNTN&nbsp;Matched — never reach outside it</h2>
  <div class="cols">
    <div class="col">
      <h3 style="color:#375623">Grow without hurting VR</h3>
      <p>• Add on-target HIIT/strength/cardio keywords (more scored households at similar intent)<br>• Widen the geo radius 7→10 mi<br>• Relax the LiveRamp income/age exclusions (active — frees tens of millions)</p>
    </div>
    <div class="col">
      <h3 style="color:#BF8F00">Grow more, at a small VR cost</h3>
      <p>• Lower the score gate (6501 → mid-band): biggest immediate reach lever. Trades some VR — but mid-scored still beats unscored 3P by far.</p>
    </div>
  </div>
  <p class="note">Every one of these adds <b>scored</b> reach. 3P adds unscored reach, which is why it failed.</p>
</section>

<section>
  <h2>The 3P segments: drop all 11</h2>
  <ul>
    <li><b>~87%</b> of their households match no Orange Theory keyword → no intent signal.</li>
    <li>Delivering segments are broad-fitness or <b>yoga/pilates</b> — OTF is HIIT (wrong modality).</li>
    <li>Delivery is <b>bursty</b> (each loads ~2–4 days/month) — unreliable for pacing.</li>
    <li>Under the score gate they contribute <b>~1.5%</b> of delivery anyway.</li>
  </ul>
</section>

<section>
  <h2>Keywords: ~1 in 4 is off-target</h2>
  <p>The 379 MNTN&nbsp;Matched keywords are the engine — but <b>~94 (25%)</b> should be pruned or reviewed:</p>
  <p class="junk">Above Ground Pools · Antifreeze · Beer Mugs · CPUs · Motorcycle Lighting · Sway Bars · Coffee Grinders · "Class" · "Power" · "Experience"</p>
  <p class="note">Replace with on-target HIIT / strength / cardio / recovery terms — grows reach at similar intent.</p>
</section>

<section>
  <h2>Geo &amp; exclusions — not the bottleneck</h2>
  <ul>
    <li><b>Geo:</b> 946 studios × 7&nbsp;mi covers ~half the populated US, applied to both layers. Not the constraint.</li>
    <li><b>Income/age exclusions (LiveRamp):</b> <b>active</b> — remove tens of millions of IPs. A real reach lever if relaxed.</li>
    <li><b>Income/age exclusions (Oracle):</b> inert (no delivery) — cosmetic.</li>
    <li><b>Keep:</b> CRM-suppression, T-Mobile-cellular, past-visitor exclusions (hygiene).</li>
  </ul>
</section>

<section>
  <h2>Recommendations</h2>
  <ol>
    <li><b>Remove all 11 3P segments</b> — inert under the gate, junk without it.</li>
    <li><b>Grow size by expanding MNTN&nbsp;Matched</b>, in order: add on-target keywords + prune the ~94 off-target ones; widen geo 7→10 mi; relax LiveRamp income/age exclusions; then lower the score gate if more is needed.</li>
    <li><b>Lift VR now</b> by weighting toward the High-Intent (10000) tier.</li>
    <li><b>Set expectations:</b> creative/offer is the real ceiling — the biggest VR gains are on the advertiser's side.</li>
    <li><b>Want proof of lift?</b> Run a holdout (incrementality) test.</li>
  </ol>
</section>

<section>
  <h2>To <i>prove</i> our lift — run a holdout</h2>
  <p class="lead">Visit rate is observational. The only defensible "here's the incremental lift MNTN drives for you" number comes from a <b>holdout / incrementality test</b>.</p>
  <p class="note">Recommend offering one if Sales wants a number to put in front of the customer.</p>
</section>

<section>
  <h2>Drop 3P. Tighten to intent. The ceiling is creative.</h2>
  <p class="byline">TI-1026 · Targeting Infrastructure · independently validated (8-agent adversarial check)</p>
</section>
"""

HTML = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Orange Theory National — Audience Evaluation (TI-1026)</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/theme/white.css">
<style>
 .reveal {{ font-size: 32px; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color:#222; }}
 .reveal h1 {{ font-size: 2em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h2 {{ font-size: 1.4em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h3 {{ font-size: 1em; margin-top:0; text-transform:none; }}
 .reveal section.present {{ background:#FAFAFA; }}
 .reveal ul, .reveal ol {{ font-size: 0.82em; line-height:1.5; width:88%; margin:0 auto; }}
 .reveal li {{ margin-bottom:0.45em; }}
 .reveal p.sub {{ font-size:0.9em; color:#555; margin-top:0.2em; }}
 .reveal p.byline {{ font-size:0.5em; color:#888; margin-top:1.2em; }}
 .reveal p.big {{ font-size:1.1em; font-weight:bold; color:#1F3864; margin:0.2em 0; }}
 .reveal p.big2 {{ font-size:0.9em; font-weight:bold; color:#222; margin:0.2em 0; }}
 .reveal p.note {{ font-size:0.62em; color:#777; margin-top:0.8em; }}
 .reveal p.take {{ font-size:0.6em; color:#444; margin-top:0.4em; width:88%; margin-left:auto; margin-right:auto; }}
 .reveal p.lead {{ font-size:0.95em; line-height:1.45; margin:0.3em 0; }}
 .reveal p.junk {{ font-size:0.62em; color:#C00000; font-style:italic; width:82%; margin:0.5em auto; line-height:1.5; }}
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
