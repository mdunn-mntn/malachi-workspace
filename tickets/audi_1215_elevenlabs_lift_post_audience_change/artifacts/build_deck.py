"""Build the AUDI-1215 RevealJS deck (charts inlined as base64) and its standalone copy."""
import base64
import os
import urllib.request

T = "tickets/audi_1215_elevenlabs_lift_post_audience_change/artifacts"

def b64(name):
    with open(f"{T}/{name}", "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode()

CH_DAILY, CH_PREPOST, CH_FREQ = (b64(f"audi_1215_chart_{n}.png") for n in ("daily_lift", "prepost_lift", "frequency_lift"))

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ElevenLabs CTV Incrementality: The Study You Asked For</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root {{ --navy:#1B2A4A; --blue:#2E5090; --red:#D63B2F; --text:#222; --text-light:#666; }}
.reveal {{ font-size: 32px; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color: var(--text); }}
.reveal h1 {{ font-size: 1.7em; color: var(--navy); margin-top: 0; text-transform: none; letter-spacing: -0.5px; }}
.reveal h2 {{ font-size: 1.15em; color: var(--navy); margin-top: 0; text-transform: none; letter-spacing: -0.3px; }}
.reveal p {{ font-size: 0.8em; line-height: 1.5; }}
.reveal .quote {{ font-size: 0.85em; color: var(--navy); font-style: italic; line-height: 1.5; max-width: 26em; margin: 0 auto; }}
.reveal .big-number {{ font-size: 4.6em; font-weight: 700; color: var(--red); letter-spacing: -2px; line-height: 1.05; }}
.reveal .big-number-context {{ font-size: 0.72em; color: var(--text-light); }}
.reveal .footer-note {{ text-align: center; font-size: 0.5em; color: #999; margin-top: 0.6em; }}
.reveal img {{ max-height: 560px; border: none; box-shadow: none; background: transparent; }}
.reveal .power-line {{ font-size: 1.45em; font-weight: 700; color: var(--navy); line-height: 1.35; }}
.reveal .steps {{ font-size: 0.72em; line-height: 1.9; text-align: left; display: inline-block; }}
.reveal .steps b {{ color: var(--navy); }}
.reveal .num {{ color: #BBB; margin-right: 0.4em; }}
.reveal table {{ font-size: 0.55em; margin: 0 auto; }}
.reveal td, .reveal th {{ padding: 0.35em 0.6em; }}
.reveal .author {{ color: var(--navy); font-size: 0.75em; margin-top: 1.2em; }}
.reveal .kicker {{ font-size: 0.6em; letter-spacing: 2.5px; text-transform: uppercase; color: var(--text-light); }}
</style>
</head>
<body>
<div class="reveal"><div class="slides">

<section>
  <p class="kicker">MNTN Measurement</p>
  <h1>ElevenLabs CTV Incrementality:<br>The Study You Asked For</h1>
  <p class="author">Malachi Dunn</p>
  <p class="footer-note">August 2026</p>
</section>

<section>
  <p class="kicker">June. Your measurement review, question 2</p>
  <p class="quote">"Can we run a MNTN-side conversion-lift study (ghost ads / PSA holdout) to triangulate with our geo results?"</p>
  <p class="fragment" style="margin-top:1.2em; font-weight:700; color:var(--navy); font-size:0.95em;">We ran it. It has been running the whole time.</p>
  <aside class="notes">Tone: generous, not gotcha. "You asked exactly the right question. Here is the answer it produces."</aside>
</section>

<section>
  <h2>Ghost bidding: a clinical trial inside the bidder</h2>
  <div class="steps">
    <span class="fragment"><span class="num">1</span><b>Random split.</b> 10% of your audience is always held out, by household.<br></span>
    <span class="fragment"><span class="num">2</span><b>Same auctions.</b> The bidder values every auction identically for both groups.<br></span>
    <span class="fragment"><span class="num">3</span><b>One difference.</b> For held-out households the bid is logged, never served.<br></span>
    <span class="fragment"><span class="num">4</span><b>Compare.</b> The visit-rate gap is what the ads caused.</span>
  </div>
  <p class="footer-note">No media is spent on the control. 7-day outcome windows and quality gates (detailed in the attached one-pager) keep it conservative.</p>
  <aside class="notes">Story: On July 11 the bidder priced an auction for a household it had been told never to serve. Same auction, same valuation, same targeting. It logged the bid and stayed silent. It has done that 672,000 times since. Those households answer the only question that matters: what would have happened anyway. They visited at 0.136%. The households that saw your ads: 0.158%.</aside>
</section>

<section>
  <p class="big-number-context">Since July 11: ads reached 6.6M households. 672K were held out.</p>
  <div class="big-number fragment">+16.5%</div>
  <p class="big-number-context">visit lift versus the holdout since your July changes &nbsp;·&nbsp; p &lt; 0.000003</p>
</section>

<section>
  <h2>Lift held through the July changes</h2>
  <img src="{CH_PREPOST}" alt="Pre vs post lift, visits and conversions">
  <p class="footer-note">Randomized holdout, one count per household. Transition window 7/1-7/10 excluded from both periods.</p>
</section>

<section>
  <h2>Day by day: a dip while changes settled, then a climb</h2>
  <img src="{CH_DAILY}" alt="Daily visit lift with change dates marked">
  <p class="footer-note">7-day rolling window. Dashed lines mark each change on the account.</p>
</section>

<section>
  <h2>Why your dashboards look worse</h2>
  <p style="max-width:30em; margin:0.4em auto;">The precision audience visits your site <b style="color:var(--navy);">6x less on its own</b>. That was the point: less spend on people who were coming anyway.</p>
  <p style="max-width:30em; margin:0.4em auto;">Credited visits fell. The share of visits <b style="color:var(--navy);">caused by the ads</b> held, then rose.</p>
  <p style="max-width:30em; margin:0.4em auto;">Attribution counts touches. Incrementality counts causes.</p>
  <p class="footer-note">Measured in the holdout, no ads involved: baseline visit rate 0.824% before, 0.136% after. The holdout fell 6x too. The gap is what grew.</p>
</section>

<section>
  <h2>Your null and our lift are the same finding</h2>
  <p style="max-width:32em; margin:0.4em auto;">At a 0.06% B2B conversion rate, this spend cannot resolve even a 5% conversion lift. Your largest country read still landed at ~0% (p=0.81). Ours is directionally up, not yet significant. All three are the same statement.</p>
  <p class="fragment" style="margin:0.8em auto; font-size:1.05em;">Detecting a 5% conversion lift: <b style="color:var(--navy);">$2M/month</b>.<br>The same lift on visits: <b style="color:var(--navy);">$36K</b>.</p>
  <p style="max-width:32em; margin:0.4em auto; color:var(--text-light); font-size:0.72em;">A power problem, not a performance problem. Conversions lag; visits are the readable KPI.</p>
</section>

<section>
  <h2>The next lever: frequency</h2>
  <img src="{CH_FREQ}" alt="Visit lift by exposure band">
  <p class="footer-note">70% of reached households see 3 or fewer impressions while the 11+ tail absorbs spend at negative lift. Frequency targets move spend from the red bar to the navy bars.</p>
</section>

<section>
  <h2>Three next steps</h2>
  <div style="margin-top:1em; font-size:0.78em; line-height:2; text-align:left; display:inline-block; max-width:30em;">
    <span class="num">1.</span> Resume the campaign. The holdout only measures while it runs: no spend, no experiment, and the conversion answer never arrives.<br>
    <span class="num">2.</span> Set frequency targets on the campaign. The playbook exists and feasibility is confirmed.<br>
    <span class="num">3.</span> Read visits as the primary KPI, and enroll in the incrementality measurement beta so this view is always on.
  </div>
</section>

<section>
  <div class="power-line">Your CTV causes visits.<br>Measured the way you asked.</div>
  <p style="text-align:center; color:var(--text-light); font-size:0.7em; margin-top:1.4em;">+11.1% before your changes, +16.5% after, each significant on its own. Randomized, always-on holdout.</p>
</section>

<section>
  <h2>Appendix: what changed, when</h2>
  <div style="font-size:0.62em; line-height:1.9; text-align:left; display:inline-block;">
    <span class="num">6/30</span> Audience swapped to MNTN-suggested precision segments; conversion window to 43 days; visitor and converter blocks 30 to 90 days<br>
    <span class="num">7/1</span> CRM customer suppression added<br>
    <span class="num">7/16</span> Three custom ElevenLabs segments added<br>
    <span class="num">7/24 · 7/29</span> Further audience additions and a targeting rewrite<br>
  </div>
  <p class="footer-note">The 7/1-7/10 transition window is excluded from every pre/post average in this deck.</p>
</section>

<section>
  <h2>Appendix: method and full numbers</h2>
  <table>
    <tr><th>Period</th><th>Outcome</th><th>Reached</th><th>Holdout</th><th>Reached rate</th><th>Holdout rate</th><th>Lift</th><th>p</th></tr>
    <tr><td>Pre 6/23-6/30</td><td>Visits</td><td>4,200,746</td><td>441,242</td><td>0.916%</td><td>0.824%</td><td><b>+11.14%</b></td><td>1.7e-10</td></tr>
    <tr><td>Post 7/11-8/13</td><td>Visits</td><td>6,639,322</td><td>672,152</td><td>0.158%</td><td>0.136%</td><td><b>+16.46%</b></td><td>2.6e-06</td></tr>
    <tr><td>Pre 6/23-6/30</td><td>Conversions</td><td>4,200,746</td><td>441,242</td><td>0.0179%</td><td>0.0161%</td><td>+11.25%</td><td>0.37</td></tr>
    <tr><td>Post 7/11-8/13</td><td>Conversions</td><td>6,639,322</td><td>672,152</td><td>0.0040%</td><td>0.0030%</td><td>+34.65%</td><td>0.15</td></tr>
  </table>
  <p class="footer-note">Intent-to-treat on the randomized ghost-bid holdout. One count per household at first bid; outcomes within 7 days. Holdout share 9.2-9.5% in every period.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>
Reveal.initialize({{ hash: true, slideNumber: true, controls: true, progress: true, center: true,
  transition: 'fade', transitionSpeed: 'slow', width: 1100, height: 800, margin: 0.01, minScale: 0.2, maxScale: 1.5 }});
</script>
</body>
</html>"""

deck = f"{T}/audi_1215_presentation_deck.html"
with open(deck, "w") as f:
    f.write(HTML)

cdn = [("reveal.css", "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css"),
       ("white.css", "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css"),
       ("reveal.js", "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js")]
html = HTML
for name, url in cdn:
    path = f"/tmp/revealjs_{name}"
    if not os.path.exists(path):
        urllib.request.urlretrieve(url, path)
    content = open(path).read()
    if name.endswith(".css"):
        html = html.replace(f'<link rel="stylesheet" href="{url}">', f"<style>{content}</style>")
    else:
        html = html.replace(f'<script src="{url}"></script>', f"<script>{content}</script>")
with open(f"{T}/audi_1215_presentation_deck_standalone.html", "w") as f:
    f.write(html)
print("deck + standalone written")
