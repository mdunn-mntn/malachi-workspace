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
<title>ElevenLabs CTV Incrementality: Pre vs Post</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root {{ --navy:#1B2A4A; --text:#222; --text-light:#666; }}
.reveal {{ font-size: 32px; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color: var(--text); }}
.reveal h1 {{ font-size: 1.6em; color: var(--navy); margin-top: 0; text-transform: none; letter-spacing: -0.5px; }}
.reveal h2 {{ font-size: 1.1em; color: var(--navy); margin-top: 0; text-transform: none; letter-spacing: -0.3px; }}
.reveal p {{ font-size: 0.75em; line-height: 1.5; }}
.reveal .footer-note {{ text-align: center; font-size: 0.5em; color: #999; margin-top: 0.6em; }}
.reveal img {{ max-height: 580px; border: none; box-shadow: none; background: transparent; }}
.reveal table {{ font-size: 0.55em; margin: 0 auto; }}
.reveal td, .reveal th {{ padding: 0.35em 0.6em; }}
.reveal .author {{ color: var(--navy); font-size: 0.7em; margin-top: 1.2em; }}
.reveal .num {{ color: #BBB; margin-right: 0.4em; }}
</style>
</head>
<body>
<div class="reveal"><div class="slides">

<section>
  <h1>ElevenLabs CTV Incrementality:<br>Pre vs Post the July Changes</h1>
  <p class="author">Malachi Dunn</p>
  <p class="footer-note">August 2026 · randomized ghost-bid holdout, campaign group 122748</p>
</section>

<section>
  <h2>Visit lift: +11.1% pre, +16.5% post. Significant in both periods.</h2>
  <img src="{CH_PREPOST}" alt="Pre vs post lift, visits and conversions">
  <p class="footer-note">Pre = 6/23-6/30, post = 7/11-8/13. Transition window 7/1-7/10 excluded from both.</p>
</section>

<section>
  <h2>Day by day: a dip during the transition, then a climb</h2>
  <img src="{CH_DAILY}" alt="Daily visit lift with change dates marked">
  <p class="footer-note">7-day rolling window. Dashed lines mark each account change.</p>
</section>

<section>
  <h2>Conversion lift is not measurable at this spend</h2>
  <p>+11% pre, +35% post. Neither is significant at the 0.06% conversion base rate.</p>
  <p>Detecting a 5% conversion lift takes ~$2M/month; the same lift on visits takes $36K.</p>
</section>

<section>
  <h2>Dashboards fell because the audience changed, not the ads</h2>
  <p>The new audience's baseline visit rate is 6x lower (holdout: 0.824% before, 0.136% after, no ads involved).</p>
  <p>Attributed visits fell with it. Lift versus holdout held.</p>
</section>

<section>
  <h2>Lift by frequency: 2-10 exposures best, 11+ negative</h2>
  <img src="{CH_FREQ}" alt="Visit lift by exposure band">
  <p class="footer-note">70% of reached households see 3 or fewer impressions. Frequency targets are the next change.</p>
</section>

<section>
  <h2>What changed</h2>
  <div style="font-size:0.65em; line-height:1.9; text-align:left; display:inline-block;">
    <span class="num">6/30</span> Audience swapped to precision segments; conversion window to 43 days; visitor/converter blocks 30 to 90 days<br>
    <span class="num">7/1</span> CRM customer suppression added<br>
    <span class="num">7/16</span> Three custom ElevenLabs segments added<br>
    <span class="num">7/24 · 7/29</span> Further segment additions and a targeting rewrite
  </div>
</section>

<section>
  <h2>How it is measured</h2>
  <p>10% of the audience is always held out at random. The bidder logs the bid it would have placed, never serves it, and lift is the visit-rate gap.</p>
  <p class="footer-note">One count per household at first bid, outcomes within 7 days. Holdout share 9.2-9.5% every period. One-pager attached.</p>
</section>

<section>
  <h2>Full numbers</h2>
  <table>
    <tr><th>Period</th><th>Outcome</th><th>Reached</th><th>Holdout</th><th>Reached rate</th><th>Holdout rate</th><th>Lift</th><th>p</th></tr>
    <tr><td>Pre 6/23-6/30</td><td>Visits</td><td>4,200,746</td><td>441,242</td><td>0.916%</td><td>0.824%</td><td><b>+11.14%</b></td><td>1.7e-10</td></tr>
    <tr><td>Post 7/11-8/13</td><td>Visits</td><td>6,639,322</td><td>672,152</td><td>0.158%</td><td>0.136%</td><td><b>+16.46%</b></td><td>2.6e-06</td></tr>
    <tr><td>Pre 6/23-6/30</td><td>Conversions</td><td>4,200,746</td><td>441,242</td><td>0.0179%</td><td>0.0161%</td><td>+11.25%</td><td>0.37</td></tr>
    <tr><td>Post 7/11-8/13</td><td>Conversions</td><td>6,639,322</td><td>672,152</td><td>0.0040%</td><td>0.0030%</td><td>+34.65%</td><td>0.15</td></tr>
  </table>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>
Reveal.initialize({{ hash: true, slideNumber: true, controls: true, progress: true, center: true,
  transition: 'none', width: 1100, height: 800, margin: 0.01, minScale: 0.2, maxScale: 1.5 }});
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
print("plain deck written")
