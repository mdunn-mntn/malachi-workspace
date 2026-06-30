"""AUDI-1070 — Avon (31921) VERIFIED-CASE deck (RevealJS). Embeds the 6 avon_case_*.png
charts as base64. Three-act: concern -> revelation (fewer users, more money) -> why + proof.
Distinct from build_avon_deck.py (other session). Builds CDN deck + zero-dependency standalone.
All numbers verified (workflow wf_733743cd-c9c)."""
import base64, pathlib, urllib.request
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(name): return "data:image/png;base64," + base64.b64encode((DIR / name).read_bytes()).decode()
C1=b64("avon_case_1_headline.png"); C2=b64("avon_case_2_trend.png"); C3=b64("avon_case_3_inflection.png")
C4=b64("avon_case_4_no_expansion.png"); C5=b64("avon_case_5_audience_timeline.png"); C6=b64("avon_case_6_triangulation.png")

HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Avon — Performance Diagnosis (AUDI-1070)</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--blue:#2E5090;--green:#2E8B57;--red:#D63B2F;--text:#222;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:var(--text);}
.reveal h1{margin-top:0;font-size:1.55em;color:var(--navy);line-height:1.15;}
.reveal h2{margin-top:0;font-size:1.18em;color:var(--navy);margin-bottom:0.25em;}
.reveal section img{margin:0.05em auto 0;border:0;box-shadow:none;background:#FAFAFA;max-height:600px;}
.sub{color:var(--tl);font-size:0.6em;} .claim{font-size:1.05em;color:var(--navy);font-weight:bold;line-height:1.35;}
.red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.lead{font-size:0.6em;color:var(--tl);line-height:1.5;margin-top:0.7em;}
.note{font-size:0.44em;color:#999;margin-top:0.4em;}
ul.tight{font-size:0.62em;line-height:1.5;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.55em;}
.power-line{font-size:1.5em;color:var(--navy);font-weight:bold;line-height:1.3;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<h1>Avon: is performance declining?</h1>
<p class="sub" style="margin-top:0.6em;">A raw-counts diagnosis of the YoY question &nbsp;|&nbsp; AUDI-1070 &nbsp;|&nbsp; advertiser 31921</p>
<p class="sub" style="margin-top:1.1em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; 2026-06-30</p>
</section>

<section>
<h2>The concern</h2>
<p class="claim">"Avon's visits are down year-over-year —<br>is MNTN Matched degrading?"</p>
<p class="lead">Avon was flagged alongside Caraway &amp; HexClad as evidence of "general degradation in MNTN Matched over time." We rebuilt Avon from raw counts to test it — every number below is reproduced and triangulated against independent source-of-truth tables.</p>
</section>

<section>
<p class="sub">Bottom line</p>
<p class="power-line" style="margin-top:0.3em;">Avon reached fewer users —<br>and made <span class="green">more money</span>.</p>
<p class="lead">On <b>12% less spend</b>, Avon served fewer impressions to fewer users, but delivered <b>more conversions, more revenue, and higher ROAS</b>. The "decline" is a volume story - a smaller budget at higher CPM - not a performance story.</p>
</section>

<section>
<h2>Fewer users, more money</h2>
<img src="__C1__">
<p class="note">Jan-May 2025 vs 2026, last-touch. Volume metrics (gray) fell; money metrics (green) all rose. The only statistically significant changes: conversion rate +22% (up) and visits -16% (down).</p>
</section>

<section>
<h2>Performance holds across 30 months</h2>
<img src="__C2__">
<p class="note">ROAS never trends down - it stays in a healthy 10-38x band and its Jan-May average rises 17.3x to 20.7x. Volume swings with budget; efficiency does not erode.</p>
</section>

<section>
<h2>Why fewer users: a reach ceiling, not a loss</h2>
<img src="__C3__">
<p class="note">Jan-May 2026 each $1k reaches ~31k users vs ~36k in 2025 as frequency rises +13%. Extra delivery now buys repetition, not new users - and performance still held.</p>
</section>

<section>
<h2>No audience expansion to blame</h2>
<img src="__C4__">
<p class="note">Avon's single prospecting campaign contracted -26% while its ROAS rose; 100% of 2026 impressions came from campaigns also active in 2025. Caraway/HexClad, by contrast, added 9-19M net prospecting impressions.</p>
</section>

<section>
<h2>Honest caveat: the audience is not static</h2>
<img src="__C5__">
<p class="note">Avon's prospecting is MNTN-derived (Fangorn never used) but was progressively refined toward higher intent - broad vertical to MNTN-Matched to conquest scoring. Any YoY spans two audience/scoring regimes; this points toward fewer, higher-quality users, not degradation.</p>
</section>

<section>
<h2>Verified across three independent tables</h2>
<img src="__C6__">
<p class="note">The rollup, the clickpass visit log, and the conversion log all show the same signature: visits down double-digit, money up mid-single-digit. The case does not depend on one table.</p>
</section>

<section>
<h2>Three takeaways</h2>
<ul class="tight">
<li><span class="navy">1. Avon's performance did not decline.</span> On <b>12% less spend</b>: <span class="green">+19% ROAS, +22% conversion rate, +4% revenue</span>, visit rate flat (Jan-May YoY).</li>
<li><span class="navy">2. The drop is volume, not quality.</span> Visits fell because spend was cut 12% at +5% CPM; the high-intent pool also reaches fewer users per dollar (frequency up) - the bidder trades reach for frequency, never quality.</li>
<li><span class="navy">3. Avon is the clean control against "MM degradation."</span> Same campaigns, no expansion, audience refined toward higher intent - quality held while the cookware scalers saturated.</li>
</ul>
<p class="claim" style="font-size:0.8em;margin-top:0.7em;">Avon reached fewer users - and made more money.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>
Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,
transition:'fade',transitionSpeed:'slow',width:1100,height:800,margin:0.01,minScale:0.2,maxScale:1.5});
</script></body></html>
"""
for k, v in [("__C1__",C1),("__C2__",C2),("__C3__",C3),("__C4__",C4),("__C5__",C5),("__C6__",C6)]:
    HTML = HTML.replace(k, v)
out = DIR / "avon_case_deck.html"; out.write_text(HTML)
print(f"wrote {out.name} ({len(HTML)//1024} KB, 11 slides)")

CDN = [("reveal.css","https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css"),
       ("white.css","https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css"),
       ("reveal.js","https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js")]
h = HTML
for name, url in CDN:
    tmp = pathlib.Path(f"/tmp/{name}")
    if not tmp.exists():
        urllib.request.urlretrieve(url, tmp)
    content = tmp.read_text()
    if name.endswith(".css"):
        h = h.replace(f'<link rel="stylesheet" href="{url}">', f"<style>{content}</style>")
    else:
        h = h.replace(f'<script src="{url}"></script>', f"<script>{content}</script>")
sa = DIR / "avon_case_deck_standalone.html"; sa.write_text(h)
print(f"wrote {sa.name} ({len(h)//1024} KB, zero-dependency)")
