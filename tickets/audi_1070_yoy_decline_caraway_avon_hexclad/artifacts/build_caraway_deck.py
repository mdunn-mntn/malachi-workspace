"""AUDI-1070 — CARAWAY deck. Story: Caraway didn't LEAVE High-Intent (gate held, flagship 82-99% HI) —
it OVERWHELMED it (spend +191% into a finite HI pool -> within-HI VR collapsed). The pacing-ceiling case,
the counterpart to HexClad's gate-removal. Same outline as HexClad v4."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
SIG=b64("caraway_signature.png"); PACING=b64("audi_1070_hexclad_pacing.png"); GANTT=b64("caraway_gantt.png"); BLIND=b64("caraway_score_blind.png"); SATUR=b64("caraway_saturation.png")

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — Caraway</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--red:#D63B2F;--green:#2E8B57;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:#222;}
.reveal h1{margin-top:0;font-size:1.4em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.0em;color:var(--navy);margin-bottom:0.25em;}
.reveal section img{margin:0.05em 0 0;border:0;box-shadow:none;background:#FAFAFA;max-height:600px;}
.cmp{font-size:0.6em;margin:0.15em auto;border-collapse:collapse;}
.cmp th{background:var(--navy);color:#fff;padding:0.3em 0.8em;}
.cmp td{padding:0.26em 0.8em;border-bottom:1px solid #e3e3e3;text-align:right;}
.big td{font-size:1.05em;padding:0.3em 1.0em;}
.claim{font-size:1.0em;color:var(--navy);font-weight:bold;line-height:1.3;}
.sub{color:var(--tl);font-size:0.58em;} .red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.note{font-size:0.46em;color:#999;margin-top:0.35em;}
.kpis{display:flex;justify-content:center;gap:1.4em;margin-top:0.7em;}
.kpi{text-align:center;} .kpi .n{font-size:1.5em;font-weight:bold;} .kpi .l{font-size:0.5em;color:var(--tl);}
ul.tight{font-size:0.62em;line-height:1.45;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.35em;}
.divider{color:#bbb;font-size:0.5em;letter-spacing:0.2em;}
</style></head><body><div class="reveal"><div class="slides">

<!-- 1 TITLE -->
<section>
<h1>Caraway — it didn't leave High-Intent, it overwhelmed it</h1>
<p class="claim" style="font-size:0.8em;margin-top:0.4em;">Spend nearly tripled into a finite High-Intent pool — the visit rate collapsed <i>inside</i> it.</p>
<div class="kpis" style="margin-top:0.8em;">
<div class="kpi"><div class="n red">+191%</div><div class="l">Spend</div></div>
<div class="kpi"><div class="n green">82–99%</div><div class="l">HI-share (held)</div></div>
<div class="kpi"><div class="n red">−69%</div><div class="l">Visit rate</div></div>
<div class="kpi"><div class="n red">4.34→1.26</div><div class="l">ROAS</div></div>
</div>
<p class="sub" style="margin-top:1.0em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; AUDI-1070 &middot; prospecting, Jan–May YoY</p>
</section>

<!-- 2 CAMPAIGNS -->
<section>
<h2>1 · The campaigns (client groups)</h2>
<table class="cmp">
<tr><th style="text-align:left">Group</th><th style="text-align:left">Client campaign</th><th>Role</th><th>Active</th><th>Spend</th></tr>
<tr style="background:#eef3f8"><td>92099</td><td style="text-align:left"><b>CTV Prospecting</b> (flagship 439156)</td><td>Prospecting</td><td>Jun '25–now</td><td class="navy"><b>$1.33M</b></td></tr>
<tr><td>123920</td><td style="text-align:left">CTV Prospecting - High DMA</td><td>Prospecting</td><td>May '26+</td><td>$144K</td></tr>
<tr><td>101460</td><td style="text-align:left">CTV Prospecting Test Campaign</td><td>Prospecting</td><td>Oct–Dec '25</td><td>$82K</td></tr>
<tr><td>123929</td><td style="text-align:left">CTV Prospecting - Low DMA</td><td>Prospecting</td><td>May '26+</td><td>$49K</td></tr>
<tr><td>88892</td><td style="text-align:left">CTV Prospecting All DMAs</td><td>Prospecting</td><td>Jun–Aug '25</td><td>$29K</td></tr>
</table>
<p class="note">One dominant flagship — <b>"CTV Prospecting" (439156, $1.33M, still running)</b> — carries the story. The rest are geographic (High/Low DMA) test cells and an Oct test. The flagship actually runs <b>many short flights</b> (18 of 47 ≤72h, per <code>core.flights</code>) — but unlike HexClad, its gate <b>held HI</b> through them (the short-flight HHST=0 wasn't applied here — Tofer's manual practice has gaps). So short-flights are not Caraway's cause.</p>
</section>

<section>
<h2>1b · Are the campaigns doing the same thing? — run-times &amp; gate</h2>
<img src="__GANTT__" style="max-height:440px">
<p class="note"><b>No handoff or competing campaign in the core window.</b> <b>Jul '25–Apr '26 = ONE campaign</b> (the flagship), gate held HI (green) — so the "same HI-share, half VR" is a single campaign genuinely over-scaling, not a HexClad-style blend. Two exceptions: a <b>Dec '25 holiday gate-drop</b> (red, 18.6% HI) and a <b>May 13 '26 handoff</b> — the flagship turned OFF and the new DMA test cells turned ON at 44–57% HI (that's the "one off / one on" pattern, but it explains the May–Jun dip, not the core collapse).</p>
</section>

<!-- 3 ASSUMPTIONS -->
<section>
<h2>2 · The assumptions — all <span class="red">false</span></h2>
<table class="cmp big">
<tr><th style="text-align:left">Assumption</th><th>Reality</th><th></th></tr>
<tr><td style="text-align:left">"It stepped outside High-Intent" (like HexClad)</td><td class="green">flagship stayed 82–99% HI</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"The audience/MM was cut"</td><td class="green">HI substrate (DS13∩DS19) intact</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"It's Fangorn"</td><td class="green">Fangorn only May–Jun '26 (after window)</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"Smaller baskets"</td><td class="green">AOV flat ($424→$447)</td><td class="red">✗</td></tr>
<tr><td style="text-align:left"><b>"Reach expansion can't cause this"</b></td><td class="red">it can — it overwhelmed the HI pool</td><td class="green">✓</td></tr>
</table>
<p class="note">Unlike HexClad, Caraway kept its gate on and stayed in High-Intent. The decline is a <b>pacing</b> story, not a gate story.</p>
</section>

<!-- 4 SCORE DIST BY CAMPAIGN -->
<section>
<h2>3 · Score distribution by campaign — the flagship stayed in HI</h2>
<table class="cmp">
<tr><th style="text-align:left">Campaign</th><th>Imps</th><th>HH</th><th>HI</th><th>PP</th><th>MI</th><th>Unscored</th></tr>
<tr style="background:#eef7f0"><td style="text-align:left"><b>439156 CTV Prospecting</b> (flagship)</td><td>55.5M</td><td>18.7M</td><td class="green"><b>82.4%</b></td><td>1.7</td><td>5.2</td><td>10.1</td></tr>
<tr><td style="text-align:left">419915 All-DMAs (Jun–Aug '25)</td><td>0.7M</td><td>0.5M</td><td class="green">99.9%</td><td>0</td><td>0</td><td>0.1</td></tr>
<tr><td style="text-align:left">490249 Test (Oct–Dec '25)</td><td>3.3M</td><td>2.3M</td><td>89.3%</td><td>9.1</td><td>0.8</td><td>0.7</td></tr>
<tr><td style="text-align:left">613551 High-DMA (May '26+)</td><td>4.9M</td><td>2.1M</td><td class="red">44.7%</td><td>15.2</td><td>8.4</td><td>27.7</td></tr>
<tr><td style="text-align:left">613591 Low-DMA (May '26+)</td><td>1.8M</td><td>0.8M</td><td>57.3%</td><td>10.2</td><td>7.2</td><td>20.4</td></tr>
</table>
<p class="note">The flagship (94% of prospecting volume) delivered <b>82% HI over its whole life</b> — it never left High-Intent (the 10% unscored is the Dec holiday + the May–Jun Fangorn onset). Contrast HexClad's flagship, which fell 97.8%→31% when its gate was removed. Caraway's gate <b>held</b>. RTC-excluded; HI = household_score ≥ 8,001.</p>
</section>

<!-- 5 SIGNATURE CHART -->
<section>
<h2>4 · Stayed in HI — but the visit rate collapsed inside it</h2>
<img src="__SIG__">
<p class="note"><b>The whole case in one chart.</b> HI-share (bars) held ~85–100% through 2026, yet the visit rate (line) fell by half: <b>Jul '25 = 99% HI → 0.37% VR; Mar '26 = 99.9% HI → 0.15% VR.</b> Same HI-share, half the visit rate. The only thing that changed is <b>how hard the pool was pushed</b> — impressions nearly tripled. It wasn't re-serving the same households (HI frequency stayed flat ~1.5); it reached <b>+136% more distinct HI households</b> (1.66M→3.92M) — <b>deeper into the pool, to weaker/marginal HI</b>. "HI" (score 10,000) is a membership flag, not a visit guarantee: at low spend you skim the best HI, at 3× you scrape the marginal HI that convert far worse.</p>
</section>

<section>
<h2>4b · The score is BLIND to it — maxed while VR halved</h2>
<img src="__BLIND__">
<p class="note"><b>Why it never recovers, and why score dashboards don't show it.</b> The scored-only average household_score sits at <b>~10,000 in EVERY gated month</b> — the flag is essentially binary (99–100% of scored impressions are <i>exactly</i> 10,000). <b>Aug '25: score 9,995 (highest) → VR 0.13% (lowest); Mar '26: score 10,000 (perfect) → VR 0.15%.</b> The score can't tell the best HI (0.37% VR) from the marginal HI (0.15% VR) — both read 10,000. So the collapse is invisible to the score, and from the platform's view "nothing degraded." <b>This is exactly what continuous scoring (Fangorn) fixes</b> — it grades within HI. (Caraway's Fangorn onset is May–Jun '26, after the window.)</p>
</section>

<section>
<h2>4c · The saturation, confirmed — the HI pool ran dry</h2>
<img src="__SATUR__">
<p class="note"><b>Direct confirmation.</b> Cumulative distinct HI households reached climbed to <b>16.5M</b> (a lower bound on the pool). The <b>brand-new share of monthly HI reach fell 100% → 35%</b>, crossing 50% around <b>Oct–Nov '25</b> — from then on the majority of HI reach is <b>re-served</b> HI (the lower end of the tier), and fresh-HI inflow declined through 2026. So spend tripled, the fresh HI ran out ~Oct–Nov, and delivery increasingly recycled marginal HI — exactly what a finite pool under 3× spend produces, and why VR never recovers. (Dec is the gate-off outlier.)</p>
</section>

<!-- 6 RATE METRICS -->
<section>
<h2>5 · Rate metrics — spend +191%, but visits & value FELL</h2>
<table class="cmp big">
<tr><th style="text-align:left">Prospecting, Jan–May</th><th>2025</th><th>2026</th><th>YoY</th></tr>
<tr><td style="text-align:left">Spend</td><td>$278K</td><td>$809K</td><td class="red" style="font-weight:bold">+191%</td></tr>
<tr><td style="text-align:left">Impressions</td><td>13.2M</td><td>36.1M</td><td class="red">+173%</td></tr>
<tr><td style="text-align:left">Visits</td><td>61,559</td><td>52,904</td><td class="red">−14%</td></tr>
<tr><td style="text-align:left"><b>Visit rate</b></td><td>0.465%</td><td>0.146%</td><td class="red" style="font-weight:bold">−69%</td></tr>
<tr><td style="text-align:left">Conversions</td><td>2,846</td><td>2,272</td><td class="red">−20%</td></tr>
<tr><td style="text-align:left">AOV</td><td>$424</td><td>$447</td><td class="navy">+6% (flat)</td></tr>
<tr><td style="text-align:left"><b>ROAS</b></td><td>4.34×</td><td>1.26×</td><td class="red" style="font-weight:bold">−71%</td></tr>
</table>
<p class="note">Nearly <b>3× the impressions produced FEWER visits</b> — the marginal impression, served into a saturated HI pool, converted at a fraction of the rate. Flat AOV ⇒ the value drop is a <b>conversion-count</b> problem, not smaller baskets.</p>
</section>

<!-- 7 WHAT HAPPENED + HEXCLAD CONTRAST -->
<section>
<h2>6 · Two advertisers, two failure modes — same HI-supply ceiling</h2>
<table class="cmp">
<tr><th style="text-align:left"></th><th>HexClad (34611)</th><th>Caraway (40341)</th></tr>
<tr><td style="text-align:left">Failure mode</td><td class="red">gate REMOVED</td><td class="red">OVER-SCALED HI</td></tr>
<tr><td style="text-align:left">Flagship HI-share</td><td>97.8% → <b>31%</b> (left HI)</td><td class="green">stayed <b>82–99%</b> (held HI)</td></tr>
<tr><td style="text-align:left">Spend YoY</td><td>+45%</td><td class="red">+191% (tripled)</td></tr>
<tr><td style="text-align:left">The gate</td><td>removed Nov 11, never reverted</td><td>mostly held (Dec dip only)</td></tr>
<tr><td style="text-align:left">Flights</td><td>short (&lt;72h → auto-0)</td><td>long (82-day avg)</td></tr>
<tr><td style="text-align:left"><b>Root cause</b></td><td><b>config</b> (gate off + short flights)</td><td><b>pacing</b> (spend outran HI supply)</td></tr>
</table>
<p class="note">Both decline because the addressable High-Intent supply is finite — HexClad <b>left</b> it (gate off), Caraway <b>exhausted</b> it (over-spent into it). Neither is a degraded MM model or a cut audience.</p>
</section>

<!-- 8 PACING -->
<section>
<h2>7 · The ceiling — pace against the live pool, not the budget</h2>
<img src="__PACING__" style="max-height:430px">
<p class="note">(Model from HexClad; the mechanism is identical.) HI is a <b>flow</b> — the live 30-day pool is ~half the lifetime figure, refilled by a roughly constant new-HI inflow. Sustainable HI spend is bounded by that inflow. Caraway pushed spend ~3× past it while staying gated → each marginal HI impression went to a re-served / weaker household → visit rate collapsed. The fix is <b>pacing</b>, not "fix the model."</p>
</section>

<!-- 9 QUESTIONS ANSWERED -->
<section>
<h2>8 · Questions answered</h2>
<table class="cmp" style="font-size:0.56em">
<tr><th style="text-align:left">Question</th><th style="text-align:left">Answer</th></tr>
<tr><td style="text-align:left">Did it step outside High-Intent (like HexClad)?</td><td style="text-align:left" class="green">No — flagship stayed 82–99% HI; the gate held</td></tr>
<tr><td style="text-align:left">Is it Fangorn / a scoring change?</td><td style="text-align:left" class="green">No — Fangorn only from May–Jun '26 (after the window)</td></tr>
<tr><td style="text-align:left">Did we cut the audience / MM?</td><td style="text-align:left" class="green">No — vertical DS13 ∩ keyword DS19 intact</td></tr>
<tr><td style="text-align:left">Smaller baskets?</td><td style="text-align:left" class="green">No — AOV flat/up ($424→$447)</td></tr>
<tr><td style="text-align:left">Can reach expansion really cause this?</td><td style="text-align:left">Yes — 3× impressions, fewer visits: over-scaling a finite HI pool</td></tr>
<tr><td style="text-align:left">Secondary factors?</td><td style="text-align:left">Dec '25 holiday gate-loosening; May–Jun '26 DMA test cells + Fangorn onset</td></tr>
</table>
</section>

<!-- 10 CONCLUSION -->
<section>
<h2>9 · Conclusion &amp; the fix</h2>
<div style="text-align:left;display:inline-block;margin-top:0.2em;">
<ul class="tight">
<li><b>Cause:</b> Caraway nearly <b>tripled prospecting spend (+191%)</b> while staying HI-gated. The finite High-Intent pool couldn't absorb it → within-HI visit rate collapsed −69% → ROAS 4.34→1.26. Not the audience, model, attribution, or a gate removal.</li>
<li><b class="navy">Pace HI spend</b> to what the pool can absorb (sustainable ~inflow-bounded rate); don't push 3× budget through a fixed High-Intent pool.</li>
<li><b class="navy">Or widen the addressable pool</b> — expand keywords / accept a defined PP tier — rather than re-serving the same HI harder.</li>
<li><b class="navy">Watch the new DMA test cells &amp; Fangorn onset</b> (May–Jun '26) — they add unscored/continuous-score delivery on top.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.4em;">Don't spend more into High-Intent than High-Intent can supply.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML=HTML.replace("__SIG__",SIG).replace("__PACING__",PACING).replace("__GANTT__",GANTT).replace("__BLIND__",BLIND).replace("__SATUR__",SATUR)
(DIR/"audi_1070_caraway_deck.html").write_text(HTML)
print(f"wrote audi_1070_caraway_deck.html ({len(HTML)//1024} KB, {HTML.count('<section>')} slides)")
