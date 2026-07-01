"""AUDI-1070 — HexClad deck v3: numbers/tables-heavy, assumptions upfront, tight build.
campaigns->audience->symptom->rule-outs->backbone->gate->clincher->rebuttal->why->what-changed->numbers.
Last-touch (Mike's report) for perf; tier composition uses H2-2025 (Jan-May 2025 predates scoring). 5 charts."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
PARADOX=b64("audi_1070_hexclad_paradox.png"); COMPO=b64("audi_1070_hexclad_composition.png")
HHST=b64("audi_1070_hexclad_hhst.png"); TIER=b64("audi_1070_hexclad_visit_rate_by_tier.png")
SUPPLY=b64("audi_1070_hexclad_supply.png")

MET=[("Spend","$642,267","$931,422","+45%","in"),("Impressions","30.7M","40.8M","+33%","in"),
 ("Households reached","11.5M","14.1M","+22%","in"),("Verified Visits","111,053","68,214","−39%","bad"),
 ("Visit rate","0.362%","0.167%","−54%","bad"),("Conversions","4,978","2,495","−50%","bad"),
 ("AOV","$405.38","$397.38","−2%","flat"),("Order Value","$2.02M","$0.99M","−51%","bad"),
 ("ROAS","3.14×","1.06×","−66%","bad")]
def kc(k): return {"in":"#27496D","bad":"#D63B2F","flat":"#888"}[k]
metrows="\n".join(f'<tr><td style="text-align:left">{m}</td><td>{a}</td><td>{b}</td>'
    f'<td style="color:{kc(k)};font-weight:bold">{y}</td></tr>' for (m,a,b,y,k) in MET)

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — HexClad</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--red:#D63B2F;--green:#2E8B57;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:#222;}
.reveal h1{margin-top:0;font-size:1.4em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.0em;color:var(--navy);margin-bottom:0.25em;}
.reveal section img{margin:0.05em 0 0;border:0;box-shadow:none;background:#FAFAFA;max-height:600px;}
.cmp{font-size:0.6em;margin:0.15em auto;border-collapse:collapse;}
.cmp th{background:var(--navy);color:#fff;padding:0.3em 0.9em;}
.cmp td{padding:0.26em 0.9em;border-bottom:1px solid #e3e3e3;text-align:right;}
.big td{font-size:1.05em;padding:0.3em 1.0em;}
.claim{font-size:1.0em;color:var(--navy);font-weight:bold;line-height:1.3;}
.sub{color:var(--tl);font-size:0.58em;} .red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.note{font-size:0.46em;color:#999;margin-top:0.35em;}
.kpis{display:flex;justify-content:center;gap:1.4em;margin-top:0.7em;}
.kpi{text-align:center;} .kpi .n{font-size:1.5em;font-weight:bold;} .kpi .l{font-size:0.5em;color:var(--tl);}
ul.tight{font-size:0.6em;line-height:1.4;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.3em;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<h1>HexClad — Prospecting ROAS Fell 3×</h1>
<p class="claim" style="font-size:0.8em;margin-top:0.4em;">It didn't get worse at High-Intent — it ran out of it.</p>
<div class="kpis" style="margin-top:0.8em;">
<div class="kpi"><div class="n navy">+45%</div><div class="l">Spend</div></div>
<div class="kpi"><div class="n red">−51%</div><div class="l">Order Value</div></div>
<div class="kpi"><div class="n red">3.14→1.06</div><div class="l">ROAS</div></div>
<div class="kpi"><div class="n red">95%→49%</div><div class="l">% High-Intent</div></div>
</div>
<p class="sub" style="margin-top:1.0em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; AUDI-1070 &middot; Jan–May 2025 vs 2026, prospecting</p>
</section>

<section>
<h2>The 4 assumptions everyone holds — all <span class="red">false</span></h2>
<table class="cmp">
<tr><th style="text-align:left">Assumption</th><th>2025</th><th>2026</th><th></th></tr>
<tr><td style="text-align:left">"Same campaign YoY"</td><td>225087</td><td>446801</td><td class="red">✗</td></tr>
<tr><td style="text-align:left"><b>"We only target High-Intent"</b></td><td><b class="green">95.6% HI</b></td><td><b class="red">49.6% HI</b></td><td class="red">✗</td></tr>
<tr><td style="text-align:right;color:#888;font-size:0.9em">Peak Performance (8k)</td><td>0.7%</td><td class="red" style="font-weight:bold">21.6%</td><td></td></tr>
<tr><td style="text-align:right;color:#888;font-size:0.9em">Mid / unscored</td><td>3.0%</td><td>28.5%</td><td></td></tr>
<tr><td style="text-align:left">"Same % of HI"</td><td>95.6%</td><td class="red" style="font-weight:bold">49.6%</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"Audience mis-set / cut"</td><td>MM · 78 kw</td><td>MM · 89 kw</td><td class="red">✗</td></tr>
</table>
<p class="note">Same campaigns? No. Only HI? No — 51% is now lower tiers. Same %? No — HI halved. Broken config? No — clean MM, keywords grew. The rest of the deck proves each.</p>
</section>

<section>
<h2>Campaigns (2 years)</h2>
<table class="cmp">
<tr><th style="text-align:left">Group</th><th>obj</th><th>Channel</th><th>2026 ROAS</th><th>Scope</th></tr>
<tr><td style="text-align:left">Prospecting — Stage 1 (Beeswax TV)</td><td>1</td><td>CTV</td><td>1.06×</td><td class="green">analyzed</td></tr>
<tr><td style="text-align:left">Prospecting — Multi-Touch</td><td>1</td><td>CTV/disp</td><td>—</td><td class="green">all-stages</td></tr>
<tr><td style="text-align:left">Retargeting</td><td>4</td><td>CTV/disp</td><td>61.9×</td><td>excluded — healthy</td></tr>
</table>
<p class="note">Prospecting = objective_id 1. Retargeting (obj 4) is healthy (ROAS 55→62) → not a tracking/site-wide issue. Analysis = prospecting only.</p>
</section>

<section>
<h2>Audience — clean MM, unchanged config</h2>
<table class="cmp">
<tr><th style="text-align:left">Layer</th><th>2025</th><th>2026</th></tr>
<tr><td style="text-align:left">MM keywords (DS19)</td><td>78</td><td class="green">89 (grew)</td></tr>
<tr><td style="text-align:left">Exclusions (CRM / visitor / converter)</td><td>yes</td><td>yes</td></tr>
<tr><td style="text-align:left">Geo</td><td>US only</td><td>US only</td></tr>
<tr><td style="text-align:left">Holdout / RTC / HHST gate</td><td>yes</td><td>yes</td></tr>
<tr><td style="text-align:left">3P / geo-narrow overlay</td><td>none</td><td>none</td></tr>
</table>
<p class="note">Proper MM prospecting both years. Not a bad overlay, not a keyword cut. The change is in the scoring/gate layer downstream.</p>
</section>

<section>
<h2>The symptom: spend +45%, but visits & order value FELL</h2>
<img src="__PARADOX__">
<p class="note"><b>Jan–May 2025 vs Jan–May 2026 — same months, YoY</b> (Last-Touch report, reproduced to the dollar). Why do visits/conversions fall when spend rises? Spend bought +33% impressions, but the <b>visit rate collapsed −54%</b> (delivery shifted to a worse audience) → visits = impressions × visit-rate = <b>−39%</b>. The quality drop outweighs the volume gain. Saturation would hold OV flat; it halved.</p>
</section>

<section>
<h2>Rule-outs — one line each</h2>
<table class="cmp big">
<tr><th style="text-align:left">Hypothesis</th><th>Number</th><th></th></tr>
<tr><td style="text-align:left">Smaller orders?</td><td>AOV $405 → $397 (flat)</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">Saturation?</td><td>reach 11.5M → 14.1M (<b>+22%</b>)</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">Tracking break?</td><td>retargeting ROAS 55 → 62</td><td class="red">✗</td></tr>
<tr><td style="text-align:left"><b>Root cause →</b></td><td><b>visit rate 0.362% → 0.167% (−54%)</b></td><td class="green">✓</td></tr>
</table>
</section>

<section>
<h2>Backbone — did NOT stay in High-Intent</h2>
<img src="__COMPO__">
<p class="note">HI = 10,000 (vertical AND keyword); PP = 8,000 (vertical only). 2025 = Jun–Oct (first scored months; Jan–May 2025 predates score logging).</p>
</section>

<section>
<h2>Raw counts by tier (impressions)</h2>
<table class="cmp">
<tr><th style="text-align:left">Tier</th><th>2025</th><th>2026</th><th>YoY</th></tr>
<tr><td style="text-align:left"><b>High-Intent (10k)</b></td><td>27.5M &nbsp;95.6%</td><td>17.8M &nbsp;49.6%</td><td class="red" style="font-weight:bold">−35%</td></tr>
<tr><td style="text-align:left">Peak Performance (8k)</td><td>0.2M &nbsp;0.7%</td><td>7.7M &nbsp;21.6%</td><td>+40×</td></tr>
<tr><td style="text-align:left">Mid</td><td>0.7M &nbsp;2.5%</td><td>3.2M &nbsp;9.0%</td><td>+4.4×</td></tr>
<tr><td style="text-align:left">unscored</td><td>0.1M &nbsp;0.5%</td><td>7.0M &nbsp;19.5%</td><td>+49×</td></tr>
<tr><td style="text-align:left"><b>Total</b></td><td><b>28.7M</b></td><td><b>35.9M</b></td><td>+25%</td></tr>
<tr><td style="text-align:left">Avg score (unscored=0)</td><td>9,769</td><td>7,156</td><td>−27%</td></tr>
<tr style="border-top:2px solid var(--navy)"><td style="text-align:left"><b>Addressable HI reached</b> <span style="font-size:0.8em;color:#888">(distinct households)</span></td><td><b>7.0M</b></td><td><b>7.3M</b></td><td class="navy" style="font-weight:bold">+4%</td></tr>
<tr><td style="text-align:left">Total households reached</td><td>7.4M</td><td>14.8M</td><td class="red" style="font-weight:bold">+100%</td></tr>
</table>
<p class="note"><b>The tell:</b> HI households reached is FLAT (~7M, maxed both years) while total reach DOUBLED. The +45% budget found essentially no more High-Intent — every incremental household was PP/Mid/unscored. (2025 = scored ref; Jan–May 2025 predates scoring.)</p>
</section>

<section>
<h2>The gate (HHST) loosened to keep spending</h2>
<img src="__HHST__">
<p class="note">Steady 6,666 in 2025 (HI+PP); 2026 swung to 10,000 then dropped to 3,333–4,500, admitting Mid.</p>
</section>

<section>
<h2>The clincher — lower tiers convert ⅓ as well</h2>
<img src="__TIER__">
<p class="note">Per-household visit rate, 2026. HI 3.84% is the only tier that performs; PP/Mid/unscored ≈1% or less.</p>
</section>

<section>
<h2>Why "OV should've stayed $2M" is wrong</h2>
<table class="cmp big">
<tr><th style="text-align:left"></th><th>2025</th><th>2026</th></tr>
<tr><td style="text-align:left">Order Value</td><td>$2.02M</td><td>$0.99M</td></tr>
<tr><td style="text-align:left">÷ AOV (flat)</td><td>$405</td><td>$397</td></tr>
<tr><td style="text-align:left"><b>= Conversions</b></td><td><b>4,978</b></td><td class="red" style="font-weight:bold">2,495 (−50%)</td></tr>
<tr><td style="text-align:left">HI impressions</td><td>27.5M</td><td class="red">17.8M (−35%)</td></tr>
</table>
<p class="note">HI wasn't <i>added to</i>, it was <b>displaced</b> — every PP impression is one that would've been HI. AOV flat → OV = conversion count → conversions halved. Flat AOV <b>proves</b> it's count, not size.</p>
</section>

<section>
<h2>Spend or supply? — Supply.</h2>
<img src="__SUPPLY__">
<p class="note">Same spend, 2–3× HI swing: Jan '26 $152K→79% HI vs Feb '26 $185K→30% HI. Feb: gate set to HI-only (10,000), still got 30% — wanted HI, couldn't find it.</p>
</section>

<section>
<h2>What changed (TI-33, deployed 7/14/2025)</h2>
<table class="cmp">
<tr><th style="text-align:left">Vertical "Kitchen &amp; Cookware" (120004)</th><th>2025</th><th>2026</th></tr>
<tr><td style="text-align:left">Vertical size (IPs)</td><td>9.5M</td><td class="navy" style="font-weight:bold">15.0M (+57%)</td></tr>
<tr><td style="text-align:left">Original IPs churned out</td><td>—</td><td class="red">14%</td></tr>
</table>
<p class="note">New domain classifier: domain → ChatGPT description → embedding → vertical match, + non-ecommerce filter. The vertical <b>grew</b> (not a shrink) — but with vertical-only IPs that score <b>PP not HI</b>. So "High-Intent" is anchored on different IPs YoY, and the pool grew in the <b>wrong tier</b>. Fangorn NOT involved (HexClad is bucketed).</p>
</section>

<section>
<h2>The numbers — prospecting, last-touch, Jan–May (same months YoY)</h2>
<table class="cmp">
<tr><th style="text-align:left">Metric</th><th>2025</th><th>2026</th><th>YoY</th></tr>
__MET__
</table>
<p class="note">First-touch confirms: all-prospecting ROAS 8.78→3.87 (−56%), down every month. Every lens, every month: real decline.</p>
</section>

<section>
<h2>Conclusion &amp; levers</h2>
<div style="text-align:left;display:inline-block;margin-top:0.2em;">
<ul class="tight">
<li><b>Cause:</b> +45% spend outran the keyword-matched HI supply → delivery slid HI→PP (⅓ the conversion rate) → OV halved. Not saturation, tracking, AOV, or config.</li>
<li><b>Pace to HI capacity</b> — cap spend to what HI can absorb, not a budget target.</li>
<li><b>Grow the HI pool</b> — expand keywords so more in-vertical IPs qualify as HI (10k) not PP (8k).</li>
<li><b>Open (scoring team):</b> within-tier HI quality (clickpass purged for 2025) — the one thing not measurable here.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.4em;">HexClad didn't get worse at the same audience — it ran out of the good one.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML=HTML.replace("__MET__",metrows).replace("__PARADOX__",PARADOX).replace("__COMPO__",COMPO).replace("__HHST__",HHST).replace("__TIER__",TIER).replace("__SUPPLY__",SUPPLY)
(DIR/"audi_1070_hexclad_deck.html").write_text(HTML)
print(f"wrote audi_1070_hexclad_deck.html ({len(HTML)//1024} KB, {HTML.count('<section>')} slides)")
