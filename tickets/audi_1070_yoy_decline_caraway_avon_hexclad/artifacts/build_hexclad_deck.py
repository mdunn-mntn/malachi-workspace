"""AUDI-1070 — HexClad deck v3: numbers/tables-heavy, assumptions upfront, tight build.
campaigns->audience->symptom->rule-outs->backbone->gate->clincher->rebuttal->why->what-changed->numbers.
Last-touch (Mike's report) for perf; tier composition uses H2-2025 (Jan-May 2025 predates scoring). 5 charts."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
PARADOX=b64("audi_1070_hexclad_paradox.png"); COMPO=b64("audi_1070_hexclad_composition.png")
HHST=b64("audi_1070_hexclad_hhst.png"); TIER=b64("audi_1070_hexclad_visit_rate_by_tier.png")
SUPPLY=b64("audi_1070_hexclad_supply.png")
GATE=b64("audi_1070_hexclad_gate_eventstudy.png"); FANGORN=b64("audi_1070_hexclad_fangorn.png")
TRANSITION=b64("audi_1070_hexclad_transition_map.png")
PACING=b64("audi_1070_hexclad_pacing.png")

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
<p class="note">HI substrate (vertical DS13 ∩ keyword DS19) intact through May 2026 — not a keyword/HI cut. Source add/removes did happen (Feb 18: +CRM DS4; Mar 4: −DS1/−DS35 LiveRamp; Jun 3: +DS46 Fangorn), but the HI-defining layers stayed. The decline is in the scoring/gate layer downstream, not the audience.</p>
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
<tr><td style="text-align:left">HI frequency <span style="font-size:0.8em;color:#888">(impr ÷ households)</span></td><td>3.9×</td><td>2.4×</td><td class="red">−38%</td></tr>
<tr><td style="text-align:left">Total households reached</td><td>7.4M</td><td>14.8M</td><td class="red" style="font-weight:bold">+100%</td></tr>
</table>
<p class="note"><b>The tell:</b> HI households reached is FLAT (~7M, maxed both years) while total reach DOUBLED. Not retargeting — Stage-1 prospecting serves each household ~2–4× (frequency). And HexClad served HI <i>less</i> in 2026 (freq 3.9→2.4), redirecting budget to PP/Mid. (2025 = scored ref; Jan–May 2025 predates scoring.)</p>
</section>

<section>
<h2>The steep drop-offs = the intent gate flipped, overnight</h2>
<img src="__GATE__">
<p class="note"><b>"No way reach caused this level of decline" — correct. Reach didn't. The HHST intent gate did.</b> The prospecting gate was changed <b>51 times</b> Jan–May 2026 (0 ↔ 10,000; steady 6,666 all of 2025). Each flip inverts delivery the <b>next day</b>: gate→10,000 (Jan 5, Feb 26) = 100% HI overnight; gate→0 (Feb 5, Mar 6) = 12% HI / 57% unscored overnight. The decline is a config lever thrashed to chase spend — not gradual, not model degradation. When the gate is tight, MM still delivers 100% HI and performs.</p>
</section>

<section>
<h2>The case for changes, month by month (Jun→Dec 2025)</h2>
<img src="__TRANSITION__">
<p class="note">Clean HI-only regime (gate ~6,666, 95–100% HI) June–October — HI reach <b>PEAKED</b> in Oct (3.86M distinct households, 2.08M net-new). <b>Nov 11: the HHST gate was REMOVED</b> (→0/−1) to chase 20× Black-Friday volume → 100% HI to 13% HI overnight. December never re-gated (−1 all month, ~11% HI). Not gradual, not HI exhaustion (HI was still abundant) — one deliberate config flip. Same pattern on Caraway (gate removed Nov 28).</p>
</section>

<section>
<h2>"But a 10,000 gate showed a mix" — the gate binds; the mix is 3 other things</h2>
<table class="cmp big">
<tr><th style="text-align:left">Jan 2026, gate = 10,000 window</th><th>% of imps</th><th>HI (10k)</th><th>non-HI</th></tr>
<tr><td style="text-align:left"><b>Normal prospecting (gated)</b></td><td>92.0%</td><td class="green" style="font-weight:bold">99.99%</td><td>0.01%</td></tr>
<tr><td style="text-align:left">RTC — Real-Time Conquest <span style="font-size:0.8em;color:#888">(bypasses gate by design)</span></td><td>8.0%</td><td>34.8%</td><td class="red" style="font-weight:bold">65.2%</td></tr>
</table>
<p class="note"><b>The gate binds essentially perfectly</b> — on the gated path, <b>99.99%</b> of impressions are exactly 10,000 (the 0.01% is ~1-day propagation lag). Verified independently Oct 27–Nov 7 2025: sustained 10k gate = <b>0–1 non-HI imp/day out of ~150K</b>. The apparent "mix" is: <b>(1)</b> RTC conquest (~8%) fires <i>before</i> the intent gate and serves competitors' households regardless of score — a mix by design, not a leak; <b>(2)</b> monthly aggregation blending 5 no-gate days (Jan 1–5, gate −1, ~12% HI) with 26 gated days (Jan 6–31, ~100% HI); <b>(3)</b> flip-day lag. Use household_score for gate reasoning (advertiser_household_score misclassifies ~10%). RTC was OFF in 2025, ON in 2026 — itself a change.</p>
</section>

<section>
<h2>Pacing: the ceiling is the ~3.8M live pool, not the 7M lifetime figure</h2>
<img src="__PACING__">
<p class="note"><b>You pace against the LIVE 30-day pool, not the 7M cumulative.</b> The instantaneously reachable HI pool tops at <b>~3.8M IPs</b> (~half of 7M; fewer in households after IP churn), set by new-HI inflow (~61K/day) × the 30-day TTL. Sustainable HI spend ≈ <b>$150–160K/mo (~$5K/day)</b>. <b>October is where it bit:</b> spend hit $224K (~40% over sustainable) → brand-new share of reach fell 100%→54%, reach/$ rolled over, cumulative crossed 7M (Oct 26) — the pool began re-serving itself. But it's a <b>refreshable flow limit, not a wall</b>: 2026 reach/$ recovered ABOVE the 2025 baseline at +23% higher spend. Net: supply was the <i>emerging</i> constraint (Oct); the Nov-11 gate removal was the <i>actual</i> cause. <b>Fix:</b> pace HI spend near ~$5K/day sustained, or spread HI IPs across the flight so a spike doesn't drain the pool.</p>
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
<h2>The mechanism is universal — Avon confirms it</h2>
<table class="cmp big">
<tr><th style="text-align:left"></th><th>HexClad</th><th>Avon</th></tr>
<tr><td style="text-align:left">Typical monthly spend</td><td>~$150K</td><td>~$9K</td></tr>
<tr><td style="text-align:left">HI+PP share, base months</td><td class="red">swings 30–79%</td><td class="green">stable 34–45%</td></tr>
<tr><td style="text-align:left">When spend spikes</td><td>chronic</td><td class="red">Nov $36K → 18.6% HI+PP</td></tr>
<tr><td style="text-align:left">Post-holiday Dec</td><td>11% HI</td><td class="red">3.3% HI+PP</td></tr>
</table>
<p class="note"><b>Same physics.</b> Low spend fits inside the finite HI pool → Avon stays in HI → healthy (this is <i>why</i> Avon is fine). But the instant Avon spends big (Nov $36K), its HI-share collapses to 18.6% — identical to HexClad. Not "MM degraded": it's spend-vs-finite-HI-supply, gated by HHST. Avon didn't stay flat by luck — it's the confirming experiment from the low-spend end.</p>
</section>

<section>
<h2>What changed (TI-33, deployed 7/14/2025)</h2>
<table class="cmp">
<tr><th style="text-align:left">Vertical "Kitchen &amp; Cookware" (120004)</th><th>2025</th><th>2026</th></tr>
<tr><td style="text-align:left">Vertical size (IPs)</td><td>9.5M</td><td class="navy" style="font-weight:bold">15.0M (+57%)</td></tr>
<tr><td style="text-align:left">Original IPs churned out</td><td>—</td><td class="red">14%</td></tr>
</table>
<p class="note">New domain classifier: domain → ChatGPT description → embedding → vertical match, + non-ecommerce filter. The vertical <b>grew</b> (not a shrink) — but with vertical-only IPs that score <b>PP not HI</b>. So "High-Intent" is anchored on different IPs YoY, and the pool grew in the <b>wrong tier</b>. (And it's not Fangorn — next slide.)</p>
</section>

<section>
<h2>It's not Fangorn — proven, not assumed</h2>
<img src="__FANGORN__">
<p class="note">Fangorn writes <b>continuous</b> scores (High 8001–9999); bucketed writes <b>discrete</b> (HI = exactly 10,000). HexClad ran <b>0.0% continuous scores every month Jun 2025–May 2026</b> = 100% bucketed. It migrated to Fangorn <b>Jun 4–5, 2026</b> — after the entire decline window, so Fangorn cannot explain Jan–May. It IS live now (38% continuous in June) → June-forward is a new regime, flagged to the scoring team.</p>
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
<li><b>Cause:</b> +45% spend outran the keyword-matched HI supply → delivery slid HI→PP (⅓ the conversion rate) → OV halved. Not saturation, tracking, AOV, config, or Fangorn.</li>
<li><b>Why it's rapid, not gradual:</b> the HHST intent gate is thrashed 51× (0 ↔ 10,000) to chase spend — each flip inverts delivery overnight. The steep drop-offs ARE the gate.</li>
<li><b>Pace to HI capacity</b> — cap spend to what HI can absorb, not a budget target; stop flipping the gate to force fill.</li>
<li><b>Grow the HI pool</b> — expand keywords so more in-vertical IPs qualify as HI (10k) not PP (8k).</li>
<li><b>Open (scoring team):</b> within-tier HI quality (clickpass purged for 2025) — the one thing not measurable here.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.4em;">HexClad didn't get worse at the same audience — it ran out of the good one.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML=HTML.replace("__MET__",metrows).replace("__PARADOX__",PARADOX).replace("__COMPO__",COMPO).replace("__HHST__",HHST).replace("__TIER__",TIER).replace("__SUPPLY__",SUPPLY).replace("__GATE__",GATE).replace("__FANGORN__",FANGORN).replace("__TRANSITION__",TRANSITION).replace("__PACING__",PACING)
(DIR/"audi_1070_hexclad_deck.html").write_text(HTML)
print(f"wrote audi_1070_hexclad_deck.html ({len(HTML)//1024} KB, {HTML.count('<section>')} slides)")
