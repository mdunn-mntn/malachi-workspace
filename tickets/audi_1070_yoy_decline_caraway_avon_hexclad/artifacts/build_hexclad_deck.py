"""AUDI-1070 — HexClad FULL WALK-THROUGH deck. Built ground-up like the Avon deck:
campaigns -> audience -> the paradox -> what it isn't -> the backbone (did NOT stay in HI)
-> the gate (HHST) -> the clincher (visit rate by tier) -> WHY (supply not spend) ->
the numbers -> levers. Last-touch (Mike's report) as the hook; FT confirms. 5 charts."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
PARADOX=b64("audi_1070_hexclad_paradox.png"); COMPO=b64("audi_1070_hexclad_composition.png")
HHST=b64("audi_1070_hexclad_hhst.png"); TIER=b64("audi_1070_hexclad_visit_rate_by_tier.png")
SUPPLY=b64("audi_1070_hexclad_supply.png")

MET=[  # last-touch, Mike's report (2025 CTV Prospecting -> 2026 High-Intent)
 ("Spend","$642,267","$931,422","+45%","in"),("Impressions","30.7M","40.8M","+33%","in"),
 ("Households reached","11.5M","14.1M","+22%","in"),("Verified Visits","111,053","68,214","−39%","bad"),
 ("Visit rate","0.362%","0.167%","−54%","bad"),("Conversions","4,978","2,495","−50%","bad"),
 ("AOV","$405.38","$397.38","−2%","flat"),("Order Value","$2.02M","$0.99M","−51%","bad"),
 ("ROAS","3.14×","1.06×","−66%","bad")]
def kc(k): return {"in":"#27496D","bad":"#D63B2F","flat":"#888"}[k]
metrows="\n".join(f'<tr><td style="text-align:left">{m}</td><td>{a}</td><td>{b}</td>'
    f'<td style="color:{kc(k)};font-weight:bold">{y}</td></tr>' for (m,a,b,y,k) in MET)

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — HexClad Walk-through</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--red:#D63B2F;--green:#2E8B57;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:#222;}
.reveal h1{margin-top:0;font-size:1.42em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.0em;color:var(--navy);margin-bottom:0.22em;}
.reveal section img{margin:0.1em 0 0;border:0;box-shadow:none;background:#FAFAFA;max-height:552px;}
.cmp{font-size:0.54em;margin:0.2em auto;border-collapse:collapse;}
.cmp th{background:var(--navy);color:#fff;padding:0.32em 0.85em;}
.cmp td{padding:0.24em 0.85em;border-bottom:1px solid #e3e3e3;text-align:right;}
.claim{font-size:1.0em;color:var(--navy);font-weight:bold;line-height:1.35;}
.sub{color:var(--tl);font-size:0.6em;} .red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.lead{font-size:0.57em;color:var(--tl);line-height:1.5;margin-top:0.5em;}
.note{font-size:0.44em;color:#999;margin-top:0.35em;}
ul.tight{font-size:0.58em;line-height:1.45;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.3em;}
ol.road{font-size:0.6em;line-height:1.55;text-align:left;display:inline-block;} ol.road li{margin-bottom:0.35em;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<h1>HexClad — Why Prospecting ROAS Fell 3×</h1>
<p class="claim" style="font-size:0.82em;margin-top:0.5em;">HexClad didn't get worse at High-Intent — it ran out of it.</p>
<p class="sub" style="margin-top:0.9em;">A ground-up walk-through: campaigns → audience → what we served → why. Jan–May 2025 vs 2026, prospecting. | AUDI-1070</p>
<p class="sub" style="margin-top:0.7em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; 2026-06-30</p>
</section>

<section>
<h2>Bottom line</h2>
<p class="claim">Spend rose <span class="navy">+45%</span>, but order value <span class="red">halved</span> and ROAS fell <span class="red">3.14× → 1.06×</span>.</p>
<p class="lead">The cause is <b>not</b> saturation, tracking, or smaller orders. As HexClad scaled, its delivery mix fell out of true <b>High-Intent (95% → 49%)</b> into <b>Peak Performance</b>, a tier that converts a third as well. The audience didn't get worse per household — <b>we served far less of the good one.</b> (Opposite of Avon, which is healthy.)</p>
</section>

<section>
<h2>How we built this</h2>
<div style="text-align:left;display:inline-block;margin-top:0.35em;">
<ol class="road">
<li><b>The campaigns</b> — what HexClad runs; isolate prospecting.</li>
<li><b>The audience</b> — confirm it's a proper, clean MM prospecting build.</li>
<li><b>The symptom</b> — spend up, order value down; rule out the easy explanations.</li>
<li><b>What we actually served</b> — the intent-tier mix, and the gate (HHST) that governs it.</li>
<li><b>Why</b> — is it spend, or High-Intent supply? Then the levers.</li>
</ol></div>
</section>

<section>
<h2>1 · The campaigns (last 2 years)</h2>
<table class="cmp">
<tr><th style="text-align:left">Group</th><th>Objective</th><th>Channel</th><th>In scope</th></tr>
<tr><td style="text-align:left"><b>Prospecting — Stage 1</b> (Beeswax TV Prospecting)</td><td>obj 1</td><td>CTV</td><td><span class="green">✓ analyzed</span></td></tr>
<tr><td style="text-align:left">Prospecting — Multi-Touch (S2/S3)</td><td>obj 1</td><td>CTV/display</td><td><span class="green">✓ (all-stages)</span></td></tr>
<tr><td style="text-align:left">Retargeting ("TV Retargeting")</td><td>obj 4</td><td>CTV/display</td><td><span class="red">✗ excluded — healthy (ROAS 55→62)</span></td></tr>
</table>
<p class="note">Prospecting = <b>objective_id = 1</b>; retargeting = obj 4 (separate, and healthy — ruling out a tracking/site-wide problem). Per the ask we analyze prospecting only. The 2026 group is renamed "CTV Prospecting High-Intent."</p>
</section>

<section>
<h2>2 · The audience — a proper, clean MM prospecting build</h2>
<table class="cmp">
<tr><th style="text-align:left">Layer</th><th style="text-align:left">Configuration</th><th>Verdict</th></tr>
<tr><td style="text-align:left">Core audience</td><td style="text-align:left">MNTN Matched keywords (DS19) — count <b>grew 78→89</b> YoY</td><td><span class="green">✓ MM, not cut</span></td></tr>
<tr><td style="text-align:left">Exclusions</td><td style="text-align:left">NOT CRM (DS4) · NOT 30-day site-visitors (DS34) · NOT 30-day converters (DS21)</td><td><span class="green">✓ clean</span></td></tr>
<tr><td style="text-align:left">Geo / controls</td><td style="text-align:left">US-only (237) · 10% holdout · RTC scoring · <b>HHST intent gate</b></td><td><span class="green">✓</span></td></tr>
<tr><td style="text-align:left">Overlays</td><td style="text-align:left">No 3P/LiveRamp intersection · no geo-narrowing</td><td><span class="green">✓ none</span></td></tr>
</table>
<p class="note">The config is clean — this is NOT a bad-overlay or a keyword-cut story. The change vs 2025 is the intent <b>scoring/gate</b> layer (DS13→DS46 reference, RTC on), which is where the story lives.</p>
</section>

<section>
<h2>3 · The symptom: spend +45%, but order value −51%</h2>
<img src="__PARADOX__">
<p class="note">Reproduces Mike's Last-Touch report exactly. Order value halved while spend rose — the paradox that saturation can't explain (saturation would hold OV ~flat).</p>
</section>

<section>
<h2>Ruling out the easy answers</h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b>Not smaller orders</b> — AOV flat ($405→$397). OV halved because <b>conversions halved</b>.</li>
<li><b>Not saturation</b> — reach GREW +22% (more households, not fewer hit harder).</li>
<li><b>Not tracking</b> — retargeting healthy ($8.4M→$7.8M, ROAS 55→62). The pixel works.</li>
<li><b>The root is a visit-rate collapse (−54%)</b> — +33% impressions produced −39% FEWER visits.</li>
</ul></div>
<p class="note">So the audience served became ~2× less responsive per impression. The rest of the deck is: which audience, and why.</p>
</section>

<section>
<h2>4 · The backbone: HexClad did NOT stay in High-Intent</h2>
<img src="__COMPO__">
<p class="note">Intent tiers (MNTN scoring): <b>High-Intent = 10,000</b> (in the vertical AND the keywords); <b>Peak Performance = 8,000</b> (vertical only). The served mix fell from ~95% HI to <b>49%</b>, with 34% diverted to Peak Performance. The core assumption — "we're still serving High-Intent" — is false.</p>
</section>

<section>
<h2>The raw numbers — High-Intent lost <span class="red">volume</span>, not just share</h2>
<table class="cmp">
<tr><th style="text-align:left">Tier (score)</th><th>2025 (Jun–Oct)</th><th>2026 (Jan–May)</th><th>YoY imps</th></tr>
<tr><td style="text-align:left"><b>High-Intent (10,000)</b></td><td>27.5M &nbsp;<b>95.6%</b></td><td>17.8M &nbsp;<b>49.6%</b></td><td style="color:#D63B2F;font-weight:bold">−35%</td></tr>
<tr><td style="text-align:left">Peak Performance (8,000)</td><td>0.2M &nbsp;0.7%</td><td>7.7M &nbsp;21.6%</td><td>+40×</td></tr>
<tr><td style="text-align:left">Mid (3,333–6,665)</td><td>0.7M &nbsp;2.5%</td><td>3.2M &nbsp;9.0%</td><td>+4.4×</td></tr>
<tr><td style="text-align:left">unscored</td><td>0.1M &nbsp;0.5%</td><td>7.0M &nbsp;19.5%</td><td>+49×</td></tr>
<tr><td style="text-align:left"><b>Total impressions</b></td><td><b>28.7M</b></td><td><b>35.9M</b></td><td>+25%</td></tr>
<tr><td style="text-align:left">Intent gate (HHST)</td><td>6,666</td><td>as low as 3,333</td><td></td></tr>
<tr><td style="text-align:left">Avg score (unscored = 0)</td><td>9,769</td><td>7,156</td><td>−27%</td></tr>
</table>
<p class="note">Total volume rose +25%, but <b>High-Intent impressions FELL −35%</b> (27.5M→17.8M). The budget growth <i>and</i> the ~10M vanished HI impressions all flowed into PP / Mid / unscored. (2025 = first scored months; Jan–May 2025 predates score logging.)</p>
</section>

<section>
<h2>The gate (HHST) loosened to keep spending</h2>
<img src="__HHST__">
<p class="note">HHST = the minimum intent score the bidder will serve. Steady at 6,666 in 2025 (HI+PP); in 2026 it swung to 10,000 then <b>collapsed to 3,333–4,500</b>, admitting Mid-intent — the bidder lowering the bar to fill the bigger budget.</p>
</section>

<section>
<h2>5 · The clincher: the tiers below HI convert ⅓ as well</h2>
<img src="__TIER__">
<p class="note">Per-household visit rate by delivered tier (2026): <b>HI 3.84%</b>, <b>PP 1.19%</b> (31% of HI), Mid 1.13%, unscored 0.58%. High-Intent is the only tier that performs. Moving a third of delivery out of HI is exactly why visits, conversions, and order value halved.</p>
</section>

<section>
<h2>Why did HI share fall — spend, or supply?</h2>
<img src="__SUPPLY__">
<p class="note"><b>It's supply, not budget.</b> At near-identical spend, HI swings 2–3×: Jan '26 ($152K → 79% HI) vs Feb '26 ($185K → 30% HI). In Feb the bidder set HHST=10,000 (HI-only) but still got only 30% HI — it <i>wanted</i> High-Intent and couldn't find it. A bigger budget on a fixed pool can't do that.</p>
</section>

<section>
<h2>The definitive answer — four assumptions, all <span class="red">false</span></h2>
<div style="text-align:left;display:inline-block;margin-top:0.2em;">
<ul class="tight">
<li><span class="red">✗ "It's the same campaign YoY"</span> — 2025's campaign (225087) ended Sep 2025; a new "CTV Prospecting High-Intent" campaign (446801) replaced it.</li>
<li><span class="red">✗ "We only target High-Intent"</span> — 2026 delivery is 49.6% HI, 21.6% PP, 9% Mid, 19.5% unscored; the gate fell to 3,333.</li>
<li><span class="red">✗ "We target the same % of HI"</span> — 95.6% (2025) → 49.6% (2026).</li>
<li><span class="red">✗ "The audience was misconfigured / broken"</span> — it's a proper MM campaign both years; keywords GREW (78→89), CRM/visitor/converter exclusions & US-geo are consistent, no bad overlay. What changed is which tiers got <b>served</b>, not the setup.</li>
</ul></div>
<p class="claim" style="font-size:0.76em;margin-top:0.4em;">The AOV is flat YoY ($405→$397). HexClad performs worse simply because <span class="red">we reached more people who are less likely to visit or convert</span> — HI 3.84% vs PP 1.19%. Same orders when they buy; fewer of them buy.</p>
</section>

<section>
<h2>The numbers — prospecting, last-touch, Jan–May</h2>
<table class="cmp">
<tr><th style="text-align:left">Metric</th><th>2025</th><th>2026</th><th>YoY</th></tr>
__MET__
</table>
<p class="note">First-touch (industry_standard) confirms: all-prospecting ROAS 8.78→3.87 (−56%), conv rate −28%, CPA +130%, AOV flat — and <b>down every single month</b>. Every lens, every month: a real decline.</p>
</section>

<section>
<h2>Conclusion & levers</h2>
<div style="text-align:left;display:inline-block;margin-top:0.25em;">
<ul class="tight">
<li><b>Root cause:</b> HexClad scaled spend +45% past its High-Intent supply; a third of delivery fell into Peak Performance (⅓ the conversion rate) → order value & ROAS halved. Not saturation, tracking, AOV, or config.</li>
<li><b>Lever 1 — pace to HI capacity:</b> cap prospecting spend to what the High-Intent supply can absorb, not to a budget target.</li>
<li><b>Lever 2 — grow the HI pool:</b> keep expanding keywords (already 78→89) so more in-vertical IPs qualify as HI (10k) not PP (8k).</li>
<li><b>Lever 3 — evaluate Fangorn:</b> HexClad is on old bucketed PP; Fangorn-scored PP reportedly performs better.</li>
<li><b>Why did HI supply fall? Ruled out:</b> vertical reassignment (vertical 120004 stable since 2024), keyword cut (grew 78→89), pure spend (HI swings 2–3× at constant spend). <b>Leading candidate:</b> the platform vertical-classification change (~July 2025, "Jaguar"/DS13) that re-drew which IPs are in each vertical — right timeline, but the vertical-size history isn't retained in BQ. <b>Measurement/scoring must confirm</b> by measuring vertical 120004's HI pool pre-July-2025 vs now (vs Q1-Q4 seasonality).</li>
</ul></div>
<p class="claim" style="font-size:0.7em;margin-top:0.5em;">HexClad didn't get worse at the same audience — it ran out of the good one.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML=(HTML.replace("__MET__",metrows).replace("__PARADOX__",PARADOX).replace("__COMPO__",COMPO)
      .replace("__HHST__",HHST).replace("__TIER__",TIER).replace("__SUPPLY__",SUPPLY))
(DIR/"audi_1070_hexclad_deck.html").write_text(HTML)
print(f"wrote audi_1070_hexclad_deck.html ({len(HTML)//1024} KB, 15 slides)")
