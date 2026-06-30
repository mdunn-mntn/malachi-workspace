"""AUDI-1070 technical deck builder (RevealJS, claim->evidence). Embeds the 3
PNG charts as base64. Output: audi_1070_presentation_deck.html (CDN, for dev)."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(name):
    return "data:image/png;base64," + base64.b64encode((DIR / name).read_bytes()).decode()
CH_AID = b64("audi_1070_chart_per_aid_yoy.png")
CH_SAT = b64("audi_1070_chart_saturation_gradient.png")
CH_REACH = b64("audi_1070_chart_reach_expansion.png")

HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>AUDI-1070 — YoY Decline Diagnosis</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--blue:#2E5090;--mid:#5A7DB5;--muted:#C8CDD4;--red:#D63B2F;--green:#2E8B57;--text:#222;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:var(--text);}
.reveal h1{margin-top:0;font-size:1.7em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.25em;color:var(--navy);}
.reveal h3{margin-top:0;font-size:0.9em;color:var(--blue);}
.reveal section img{margin:0;border:0;box-shadow:none;background:#FAFAFA;max-height:560px;}
.reveal table{font-size:0.5em;margin:0.3em auto;}
.reveal table th{background:var(--navy);color:#fff;padding:0.35em 0.6em;}
.reveal table td{padding:0.3em 0.6em;border-bottom:1px solid #ddd;}
.claim{font-size:1.15em;color:var(--navy);font-weight:bold;line-height:1.35;}
.sub{color:var(--tl);font-size:0.62em;}
.q{color:var(--navy);font-weight:bold;}
.a{color:var(--text);}
.red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.lead{font-size:0.62em;color:var(--tl);line-height:1.5;}
.kicker{font-size:0.5em;color:var(--mid);letter-spacing:0.15em;text-transform:uppercase;margin-bottom:0.4em;}
ul.tight{font-size:0.66em;line-height:1.55;} ul.tight li{margin-bottom:0.25em;}
.note{font-size:0.46em;color:#999;margin-top:0.6em;}
.big{font-size:2.2em;color:var(--red);font-weight:bold;line-height:1;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<p class="kicker">Audience Intelligence (AUDI) &middot; AUDI-1070</p>
<h1>YoY Performance Decline:<br>Caraway, Avon &amp; HexClad</h1>
<p class="sub" style="margin-top:0.6em;">Is MNTN Matched degrading over time? — A factual diagnosis</p>
<p class="sub" style="margin-top:1.2em;"><b>Malachi Dunn</b> &nbsp;&middot;&nbsp; 2026-06-30</p>
</section>

<section>
<p class="kicker">The claim</p>
<p class="claim">The decline is <span class="red">spend-driven audience saturation</span>,<br>not a degradation of MNTN Matched.</p>
<p class="lead" style="margin-top:1em;">As Caraway and HexClad scaled spend, the bidder reached <b>more, lower-intent</b> users — so visit-rate per impression collapsed. Avon held spend flat and did <b>not</b> decline. Every test below builds this case; the cohort proof closes it.</p>
</section>

<section>
<p class="kicker">What we set out to answer</p>
<h2>Kaila's five investigation areas</h2>
<div style="margin-top:0.8em;text-align:left;display:inline-block;">
<ul class="tight">
<li>1. Why have <b>visits &amp; ROAS declined YoY</b> relative to spend?</li>
<li>2. Is <b>attribution</b> (FT vs LT, attribution/visit windows) contributing?</li>
<li>3. Did <b>audience quality / targeting</b> change? (high-intent, Peak Performance, expansion, intent degradation)</li>
<li>4. Any <b>data-source or targeting-logic</b> changes behind the shift?</li>
<li>5. Is increased spend producing <b>unexpected diminishing returns</b>?</li>
</ul></div>
<p class="note">Each is answered explicitly on the summary slide near the end.</p>
</section>

<section>
<p class="kicker">Evidence 1 &middot; the pattern (answers Q1, Q5)</p>
<h2>The decline scales with spend growth</h2>
<img src="__CH_AID__">
<p class="note">Feb–May YoY (common window; Caraway launched Feb 2025). Flat-spend Avon's ROAS rose; the two scalers fell in proportion to how hard they scaled.</p>
</section>

<section>
<p class="kicker">Evidence 2 &middot; localize it (answers Q1)</p>
<h2>It's the visit rate — not price, value, or mix</h2>
<table>
<tr><th>YoY Δln(ROAS) attributed to</th><th>HexClad</th><th>Caraway</th></tr>
<tr><td>Visit rate (visits / impression)</td><td class="red">81%</td><td class="red">~99%</td></tr>
<tr><td>CPM inflation</td><td>28%</td><td>5%</td></tr>
<tr><td>Conversion rate (per visit)</td><td>+12% (better)</td><td>~flat</td></tr>
<tr><td>AOV (revenue / conversion)</td><td>−6%</td><td>+10%</td></tr>
</table>
<p class="lead" style="margin-top:0.6em;">Residual-free log decomposition, run at <b>campaign grain</b> (Simpson-safe). Conversion rate of the visits that <i>did</i> happen actually <span class="green">improved</span> — the problem is purely that far fewer impressions produced a visit.</p>
</section>

<section>
<p class="kicker">Evidence 3 &middot; the mechanism (answers Q3, Q5)</p>
<h2>Scaling reached more, lower-intent users</h2>
<img src="__CH_REACH__">
<p class="note">Reach via HLL distinct users. Frequency held flat (~2.7–2.9×) — so it's not "same users more often"; it's genuinely <b>more users, each far less likely to visit</b>. Avon contracted reach → quality rose.</p>
</section>

<section>
<p class="kicker">Evidence 4 &middot; at the score level (answers Q3)</p>
<h2>The served audience didn't get "worse" — there's just less of it scored</h2>
<div style="text-align:left;display:inline-block;margin-top:0.4em;">
<ul class="tight">
<li>Scored impressions stay at <span class="navy">~9,900 / 10,000</span> — the per-advertiser MM score is effectively <b>binary</b> (scored vs unscored), not a sliding "quality."</li>
<li>HexClad's <b>scored fraction fell ~97% → 54–76%</b> as spend scaled — delivery spilled into <span class="red">unscored inventory</span>, the expansion at the impression level.</li>
<li>So: not intent-model degradation — the same scoring, applied to a pool stretched past its scored supply.</li>
</ul></div>
<p class="note">advertiser_household_score; RTC excluded. (Baseline note: this column only populates from ~June 2025, so a 2025-vs-2026 score YoY isn't possible — the impression-level expansion evidence above carries it.)</p>
</section>

<section>
<p class="kicker">Evidence 5 &middot; targeting logic (answers Q4)</p>
<h2>The targeting logic did not change — the budget did</h2>
<div style="text-align:left;display:inline-block;margin-top:0.4em;">
<ul class="tight">
<li>2026 campaigns still run <b>MM (DS13/DS19) + Peak Performance</b> — high-intent targeting was <b>not</b> abandoned.</li>
<li><b>No Fangorn-driven break</b>: HexClad uses zero DS46; Caraway added a little Fangorn (designed to <i>raise</i> intent, not lower it).</li>
<li>The expansion came from <b>new mega-prospecting campaigns</b> on the same logic: HexClad <span class="red">28M impressions @ 0.16% VR</span>; Caraway <span class="red">19M @ 0.15%</span> — vs retargeting campaigns that held VR flat.</li>
</ul></div>
</section>

<section>
<p class="kicker">Evidence 6 &middot; attribution (answers Q2)</p>
<h2>Attribution amplifies the client's view — it isn't the cause</h2>
<div style="text-align:left;display:inline-block;margin-top:0.4em;">
<ul class="tight">
<li>MNTN <b>"industry standard" = First Touch</b> (inverts the MMP convention). HexClad's UI is FT → that's the "ROAS &lt; 1x" view.</li>
<li>Our source data is <b>last-touch-consistent across both years</b>, and the decline is <b>still there</b> (ROAS 14.5→8.1×) → real, not an attribution artifact.</li>
<li>A likely <b>Dec-2025 LT→FT migration</b> can distort the client's own YoY; <b>windows</b> (90d/30d, unchanged) can't manufacture a drop — a longer window only <i>adds</i> visits.</li>
</ul></div>
<p class="note">Recommendation: any client-facing YoY must hold attribution constant (FT-2026 vs FT-2025).</p>
</section>

<section>
<p class="kicker">Evidence 7 &middot; the within-advertiser tell (answers Q4, Q5)</p>
<h2>Visit rate moves inversely with spend — over time, per advertiser</h2>
<div style="text-align:left;display:inline-block;margin-top:0.4em;">
<ul class="tight">
<li><b>HexClad's VR recovered 0.73% → 1.80%</b> when it <i>cut</i> spend in early-2025 — then fell again when it re-scaled.</li>
<li>Caraway's VR fell monotonically as reach grew 0.4M → 5.0M.</li>
<li>The decline tracks <b>each advertiser's own spend ramp</b> — not any platform date (Peak Performance launch, Max-Reach-off, Fangorn). A platform fault would be synchronized; this isn't.</li>
</ul></div>
</section>

<section>
<p class="kicker">Evidence 8 &middot; the proof (answers Q3, Q5)</p>
<h2>Across 294 advertisers: cut spend → VR rises; grow spend → VR falls</h2>
<img src="__CH_SAT__">
<p class="note">If Matched were degrading <i>systemically</i>, flat-spend advertisers' VR would fall too. It <b>rose ×1.26</b>. Only spend-growers declined. The hypothesis is falsified at the population level.</p>
</section>

<section>
<p class="kicker">Answering all five — definitively</p>
<table>
<tr><th>Question</th><th>Answer</th></tr>
<tr><td>1. Why visits/ROAS down vs spend?</td><td>Spend scaled into audience <b>expansion</b>; visit-rate collapse = 81–99% of the ROAS drop.</td></tr>
<tr><td>2. Is attribution contributing?</td><td><b>Not the cause.</b> Decline is real under consistent last-touch; FT lens + Dec-2025 switch distort the <i>client's</i> view; windows can't cause it.</td></tr>
<tr><td>3. Audience quality / PP / intent?</td><td><b>No degradation.</b> Same MM/Peak-Performance; scored users still max; the audience was <b>expanded</b> into lower-intent/unscored supply.</td></tr>
<tr><td>4. Data-source / targeting-logic change?</td><td><b>None that explains it.</b> No Fangorn break; new big prospecting campaigns scaled the same logic.</td></tr>
<tr><td>5. Diminishing returns from spend?</td><td><b>Yes — the central mechanism</b>, confirmed cohort-wide (n=294).</td></tr>
</table>
</section>

<section>
<p class="kicker">Per advertiser</p>
<table>
<tr><th>AID</th><th>Spend</th><th>VR</th><th>ROAS</th><th>Read</th></tr>
<tr><td><b>Caraway</b> 40341</td><td>+119%</td><td class="red">−66%</td><td class="red">−66%</td><td>Most severe — ×2.2 into one 19M-imp prospecting campaign @0.15% VR.</td></tr>
<tr><td><b>HexClad</b> 34611</td><td>+38%</td><td>−38%</td><td>−44%</td><td>Real but milder; FT lens makes the client UI look catastrophic vs ~8× LT reality.</td></tr>
<tr><td><b>Avon</b> 31921</td><td>−14%</td><td class="green">flat</td><td class="green">+16%</td><td>Healthy control — flat spend, no decline.</td></tr>
</table>
<p class="note">YoY Feb–May 2026 vs 2025.</p>
</section>

<section>
<p class="kicker">What to do</p>
<h2>Recommendations</h2>
<div style="text-align:left;display:inline-block;margin-top:0.4em;">
<ul class="tight">
<li><b>Treat as prospecting pacing / saturation — not "fix Matched."</b> Right-size prospecting budgets to the addressable high-intent pool; marginal scaled impressions are near-zero-VR.</li>
<li><b>Hold attribution constant</b> in any client YoY (FT-vs-FT); confirm the Dec-2025 reporting migration date with Prod Ops.</li>
<li><b>Separate the market-wide ROAS softness</b> (it hit flat-spend advertisers too) from advertiser action — it isn't Matched.</li>
<li><i>Optional:</i> a holdout/incrementality read — a falling <i>attributed</i> visit-rate can overstate true value loss.</li>
</ul></div>
</section>

<section>
<p class="claim">Spend scaled the audience past its high-intent supply.<br><span class="red">Matched isn't broken — the prospecting pool is saturated.</span></p>
<p class="sub" style="margin-top:1.2em;">Flat-spend advertisers' visit-rate rose. Only the scalers declined. (n=294)</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>
Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,
transition:'fade',transitionSpeed:'slow',width:1100,height:800,margin:0.01,minScale:0.2,maxScale:1.5});
</script></body></html>
"""

HTML = HTML.replace("__CH_AID__", CH_AID).replace("__CH_SAT__", CH_SAT).replace("__CH_REACH__", CH_REACH)
out = DIR / "audi_1070_presentation_deck.html"
out.write_text(HTML)
print(f"wrote {out} ({len(HTML)//1024} KB)")
