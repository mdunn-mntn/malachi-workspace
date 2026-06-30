"""AUDI-1070 technical deck builder (RevealJS, claim->evidence, tight/internal).
Embeds the 3 PNG charts as base64. Output: audi_1070_presentation_deck.html."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(name):
    return "data:image/png;base64," + base64.b64encode((DIR / name).read_bytes()).decode()
CH_AID, CH_SAT, CH_REACH = b64("audi_1070_chart_per_aid_yoy.png"), b64("audi_1070_chart_saturation_gradient.png"), b64("audi_1070_chart_reach_expansion.png")

HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>AUDI-1070 — YoY Decline Diagnosis</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--blue:#2E5090;--mid:#5A7DB5;--red:#D63B2F;--green:#2E8B57;--text:#222;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:var(--text);}
.reveal h1{margin-top:0;font-size:1.6em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.15em;color:var(--navy);margin-bottom:0.3em;}
.reveal section img{margin:0.1em 0 0;border:0;box-shadow:none;background:#FAFAFA;max-height:560px;}
.reveal table{font-size:0.5em;margin:0.2em auto;}
.reveal table th{background:var(--navy);color:#fff;padding:0.35em 0.6em;text-align:left;}
.reveal table td{padding:0.32em 0.6em;border-bottom:1px solid #ddd;vertical-align:top;}
.claim{font-size:1.05em;color:var(--navy);font-weight:bold;line-height:1.35;}
.sub{color:var(--tl);font-size:0.6em;}
.red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.lead{font-size:0.6em;color:var(--tl);line-height:1.5;margin-top:0.7em;}
ul.tight{font-size:0.62em;line-height:1.5;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.5em;}
.note{font-size:0.45em;color:#999;margin-top:0.5em;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<h1>YoY Performance Decline<br>Caraway &middot; Avon &middot; HexClad</h1>
<p class="sub" style="margin-top:0.6em;">Diagnosis: is the decline driven by MNTN Matched? &nbsp;|&nbsp; AUDI-1070</p>
<p class="sub" style="margin-top:1.1em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; 2026-06-30</p>
</section>

<section>
<h2>Bottom line</h2>
<p class="claim">The decline is <span class="red">spend-driven audience saturation</span> —<br>not a degradation of MNTN Matched.</p>
<p class="lead">As Caraway &amp; HexClad scaled spend, delivery expanded into <b>more, lower-intent</b> users, so visit-rate per impression collapsed. Avon held spend flat and did not decline. All five investigation areas resolve to the same cause; a 294-advertiser cohort confirms it.</p>
</section>

<section>
<h2>Decline scales with spend growth</h2>
<img src="__CH_AID__">
<p class="note">Feb–May YoY (common window). Flat-spend Avon's ROAS rose; the two scalers fell in proportion to how hard they scaled.</p>
</section>

<section>
<h2>The ROAS drop is a visit-rate drop</h2>
<table>
<tr><th>YoY Δln(ROAS) attributed to</th><th>HexClad</th><th>Caraway</th></tr>
<tr><td>Visit rate (visits / impression)</td><td class="red">81%</td><td class="red">~99%</td></tr>
<tr><td>CPM inflation</td><td>28%</td><td>5%</td></tr>
<tr><td>Conversion rate (per visit)</td><td>+12% (better)</td><td>~flat</td></tr>
<tr><td>AOV (revenue / conversion)</td><td>−6%</td><td>+10%</td></tr>
</table>
<p class="lead">Residual-free log decomposition at <b>campaign grain</b> (Simpson-safe). The conversion rate of the visits that occurred actually <span class="green">improved</span> — the loss is purely fewer visits per impression.</p>
</section>

<section>
<h2>Scaling reached more, lower-intent users</h2>
<img src="__CH_REACH__">
<p class="note">Frequency held flat (~2.7–2.9×) → more users, not more exposure. At the score level, HexClad's scored-impression share fell ~97% → 54–76% as delivery spilled into unscored inventory.</p>
</section>

<section>
<h2>Ruled out: attribution &amp; targeting-logic changes</h2>
<ul class="tight">
<li><b>Attribution.</b> "Industry standard" = <b>First Touch</b> (the client's UI view). Our last-touch-consistent source still shows the decline (ROAS 14.5→8.1×) → real, not an attribution artifact. Windows (90d/30d, unchanged) can't manufacture a drop; a likely Dec-2025 LT→FT migration distorts the client's own YoY.</li>
<li><b>Targeting logic.</b> 2026 campaigns still run MM + Peak Performance. No Fangorn-driven break (HexClad: none; Caraway: one small DS46 campaign, which <i>raises</i> intent). The expansion came from <b>new mega-prospecting campaigns on the same logic</b> — 28M &amp; 19M impressions at ~0.15% VR.</li>
</ul>
</section>

<section>
<h2>294 advertisers: the same saturation law</h2>
<img src="__CH_SAT__">
<p class="note">A systemic Matched fault would drag flat-spend advertisers too — their VR instead <b>rose ×1.26</b>. Only spend-growers declined. Hypothesis falsified at the population level.</p>
</section>

<section>
<h2>The five investigation areas — answered</h2>
<table>
<tr><th>Area</th><th>Answer</th></tr>
<tr><td>Why visits/ROAS down vs spend</td><td>Spend scaled into audience <b>expansion</b>; visit-rate collapse = 81–99% of the ROAS drop.</td></tr>
<tr><td>Attribution (FT/LT, windows)</td><td><b>Not the cause.</b> Decline is real under consistent last-touch; FT lens + Dec-2025 switch distort the client view; windows can't cause it.</td></tr>
<tr><td>Audience quality / PP / intent</td><td><b>No degradation.</b> Same MM/Peak-Performance; scored users still max; the audience was <b>expanded</b> into lower-intent supply.</td></tr>
<tr><td>Data-source / targeting-logic change</td><td><b>None that explains it.</b> No Fangorn break; new big prospecting campaigns scaled the same logic.</td></tr>
<tr><td>Diminishing returns from spend</td><td><b>Yes — the central mechanism</b>, confirmed cohort-wide (n=294).</td></tr>
</table>
</section>

<section>
<h2>Per advertiser</h2>
<table>
<tr><th>Advertiser</th><th>Spend</th><th>VR</th><th>ROAS</th><th>Read</th></tr>
<tr><td><b>Caraway</b></td><td>+119%</td><td class="red">−66%</td><td class="red">−66%</td><td>Most severe — ×2.2 into one 19M-imp prospecting campaign @0.15% VR.</td></tr>
<tr><td><b>HexClad</b></td><td>+38%</td><td>−38%</td><td>−44%</td><td>Real but milder; FT lens makes the client UI look catastrophic vs ~8× LT reality.</td></tr>
<tr><td><b>Avon</b></td><td>−14%</td><td class="green">flat</td><td class="green">+16%</td><td>Healthy control — flat spend, no decline.</td></tr>
</table>
<p class="note">YoY Feb–May 2026 vs 2025.</p>
</section>

<section>
<h2>Recommendations</h2>
<ul class="tight">
<li><b>Pace prospecting to the high-intent pool.</b> Ration HI delivery across the flight so performance stays consistent instead of front-loading then crashing — the direct fix for any high-spend advertiser whose spend outruns HI supply.</li>
<li><b>Evaluate a full Fangorn rollout for headroom.</b> Fangorn can score a larger high-intent pool, raising the saturation ceiling (EX50 Peak-Performance lift: <b>+36% IVR</b>). Validate the per-advertiser pool gain first — HexClad isn't on Fangorn yet.</li>
<li><b>Add a saturation guardrail.</b> Alert when an advertiser's scored-impression share falls as spend scales — the early-warning signal before visit-rate craters.</li>
</ul>
<p class="claim" style="font-size:0.78em;margin-top:0.8em;">Matched isn't broken — the prospecting pool is saturated.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>
Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,
transition:'fade',transitionSpeed:'slow',width:1100,height:800,margin:0.01,minScale:0.2,maxScale:1.5});
</script></body></html>
"""
HTML = HTML.replace("__CH_AID__", CH_AID).replace("__CH_SAT__", CH_SAT).replace("__CH_REACH__", CH_REACH)
(DIR / "audi_1070_presentation_deck.html").write_text(HTML)
print(f"wrote deck ({len(HTML)//1024} KB, 10 slides)")
