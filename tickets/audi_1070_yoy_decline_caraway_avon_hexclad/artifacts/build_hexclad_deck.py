"""AUDI-1070 — HexClad decline deck: why prospecting ROAS fell 3x (3.14 -> 1.06).
Built ground-up: the paradox -> what it isn't -> the HI->PP mechanism -> the clincher
(visit rate by tier) -> why -> levers. Last-touch (Mike's report). Embeds 3 charts."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
PARADOX = b64("audi_1070_hexclad_paradox.png")
SHIFT   = b64("audi_1070_hexclad_tier_shift.png")
TIER    = b64("audi_1070_hexclad_visit_rate_by_tier.png")

# metrics (last-touch: 2025 CTV Prospecting -> 2026 High-Intent). (metric,2025,2026,yoy,kind)
MET = [
 ("Spend", "$642,267", "$931,422", "+45%", "in"),
 ("Impressions", "30.7M", "40.8M", "+33%", "in"),
 ("Households reached", "11.5M", "14.1M", "+22%", "in"),
 ("Verified Visits", "111,053", "68,214", "−39%", "bad"),
 ("Visit rate", "0.362%", "0.167%", "−54%", "bad"),
 ("Conversions", "4,978", "2,495", "−50%", "bad"),
 ("AOV", "$405.38", "$397.38", "−2%", "flat"),
 ("Order Value", "$2.02M", "$0.99M", "−51%", "bad"),
 ("ROAS", "3.14×", "1.06×", "−66%", "bad"),
]
def kc(k): return {"in":"#27496D","bad":"#D63B2F","flat":"#888"}[k]
metrows = "\n".join(f'<tr><td style="text-align:left">{m}</td><td>{a}</td><td>{b}</td>'
    f'<td style="color:{kc(k)};font-weight:bold">{y}</td></tr>' for (m,a,b,y,k) in MET)

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — HexClad</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--red:#D63B2F;--green:#2E8B57;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:#222;}
.reveal h1{margin-top:0;font-size:1.5em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.02em;color:var(--navy);margin-bottom:0.25em;}
.reveal section img{margin:0.1em 0 0;border:0;box-shadow:none;background:#FAFAFA;max-height:560px;}
.cmp{font-size:0.54em;margin:0.2em auto;border-collapse:collapse;}
.cmp th{background:var(--navy);color:#fff;padding:0.35em 0.9em;}
.cmp td{padding:0.26em 0.9em;border-bottom:1px solid #e3e3e3;text-align:right;}
.claim{font-size:1.0em;color:var(--navy);font-weight:bold;line-height:1.35;}
.sub{color:var(--tl);font-size:0.6em;} .red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.lead{font-size:0.58em;color:var(--tl);line-height:1.5;margin-top:0.5em;}
.note{font-size:0.45em;color:#999;margin-top:0.4em;}
ul.tight{font-size:0.6em;line-height:1.5;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.35em;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<h1>HexClad — Why Prospecting ROAS Fell 3×</h1>
<p class="claim" style="font-size:0.8em;margin-top:0.5em;">HexClad ran out of High-Intent — and its fallback converts a third as well.</p>
<p class="sub" style="margin-top:0.9em;">Jan–May 2025 vs 2026, prospecting, last-touch (the "Campaign - Last Touch" report) &nbsp;|&nbsp; AUDI-1070</p>
<p class="sub" style="margin-top:0.8em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; 2026-06-30</p>
</section>

<section>
<h2>The paradox that started this</h2>
<p class="claim">Spend rose <span class="navy">+45%</span> — but order value <span class="red">halved</span> ($2.0M → $1.0M), and ROAS fell <span class="red">3.14× → 1.06×</span>.</p>
<p class="lead">This is <b>not</b> saturation. If HexClad had simply spent extra into weaker audiences, order value would hold near $2M and ROAS would settle around 2×. Instead <b>total order value fell by half</b> — the audience itself produced far less. Something changed in <i>who</i> we served. (Unlike Avon, which is genuinely healthy — this is a real decline.)</p>
</section>

<section>
<h2>What it is — and what it isn't</h2>
<img src="__PARADOX__">
<p class="note"><b>Not smaller orders:</b> AOV flat ($405→$397). Order value halved because <b>conversions halved</b>. &nbsp; <b>Not saturation:</b> reach GREW +22% (more households, not fewer hit harder). &nbsp; <b>Not tracking:</b> retargeting is healthy ($8.4M→$7.8M OV, ROAS 55→62). &nbsp; <b>The root is a visit-rate collapse (−54%):</b> +33% more impressions produced −39% FEWER visits.</p>
</section>

<section>
<h2>What changed: delivery fell out of High-Intent into Peak Performance</h2>
<img src="__SHIFT__">
<p class="note">Intent scoring (Confluence): <b>High-Intent = 10,000</b> (in the vertical AND the campaign's keywords); <b>Peak Performance = 8,000</b> (in the vertical, NOT the keywords). 2025 delivery was ~95% pure High-Intent. Through 2026, up to <b>34%</b> shifted into Peak Performance as spend scaled and the High-Intent pool ran dry.</p>
</section>

<section>
<h2>The clincher: Peak Performance converts at ⅓ the rate of High-Intent</h2>
<img src="__TIER__">
<p class="note">Per-IP visit rate by delivered tier (2026): <b>High-Intent 3.84%</b>, <b>Peak Performance 1.19%</b> (31% of HI), Mid 1.13%, unscored 0.58%. High-Intent is the only tier that performs; everything below it visits ~3× worse. Moving a third of delivery HI→PP is exactly why visits, conversions, and order value halved.</p>
</section>

<section>
<h2>Why it happened — High-Intent is scarce, Peak Performance is abundant</h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b>The pool asymmetry (MNTN platform, May 2026):</b> High-Intent = <span class="green">14B IPs</span> vs Peak Performance = <span class="red">33B IPs</span> — PP is 2.4× larger. HI (vertical∩keyword) is scarce; PP (vertical-only) is abundant.</li>
<li><b>HexClad scaled prospecting spend +45%</b> into a finite High-Intent pool → exhausted it → the bidder fell back into the abundant Peak-Performance pool, which converts a third as well.</li>
<li><b>The audience also broadened:</b> 2025 = DS13 (Peak-Perf vertical) + DS19 (MM keywords); 2026 = DS46 + DS19 + RTC conquest — a wider net that made the PP fallback easier.</li>
</ul></div>
<p class="note">Note: HexClad is on <b>bucketed (non-Fangorn) scoring</b> — the May-1 Fangorn continuous rollout (3 launch advertisers) does not apply to it. So it's on the <b>old, un-improved</b> Peak Performance tier.</p>
</section>

<section>
<h2>The numbers — prospecting, last-touch, Jan–May</h2>
<table class="cmp">
<tr><th style="text-align:left">Metric</th><th>2025</th><th>2026</th><th>YoY</th></tr>
__MET__
</table>
<p class="note">2025 "CTV Prospecting" vs 2026 "CTV Prospecting High-Intent" (the equivalent main groups). Reproduces the client's Last-Touch report to the dollar. <span style="color:#27496D">More spend & reach</span>, but <span class="red">every outcome collapsed</span> — because the audience mix moved from HI to PP.</p>
</section>

<section>
<h2>Conclusion & levers</h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b>Root cause:</b> HexClad scaled spend past its High-Intent pool; a third of delivery fell into Peak Performance, which converts at ⅓ the rate → order value & ROAS halved. Not saturation, tracking, or AOV.</li>
<li><b>Lever 1 — pace to HI capacity:</b> cap/pace prospecting spend to what the High-Intent pool can absorb, rather than scaling into PP.</li>
<li><b>Lever 2 — grow the HI pool:</b> expand the campaign's keywords (DS19) so more in-vertical IPs qualify as High-Intent (10k) instead of Peak Performance (8k).</li>
<li><b>Lever 3 — evaluate Fangorn:</b> HexClad is on old bucketed PP; Fangorn-scored PP reportedly performs better. Test enabling continuous Fangorn scoring.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.6em;">HexClad didn't get worse at the same audience — it ran out of the good one.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML = (HTML.replace("__MET__", metrows).replace("__PARADOX__", PARADOX)
        .replace("__SHIFT__", SHIFT).replace("__TIER__", TIER))
(DIR / "audi_1070_hexclad_deck.html").write_text(HTML)
print(f"wrote audi_1070_hexclad_deck.html ({len(HTML)//1024} KB, 8 slides)")
