"""AUDI-1070 Avon deck — all Avon graphs, claim->evidence. Embeds 4 PNGs as base64."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
YOY   = b64("audi_1070_avon_yoy_no_change.png")
CURVE = b64("audi_1070_avon_roas_vs_spend.png")
AUD   = b64("audi_1070_avon_audience_size.png")
PACE  = b64("audi_1070_avon_pacing.png")

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — Avon YoY Review</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--red:#D63B2F;--green:#2E8B57;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:#222;}
.reveal h1{margin-top:0;font-size:1.55em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.05em;color:var(--navy);margin-bottom:0.25em;}
.reveal section img{margin:0.1em 0 0;border:0;box-shadow:none;background:#FAFAFA;max-height:600px;}
.claim{font-size:1.0em;color:var(--navy);font-weight:bold;line-height:1.35;}
.sub{color:var(--tl);font-size:0.6em;} .red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;}
.lead{font-size:0.6em;color:var(--tl);line-height:1.5;margin-top:0.6em;}
.note{font-size:0.44em;color:#999;margin-top:0.4em;}
ul.tight{font-size:0.62em;line-height:1.5;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.45em;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<h1>Avon — YoY Performance Review</h1>
<p class="sub" style="margin-top:0.6em;">Is Avon's performance declining? &nbsp;|&nbsp; AUDI-1070</p>
<p class="sub" style="margin-top:1.1em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; 2026-06-30</p>
</section>

<section>
<h2>Bottom line</h2>
<p class="claim">Avon's performance did <span class="green">not</span> decline.<br>The real issue: we can't fully deliver its budget because the <span class="red">high-intent audience is shrinking</span>.</p>
<p class="lead">On a consistent lens, revenue and conversions held and every efficiency rate is flat-to-up. Volume is down only because spend is down — and spend is down because the targetable audience contracted and CPM rose, so we under-deliver the budget. A supply problem, not a Matched problem.</p>
</section>

<section>
<h2>1. Performance did not decline</h2>
<img src="__YOY__">
<p class="note">Feb–May YoY, consistent last-touch lens. Revenue −1%, conversions −3%, ROAS +9%, conv-rate +23% — none of the performance metrics significantly down (Welch t-tests).</p>
</section>

<section>
<h2>2. ROAS is driven by spend, not by year</h2>
<img src="__CURVE__">
<p class="note">Avon's ROAS falls as its monthly spend rises (saturation). 2026 (red) sits on/above the same curve as 2024–25 → no year effect (spend-controlled regression: p=0.10, not significant).</p>
</section>

<section>
<h2>3. The targetable audience shrank −26% in mid-2025</h2>
<img src="__AUD__">
<p class="note">Supply-side contraction (perml audience size): ~89M (Feb 2025) → ~64M (Jun–Jul 2025), and it never recovered. Fewer high-intent IPs available to buy.</p>
</section>

<section>
<h2>4. So we under-deliver the budget</h2>
<img src="__PACE__">
<p class="note">Avon fills only ~40–60% of budget. Feb–May 2026: budget +8% but spend −14% → pacing fell 59% → 47%. We delivered less of a bigger budget.</p>
</section>

<section>
<h2>The reframe — and what to do</h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b>"Performance is worse" is false.</b> Same revenue/conversions on less spend; efficiency rose.</li>
<li><b>"We're not reaching spend goals" is true</b> — and it's a <span class="red">supply/deliverability</span> problem: the high-intent pool shrank −26% and CPM rose +6%, so we can't fill the budget.</li>
<li><b>The lever is audience supply, not "fix Matched":</b> grow/refresh the high-intent eligible pool (identity, data-source freshness) so we can deliver the budget at the same quality.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.7em;">Avon isn't underperforming — we're running out of high-intent audience to spend on.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1100,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML = (HTML.replace("__YOY__", YOY).replace("__CURVE__", CURVE).replace("__AUD__", AUD).replace("__PACE__", PACE))
(DIR / "audi_1070_avon_deck.html").write_text(HTML)
print(f"wrote Avon deck ({len(HTML)//1024} KB, 7 slides)")
