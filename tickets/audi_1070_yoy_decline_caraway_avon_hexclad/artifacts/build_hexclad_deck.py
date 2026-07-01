"""AUDI-1070 — HexClad deck v4 (TIGHT). Story: campaigns -> assumptions -> score distribution + served
counts -> rate metrics -> questions answered. Through-line: still in the '93373 High-Intent' campaign;
the gate has been THRASHED (removed mid-Nov 2025, restored to 10000 Jan 5, off again Feb, oscillating since) —
delivery HI-share tracks it every time. Detail charts moved to an Appendix."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
GANTT=b64("audi_1070_hexclad_campaign_gantt.png"); TIER=b64("audi_1070_hexclad_visit_rate_by_tier.png")
GATE=b64("hexclad_gate_thrash.png"); FANGORN=b64("audi_1070_hexclad_fangorn.png")
PACING=b64("audi_1070_hexclad_pacing.png"); MASTER=b64("audi_1070_hexclad_master_timeline.png")

MET=[("Spend","$642,267","$931,422","+45%","in"),("Verified Visits","111,053","68,214","−39%","bad"),
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
<h1>HexClad — the High-Intent campaign that lost its gate</h1>
<p class="claim" style="font-size:0.8em;margin-top:0.4em;">The intent gate was thrashed from November on — never durably restored to High-Intent.</p>
<div class="kpis" style="margin-top:0.8em;">
<div class="kpi"><div class="n green">97.8%</div><div class="l">HI before (Jul–Oct)</div></div>
<div class="kpi"><div class="n red">31%</div><div class="l">HI now (still running)</div></div>
<div class="kpi"><div class="n red">3.14→1.06</div><div class="l">ROAS</div></div>
<div class="kpi"><div class="n red">−51%</div><div class="l">Order Value</div></div>
</div>
<p class="sub" style="margin-top:1.0em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; AUDI-1070 &middot; prospecting, Jan–May YoY</p>
</section>

<!-- 2 CAMPAIGNS -->
<section>
<h2>1 · The campaigns — to the client, each is a campaign_group</h2>
<img src="__GANTT__" style="max-height:440px">
<p class="note"><b>campaign_id</b> = our internal funnel stages (S1 Prospecting / Multi-Touch / Retargeting); the client sees the <b>group</b>. The decline is the flagship <b>"CTV Prospecting High-Intent" (93373, $2.73M)</b> — <b>still running today</b>. It ran clean HI-only Jul–Oct, then <b>mid-Nov the intent gate was removed</b> (holiday max-reach). Since then it's been <b>thrashed</b> — restored to 10000 in Jan (delivery recovered to ~80% HI), off again in Feb, oscillating since — so the campaign spends <b>repeated, extended stretches ungated</b>, and delivery HI-share tracks the gate every time. Oct was a "Scale Up" A/B test; Mar '26 added a "General Interest" campaign. Retargeting (56957) is a separate, healthy campaign.</p>
</section>

<!-- 3 ASSUMPTIONS -->
<section>
<h2>2 · The assumptions everyone holds — all <span class="red">false</span></h2>
<table class="cmp big">
<tr><th style="text-align:left">Assumption</th><th>Reality</th><th></th></tr>
<tr><td style="text-align:left">"Same campaign, so compare directly"</td><td>gate thrashed mid-flight (Nov+)</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"We target High-Intent"</td><td class="red">now 31% HI (was 97.8%)</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"The audience/MM was cut"</td><td class="green">HI substrate (DS13∩DS19) intact</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"It's Fangorn / a model change"</td><td class="green">bucketed all window; Fangorn Jun 4</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"High-Intent ran out"</td><td>emerging in Oct; gate is the cause</td><td class="red">✗</td></tr>
</table>
<p class="note">Every intuition points at the audience or the model. The data points at one config lever: the household-score gate.</p>
</section>

<!-- 4 SCORE DISTRIBUTION + COUNTS (centerpiece) -->
<section>
<h2>3 · Score distribution &amp; served volume — by campaign</h2>
<table class="cmp" style="font-size:0.52em">
<tr><th style="text-align:left">Client campaign</th><th>Imps</th><th>HH</th><th>HI</th><th>PP</th><th>MI</th><th>MaxR</th><th>Unscored</th></tr>
<tr><td style="text-align:left">56914 · early prospecting</td><td>2.0M</td><td>1.2M</td><td class="green">100%</td><td>0</td><td>0</td><td>0</td><td>0</td></tr>
<tr style="background:#eef7f0"><td style="text-align:left"><b>93373 · High-Intent</b> (Jul–Nov 10)</td><td>16.3M</td><td>6.5M</td><td class="green"><b>97.8%</b></td><td>0</td><td>2</td><td>0</td><td>0.2</td></tr>
<tr style="background:#fbeaea"><td style="text-align:left"><b>93373 · High-Intent</b> (Nov 11+) <b>← STILL LIVE</b></td><td>83.1M</td><td>21.3M</td><td class="red"><b>31.2%</b></td><td>15.8</td><td>14.8</td><td>4.0</td><td class="red"><b>34.2%</b></td></tr>
<tr><td style="text-align:left">100739 · Cell A BAU (Oct test)</td><td>4.8M</td><td>2.5M</td><td>86.6%</td><td>2</td><td>3</td><td>7</td><td>1</td></tr>
<tr><td style="text-align:left">100744 · Cell B Scale Up (Oct test)</td><td>8.5M</td><td>3.3M</td><td>85.9%</td><td>3</td><td>3</td><td>8</td><td>1</td></tr>
<tr style="background:#fbeaea"><td style="text-align:left">111708 · General Interest (Mar '26+)</td><td>1.6M</td><td>1.2M</td><td class="red">0%</td><td>0</td><td>0</td><td>0</td><td class="red">100%</td></tr>
</table>
<p class="note"><b>The whole story in one table.</b> The flagship "High-Intent" campaign was <b>97.8% HI</b> (Jul–Oct) — and it's <b>still running, averaging ~31% HI / 34% unscored post-November</b> (same campaign, 5× the volume) because the gate was <b>removed mid-Nov and thrashed since</b> (restored Jan → ~80% HI, off again Feb, oscillating). The "General Interest" campaign added in March is <b>100% unscored by design</b>. RTC-excluded; HI = household_score ≥ 8,001.</p>
</section>

<!-- 5 RATE METRICS -->
<section>
<h2>4 · Rate metrics — why the mix is the whole game</h2>
<img src="__TIER__" style="max-height:420px">
<table class="cmp" style="font-size:0.5em;margin-top:0.1em">
<tr><th style="text-align:left">YoY (Jan–May)</th><th>2025</th><th>2026</th><th></th></tr>
<tr><td style="text-align:left">Visit rate</td><td>0.362%</td><td>0.167%</td><td class="red">−54%</td></tr>
<tr><td style="text-align:left">Conversions</td><td>4,978</td><td>2,495</td><td class="red">−50%</td></tr>
<tr><td style="text-align:left">AOV</td><td>$405</td><td>$397</td><td class="navy">flat</td></tr>
<tr><td style="text-align:left">ROAS</td><td>3.14×</td><td>1.06×</td><td class="red">−66%</td></tr>
</table>
<p class="note">High-Intent converts <b>~3× better</b> (3.84% vs ~1% for PP/Mid/unscored). Flat AOV → the ROAS drop is a <b>conversion-count</b> problem, and conversions track the HI share. Shift 98%→31% HI and the rate metrics fall exactly as observed.</p>
</section>

<!-- 6 WHAT HAPPENED -->
<section>
<h2>5 · What happened — and why it's still happening</h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b class="green">Jul–Oct 2025:</b> clean High-Intent regime — gate 6,666–10,000, <b>97.8% HI</b>.</li>
<li><b class="navy">Oct:</b> a <b>"Scale Up"</b> A/B test pushed spend ~40% over what the HI pool refreshes — supply began to strain.</li>
<li><b class="red">~Nov:</b> a <b>manual change set the HHST gate to 0</b> on the main campaign to hit spend → HI <b>98% → 13%</b> overnight; Black-Friday volume ~20×.</li>
<li><b class="red">Short flights force it back:</b> the client runs 1–3 day flights — <b>any flight under 72h auto-sets HHST to 0</b> for deliverability, so the gate keeps getting forced off.</li>
<li><b class="red">Then forgotten:</b> left at 0 on 93373 and never restored → <b>still 31% HI today.</b> (+ a "General Interest" campaign added Mar '26, 100% unscored.)</li>
</ul></div>
<p class="claim" style="font-size:0.66em;margin-top:0.4em;">Not gradual, not the audience — a gate switched off (manually + by short flights) and left off.</p>
</section>

<!-- 7 QUESTIONS ANSWERED -->
<section>
<h2>6 · Questions answered</h2>
<table class="cmp" style="font-size:0.56em">
<tr><th style="text-align:left">Question</th><th style="text-align:left">Answer</th></tr>
<tr><td style="text-align:left"><b>Why did the gate go to 0?</b></td><td style="text-align:left"><b>Short flights (&lt;72h) auto-set HHST to 0 + a manual Nov change, then left/forgotten</b> (PEX to educate client)</td></tr>
<tr><td style="text-align:left">Is it Fangorn / a scoring change?</td><td style="text-align:left" class="green">No — 0% continuous scores through May; migrated Jun 4–5 (after the window)</td></tr>
<tr><td style="text-align:left">Did we cut the audience / MM?</td><td style="text-align:left" class="green">No — vertical DS13 ∩ keyword DS19 intact all window</td></tr>
<tr><td style="text-align:left">Is the "mix under a 10k gate" a bug?</td><td style="text-align:left" class="green">No — gated path is 99.99% HI; mix = monthly blend + RTC (8%, bypasses by design)</td></tr>
<tr><td style="text-align:left">Did High-Intent run out?</td><td style="text-align:left">Emerging in Oct (Scale-Up); refreshable — recovered in 2026 when the gate allowed</td></tr>
<tr><td style="text-align:left">Attribution changes?</td><td style="text-align:left">Same lens both years (industry_standard = last-touch + competing) — not a lens artifact</td></tr>
<tr><td style="text-align:left">Same pattern elsewhere?</td><td style="text-align:left">Yes — Caraway replicates (gate removed Nov 28); Avon healthy (low spend stays in HI)</td></tr>
</table>
<p class="note">Every alternative explanation was tested and ruled out. Detail + charts in the appendix.</p>
</section>

<!-- 8 CONCLUSION -->
<section>
<h2>7 · Conclusion &amp; the fix</h2>
<div style="text-align:left;display:inline-block;margin-top:0.2em;">
<ul class="tight">
<li><b>Cause (confirmed):</b> the HHST gate on 93373 sits at 0 — a manual Nov change to hit spend, then left/forgotten, and repeatedly re-forced by <b>short flights (&lt;72h auto-set the gate to 0)</b>. Delivery slid 98%→31% HI → ROAS 3.1→1.1. Not the audience, model, or attribution.</li>
<li><b class="navy">PEX: educate the client</b> — run flights ≥72h so the intent gate stays engaged; the short-flight bursts are what auto-drop it.</li>
<li><b class="navy">Restore &amp; hold the gate</b> on 93373 (HI / clean floor).</li>
<li><b class="navy">Pace HI spend</b> ~$5K/day sustained so a "Scale Up" doesn't drain the ~3.8M live pool like October.</li>
<li><b class="navy">Revisit "General Interest"</b> (100% unscored) — decide if it belongs in the High-Intent campaign's numbers.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.4em;">Turn the gate back on — and stop the short flights that switch it off.</p>
</section>

<!-- APPENDIX -->
<section>
<h2 class="divider">— APPENDIX · backup for the questions —</h2>
<p class="sub">The gate mechanism, Fangorn rule-out, pacing model, full timeline, and the exact numbers.</p>
</section>

<section>
<h2>A1 · The gate was thrashed — and delivery tracks it every time</h2>
<img src="__GATE__">
<p class="note"><b>Not "set to 0 and never reverted" — it was thrashed.</b> Flagship 446801 gate: ~6666 Jul–Oct (93–98% HI) → <b>removed mid-Nov</b> for the holiday (HI → 11–15%, on 28M/19M imps) → <b>restored to 10,000 Jan 5</b> (HI recovers to 80%) → <b>off again Feb 5</b> (31%) → oscillates Mar–Jun (Fangorn-era ramps interrupted by drops to 0). <b>Delivery HI-share follows the gate every time</b> — the Jan recovery is the natural experiment proving the gate is the lever. The damage is the repeated <b>ungated stretches</b>, above all the Nov–Dec holiday blowout when spend & volume also exploded.</p>
</section>

<section>
<h2>A2 · It's not Fangorn — proven</h2>
<img src="__FANGORN__">
<p class="note">0% continuous (Fangorn) scores every month Jun 2025–May 2026 = 100% bucketed. Migrated to Fangorn Jun 4–5 2026, after the entire decline window.</p>
</section>

<section>
<h2>A3 · Pacing — the ceiling is the ~3.8M live pool, not 7M</h2>
<img src="__PACING__">
<p class="note">You pace against the live 30-day pool (~3.8M HI IPs), not the 7M lifetime figure. Sustainable ~$5K/day. October's "Scale Up" ran ~40% over → HI "running on refresh." Refreshable flow limit, not a wall.</p>
</section>

<section>
<h2>A4 · Full change timeline (every lever)</h2>
<img src="__MASTER__">
<p class="note">Audience DS moves (blue) never touched the HI substrate. The one thing that flips delivery is the <b>gate</b>: removed mid-Nov (Dec held off), restored Jan (HI→80%), then thrashed through 2026 (66 changes). Delivery HI-share tracks it every time.</p>
</section>

<section>
<h2>A5 · The numbers — prospecting, last-touch, Jan–May YoY</h2>
<table class="cmp">
<tr><th style="text-align:left">Metric</th><th>2025</th><th>2026</th><th>YoY</th></tr>
__MET__
</table>
<p class="note">The <b>industry_standard</b> (last-touch + <code>competing_*</code>) lens confirms it: all-prospecting ROAS 8.78→3.87 (−56%), down every month. Every lens, every month: real decline.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML=HTML.replace("__MET__",metrows).replace("__GANTT__",GANTT).replace("__TIER__",TIER)\
    .replace("__GATE__",GATE).replace("__FANGORN__",FANGORN).replace("__PACING__",PACING).replace("__MASTER__",MASTER)
(DIR/"audi_1070_hexclad_deck.html").write_text(HTML)
print(f"wrote audi_1070_hexclad_deck.html ({len(HTML)//1024} KB, {HTML.count('<section>')} slides)")
