"""AUDI-1070 — Kindred Bravely (35094) deck. Story: gate-removal/thrash + mix-shift; over-scaling DISPROVEN.
Scaled spend +65% but within-HI VR held -> NOT saturation; ROAS -81% lens-invariant; gate off for holidays + Feb thrash.
Same outline as Avon/HexClad/Caraway/Bouqs v4."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
YOY=b64("kindred_yoy.png"); SAT=b64("kindred_not_saturation.png"); GATE=b64("kindred_gate.png"); LENS=b64("kindred_lens.png")

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — Kindred Bravely</title>
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
</style></head><body><div class="reveal"><div class="slides">

<!-- 1 TITLE -->
<section>
<h1>Kindred Bravely — the gate came off for the holidays</h1>
<p class="claim" style="font-size:0.8em;margin-top:0.4em;">Spend up +65%, ROAS down −81% — a gate failure, not a scale failure.</p>
<div class="kpis" style="margin-top:0.8em;">
<div class="kpi"><div class="n red">+65%</div><div class="l">Spend</div></div>
<div class="kpi"><div class="n red">−81%</div><div class="l">ROAS (9.8→1.8)</div></div>
<div class="kpi"><div class="n red">−63%</div><div class="l">Visit rate</div></div>
<div class="kpi"><div class="n navy">flat</div><div class="l">AOV</div></div>
</div>
<p class="sub" style="margin-top:1.0em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; AUDI-1070 &middot; Kindred Bravely (35094) &middot; prospecting, Jan–May YoY</p>
</section>

<!-- 2 THE ALARM -->
<section>
<h2>1 · Spend UP, results DOWN — an efficiency collapse</h2>
<img src="__YOY__">
<p class="note"><b>Kindred scaled spend +65%</b> (and impressions +43%) — but <b>visits fell −47%, visit rate −63%, ROAS −81%</b> (9.76→1.81). Conversion rate −41% and CPM +15% compound the collapse. <b>AOV is flat (−2%)</b>, so it's not a revenue-mix or basket story — it's a genuine per-impression efficiency loss.</p>
</section>

<!-- 3 CAMPAIGNS -->
<section>
<h2>2 · The campaigns — one thrashed flagship + a 2026 expansion</h2>
<table class="cmp">
<tr><th style="text-align:left">Group</th><th style="text-align:left">Client campaign</th><th>Role</th><th>Note</th></tr>
<tr style="background:#eef3f8"><td>69884</td><td style="text-align:left"><b>261318</b> "Beeswax TV Prospecting" (High Pop)</td><td>Prospecting S1</td><td class="red">both years · 198 gate changes</td></tr>
<tr><td>109926 / 96108</td><td style="text-align:left">Mid-Pop (540723) + Low-Pop (463188)</td><td>Prospecting S1</td><td>new 2026 / Aug '25</td></tr>
<tr><td>115943/5/6</td><td style="text-align:left">Q1-2026 variant fleet (Motherhood / Mom-Focus / Harter)</td><td>Prospecting S1</td><td>added Mar 25 '26</td></tr>
<tr><td>—</td><td style="text-align:left">Multi-Touch companions (329967, 329966, …)</td><td>MT2 / MT3</td><td>unscored <b>by design</b></td></tr>
</table>
<p class="note">Kindred kept its <b>flagship 261318</b> across both years (the direct YoY subject) and in 2026 <b>expanded</b> into new "Pop"-tier and Q1 variant prospecting groups. The flagship's gate was changed <b>198 times</b> — a constant walking-ramp thrash. Retargeting (group 89071) is a separate, stable set.</p>
</section>

<!-- 4 DECISIVE: OVER-SCALING DISPROVEN -->
<section>
<h2>3 · Over-scaling DISPROVEN — the HI pool held</h2>
<img src="__SAT__">
<p class="note"><b>The obvious hypothesis is over-scaling</b> — spend +65% into a finite High-Intent pool. But it's wrong: as spend climbed, the <b>within-HI visit rate held ~0.7–1.1%</b> and does NOT fall with spend (Pearson r POSITIVE) — the highest-spend months are among the highest within-HI rates. If Kindred had drained the pool (Caraway), within-HI VR would collapse. It didn't. <b>So the fix is the gate, not capping spend.</b> (Score is binary ~9,900–10,000 → avg score is blind; within-HI VR is the correct lens.)</p>
</section>

<!-- 5 THE GATE -->
<section>
<h2>4 · The mechanism — the holiday gate-removal, then a Feb thrash</h2>
<img src="__GATE__">
<p class="note">The flagship gate <b>held ~55% HI Jun–Oct</b>, then dropped to <b>0/−1 on Nov 19</b> for the holidays (HI-share → 2.6% by December, ungated to Jan 6). In <b>February it oscillated daily</b> (10000 ↔ 0 ↔ 3334 Mid-Intent), then loosened through May. Even in "good" months ~45% of delivery is unscored — the <b>mix-shift</b> that drags blended ROAS: 2026 overall visit rate (0.43%) is <i>half</i> the within-HI rate (0.88%).</p>
</section>

<!-- 6 LENS -->
<section>
<h2>5 · The drop is real — not an attribution artifact</h2>
<img src="__LENS__">
<p class="note">ROAS falls <b>−81% under BOTH lenses</b> — plain last-touch (9.76→1.81) and the client's last-touch + <code>competing_*</code> "industry_standard" view (19.13→3.53). So it's not a reporting-lens or window trick. We also ruled out a tracking outage (views and conversions both track), an expression swap, and Fangorn (appears only in May '26, too late). RTC is live here and correctly excluded. The decline is real, lens-invariant, and reversible.</p>
</section>

<!-- 7 CONTRAST -->
<section>
<h2>6 · Five advertisers, one lever — the gate</h2>
<table class="cmp">
<tr><th style="text-align:left"></th><th>HexClad</th><th>Caraway</th><th>Avon</th><th>Bouqs</th><th>Kindred</th></tr>
<tr><td style="text-align:left">Spend YoY</td><td>+45%</td><td>+191%</td><td class="green">−18%</td><td>−20%</td><td>+65%</td></tr>
<tr><td style="text-align:left">Mode</td><td class="red">gate thrash</td><td class="red">over-scaled</td><td class="green">healthy</td><td class="red">gate thrash</td><td class="red">gate thrash</td></tr>
<tr><td style="text-align:left">HI pool health</td><td>—</td><td class="red">exhausted</td><td class="green">held</td><td class="green">healthy (VR↑)</td><td class="green">held (VR flat)</td></tr>
<tr><td style="text-align:left"><b>ROAS YoY</b></td><td class="red">−66%</td><td class="red">−71%</td><td class="green">+8%</td><td class="red">−54%</td><td class="red">−81%</td></tr>
</table>
<p class="note">Kindred is the cleanest counter-example to "the well ran dry": it scaled harder than anyone except Caraway, yet its High-Intent pool <b>held</b>. Four of five declines are the <b>gate</b>, not the MM audience. Only Caraway is genuine over-scaling.</p>
</section>

<!-- 8 CONCLUSION -->
<section>
<h2>7 · Conclusion & fix</h2>
<div style="text-align:left;display:inline-block;margin-top:0.2em;">
<ul class="tight">
<li><b>A gate failure, not a scale failure.</b> ROAS −81% (lens-invariant) while spend rose +65% — but within-HI visit rate never fell with spend. The audience is fine.</li>
<li><b class="navy">Restore and HOLD the HHST gate at 10,000 year-round</b> — stop the holiday gate-off, the February daily thrash, and the May loosening that admit Mid-Intent (3334) and unscored inventory. That is the primary lever.</li>
<li><b class="navy">Pace HI impressions across flights</b> as a sensible complement — but do <i>not</i> frame the fix as capping spend to a saturated pool; the data doesn't support saturation.</li>
<li><b>Investigate separately</b> the per-impression efficiency loss (CPM +15%, conv-rate −41%) — the gate explains the mix, not all of the efficiency drop. Monitor within-HI VR and HI-share, not avg score (binary and blind).</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.4em;">Kindred pulled the intent gate off for the holidays — ROAS fell 81%. Turn it back on, and hold it.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML=HTML.replace("__YOY__",YOY).replace("__SAT__",SAT).replace("__GATE__",GATE).replace("__LENS__",LENS)
(DIR/"audi_1070_kindred_deck.html").write_text(HTML)
print(f"wrote audi_1070_kindred_deck.html ({len(HTML)//1024} KB, {HTML.count('<section>')} slides)")
