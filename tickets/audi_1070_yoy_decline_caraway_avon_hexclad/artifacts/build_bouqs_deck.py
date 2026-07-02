"""AUDI-1070 — The Bouqs (32147, eCommerce Unit) deck. Story: gate-removal/thrash (HexClad family), NOT over-scaling.
Decisive: within-HI VR ROSE while HI-share collapsed -> delivery left a HEALTHY pool. Power line: it stopped asking for HI.
Same outline as Avon/HexClad/Caraway v4."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
YOY=b64("bouqs_yoy.png"); POOL=b64("bouqs_pool_health.png"); MONTHLY=b64("bouqs_monthly.png"); RIBBON=b64("bouqs_gate_ribbon.png")

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — The Bouqs</title>
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
<h1>The Bouqs — it stopped asking for High-Intent</h1>
<p class="claim" style="font-size:0.8em;margin-top:0.4em;">A real prospecting decline — but the audience didn't fail. The intent gate did.</p>
<div class="kpis" style="margin-top:0.8em;">
<div class="kpi"><div class="n red">−55%</div><div class="l">Prospecting visits</div></div>
<div class="kpi"><div class="n red">−54%</div><div class="l">Prospecting ROAS</div></div>
<div class="kpi"><div class="n navy">4%</div><div class="l">HI-share (was 55%)</div></div>
<div class="kpi"><div class="n green">↑</div><div class="l">within-HI visit rate</div></div>
</div>
<p class="sub" style="margin-top:1.0em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; AUDI-1070 &middot; The Bouqs eCommerce Unit (32147) &middot; prospecting, Jan–May YoY</p>
</section>

<!-- 2 THE ALARM -->
<section>
<h2>1 · The decline is real — and it lives in prospecting</h2>
<img src="__YOY__">
<p class="note"><b>Prospecting visits −55% and ROAS −54% (2.77→1.27) on only −20% spend</b> — visits fell far faster than spend, so the visit rate itself collapsed −34%. <b>AID-wide is only −14% visits</b> because retargeting held. The entire drag is the prospecting funnel — where the intent gate lives.</p>
</section>

<!-- 3 CAMPAIGNS -->
<section>
<h2>2 · The campaigns — a 2026 relaunch into a variant fleet</h2>
<table class="cmp">
<tr><th style="text-align:left">Group</th><th style="text-align:left">Client campaign</th><th>Role</th><th>Note</th></tr>
<tr style="background:#eef3f8"><td>—</td><td style="text-align:left"><b>398872</b> "Beeswax TV Prospecting" (2025 flagship)</td><td>Prospecting S1</td><td>$433K · dark Dec–Feb</td></tr>
<tr style="background:#eef3f8"><td>119362</td><td style="text-align:left"><b>595017</b> "CTV eComm Prospecting 2026" (new flagship)</td><td>Prospecting S1</td><td class="red">launched Apr 15 · ~83% ungated</td></tr>
<tr><td>119361/3</td><td style="text-align:left">High-/Low-/Auto-Frequency <b>variant fleet</b> + VDay (529549)</td><td>Prospecting S1</td><td class="red">heavily short-flighted</td></tr>
<tr><td>—</td><td style="text-align:left">Multi-Touch companions (398870, 595018, 529546)</td><td>MT2 / MT3</td><td>unscored <b>by design</b></td></tr>
</table>
<p class="note">The 2025 flagship (398872) was replaced by a <b>2026 relaunch</b> into a proliferating fleet of High/Low/Auto-Frequency variant campaigns (group 119362 + siblings). Two things to separate: the <b>obj=1 stage-1</b> campaigns (595017 etc.) carry the intent gate; the <b>Multi-Touch (MT2/MT3)</b> companions are unscored by funnel design — not a gate to fix.</p>
</section>

<!-- 4 DECISIVE: NOT OVER-SCALING -->
<section>
<h2>3 · Not over-scaling — delivery LEFT a healthy pool</h2>
<img src="__POOL__">
<p class="note"><b>This is the decisive result.</b> As served HI-share collapsed 55%→4%, the <b>within-HI visit rate ROSE 0.30%→2.40%</b>. If the High-Intent audience were exhausted (Caraway), within-HI visit rate would FALL. It rose — the households that clear the gate convert <i>better than ever</i>. So the pool is healthy; the gate simply stopped serving it. (Score is binary — all 10000 — so avg score is blind; within-HI visit rate is the correct lens.)</p>
</section>

<!-- 5 GATE + MONTHLY -->
<section>
<h2>4 · The gate was thrashed, then relaunched ungated</h2>
<img src="__MONTHLY__">
<p class="note"><b>51 gate changes on the flagship</b> (a one-point-a-day "walking ramp"), a hard removal (0/−1) Nov 11–24 for the holiday blowout, then a clean <b>re-gate to 10,000 in March → HI-share jumped to 77% within days</b> (proof the gate controls composition). But the <b>2026 relaunch reintroduced the leak</b>: the stage-1 fleet (595017) runs ~83% of spend ungated across short flights. Nuance: the gate steers <i>composition</i>, not the blended visit rate directly (they don't co-move month-to-month) — the damage is that delivery walked out of a good pool.</p>
</section>

<!-- 4b GATE RIBBON -->
<section>
<h2>4b · Every prospecting campaign, and its gate on/off over time</h2>
<img src="__RIBBON__">
<p class="note">Each row is an obj=1 stage-1 prospecting campaign; color = its HHST gate that day (<span class="green">green</span> = gated HI/Peak, <span style="color:#C77B30;font-weight:bold">amber</span> = mid/continuous "walking ramp", <span class="red">red</span> = <b>no gate</b>). The 2025 flagship <b>398872</b> ran ungated (red) from mid-Nov through Feb; the 2026 relaunch flagship <b>595017</b> goes red again in May. The High/Low/Auto-Frequency variant fleet mostly runs mid/continuous, rarely locked to HI. Multi-Touch (obj 5/6) companions are unscored by design and omitted.</p>
</section>

<!-- 6 WHAT WE RULED OUT -->
<section>
<h2>5 · What we checked — and ruled out</h2>
<table class="cmp big">
<tr><th style="text-align:left">Suspect</th><th>Verdict</th></tr>
<tr><td style="text-align:left">Over-scaling / exhausted HI pool</td><td class="green">RULED OUT — within-HI VR rose</td></tr>
<tr><td style="text-align:left">Attribution lens (first vs last touch)</td><td class="green">RULED OUT — −55% under both lenses</td></tr>
<tr><td style="text-align:left">Tracking / pixel outage</td><td class="green">RULED OUT — daily scan, no craters</td></tr>
<tr><td style="text-align:left">Audience-expression change</td><td class="green">RULED OUT — standard MM DS mix</td></tr>
<tr><td style="text-align:left"><b>The "71% unscored" headline</b></td><td>~47% is MT2/MT3 <b>unscored by design</b> (mix)</td></tr>
<tr><td style="text-align:left">March "recovery"</td><td>HI-share instant; performance ramps ~4 wks (TI-780)</td></tr>
</table>
<p class="note">Two honesty flags for leadership: (1) the "71% unscored" is inflated — only ~half is a gate to fix (595017 obj=1 = 49% unscored); the rest is Multi-Touch funnel campaigns unscored by design. (2) RTC is <b>live</b> for The Bouqs (unlike the first three), correctly excluded. Also: Dec-2025 was a full-account pause, and the Subscriptions unit (31906) is dark in 2026 — clean the YoY before blending.</p>
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
<p class="note">Four of five declines trace to the <b>HHST intent gate</b> being removed/thrashed — <b>not</b> the MM audience. Only Caraway is true over-scaling; The Bouqs and Kindred both kept a <i>healthy</i> HI pool (within-HI VR rose/held), so their fix is the gate, not capping spend. Avon is the control that re-gated and recovered.</p>
</section>

<!-- 8 CONCLUSION -->
<section>
<h2>7 · Conclusion & fix</h2>
<div style="text-align:left;display:inline-block;margin-top:0.2em;">
<ul class="tight">
<li><b>Real decline, healthy audience.</b> Prospecting visits −55%, ROAS −54% — but within-HI visit rate ROSE. Delivery left a good pool; it didn't run out of one.</li>
<li><b class="navy">Set and HOLD the gate (≥8001) on the stage-1 2026 fleet</b> (595017 / group 119362, currently ~83% ungated) and stop the walking-ramp thrash. Consolidate the High/Low/Auto-Frequency variants to end group-level short-flight (&lt;72h) auto-ungating.</li>
<li><b class="navy">Leave the Multi-Touch campaigns alone</b> — they're unscored by funnel design (~47% of the "unscored" surge). Pair the gate with pacing and allow ~4 weeks to ramp (composition recovers overnight, performance follows).</li>
<li><b>Clean the YoY:</b> exclude the Dec-2025 full-account pause; decide whether the dark Subscriptions unit belongs in the blend.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.4em;">The Bouqs didn't run out of high-intent — it stopped asking for it. Turn the gate back on.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML=HTML.replace("__YOY__",YOY).replace("__POOL__",POOL).replace("__MONTHLY__",MONTHLY).replace("__RIBBON__",RIBBON)
(DIR/"audi_1070_bouqs_deck.html").write_text(HTML)
print(f"wrote audi_1070_bouqs_deck.html ({len(HTML)//1024} KB, {HTML.count('<section>')} slides)")
