"""AUDI-1070 — AVON deck v4 (tight). Story: Avon is the CONTROL that PROVES the fix — it had the SAME
holiday gate-removal as HexClad (Nov 19) but RE-GATED Jan 6 and recovered; low spend, stayed in HI -> healthy.
Same outline as HexClad/Caraway v4."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
GATE=b64("avon_gate.png")

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — Avon</title>
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
<h1>Avon — the control that proves the fix</h1>
<p class="claim" style="font-size:0.8em;margin-top:0.4em;">Same holiday gate-removal as HexClad — but Avon turned the gate back on, and recovered.</p>
<div class="kpis" style="margin-top:0.8em;">
<div class="kpi"><div class="n green">−18%</div><div class="l">Spend</div></div>
<div class="kpi"><div class="n green">+8%</div><div class="l">ROAS (7.9→8.6)</div></div>
<div class="kpi"><div class="n green">4.2%</div><div class="l">Visit rate</div></div>
<div class="kpi"><div class="n navy">flat</div><div class="l">AOV</div></div>
</div>
<p class="sub" style="margin-top:1.0em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; AUDI-1070 &middot; prospecting, Jan–May YoY</p>
</section>

<!-- 2 CAMPAIGNS -->
<section>
<h2>1 · The campaigns (client groups)</h2>
<table class="cmp">
<tr><th style="text-align:left">Group</th><th style="text-align:left">Client campaign</th><th>Role</th><th>Active</th><th>Spend</th></tr>
<tr style="background:#eef3f8"><td>69271</td><td style="text-align:left"><b>CTV Prospecting 2026</b> (flagship 259556 + Multi-Touch)</td><td>Prospecting</td><td>Jun '25–now</td><td class="navy"><b>$154K</b></td></tr>
<tr><td>69273</td><td style="text-align:left">CTV Retargeting 2026 (6 stages)</td><td>Retargeting</td><td>Jun '25–now</td><td>$56K</td></tr>
</table>
<p class="note">Simple, stable structure: one prospecting group (flagship 259556 "Beeswax Television Prospecting" + multi-touch) + one retargeting group. <b>No new mega-prospecting campaign</b> (unlike HexClad/Caraway), and the flagship <i>contracted</i> — Avon did not scale into a bigger pool.</p>
</section>

<!-- 3 ASSUMPTIONS -->
<section>
<h2>2 · The assumptions — all <span class="red">false</span></h2>
<table class="cmp big">
<tr><th style="text-align:left">Assumption</th><th>Reality</th><th></th></tr>
<tr><td style="text-align:left">"Avon's performance declined"</td><td class="green">ROAS +8%, conv-rate up, VR ~4.2%</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"Spend stayed the same"</td><td class="green">spend −18% (Avon contracted)</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"Avon isn't in High-Intent (mix)"</td><td class="green">prospecting 97–100% HI in base months</td><td class="red">✗</td></tr>
<tr><td style="text-align:left">"The mix means it broke"</td><td>holiday gate-off + RTC — transient</td><td class="red">✗</td></tr>
</table>
<p class="note">The "Avon declined" premise (Paulo #1 / Mike Dolt) is the wrong window/lens: 2025→2026 Avon is healthy. The apparent decline lives in a 2024→2025 comparison (Avon scaled +71% then) and/or the first-touch client lens.</p>
</section>

<!-- 4 THE GATE STORY -->
<section>
<h2>3 · The "mix," explained — same holiday gate-removal, but RE-GATED</h2>
<img src="__GATE__">
<p class="note"><b>Avon's prospecting is mostly HI (97–100%) in the base months.</b> The mix appears in three places: <b>Nov 19 '25 the gate was REMOVED (→0/−1)</b> for the holiday spend spike (Dec 8% HI) — the <i>same</i> event as HexClad (Nov 11) and Caraway (~Nov 28); <b>Jan 6 '26 it was RE-GATED to 10,000</b> → recovered to 99.9% HI (Feb–Mar); May–Jun loosening + Fangorn onset. Plus RTC (~11%) bypasses the gate by design. <b>Avon turned the gate back on — HexClad never did. That one decision is the difference.</b></p>
</section>

<!-- 5 RATE METRICS -->
<section>
<h2>4 · Rate metrics — healthy on every metric that pays</h2>
<table class="cmp big">
<tr><th style="text-align:left">Prospecting, Jan–May</th><th>2025</th><th>2026</th><th>YoY</th></tr>
<tr><td style="text-align:left">Spend</td><td>$56,813</td><td>$46,612</td><td class="green">−18%</td></tr>
<tr><td style="text-align:left">Visit rate</td><td>4.73%</td><td>4.21%</td><td>−11%</td></tr>
<tr><td style="text-align:left">Conversions</td><td>8,810</td><td>7,771</td><td>−12%</td></tr>
<tr><td style="text-align:left">AOV</td><td>$51.09</td><td>$51.54</td><td class="navy">flat</td></tr>
<tr><td style="text-align:left"><b>ROAS</b></td><td>7.92×</td><td>8.59×</td><td class="green" style="font-weight:bold">+8%</td></tr>
</table>
<p class="note">Avon <b>contracted</b> (spend −18%) — fewer users at higher frequency — but <b>converted better</b> and <b>ROAS rose</b>. Visit rate ~4.2% is 25–30× the cookware advertisers (HexClad/Caraway ~0.15%). The client's first-touch view is also up (ROAS 9.4→10.4). Healthy on both lenses.</p>
</section>

<!-- 6 API/LENS -->
<section>
<h2>5 · Why the client's API graph ≠ our BQ numbers</h2>
<table class="cmp">
<tr><th style="text-align:left">Avon prospecting (Jan–May)</th><th>Verified Visits</th><th>ROAS</th></tr>
<tr><td style="text-align:left">Naive BQ pull (last-touch)</td><td>reproduces LT</td><td>lower</td></tr>
<tr><td style="text-align:left"><b>Client UI / API (first-touch, industry_standard)</b></td><td><b>692,888 / 598,436 EXACT</b></td><td><b>22.1 / 26.4 EXACT</b></td></tr>
</table>
<p class="note">The client UI/API = <b>CHAPI → ClickHouse</b>, not BigQuery. Three knobs: <b>(1)</b> attribution lens — first-touch (<code>industry_standard</code>, adds <code>competing_*</code>) vs last-touch → ~5× the visits, ~2–3× the ROAS; <b>(2)</b> Verified Visits = <code>clickpass</code> raw row count (~1.28×); <b>(3)</b> scope/aggregation. We reproduce the client's UI <b>to the dollar</b> in BQ by matching the lens. <b>Watch the YoY trap:</b> comparing 2025 (last-touch) vs 2026 (first-touch) manufactures ~52pp of an apparent crash — hold the lens constant.</p>
</section>

<!-- 7 CONTRAST -->
<section>
<h2>6 · Three advertisers, one finite-HI ceiling</h2>
<table class="cmp">
<tr><th style="text-align:left"></th><th>HexClad</th><th>Caraway</th><th>Avon</th></tr>
<tr><td style="text-align:left">Spend YoY</td><td>+45%</td><td>+191%</td><td class="green">−18%</td></tr>
<tr><td style="text-align:left">Gate after holiday</td><td class="red">left OFF</td><td>mostly held</td><td class="green">RE-GATED Jan 6</td></tr>
<tr><td style="text-align:left">Stayed in HI?</td><td class="red">no (31%)</td><td>yes (82–99%)</td><td class="green">yes (97–100%)</td></tr>
<tr><td style="text-align:left">Mode</td><td class="red">gate removed</td><td class="red">over-scaled HI</td><td class="green">healthy control</td></tr>
<tr><td style="text-align:left"><b>ROAS YoY</b></td><td class="red">−66%</td><td class="red">−71%</td><td class="green"><b>+8%</b></td></tr>
</table>
<p class="note">Same ceiling (finite High-Intent), three outcomes. HexClad <b>left</b> HI (gate off); Caraway <b>overwhelmed</b> HI (3× spend); Avon <b>stayed in</b> HI (low spend) and <b>re-gated</b> after the holiday. Avon is the proof that the fix works.</p>
</section>

<!-- 8 CONCLUSION -->
<section>
<h2>7 · Conclusion</h2>
<div style="text-align:left;display:inline-block;margin-top:0.2em;">
<ul class="tight">
<li><b>Avon did NOT decline</b> — spend −18%, ROAS +8%, conv-rate up, AOV flat, VR ~4.2% (25–30× the cookware advertisers). The premise is a wrong-window (2024→2025) / wrong-lens (first-touch) artifact.</li>
<li><b class="navy">Avon is the control that proves the fix:</b> it had the identical holiday gate-removal as HexClad, but <b>re-gated on Jan 6</b> and recovered. Re-gating is the lever.</li>
<li><b class="navy">For any client-facing YoY, hold the attribution lens constant</b> (FT-vs-FT or LT-vs-LT) — the LT→FT migration alone manufactures a large apparent decline.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.4em;">Turn the gate back on. Avon did — and it's healthy.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML=HTML.replace("__GATE__",GATE)
(DIR/"audi_1070_avon_v4_deck.html").write_text(HTML)
print(f"wrote audi_1070_avon_v4_deck.html ({len(HTML)//1024} KB, {HTML.count('<section>')} slides)")
