"""AUDI-1070 Avon deck (v2) — table-centric. Leads with MNTN's own Reporting UI
(the strongest, most credible exhibit), resolves the chart-vs-UI ROAS confusion
(mean-of-monthly vs aggregate; first-touch chart vs aggregate lens), then the full
YoY comparison, the visits waterfall, the spend-driven ROAS curve, and the score
distribution proving MM targeting did not degrade. NO attribution-switch slide:
Avon was always first-touch in its client reporting (no switch occurred)."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
UI = b64("audi_1070_avon_ui_reconciliation.png")
YOY = b64("audi_1070_avon_yoy_no_change.png")
CURVE = b64("audi_1070_avon_roas_vs_spend.png")
WATERFALL = b64("audi_1070_avon_visits_waterfall.png")
SCORE = b64("audi_1070_avon_score_distribution.png")

# YoY comparison (Jan-May, consistent last-touch). (metric, 2025, 2026, yoy, sig, kind)
ROWS = [
 ("Spend", "$73,078", "$63,967", "−12.5%", "ns", "vol"),
 ("Impressions", "6.15M", "5.15M", "−16%", "ns", "vol"),
 ("Visits", "526,929", "443,049", "−16%", "down", "vol"),
 ("Conversions", "23,962", "24,615", "+3%", "ns", "good"),
 ("Revenue", "$1.27M", "$1.32M", "+4%", "ns", "good"),
 ("ROAS", "17.3×", "20.7×", "+19%", "ns", "good"),
 ("Visit rate", "8.57%", "8.60%", "+0.4%", "ns", "good"),
 ("Conversion rate", "4.55%", "5.56%", "+22%", "up", "good"),
 ("AOV", "$52.86", "$53.73", "+2%", "ns", "good"),
 ("CPM", "$11.88", "$12.42", "+4.5%", "—", "cost"),
]
# ROAS-method reconciliation (resolves Mike's question). (method, 2025, 2026, yoy, dir)
METHODS = [
 ("Monthly average of the MoM chart <span style='color:#999'>(first-touch ratios)</span>", "8.94×", "8.74×", "−2%", "flat"),
 ("MNTN Reporting UI <span style='color:#999'>— aggregate</span>", "22.12×", "26.36×", "+19%", "up"),
 ("Our BigQuery <span style='color:#999'>— last-touch aggregate</span>", "17.3×", "20.7×", "+19%", "up"),
]
def sig_html(s):
    if s == "ns": return '<span style="color:#999">ns</span>'
    if s == "down": return '<span style="color:#D63B2F;font-weight:bold">▼ sig</span>'
    if s == "up": return '<span style="color:#2E8B57;font-weight:bold">▲ sig</span>'
    return '<span style="color:#999">—</span>'
def yoy_color(kind, yoy):
    if kind == "good": return "#2E8B57"
    if kind == "vol": return "#D63B2F"
    return "#666"
trows = "\n".join(
    f'<tr><td style="text-align:left">{m}</td><td>{a}</td><td>{b}</td>'
    f'<td style="color:{yoy_color(k,y)};font-weight:bold">{y}</td><td>{sig_html(s)}</td></tr>'
    for (m, a, b, y, s, k) in ROWS)
def dir_html(d):
    if d == "up": return '<span style="color:#2E8B57;font-weight:bold">↑ up</span>'
    return '<span style="color:#666;font-weight:bold">flat</span>'
mrows = "\n".join(
    f'<tr><td style="text-align:left">{m}</td><td>{a}</td><td>{b}</td>'
    f'<td style="color:{"#2E8B57" if d=="up" else "#666"};font-weight:bold">{y}</td><td>{dir_html(d)}</td></tr>'
    for (m, a, b, y, d) in METHODS)

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — Avon YoY</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--red:#D63B2F;--green:#2E8B57;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:#222;}
.reveal h1{margin-top:0;font-size:1.5em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.05em;color:var(--navy);margin-bottom:0.25em;}
.reveal section img{margin:0.1em 0 0;border:0;box-shadow:none;background:#FAFAFA;max-height:560px;}
.cmp{font-size:0.52em;margin:0.2em auto;border-collapse:collapse;}
.cmp th{background:var(--navy);color:#fff;padding:0.35em 0.9em;}
.cmp td{padding:0.28em 0.9em;border-bottom:1px solid #e3e3e3;text-align:right;}
.claim{font-size:1.0em;color:var(--navy);font-weight:bold;line-height:1.35;}
.sub{color:var(--tl);font-size:0.6em;} .red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.lead{font-size:0.6em;color:var(--tl);line-height:1.5;margin-top:0.5em;}
.note{font-size:0.46em;color:#999;margin-top:0.4em;}
ul.tight{font-size:0.62em;line-height:1.5;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.4em;}
.eq{font-size:0.9em;color:var(--navy);font-weight:bold;background:#eef1f6;border-radius:6px;padding:0.4em 0.7em;display:inline-block;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<h1>Avon — YoY Performance Review</h1>
<p class="sub" style="margin-top:0.6em;">Is Avon's performance declining? &nbsp;|&nbsp; AUDI-1070</p>
<p class="sub" style="margin-top:1.0em;"><b>Pre-period:</b> Jan–May 2025 &nbsp;&middot;&nbsp; <b>Post-period:</b> Jan–May 2026 &nbsp;&middot;&nbsp; multiple attribution lenses cross-checked</p>
<p class="sub" style="margin-top:0.8em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; 2026-06-30</p>
</section>

<section>
<h2>Bottom line</h2>
<p class="claim">Performance did <span class="green">not</span> decline.<br>Spend was <span class="red">intentionally lower (−12.5%)</span> — and MNTN's own UI shows <span class="green">ROAS up +19%</span>.</p>
<p class="lead">Two claims under review, both <b>false</b>: &nbsp; (1) "performance is declining" — every outcome and efficiency rate is flat-to-up, and MNTN's own Reporting UI shows ROAS, conversion rate, and CPA all <i>improved</i>; &nbsp; (2) "spend stayed consistent" — spend fell −12.5%, and Avon paces ~99% to its (lower) daily budget cap. The only declines are <b>volume</b> metrics, fully explained by less budget at a higher CPM.</p>
</section>

<section>
<h2>Don't take our word for it — <span class="green">MNTN's own Reporting UI</span></h2>
<img src="__UI__">
<p class="note">Avon Advertiser Reporting UI, Jan–May 2025 vs 2026. <b>Our BigQuery matches the UI's spend and impressions to the dollar</b> ($73,078 / $63,967; 6.15M / 5.15M imps) — so the counting is verified. Volume fell with the −12.5% budget; <b>every efficiency metric improved</b>: conversion rate +19%, CPA −15% (cheaper), ROAS +19% (22.12× → 26.36×).</p>
</section>

<section>
<h2>Why two MNTN reports show different ROAS — and <span class="green">both say "up"</span></h2>
<table class="cmp">
<tr><th style="text-align:left">How ROAS is calculated</th><th>2025</th><th>2026</th><th>YoY</th><th>Direction</th></tr>
__MROWS__
</table>
<div style="text-align:left;display:inline-block;margin-top:0.5em;">
<ul class="tight">
<li><b>Average-of-monthly ≠ aggregate.</b> Averaging 5 monthly ROAS <i>ratios</i> (the chart line, ≈8.7×) is not the same as total revenue ÷ total spend (the UI, ≈26×). For Avon, low-spend months post huge ROAS — averaging the ratios discards the spend weighting the aggregate keeps.</li>
<li><b>The chart is first-touch; the UI is the aggregate lens.</b> The MoM chart's monthly values are first-touch (its April 16.0× → 7.5× matches exactly), so it sits lower than the aggregate UI.</li>
</ul></div>
<p class="note">Three different calculations — all flat-to-up. The only one that reads as a "decline" (−2%) is the average-of-monthly-ratios, which isn't a real period ROAS. The two proper aggregates both land on <b>+19%</b>.</p>
</section>

<section>
<h2>YoY comparison — Jan–May 2025 vs 2026 (last-touch)</h2>
<table class="cmp">
<tr><th style="text-align:left">Metric</th><th>2025</th><th>2026</th><th>YoY</th><th>Significant?</th></tr>
__ROWS__
</table>
<p class="note">Significance = Welch t-test on weekly values. Only <span class="red">▼ Visits</span> (volume) and <span class="green">▲ Conversion rate</span> (efficiency) are significant; all else is statistical noise. More revenue & conversions on less spend.</p>
</section>

<section>
<h2>Claim 1: "Performance is declining" — <span class="red">FALSE</span></h2>
<img src="__YOY__">
<p class="note">Every outcome & rate flat-to-up: Revenue +4%, Conversions +3%, ROAS +19%, Visit rate flat, AOV +2%, Conversion rate +22% (significant). More revenue & conversions on less spend. None significantly down.</p>
</section>

<section>
<h2>So why are <span class="red">raw visits down −16%</span>? Spend + CPM — not quality</h2>
<img src="__WATERFALL__">
<p class="note">Visits = (Spend ÷ CPM) × Visit rate. The −16% is fully accounted for: −65,695 from −12.5% spend, −20,054 from +4.5% CPM, +1,544 from a flat visit rate. We bought less inventory at a higher price — targeting quality (visit rate) did not change.</p>
</section>

<section>
<h2>Claim 2: "Spend stayed consistent" — <span class="red">FALSE</span></h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b>Spend fell −12.5%</b> ($73,078 → $63,967) — it did <i>not</i> stay consistent. The UI confirms it to the dollar.</li>
<li>It was <b>intentional, not under-delivery</b>: Avon paces <span class="navy">~99% to its DSO daily budget cap</span> (Prod Ops' pacing report + verified query). The budget was set lower, and we delivered it.</li>
</ul></div>
<p style="margin-top:0.7em;" class="eq">Impressions Δ = Spend Δ − CPM Δ &nbsp;→&nbsp; −16% = −12.5% − 4.5%</p>
<p class="note">Fewer impressions = a smaller budget buying inventory that costs 4.5% more. Visits then track impressions at a <b>flat visit rate (+0.4%)</b> → visits −16% is "we bought less," not "we got worse."</p>
</section>

<section>
<h2>ROAS is driven by spend, not by year</h2>
<img src="__CURVE__">
<p class="note">Avon's ROAS rises as spend falls (small audience saturates). 2026 (red) sits on/above the 2024–25 curve → no year effect (spend-controlled regression: p≈0.10, not significant).</p>
</section>

<section>
<h2>And MM targeting <span class="green">did not degrade</span></h2>
<img src="__SCORE__">
<p class="note">Delivered intent (advertiser_household_score, unscored counted as 0) <b>tracks the budget and recovers</b>. High-spend months (Nov $37k) dip into lower-intent inventory, then snap back to ≥9,000 when spend normalizes (Feb–Apr 2026). Reversible mix-shift, not a one-way decline. (Scores logged from Jun 2025; May 2026's 65%-unscored spike is flagged for a separate look.)</p>
</section>

<section>
<h2>Conclusion</h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b>Performance did not decline</b> — outcomes & rates flat-to-up; only the volume metrics fell.</li>
<li><b>The volume drop is spend, not quality</b> — less budget (−12.5%) × higher CPM (+4.5%) = −16% impressions, at a flat visit rate.</li>
<li><b>MNTN's own UI confirms it</b> — ROAS +19%, conversion rate +19%, CPA −15%. The "decline" only appears if you average monthly first-touch ratios instead of aggregating.</li>
<li><b>MM targeting did not degrade</b> — delivered intent tracks the budget and recovers when spend normalizes.</li>
</ul></div>
<p class="claim" style="font-size:0.74em;margin-top:0.7em;">Avon isn't underperforming — it spent less, by design, and performed the same or better.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1100,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML = (HTML.replace("__ROWS__", trows).replace("__MROWS__", mrows)
        .replace("__UI__", UI).replace("__YOY__", YOY).replace("__CURVE__", CURVE)
        .replace("__WATERFALL__", WATERFALL).replace("__SCORE__", SCORE))
(DIR / "audi_1070_avon_deck.html").write_text(HTML)
print(f"wrote Avon deck v2 ({len(HTML)//1024} KB, 11 slides)")
