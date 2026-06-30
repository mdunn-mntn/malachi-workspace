"""AUDI-1070 Avon deck (CORRECTED) — table-centric: pre/post, full YoY comparison
table, the two false assumptions with proof, the Impressions=Spend-CPM identity.
Embeds 2 charts as base64."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
YOY = b64("audi_1070_avon_yoy_no_change.png")
CURVE = b64("audi_1070_avon_roas_vs_spend.png")
WATERFALL = b64("audi_1070_avon_visits_waterfall.png")
MATRIX = b64("audi_1070_avon_attribution_matrix.png")

# YoY comparison (Jan-May, consistent last-touch). (metric, 2025, 2026, yoy, sig, kind)
# kind: vol=volume(down ok), good=outcome/rate(flat-up), cost=neutral
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
<p class="sub" style="margin-top:1.0em;"><b>Pre-period:</b> Jan–May 2025 &nbsp;&middot;&nbsp; <b>Post-period:</b> Jan–May 2026 &nbsp;&middot;&nbsp; consistent last-touch lens</p>
<p class="sub" style="margin-top:0.8em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; 2026-06-30</p>
</section>

<section>
<h2>Bottom line</h2>
<p class="claim">Performance did <span class="green">not</span> decline.<br>Spend was <span class="red">intentionally lower (−12.5%)</span> — and fully delivered.</p>
<p class="lead">Two claims under review, both <b>false</b>: &nbsp; (1) "performance is declining" — every outcome and efficiency rate is flat-to-up; &nbsp; (2) "spend stayed consistent" — spend fell −12.5%, and Avon paces ~99% to its (lower) daily budget cap. The only declines are <b>volume</b> metrics, fully explained by less budget at a higher CPM.</p>
</section>

<section>
<h2>YoY comparison — Jan–May 2025 vs 2026 (last-touch)</h2>
<table class="cmp">
<tr><th style="text-align:left">Metric</th><th>2025</th><th>2026</th><th>YoY</th><th>Significant?</th></tr>
__ROWS__
</table>
<p class="note">Significance = Welch t-test on weekly values. Only <span class="red">▼ Visits</span> (volume) and <span class="green">▲ Conversion rate</span> (efficiency) are significant; all else is statistical noise. <b>Note: last-touch lens.</b></p>
</section>

<section>
<h2>Why the client sees a big "decline": the <span class="red">attribution switch</span></h2>
<img src="__MATRIX__">
<p class="note">Same Jan–May window — only the attribution lens varies per year. Avon's reporting flipped <b>last-touch (2025) → first-touch (2026)</b>, so the client's UI compares the top-right cell (<span class="red">−76%</span>). The consistent, apples-to-apples comparisons are <b>−14% (LT)</b> or <b>−33% (FT)</b>. The −76% is the lens switch. (Even the consistent FT −33% is partly inflated by first-touch resolution worsening 36%→28%, a tracking issue — not performance.)</p>
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
<li><b>Spend fell −12.5%</b> ($73,078 → $63,967) — it did <i>not</i> stay consistent.</li>
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
<h2>Conclusion</h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b>Performance did not decline</b> — outcomes & rates flat-to-up; only the volume metrics fell.</li>
<li><b>The volume drop is spend, not quality</b> — less budget (−12.5%) × higher CPM (+4.5%) = −16% impressions, at a flat visit rate.</li>
<li><b>The client's "decline" is the reporting switch</b> — 2025 last-touch vs 2026 first-touch. On one consistent lens, Avon is flat-to-up.</li>
</ul></div>
<p class="claim" style="font-size:0.74em;margin-top:0.7em;">Avon isn't underperforming — it spent less, by design, and performed the same or better.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1100,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML = HTML.replace("__ROWS__", trows).replace("__YOY__", YOY).replace("__CURVE__", CURVE).replace("__WATERFALL__", WATERFALL).replace("__MATRIX__", MATRIX)
(DIR / "audi_1070_avon_deck.html").write_text(HTML)
print(f"wrote corrected Avon deck ({len(HTML)//1024} KB, 9 slides)")
