"""AUDI-1070 — Comprehensive Avon-only deck: the full story built ground-up.
campaigns -> audience -> measurement (verified vs client UI) -> percentages -> conclusion.
Prospecting only, first-touch (industry_standard). Embeds 4 charts as base64."""
import base64, pathlib
DIR = pathlib.Path("tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts")
def b64(n): return "data:image/png;base64," + base64.b64encode((DIR / n).read_bytes()).decode()
BRIDGE = b64("audi_1070_avon_three_source_bridge.png")
EFFIC  = b64("audi_1070_avon_prospecting_efficiency.png")
MOM    = b64("audi_1070_avon_prospecting_mom.png")
SCORE  = b64("audi_1070_avon_score_distribution.png")

# Aggregate metrics (all prospecting, first-touch). (metric, 2025, 2026, yoy, kind)
AGG = [
 ("Spend", "$56,833", "$46,615", "−18.0%", "vol"),
 ("Impressions", "4,479,077", "3,344,501", "−25.3%", "vol"),
 ("Households", "1,826,270", "1,144,625", "−37.3%", "vol"),
 ("Verified Visits", "272,218", "187,200", "−31.2%", "vol"),
 ("Conversions", "10,467", "9,396", "−10.2%", "vol"),
 ("Order Value", "$533,608", "$483,446", "−9.4%", "vol"),
 ("ROAS", "9.39×", "10.37×", "+10.5%", "good"),
 ("Visit Rate", "14.91%", "16.35%", "+9.7%", "good"),
 ("Conversion Rate", "3.85%", "5.02%", "+30.5%", "good"),
 ("CPA", "$5.43", "$4.96", "−8.6%", "good"),
 ("AOV", "$50.98", "$51.45", "+0.9%", "flat"),
]
def yc(k): return {"good":"#2E8B57","vol":"#888","flat":"#888"}[k]
aggrows = "\n".join(
    f'<tr><td style="text-align:left">{m}</td><td>{a}</td><td>{b}</td>'
    f'<td style="color:{yc(k)};font-weight:bold">{y}</td></tr>' for (m,a,b,y,k) in AGG)

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>AUDI-1070 — Avon Full Story</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root{--navy:#1B2A4A;--red:#D63B2F;--green:#2E8B57;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:#222;}
.reveal h1{margin-top:0;font-size:1.5em;color:var(--navy);}
.reveal h2{margin-top:0;font-size:1.02em;color:var(--navy);margin-bottom:0.25em;}
.reveal section img{margin:0.1em 0 0;border:0;box-shadow:none;background:#FAFAFA;max-height:565px;}
.cmp{font-size:0.52em;margin:0.2em auto;border-collapse:collapse;}
.cmp th{background:var(--navy);color:#fff;padding:0.35em 0.9em;}
.cmp td{padding:0.26em 0.9em;border-bottom:1px solid #e3e3e3;text-align:right;}
.claim{font-size:1.0em;color:var(--navy);font-weight:bold;line-height:1.35;}
.sub{color:var(--tl);font-size:0.6em;} .red{color:var(--red);font-weight:bold;} .green{color:var(--green);font-weight:bold;} .navy{color:var(--navy);font-weight:bold;}
.lead{font-size:0.58em;color:var(--tl);line-height:1.5;margin-top:0.5em;}
.note{font-size:0.45em;color:#999;margin-top:0.4em;}
ul.tight{font-size:0.6em;line-height:1.5;text-align:left;display:inline-block;} ul.tight li{margin-bottom:0.35em;}
ol.road{font-size:0.62em;line-height:1.6;text-align:left;display:inline-block;} ol.road li{margin-bottom:0.5em;}
.eq{font-size:0.82em;color:var(--navy);font-weight:bold;background:#eef1f6;border-radius:6px;padding:0.35em 0.7em;display:inline-block;}
.tag{font-size:0.75em;padding:0.05em 0.5em;border-radius:4px;color:#fff;}
</style></head><body><div class="reveal"><div class="slides">

<section>
<h1>Avon — The Full Prospecting Story</h1>
<p class="sub" style="margin-top:0.6em;">Is performance declining? A ground-up audit &nbsp;|&nbsp; AUDI-1070</p>
<p class="sub" style="margin-top:1.0em;">Campaigns &rarr; audience &rarr; measurement &rarr; results. Prospecting only, first-touch (the client's lens).</p>
<p class="sub" style="margin-top:0.8em;"><b>Malachi Dunn</b> &middot; Audience Intelligence &middot; 2026-06-30</p>
</section>

<section>
<h2>Bottom line</h2>
<p class="claim">Avon spent <span class="red">18% less</span> — and got <span class="green">better at everything</span>.</p>
<p style="margin-top:0.5em;" class="sub"><b>Prospecting, first-touch, Jan–May 2025 &rarr; 2026:</b> &nbsp; Spend <span class="red">−18%</span> &nbsp;·&nbsp; ROAS <span class="green">+10%</span> &nbsp;·&nbsp; Conversion rate <span class="green">+30%</span> &nbsp;·&nbsp; CPA <span class="green">−8%</span> (cheaper)</p>
<p class="lead">There is no performance decline. A smaller budget bought less inventory at a higher CPM, so <b>volume</b> fell — but every <b>efficiency</b> metric rose. The only "down" signal is April (the one month spend doubled) and the habit of reading fewer-dollars as worse-performance. Neither survives the aggregate.</p>
</section>

<section>
<h2>How we built this</h2>
<div style="text-align:left;display:inline-block;margin-top:0.4em;">
<ol class="road">
<li><b>What Avon runs</b> — every campaign, 2 years; separate prospecting from retargeting.</li>
<li><b>What's in the prospecting audience</b> — confirm it's a proper MM campaign, break down every layer.</li>
<li><b>Measure it correctly</b> — reproduce the client's UI numbers exactly, on the same first-touch lens.</li>
<li><b>Read the percentages</b> — raw counts and rates, month-by-month and aggregate.</li>
</ol></div>
</section>

<section>
<h2>1 &middot; What Avon runs (last 2 years)</h2>
<table class="cmp">
<tr><th style="text-align:left">Group</th><th>Objective</th><th>Channel</th><th>Key campaigns</th><th>2-yr spend</th><th>In scope</th></tr>
<tr><td style="text-align:left"><b>Prospecting — Stage 1 (MM)</b></td><td>obj 1</td><td>CTV</td><td>Beeswax Television Prospecting (259556)</td><td>$256K</td><td><span class="green">✓ analyzed</span></td></tr>
<tr><td style="text-align:left">Prospecting — S2/S3 (Multi-Touch)</td><td>obj 5,6</td><td>Display</td><td>Multi-Touch, Multi-Touch&nbsp;Plus</td><td>$54K</td><td><span class="green">✓ (all-stages)</span></td></tr>
<tr><td style="text-align:left">Retargeting</td><td>obj 4</td><td>CTV+Display</td><td>6× "TV Retargeting - …"</td><td>$142K</td><td><span class="red">✗ excluded</span></td></tr>
</table>
<p class="note">Prospecting vs retargeting is separated by <b>objective_id</b> (funnel_level is mixed across both). Per the request we analyze <b>prospecting only</b>. The MM prospecting engine is <b>Stage 1 (obj=1)</b>, a CTV campaign.</p>
</section>

<section>
<h2>2 &middot; What's in it — a proper MM prospecting campaign</h2>
<table class="cmp">
<tr><th style="text-align:left">Layer</th><th style="text-align:left">Configuration</th><th>Verdict</th></tr>
<tr><td style="text-align:left">Core audience</td><td style="text-align:left"><b>MNTN Matched (DS19)</b> — ~180 intent categories</td><td><span class="green">✓ it's MM</span></td></tr>
<tr><td style="text-align:left">Reach add-on</td><td style="text-align:left">Oracle 3P (DS1) OR'd in → MM ∪ 3P (delivery 92%+ MM-scored)</td><td>note</td></tr>
<tr><td style="text-align:left">Exclusions</td><td style="text-align:left">NOT CRM (DS4) · NOT 30-day site-visitors (DS34) · NOT 30-day converters (DS21)</td><td><span class="green">✓ clean</span></td></tr>
<tr><td style="text-align:left">Geo</td><td style="text-align:left">US only (loc 237)</td><td><span class="green">✓</span></td></tr>
<tr><td style="text-align:left">Controls</td><td style="text-align:left">10% holdout · RTC scoring</td><td><span class="green">✓</span></td></tr>
</table>
<p class="note">No 3P-intersection narrowing, no geo-narrowing, no CRM-include. A textbook MM prospecting setup with correct hygiene (excludes existing customers & recent visitors). <b>No issues.</b></p>
</section>

<section>
<h2>3 &middot; Measured correctly — we reproduce the client UI exactly</h2>
<img src="__BRIDGE__">
<p class="note">The API/MoM chart looked "low" only because it's <b>prospecting-scope</b>; the UI adds TV retargeting (scope) and first-touch (industry_standard). Rebuilt from BigQuery on the client's own formula, we match the UI <b>to the dollar/visit</b>: Verified Visits <b>692,888 = 692,888</b>, ROAS 26.36 = 26.36, CPA $2.39 = $2.39. So the numbers below are the client's numbers.</p>
</section>

<section>
<h2>4 &middot; The result: −18% spend, every efficiency metric up</h2>
<img src="__EFFIC__">
<p class="note">Volume tracks the smaller budget (fewer dollars × +10% CPM). Efficiency rose across the board — conversion rate +30%, visit rate +10%, ROAS +10%, CPA −9% cheaper.</p>
</section>

<section>
<h2>The numbers — prospecting, first-touch, Jan–May</h2>
<table class="cmp">
<tr><th style="text-align:left">Metric</th><th>2025</th><th>2026</th><th>YoY</th></tr>
__AGG__
</table>
<p class="note"><span style="color:#888">Volume down</span> with the −18% budget; <span class="green">efficiency up</span>. Order value fell only −9% on an 18% smaller budget = more revenue per dollar. (Stage-1-only is near-identical: ROAS +9.8%, conv rate +31%.)</p>
</section>

<section>
<h2>Month-by-month: April is the only down month</h2>
<img src="__MOM__">
<p class="note">Where spend fell, ROAS rose. The lone drop — April — is the one month Avon <b>doubled</b> spend (+89%), so ROAS halved (diminishing returns on the same audience). This is the "down by half" that started the question. Conversion rate rose in <b>all five</b> months.</p>
</section>

<section>
<h2>And the MM audience did <span class="green">not</span> degrade</h2>
<img src="__SCORE__">
<p class="note">Delivered MM intent tracks the budget and recovers (high-spend months dip, then rebound to ≥9,000). And visit rate — the audience-quality KPI — <b>rose +10%</b>: the MM audience is finding <i>more</i> responsive users per household, not fewer. The "MM degradation" hypothesis is not supported.</p>
</section>

<section>
<h2>Conclusion</h2>
<div style="text-align:left;display:inline-block;margin-top:0.3em;">
<ul class="tight">
<li><b>Proper MM prospecting</b> — MM core, correct exclusions, US geo, holdout, RTC. No issues.</li>
<li><b>Verified measurement</b> — reproduces the client's UI to the dollar; the API "gap" was scope + first-touch.</li>
<li><b>Spent 18% less, performed better</b> — ROAS +10%, conversion rate +30%, CPA −9%; only volume fell.</li>
<li><b>MM quality improved</b> — visit rate +10%; no degradation. The lone down month (April) is doubled spend.</li>
</ul></div>
<p class="claim" style="font-size:0.72em;margin-top:0.7em;">Avon isn't underperforming — it's a smaller, sharper year.</p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({hash:true,slideNumber:true,controls:true,progress:true,center:true,transition:'fade',transitionSpeed:'slow',width:1120,height:800,margin:0.01,minScale:0.2,maxScale:1.5});</script>
</body></html>"""
HTML = (HTML.replace("__AGG__", aggrows).replace("__BRIDGE__", BRIDGE)
        .replace("__EFFIC__", EFFIC).replace("__MOM__", MOM).replace("__SCORE__", SCORE))
(DIR / "audi_1070_avon_full_deck.html").write_text(HTML)
print(f"wrote audi_1070_avon_full_deck.html ({len(HTML)//1024} KB, 11 slides)")
