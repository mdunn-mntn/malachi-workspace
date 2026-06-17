#!/usr/bin/env python3
"""TI-1027 EXEC deck — pared down for leadership: bottom line, why, what to pay, untapped upside.
Self-contained RevealJS HTML, charts embedded base64. Author on title slide; no other names."""
import base64
from pathlib import Path
HERE = Path(__file__).resolve().parent
OUT = HERE / "ti_1027_exec_deck.html"

def img(name):
    return f"data:image/png;base64,{base64.b64encode((HERE / name).read_bytes()).decode()}"

C_NETNEW = img("ti_1027_chart_netnew_vs_free.png")
C_PRICING = img("ti_1027_chart_pricing.png")
C_RECENCY = img("ti_1027_chart_recency.png")

SLIDES = f"""
<section>
  <h1>5x5 Data (DS&nbsp;25) — Renewal Decision</h1>
  <h2 style="color:#C00000">Is the data worth what we pay?</h2>
  <p class="sub">A flat-fee data partner feeding MNTN Matched · contract up end of June</p>
  <p class="byline">Malachi Dunn · Targeting Infrastructure · TI-1027</p>
</section>

<section>
  <h2>Bottom line</h2>
  <p class="lead"><b style="color:#C00000">KEEP — renew.</b></p>
  <ul>
    <li><b>Outsized:</b> 3.6% of our raw site-visit data, but <b>~3.4×</b> that in unique value to MNTN Matched.</li>
    <li><b>Not redundant:</b> <b>72%</b> of its data is net-new vs our own <i>free</i> bidstream + pixel — and ~71% has no in-window substitute from any vendor.</li>
    <li><b>Strategic:</b> its unique coverage is concentrated in <b>B2B-audience</b> verticals.</li>
  </ul>
  <p class="lead">Fair price: <b>~$15–50K/mo</b> (anchor ~$25–30K) with a volume minimum. Worth paying anything below the walk-away ceiling.</p>
</section>

<section>
  <h2>Why it's worth it — we're not paying for what we get free</h2>
  <img src="{C_NETNEW}" style="width:88%">
  <p class="take">We already get site-visit data free from our own bidstream (augmentor) + pixel (guid). 5x5 only earns its fee on net-new — and <b>72% is net-new AND classifiable</b>, with no free substitute.</p>
</section>

<section>
  <h2>And it's irreplaceable within the window we target</h2>
  <img src="{C_RECENCY}" style="width:90%">
  <p class="take">Targeting uses the last 30 days; vendors deliver irregularly. <b>~71%</b> of 5x5's household→site pairs are the sole or freshest source in that window — only ~5% does another vendor have fresher.</p>
</section>

<section>
  <h2>What we should pay — the definitive answer</h2>
  <img src="{C_PRICING}" style="width:96%">
  <p class="take">Anchor on a monthly rate (~$15–50K/mo) + a volume minimum. If billed CPM, insist it's on <b>matched</b> impressions ($0.50 fair) — not all touched traffic.</p>
</section>

<section>
  <h2>How valuable is the untapped data?</h2>
  <table class="vmap">
    <thead><tr><th>Discarded field (feed)</th><th>On&nbsp;~%&nbsp;of&nbsp;events</th><th>Unlocks</th><th>Why it matters</th></tr></thead>
    <tbody>
      <tr><td>Page categories + keywords (33Across) · concepts (Predactiv)</td><td>~65%</td><td>Page-level content classification</td><td>We pay OpenAI to classify domains→verticals; this is richer &amp; ~free</td></tr>
      <tr><td>Geo — city / ZIP / DMA (Predactiv, 33Across)</td><td>~76%</td><td>Geo without MaxMind lookups</td><td>Fills the 20–25% of bids that lack geodata</td></tr>
      <tr><td>Hashed emails (Predactiv)</td><td>~60%</td><td>Identity resolution</td><td>Feeds the identity graph / CRM match</td></tr>
      <tr><td>Device / OS / user-agent (most feeds)</td><td>~99%</td><td>Device features + bot filtering</td><td>CTV vs mobile vs desktop</td></tr>
    </tbody>
  </table>
  <p class="take">We <b>already pay</b> for these feeds — tapping it is a <b>pipeline change, not new vendor cost</b>. (5x5/Cybba are thin.) Compliance note: GPP/GPC consent fields are also dropped.</p>
</section>

<section>
  <h2>Untapped upside — more value is on the table</h2>
  <div class="cols">
    <div class="col">
      <h3 style="color:#1F3864">Ask 5x5 for more (renewal lever)</h3>
      <p>5x5 sends only <b>IP · URL · timestamp</b> — no device/user-agent, and 96% bare domains. In renewal, ask for <b>extended URLs</b> (page/keyword-level signal) and <b>device/user-agent</b>. Raises value per dollar.</p>
    </div>
    <div class="col">
      <h3 style="color:#375623">Already in hand, not used (pipeline lever)</h3>
      <p>Our pixel feeds + Predactiv <b>already send</b> user-agent, referrer, query params, mobile flag — we drop them at ingestion. Tapping that = device/context features at <b>no extra vendor cost</b>.</p>
    </div>
  </div>
  <p class="note"><b>Next step:</b> confirm the 5x5 flat fee with billing → place it on the scale above. Renewal asks: extended URLs + device.</p>
</section>

<section>
  <h2>Keep 5x5 — it's outsized, net-new, and irreplaceable in-window.</h2>
  <p class="byline">TI-1027 · Targeting Infrastructure</p>
</section>

<section>
  <h2>Appendix — how the pay numbers are derived</h2>
  <ul>
    <li><b>Two inputs:</b> market rate <b>$0.50 / 1,000 impressions</b> (peer rate; our logs show ~$0.001/impr data cost ≈ $1 CPM) · 5x5 touches <b>34.35M impr/day</b>, of which <b>213.5K/day</b> go to households only it saw.</li>
    <li><b>Ceiling ~$525K/mo:</b> 34.35M × $0.50 ÷ 1,000 × 365 = $6.3M/yr — peer rate on ALL touched impressions (hard upper bound; 74% of its IPs we already see).</li>
    <li><b>Floor ~$3K/mo:</b> 213.5K × $0.50 ÷ 1,000 × 365 = $40K/yr — only impressions to households 5x5 uniquely brought.</li>
    <li><b>Fair $15–50K/mo:</b> ~12% of MM's domain signal × MM's $210–385M/yr touched media, cross-checked vs typical DDP flat fees (5x5 = #2 most-unique → upper end).</li>
    <li><b>Volume mins:</b> ≥2.5B rows/mo (lock delivery) + ≥25M unique IP×domain pairs/day (anti-padding — 93M events collapse to ~33M distinct).</li>
    <li><b>CPM caveat:</b> $0.50 only on MATCHED impressions; on ALL touched it's $0.02–0.05 — $0.50 on all-touched = the $6.3M trap.</li>
  </ul>
  <p class="note">Floor &amp; ceiling are measured; the fair point is an estimate bounded by them.</p>
</section>
"""

HTML = f"""<!doctype html><html><head><meta charset="utf-8">
<title>5x5 Renewal Decision — Exec Summary (TI-1027)</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/theme/white.css">
<style>
 .reveal {{ font-size: 32px; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color:#222; }}
 .reveal h1 {{ font-size: 1.9em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h2 {{ font-size: 1.4em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h3 {{ font-size: 1em; margin-top:0; text-transform:none; }}
 .reveal section.present {{ background:#FAFAFA; }}
 .reveal ul {{ font-size: 0.86em; line-height:1.5; width:90%; margin:0.3em auto; }}
 .reveal li {{ margin-bottom:0.5em; }}
 .reveal p.sub {{ font-size:0.9em; color:#555; margin-top:0.2em; }}
 .reveal p.byline {{ font-size:0.5em; color:#888; margin-top:1.2em; }}
 .reveal p.lead {{ font-size:0.95em; line-height:1.45; margin:0.3em 0; }}
 .reveal p.note {{ font-size:0.62em; color:#555; margin-top:0.8em; width:88%; margin-left:auto; margin-right:auto; }}
 .reveal p.take {{ font-size:0.58em; color:#444; margin-top:0.4em; width:90%; margin-left:auto; margin-right:auto; }}
 .reveal .cols {{ display:flex; gap:1.4em; width:92%; margin:0.4em auto; }}
 .reveal .col {{ flex:1; background:#fff; border:1px solid #E0E0E0; border-radius:8px; padding:0.7em 0.9em; }}
 .reveal .col p {{ font-size:0.6em; line-height:1.45; }}
 .reveal table.vmap {{ font-size:0.5em; width:96%; margin:0.3em auto; border-collapse:collapse; }}
 .reveal table.vmap th {{ background:#1F3864; color:#fff; padding:6px 8px; text-align:left; }}
 .reveal table.vmap td {{ padding:6px 8px; text-align:left; border-bottom:1px solid #E6E6E6; vertical-align:top; }}
</style></head><body>
<div class="reveal"><div class="slides">{SLIDES}</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/reveal.js"></script>
<script>
Reveal.initialize({{ hash:true, slideNumber:true, controls:true, progress:true, center:true,
  transition:'fade', transitionSpeed:'slow', width:1100, height:800, margin:0.01, minScale:0.2, maxScale:1.5 }});
</script></body></html>"""
OUT.write_text(HTML)
print(f"Wrote {OUT} ({len(HTML)//1024} KB)")
