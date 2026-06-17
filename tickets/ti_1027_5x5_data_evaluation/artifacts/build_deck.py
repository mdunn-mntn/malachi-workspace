#!/usr/bin/env python3
"""Build the TI-1027 RevealJS deck (self-contained HTML, charts embedded base64, CDN reveal.js).
House config per documentation/docs/revealjs_guide.md. Author on title slide; no other names."""
import base64
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "ti_1027_presentation_deck.html"

def img(name):
    b = base64.b64encode((HERE / name).read_bytes()).decode()
    return f"data:image/png;base64,{b}"

C_LEV = img("ti_1027_chart_leverage.png")
C_VENDOR = img("ti_1027_chart_vendor_comparison.png")
C_SCORE = img("ti_1027_chart_scorecard.png")
C_VERT = img("ti_1027_chart_vertical_dependence.png")
C_TIERS = img("ti_1027_chart_score_tiers.png")
C_LAYERS = img("ti_1027_chart_layered_uniqueness.png")
C_WTP = img("ti_1027_chart_wtp_scale.png")
C_RECENCY = img("ti_1027_chart_recency.png")
C_SPEND = img("ti_1027_chart_spend_comparison.png")
C_PRICING = img("ti_1027_chart_pricing.png")
C_DEPTH = img("ti_1027_chart_depth.png")
C_PERIP = img("ti_1027_chart_perip_dist.png")
C_ADDITIVITY = img("ti_1027_chart_additivity.png")
C_NETNEW = img("ti_1027_chart_netnew_vs_free.png")

import csv as _csv
_d = sorted(_csv.DictReader(open(HERE.parent / "outputs" / "ti_1027_per_ip_depth.csv")),
            key=lambda r: -int(r["events"]))
def _m(x): return f"{int(x)/1e6:.0f}M"
def _k(x): return f"{int(x)/1e3:.0f}K"
_hdr = "".join(f"<th>{h}</th>" for h in
    ["Vendor","Events/day","IPs","Domains","IP×domain pairs","visits/IP","unique dom/IP","% pairs unique"])
_body = ""
for r in _d:
    hot = ' style="background:#FBE9EC;font-weight:bold"' if r["data_source_id"]=="25" else ""
    _body += ("<tr%s><td style='text-align:left'>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
              "<td>%s</td><td>%s</td><td>%s%%</td></tr>") % (
        hot, r["partner"], _m(r["events"]), _m(r["ips"]), _k(r["domains"]), _m(r["ip_domain_pairs"]),
        r["visits_per_ip"], r["unique_domains_per_ip"], r["pct_pairs_unique"])
RAW_TABLE = f"<table class='raw'><thead><tr>{_hdr}</tr></thead><tbody>{_body}</tbody></table>"

SLIDES = f"""
<section>
  <h1>5x5 — Data Provider Evaluation</h1>
  <h2 style="color:#C00000">Is it worth renewing?</h2>
  <p class="sub">An estimation exercise — what 5x5 brings to MNTN Matched, and how it rates vs our other partners</p>
  <p class="byline">Malachi Dunn · Targeting Infrastructure · TI-1027</p>
</section>

<section>
  <h2>The question</h2>
  <ul>
    <li>5x5's contract ends <b>end of June</b>. Renew, drop, or renegotiate?</li>
    <li>It's a flat-fee data partner feeding <b>MNTN Matched</b> — one of ~8 external + 2 internal site-visit sources.</li>
    <li>The real question: is its impact on MNTN Matched <b>outsized relative to its scale, or in line?</b></li>
  </ul>
</section>

<section>
  <h2>Bottom line</h2>
  <p class="lead"><b style="color:#C00000">KEEP (renew).</b></p>
  <p class="lead">5x5 is only <b>3.6%</b> of raw data but contributes <b>~12%</b> of unique MNTN-Matched domain signal — <b>~3.4× outsized.</b></p>
  <p class="lead">It's the <b>#2 most-unique</b> of all our data partners, concentrated in <b>B2B-audience verticals</b> (the targeting our B2B-campaign advertisers rely on), and it's <b>flat-fee</b>, so the cost doesn't scale.</p>
</section>

<section>
  <h2>How 5x5 feeds MNTN Matched</h2>
  <p class="big">IP → website visit → domain → vertical → MNTN&nbsp;Matched score</p>
  <p class="note">5x5 sends which households visited which sites. We classify those domains into industries; that signal helps score who to target. Its value is in the <b>domains</b> it sees — not new reach (we already see 74% of its households).</p>
</section>

<section>
  <img src="{C_LEV}" style="width:80%">
  <p class="take">3.6% of the raw data, ~12% of the unique usable signal — 5x5 punches ~3.4× above its weight.</p>
</section>

<section>
  <img src="{C_VENDOR}" style="width:88%">
  <p class="take">5x5 is the #2 unique-domain partner. The per-use $0.50-CPM vendors (33Across API, Sovrn, Cybba) add almost nothing unique.</p>
</section>

<section>
  <img src="{C_SPEND}" style="width:88%">
  <p class="take">On raw "touched spend" every big vendor looks the same (~$350–400K/day) — because they all see the same households. Touched volume can't tell vendors apart; only unique contribution can.</p>
</section>

<section>
  <h2>The raw numbers (per vendor, per day)</h2>
  {RAW_TABLE}
  <p class="take">33Across is the biggest feed by far (834M events) — but raw volume is mostly repeat-visits to common domains. What matters is the rightmost columns: how much we learn per household.</p>
</section>

<section>
  <img src="{C_DEPTH}" style="width:84%">
  <p class="take">Depth per IP: 33Across is huge but shallowest in unique site-visits/IP (0.65) — basically re-reporting common domains. 5x5 is mid; our own bidstream (augmentor) is deepest. Volume ≠ value.</p>
</section>

<section>
  <h2>How much does 5x5 actually add per household?</h2>
  <img src="{C_PERIP}" style="width:80%">
  <p class="take">The average (1.2) hides the shape. The median IP gets <b>+1</b> net-new domain. For ~85% of households 5x5 adds ≥1 unique domain — but it's broad &amp; shallow (only ~14% get 2+). One more unique data point per household, across ~18M households/day.</p>
</section>

<section>
  <h2>Are the vendors additive, or just sharing the same domains?</h2>
  <img src="{C_ADDITIVITY}" style="width:84%">
  <p class="take"><b>Additive.</b> 76% of all 447M IP→domain observations come from ONE vendor. For an IP seen by 5 vendors, the best single sees 6.7 sites but all 5 together see 11.1 (1.65×) — only ~29% overlap. Each vendor genuinely adds net-new visits. <b>That's the value of having them.</b></p>
</section>

<section>
  <h2>The real test: are we paying for what we get free?</h2>
  <img src="{C_NETNEW}" style="width:90%">
  <p class="take">We get site-visit data <b>free</b> from our own bidstream (augmentor) and pixel (guid). A paid vendor only earns its fee on what's net-new vs those. For 5x5: only <b>18%</b> is already free; <b>72% is net-new AND classifiable</b> — the data we'd lose with no free substitute. That's the justification to pay.</p>
</section>

<section>
  <img src="{C_SCORE}" style="width:86%">
  <p class="take">Rated on value × uniqueness × quality, with cost. The flat-fee feeds (Predactiv, 5x5) are the best deals; the redundant per-use feeds are the waste.</p>
</section>

<section>
  <img src="{C_VERT}" style="width:80%">
  <p class="take">If we drop 5x5, B2B-audience verticals lose the most fresh domain coverage — so advertisers running B2B campaigns take the hit. (These are our customers' targeting verticals, not MNTN's own mid-market-B2B acquisition target.)</p>
</section>

<section>
  <img src="{C_TIERS}" style="width:88%">
  <p class="take">Scored ≠ high-value — so we checked. 5x5's households are top-tier: 39% land in High Intent, the highest of any high-volume partner. It's not bringing low-value traffic.</p>
</section>

<section>
  <img src="{C_LAYERS}" style="width:74%">
  <p class="take">The "unique data" question, answered: only 20% of its IPs are unique, but 77% of the specific household→site visits are 5x5-only. The value is the data, not the reach.</p>
</section>

<section>
  <h2>"But don't other vendors already have it?" — No.</h2>
  <img src="{C_RECENCY}" style="width:90%">
  <p class="take">We only target the last 30 days, and vendors deliver on irregular cadences. So 5x5 is the <b>sole</b> source for 70% of its data (+1% it delivers freshest) — ~71% has no in-window substitute. 24% is a same-day tie (a copy survives if we drop 5x5); only ~5% does another vendor have fresher. Snapshot "overlap" overstated redundancy.</p>
</section>

<section>
  <img src="{C_WTP}" style="width:92%">
  <p class="take">$0.50 CPM = per 1,000 impressions. 5x5 touches ~34M impr/day → ~$6.3M/yr if billed like a CPM peer (the ceiling). Fair value ~$150–600K/yr. Place the flat fee here.</p>
</section>

<section>
  <h2>What we should pay — the definitive answer</h2>
  <img src="{C_PRICING}" style="width:96%">
  <p class="take">Anchor on a monthly rate (~$15–50K/mo) with a volume minimum. If they bill CPM, insist it's on matched impressions ($0.50 fair) — not all touched traffic. The recency finding makes the floor firmer: 70% of 5x5's data has no in-window substitute.</p>
</section>

<section>
  <h2>Is it worth the fee?</h2>
  <ul>
    <li>MNTN Matched touches <b>~$210–385M/yr</b> of media and drives a measured <b>~10–36% visit-rate lift</b>.</li>
    <li>Its value (via advertiser retention) is conservatively <b>tens of $M/yr</b>.</li>
    <li>5x5 uniquely supplies <b>~12%</b> of MM's domain signal (much more in B2B) — that clears a typical data-partner flat fee with margin.</li>
    <li><b>Keep</b> unless the fee is unusually large; then <b>renegotiate</b> (ask for full URLs, or a lower fee).</li>
  </ul>
</section>

<section>
  <h2>The counter-intuitive part</h2>
  <div class="cols">
    <div class="col">
      <h3 style="color:#375623">Flat-fee feeds = the good deals</h3>
      <p>5x5 &amp; Predactiv: most unique signal, fixed cost. The line item looks fixed, but you're getting the most non-redundant data per dollar.</p>
    </div>
    <div class="col">
      <h3 style="color:#C00000">Per-use feeds = the waste</h3>
      <p>33Across API (3% unique), Sovrn (2%), Cybba (6%) bill <b>per impression</b> for data we mostly already have. These — not 5x5 — are the cost-review targets.</p>
    </div>
  </div>
</section>

<section>
  <h2>How valuable is the untapped data?</h2>
  <table class="vmap">
    <thead><tr><th>Discarded field (feed)</th><th>On&nbsp;~%&nbsp;of&nbsp;events</th><th>Unlocks</th><th>Why it matters</th></tr></thead>
    <tbody>
      <tr><td>Page categories + keywords (33Across) · concepts/keywords (Predactiv)</td><td>~65%</td><td>Page-level content classification</td><td>We pay OpenAI to classify domains→verticals; this is richer (page-level) and ~free</td></tr>
      <tr><td>Geo — city / ZIP / DMA (Predactiv, 33Across)</td><td>~76%</td><td>Geo without MaxMind lookups</td><td>Fills the 20–25% of bids that lack geodata (known revenue gap)</td></tr>
      <tr><td>Hashed emails (Predactiv)</td><td>~60%</td><td>Identity resolution</td><td>Feeds the identity graph / CRM match</td></tr>
      <tr><td>Device / OS / user-agent (most feeds)</td><td>~99%</td><td>Device features + bot filtering</td><td>CTV vs mobile vs desktop; cleaner signal</td></tr>
      <tr><td>domain_industries (Predactiv)</td><td>—</td><td>Firmographics</td><td>B2B targeting (Q2 growth theme)</td></tr>
    </tbody>
  </table>
  <p class="take">We <b>already pay</b> for these feeds — tapping the metadata is a <b>pipeline change, not new vendor cost</b> (high ROI). 5x5/Cybba are thin (no metadata). Compliance note: GPP/GPC consent fields are also dropped.</p>
</section>

<section>
  <h2>Untapped data — where more value could come from</h2>
  <div class="cols">
    <div class="col">
      <h3 style="color:#1F3864">Ask 5x5 for more (renewal lever)</h3>
      <p>Today 5x5 sends only <b>IP · URL · timestamp</b> — no device, no user-agent, and <b>96% bare domains</b> (no path). In renewal, ask for:<br>• <b>Extended URLs / full paths</b> → page- &amp; keyword-level signal, not just domain→vertical<br>• <b>Device / user-agent</b> → CTV vs mobile vs desktop context</p>
    </div>
    <div class="col">
      <h3 style="color:#375623">Already in hand, not used (pipeline lever)</h3>
      <p>Our pixel feeds (Justuno, Sovrn, Klickly, 33Across&nbsp;API) <b>already send</b> user-agent, referrer, query params, a mobile flag — and Predactiv sends user-agent. We <b>drop all of it</b> at site_visit_signal. Tapping it = device/context features at <b>no extra vendor cost</b>.</p>
    </div>
  </div>
  <p class="note">Today we ingest only IP + URL. Both 5x5 (via renewal) and feeds we already pay for (via pipeline) hold value we're not capturing.</p>
</section>

<section>
  <h2>Recommendations</h2>
  <ol>
    <li><b>Keep 5x5</b> (and Predactiv, Justuno) — high unique value. <b>Renew ≤ ~$50K/mo ($600K/yr)</b> with a volume minimum; or $0.50 CPM on matched impressions.</li>
    <li><b>Review the redundant per-use partners</b> (33Across API, Sovrn, Cybba) for savings — a separate exercise.</li>
    <li><b>Confirm the 5x5 flat fee</b> with billing to finalize keep vs renegotiate.</li>
    <li><b>Re-rate quarterly</b> — uniqueness shifts as our own bidstream coverage grows.</li>
  </ol>
</section>

<section>
  <h2>Keep the unique flat-fee feeds. Review the redundant per-use ones.</h2>
  <p class="byline">TI-1027 · Targeting Infrastructure</p>
</section>
"""

HTML = f"""<!doctype html><html><head><meta charset="utf-8">
<title>5x5 Data Provider Evaluation (TI-1027)</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@4.6.1/dist/theme/white.css">
<style>
 .reveal {{ font-size: 32px; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color:#222; }}
 .reveal h1 {{ font-size: 1.9em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h2 {{ font-size: 1.4em; margin-top:0; color:#1F3864; text-transform:none; }}
 .reveal h3 {{ font-size: 1em; margin-top:0; text-transform:none; }}
 .reveal section.present {{ background:#FAFAFA; }}
 .reveal ul, .reveal ol {{ font-size: 0.82em; line-height:1.5; width:88%; margin:0 auto; }}
 .reveal li {{ margin-bottom:0.45em; }}
 .reveal p.sub {{ font-size:0.9em; color:#555; margin-top:0.2em; }}
 .reveal p.byline {{ font-size:0.5em; color:#888; margin-top:1.2em; }}
 .reveal p.big {{ font-size:1.0em; font-weight:bold; color:#1F3864; margin:0.2em 0; }}
 .reveal p.note {{ font-size:0.62em; color:#777; margin-top:0.8em; width:84%; margin-left:auto; margin-right:auto; }}
 .reveal p.take {{ font-size:0.58em; color:#444; margin-top:0.4em; width:88%; margin-left:auto; margin-right:auto; }}
 .reveal p.lead {{ font-size:0.95em; line-height:1.45; margin:0.3em 0; }}
 .reveal .cols {{ display:flex; gap:1.4em; width:90%; margin:0.4em auto; }}
 .reveal .col {{ flex:1; background:#fff; border:1px solid #E0E0E0; border-radius:8px; padding:0.7em 0.9em; }}
 .reveal .col p {{ font-size:0.6em; line-height:1.45; }}
 .reveal table.raw {{ font-size:0.46em; width:94%; margin:0.3em auto; border-collapse:collapse; }}
 .reveal table.raw th {{ background:#1F3864; color:#fff; padding:5px 7px; text-align:right; font-weight:bold; }}
 .reveal table.raw td {{ padding:4px 7px; text-align:right; border-bottom:1px solid #E6E6E6; }}
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
