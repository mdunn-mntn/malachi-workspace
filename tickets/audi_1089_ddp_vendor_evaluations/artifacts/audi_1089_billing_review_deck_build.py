#!/usr/bin/env python3
"""Build the AUDI-1089 billing-review RevealJS deck (CDN + self-contained standalone).
Charts base64-embedded from this folder. Reveal assets inlined from /tmp for standalone.
Reproducible: rerun -> identical files. Author on title slide; no named attributions; ranked tables."""
import base64, os

HERE = os.path.dirname(os.path.abspath(__file__))
def b64(name):
    with open(os.path.join(HERE, name), "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode()

PROOF = b64("audi_1089_billing_review_preemption_proof.png")
WATER = b64("audi_1089_billing_review_waterfall.png")

CSS = """
:root{--navy:#1B2A4A;--blue:#2E5090;--mid:#5A7DB5;--muted:#C8CDD4;--red:#D63B2F;--text:#222;--tl:#666;}
.reveal{font-size:32px;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;color:var(--text);}
.reveal h1{margin-top:0;color:var(--navy);font-size:2em;}
.reveal h2{margin-top:0;color:var(--navy);font-size:1.32em;text-transform:none;}
.reveal h3{margin-top:0;color:var(--navy);font-size:1em;}
.reveal section{text-align:left;}
.reveal .center{text-align:center;}
.reveal em{color:var(--tl);font-style:normal;}
.reveal .lead{font-size:1.05em;line-height:1.5;}
.reveal .sub{color:var(--tl);font-size:0.7em;line-height:1.4;}
.reveal .kicker{color:var(--red);font-weight:bold;font-size:0.6em;letter-spacing:.08em;text-transform:uppercase;}
.reveal table{font-size:0.5em;border-collapse:collapse;margin:0.3em auto;}
.reveal table th{background:var(--navy);color:#fff;padding:.4em .7em;text-align:left;font-weight:600;}
.reveal table td{padding:.32em .7em;border-bottom:1px solid #E4E7EB;}
.reveal table td.n,.reveal table th.n{text-align:right;}
.reveal .red{color:var(--red);font-weight:bold;}
.reveal .navy{color:var(--navy);font-weight:bold;}
.reveal .big{color:var(--red);font-weight:bold;font-size:2.6em;line-height:1;}
.reveal .bignavy{color:var(--navy);font-weight:bold;font-size:2.2em;line-height:1;}
.reveal img.chart{display:block;margin:0.2em auto;max-height:520px;background:#FAFAFA;}
.reveal ul{font-size:0.8em;line-height:1.55;}
.reveal .foot{color:#999;font-size:0.4em;margin-top:0.6em;}
.reveal code{background:#F0F2F5;padding:.05em .3em;border-radius:3px;font-size:0.85em;color:var(--navy);}
.reveal .powerline{color:var(--navy);font-weight:bold;font-size:1.35em;line-height:1.35;text-align:center;}
"""

SLIDES = f"""
<section class="center">
  <div class="kicker">AUDI-1089 · DDP Vendor Evaluation</div>
  <h1>DDP Vendor Billing Review</h1>
  <p class="lead">Do we pay vendors for signal our free logs already have?<br>
     And what should we actually pay?</p>
  <p class="sub">Malachi Dunn · Audience Intelligence · 2026-07-20 · every number is reproducible (see audit pack)</p>
</section>

<section>
  <div class="kicker">The question on the table</div>
  <h2>Does the meter already skip free-covered signal?</h2>
  <p class="lead" style="margin-top:.6em;">The assumption: <em>"the meter doesn't credit a vendor when our own
     free logs (guid, augmentor) already captured the impression — so there's nothing to recover."</em></p>
  <p class="lead navy" style="margin-top:.7em;">It does credit them. Here is the proof — and the price.</p>
  <p class="sub" style="margin-top:.8em;">Free logs = guid_log (DS23, MNTN pixel) + augmentor (DS30, bid-time). Both internal, \\$0, kept either way.</p>
</section>

<section class="center">
  <div class="kicker">Proof 1 — the billing table itself (June, verified live)</div>
  <h2>tv_cpm tracks "did a paid vendor win" — free co-presence is ignored</h2>
  <img class="chart" src="{PROOF}" alt="preemption proof">
  <p class="lead"><span class="red">269M impressions</span> where a free log co-won the exact impression —
     the paid vendor was <span class="red">still charged \\$0.50</span>, 100% of the time.</p>
  <p class="foot">dw-main-gold.reporting.ddp_mm_winners_imp_202606 · a preemptive meter would show \\$0 on the red bar.</p>
</section>

<section class="center">
  <div class="kicker">The cost of not preempting</div>
  <h2>\\$273.7K/yr is billed on data we already have for free</h2>
  <img class="chart" src="{WATER}" style="max-height:440px;" alt="preemption waterfall">
  <p class="lead">Stop billing on free-covered signal → roster <span class="navy">\\$812K → \\$539K/yr</span>
     (−33.7%), <span class="navy">and we keep every vendor's data.</span></p>
  <p class="foot">Dollars = each vendor's free-co-held share (q3c masks) × its actual June meter bill — NOT the winners-table impression counts.</p>
</section>

<section>
  <div class="kicker">Where the \\$273.7K sits</div>
  <h2>Almost all of it is the two 33Across feeds</h2>
  <table>
    <tr><th>Vendor</th><th class="n">Bill / yr</th><th class="n">Free-co-held</th><th class="n">Recoverable</th><th class="n">Bill after</th></tr>
    <tr><td>33Across</td><td class="n">\\$422.0K</td><td class="n">52.5%</td><td class="n red">\\$221.7K</td><td class="n">\\$200.4K</td></tr>
    <tr><td>33Across API</td><td class="n">\\$175.9K</td><td class="n">23.8%</td><td class="n red">\\$41.9K</td><td class="n">\\$134.0K</td></tr>
    <tr><td>Cybba</td><td class="n">\\$21.5K</td><td class="n">28.2%</td><td class="n">\\$6.1K</td><td class="n">\\$15.4K</td></tr>
    <tr><td>Justuno</td><td class="n">\\$77.1K</td><td class="n">4.9%</td><td class="n">\\$3.8K</td><td class="n">\\$73.3K</td></tr>
    <tr><td>Sovrn</td><td class="n">\\$115.9K</td><td class="n">0.2%</td><td class="n">\\$0.3K</td><td class="n">\\$115.6K</td></tr>
    <tr><td class="navy">Roster</td><td class="n navy">\\$812.4K</td><td class="n navy">33.7%</td><td class="n red">\\$273.7K</td><td class="n navy">\\$538.7K</td></tr>
  </table>
  <p class="sub">Sovrn &amp; Justuno are barely overlap-driven — preemption doesn't fix them (they need repricing, next).</p>
</section>

<section>
  <div class="kicker">How I valued them</div>
  <h2>One substrate, two independent lenses</h2>
  <ul>
    <li><span class="navy">One substrate</span> — all 10 sources (8 paid + 2 free) measured identically; the free logs are the \\$0 baseline every vendor is judged against.</li>
    <li><span class="navy">Lens A — dependency ceiling.</span> Media revenue on impressions <em>only that vendor</em> could enable × a defensible margin. Above it, a loss is guaranteed.</li>
    <li><span class="navy">Lens B — coverage/uniqueness.</span> What does the vendor add over what we already have free? Holder-mask signature per (IP×domain×date) → any keep-set's coverage, exact.</li>
    <li>Value is always read against the <span class="navy">billed</span> base (the won impression), never raw rows delivered.</li>
  </ul>
  <p class="sub">The two lenses are never added — the same impression can't be priced at media CPM and data CPM at once.</p>
</section>

<section>
  <div class="kicker">What I did</div>
  <h2>Six measured steps per source, joined to the meter</h2>
  <ul>
    <li><span class="navy">Delivery → usable survival</span> (reaches DS13 vertical or DS19 keyword) → <span class="navy">uniqueness</span> (sole / redundant / free-co-held).</li>
    <li><span class="navy">Serving → performance → dollars</span> — won impressions, visit rate vs no-data baseline, media revenue on sole serves.</li>
    <li>Joined to the meter (<code>usage_reporting_data</code>) and the BAE winners table (<code>ddp_mm_winners_imp</code>).</li>
    <li><span class="navy">33 queries + 8 self-contained "deck" queries</span>, all read-only, exact run command in each header.</li>
  </ul>
  <p class="sub">Anchors a reviewer can check: meter identity (imps×CPM = usage exactly); dropping all metered vendors recovers exactly \\$812,397/yr.</p>
</section>

<section class="center">
  <div class="kicker">Why the overlap is so large</div>
  <h2>The free logs alone cover most of the universe</h2>
  <div class="big" style="margin:.2em 0;">59.4%</div>
  <p class="lead">of the 13.3B billable (IP × domain × date) visit-days, 30 days, are already covered by
     guid + augmentor — <span class="navy">no paid vendor required</span>. (60.4% at pair grain.)</p>
  <p class="foot">deck_d1 / q3c mask histogram. This is why over half of some vendors' credited signal is redundant with free.</p>
</section>

<section>
  <div class="kicker">What the results show</div>
  <h2>Even after preemption, most bills exceed fair value</h2>
  <table>
    <tr><th>Vendor</th><th class="n">Bill after preempt</th><th class="n">Fair value (generous)</th><th class="n">Worth ÷ bill</th><th>Read</th></tr>
    <tr><td>33Across</td><td class="n">\\$200.4K</td><td class="n">\\$72–217K</td><td class="n">1.08×</td><td>defensible only at the ceiling</td></tr>
    <tr><td>33Across API</td><td class="n">\\$134.0K</td><td class="n">\\$45–134K</td><td class="n">1.00×</td><td>exactly at fair ceiling</td></tr>
    <tr><td>Sovrn</td><td class="n">\\$115.6K</td><td class="n">\\$11–34K</td><td class="n red">0.29×</td><td>~3× over</td></tr>
    <tr><td>Cybba</td><td class="n">\\$15.4K</td><td class="n">\\$1–3K</td><td class="n red">0.19×</td><td>~5× over</td></tr>
    <tr><td>Justuno</td><td class="n">\\$73.3K</td><td class="n">\\$4–11K</td><td class="n red">0.15×</td><td>~7× over</td></tr>
  </table>
  <p class="sub">Preemption fixes the double-pay; it does not make Sovrn / Cybba / Justuno worth their bill.</p>
</section>

<section>
  <div class="kicker">What we should pay</div>
  <h2>Two moves, in order</h2>
  <p class="lead"><span class="navy">1. Preempt</span> — stop billing free-covered signal: <span class="navy">−\\$273.7K/yr</span>, keep all data, needs no vendor.
     &nbsp;&nbsp;<span class="navy">2. Reprice the residual</span> to fair value:</p>
  <table>
    <tr><th>Vendor</th><th class="n">Current</th><th class="n">Fair range</th><th>Action</th></tr>
    <tr><td>33Across (DS28)</td><td class="n">\\$422.0K</td><td class="n">\\$30–100K</td><td>Renegotiate ≤ ~\\$100K or drop — biggest lever</td></tr>
    <tr><td>33Across API (DS40)</td><td class="n">\\$175.9K</td><td class="n">\\$10–40K</td><td>Drop / renegotiate (same vendor as DS28)</td></tr>
    <tr><td>Sovrn (DS33)</td><td class="n">\\$115.9K</td><td class="n">\\$0.5–2.4K</td><td class="red">Drop</td></tr>
    <tr><td>Cybba (DS36)</td><td class="n">\\$21.5K</td><td class="n">\\$1.1–4.7K</td><td class="red">Drop</td></tr>
    <tr><td>Justuno (DS24)</td><td class="n">\\$77.1K</td><td class="n">\\$14–60K</td><td>Trim the meter</td></tr>
    <tr><td>Klickly / Predactiv / 5x5</td><td class="n">flat</td><td class="n">—</td><td>Klickly drop unless ~free · Predactiv &amp; 5x5 keep (lock price)</td></tr>
  </table>
  <p class="sub">The \\$0.50 rate isn't the problem (it's below break-even) — the volume is. Sequence: lock flats → preempt → renegotiate 33Across → drop Sovrn/Cybba.</p>
</section>

<section>
  <div class="kicker">Don't trust me — run it</div>
  <h2>Audit this yourself</h2>
  <ul>
    <li><span class="navy">Fast path:</span> 8 self-contained queries <code>deck_d1..d8</code> — plain <code>bq query</code>, run as-is. Full map: <code>audi_1089_audit_map.md</code>.</li>
    <li><span class="navy">The preemption proof</span> (this deck's spine): one query on <code>ddp_mm_winners_imp_202606</code> → the \\$0.50-on-free-covered result.</li>
    <li><span class="navy">The source table</span> <code>bronze.external.targeted_signal</code> is now BQ-queryable (partitioned by <code>source_data_source_id</code>) — count vendor credit on free-covered rows directly.</li>
    <li><span class="navy">Anchors:</span> meter identity, boundary identity, mask consistency — all in <code>VALIDATION_GUIDE.md</code>.</li>
  </ul>
  <p class="sub">Caveats disclosed: N=1 valuation week (July trough) → dependency is a scenario envelope; May-2026 regime change (never mix months); flat fees pending finance.</p>
</section>

<section class="center">
  <p class="powerline">The meter pays \\$273.7K/yr for data we already own.<br>
     Preempt it, reprice the rest — and keep every signal.</p>
  <p class="sub" style="margin-top:1.2em;">Next: confirm the free-preemption rule with the meter owners · reprice the two 33Across feeds · drop Sovrn + Cybba.</p>
</section>
"""

DECK = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AUDI-1089 — DDP Vendor Billing Review</title>
__HEAD__
<style>__CSS__</style></head><body>
<div class="reveal"><div class="slides">__SLIDES__</div></div>
__SCRIPT__
<script>
Reveal.initialize({hash:true,slideNumber:'c/t',controls:true,progress:true,center:true,
  transition:'fade',transitionSpeed:'slow',width:1100,height:800,margin:0.02,minScale:0.2,maxScale:1.5});
</script></body></html>"""

deck = DECK.replace("__CSS__", CSS).replace("__SLIDES__", SLIDES)
deck = deck.replace("\\$", "$")  # HTML has no mathtext — strip the matplotlib-style $ escapes
# CDN version
cdn_head = ('<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">\n'
            '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">')
cdn_script = '<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>'
with open(os.path.join(HERE, "audi_1089_billing_review_deck.html"), "w") as f:
    f.write(deck.replace("__HEAD__", cdn_head).replace("__SCRIPT__", cdn_script))

# standalone (inline reveal assets from /tmp)
def rd(p):
    with open(p) as fh: return fh.read()
inline_head = f"<style>{rd('/tmp/reveal.css')}</style>\n<style>{rd('/tmp/white.css')}</style>"
inline_script = f"<script>{rd('/tmp/reveal.js')}</script>"
with open(os.path.join(HERE, "audi_1089_billing_review_deck_standalone.html"), "w") as f:
    f.write(deck.replace("__HEAD__", inline_head).replace("__SCRIPT__", inline_script))
print("wrote audi_1089_billing_review_deck.html + _standalone.html")
