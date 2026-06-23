"""TI-1044 — internal FACTS deck (plain numbers, no persuasion).
Structure: campaign facts -> what was reported -> what we measured (truth) -> how they differ
-> caveats -> what to do. Reads charts from artifacts/.
"""
import base64, pathlib
BASE = pathlib.Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ti_1044_elevenlabs_ctv_incrementality")
def img(n): return "data:image/png;base64,"+base64.b64encode((BASE/"artifacts"/n).read_bytes()).decode()
LIFT, POWER = img("ti_1044_chart_lift_measurement.png"), img("ti_1044_chart_power_contrast.png")
NAVY,RED,MINT,GRAY="#1f3a5f","#c0392b","#16a085","#777"

def th(*c): return "".join(f'<th style="padding:0.35em 0.6em;text-align:left;border-bottom:2px solid #999;">{x}</th>' for x in c)
def td(*c): return "".join(f'<td style="padding:0.3em 0.6em;border-bottom:1px solid #ddd;">{x}</td>' for x in c)

slides = f"""
<section style="text-align:left;">
  <h2 style="font-size:0.7em;color:#1a1a1a;">TI-1044 — ElevenLabs CTV incrementality</h2>
  <p style="font-size:0.5em;color:#444;">Internal findings · facts &amp; numbers · not a customer deck</p>
  <p style="font-size:0.42em;color:#999;margin-top:1em;">Audience Intelligence · Malachi Dunn · 2026-06-23</p>
  <p style="font-size:0.4em;color:#888;margin-top:1.5em;">Question: ElevenLabs' vendor review says CTV drove no incremental conversions. Is that true,
  and what do our own numbers say?</p>
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.55em;">1 · The campaign (facts)</h3>
  <table style="font-size:0.44em;border-collapse:collapse;width:100%;">
    {''.join(f'<tr>{td(k,v)}</tr>' for k,v in [
      ('Advertiser','ElevenLabs — AID 51660 (B2B AI, US)'),
      ('Channel','CTV via Beeswax (channel_id 8); prospecting, ~100% (objective 1)'),
      ('National scale','~2026-05-07 (deck says May 17); ~5-week post window'),
      ('Spend','~$1.0–1.5M / month (advertiser-billed CTV)'),
      ('Baseline visit rate','~3.07% (per advertised IP)'),
      ('Baseline CVR','~0.062% (per advertised IP) — very low, typical B2B'),
      ('Conversion windows','30-day click + 30-day view-through (over-credits)'),
    ])}
  </table>
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.55em;">2 · What was REPORTED (attribution)</h3>
  <ul style="font-size:0.46em;line-height:1.6;">
    <li>Platform dashboards credit healthy CSF + new-subscriber volume to CTV.</li>
    <li>Our MNTN-attributed (clickpass) visit-rate holdout lift = <strong>+143%</strong> (p&lt;0.001).</li>
    <li>Attribution = "saw a CTV ad, then converted → CTV gets credit." Correlational.</li>
  </ul>
  <p style="font-size:0.42em;color:{GRAY};">Reported performance looks strong. That is attribution, not incrementality.</p>
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.55em;">3 · What ELEVENLABS found (their geo test)</h3>
  <ul style="font-size:0.46em;line-height:1.6;">
    <li>Methods: Synthetic Control + Time-Based Regression + Diff-in-Diff; country (US vs intl) + state (48 vs GA/IL/OH go-dark).</li>
    <li>Country · new subscribers: <strong>−0.0%, p=0.81</strong> (R²=0.87, well-powered).</li>
    <li>Contact Sales Forms: −104 cumulative (p=0.062); DiD −12.5% vs intl.</li>
    <li>State go-dark holdout: +3.8% to +7.4% but all <strong>p&gt;0.10</strong>, R²&lt;0.70 (underpowered).</li>
    <li>Conclusion: <strong>no statistically significant incremental lift.</strong></li>
  </ul>
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.52em;">4 · What WE measured — household ghost-ad holdout</h3>
  <p style="font-size:0.4em;color:{GRAY};">New bidder ghost logs (bid_price_log, ghostBid) · clean ITT (targeted vs held-out) · Jun 13–22 · 6.6M households · same method as TI-837 / TI-933</p>
  <table style="font-size:0.42em;border-collapse:collapse;width:100%;margin-top:0.3em;">
    <tr>{th('Metric','Held-out','Targeted','Lift (ITT)','95% CI','p','')}</tr>
    <tr>{td('Attributed visits (clickpass)','0.66%','1.60%',f'<b style=color:{GRAY}>+143%</b>','+135…+151%','&lt;0.001','attribution')}</tr>
    <tr>{td('Total site traffic (guid)','2.10%','2.10%',f'<b style=color:{NAVY}>+0%</b>','−2…+2%','0.84','n.s.')}</tr>
    <tr>{td('Conversions (CVR)','0.0461%','0.0455%',f'<b style=color:{RED}>−1%</b>','−13…+12%','0.84','n.s.')}</tr>
  </table>
  <p style="font-size:0.4em;color:#444;margin-top:0.4em;">ATT (served-only vs held-out) is higher — visits +36%, conversions +35% — but that is <b>win-selection</b>
  (we win impressions for higher-value households who visit/convert more anyway). With a ~57% win rate, a real +36% would
  show ~+21% in the ITT; we see +0% → true causal lift is small. A clean ATT needs ghost-<i>wins</i> (not yet logged).</p>
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.55em;">4b · The same picture, visually</h3>
  <img src="{LIFT}" style="width:70%;border:none;box-shadow:none;background:none;">
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.55em;">5 · Why conversions can't be measured here (power)</h3>
  <ul style="font-size:0.46em;line-height:1.6;">
    <li>At CVR 0.062%, detecting a <b>5% (relative)</b> lift needs ~<b>$1.83M</b>/mo (80% power, 10% holdout).</li>
    <li>At ~$1M spend the smallest detectable CVR lift is ~<b>7%</b> — larger than any realistic CTV effect.</li>
    <li>Visits are well-powered: ~$36K detects a 5% lift (MDE ~1%).</li>
  </ul>
  <img src="{POWER}" style="width:46%;border:none;box-shadow:none;background:none;">
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.55em;">6 · How reported vs truth differ</h3>
  <table style="font-size:0.44em;border-collapse:collapse;width:100%;">
    <tr>{th('','Reported (attribution)','Truth (incrementality)')}</tr>
    <tr>{td('Visits','+143% (clickpass)','≈0 incremental total traffic (ITT); ≤+36% unverified')}</tr>
    <tr>{td('Conversions','credited to CTV','≈0 (our ITT −1%, their geo ~0)')}</tr>
    <tr>{td('Why','an ad served → visit/convert gets credited','held-out households visit/convert at the same rate')}</tr>
  </table>
  <p style="font-size:0.44em;color:#444;margin-top:0.5em;">Two independent methods — their geo test and our household holdout — agree: <b>no detectable incremental conversion lift.</b>
  Attribution re-credits demand that would have happened anyway.</p>
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.55em;">7 · Caveats / limitations</h3>
  <ul style="font-size:0.44em;line-height:1.55;">
    <li><b>No ghost-wins yet</b> → can't form a clean served-counterfactual (TOT); we report ITT (clean of selection) + ATT (selection-confounded).</li>
    <li>ITT is diluted by the 57% win rate — but that argues the true effect is small, not large.</li>
    <li>Holdout window = 10 days (bid_price_log TTL); ghost logging live 2026-05-27, no backfill.</li>
    <li>Held-out households may carry pre–May-27 ad exposure (slight downward bias on lift).</li>
    <li>guid = total visits to ElevenLabs' site only (advertiser_id 51660).</li>
  </ul>
</section>

<section style="text-align:left;">
  <h3 style="font-size:0.55em;">8 · What to do going forward</h3>
  <ol style="font-size:0.46em;line-height:1.6;">
    <li>Stand up the <b>ghost-win simulation</b> (win-rate sampling) → clean visit &amp; conversion TOT.</li>
    <li>For low-CVR / B2B advertisers, measure <b>visits</b>, not conversions; don't sanction conversion
        incrementality tests below the MDE spend (~$2M for ElevenLabs).</li>
    <li>ElevenLabs specifically: conversion incrementality is <b>unmeasurable</b> at current spend;
        visit incrementality is ≈0 (clean) / unverified-positive (ATT). Set expectations accordingly.</li>
    <li>Reuse this pipeline (bid_price_log ghostBid → ITT) for the next incrementality reads.</li>
  </ol>
</section>
"""

html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>TI-1044 ElevenLabs incrementality — internal facts</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
 .reveal {{ font-size: 30px; font-family:'Helvetica Neue',Helvetica,Arial,sans-serif; }}
 .reveal h2,.reveal h3 {{ margin:0 0 0.4em 0; color:#1a1a1a; font-weight:700; text-transform:none; }}
 .reveal table {{ margin:0; }} .reveal td,.reveal th {{ vertical-align:top; }}
 .reveal section img {{ border:none; box-shadow:none; background:none; }}
 .reveal ul,.reveal ol {{ margin-left:1em; }}
</style></head><body>
<div class="reveal"><div class="slides">{slides}</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({{ center:false, hash:true, width:1180, height:840, margin:0.05, transition:'none' }});</script>
</body></html>"""

out = BASE/"artifacts"/"ti_1044_internal_facts_deck.html"
out.write_text(html); print(f"facts deck -> {out} ({len(html)//1024}KB)")
