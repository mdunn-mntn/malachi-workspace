"""Build the ElevenLabs-facing response deck (RevealJS, self-contained, base64 charts)."""
import base64, pathlib
BASE = pathlib.Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ti_1044_elevenlabs_ctv_incrementality")
def img(name):
    b = base64.b64encode((BASE/"artifacts"/name).read_bytes()).decode()
    return f"data:image/png;base64,{b}"
POWER, CURVE, TREND = img("ti_1044_chart_power_contrast.png"), img("ti_1044_chart_mde_curve.png"), img("ti_1044_chart_visit_vs_cvr.png")
CONV = img("ti_1044_chart_conv_att_vs_itt.png")
LIFT = img("ti_1044_chart_lift_measurement.png")

NAVY, RED, MINT = "#1f3a5f", "#c0392b", "#16a085"

slides = f"""
<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.5em;">US CTV Campaign · Measurement Review</p>
  <h1 style="font-size:1.5em;line-height:1.15;">Is CTV driving<br>incremental conversions?</h1>
  <p style="font-size:0.6em;color:#555;">What we found on our side — and what we'd measure next</p>
  <p style="color:#999;font-size:0.42em;margin-top:1.2em;">MNTN · Audience Intelligence · Malachi Dunn · June 2026</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">The short answer</p>
  <h2 style="font-size:1.05em;line-height:1.25;">We ran your numbers and reach<br>the same conclusion — with one<br>critical nuance.</h2>
  <p style="font-size:0.62em;color:#444;margin-top:0.6em;">The campaign isn't <em>proven</em> to have no effect.<br>
  The <strong style="color:{RED};">test was underpowered</strong> — by design, a 0.062% conversion rate is<br>nearly impossible to measure.</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.42em;">The 0.062% problem</p>
  <img src="{POWER}" style="width:78%;border:none;box-shadow:none;background:none;">
  <p class="footer-note">Lewis-Rao power analysis · 80% power · 10% holdout · MNTN data</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">What a conversion test costs</p>
  <h1 style="font-size:2.4em;color:{RED};margin:0.1em 0;">$1.83M</h1>
  <p style="font-size:0.7em;color:#444;">to detect even a 5% conversion lift at 80% power.</p>
  <p style="font-size:0.6em;color:#555;margin-top:0.8em;">You invested <strong>~$1M</strong>. At that spend the test can only<br>
  see a <strong style="color:{RED};">~7% lift or larger</strong> — bigger than any realistic CTV effect.</p>
  <p style="font-size:0.48em;color:#999;margin-top:0.7em;">(A visit-rate test, by contrast, resolves a ~1% lift for ~$36K — but conversions are the metric in question.)</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">Your own analysis already shows it</p>
  <h2 style="font-size:0.9em;">Where the test was powered, the answer is ~0.</h2>
  <div style="display:flex;justify-content:center;gap:1.5em;margin-top:0.8em;font-size:0.62em;">
    <div style="background:#eef3f8;padding:0.8em 1.1em;border-radius:8px;width:38%;">
      <div style="color:{MINT};font-weight:bold;font-size:1.4em;">~0%</div>
      <div style="color:#333;">Country · new subscribers</div>
      <div style="color:#888;font-size:0.85em;">p=0.81 · R²=0.87 — <strong>well-powered, trustworthy</strong></div>
    </div>
    <div style="background:#f7eef0;padding:0.8em 1.1em;border-radius:8px;width:38%;">
      <div style="color:{RED};font-weight:bold;font-size:1.4em;">+7.4%</div>
      <div style="color:#333;">3-state go-dark holdout</div>
      <div style="color:#888;font-size:0.85em;">p&gt;0.10 · R²&lt;0.70 — <strong>underpowered, noise</strong></div>
    </div>
  </div>
  <p style="font-size:0.55em;color:#666;margin-top:0.8em;">The positive signs appear only where the test was too small to trust.</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.42em;">What IS measurable</p>
  <img src="{CURVE}" style="width:72%;border:none;box-shadow:none;background:none;">
  <p class="footer-note">At ~$1M, visits resolve a ~1% lift; conversions only a ~7% lift</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.42em;">The pattern in your data</p>
  <img src="{TREND}" style="width:66%;border:none;box-shadow:none;background:none;">
  <p class="footer-note">Spend scaled ~10× · attributed visits responded · conversions stayed flat &amp; invisible</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">We ran your test, on our side</p>
  <h2 style="font-size:0.78em;">Our household ghost-ad holdout: visits lift, conversions don't.</h2>
  <img src="{LIFT}" style="width:74%;border:none;box-shadow:none;background:none;">
  <p class="footer-note">New bidder ghost-ad holdout · same method as our prior lift studies · clean ITT · 6.6M households · ~10 days</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">The measured lift</p>
  <table style="margin:0.7em auto;border-collapse:collapse;font-size:0.52em;width:92%;">
    <tr style="background:{NAVY};color:#fff;">
      <th style="padding:0.5em 0.6em;text-align:left;">Metric</th>
      <th style="padding:0.5em 0.6em;">Held-out</th><th style="padding:0.5em 0.6em;">Reached</th>
      <th style="padding:0.5em 0.6em;">Lift</th><th style="padding:0.5em 0.6em;">95% CI</th>
      <th style="padding:0.5em 0.6em;">p</th><th style="padding:0.5em 0.6em;">Verdict</th></tr>
    <tr style="background:#eef6f3;">
      <td style="padding:0.5em 0.6em;text-align:left;"><strong>Visit rate (IVR)</strong></td>
      <td style="padding:0.5em 0.6em;text-align:center;">0.65%</td><td style="padding:0.5em 0.6em;text-align:center;">1.59%</td>
      <td style="padding:0.5em 0.6em;text-align:center;color:{MINT};"><strong>+143%</strong></td>
      <td style="padding:0.5em 0.6em;text-align:center;">+136% … +151%</td>
      <td style="padding:0.5em 0.6em;text-align:center;">&lt;0.001</td>
      <td style="padding:0.5em 0.6em;text-align:center;color:{MINT};">✅ significant</td></tr>
    <tr style="background:#f9eef0;">
      <td style="padding:0.5em 0.6em;text-align:left;"><strong>Conversion rate (CVR)</strong></td>
      <td style="padding:0.5em 0.6em;text-align:center;">0.0459%</td><td style="padding:0.5em 0.6em;text-align:center;">0.0452%</td>
      <td style="padding:0.5em 0.6em;text-align:center;color:{RED};"><strong>−2%</strong></td>
      <td style="padding:0.5em 0.6em;text-align:center;">−13% … +11%</td>
      <td style="padding:0.5em 0.6em;text-align:center;">0.79</td>
      <td style="padding:0.5em 0.6em;text-align:center;color:{RED};">✕ no lift</td></tr>
  </table>
  <p style="font-size:0.5em;color:#555;">IVR is well-powered and clearly significant — <strong>the media is working.</strong> CVR is indistinguishable from zero — the 0.062% B2B conversion base rate is the wall, not the media.</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">Why the conversion number is trustworthy</p>
  <h2 style="font-size:0.8em;">We removed win-selection. The 'lift' was an artifact.</h2>
  <img src="{CONV}" style="width:58%;border:none;box-shadow:none;background:none;">
  <p class="footer-note">A naive served-vs-ghost read shows +35% — but that's serving your highest-value households; the clean ITT is ≈0.</p>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">What our holdout shows</p>
  <div style="font-size:0.55em;text-align:left;max-width:84%;margin:0.4em auto;line-height:1.7;">
    <p>📈 <strong style="color:{MINT};">Visits (IVR): +143%, highly significant.</strong> MNTN is driving real, incremental site visits — the media is working.</p>
    <p>🧱 <strong style="color:{RED};">Conversions (CVR): −2%, not significant.</strong> The 0.062% B2B conversion base rate is the wall — that's the product/funnel, not the media.</p>
    <p>🤝 <strong>Two independent methods</strong> — your geo test and our household holdout — land on the same conversion answer.</p>
    <p style="color:#555;">Incrementality testing on conversions isn't the right lens here until the conversion rate is high enough to power it.</p>
  </div>
</section>

<section>
  <h2 style="font-size:0.85em;">Your four questions</h2>
  <div style="font-size:0.5em;text-align:left;max-width:88%;margin:0.4em auto;line-height:1.45;">
    <p><strong style="color:{NAVY};">1 · Reach &amp; overlap.</strong> ~100% of your CTV reach is <strong>prospecting</strong>; retargeting ≈ 0. We only "see" deep-funnel via pixel <strong>site-visitors</strong> — we can <strong>block</strong> those, but can't measure true funnel depth.</p>
    <p><strong style="color:{NAVY};">2 · Incrementality our side.</strong> <strong>Done</strong> — our household ghost-ad holdout (prior slide) lands at <strong>≈0 conversion lift</strong>, independently confirming your geo result.</p>
    <p><strong style="color:{NAVY};">3 · Conversion windows.</strong> 30-day <strong>view-through</strong> + click + conversion windows. View-through over-credits CTV — which is <em>why</em> attribution looks strong while topline is flat.</p>
    <p><strong style="color:{NAVY};">4 · Creative &amp; targeting.</strong> High-intent geo (SF/TX/FL) worked; the national broad scale <strong>diluted</strong> it. Audience is broad 3rd-party. We have no incrementality-trained model yet, so audience changes are <strong>exploratory</strong>, not guaranteed lift.</p>
  </div>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">What we'd measure next</p>
  <div style="font-size:0.56em;text-align:left;max-width:80%;margin:0.5em auto;line-height:1.8;">
    <p>① <strong>Measure visits, not conversions</strong> — a 1% lift is detectable for ~$36K.</p>
    <p>② Or run a <strong>larger/longer geo test</strong> sized to the ~$2M conversion MDE.</p>
    <p>③ <strong>Re-concentrate</strong> budget on the high-intent geos that originally worked.</p>
    <p>④ <strong>Reset attribution expectations</strong> — 30-day view-through over-credits.</p>
  </div>
</section>

<section>
  <p style="text-transform:uppercase;letter-spacing:3px;color:#888;font-size:0.45em;">The takeaway</p>
  <h2 style="font-size:1.0em;line-height:1.3;color:{NAVY};">"No detectable lift" isn't "no lift."<br>It's an underpowered test.</h2>
  <p style="font-size:0.62em;color:#444;margin-top:0.7em;">We can detect a <strong style="color:{MINT};">1% visit lift for $36K.</strong><br>Let's measure what's measurable.</p>
</section>
"""

html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>ElevenLabs CTV Incrementality — MNTN Response</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
  .reveal {{ font-size: 32px; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }}
  .reveal h1, .reveal h2, .reveal h3 {{ margin-top: 0; text-transform: none; font-weight: 700; color:#1a1a1a; }}
  .reveal section img {{ border: none; box-shadow: none; background: none; }}
  .footer-note {{ text-align: center; font-size: 0.4em; color: #AAA; margin-top: 0.5em; }}
  .reveal p {{ margin: 0.3em 0; }}
</style></head><body>
<div class="reveal"><div class="slides">{slides}</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>Reveal.initialize({{ center: true, hash: true, width: 1100, height: 800,
  margin: 0.06, transition: 'fade' }});</script>
</body></html>"""

out = BASE/"artifacts"/"ti_1044_elevenlabs_response_deck.html"
out.write_text(html)
print(f"deck written: {out} ({len(html)//1024}KB)")
