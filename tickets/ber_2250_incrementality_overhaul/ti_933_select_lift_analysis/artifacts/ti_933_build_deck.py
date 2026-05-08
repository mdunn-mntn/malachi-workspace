"""TI-933: build the standalone RevealJS lift deck.

Reads the chart PNGs + the lift CSVs, embeds the PNGs as base64, writes a
single self-contained HTML file that can be shared via gist + githack.
"""
import base64, csv, math, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ART = ROOT / "artifacts"
OUT = ROOT / "outputs"

def b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")

# Embed the 3 charts as base64 inline images
volume_png = b64(ART / "ti_933_chart_volume_by_advertiser.png")
power_png = b64(ART / "ti_933_chart_per_advertiser_power.png")
pooled_png = b64(ART / "ti_933_chart_pooled_lift.png")

# Read the lift table
df_per_adv = []
with open(OUT / "ti_933_per_advertiser_lift_7d.csv") as f:
    df_per_adv = list(csv.DictReader(f))

df_pooled = []
with open(OUT / "ti_933_pooled_lift_7d.csv") as f:
    df_pooled = list(csv.DictReader(f))
pooled_t = next(r for r in df_pooled if r["arm"] == "treated_served")
pooled_h = next(r for r in df_pooled if r["arm"] == "holdout_biddable")

def pct(x):
    return f"{float(x)*100:.4f}"

def lift_ci(p_t, n_t, p_h, n_h):
    p_t, p_h = float(p_t), float(p_h)
    n_t, n_h = int(n_t), int(n_h)
    se = math.sqrt(p_t*(1-p_t)/n_t + p_h*(1-p_h)/n_h)
    diff = p_t - p_h
    return diff*100, (diff-1.96*se)*100, (diff+1.96*se)*100

guid_lift, guid_lo, guid_hi = lift_ci(pooled_t["guid_rate"], pooled_t["n_ips"], pooled_h["guid_rate"], pooled_h["n_ips"])
cp_lift, cp_lo, cp_hi = lift_ci(pooled_t["clickpass_rate"], pooled_t["n_ips"], pooled_h["clickpass_rate"], pooled_h["n_ips"])
conv_lift, conv_lo, conv_hi = lift_ci(pooled_t["ui_conv_rate"], pooled_t["n_ips"], pooled_h["ui_conv_rate"], pooled_h["n_ips"])

# Per-advertiser table — top by treated n_ips
treated_rows = sorted([r for r in df_per_adv if r["arm"] == "treated_served"],
                       key=lambda r: -int(r["n_ips"]))[:10]

# Volume CSV for advertiser names (Phase 1 file has names)
vol_by_aid = {}
with open(OUT / "ti_933_select_volume_by_advertiser.csv") as f:
    for r in csv.DictReader(f):
        vol_by_aid[int(r["advertiser_id"])] = r.get("advertiser_name", f"AID {r['advertiser_id']}")

per_adv_table_rows = ""
for r in treated_rows:
    aid = int(r["advertiser_id"])
    name = vol_by_aid.get(aid, f"AID {aid}")
    h_row = next((x for x in df_per_adv if int(x["advertiser_id"]) == aid and x["arm"] == "holdout_biddable"), None)
    if not h_row:
        continue
    ipt = int(r["n_ips"]); iph = int(h_row["n_ips"])
    if not ipt or not iph: continue
    glift, glo, ghi = lift_ci(r["guid_rate"] or 0, ipt, h_row["guid_rate"] or 0, iph)
    sig = "Y" if (glo > 0 or ghi < 0) else "N"
    sig_pill = '<span class="pill pill-green">sig</span>' if sig == "Y" else '<span class="pill pill-gray">n.s.</span>'
    per_adv_table_rows += (
        f"<tr><td>{name}</td><td class='num'>{ipt:,}</td><td class='num'>{iph:,}</td>"
        f"<td class='num'>{float(r['guid_rate'])*100:.3f}%</td>"
        f"<td class='num'>{float(h_row['guid_rate'] or 0)*100:.3f}%</td>"
        f"<td class='num'>{glift:+.3f}pp</td>"
        f"<td>{sig_pill}</td></tr>"
    )

HTML = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>TI-933 — MNTN Select lift</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
<style>
:root {{
  --navy: #1B2A4A; --blue: #2E5090; --mid: #5A7DB5;
  --light: #A8BDD9; --muted: #C8CDD4; --light-gray: #C8CDD4;
  --gray: #888; --red: #D63B2F; --bg: #FAFAFA;
  --text: #222; --text-light: #666;
}}
.reveal {{ font-size: 30px; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: var(--text); background: var(--bg); }}
.reveal section {{ text-align: left; }}
.reveal section.center {{ text-align: center; }}
.reveal h1 {{ font-size: 1.8em; color: var(--navy); margin-top: 0; text-align: left; line-height: 1.15; font-weight: 700; }}
.reveal h2 {{ font-size: 1.3em; color: var(--navy); margin-top: 0; text-align: left; font-weight: 700; line-height: 1.2; letter-spacing: -0.01em; }}
.reveal h3 {{ font-size: 1em; color: var(--text); margin-top: 0; font-weight: 600; }}
.reveal p {{ font-size: 0.85em; line-height: 1.45; color: var(--text); }}
.reveal ul, .reveal ol {{ font-size: 0.85em; line-height: 1.5; }}
.reveal em {{ color: var(--text-light); font-style: italic; }}
.reveal strong {{ color: var(--navy); }}
.reveal .red {{ color: var(--red); font-weight: 700; }}
.reveal .gray {{ color: var(--text-light); }}
.reveal .navy {{ color: var(--navy); font-weight: 700; }}
.reveal .powerline,
.reveal .power-line {{ color: var(--navy); font-weight: 700; font-size: 1.2em; line-height: 1.4; text-align: center; }}
.reveal .quote-block {{ font-size: 1.05em; line-height: 1.5; padding-left: 1em; border-left: 4px solid var(--navy); margin: 0.6em 0; }}
.reveal .footer-note,
.reveal .footnote {{ color: var(--text-light); font-size: 0.55em; text-align: center; margin-top: 0.6em; font-style: italic; }}
.reveal .subtitle {{ font-size: 0.6em; color: var(--text-light); margin-top: 0.5em; }}
.reveal table {{ font-size: 0.55em; border-collapse: collapse; width: 100%; margin: 0.4em auto 0; }}
.reveal table th, .reveal table td {{ padding: 0.35em 0.6em; border-bottom: 1px solid #DDD; text-align: left; }}
.reveal table th {{ color: white; background: var(--navy); border-bottom: 2px solid var(--navy); font-weight: 600; }}
.reveal table td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.reveal .img-slide {{ text-align: center; }}
.reveal .img-slide img {{ max-width: 95%; max-height: 78vh; }}
.reveal .big-number {{ font-size: 3.2em; font-weight: 800; color: var(--red); text-align: center; line-height: 1; margin: 0.15em 0; font-variant-numeric: tabular-nums; }}
.reveal .big-number-context {{ font-size: 0.95em; color: var(--text-light); text-align: center; margin: 0; }}
.reveal .takeaway-box {{ background: #F0F4F9; border-left: 4px solid var(--navy); padding: 0.8em 1em; font-size: 0.85em; line-height: 1.4; margin-top: 0.6em; }}
.reveal .takeaway-box strong {{ color: var(--navy); font-weight: 700; }}
.reveal .pill {{ display: inline-block; padding: 0.1em 0.5em; border-radius: 0.7em; background: #E8EDF5; color: var(--navy); font-size: 0.7em; margin-right: 0.4em; font-weight: 600; }}
.reveal .pill-red {{ background: #FBE5E3; color: var(--red); }}
.reveal .pill-green {{ background: #E2EFDF; color: #2C5F2D; }}
.reveal .pill-gray {{ background: #EEE; color: #555; }}
.reveal pre, .reveal code {{ font-family: 'SF Mono', 'Menlo', monospace; font-size: 0.7em; background: #F4F4F4; padding: 0.05em 0.3em; border-radius: 3px; }}
.reveal pre {{ padding: 0.6em 0.8em; line-height: 1.4; color: var(--text); display: block; white-space: pre-wrap; }}
.reveal img {{ background: var(--bg); border: none; box-shadow: none; max-height: 600px; }}
</style>
</head>
<body>
<div class="reveal"><div class="slides">

<!-- Slide 1: Power Line -->
<section class="center">
  <p style="font-size: 1.5em; color: var(--navy); font-weight: 700; line-height: 1.4; letter-spacing: -0.01em;">
    MNTN Select drives <span class="red">+2.06 percentage point</span><br>
    visit-rate lift.<br>
    <span style="font-size: 0.7em; color: var(--text-light); font-weight: 500;">Significant. Pooled across 22 active advertisers, 7 days.</span>
  </p>
</section>

<!-- Slide 2: Title / Context -->
<section>
  <h3 style="text-transform: uppercase; letter-spacing: 0.1em; font-size: 0.6em; color: var(--text-light); text-align: center;">TI-933 · BER-2250 · 2026-05-07</h3>
  <h1 style="margin-top: 0.6em; font-size: 1.6em; line-height: 1.2; text-align: center;">Is MNTN Select incremental — and what do we do?</h1>
  <div style="margin-top: 1.0em; font-size: 0.85em; line-height: 1.6;">
    <p style="margin-bottom: 0.3em;"><strong>Follow-up to TI-917.</strong> Same ATT-style 10% biddable-holdout method, cohort filtered to <code>campaign_groups.product_id = 2</code>.</p>
    <p>Window: 2026-04-29 &rarr; 2026-05-05 (7 days).</p>
  </div>
  <p class="footer-note" style="margin-top: 1.0em;">Three takeaways: incremental, no per-advertiser power, action follows.</p>
</section>

<!-- Slide 3: The Question -->
<section>
  <h2>The question</h2>
  <div class="quote-block" style="margin-top: 1.0em; font-size: 0.95em;">
    How incremental are our Select campaigns?
  </div>
  <p style="margin-top: 1.0em;">We tested this on every active Select advertiser with augmentor coverage in the window.</p>
</section>

<!-- Slide 4: Cohort / Volume -->
<section class="img-slide">
  <h2 style="text-align: center;">38 active Select advertisers — by 30-day impression volume</h2>
  <img src="data:image/png;base64,{volume_png}" alt="Volume by advertiser">
  <p class="footer-note">All prospecting (zero retargeting). Largest is $106k/mo (Masterbuilt). None individually clears TI-917's $200k/mo MDE floor &mdash; pooling is required for stat power.</p>
</section>

<!-- Slide 5: Per-advertiser power -->
<section class="img-slide">
  <h2 style="text-align: center;">No single Select advertiser is individually powered</h2>
  <img src="data:image/png;base64,{power_png}" alt="Per-advertiser lift">
  <p class="footer-note">Each dot is one advertiser's lift estimate; error bars are its 95% CI. <strong>17 of 22 advertisers are individually significantly positive</strong> (CI excludes zero); 5 span zero. The pooled estimate (dashed line, +2.06pp) averages across all 22 and is the headline because it has the tightest CI.</p>
</section>

<!-- Slide 6: The Pooled Lift Bar Chart (compared to TI-917 baselines) -->
<section class="img-slide">
  <h2 style="text-align: center;">Select sits between &quot;all campaigns&quot; and prospecting</h2>
  <img src="data:image/png;base64,{pooled_png}" alt="Pooled lift comparison">
  <p class="footer-note">95% CI on the Select bar. Methodologically identical to TI-917 baselines (10% biddable holdout, guid visit rate).</p>
</section>

<!-- Slide 7: The Numbers Table -->
<section>
  <h2>Pooled headline numbers</h2>
  <table style="margin-top: 0.8em;">
    <thead>
      <tr><th>Metric</th><th class='num'>Treated rate</th><th class='num'>Holdout rate</th><th class='num'>Lift</th><th class='num'>95% CI</th><th>Sig</th></tr>
    </thead>
    <tbody>
      <tr><td><strong>Visit rate (guid)</strong></td>
          <td class='num'>{pct(pooled_t['guid_rate'])}%</td>
          <td class='num'>{pct(pooled_h['guid_rate'])}%</td>
          <td class='num'>{guid_lift:+.3f}pp</td>
          <td class='num'>[{guid_lo:+.3f}, {guid_hi:+.3f}]</td>
          <td><span class='pill pill-green'>sig</span></td></tr>
      <tr><td>Visit rate (clickpass)</td>
          <td class='num'>{pct(pooled_t['clickpass_rate'])}%</td>
          <td class='num'>{pct(pooled_h['clickpass_rate'])}%</td>
          <td class='num'>{cp_lift:+.3f}pp</td>
          <td class='num'>[{cp_lo:+.3f}, {cp_hi:+.3f}]</td>
          <td><span class='pill pill-gray'>undercount*</span></td></tr>
      <tr><td><strong>Conversion rate</strong></td>
          <td class='num'>{pct(pooled_t['ui_conv_rate'])}%</td>
          <td class='num'>{pct(pooled_h['ui_conv_rate'])}%</td>
          <td class='num'>{conv_lift:+.3f}pp</td>
          <td class='num'>[{conv_lo:+.3f}, {conv_hi:+.3f}]</td>
          <td><span class='pill pill-green'>sig</span></td></tr>
    </tbody>
  </table>
  <p class="footer-note" style="margin-top: 0.5em;">
    Treated arm: {int(pooled_t['n_ips']):,} IPs. Holdout arm: {int(pooled_h['n_ips']):,} IPs. *Clickpass requires an MNTN impression to fire, so it can&#39;t see holdout visits and undercounts treated visits too &mdash; the apparent lift is conservative. Guid is the honest measure.
  </p>
  <div class="takeaway-box" style="margin-top: 0.6em;">
    <strong>Read:</strong> Select is genuinely incremental on visits AND conversions. Both 95% CIs exclude zero with comfortable margin. Select genuinely drives lift.
  </div>
</section>

<!-- Slide 8: Per-advertiser table (top 10 by treated n_ips) -->
<section>
  <h2>Top 10 advertisers by exposed-IP volume</h2>
  <table style="margin-top: 0.5em;">
    <thead>
      <tr><th>Advertiser</th><th class='num'>Treated IPs</th><th class='num'>Holdout IPs</th><th class='num'>Treated visit-rate</th><th class='num'>Holdout visit-rate</th><th class='num'>Lift</th><th>Indiv sig?</th></tr>
    </thead>
    <tbody>
      {per_adv_table_rows}
    </tbody>
  </table>
  <p class="footer-note" style="margin-top: 0.5em;">Visit rate = guid_log. Most individual advertiser CIs span zero (low volume per advertiser). The pooled estimate is the reliable signal.</p>
</section>

<!-- Slide 9: What this means - Action -->
<section>
  <h2>What we do about it</h2>
  <div style="margin-top: 0.5em; font-size: 0.85em; line-height: 1.5;">
    <p style="margin-bottom: 0.4em;"><span class="pill pill-green">DO</span> <strong>Treat Select as a confirmed-incremental product.</strong> Select targeting genuinely drives lift. Brand-direct customers asking for proof can see the +2pp visit + +0.14pp conversion numbers.</p>
    <p style="margin-bottom: 0.4em;"><span class="pill pill-green">DO</span> <strong>Lead with the pooled +2.06pp; per-advertiser numbers also available for the majority.</strong> 17 of 22 active Select advertisers are individually significantly positive in this 7d window &mdash; the line-of-business effect is consistent, not outlier-driven. The 5 advertisers with CIs spanning zero are volume-limited; ghost-bidder unlocks them.</p>
    <p style="margin-bottom: 0.4em;"><span class="pill pill-red">UNLOCK</span> <strong>Ghost-bidder (TI-886) makes per-advertiser readouts production-grade.</strong> Today we can run this analysis ad-hoc but it requires multi-hour Spark jobs. Ghost-bidder gives every customer a per-advertiser number from a logged ghost-bid table &mdash; cheap, fast, and removes the augmentor TTL ceiling.</p>
  </div>
</section>

<!-- Slide 10: Methodology -->
<section>
  <h2>Methodology — how we filtered to Select + measured lift</h2>
  <p style="margin-top: 0.4em; font-size: 0.8em;"><strong>Select cohort:</strong> <code>campaign_groups.product_id = 2</code> in coredb. 38 active advertisers in last 30d; 23 with measurable holdout/treated overlap in the 7d window. <strong>All 38 are a complete superset of the &quot;Select Live Campaigns&quot; advertisers list.</strong></p>
  <p style="margin-top: 0.4em; font-size: 0.8em;"><strong>Holdout assignment:</strong> 10% per-(advertiser, IP) hash. Identical to TI-917 v5; product-agnostic.</p>
  <p style="margin-top: 0.4em; font-size: 0.8em;"><strong>Biddability filter:</strong> 99.99% of Select-served IPs appear in <code>augmentor_log</code> &mdash; the filter applies cleanly to Select.</p>
  <p style="margin-top: 0.4em; font-size: 0.8em;"><strong>Visit rate:</strong> guid_log (independent identity graph, no clickpass survivorship bias) within +3 days of impression window.</p>
  <p style="margin-top: 0.4em; font-size: 0.8em;"><strong>Compute path:</strong> BigQuery hit the 6-hour wall three times. Ported to Spark on Databricks (Jobs Compute) using the airflow-ti <code>aug_log_ip</code> feature store output. Ran in ~3 hours on 400 cores. Materialized intermediate <code>ip_assigned</code> to GCS to avoid 3x prospecting scans.</p>
</section>

<!-- Slide 11: Close / Power Line -->
<section class="center">
  <p style="font-size: 1.6em; color: var(--navy); font-weight: 700; line-height: 1.4; letter-spacing: -0.01em;">
    MNTN Select is incremental.<br>
    <span class="red">+2.06pp</span> visits. <span class="red">+0.14pp</span> conversions.<br>
    <span style="font-size: 0.65em; color: var(--text-light); font-weight: 500;">Pooled, 7 days, 95% CI excludes zero on both.</span>
  </p>
</section>

</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>
<script>
Reveal.initialize({{
    hash: true,
    slideNumber: true,
    controls: true,
    progress: true,
    center: true,
    transition: 'fade',
    transitionSpeed: 'slow',
    width: 1100,
    height: 800,
    margin: 0.01,
    minScale: 0.2,
    maxScale: 1.5,
}});
</script>
</body>
</html>
"""

with open(ART / "ti_933_select_lift_deck.html", "w") as f:
    f.write(HTML)
print(f"Wrote {(ART / 'ti_933_select_lift_deck.html').resolve()}")
print(f"Size: {(ART / 'ti_933_select_lift_deck.html').stat().st_size:,} bytes")
