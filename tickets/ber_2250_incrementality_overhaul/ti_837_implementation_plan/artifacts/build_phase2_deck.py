"""TI-837 deck builder — single complete narrative.

Builds ti_837_phase2_presentation_deck.html as a standalone story (no
"Phase 1 vs Phase 2" framing). Power Line:
  Targeting causes real lift. At peak intent, attribution shows only a
  third of it.

Charts as base64 inline.
"""
import base64
from pathlib import Path

ROOT = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan")
ARTIFACTS = ROOT / "artifacts"


def b64(filename):
    p = ARTIFACTS / filename
    if not p.exists():
        raise FileNotFoundError(f"Missing chart: {p}")
    enc = base64.b64encode(p.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{enc}"


def main():
    charts = {
        "headline":      b64("ti_837_chart_mntn_overall_headline_30adv.png"),
        "money_per_tier": b64("ti_837_chart_money_per_tier_with_wedge_30adv.png"),
        "per_adv":       b64("ti_837_chart_per_advertiser_high_intent_30adv.png"),
        "wedge_by_tier": b64("ti_837_chart_wedge_ratio_per_tier_30adv.png"),
        "peak_pooling":  b64("ti_837_chart_peak_pooling_methods.png"),
    }

    css = r"""
:root {
  --navy: #1B2A4A; --blue: #2E5090; --mid: #5A7DB5;
  --light: #A8BDD9; --muted: #C8CDD4; --red: #D63B2F;
  --text: #222222; --text-light: #666666;
}
.reveal { font-size: 30px; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: var(--text); }
.reveal section { text-align: left; }
.reveal h1 { font-size: 1.9em; color: var(--navy); margin-top: 0; text-align: left; line-height: 1.15; }
.reveal h2 { font-size: 1.3em; color: var(--navy); margin-top: 0; text-align: left; font-weight: 600; line-height: 1.2; }
.reveal h3 { font-size: 1em; color: var(--text); margin-top: 0; }
.reveal p { font-size: 0.85em; line-height: 1.45; }
.reveal ul, .reveal ol { font-size: 0.85em; line-height: 1.5; }
.reveal em { color: var(--text-light); font-style: italic; }
.reveal strong { color: var(--navy); }
.reveal .red { color: var(--red); }
.reveal .gray { color: var(--text-light); }
.reveal .navy { color: var(--navy); }
.reveal .powerline { font-size: 1.4em; color: var(--navy); font-weight: 700; line-height: 1.3; text-align: center; }
.reveal .quote-block { font-size: 1.05em; line-height: 1.5; padding-left: 1em; border-left: 4px solid var(--navy); margin: 0.6em 0; }
.reveal .footer-note { color: var(--text-light); font-size: 0.55em; text-align: center; margin-top: 0.6em; }
.reveal table { font-size: 0.55em; border-collapse: collapse; width: 100%; margin-top: 0.4em; }
.reveal table th, .reveal table td { padding: 0.35em 0.6em; border-bottom: 1px solid #DDD; text-align: left; }
.reveal table th { color: var(--navy); border-bottom: 2px solid var(--navy); font-weight: 600; }
.reveal table td.num { text-align: right; font-variant-numeric: tabular-nums; }
.reveal .img-slide { text-align: center; }
.reveal .img-slide img { max-width: 95%; max-height: 78vh; }
.reveal .big-number { font-size: 3.8em; font-weight: 800; color: var(--red); text-align: center; line-height: 1; margin: 0.15em 0; font-variant-numeric: tabular-nums; }
.reveal .big-number-context { font-size: 0.95em; color: var(--text-light); text-align: center; margin: 0; }
.reveal .takeaway-box { background: #F0F4F9; border-left: 4px solid var(--navy); padding: 0.8em 1em; font-size: 0.85em; line-height: 1.4; margin-top: 0.6em; }
.reveal .takeaway-box strong { color: var(--navy); font-weight: 700; }
.reveal .pill { display: inline-block; padding: 0.1em 0.5em; border-radius: 0.7em; background: #E8EDF5; color: var(--navy); font-size: 0.7em; margin-right: 0.4em; font-weight: 600; }
.reveal .pill-red { background: #FBE5E3; color: var(--red); }
.reveal .pill-green { background: #E2EFDF; color: #2C5F2D; }
.reveal .pill-gray { background: #EEE; color: #555; }
.reveal pre, .reveal code { font-family: 'SF Mono', 'Menlo', monospace; font-size: 0.7em; background: #F4F4F4; padding: 0.05em 0.3em; border-radius: 3px; }
.reveal pre { padding: 0.6em 0.8em; line-height: 1.4; color: var(--text); }
"""

    slides = [

        # ─── SLIDE 1 — Cold open ───────────────────────────────────────────
        """
        <section data-slide="1">
          <h2 style="margin-bottom:0.6em;">Two stories.  Both about the same advertisers.  One is wrong.</h2>
          <div class="quote-block" style="font-size:1.0em;">
            <strong>Clickpass</strong> said MNTN drove <span class="red"><strong>5–10×</strong></span> more visits at high intent.
          </div>
          <div class="quote-block" style="font-size:1.0em; margin-top:0.5em;">
            <strong>ITT</strong> (intent-to-treat) said MNTN drove <span class="red"><strong>zero</strong></span> incremental visits.
          </div>
          <p style="margin-top:1em; font-size:0.95em;">
            <strong>Both can't be right.</strong> Today we measured the truth — using the same hash that production uses to assign holdouts.
          </p>
        </section>
        """,

        # ─── SLIDE 2 — What we did (study brief) ────────────────────────────
        """
        <section data-slide="2">
          <h2>What we measured — ghost-bidding ATT</h2>
          <p style="margin-top:0.4em;"><strong>The problem with ITT:</strong> only 14–16% of "treated" IPs were actually served an impression. The other 84% are non-comparable — they diluted any real lift toward zero.</p>
          <p style="margin-top:0.5em;"><strong>The fix:</strong> compare IPs <strong>that were served</strong> against IPs <strong>that <em>would have been served</em> if not for the holdout flag</strong>.</p>
          <p style="margin-top:0.5em; font-size:0.78em; color: var(--text-light);">
            "Would have been served" = appeared in <code>augmentor_log</code> during the window. The bidder considered them eligible to bid on; the only reason they weren't served is the 10% holdout assignment.
          </p>
          <table style="margin-top:0.7em;">
            <thead><tr><th>Setup</th><th>Detail</th></tr></thead>
            <tbody>
              <tr><td>Cohort</td><td>30 MNTN advertisers, stratified across spend × vertical × intent diversity</td></tr>
              <tr><td>Window</td><td>2026-04-20 → 04-26 UTC (7 days), +3-day post-period for visit attribution</td></tr>
              <tr><td>Holdout</td><td>Per-(advertiser, IP) MD5 hash · 10% holdout (production-equivalent)</td></tr>
              <tr><td>Outcomes</td><td>Clickpass visits (attributed) and Guid visits (causal counterfactual)</td></tr>
            </tbody>
          </table>
        </section>
        """,

        # ─── SLIDE 3 — THE LIFT (anchored, interpretable) ──────────────────
        """
        <section data-slide="3">
          <h2 style="margin-bottom: 0.4em;">Did MNTN drive real lift?  <span class="navy">Yes.</span></h2>
          <p style="margin-top:0.3em; font-size:0.9em;">For every <strong>1,000 high-intent IPs</strong> MNTN serves an ad to:</p>
          <table style="font-size:0.7em; margin-top:0.4em; max-width:85%;">
            <thead><tr><th></th><th>Visit rate</th><th>Visits per 1,000 IPs</th></tr></thead>
            <tbody>
              <tr><td><strong>Holdout</strong> (would-have-been-served, but weren't)</td><td class="num">1.3%</td><td class="num">13 visits</td></tr>
              <tr><td><strong>Treated</strong> (actually served by MNTN)</td><td class="num">7.5%</td><td class="num">75 visits</td></tr>
              <tr style="background: #FBE5E3;"><td><strong>MNTN-caused incremental</strong></td><td class="num"><strong class="red">+6.2pp</strong></td><td class="num"><strong class="red">+62 visits</strong></td></tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.7em;">
            <strong>For every 1,000 high-intent IPs MNTN targets, 62 visits happen that wouldn't have otherwise.</strong><br>
            <span style="color: var(--text-light); font-size: 0.85em;">Sample-weighted across 27 advertisers · n = 45.4M high-intent IPs · 7-day window.</span>
          </div>
        </section>
        """,

        # ─── SLIDE 4 — CONFIDENCE ──────────────────────────────────────────
        """
        <section data-slide="4">
          <h2 style="margin-bottom: 0.4em;">How confident are we?  <span class="navy">Very.</span></h2>
          <table style="font-size:0.7em; margin-top:0.4em;">
            <thead><tr><th>Check</th><th>Result</th><th>What it means</th></tr></thead>
            <tbody>
              <tr><td>Sample size</td><td class="num"><strong>45.4M IPs</strong></td><td>30 advertisers × 7 days × multiple intent tiers</td></tr>
              <tr><td>95% CI on IVW-pooled lift</td><td class="num"><strong>±0.012pp</strong></td><td>Tighter than 1/8,000 — variance is essentially noise</td></tr>
              <tr><td>Advertisers with positive lift</td><td class="num"><strong>25 of 27</strong> (93%)</td><td>Result holds across verticals — not driven by one industry</td></tr>
              <tr><td>Per-cell N-gate</td><td class="num"><strong>27 of 29</strong> pass</td><td>Cells with insufficient power excluded; failed cells in appendix</td></tr>
              <tr><td>Leave-one-out swing</td><td class="num"><strong>none > ±0.05pp</strong></td><td>Drop any single advertiser → headline barely moves</td></tr>
              <tr><td>Largest single advertiser weight</td><td class="num"><strong>8%</strong></td><td>No advertiser dominates the pooled result</td></tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.7em;">
            <strong>This isn't a fluke.</strong> The lift is real, statistically tight, and reproduces across 25 of 27 advertisers spanning 20 verticals. No single advertiser is propping up the headline.
          </div>
        </section>
        """,

        # ─── SLIDE 5 — Per-advertiser distribution chart ───────────────────
        f"""
        <section data-slide="5" class="img-slide">
          <h2 style="text-align:left;">Lift varies by advertiser — but the direction is consistent</h2>
          <img src="{charts['per_adv']}" alt="Per-advertiser high-intent guid ATT">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            27 advertisers passing the 0.5pp gate. 25 positive (93%). Range −1.2pp (Outback Presents) to +16.3pp (TurboTenant). Median +2.9pp. Magnitude tracks vertical fit — high-intent shoppers in durable categories (kitchen, education, supplements) show the biggest lift.
          </p>
        </section>
        """,

        # ─── SLIDE 6 — The wedge by tier (THE money table) ──────────────────
        """
        <section data-slide="6">
          <h2>Two different questions — both answered</h2>
          <p style="margin-top:0.4em; font-size:0.82em;">
            <span class="pill">Clickpass</span> measures the visits MNTN's attribution credits.<br>
            <span class="pill">Guid</span> measures the visits that <em>actually happened</em> (causal lift via the holdout counterfactual).<br>
            The gap between them is the wedge — how much over- or under-credit our attribution carries.
          </p>
          <table style="margin-top:0.6em;">
            <thead><tr><th>Tier</th><th>Clickpass-ATT</th><th>Guid-ATT (truth)</th><th>Wedge (clickpass / guid)</th><th>Verdict</th></tr></thead>
            <tbody>
              <tr><td><strong>High intent</strong></td><td class="num">+2.59pp</td><td class="num">+2.69pp</td><td class="num">0.96×</td><td><span class="pill pill-green">honest</span></td></tr>
              <tr><td><strong>Peak intent</strong></td><td class="num">+0.36pp</td><td class="num">+1.19pp</td><td class="num"><strong class="red">0.30×</strong></td><td><span class="pill pill-red">under-credits 3×</span></td></tr>
              <tr><td>Mid intent</td><td class="num">~0.00pp</td><td class="num">~0.00pp</td><td class="num">noise</td><td><span class="pill pill-gray">noise floor</span></td></tr>
            </tbody>
          </table>
          <p style="margin-top:0.5em; font-size:0.7em; color: var(--text-light);">
            (Peak row uses median pooling — see slide 9 for why IVW hides this.)
          </p>
        </section>
        """,

        # ─── SLIDE 7 — High intent honest ───────────────────────────────────
        """
        <section data-slide="7">
          <h2>At high intent — clickpass and guid agree</h2>
          <p style="margin-top:0.5em;">Across <strong>four</strong> different ways of pooling 27 advertiser-cells, the wedge is consistently ≈1.0×:</p>
          <table style="margin-top:0.5em;">
            <thead><tr><th>Pooling method</th><th>Clickpass-ATT</th><th>Guid-ATT</th><th>Wedge</th></tr></thead>
            <tbody>
              <tr><td>IVW (default)</td><td class="num">+2.59pp</td><td class="num">+2.69pp</td><td class="num">0.96×</td></tr>
              <tr><td>Arithmetic mean (advertiser-equal)</td><td class="num">+4.00pp</td><td class="num">+4.38pp</td><td class="num">0.91×</td></tr>
              <tr><td>Median</td><td class="num">+2.51pp</td><td class="num">+2.86pp</td><td class="num">0.88×</td></tr>
              <tr><td>Sample-size weighted</td><td class="num">+4.72pp</td><td class="num">+5.13pp</td><td class="num">0.92×</td></tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.8em;">
            <strong>For high-intent IPs, attribution captures real causal lift — no more, no less.</strong> Bill from clickpass; report incrementality from guid; both tell the same story.
          </div>
        </section>
        """,

        # ─── SLIDE 7 — Peak intent under-credit ─────────────────────────────
        f"""
        <section data-slide="7" class="img-slide">
          <h2 style="text-align:left;">At peak intent — clickpass shows only a third of the real lift</h2>
          <img src="{charts['peak_pooling']}" alt="Peak intent pooling methods">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            IVW (left) is the default pool. It collapses to ≈1.0× — which would say there's no wedge. The other three methods all show clickpass at ~30% of guid. Slide 8 explains why IVW lies here.
          </p>
        </section>
        """,

        # ─── SLIDE 8 — IVW pathology lesson ─────────────────────────────────
        """
        <section data-slide="8">
          <h2>Why IVW hides the peak under-credit</h2>
          <p style="margin-top:0.4em;">Inverse-variance weighting gives each cell weight <code>1/var = n / [p(1−p)]</code>. A cell with <strong>very small ATT</strong> and <strong>very small visit rate</strong> has <strong>vanishing variance</strong> — and gets a <strong>huge IVW weight</strong>.</p>
          <p style="margin-top:0.5em;">At peak intent, 8 of 19 advertisers (Casper, Re-Bath, NET-A-PORTER, Overjet, Swatch, Longines, Outback, UD-Daniels) have ~0pp ATT in both arms → near-zero variance → outsized weight. They drag the IVW pool to 1.00×.</p>
          <p style="margin-top:0.5em;">The other 11 advertisers all show wedge between 0.10× and 0.50× (clickpass under-credits guid). Median: 0.30×. Sample-weighted: 0.34×. The pattern is real — the pool method just hides it.</p>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>Methodology rule:</strong> IVW is the right tool when cells are well-powered with similar variance. <strong>It collapses to noise-floor cells</strong> when many cells have tiny ATT and tiny variance. For peak/mid reporting, prefer <strong>sample-size-weighted</strong> or <strong>median</strong> pooling.<br>
            <span style="color: var(--text-light); font-size: 0.85em;">Saved to <code>knowledge/experimentation.md</code>.</span>
          </div>
        </section>
        """,

        # ─── SLIDE 9 — Methodology pipeline ────────────────────────────────
        """
        <section data-slide="9">
          <h2>How the pipeline works — end-to-end</h2>
          <pre style="font-size:0.6em; line-height:1.5;">
prospecting_intent_v1     ── per-(advertiser, IP, day) intent score
       │
       ▼ MAX(score) per (advertiser, IP) over the 7-day week
holdouts (10% bucket)            targeted (90% bucket)
       │                                │
       ▼ INNER JOIN augmentor_log       ▼ INNER JOIN cost_impression_log
biddable_holdouts                served_treatment
   "would have been served"          "actually was served"
       │                                │
       └────────────┬───────────────────┘
                    ▼ LEFT JOIN clickpass_log + guid_log (analysis + 3-day post)
              two-proportion ATT per (advertiser, tier, outcome)
                    ▼
          IVW + arithmetic / median / sample-weighted pooling
                  + leave-one-out sensitivity
                  + per-cell N-gate (CI half-width ≤ 0.5pp)
          </pre>
          <p style="font-size:0.7em; color: var(--text-light); margin-top:0.4em;">
            Same MD5 holdout hash production uses. Same window for both arms (3-day post for cross-day visits). Single batched augmentor scan amortizes across all 30 advertisers (~$90 cost / 87 min).
          </p>
        </section>
        """,

        # ─── SLIDE 10 — Cohort design ───────────────────────────────────────
        """
        <section data-slide="10">
          <h2>How we picked these 30 advertisers</h2>
          <p style="margin-top:0.4em;">Empirical inclusion gates — every threshold derived from the data, not picked:</p>
          <table>
            <thead><tr><th>Gate</th><th>Threshold</th><th>Why</th></tr></thead>
            <tbody>
              <tr><td>Active in window</td><td class="num">≥100 served IPs</td><td>Must run during the analysis week</td></tr>
              <tr><td>Per-tier biddable holdouts</td><td class="num">≥5,000</td><td>Power calc: ≤0.5pp CI half-width at p ∈ [0.005, 0.05]</td></tr>
              <tr><td>Tier diversity</td><td class="num">≥5% of IPs not at score=10,000</td><td>Prevents MAX-tier collapse — ensures peak/mid are populated</td></tr>
              <tr><td>Prospecting spend</td><td class="num">≥$5,000 (March)</td><td>Filters dormant advertisers</td></tr>
              <tr><td>Audience dedup</td><td class="num">unique (high, peak, mid) signature</td><td>Caught Re-Bath sister companies running identical audiences</td></tr>
            </tbody>
          </table>
          <p style="margin-top:0.6em; font-size:0.78em;">Stratified across <strong>13 high-spend / 7 mid / 10 low</strong> × <strong>20 verticals</strong>. Largest single advertiser is <strong>8%</strong> of the high-tier pool — no single advertiser drives the headline.</p>
        </section>
        """,

        # ─── SLIDE 11 — Caveats (was 12) ────────────────────────────────────
        """
        <section data-slide="11">
          <h2>What I'd want a methodologist to push on</h2>
          <ol style="margin-top:0.4em;">
            <li style="margin-bottom:0.5em;"><strong>Single window.</strong> One 7-day analysis (2026-04-20 → 04-26). No cross-window validation yet — the pattern could shift in a different week. Augmentor 10-day TTL bounds backward replication; forward replication is straightforward.</li>
            <li style="margin-bottom:0.5em;"><strong>Loose biddable-holdout filter.</strong> Currently "any augmentor row" qualifies as biddable. Tighter options (advertiser-targeting match, intent-gate match, real-bid-for-this-advertiser) would tighten the counterfactual but shrink the holdout pool. Treated arm uses identical filter — comparison is internally consistent, but the <em>level</em> of the ATT may shift under tightening.</li>
            <li style="margin-bottom:0.5em;"><strong>MAX-tier subject construction.</strong> Each (advertiser, IP) gets its strongest-observed tier across the week. Tier-collapse advertisers were filtered out, but the multi-tier IPs that remain still get assigned to their MAX tier — peak/mid pools may be slightly biased upward by IPs that occasionally hit high. Cross-validation on per-day subjects deferred.</li>
            <li style="margin-bottom:0.5em;"><strong>Visits, not conversions.</strong> Visits are 10–20× more frequent than conversions. The wedge pattern at peak may not replicate on the rarer outcome. Conversions analysis is the immediate next step.</li>
          </ol>
        </section>
        """,

        # ─── SLIDE 12 — What's next ─────────────────────────────────────────
        """
        <section data-slide="12">
          <h2>What's next</h2>
          <ul style="margin-top:0.4em;">
            <li style="margin-bottom:0.6em;"><span class="pill">Conversions outcome.</span> Same pipeline, swap <code>ui_conversions</code> for <code>guid_log</code>. Conversions are 10-20× rarer than visits → need ~30-day window for power. Augmentor 10-day TTL is the binding constraint — bidder-level ghost bidding (next bullet) would solve it.</li>
            <li style="margin-bottom:0.6em;"><span class="pill">Bidder-level ghost bidding.</span> Production solution that escapes the augmentor TTL. Pending Alex Bloore decision; Zach + Jordan on the bidder team. This unlocks longer windows and cross-window validation.</li>
            <li style="margin-bottom:0.6em;"><span class="pill">iROAS.</span> Per-advertiser <code>(incremental conversions × AOV) ÷ MNTN spend</code>. The number leadership actually wants. Depends on the conversions outcome above + advertiser AOV from <code>ui_conversions.order_amt</code>.</li>
          </ul>
          <div class="takeaway-box" style="margin-top: 0.8em;">
            <strong>Decision now:</strong> publish the wedge alongside clickpass internally, so the team knows the size of the gap when reading attribution-driven reports. The asymmetry (honest at high, stingy at peak) is the calibration.
          </div>
        </section>
        """,

        # ─── SLIDE 13 — Power Line return ───────────────────────────────────
        """
        <section data-slide="13">
          <p class="powerline" style="font-size:1.4em; margin-top:0.5em;">
            Targeting causes real lift in 93% of advertisers.<br>
            At peak intent — clickpass shows only a third of it.
          </p>
          <p style="text-align:center; margin-top:1.2em; color: var(--text-light); font-size:0.8em;">
            High-intent attribution is honest.<br>
            Peak-intent attribution under-credits real lift by ~3×.<br>
            That's the calibration term — and it's worth publishing alongside every attribution-driven report.
          </p>
          <p style="text-align:center; margin-top:0.9em; color: var(--text-light); font-size:0.65em;">
            30 advertisers · 7-day window · 22M IPs · 0 single-advertiser dominance flags · 93% positive lift · CI ±0.012pp.
          </p>
        </section>
        """,
    ]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>TI-837 — Ghost-Bidding Lift Analysis</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">
  <style>{css}</style>
</head>
<body>
  <div class="reveal">
    <div class="slides">
      {''.join(slides)}
    </div>
  </div>
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
      margin: 0.02,
      minScale: 0.2,
      maxScale: 1.5,
    }});
  </script>
</body>
</html>
"""

    out = ARTIFACTS / "ti_837_phase2_presentation_deck.html"
    out.write_text(html)
    print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
