"""TI-837 deck builder — v5 multi-segment results.

Power Line:
  Retargeting drives the lift. Pure prospecting drives almost none.
  Combined views hide both stories.

Charts use v5 data (4-segment: all / prospecting all stages / Stage 1 only / retargeting).
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
        "headline":      b64("ti_837_chart_segment_headline_v5.png"),
        "by_tier":       b64("ti_837_chart_segment_x_tier_v5.png"),
        "wedge":         b64("ti_837_chart_segment_wedge_v5.png"),
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
.reveal .big-number { font-size: 3.2em; font-weight: 800; color: var(--red); text-align: center; line-height: 1; margin: 0.15em 0; font-variant-numeric: tabular-nums; }
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

        # ─── SLIDE 1 — POWER LINE + story (Disruption) ─────────────────────
        """
        <section data-slide="1">
          <p class="powerline" style="font-size:1.7em; margin-top:1em; line-height:1.25;">
            Pure prospecting drives near-zero incremental lift.
          </p>
          <p style="text-align:center; margin-top:1.4em; color: var(--text-light); font-size:0.95em; line-height:1.5;">
            Same 30 advertisers, same 7 days, four ways of slicing it.<br>
            The combined <strong>+3.12pp</strong> headline tells none of the four stories.
          </p>
          <p class="footer-note" style="margin-top:1.6em;">
            v5 multi-segment ghost-bidding ATT · 2026-04-20 → 04-26 · 30 advertisers
          </p>
        </section>
        """,

        # ─── SLIDE 2 — THE HEADLINE CHART (was slide 3) ─────────────────────
        f"""
        <section data-slide="2" class="img-slide">
          <h2 style="text-align:left;">Retargeting carries the lift. Three of four segments don't.</h2>
          <img src="{charts['headline']}" alt="High-intent guid-ATT by segment">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            High-intent guid-ATT across 4 segments, IVW pool (solid) and sample-weighted (hatched). Retargeting: +21pp guid lift. Stage 1 only: ≈ 0. The "all campaigns" combined view (+3.12pp) is dominated by retargeting being mixed in.
          </p>
        </section>
        """,

        # ─── SLIDE 3 — 4-BAR CSS CHART (replaces old slide 4 numbers table) ─
        """
        <section data-slide="3">
          <h2>The 4-segment ATT, in proportion</h2>
          <p style="color:var(--text-light); font-size:0.78em;">High-intent guid-ATT, IVW-pooled. Retargeting bar = 90% of slide width; others scaled relative.</p>
          <div style="font-size:0.78em; max-width:90%; margin:0 auto; padding-right:1em;">
            <div style="display:flex; align-items:center; margin-bottom:0.55em;">
              <div style="width:9em; flex-shrink:0; font-weight:600; color:var(--navy);">Retargeting only</div>
              <div style="flex:1; position:relative; height:2em; background:#F4F4F4;">
                <div style="position:absolute; left:0; top:0; height:100%; width:90%; background:var(--red); display:flex; align-items:center; padding-left:0.6em; color:white; font-weight:700; font-variant-numeric:tabular-nums;">+21.07pp</div>
              </div>
            </div>
            <div style="display:flex; align-items:center; margin-bottom:0.55em;">
              <div style="width:9em; flex-shrink:0; color:var(--text-light);">All campaigns</div>
              <div style="flex:1; position:relative; height:2em; background:#F4F4F4;">
                <div style="position:absolute; left:0; top:0; height:100%; width:13.3%; background:var(--mid); display:flex; align-items:center; padding-left:0.6em; color:white; font-variant-numeric:tabular-nums;">+3.12pp</div>
              </div>
            </div>
            <div style="display:flex; align-items:center; margin-bottom:0.55em;">
              <div style="width:9em; flex-shrink:0; color:var(--text-light);">Prospecting (1+2+3)</div>
              <div style="flex:1; position:relative; height:2em; background:#F4F4F4;">
                <div style="position:absolute; left:0; top:0; height:100%; width:3.3%; background:var(--mid);"></div>
                <div style="position:absolute; left:4%; top:0; height:100%; display:flex; align-items:center; color:var(--text); font-variant-numeric:tabular-nums;">+0.78pp</div>
              </div>
            </div>
            <div style="display:flex; align-items:center; margin-bottom:0.55em;">
              <div style="width:9em; flex-shrink:0; color:var(--text-light);">Stage 1 only</div>
              <div style="flex:1; position:relative; height:2em; background:#F4F4F4;">
                <div style="position:absolute; left:45%; top:0; height:100%; width:2px; background:var(--text-light);"></div>
                <div style="position:absolute; left:46%; top:0; height:100%; display:flex; align-items:center; color:var(--text-light); font-variant-numeric:tabular-nums; font-style:italic;">≈ 0 &nbsp; (point estimate −0.06pp)</div>
              </div>
            </div>
          </div>
          <div class="takeaway-box" style="margin-top: 0.8em; max-width:90%; margin-left:auto; margin-right:auto;">
            <strong>The lift is concentrated in retargeting.</strong> Stage 1 alone is at the noise floor. The "+3.12pp combined" headline is mostly retargeting bleeding through.
          </div>
        </section>
        """,

        # ─── SLIDE 4 — Methodology compressed (combines old 2 + 2b) ────────
        """
        <section data-slide="4">
          <h2>How we measured it — ghost-bidding ATT</h2>
          <p style="margin-top:0.4em;">Compare <strong>IPs we actually served</strong> against IPs that <strong>would have been served</strong> if not for the random 10% holdout. Per-(advertiser, IP) MD5 hash, production-equivalent. Holdout subsampled at per-(advertiser, segment) win rate so the denominator matches treated's "actually-served" condition.</p>
          <table style="margin-top:0.7em;">
            <thead><tr><th>Segment</th><th>Filter</th><th>What it isolates</th></tr></thead>
            <tbody>
              <tr><td><strong>All campaigns</strong></td><td><code>(no filter)</code></td><td>Every paid impression for the advertiser</td></tr>
              <tr><td><strong>Prospecting (all stages)</strong></td><td><code>objective_id IN (1,5,6)</code></td><td>Stage 1 + Multi-Touch (S2) + MTFF (S3). Excludes retargeting (4).</td></tr>
              <tr><td><strong>Stage 1 only</strong></td><td><code>... AND funnel_level = 1</code></td><td>Pure top-of-funnel — first touch, no multi-touch reinforcement</td></tr>
              <tr><td><strong>Retargeting only</strong></td><td><code>objective_id = 4</code></td><td>Already-engaged IPs (past site visit, click)</td></tr>
            </tbody>
          </table>
          <p style="font-size:0.7em; color:var(--text-light); margin-top:0.5em;">
            <code>guid_log</code> (cause-agnostic visit signal) is never segment-filtered; it captures every advertiser-site visit regardless of which campaign drove it. Two outcomes per arm: <strong>guid</strong> (causal counterfactual) and <strong>clickpass</strong> (attribution-credited). Pipeline detail in appendix.
          </p>
        </section>
        """,

        # ─── SLIDE 5 — RIGOR ANCHORS (NEW) ─────────────────────────────────
        """
        <section data-slide="5">
          <h2>Two empirical sanity checks (before you ask)</h2>
          <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 1.2em; margin-top:0.8em;">
            <div style="background:#F0F4F9; border-left:4px solid var(--navy); padding:1em 1.2em;">
              <p style="font-size:0.7em; color:var(--text-light); margin:0; text-transform:uppercase; letter-spacing:0.05em;">Holdout enforcement</p>
              <p class="big-number" style="font-size:2.4em; margin:0.3em 0; color:var(--navy);">0 / 5,432,546</p>
              <p style="font-size:0.78em; margin:0; line-height:1.4;">Served IPs landing in the holdout bucket across 8 (objective_id × funnel_level) cells. Production bidder enforces holdout for prospecting <em>and</em> retargeting. Refutes the "retargeting bypasses holdout" failure mode.</p>
            </div>
            <div style="background:#F0F4F9; border-left:4px solid var(--navy); padding:1em 1.2em;">
              <p style="font-size:0.7em; color:var(--text-light); margin:0; text-transform:uppercase; letter-spacing:0.05em;">Cross-window reproduction</p>
              <p class="big-number" style="font-size:2.4em; margin:0.3em 0; color:var(--navy);">+29.06pp</p>
              <p style="font-size:0.78em; margin:0; line-height:1.4;">Retargeting sample-weighted ATT on a different week (04-22 → 04-28). v5 was +28.89pp on the canonical week — Δ −1.29pp. Segment ordering reproduces. Prospecting +0.39pp vs +0.43pp (Δ −0.04pp).</p>
            </div>
          </div>
          <p style="margin-top:1em; font-size:0.85em;"><strong>Plus:</strong> <em>Three universal rules confirmed by the audience-platform owners: every campaign has a 10% holdout, every campaign has an audience expression, CRM lists are only on prospecting (never retargeting). All empirically consistent with the result above.</em></p>
        </section>
        """,

        # ─── SLIDE 6 — Why retargeting drives so much ──────────────────────
        """
        <section data-slide="6">
          <h2>Why retargeting drives 21pp — and why we should be careful with the number</h2>
          <p style="margin-top:0.4em;">Retargeting targets IPs <strong>already engaged</strong> with the advertiser — they visited the site, browsed products, or were uploaded as a CRM list. Two effects compound:</p>
          <ol style="margin-top:0.5em;">
            <li style="margin-bottom:0.5em;"><strong>True causal effect:</strong> reminding an engaged user of the brand drives them back. Real and substantial — repeated exposure is what retargeting is designed for.</li>
            <li style="margin-bottom:0.5em;"><strong>Selection bias from audience size:</strong> the bidder gives preference to campaigns with smaller targeting audiences. Retargeting audiences are smaller than prospecting, so when an IP is eligible for both, retargeting wins disproportionately. The bidder doesn't differentiate by retargeting-vs-prospecting at IP level — it differentiates by audience size, and retargeting just happens to be smaller. Our random hash subsample doesn't replicate this win pattern, so the "treated retargeting" set is systematically more visit-prone than the comparable holdout subsample.</li>
          </ol>
          <p style="margin-top:0.5em;">The +21pp number is the <strong>combined effect</strong> of both. Honest reading: retargeting drives substantial real lift, somewhere between zero and +21pp, with selection inflating the measurement. Bounding the true causal share requires bidder-level ghost bidding (Phase 2b).</p>
        </section>
        """,

        # ─── SLIDE 7 — Stage 1 zero lift (the surprising one) ──────────────
        """
        <section data-slide="7">
          <h2>Stage 1 prospecting alone: approximately zero incremental lift at high intent</h2>
          <p style="margin-top:0.4em;">When we filter to <strong>Stage 1 only</strong> (pure top-of-funnel prospecting, before any multi-touch reinforcement), guid-ATT at high intent is <strong>approximately zero</strong> (point estimate −0.06pp; sample-weighted −1.03pp; 12 of 25 advertisers positive). The 7-day-window CI half-widths exceed the point estimate, so the right read is "no measurable lift," not "negative lift."</p>
          <p style="margin-top:0.5em;"><strong>Interpretation:</strong> high-intent shoppers were going to convert anyway. Stage 1 prospecting is reaching IPs who would visit the site naturally — search, direct, brand pull. MNTN's pure top-of-funnel layer doesn't add measurable incrementality at the highest intent tier.</p>
          <p style="margin-top:0.5em;"><strong>Channel context:</strong> a CTV ad's call-to-action is fundamentally weaker than display. A high-intent shopper sitting on their phone can click immediately; a high-intent shopper watching their TV cannot. The conversion path is longer, the call-to-action effect is smaller, and a 7-day window is short for CTV-driven brand pull to materialize. Low Stage 1 measurement at 7 days is partly a window/channel artifact, not just a "no lift" finding.</p>
          <p style="margin-top:0.5em;"><strong>The headroom for incremental lift is downstream</strong> (multi-touch nurturing — Stage 2/3 within prospecting carry the +0.78pp average) <strong>or upstream</strong> (mid-intent shoppers, where MNTN has room to push customers who haven't yet committed). The high-intent tier is where MNTN's incremental room is smallest.</p>
        </section>
        """,

        # ─── SLIDE 8 — Wedge chart (clickpass vs guid by segment) ─────────
        f"""
        <section data-slide="8" class="img-slide">
          <h2 style="text-align:left;">Attribution wedge by segment — clickpass over- and under-credits</h2>
          <img src="{charts['wedge']}" alt="Wedge by segment">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            <strong>Stage 1 wedge is negative</strong> — clickpass shows positive lift (+0.47pp) while guid shows zero (−0.06pp). Attribution is crediting Stage 1 with visits that would have happened anyway. <strong>Retargeting wedge 0.66×</strong> — clickpass under-credits real lift by ~34%. <strong>Prospecting wedge 1.58×</strong> — clickpass over-credits by 58%.
          </p>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>This slide is about modeling decisions, not just attribution observation.</strong> The wedge tells us why <code>guid_log</code> is the right label to use for incrementality modeling: clickpass over-credits attributed visits in prospecting and under-credits in retargeting. <code>guid_log</code> is the cause-agnostic visit signal — it's what the targeting model should be trained against.
          </div>
        </section>
        """,

        # ─── SLIDE 9 — REMOVED 2026-04-30 per Alex K. "Two methodology fixes vs prior internal numbers" belongs in the verbal preamble, not as a slide. The mixed-segment treatment denominator point is implicit in the 4-segment framing of slides 1, 3, 4. The holdout-subsampling point is on slide 2.

        # ─── SLIDE 10 — Pipeline ──────────────────────────────────────────
        """
        <section data-slide="10">
          <h2>How the pipeline works — end-to-end</h2>
          <pre style="font-size:0.55em; line-height:1.45;">
prospecting_intent_v1     ── per-(advertiser, IP, day) intent score
       │
       ▼ MAX(score) per (advertiser, IP) over 7-day week → tier
holdouts (10% bucket)            targeted (90% bucket)
       │                                │
       ▼ INNER JOIN augmentor_log       ▼ INNER JOIN cost_impression_log
       ▼ subsample at win_rate          ▼ FILTER per segment
       ▼ (4 subsamples, one              ▼ (4 segment-specific
           per segment)                       served sets)
biddable_holdouts (×4)          served_treatment (×4)
       │                                │
       └────────────┬───────────────────┘
                    ▼ LEFT JOIN clickpass_log (segment-filtered)
                    ▼ LEFT JOIN guid_log (segment-agnostic)
              two-proportion ATT per (segment, advertiser, tier, outcome)
                    ▼
          IVW + arithmetic / median / sample-weighted pooling
                  + leave-one-out sensitivity
                  + per-cell N-gate (CI half-width ≤ 0.5pp)
          </pre>
          <p style="font-size:0.7em; color: var(--text-light); margin-top:0.4em;">
            v5 cost: ~6 hr wall, ~4.5T slot-ms, 126.7 TB billed (augmentor scan dominates and is segment-agnostic; the 4-segment UNION ALL inflated stage count to 139). Phase 2b (bidder-level ghost bidding) is the production successor.
          </p>
        </section>
        """,

        # ─── SLIDE 11 — Cohort design ───────────────────────────────────────
        """
        <section data-slide="11">
          <h2>How we picked these 30 advertisers</h2>
          <p style="margin-top:0.4em;">Empirical inclusion gates — every threshold derived from the data:</p>
          <table>
            <thead><tr><th>Gate</th><th>Threshold</th><th>Why</th></tr></thead>
            <tbody>
              <tr><td>Active in window</td><td class="num">≥100 served IPs</td><td>Must run during the analysis week</td></tr>
              <tr><td>Per-tier biddable holdouts</td><td class="num">≥5,000</td><td>Power calc: ≤0.5pp CI half-width at p ∈ [0.005, 0.05]</td></tr>
              <tr><td>Tier diversity</td><td class="num">≥5% of IPs not at score=10,000</td><td>Prevents MAX-tier collapse — peak/mid populated</td></tr>
              <tr><td>Prospecting spend</td><td class="num">≥$5,000 (March)</td><td>Filters dormant advertisers</td></tr>
              <tr><td>Audience dedup</td><td class="num">unique audience signature</td><td>Caught Re-Bath sister companies running identical audiences</td></tr>
            </tbody>
          </table>
          <p style="margin-top:0.6em; font-size:0.78em;">Stratified across <strong>13 high / 7 mid / 10 low</strong> spend × <strong>20 verticals</strong>. Largest single advertiser is 8% of pooled high-tier weight — no single advertiser drives the headline. <strong>23 of 30 advertisers run retargeting</strong> (data for segment 4 covers most but not all).</p>
          <div class="takeaway-box" style="margin-top: 0.6em; font-size:0.78em;">
            <strong>Caveat:</strong> the tier-diversity gate is on the <em>prospecting-score distribution</em> for each advertiser's IPs, not on what the campaign actually <em>served</em>. Some advertisers in this cohort have IPs that score peak/mid by the model, but never received an impression at that tier because the campaign budget concentrated at high intent. The IP-level tier and the served tier can diverge — peak/mid measurements partly reflect "did the budget reach this tier?" not just "is this tier incremental?"
          </div>
        </section>
        """,

        # ─── SLIDE 12 — Caveats ─────────────────────────────────────────────
        """
        <section data-slide="12">
          <h2>Where the analysis is fragile</h2>
          <ol style="margin-top:0.4em; font-size:0.78em;">
            <li style="margin-bottom:0.55em;"><strong>Retargeting counterfactual.</strong> +21pp is incremental within "served retargeting vs would-have-been-served retargeting." The harder question — "what if we didn't run retargeting at all?" — needs bidder-level ghost bidding (Phase 2b) to replicate selection.</li>
            <li style="margin-bottom:0.55em;"><strong>Cohort selection.</strong> We filtered for tier-diverse advertisers. Most MNTN advertisers target high-intent only, so the 30 here may not represent "the typical MNTN advertiser." Phase 1 (30 net-new advertisers) replicates on a fresh cohort.</li>
            <li style="margin-bottom:0.55em;"><strong>Single window — cross-window done on 2 segments.</strong> Re-ran rtg + prosp on 2026-04-22 → 04-28: retargeting +28.89 → +29.06pp (Δ −1.29pp); prospecting +0.43 → +0.39pp (Δ −0.04pp). Ordering reproduces. Phase 2a extends to 30-day on Databricks.</li>
            <li style="margin-bottom:0.55em;"><strong>Subsampling matches size, not selection.</strong> Random hash subsampling at win_rate gives unbiased lift under conditional independence (bidder selection uncorrelated with visit propensity within biddable). For retargeting that assumption is the most fragile — Phase 2b closes it.</li>
          </ol>
        </section>
        """,

        # ─── SLIDE 13 — What's next ─────────────────────────────────────────
        """
        <section data-slide="13">
          <h2>What's next</h2>
          <ul style="margin-top:0.4em;">
            <li style="margin-bottom:0.5em;"><span class="pill">Bidder-level ghost bidding</span> Production solution that escapes the augmentor 10-day TTL AND replicates bidder selection logic. Best path to a tighter retargeting counterfactual.</li>
            <li style="margin-bottom:0.5em;"><span class="pill">Migrate to Databricks</span> Read augmentor + guid logs directly from GCS (<code>gs://mntn-data-archive-prod/</code>). 5–10× speedup; skip BQ scan billing. Enables affordable cross-window validation + Phase 2a.</li>
            <li style="margin-bottom:0.5em;"><span class="pill">Conversions outcome</span> Same pipeline, swap <code>ui_conversions</code> for <code>guid_log</code>. Conversions are 10-20× rarer → need ~30-day window. Augmentor TTL is the binding constraint — Databricks GCS reads remove it.</li>
            <li style="margin-bottom:0.5em;"><span class="pill">iROAS</span> Per-advertiser <code>(incremental conversions × AOV) ÷ MNTN spend</code>. Depends on conversions outcome.</li>
          </ul>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>Decision for the room:</strong> agree that <em>combined</em> campaign-level lift is no longer the canonical incrementality headline. <em>Segment-specific</em> is. Yes/no by Friday so we can update internal reporting + downstream modeling docs accordingly.
          </div>
        </section>
        """,

        # ─── SLIDE 14 — Power Line close ────────────────────────────────────
        """
        <section data-slide="14">
          <p class="powerline" style="font-size:1.7em; margin-top:1.5em; line-height:1.25;">
            Pure prospecting drives near-zero incremental lift.
          </p>
          <p style="text-align:center; margin-top:1em; color: var(--text-light); font-size:0.85em; font-style:italic;">
            Retargeting carries the lift. Combined views hide it.
          </p>
        </section>
        """,
    ]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>TI-837 — Multi-Segment Incrementality Analysis</title>
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
