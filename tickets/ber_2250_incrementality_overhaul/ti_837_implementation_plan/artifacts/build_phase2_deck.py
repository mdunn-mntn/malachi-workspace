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

        # ─── SLIDE 1 — Cold open ───────────────────────────────────────────
        """
        <section data-slide="1">
          <h2 style="margin-bottom:0.6em;">When we measure MNTN's lift, the answer depends on which campaigns count.</h2>
          <p style="margin-top:0.6em;">A combined "MNTN drove +3.1pp lift" headline obscures very different sub-stories.</p>
          <p style="margin-top:0.4em;">We measured the same 30 advertisers, same 7 days, same hash — <strong>4 ways</strong>:</p>
          <ul style="margin-top:0.4em;">
            <li>All campaigns combined</li>
            <li>Prospecting (all stages: 1, 2, 3)</li>
            <li>Stage 1 only (pure top-of-funnel)</li>
            <li>Retargeting only</li>
          </ul>
          <p style="margin-top:1em;"><strong>The four numbers tell different stories.</strong></p>
        </section>
        """,

        # ─── SLIDE 2 — Methodology brief ────────────────────────────────────
        """
        <section data-slide="2">
          <h2>What we measured — ghost-bidding ATT</h2>
          <p style="margin-top:0.4em;"><strong>The problem with ITT:</strong> only 14–16% of "treated" hash-bucket IPs were actually served. The other 84% diluted the effect.</p>
          <p style="margin-top:0.5em;"><strong>The fix:</strong> compare <strong>IPs we actually served</strong> against IPs that <strong>would have been served</strong> if not for the random 10% holdout.</p>
          <table style="margin-top:0.7em;">
            <thead><tr><th>Setup</th><th>Detail</th></tr></thead>
            <tbody>
              <tr><td>Cohort</td><td>30 MNTN advertisers, stratified across spend × vertical × intent diversity</td></tr>
              <tr><td>Window</td><td>2026-04-20 → 04-26 UTC (7 days), +3-day post-period for visit attribution</td></tr>
              <tr><td>Holdout</td><td>Per-(advertiser, IP) MD5 hash · 10% holdout · production-equivalent</td></tr>
              <tr><td>Holdout denominator</td><td>Subsampled at per-(advertiser, segment) empirical win rate to match treated arm's "actually-served" condition</td></tr>
              <tr><td>Outcomes</td><td>Clickpass visits (attributed) and Guid visits (causal counterfactual)</td></tr>
            </tbody>
          </table>
        </section>
        """,

        # ─── SLIDE 2b — How the 4 segments are defined ─────────────────────
        """
        <section data-slide="2b">
          <h2>How the 4 segments are defined</h2>
          <p style="margin-top:0.4em;">Each segment filters <code>cost_impression_log</code> and <code>clickpass_log</code> on the campaign's <code>objective_id</code> and <code>funnel_level</code>. <code>guid_log</code> is never filtered (it's a cause-agnostic visit signal — every advertiser-site visit fires regardless of which campaign drove it).</p>
          <table style="margin-top:0.5em;">
            <thead><tr><th>Segment</th><th>SQL filter</th><th>Strategy isolated</th></tr></thead>
            <tbody>
              <tr><td><strong>All campaigns</strong></td><td><code>(no filter)</code></td><td>Every paid impression for the advertiser, regardless of campaign type</td></tr>
              <tr><td><strong>Prospecting (all stages)</strong></td><td><code>objective_id IN (1, 5, 6)</code></td><td>Stage 1 prospecting + Multi-Touch (S2) + Multi-Touch Full Funnel (S3). Excludes retargeting (4) and ego (7).</td></tr>
              <tr><td><strong>Stage 1 only</strong></td><td><code>objective_id IN (1, 5, 6) AND funnel_level = 1</code></td><td>Pure top-of-funnel prospecting — first touch, no multi-touch reinforcement. <code>funnel_level</code> is authoritative for stage; <code>objective_id</code> alone is unreliable since UI migration broke the mapping.</td></tr>
              <tr><td><strong>Retargeting only</strong></td><td><code>objective_id = 4</code></td><td>Already-engaged IPs (past site visit, click, CRM list)</td></tr>
            </tbody>
          </table>
          <p style="font-size:0.7em; color:var(--text-light); margin-top:0.5em;">
            The same IP can appear in multiple segments if the advertiser served them via more than one campaign type. Segment counts overlap; they're not partitions.
          </p>
        </section>
        """,

        # ─── SLIDE 3 — THE HEADLINE CHART ──────────────────────────────────
        f"""
        <section data-slide="3" class="img-slide">
          <h2 style="text-align:left;">Retargeting drives the lift.  Pure prospecting drives almost none.</h2>
          <img src="{charts['headline']}" alt="High-intent guid-ATT by segment">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            High-intent guid-ATT across 4 segments, IVW pool (solid) and sample-weighted (hatched). Retargeting: +21pp guid lift. Stage 1 only: −0.06pp. The "all campaigns" combined view (+3.12pp) is dominated by retargeting being mixed in.
          </p>
        </section>
        """,

        # ─── SLIDE 4 — Headline numbers table ──────────────────────────────
        """
        <section data-slide="4">
          <h2>The 4-segment headline numbers</h2>
          <table style="margin-top:0.5em;">
            <thead><tr><th>Segment</th><th>guid IVW</th><th>guid sample-wt</th><th>clickpass IVW</th><th>wedge</th><th>Cells</th></tr></thead>
            <tbody>
              <tr style="background:#FBE5E3;">
                <td><strong>Retargeting only</strong></td>
                <td class="num"><strong class="red">+21.07pp</strong></td>
                <td class="num">+28.89pp</td>
                <td class="num">+13.97pp</td>
                <td class="num">0.66×</td>
                <td class="num">8/8 pos</td>
              </tr>
              <tr><td>All campaigns combined</td><td class="num">+3.12pp</td><td class="num">+5.44pp</td><td class="num">+2.88pp</td><td class="num">0.92×</td><td class="num">25/27 pos</td></tr>
              <tr><td>Prospecting (all stages 1+2+3)</td><td class="num">+0.78pp</td><td class="num">+0.46pp</td><td class="num">+1.24pp</td><td class="num">1.58×</td><td class="num">20/26 pos</td></tr>
              <tr style="background:#EEEEEE;">
                <td><strong>Stage 1 only</strong></td>
                <td class="num"><strong>−0.06pp</strong></td>
                <td class="num">−1.03pp</td>
                <td class="num">+0.47pp</td>
                <td class="num">−8.5×</td>
                <td class="num">12/25 pos</td>
              </tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>The lift is heavily concentrated in the retargeting layer.</strong> Stage 1 prospecting alone shows zero or slightly negative lift. The "+3.12pp combined" headline is mostly retargeting bleeding through.
          </div>
        </section>
        """,

        # ─── SLIDE 5 — by-tier chart ───────────────────────────────────────
        f"""
        <section data-slide="5" class="img-slide">
          <h2 style="text-align:left;">Lift profile by tier — segment matters more than tier</h2>
          <img src="{charts['by_tier']}" alt="Segment × tier lift profile">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            Retargeting (red) shows large positive lift across high + peak performance. Stage 1 prospecting (gray) shows zero or slightly negative lift across all three tiers. Mid intent is at the noise floor everywhere — none of the segments show meaningful mid-tier lift in this 7-day window.
          </p>
        </section>
        """,

        # ─── SLIDE 6 — Why retargeting drives so much ──────────────────────
        """
        <section data-slide="6">
          <h2>Why retargeting drives 21pp — and why we should be careful with the number</h2>
          <p style="margin-top:0.4em;">Retargeting targets IPs <strong>already engaged</strong> with the advertiser — they visited the site, browsed products, or were uploaded as a CRM list. Two effects compound:</p>
          <ol style="margin-top:0.5em;">
            <li style="margin-bottom:0.5em;"><strong>True causal effect:</strong> reminding an engaged user of the brand drives them back. Real and substantial — repeated exposure is what retargeting is designed for.</li>
            <li style="margin-bottom:0.5em;"><strong>Selection bias:</strong> the bidder preferentially bids higher on visit-prone IPs (past converters score higher). Our random hash subsample doesn't replicate this selection — so the "treated retargeting" set may be systematically more visit-prone than the comparable holdout subsample.</li>
          </ol>
          <p style="margin-top:0.5em;">The +21pp number is the <strong>combined effect</strong> of both. Honest reading: retargeting drives substantial real lift, somewhere between zero and +21pp, with selection inflating the measurement. Bounding the true causal share requires bidder-level ghost bidding (Phase 2b).</p>
        </section>
        """,

        # ─── SLIDE 7 — Stage 1 zero lift (the surprising one) ──────────────
        """
        <section data-slide="7">
          <h2>Stage 1 prospecting alone: zero incremental lift at high intent</h2>
          <p style="margin-top:0.4em;">When we filter to <strong>Stage 1 only</strong> (pure top-of-funnel prospecting, before any multi-touch reinforcement), guid-ATT at high intent is <span class="red"><strong>−0.06pp</strong></span>. Sample-weighted: <span class="red"><strong>−1.03pp</strong></span>. Only <strong>12 of 25</strong> advertisers (48%) show positive lift.</p>
          <p style="margin-top:0.5em;"><strong>Interpretation:</strong> high-intent shoppers were going to convert anyway. Stage 1 prospecting is reaching IPs who would visit the site naturally — search, direct, brand pull. MNTN's pure top-of-funnel layer doesn't add measurable incrementality at the highest intent tier.</p>
          <p style="margin-top:0.5em;"><strong>The headroom for incremental lift is downstream</strong> (multi-touch nurturing — Stage 2/3 within prospecting carry the +0.78pp average) <strong>or upstream</strong> (mid-intent shoppers, where MNTN has room to push customers who haven't yet committed). The high-intent tier is where MNTN's incremental room is smallest.</p>
        </section>
        """,

        # ─── SLIDE 8 — Wedge chart (clickpass vs guid by segment) ─────────
        f"""
        <section data-slide="8" class="img-slide">
          <h2 style="text-align:left;">Attribution wedge by segment — clickpass over- and under-credits</h2>
          <img src="{charts['wedge']}" alt="Wedge by segment">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            <strong>Stage 1 wedge is negative</strong> — clickpass shows positive lift (+0.47pp) while guid shows zero/negative (−0.06pp). Attribution is crediting Stage 1 with visits that would have happened anyway. <strong>Retargeting wedge 0.66×</strong> — clickpass under-credits real lift by ~34%. <strong>Prospecting wedge 1.58×</strong> — clickpass over-credits by 58%.
          </p>
        </section>
        """,

        # ─── SLIDE 9 — Two methodology fixes ────────────────────────────────
        """
        <section data-slide="9">
          <h2>Two methodology fixes versus prior internal numbers</h2>
          <p style="margin-top:0.4em;">Earlier internal "incrementality" reports overstated lift for two reasons we now correct:</p>
          <ol style="margin-top:0.5em;">
            <li style="margin-bottom:0.6em;"><strong>Holdout denominator artificially large.</strong> "In augmentor_log" ≠ "would have been served." MNTN's bidder wins ~1% of auctions. Subsampling biddable_holdouts at the per-(advertiser, segment) empirical win rate makes the holdout denominator apples-to-apples with treated arm's "actually-served" condition.</li>
            <li style="margin-bottom:0.6em;"><strong>Mixed-segment treatment denominator.</strong> Earlier reports counted ALL impressions for an advertiser as "treated" — conflating retargeting (+21pp lift) with prospecting (+0.78pp lift) into a single misleading +3.12pp combined headline. v5 separates the four segments so each is measured against its appropriate counterfactual.</li>
          </ol>
          <p style="margin-top:0.5em; font-size:0.78em; color: var(--text-light);">
            Both fixes preserve internal consistency: same hash for both arms, same window, same advertisers. Per-segment win_rates are computed from prospecting-only / retargeting-only / stage-1-only served counts respectively.
          </p>
        </section>
        """,

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
        </section>
        """,

        # ─── SLIDE 12 — Caveats ─────────────────────────────────────────────
        """
        <section data-slide="12">
          <h2>What I'd want a methodologist to push on</h2>
          <ol style="margin-top:0.4em;">
            <li style="margin-bottom:0.4em;"><strong>Retargeting counterfactual scope.</strong> The +21pp retargeting lift is what the experiment measured: served retargeting vs would-have-been-served retargeting holdouts (subsampled at retargeting win rate). It IS incremental within that frame. The harder question — "what would happen if MNTN didn't run retargeting at all?" — needs a tighter counterfactual that replicates the bidder's selection logic. That's bidder-level ghost bidding (Phase 2b).</li>
            <li style="margin-bottom:0.4em;"><strong>Cohort selection bias.</strong> We filtered for tier-diverse advertisers — those whose IPs span multiple intent tiers. Most MNTN advertisers target high-intent only, so our 30 may not represent "the typical MNTN advertiser." Replication on a random sample is future work.</li>
            <li style="margin-bottom:0.4em;"><strong>Single window — no cross-window validation yet.</strong> Cross-window validation = re-run the same analysis on a different 7-day window (e.g., 2026-04-13 → 04-19) and check whether the segment ordering and magnitudes reproduce. If retargeting +21pp shows up consistently across multiple windows, the result is real. If it varies by 5-10pp week-over-week, the single-window number is sample noise as much as signal. Augmentor's 10-day TTL bounds backward replication; Databricks GCS reads remove that constraint for forward replication.</li>
            <li style="margin-bottom:0.4em;"><strong>Intent-score movement during window.</strong> An IP could score "peak performance" in pre-period and "high intent" mid-week, but they're locked in their MAX-tier subject pool. Partial explanation for peak-tier numbers being noisy.</li>
            <li style="margin-bottom:0.4em;"><strong>CTV multi-advertiser confounding.</strong> A CTV viewer sees ads from many advertisers concurrently. Some attributed lift may be from competitor concurrent campaigns. Hard to disentangle without cross-platform exposure data.</li>
            <li style="margin-bottom:0.4em;"><strong>Random subsampling math.</strong> Random hash subsampling at win_rate matches denominator <em>size</em> but doesn't replicate bidder <em>selection</em>. The lift estimate is unbiased under the conditional-independence assumption (bidder selection uncorrelated with visit propensity within the biddable population). For retargeting, that assumption is the most fragile.</li>
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
            <strong>Decision to take to leadership:</strong> publish segment-specific incrementality reports internally. Stage 1 ≈ zero, multi-touch ≈ modest, retargeting ≈ large-but-selection-inflated. The "all-campaigns" headline conflates all three.
          </div>
        </section>
        """,

        # ─── SLIDE 14 — Power Line ──────────────────────────────────────────
        """
        <section data-slide="14">
          <p class="powerline" style="font-size:1.4em; margin-top:0.5em;">
            Retargeting drives the lift.<br>
            Pure prospecting drives almost none.<br>
            Combined views hide both.
          </p>
          <p style="text-align:center; margin-top:1.2em; color: var(--text-light); font-size:0.8em;">
            Retargeting +21pp guid lift (real but selection-inflated).<br>
            Stage 1 prospecting −0.06pp (zero incremental lift at high intent).<br>
            Prospecting all stages averages to +0.78pp; "all campaigns" reads +3.12pp.
          </p>
          <p style="text-align:center; margin-top:0.9em; color: var(--text-light); font-size:0.65em;">
            30 advertisers · 7-day window · 4 segments · 23 advertisers run retargeting · 0 single-advertiser dominance flags.
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
