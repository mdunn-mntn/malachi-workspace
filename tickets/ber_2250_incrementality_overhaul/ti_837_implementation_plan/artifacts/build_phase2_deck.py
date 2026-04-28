"""TI-837 deck builder — v4 results, single complete narrative.

Power Line:
  Targeting causes real but modest lift.
  Attribution shows it 60% larger than reality.

Charts use v4 data (prospecting-only + win-rate-corrected). Embedded
as base64 inline for portability.
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
        "headline":      b64("ti_837_chart_mntn_overall_headline_v4.png"),
        "money_per_tier": b64("ti_837_chart_money_per_tier_with_wedge_v4.png"),
        "per_adv":       b64("ti_837_chart_per_advertiser_high_intent_v4.png"),
        "wedge_by_tier": b64("ti_837_chart_wedge_ratio_per_tier_v4.png"),
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
.reveal .big-number { font-size: 3.5em; font-weight: 800; color: var(--red); text-align: center; line-height: 1; margin: 0.15em 0; font-variant-numeric: tabular-nums; }
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
            <strong>Both were wrong.</strong> Today we measured the truth — using the same hash that production uses to assign holdouts, and properly scoped to prospecting-only impressions.
          </p>
        </section>
        """,

        # ─── SLIDE 2 — What we did ──────────────────────────────────────────
        """
        <section data-slide="2">
          <h2>What we measured — ghost-bidding ATT</h2>
          <p style="margin-top:0.4em;"><strong>The problem with ITT:</strong> only 14–16% of "treated" IPs were actually served an impression. The other 84% diluted the effect.</p>
          <p style="margin-top:0.5em;"><strong>The fix:</strong> compare <strong>IPs we actually served</strong> against IPs that <strong>would have been served</strong> if not for the random 10% holdout. Same outcome, apples-to-apples comparison.</p>
          <table style="margin-top:0.7em;">
            <thead><tr><th>Setup</th><th>Detail</th></tr></thead>
            <tbody>
              <tr><td>Cohort</td><td>30 MNTN advertisers, stratified across spend × vertical × intent diversity</td></tr>
              <tr><td>Window</td><td>2026-04-20 → 04-26 UTC (7 days), +3-day post-period for visit attribution</td></tr>
              <tr><td>Holdout</td><td>Per-(advertiser, IP) MD5 hash · 10% holdout · production-equivalent</td></tr>
              <tr><td>Filter</td><td><strong>Prospecting-only impressions</strong> (objective_id IN 1, 5, 6) — excludes retargeting</td></tr>
              <tr><td>Holdout denominator</td><td>Subsampled at per-advertiser empirical win rate to match treated arm's "actually-served" condition</td></tr>
              <tr><td>Outcomes</td><td>Clickpass visits (attributed) and Guid visits (causal counterfactual)</td></tr>
            </tbody>
          </table>
        </section>
        """,

        # ─── SLIDE 3 — THE LIFT (anchored, interpretable) ──────────────────
        """
        <section data-slide="3">
          <h2 style="margin-bottom: 0.4em;">Did MNTN drive real lift?  <span class="navy">Yes — but modest.</span></h2>
          <p style="margin-top:0.3em; font-size:0.9em;">For every <strong>1,000 high-intent prospecting IPs</strong> MNTN serves an ad to:</p>
          <table style="font-size:0.7em; margin-top:0.4em; max-width:85%;">
            <thead><tr><th></th><th>Visit rate</th><th>Visits per 1,000 IPs</th></tr></thead>
            <tbody>
              <tr><td><strong>Holdout</strong> (would-have-been-served, but weren't)</td><td class="num">2.31%</td><td class="num">23 visits</td></tr>
              <tr><td><strong>Treated</strong> (actually served by MNTN, prospecting only)</td><td class="num">2.76%</td><td class="num">28 visits</td></tr>
              <tr style="background: #FBE5E3;"><td><strong>MNTN-caused incremental</strong></td><td class="num"><strong class="red">+0.44pp</strong></td><td class="num"><strong class="red">+5 visits</strong></td></tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.7em;">
            <strong>For every 1,000 high-intent prospecting IPs MNTN serves, ~5 incremental visits happen that wouldn't otherwise.</strong><br>
            <span style="color: var(--text-light); font-size: 0.85em;">Sample-weighted across 27 advertisers (passing power gate) · n = 12.0M served-vs-holdout IPs · 7-day window.</span>
          </div>
        </section>
        """,

        # ─── SLIDE 4 — CONFIDENCE ──────────────────────────────────────────
        """
        <section data-slide="4">
          <h2 style="margin-bottom: 0.4em;">How confident are we?  <span class="navy">Robust across 4 pooling methods.</span></h2>
          <p style="font-size:0.85em; margin-top:0.3em;">High-intent guid-ATT under different ways of combining 27 advertiser cells:</p>
          <table style="font-size:0.7em; margin-top:0.4em;">
            <thead><tr><th>Method</th><th>guid-ATT</th><th>What it answers</th></tr></thead>
            <tbody>
              <tr><td>IVW (default)</td><td class="num">+0.77pp ± 0.02</td><td>Variance-optimal pool</td></tr>
              <tr><td>Median (advertiser-equal)</td><td class="num">+0.56pp</td><td>Typical advertiser</td></tr>
              <tr><td>Arithmetic mean</td><td class="num">+0.98pp</td><td>Equal-weight average</td></tr>
              <tr><td>Sample-size weighted</td><td class="num">+0.44pp</td><td>Per-impression average</td></tr>
            </tbody>
          </table>
          <p style="font-size:0.78em; margin-top:0.6em;">All four converge on <strong>~+0.4–1.0pp lift</strong> at high intent. <strong>The pattern is real, not a pooling artifact.</strong></p>
          <table style="font-size:0.7em; margin-top:0.5em;">
            <thead><tr><th>Robustness check</th><th>Result</th></tr></thead>
            <tbody>
              <tr><td>Advertisers with positive lift</td><td><strong>21 of 27</strong> (78%)</td></tr>
              <tr><td>Largest leave-one-out swing</td><td><strong>< ±0.05pp</strong> — no advertiser drives the result</td></tr>
              <tr><td>Per-cell N-gate (CI half-width ≤0.5pp)</td><td>27 of 29 cells pass</td></tr>
            </tbody>
          </table>
        </section>
        """,

        # ─── SLIDE 5 — Per-advertiser distribution ──────────────────────────
        f"""
        <section data-slide="5" class="img-slide">
          <h2 style="text-align:left;">Lift varies by advertiser — most positive, some negative</h2>
          <img src="{charts['per_adv']}" alt="Per-advertiser high-intent guid ATT">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            27 advertisers passing the 0.5pp gate. 21 positive (78%). Range −3.30pp (Ferguson Home — strong brand, low room for incremental lift) to +6.88pp (TurboTenant). Median +0.56pp. Magnitude tracks vertical fit and brand strength.
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
            The gap between them is the <strong>wedge</strong> — how much over- or under-credit our attribution carries.
          </p>
          <table style="margin-top:0.6em;">
            <thead><tr><th>Tier</th><th>Clickpass-ATT (IVW)</th><th>Guid-ATT (IVW, truth)</th><th>Wedge</th><th>Verdict</th></tr></thead>
            <tbody>
              <tr><td><strong>High intent</strong></td><td class="num">+1.22pp</td><td class="num">+0.77pp</td><td class="num"><strong class="red">1.59×</strong></td><td><span class="pill pill-red">over-credits 60%</span></td></tr>
              <tr><td><strong>Peak intent</strong></td><td class="num">+0.12pp</td><td class="num">−0.02pp</td><td class="num">undefined</td><td><span class="pill pill-gray">no real lift</span></td></tr>
              <tr><td>Mid intent</td><td class="num">+0.02pp</td><td class="num">+0.00pp</td><td class="num">noise</td><td><span class="pill pill-gray">noise floor</span></td></tr>
            </tbody>
          </table>
          <p style="margin-top:0.5em; font-size:0.7em; color: var(--text-light);">
            (Sample-weighted view: high lift +0.44pp, peak lift −0.36pp, mid lift +0.02pp. Both methods agree on the asymmetry.)
          </p>
        </section>
        """,

        # ─── SLIDE 7 — High intent: clickpass over-credit ──────────────────
        """
        <section data-slide="7">
          <h2>At high intent — clickpass over-credits real lift by ~60%</h2>
          <p style="margin-top:0.4em;">All four pooling methods agree: clickpass shows more lift than guid measures actually happened.</p>
          <table style="margin-top:0.5em;">
            <thead><tr><th>Pooling method</th><th>Clickpass-ATT</th><th>Guid-ATT</th><th>Wedge (c/g)</th></tr></thead>
            <tbody>
              <tr><td>IVW (default)</td><td class="num">+1.22pp</td><td class="num">+0.77pp</td><td class="num"><strong class="red">1.59×</strong></td></tr>
              <tr><td>Median</td><td class="num">+1.62pp</td><td class="num">+0.56pp</td><td class="num"><strong class="red">2.91×</strong></td></tr>
              <tr><td>Arithmetic mean</td><td class="num">+2.17pp</td><td class="num">+0.98pp</td><td class="num"><strong class="red">2.21×</strong></td></tr>
              <tr><td>Sample-size weighted</td><td class="num">+2.33pp</td><td class="num">+0.44pp</td><td class="num"><strong class="red">5.29×</strong></td></tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.7em;">
            <strong>Attribution captures real lift, but inflates it.</strong><br>
            <span style="color: var(--text-light); font-size: 0.85em;">Range 1.6× to 5.3× depending on method. Conservative reading: clickpass over-credits real causal lift by ~60% at high intent. iROAS calculations should discount accordingly.</span>
          </div>
        </section>
        """,

        # ─── SLIDE 8 — Peak intent: no real lift ───────────────────────────
        """
        <section data-slide="8">
          <h2>At peak intent — prospecting drives no incremental lift</h2>
          <p style="margin-top:0.4em;">Across all pooling methods, guid-ATT at peak hovers at zero or slightly negative:</p>
          <table style="margin-top:0.4em;">
            <thead><tr><th>Pooling method</th><th>Guid-ATT (peak)</th></tr></thead>
            <tbody>
              <tr><td>IVW</td><td class="num">−0.02pp</td></tr>
              <tr><td>Median</td><td class="num">−0.06pp</td></tr>
              <tr><td>Arithmetic mean</td><td class="num">−0.04pp</td></tr>
              <tr><td>Sample-weighted</td><td class="num">−0.36pp</td></tr>
            </tbody>
          </table>
          <p style="font-size:0.85em; margin-top:0.6em;">Treated visit rate <strong>0.40%</strong>, holdout visit rate <strong>0.76%</strong> (sample-weighted). Treated arm visits at <em>lower</em> rate than holdout. <strong>Only 9 of 22 advertisers (41%) show positive lift at peak.</strong></p>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>Peak-intent prospecting is not driving incremental visits in this cohort.</strong><br>
            <span style="color: var(--text-light); font-size: 0.85em;">Two non-mutually-exclusive explanations: (1) peak-intent IPs were going to visit anyway (Alex's "movable middle" hypothesis: high-intent isn't very incremental); (2) intent-score movement during the window mis-classifies subjects. See caveats slide.</span>
          </div>
        </section>
        """,

        # ─── SLIDE 9 — Methodology rigor ────────────────────────────────────
        """
        <section data-slide="9">
          <h2>Why the headline number is smaller than prior estimates</h2>
          <p style="margin-top:0.4em;">Two methodology fixes pulled the number down from earlier internal "incrementality" reports:</p>
          <ol style="margin-top:0.5em;">
            <li style="margin-bottom:0.6em;"><strong>Prospecting-only filter.</strong> Earlier reports counted ALL impressions for an advertiser as "treated" — including retargeting (objective_id=4) on already-engaged IPs. Retargeting served IPs visit at 6× the rate of prospecting-served IPs (because they're already in-funnel). Filtering to prospecting-objective campaigns (1, 5, 6) shows the true prospecting lift.</li>
            <li style="margin-bottom:0.6em;"><strong>Holdout denominator correction.</strong> "In augmentor_log" ≠ "would have been served." MNTN's bidder wins ~1% of auctions. Subsampling biddable_holdouts at the per-advertiser empirical win rate makes the holdout denominator apples-to-apples with treated arm's "actually-served" condition. (Implemented per Alex Knorr review, 2026-04-28.)</li>
          </ol>
          <p style="margin-top:0.5em; font-size:0.78em; color: var(--text-light);">
            Both fixes preserve internal consistency: same hash for both arms, same window, same advertisers. The smaller number is the true prospecting lift, not a methodology artifact.
          </p>
        </section>
        """,

        # ─── SLIDE 10 — Methodology pipeline ────────────────────────────────
        """
        <section data-slide="10">
          <h2>How the pipeline works — end-to-end</h2>
          <pre style="font-size:0.6em; line-height:1.5;">
prospecting_intent_v1     ── per-(advertiser, IP, day) intent score
       │
       ▼ MAX(score) per (advertiser, IP) over 7-day week → tier
holdouts (10% bucket)            targeted (90% bucket)
       │                                │
       ▼ INNER JOIN augmentor_log       ▼ INNER JOIN cost_impression_log
       ▼ subsample at win_rate          ▼ FILTER prospecting (objective IN 1,5,6)
biddable_holdouts                served_treatment
   "would have been served at         "actually was served via
    MNTN's empirical win rate"         a prospecting campaign"
       │                                │
       └────────────┬───────────────────┘
                    ▼ LEFT JOIN clickpass_log (prospecting-filtered) + guid_log
              two-proportion ATT per (advertiser, tier, outcome)
                    ▼
          IVW + arithmetic / median / sample-weighted pooling
                  + leave-one-out sensitivity
                  + per-cell N-gate (CI half-width ≤ 0.5pp)
          </pre>
          <p style="font-size:0.7em; color: var(--text-light); margin-top:0.4em;">
            Same MD5 holdout hash production uses. Same window for both arms (3-day post for cross-day visits). Single batched augmentor scan amortizes across all 30 advertisers (113 min wall, 126.7 TB scanned, 575 slot-hours).
          </p>
        </section>
        """,

        # ─── SLIDE 11 — Cohort design ───────────────────────────────────────
        """
        <section data-slide="11">
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

        # ─── SLIDE 12 — Caveats ─────────────────────────────────────────────
        """
        <section data-slide="12">
          <h2>What I'd want a methodologist to push on</h2>
          <ol style="margin-top:0.4em;">
            <li style="margin-bottom:0.5em;"><strong>Cohort selection bias.</strong> We filtered for tier-diverse advertisers (those whose IPs span multiple intent tiers). Most MNTN advertisers target high-intent only. Our 30 may not generalize to "all MNTN advertisers" (per Bryce, team meeting). Replication on a random sample is future work.</li>
            <li style="margin-bottom:0.5em;"><strong>Single window.</strong> 7 days, 2026-04-20 → 04-26. No cross-window validation yet. Augmentor 10-day TTL bounds backward replication; forward replication is straightforward.</li>
            <li style="margin-bottom:0.5em;"><strong>Intent-score movement.</strong> An IP could score peak in pre-period, move to high-intent during the analysis week, and get served then — but they're in the "peak" subject pool. May explain part of the peak-tier negative lift (selection artifact).</li>
            <li style="margin-bottom:0.5em;"><strong>CTV multi-advertiser confounding.</strong> A CTV viewer sees ads from many advertisers concurrently. Some "incremental" visits attributed to MNTN may be from competitor concurrent campaigns. Hard to disentangle without cross-platform exposure.</li>
            <li style="margin-bottom:0.5em;"><strong>Random subsampling preserves expected rate.</strong> The win-rate hash subsample matches denominator size but doesn't replicate bidder selection logic. If bidder selection correlates with visit propensity, the corrected lift is still biased — just with the correct sample size.</li>
          </ol>
        </section>
        """,

        # ─── SLIDE 13 — What's next ─────────────────────────────────────────
        """
        <section data-slide="13">
          <h2>What's next</h2>
          <ul style="margin-top:0.4em;">
            <li style="margin-bottom:0.6em;"><span class="pill">Migrate to Databricks</span> Read augmentor + guid logs directly from GCS (path: <code>gs://mntn-data-archive-prod/</code>). Speed up scans 5–10× and skip BQ scan billing. Cluster ready (Victor S., 2026-04-28). Critical for Phase 2a.</li>
            <li style="margin-bottom:0.6em;"><span class="pill">Conversions outcome</span> Same pipeline, swap <code>ui_conversions</code> for <code>guid_log</code>. Conversions are 10-20× rarer → need ~30-day window. Augmentor TTL is the binding constraint — Databricks GCS reads remove it.</li>
            <li style="margin-bottom:0.6em;"><span class="pill">Bidder-level ghost bidding</span> Production solution. Pending Alex Bloore decision; Zach + Jordan on bidder team. Replaces the post-hoc augmentor scan.</li>
            <li style="margin-bottom:0.6em;"><span class="pill">iROAS</span> Per-advertiser <code>(incremental conversions × AOV) ÷ MNTN spend</code>. Depends on conversions outcome above.</li>
          </ul>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>Decision now:</strong> publish the wedge alongside clickpass internally, so the team knows the size of the gap when reading attribution-driven reports. Modest real lift at high intent + zero at peak + 1.6× clickpass over-credit at high — that's the calibration term.
          </div>
        </section>
        """,

        # ─── SLIDE 14 — Power Line return ───────────────────────────────────
        """
        <section data-slide="14">
          <p class="powerline" style="font-size:1.4em; margin-top:0.5em;">
            Targeting causes real but modest lift.<br>
            Attribution shows it 60% larger than reality.
          </p>
          <p style="text-align:center; margin-top:1.2em; color: var(--text-light); font-size:0.8em;">
            High-intent prospecting drives ~5 incremental visits per 1,000 served IPs.<br>
            Clickpass shows that as ~12. The gap is the calibration term.<br>
            Peak intent: no measurable incremental lift in this cohort.
          </p>
          <p style="text-align:center; margin-top:0.9em; color: var(--text-light); font-size:0.65em;">
            30 advertisers · 7-day window · 12.0M served-vs-holdout IPs · 0 single-advertiser dominance flags · 78% of advertisers positive at high · CI ±0.022pp.
          </p>
        </section>
        """,
    ]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>TI-837 — Ghost-Bidding Incrementality Analysis</title>
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
