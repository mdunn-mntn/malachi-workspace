"""TI-837 Phase 2 deck builder.

Constructs ti_837_phase2_presentation_deck.html with all charts embedded
as base64 data URIs. Reuses the Phase 1 deck's CSS/styling.

Usage: python build_phase2_deck.py
Output: artifacts/ti_837_phase2_presentation_deck.html
"""
import base64
import os
from pathlib import Path

ROOT = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan")
ARTIFACTS = ROOT / "artifacts"


def b64(filename):
    """Read PNG file, return base64 data URI."""
    p = ARTIFACTS / filename
    if not p.exists():
        raise FileNotFoundError(f"Missing chart: {p}")
    data = p.read_bytes()
    enc = base64.b64encode(data).decode("ascii")
    return f"data:image/png;base64,{enc}"


def main():
    # Charts to embed
    charts = {
        # Phase 2 (30-advertiser)
        "headline_30adv":     b64("ti_837_chart_mntn_overall_headline_30adv.png"),
        "money_30adv":        b64("ti_837_chart_money_per_tier_with_wedge_30adv.png"),
        "per_adv_30adv":      b64("ti_837_chart_per_advertiser_high_intent_30adv.png"),
        "wedge_30adv":        b64("ti_837_chart_wedge_ratio_per_tier_30adv.png"),
        # Phase 1 (7-advertiser) for comparison
        "money_7adv":         b64("ti_837_chart_money_per_tier_with_wedge_7adv.png"),
        # New comparison charts (Phase 1 vs Phase 2)
        "p1_vs_p2_wedge":     b64("ti_837_chart_phase1_vs_phase2_wedge.png"),
        "peak_pooling":       b64("ti_837_chart_peak_pooling_methods.png"),
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
.reveal .big-number { font-size: 3.5em; font-weight: 800; color: var(--red); text-align: center; line-height: 1; margin: 0.2em 0; font-variant-numeric: tabular-nums; }
.reveal .big-number-context { font-size: 0.95em; color: var(--text-light); text-align: center; margin: 0; }
.reveal .takeaway-box { background: #F0F4F9; border-left: 4px solid var(--navy); padding: 0.8em 1em; font-size: 0.85em; line-height: 1.4; margin-top: 0.6em; }
.reveal .takeaway-box strong { color: var(--navy); font-weight: 700; }
.reveal .pill { display: inline-block; padding: 0.1em 0.5em; border-radius: 0.7em; background: #E8EDF5; color: var(--navy); font-size: 0.7em; margin-right: 0.4em; font-weight: 600; }
.reveal .pill-red { background: #FBE5E3; color: var(--red); }
.reveal .pill-green { background: #E2EFDF; color: #2C5F2D; }
.reveal .pill-gray { background: #EEE; color: #555; }
.reveal .compare-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1.2em; margin-top: 0.4em; }
.reveal .compare-grid .col { padding: 0.6em 0.8em; border-radius: 6px; }
.reveal .compare-grid .col.p1 { background: #F4F4F4; }
.reveal .compare-grid .col.p2 { background: #ECF1F8; border-left: 3px solid var(--navy); }
.reveal .compare-grid h3 { font-size: 0.95em; margin-bottom: 0.4em; }
.reveal .compare-grid p { font-size: 0.75em; margin: 0.2em 0; }
.reveal .compare-grid strong { color: var(--navy); }
"""

    slides = [
        # SLIDE 1 — Title / Power Line
        f"""
        <section data-slide="1">
          <h1 style="font-size:1.7em; line-height:1.1; margin-bottom: 0.6em;">Phase 2: stress-testing the wedge</h1>
          <p class="powerline" style="text-align:left; font-size:1.15em; line-height:1.35; color: var(--navy);">
            The clickpass over-credit was an artifact.<br>
            The peak under-credit got stronger.
          </p>
          <p style="margin-top:1em; font-size:0.7em; color: var(--text-light);">
            TI-837 — Phase 2 of incrementality methodology.<br>
            30 advertisers, 7-day window 2026-04-20 → 04-26 UTC.<br>
            Malachi Dunn · 2026-04-28
          </p>
        </section>
        """,

        # SLIDE 2 — Phase 1 recap
        """
        <section data-slide="2">
          <h2>What Phase 1 told us</h2>
          <p style="margin-top:0.4em;">Seven advertisers, ghost-bidding ATT, 7-day window. We shipped a wedge story:</p>
          <table>
            <thead><tr><th>Tier</th><th>Clickpass-ATT</th><th>Guid-ATT</th><th>Wedge (c/g)</th><th>Interpretation</th></tr></thead>
            <tbody>
              <tr><td><strong>High</strong></td><td class="num">+4.17pp</td><td class="num">+3.36pp</td><td class="num"><strong>1.24×</strong></td><td>clickpass over-credits</td></tr>
              <tr><td><strong>Peak</strong></td><td class="num">+0.55pp</td><td class="num">+0.88pp</td><td class="num"><strong>0.62×</strong></td><td>clickpass under-credits</td></tr>
              <tr><td>Mid</td><td class="num">~0.01pp</td><td class="num">~0.005pp</td><td class="num">noise</td><td>noise floor</td></tr>
            </tbody>
          </table>
          <p style="margin-top:0.8em; font-size:0.8em;">Power Line: <em>"Targeting is real. Attribution overstates by 24%."</em></p>
        </section>
        """,

        # SLIDE 3 — Three weaknesses inside Phase 1
        """
        <section data-slide="3">
          <h2>Three weaknesses inside Phase 1</h2>
          <ol style="margin-top:0.5em;">
            <li style="margin-bottom:0.6em;"><strong>Tier collapse.</strong> 4 of 7 advertisers (HexClad, First Watch, Zazzle, Northern Tool) had virtually 100% of their IPs scoring 10,000 on at least one day. MAX-tier construction absorbed peak/mid into high — peak pool reduced to 3 advertisers.</li>
            <li style="margin-bottom:0.6em;"><strong>IVW dominance.</strong> Ancient Nutrition's mid-tier weight single-handedly swung the all-cells overall ATT by <span class="red">±1.17pp</span> (leave-one-out). One advertiser was driving the headline.</li>
            <li style="margin-bottom:0.6em;"><strong>Convenience selection.</strong> The 7 came from TI-835's sufficiency screen — they had data. No spend / vertical / tier-diversity stratification. No defense against "you cherry-picked them."</li>
          </ol>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>Phase 2's job:</strong> rebuild the cohort so these three weaknesses can't be the explanation. Then check whether the wedge story holds.
          </div>
        </section>
        """,

        # SLIDE 4 — Phase 2 cohort design
        """
        <section data-slide="4">
          <h2>The new cohort: 30 advertisers, defensible</h2>
          <p style="margin-top:0.4em; font-size:0.78em; line-height:1.5;">
            <span class="pill">Stage A</span> Characterize the universe (5 BQ queries — spend, vertical, prospecting universe, treatment-side delivery).
            <span class="pill">Stage B</span> Empirical inclusion gates (no picked thresholds — every gate is derived from the data).
            <span class="pill">Stage C</span> Stratified sample across (spend tercile × vertical) cells.
          </p>
          <table>
            <thead><tr><th>Gate</th><th>Threshold</th><th>Why</th></tr></thead>
            <tbody>
              <tr><td>Active in window</td><td class="num">≥100 served IPs</td><td>Must run during analysis week</td></tr>
              <tr><td>Per-tier biddable_holdouts</td><td class="num">≥5,000</td><td>Power calc: ≤0.5pp CI half-width at p∈[0.005, 0.05]</td></tr>
              <tr><td><strong>Tier diversity</strong> (NEW)</td><td class="num">frac_high_only ≤ 0.95</td><td>≥5% of IPs not stuck at score=10,000 — fixes Phase 1 collapse</td></tr>
              <tr><td>Prospecting spend</td><td class="num">≥$5,000 (March)</td><td>Filters dormant advertisers</td></tr>
              <tr><td>Sister-company dedup</td><td class="num">unique audience signature</td><td>Caught Re-Bath ×2 → ×1</td></tr>
            </tbody>
          </table>
          <p style="margin-top:0.5em; font-size:0.7em; color: var(--text-light);">
            Phase 1 anchors Ancient Nutrition + Ferguson kept (the 2 not tier-collapsed). The other 4 collapsed Phase 1 advertisers correctly fail the new gate.
          </p>
        </section>
        """,

        # SLIDE 5 — Pipeline validation
        """
        <section data-slide="5">
          <h2>Pipeline validation — anchors reproduce</h2>
          <p style="margin-top:0.4em;">Same SQL, same window, same statistics. Anchors should reproduce within sampling noise. They do.</p>
          <table style="font-size:0.65em;">
            <thead><tr><th>Advertiser</th><th>Phase 1 (7-adv pipeline)</th><th>Phase 2 (30-adv pipeline)</th><th>Δ</th></tr></thead>
            <tbody>
              <tr><td>Ferguson Home</td><td class="num">+10.55pp</td><td class="num">+10.70pp</td><td class="num"><strong>+0.15pp</strong></td></tr>
              <tr><td>Ancient Nutrition</td><td class="num">+1.76pp</td><td class="num">+1.79pp</td><td class="num"><strong>+0.03pp</strong></td></tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.8em;">
            <strong>The pipeline is deterministic and correct.</strong> Any difference in headline numbers between phases is real — driven by cohort composition, not bugs.
          </div>
          <p style="margin-top:0.6em; font-size:0.7em; color: var(--text-light);">
            Cost: 87 min wall, 126.7 TB billed (same as Phase 1 — augmentor scan dominates and is advertiser-agnostic). 635 slot-hours.
          </p>
        </section>
        """,

        # SLIDE 6 — Headline comparison chart
        f"""
        <section data-slide="6" class="img-slide">
          <h2 style="text-align:left;">The high wedge collapsed.  The peak wedge intensified.</h2>
          <img src="{charts['p1_vs_p2_wedge']}" alt="Phase 1 vs Phase 2 wedge comparison">
        </section>
        """,

        # SLIDE 7 — Why the high wedge collapsed
        """
        <section data-slide="7">
          <h2>Why the high-intent wedge collapsed</h2>
          <p style="margin-top:0.4em;">In Phase 1, MAX-tier construction took the strongest score per IP across the 7-day week. For 4 of 7 advertisers, every IP hit score=10,000 at least once.</p>
          <p style="margin-top:0.6em;"><strong>Result.</strong> Peak/mid IPs got swept into "high." Their visits — already disproportionately credited by clickpass at the per-impression layer — landed in the high-tier bucket. Clickpass at high looked inflated.</p>
          <div class="compare-grid">
            <div class="col p1">
              <h3>Phase 1 high IVW (7 adv)</h3>
              <p>Clickpass +4.17pp · Guid +3.36pp</p>
              <p><strong class="red">Wedge 1.24×</strong> — over-credit</p>
              <p style="font-size:0.7em; color:var(--text-light);">4 of 7 advertisers tier-collapsed</p>
            </div>
            <div class="col p2">
              <h3>Phase 2 high IVW (30 adv)</h3>
              <p>Clickpass +2.59pp · Guid +2.69pp</p>
              <p><strong>Wedge 0.96×</strong> — agree</p>
              <p style="font-size:0.7em; color:var(--text-light);">Tier-diversity gate excludes collapsed</p>
            </div>
          </div>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>At a clean high-intent tier, clickpass and guid agree.</strong> All four pooling methods (IVW, mean, median, sample-weighted) give wedge 0.88-1.00×. The over-credit story doesn't survive a properly-designed cohort.
          </div>
        </section>
        """,

        # SLIDE 8 — Why peak got stronger (with chart)
        f"""
        <section data-slide="8" class="img-slide">
          <h2 style="text-align:left;">Peak: the under-credit got stronger — but only with the right pooling</h2>
          <img src="{charts['peak_pooling']}" alt="Peak pooling methods comparison">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            Phase 2 peak IVW shows wedge 1.00× — looks like nothing. Three other methods (mean, median, sample-weighted) all show clickpass at ~30% of guid.
          </p>
        </section>
        """,

        # SLIDE 9 — Methodology lesson
        """
        <section data-slide="9">
          <h2>Why IVW hides the peak under-credit</h2>
          <p style="margin-top:0.4em;">Inverse-variance weighting gives each cell weight <span class="navy"><strong>1/var = n / [p(1-p)]</strong></span>. A cell with very small ATT and very small visit rate has <strong>vanishing variance</strong> — and gets a <strong>huge IVW weight</strong>.</p>
          <p style="margin-top:0.5em;">Phase 2 peak pool: 8 advertisers (Casper, Re-Bath, NET-A-PORTER, Overjet, Swatch, Longines, Outback, UD-Daniels) have ~0pp ATT in both arms → near-zero variance → outsized weight in the IVW pool. They drag the pooled wedge to 1.00×.</p>
          <p style="margin-top:0.5em;">The other 11 advertisers all show wedge 0.10-0.50× (clickpass under-credits guid). Median: 0.30×. Sample-weighted: 0.34×.</p>
          <div class="takeaway-box" style="margin-top: 0.6em;">
            <strong>The lesson:</strong> IVW is the right tool when cells are well-powered with similar variance. <strong>It collapses to noise-floor cells</strong> when many cells have tiny ATT and tiny variance.<br>
            <span style="color: var(--text-light); font-size: 0.85em;">For peak/mid reporting, prefer <strong>sample-size-weighted</strong> or <strong>median</strong> pooling. Saved to <code>knowledge/experimentation.md</code>.</span>
          </div>
        </section>
        """,

        # SLIDE 10 — Per-advertiser distribution
        f"""
        <section data-slide="10" class="img-slide">
          <h2 style="text-align:left;">93% of advertisers show real high-intent lift</h2>
          <img src="{charts['per_adv_30adv']}" alt="Per-advertiser high-intent guid ATT">
          <p style="font-size:0.7em; color:var(--text-light); text-align:left; margin-top:0.4em;">
            27 of 29 cells pass the 0.5pp gate. 25 of 27 (93%) positive. Range −1.21pp (Outback Presents) to +16.29pp (TurboTenant). Median +2.86pp.
          </p>
        </section>
        """,

        # SLIDE 11 — Dominance fragility eliminated
        """
        <section data-slide="11">
          <h2>IVW dominance fragility — eliminated</h2>
          <p style="margin-top:0.4em;">Leave-one-advertiser-out sensitivity: drop each advertiser, recompute the overall, measure the swing.</p>
          <table>
            <thead><tr><th></th><th>Phase 1 (7 adv)</th><th>Phase 2 (30 adv)</th></tr></thead>
            <tbody>
              <tr><td>Largest LOO swing (any advertiser)</td><td class="num"><strong class="red">±1.17pp</strong> (Ancient Nutrition)</td><td class="num"><strong>none > ±0.05pp</strong></td></tr>
              <tr><td>Largest single-adv weight in high pool</td><td class="num">~40% (Ancient)</td><td class="num">8% (BoggBag)</td></tr>
              <tr><td>Cells flagged sensitive</td><td class="num">1</td><td class="num">0</td></tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.8em;">
            <strong>No single advertiser drives the headline.</strong> The Phase 2 numbers are stable under any reasonable subset of the cohort. Phase 1's headline depended on Ancient Nutrition.
          </div>
        </section>
        """,

        # SLIDE 12 — What this means for clickpass
        """
        <section data-slide="12">
          <h2>What Phase 2 says about clickpass</h2>
          <table style="font-size:0.65em;">
            <thead><tr><th>Tier</th><th>Verdict</th><th>Wedge (median)</th><th>Implication</th></tr></thead>
            <tbody>
              <tr>
                <td><strong>High</strong></td>
                <td><span class="pill pill-green">honest</span></td>
                <td class="num">0.88×</td>
                <td>Clickpass and guid agree. Bill from clickpass; report incrementality from guid; both tell the same story.</td>
              </tr>
              <tr>
                <td><strong>Peak</strong></td>
                <td><span class="pill pill-red">stingy</span></td>
                <td class="num">0.30×</td>
                <td>Clickpass under-credits real lift by ~3×. We may be under-charging at peak intent.</td>
              </tr>
              <tr>
                <td><strong>Mid</strong></td>
                <td><span class="pill pill-gray">noise</span></td>
                <td class="num">noise</td>
                <td>Both estimators near zero. Don't read into ratio.</td>
              </tr>
            </tbody>
          </table>
          <div class="takeaway-box" style="margin-top: 0.8em;">
            <strong>The Phase 1 wedge story isn't dead — it's smaller, cleaner, and asymmetric.</strong> Honest at high intent, stingy at peak intent. The asymmetry is the durable finding.
          </div>
        </section>
        """,

        # SLIDE 13 — Caveats
        """
        <section data-slide="13">
          <h2>What could still be wrong</h2>
          <ol style="margin-top:0.4em;">
            <li style="margin-bottom:0.5em;"><strong>1-day prospecting proxy.</strong> Stage A.1 ran on a single day (2026-04-23) due to external Parquet scan cost. 7-day attempts hit 30+ min wall. Tier composition for the analysis week may differ modestly from the 1-day snapshot.</li>
            <li style="margin-bottom:0.5em;"><strong>Biddability proxy.</strong> Full augmentor scan was skipped ($250-500 saved). Used hash-symmetry: <code>biddable_holdouts ≈ holdouts × biddable_rate</code> with rate=0.30 conservative. Actual ATT pipeline used full augmentor scan — ATT numbers are exact.</li>
            <li style="margin-bottom:0.5em;"><strong>Single-window snapshot.</strong> One 7-day window. Cross-window validation pending (different week or longer window). Augmentor TTL bounds historical replication: only ~3 days back from today are within the live window.</li>
            <li style="margin-bottom:0.5em;"><strong>Visit attribution windows.</strong> 3-day post-period was sufficient for visits. For Phase 2a (conversions), need to align with advertiser-specific attribution windows (typically 7-30 days).</li>
          </ol>
          <p style="margin-top:0.5em; font-size:0.75em; color: var(--text-light);">
            Caveats #1 and #2 affected cohort SELECTION, not the ATT measurement. The 30-advertiser ATT run scanned full prospecting + augmentor + cost_impression + visits — same pipeline as Phase 1.
          </p>
        </section>
        """,

        # SLIDE 14 — What's next
        """
        <section data-slide="14">
          <h2>What's next</h2>
          <ul style="margin-top:0.4em;">
            <li style="margin-bottom:0.6em;"><span class="pill">Phase 2a</span> <strong>Conversions outcome.</strong> Same pipeline, swap <code>ui_conversions</code> for <code>guid_log</code>. Conversions are 10-20× rarer than visits → need ~30-day window for power. Augmentor TTL is the binding constraint — bidder-level ghost bidding (Phase 2b) would solve it.</li>
            <li style="margin-bottom:0.6em;"><span class="pill">Phase 2c</span> <strong>iROAS.</strong> Per-advertiser <code>(incremental conversions × AOV) ÷ MNTN spend</code>. The number Kale and leadership actually want. Depends on Phase 2a + advertiser AOV from <code>ui_conversions.order_amt</code>.</li>
            <li style="margin-bottom:0.6em;"><span class="pill pill-gray">Phase 2b</span> <strong>Bidder-level ghost bidding.</strong> Production solution that escapes the augmentor 10-day TTL. Pending Alex Bloore decision; Zach + Jordan on bidder team.</li>
          </ul>
          <p style="margin-top:0.7em;"><strong>Decision points before Phase 2a starts:</strong></p>
          <ul style="margin-top:0.2em; font-size:0.8em;">
            <li>Cross-window validation first? (Re-run Phase 2 on a different week before extending to conversions.)</li>
            <li>Tighten the biddable-holdout filter? (Currently "any augmentor row" — could require advertiser-targeting match or intent-gate match.)</li>
          </ul>
        </section>
        """,

        # SLIDE 15 — Closing / Power Line
        """
        <section data-slide="15">
          <p class="powerline" style="font-size:1.5em; margin-top:0.5em;">
            The clickpass over-credit was a 7-advertiser artifact.<br>
            The peak under-credit isn't.
          </p>
          <p style="text-align:center; margin-top:1.2em; color: var(--text-light); font-size:0.8em;">
            <strong style="color:var(--navy)">Phase 2 verdict:</strong>
            High-intent attribution is honest.
            Peak-intent attribution under-credits guid by ~3×.
            That's the durable finding from a defensible cohort.
          </p>
          <p style="text-align:center; margin-top:0.8em; color: var(--text-light); font-size:0.65em;">
            30 advertisers · 7-day window · 126.7 TB scanned · 0 single-advertiser dominance flags · 93% positive lift.
          </p>
        </section>
        """,
    ]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>TI-837 Phase 2 — Stress-testing the wedge</title>
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
