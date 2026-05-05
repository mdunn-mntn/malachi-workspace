"""TI-917 — Build the combined RevealJS deck.

Strategy:
  - Read both source standalone decks (TI-837 v5 + TI-884 power).
  - Slice out their <section> blocks (preserving inline base64 charts).
  - Pick the slides we want by source-deck index (per ti_917 plan).
  - Inject new slides for: title, bridge, screening rule, worked example,
    calculator walkthrough, tier lookup, iROAS extension, recap.
  - Embed our 3 new iROAS charts as base64.
  - Emit ONE self-contained HTML using a unified CSS that supports both
    decks' class conventions (.powerline / .power-line / .img-slide /
    .takeaway-box / .pill / .footnote / .center / em-dash bullets).

Outputs:
  artifacts/ti_917_combined_deck.html              — CDN-linked dev version
  artifacts/ti_917_combined_deck_standalone.html   — zero-dep, sharable
"""
import base64
import re
import urllib.request
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
TI917_ROOT = THIS_DIR.parent
EPIC_ROOT = TI917_ROOT.parent

SRC_837 = EPIC_ROOT / "ti_837_implementation_plan" / "artifacts" / "ti_837_phase2_presentation_deck_standalone.html"
SRC_884 = EPIC_ROOT / "ti_884_power_sample_size_analysis" / "artifacts" / "ti_884_power_analysis_deck_standalone.html"

OUT_CDN = THIS_DIR / "ti_917_combined_deck.html"
OUT_STANDALONE = THIS_DIR / "ti_917_combined_deck_standalone.html"

CHART_IROAS = THIS_DIR / "ti_917_chart_iroas_mde_vs_spend.png"
CHART_TIER = THIS_DIR / "ti_917_chart_tier_breakdown.png"
CHART_SIGMA = THIS_DIR / "ti_917_chart_sigma_over_mu.png"


# --------------------------- section extraction ---------------------------

def extract_sections(html_path: Path) -> list[str]:
    """Return all top-level <section>...</section> blocks inside .slides div."""
    text = html_path.read_text()
    # Find the slides container
    m = re.search(r'<div class="slides">(.*?)</div>\s*</div>\s*<script', text, re.DOTALL)
    if not m:
        # TI-884 form: <div class="reveal"><div class="slides"> ... </div></div>
        m = re.search(r'<div class="slides">(.*)</div>\s*</div>', text, re.DOTALL)
    body = m.group(1) if m else ""
    # Naive top-level <section>...</section> split — works because neither
    # source deck uses nested vertical sections.
    sections = []
    depth = 0
    start = None
    i = 0
    while i < len(body):
        if body.startswith("<section", i):
            if depth == 0:
                start = i
            depth += 1
            i = body.find(">", i) + 1
            continue
        if body.startswith("</section>", i):
            depth -= 1
            if depth == 0 and start is not None:
                sections.append(body[start:i + len("</section>")])
                start = None
            i += len("</section>")
            continue
        i += 1
    return sections


# --------------------------- new (TI-917-original) sections ---------------------------

NEW_SLIDES = {
    "00_cold_open": """
<section class="center">
  <p style="font-size: 1.5em; color: var(--navy); font-weight: 700; line-height: 1.4; letter-spacing: -0.01em;">
    Pure prospecting:&nbsp;<span class="gray">zero lift.</span><br>
    Retargeting:&nbsp;<span class="red">+21 points.</span><br>
    <span style="font-size: 0.7em; color: var(--text-light); font-weight: 500;">Same line item.</span>
  </p>
</section>
""",
    "01_powerline": """
<section class="center">
  <h3 style="text-transform: uppercase; letter-spacing: 0.1em; font-size: 0.6em; color: var(--text-light);">TI-917 · BER-2250 · 2026-05-05</h3>
  <p class="power-line" style="margin-top: 1.2em; font-size: 1.6em; line-height: 1.3;">
    Lift is real for retargeting.<br>Measurement is real for visits.
  </p>
  <p class="footnote" style="margin-top: 1.6em;">For TI team. 20 min. Combined v5 results + power primer + screening rule.</p>
</section>
""",
    "03_two_questions": """
<section>
  <h2>Two questions, joined at the hip</h2>
  <div style="margin-top: 1.0em; font-size: 0.95em; line-height: 1.8;">
    <p><strong>1. What is MNTN's lift?</strong> The v5 ghost-bidding result.</p>
    <p><strong>2. Could we measure it if it weren't?</strong> The power analysis.</p>
  </div>
  <div class="takeaway-box" style="margin-top: 1.0em;">
    A measured "no lift" only matters if we had the power to detect lift. The screening rule at the end is what falls out when we read both together.
  </div>
</section>
""",
    "19_screening_rule_visits_cvr": """
<section>
  <h2>The screening rule &mdash; visits and conversions</h2>
  <p style="margin-top: 0.3em; font-size: 0.78em; color: var(--text-light);">Two checks before we promise any readout.</p>
  <div style="margin-top: 0.7em; font-size: 0.85em; line-height: 1.6;">
    <p><span class="pill">1</span> <strong>Visits.</strong> Pull monthly Stage 1 spend.<br>
       <span class="gray" style="margin-left: 2.6em;">Below ~$100k/mo post-stack &rArr; decline. Even the full variance stack can't power this.</span></p>
    <p><span class="pill">2</span> <strong>Conversions.</strong> Pull baseline CVR.<br>
       <span class="gray" style="margin-left: 2.6em;">Post-stack CVR MDE &gt; 10% rel &rArr; decline CVR. Quote visit-rate as upper bound.</span></p>
  </div>
  <div class="takeaway-box" style="margin-top: 0.8em;">
    <strong>Visits clear for 46 of 50 top advertisers. CVR clears for 8.</strong> The drop from visits to CVR is the first wall.
  </div>
</section>
""",
    "20_screening_rule_revenue": """
<section>
  <h2>The screening rule &mdash; revenue and iROAS</h2>
  <p style="margin-top: 0.3em; font-size: 0.78em; color: var(--text-light);">The two harder checks.</p>
  <div style="margin-top: 0.7em; font-size: 0.85em; line-height: 1.6;">
    <p><span class="pill">3</span> <strong>Revenue reported?</strong> Check <code>ui_conversions.order_amt</code> is populated.<br>
       <span class="gray" style="margin-left: 2.6em;">18 of top-50 (36%) report <span class="red">$0</span>. Education, services, lead-gen &mdash; iROAS unmeasurable at any spend.</span></p>
    <p><span class="pill">4</span> <strong>iROAS feasibility.</strong> Compute revenue σ per IP.<br>
       <span class="gray" style="margin-left: 2.6em;">Post-stack iROAS MDE &gt; 10% rel &rArr; decline. <strong>Only 2 of 50 clear this.</strong></span></p>
  </div>
  <div class="takeaway-box" style="margin-top: 0.8em;">
    <strong>Outcome menu, in order of feasibility:</strong> visits (46/50) &rArr; CVR (8/50) &rArr; iROAS (2/50).<br>
    Promise the highest tier that clears. Lead with visits when it's the only one.
  </div>
</section>
""",
    "21_story_csm": """
<section class="center">
  <p style="font-size: 0.95em; color: var(--text-light); font-style: italic; max-width: 78%; margin: 0 auto;">
    Imagine a CS lead pings the team Tuesday morning.
  </p>
  <p style="margin-top: 1.0em; font-size: 1.05em; color: var(--text); max-width: 78%; line-height: 1.5; margin-left: auto; margin-right: auto;">
    &ldquo;Client wants an iROAS readout for advertiser 34835.<br>They&rsquo;re spending $265K a month. Can we?&rdquo;
  </p>
  <p style="margin-top: 1.4em; font-size: 0.9em; color: var(--navy); font-weight: 600;">
    Five minutes. Calculator. Tier CSV.<br>
    <span class="red" style="font-size: 1.05em;">Yes &mdash; with these caveats.</span>
  </p>
  <p class="footer-note" style="margin-top: 1.4em;">The screen turns it into a five-minute conversation, not a five-day analysis.</p>
</section>
""",
    "22_worked_example": """
<section>
  <h2>The answer &mdash; AID 34835, $265k/mo</h2>
  <table style="margin-top: 0.6em; font-size: 0.55em;">
    <thead><tr><th>Step</th><th>Inputs</th><th>Result</th><th>Verdict</th></tr></thead>
    <tbody>
      <tr><td>1. Visits</td><td>n_t = 3.12M, n_c = 346k, p = 4.89%</td><td>1.32% rel MDE</td><td class="navy"><strong>well powered</strong></td></tr>
      <tr><td>2. CVR</td><td>p = 1.98%</td><td>2.10% rel MDE</td><td class="navy"><strong>well powered</strong></td></tr>
      <tr><td>3. Revenue?</td><td>order_amt populated &mdash; μ = $1.41/IP, σ = $13.95</td><td>&mdash;</td><td class="navy"><strong>yes</strong></td></tr>
      <tr><td>4. iROAS</td><td>μ, σ above</td><td>2.95% rel MDE&nbsp;&middot;&nbsp;<strong>min iROAS 0.49</strong></td><td class="navy"><strong>well powered</strong></td></tr>
    </tbody>
  </table>
  <div class="takeaway-box" style="margin-top: 0.7em;">
    <strong>What we promise back:</strong> visit lift, CVR lift, and an iROAS estimate down to a 0.49 floor.<br>
    <span class="gray">For AID 9090 (no revenue reported, no current CVR data), the same screen returns visits only.</span>
  </div>
  <p class="footer-note" style="margin-top: 0.5em;">Source: <code>outputs/ti_917_revenue_mde_per_advertiser.csv</code>. Calculator: <code>ti_884_mde_calculator.py</code>.</p>
</section>
""",
    "23_calculator": """
<section>
  <h2>Calculator &mdash; one function call</h2>
  <p style="margin-top: 0.3em; font-size: 0.78em; color: var(--text-light);"><code>ti_884/artifacts/ti_884_mde_calculator.py</code></p>
<pre style="font-size: 0.6em; line-height: 1.45; margin-top: 0.6em;">from ti_884_mde_calculator import (
    mde_binomial,    # visits, conversions
    mde_continuous,  # revenue / iROAS
    tier_label,
)

# Visits or CVR &mdash; well-powered if rel &lt; 0.05.
_, rel = mde_binomial(n_t=3_117_411, n_c=346_379,
                      p=0.0489, var_reduction=0.595)
print(rel, tier_label(rel))   # 0.0132 well_powered

# Revenue / iROAS &mdash; same shape, takes mu + sigma.
_, rel = mde_continuous(n_t=3_117_411, n_c=346_379,
                        mu=1.4145, sigma=13.9544,
                        var_reduction=0.595)
print(rel)                    # 0.0295</pre>
  <p style="margin-top: 0.6em; font-size: 0.7em; color: var(--text-light);">
    <strong>Defaults:</strong> α=0.05, power=0.8. <code>var_reduction=0.595</code> = canonical post-stack (CUPED × ghost-ad × strat).
  </p>
</section>
""",
    "25_iroas_chart": """
<section class="img-slide">
  <h2 style="text-align: left;">iROAS &mdash; only 2 of 50 well-powered</h2>
  <img src="data:image/png;base64,{IROAS_MDE_B64}" style="max-width: 92%; max-height: 68vh;" />
  <p class="footer-note" style="margin-top: 0.4em;">Per-advertiser revenue MDE (post-stack). Source: <code>outputs/ti_917_revenue_mde_per_advertiser.csv</code>.</p>
</section>
""",
    "26_iroas_thresholds": """
<section>
  <h2>iROAS thresholds &mdash; when can we promise dollar-lift?</h2>
  <div style="margin-top: 0.6em; font-size: 0.9em; line-height: 1.7;">
    <p><strong>Both have to clear:</strong></p>
    <p><span class="pill">1</span> Revenue is reported. 18 of top-50 (36%) report $0.</p>
    <p><span class="pill">2</span> σ/μ is tolerable. Per-IP revenue is heavy-tailed; most advertisers sit between 30× and 200×.</p>
  </div>
  <div class="takeaway-box" style="margin-top: 0.8em;">
    <strong>Promise iROAS only when:</strong> revenue pixel populated, post-stack rel MDE ≤ 10%, and we caveat with the absolute floor (<em>e.g.</em> "min iROAS = 0.49"). Everyone else gets visit lift.
  </div>
</section>
""",
    "28_recap": """
<section>
  <h2>Three things to take away</h2>
  <div style="margin-top: 1.0em; font-size: 0.92em; line-height: 1.9;">
    <p><span class="pill">1</span> <strong>Lift is real for retargeting.</strong> +21pp at high intent. Stage-1 prospecting is ~zero. Aggregates hide both.</p>
    <p><span class="pill">2</span> <strong>Methodology is solved.</strong> Lewis-Rao + ghost-ad + CUPED + strat = 40% SE reduction. Same math for visits, CVR, revenue.</p>
    <p><span class="pill">3</span> <strong>Spend is the binding constraint.</strong> Visits: $100k+. CVR: $2M+. iROAS: revenue pixel + tight σ/μ.</p>
  </div>
</section>
""",
    "29_close": """
<section class="center">
  <p class="power-line" style="font-size: 1.6em; line-height: 1.3;">
    Lift is real for retargeting.<br>Measurement is real for visits.
  </p>
  <p style="margin-top: 1.4em; font-size: 0.95em; color: var(--text); font-weight: 600;">
    Pull every next advertiser through the screen<br>before promising a readout.
  </p>
  <p class="footer-note" style="margin-top: 1.2em;">Calculator: <code>ti_884_mde_calculator.py</code> &middot; tier CSVs: <code>ti_884</code> &amp; <code>ti_917</code>.</p>
</section>
""",
    "A1_appendix_header": """
<section class="center">
  <h2 style="text-align: center; color: var(--text-light); font-weight: 600;">Appendix</h2>
  <p style="margin-top: 1em; font-size: 0.8em; color: var(--text-light);">Methodology depth &mdash; for the record, skipped in main flow.</p>
</section>
""",

    # Replaces TI-837 sl 6 — drops the selection-bias point, keeps the clickpass-vs-guid caution.
    "10_why_retargeting": """
<section>
  <h2>Why retargeting drives 21pp &mdash; and why we should be careful</h2>
  <div style="margin-top: 0.7em; font-size: 0.88em; line-height: 1.55;">
    <p><strong>Clickpass-attributed retargeting reads bigger than guid-attributed.</strong> At high intent, clickpass shows ~24% more lift than guid. <span class="gray">Guid is the ground truth for incremental visits.</span></p>
    <p><strong>The seven-day window understates anything with lag.</strong> Conversions that take longer than 7 days to fire don't show up. Retargeting numbers come from the most pre-qualified IPs, where lag is shortest &mdash; so we're getting the cleanest read here. Stage 1 will read worse with longer windows; retargeting will read mostly the same.</p>
  </div>
  <div class="takeaway-box" style="margin-top: 0.8em;">
    <strong>Conclusion:</strong> retargeting works. Report it as a downstream lift on a pre-qualified population, with guid (not clickpass) as the canonical number.
  </div>
</section>
""",

    # Replaces TI-837 sl 7 — adds explicit 7-day window caveat.
    "11_stage1_zero": """
<section>
  <h2>Stage 1 prospecting alone: zero incremental lift at high intent</h2>
  <div style="margin-top: 0.6em; font-size: 0.88em; line-height: 1.55;">
    <p>Pure Stage 1 prospecting at high intent: <span class="red"><strong>&minus;0.06 pp</strong></span> on guid visits. CI crosses zero.</p>
    <p>The whole MNTN audience product is Stage 1 prospecting. It's our flagship. In v5, on guid visits, it does not move the number.</p>
  </div>
  <div class="takeaway-box" style="margin-top: 0.7em; font-size: 0.85em;">
    <strong>Two reasons not to over-read this:</strong><br>
    <span class="navy">1.</span> Seven-day window. Prospecting effects can ramp over 14&ndash;30 days &mdash; Phase 2a (30-day Databricks) re-runs the same cohort to check.<br>
    <span class="navy">2.</span> Clickpass-attributed Stage 1 <em>does</em> show lift. The audience product moves attribution credit, not the count of visits in the seven-day window.
  </div>
</section>
""",

    # Replaces TI-884 sl 10 — reconciles $2M vs $5M as floor vs target.
    "17_cvr_wall": """
<section>
  <h2>Conversion-rate measurement is in another league</h2>
  <table style="margin-top: 0.6em; font-size: 0.6em;">
    <thead><tr><th>Operating point</th><th>What it buys</th><th>Min monthly Stage 1 spend</th></tr></thead>
    <tbody>
      <tr><td>Floor</td><td>CVR experiment is <em>possible</em> at all (~10% rel MDE)</td><td><strong>$2M+</strong></td></tr>
      <tr><td>Target</td><td>5% rel MDE at the cohort median &mdash; the well-powered bar</td><td><strong>$5M+</strong></td></tr>
      <tr><td>Tight</td><td>2% rel MDE at the cohort median</td><td><strong>$30M+</strong></td></tr>
    </tbody>
  </table>
  <div class="takeaway-box" style="margin-top: 0.7em;">
    <strong>38 of 47 top-50 advertisers are underpowered for CVR experiments at any current spend.</strong><br>
    <span class="gray">We have one advertiser at $5M+ Stage 1.</span> Floor is $2M; the well-powered bar is $5M; nobody hits "tight."
  </div>
</section>
""",

    # NEW educational slide: the inversion (rate → spend)
    "23b_spend_inversion": """
<section>
  <h2>From baseline rate to minimum spend &mdash; the inversion</h2>
  <p style="margin-top: 0.3em; font-size: 0.78em; color: var(--text-light);">Same Lewis-Rao, solved for n (then dollars) instead of MDE.</p>
<pre style="font-size: 0.6em; line-height: 1.45; margin-top: 0.6em;">spend_required(p, target_mde_rel, cpm, imps_per_ip, var_reduction)

# What it does, in three lines:
n_total      = (z &middot; sigma &middot; var_red / target_mde_abs)^2 / (h &middot; (1 - h))
impressions  = n_total &middot; (1 - h) &middot; imps_per_ip   # only treated arm gets served
spend        = impressions &middot; cpm / 1000</pre>
  <div class="takeaway-box" style="margin-top: 0.7em; font-size: 0.85em;">
    <strong>What dominates:</strong> baseline rate <em>p</em>. Sigma scales as &radic;p(1-p), and the inversion squares it. <span class="navy">Halving p roughly quadruples required spend.</span> CPM and imps/IP move spend linearly &mdash; rate dominates.
  </div>
  <p class="footer-note" style="margin-top: 0.4em;">Holdout fixed at 10%. Variance reduction defaults raw (1.0); pass 0.595 for the canonical post-stack.</p>
</section>
""",

    # NEW educational slide: the recommendation table
    "23c_spend_bands": """
<section>
  <h2>Recommended monthly Stage 1 spend by baseline rate</h2>
  <p style="margin-top: 0.2em; font-size: 0.78em; color: var(--text-light);">Target 5% relative MDE &middot; $25 CPM &middot; 10 imps/IP &middot; 10% holdout.</p>
  <table style="margin-top: 0.5em; font-size: 0.55em;">
    <thead><tr><th>Baseline rate (p)</th><th>Spend &mdash; raw</th><th>Spend &mdash; post-stack</th><th>Where this hits</th></tr></thead>
    <tbody>
      <tr><td>0.01% CVR</td><td>$78M</td><td>$28M</td><td>nobody at MNTN</td></tr>
      <tr><td>0.1% CVR</td><td>$7.8M</td><td>$2.8M</td><td>typical CVR floor</td></tr>
      <tr><td>0.5% CVR</td><td>$1.6M</td><td>$553k</td><td>high-CVR commerce</td></tr>
      <tr><td>1% (low IVR)</td><td>$777k</td><td>$275k</td><td>low-traffic verticals</td></tr>
      <tr><td>2% IVR</td><td>$385k</td><td>$136k</td><td><strong>typical IVR / cohort median</strong></td></tr>
      <tr><td>5% IVR</td><td>$149k</td><td>$53k</td><td>high-rate advertisers</td></tr>
      <tr><td>10% IVR</td><td>$71k</td><td>$25k</td><td>very high-rate (e.g. WGU)</td></tr>
    </tbody>
  </table>
  <div class="takeaway-box" style="margin-top: 0.6em; font-size: 0.78em;">
    <strong>How to use this:</strong> pull the advertiser's IVR and CVR. Look up the row. Multiply by their CPM/<span class="gray">$25</span> and 10/<span class="gray">imps/IP</span> if they're off-default. <strong>Post-stack column is the ask</strong> &mdash; the variance stack is canonical.
  </div>
</section>
""",

    # Replaces TI-884 sl 12 — strips named reference, keeps and tightens the substance.
    "17_what_this_means": """
<section>
  <h2>What this means &mdash; in three lines</h2>
  <ul style="margin-top: 0.7em; font-size: 0.85em; line-height: 1.65;">
    <li><span class="navy">Budget thresholds:</span> <strong>$100k+/month post-stack for visits</strong>; <strong>$2M floor / $5M target for CVR</strong>; iROAS only when revenue is reported and σ/μ cooperates.</li>
    <li><span class="navy">Recruiting an experiment cohort:</span> use the post-stack tier from <code>ti_884_top50_mde_tiers.csv</code> (visits/CVR) and <code>ti_917_revenue_mde_per_advertiser.csv</code> (iROAS). Calculator gates anyone outside the top-50.</li>
    <li><span class="navy">Stakeholder communication:</span> stop reporting "Lift %" without the matching MDE confidence band. <strong>Anything below MDE is noise &mdash; even when it looks clean.</strong></li>
  </ul>
  <div class="takeaway-box" style="margin-top: 0.7em;">
    Re-frame the conversation: incrementality measurement is a <strong>budget</strong> question, not a methodology question. Methodology is solved.
  </div>
</section>
""",

    # Tighter appendix replacement for "What I'd want a methodologist to push on"
    "A2_caveats": """
<section>
  <h2>Caveats &mdash; what to push on</h2>
  <div style="margin-top: 0.6em; font-size: 0.85em; line-height: 1.6;">
    <p><strong>1. Tier collapse.</strong> Max-household-score across the week pushes most IPs into the high tier; per-tier peak/mid pools are thin (only 3 of 7 advertisers).</p>
    <p><strong>2. Loose biddable filter.</strong> "Appeared in augmentor" is the floor; tighter intent-or-HHST gates are deferred. Treated arm has the same bias, so internally consistent &mdash; but ATT <em>levels</em> may shift.</p>
    <p><strong>3. Pooling sensitivity.</strong> All-cells inverse-variance pool is dominated by mid-tier low-rate cells. Leave-one-out can swing it 1pp. Per-tier numbers are what we lead with.</p>
  </div>
  <p class="footer-note" style="margin-top: 0.6em;">None of these break the conclusion. They temper the magnitudes.</p>
</section>
""",
}


# --------------------------- assembly plan ---------------------------

# (source, index) — index is 0-based into the section list of that source.
# Source = "837" or "884" or "new:<key>" (key into NEW_SLIDES).
PLAN = [
    # Section 1 — Hook & frame (3)
    ("new", "00_cold_open"),         # Pure prospecting: 0. Retargeting: +21. Same line item.
    ("new", "01_powerline"),         # Title + Power Line
    ("new", "03_two_questions"),     # Bridge: lift vs measurability
    # Section 2 — Methodology (4)
    ("837", 1),                      # What we measured — ghost-bidding ATT
    ("837", 10),                     # How the pipeline works — end-to-end
    ("837", 2),                      # How the 4 segments are defined
    ("837", 3),                      # Retargeting drives the lift; prospecting drives almost none
    # Section 3 — Results (4; selection-bias point removed, attribution wedge moved out of main flow)
    ("837", 4),                      # The 4-segment headline numbers
    ("837", 5),                      # Lift profile by tier
    ("new", "10_why_retargeting"),   # Replaces 837[6] — drops selection bias, adds windowing nuance
    ("new", "11_stage1_zero"),       # Replaces 837[7] — adds 7-day window caveat explicitly
    # Section 4 — Power (3)
    ("884", 1),                      # Last quarter, MNTN ran 7 incrementality tests
    ("884", 5),                      # If those tests were noise, what scale do we need
    ("884", 19),                     # Variance-reduction stack — 40% SE reduction
    # Section 5 — Spend thresholds (3)
    ("884", 7),                      # Visit-rate measurability emerges around $200k/month
    ("new", "17_cvr_wall"),          # Replaces 884[10] — reconciles $2M floor / $5M target / $30M tight
    ("new", "17_what_this_means"),   # Replaces 884[12] — strips named reference, tightens substance
    # Section 6 — Min-spend rule (instructional, 9)
    ("new", "19_screening_rule_visits_cvr"),  # Steps 1-2: visits + CVR
    ("new", "20_screening_rule_revenue"),      # Steps 3-4: revenue + iROAS
    ("new", "21_story_csm"),                   # Hall framework: a CS lead's question
    ("new", "22_worked_example"),              # The five-minute answer (table)
    ("new", "23_calculator"),                  # MDE direction (rate → MDE)
    ("new", "23b_spend_inversion"),            # NEW: spend direction (rate → spend) — the inversion
    ("new", "23c_spend_bands"),                # NEW: recommendation table across IVR/CVR bands
    ("new", "25_iroas_chart"),                 # Per-advertiser iROAS power chart
    ("new", "26_iroas_thresholds"),            # Two binding constraints
    # Section 7 — Close (3)
    ("837", 13),                     # What's next (TI-885, bidder-level)
    ("new", "28_recap"),             # Three things to take away
    ("new", "29_close"),             # Power Line + CTA
    # Appendix (4)
    ("new", "A1_appendix_header"),
    ("new", "A2_caveats"),           # Tighter replacement for 837[12]
    ("837", 8),                      # Attribution wedge (kept here for the curious)
    ("884", 13),                     # How "power" is calculated, from first principles
]


# --------------------------- unified CSS ---------------------------

UNIFIED_CSS = """
:root {
  --navy: #1B2A4A; --blue: #2E5090; --mid: #5A7DB5;
  --light: #A8BDD9; --muted: #C8CDD4; --light-gray: #C8CDD4;
  --gray: #888; --red: #D63B2F; --bg: #FAFAFA;
  --text: #222; --text-light: #666;
}
.reveal { font-size: 30px; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: var(--text); background: var(--bg); }
.reveal section { text-align: left; }
.reveal section.center { text-align: center; }
.reveal h1 { font-size: 1.8em; color: var(--navy); margin-top: 0; text-align: left; line-height: 1.15; font-weight: 700; }
.reveal h2 { font-size: 1.3em; color: var(--navy); margin-top: 0; text-align: left; font-weight: 700; line-height: 1.2; letter-spacing: -0.01em; }
.reveal h3 { font-size: 1em; color: var(--text); margin-top: 0; font-weight: 600; }
.reveal p { font-size: 0.85em; line-height: 1.45; color: var(--text); }
.reveal ul, .reveal ol { font-size: 0.85em; line-height: 1.5; }
.reveal em { color: var(--text-light); font-style: italic; }
.reveal strong { color: var(--navy); }
.reveal .red { color: var(--red); font-weight: 700; }
.reveal .gray { color: var(--text-light); }
.reveal .navy { color: var(--navy); font-weight: 700; }
.reveal .powerline,
.reveal .power-line { color: var(--navy); font-weight: 700; font-size: 1.2em; line-height: 1.4; text-align: center; }
.reveal .quote-block { font-size: 1.05em; line-height: 1.5; padding-left: 1em; border-left: 4px solid var(--navy); margin: 0.6em 0; }
.reveal .footer-note,
.reveal .footnote { color: var(--text-light); font-size: 0.55em; text-align: center; margin-top: 0.6em; font-style: italic; }
.reveal .subtitle { font-size: 0.6em; color: var(--text-light); margin-top: 0.5em; }
.reveal table { font-size: 0.55em; border-collapse: collapse; width: 100%; margin: 0.4em auto 0; }
.reveal table th, .reveal table td { padding: 0.35em 0.6em; border-bottom: 1px solid #DDD; text-align: left; }
.reveal table th { color: white; background: var(--navy); border-bottom: 2px solid var(--navy); font-weight: 600; }
.reveal table td.num { text-align: right; font-variant-numeric: tabular-nums; }
.reveal table tr.bad td { color: var(--red); font-weight: 600; }
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
.reveal pre { padding: 0.6em 0.8em; line-height: 1.4; color: var(--text); display: block; white-space: pre-wrap; }
.reveal img { background: var(--bg); border: none; box-shadow: none; max-height: 600px; }
"""


# --------------------------- HTML build ---------------------------

HEADER_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>TI-917 — Incrementality findings + screening rule</title>
{REVEAL_CSS_BLOCK}
<style>{UNIFIED_CSS}</style>
</head>
<body>
<div class="reveal"><div class="slides">
"""

FOOTER_TEMPLATE = """
</div></div>
{REVEAL_JS_BLOCK}
<script>
Reveal.initialize({
    hash: true,
    slideNumber: true,
    controls: true,
    progress: true,
    center: true,
    transition: 'fade',
    transitionSpeed: 'fast',
    width: 1200,
    height: 800,
    margin: 0.04,
    minScale: 0.2,
    maxScale: 1.6,
});
</script>
</body>
</html>
"""

CDN_CSS = """<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css">"""
CDN_JS = """<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"></script>"""


def b64_image(p: Path) -> str:
    return base64.b64encode(p.read_bytes()).decode()


def main():
    print(f"[INFO] reading source decks…")
    s837 = extract_sections(SRC_837)
    s884 = extract_sections(SRC_884)
    print(f"[INFO]   ti_837: {len(s837)} sections")
    print(f"[INFO]   ti_884: {len(s884)} sections")

    iroas_b64 = b64_image(CHART_IROAS)
    tier_b64 = b64_image(CHART_TIER)
    sigma_b64 = b64_image(CHART_SIGMA)

    new_filled = {
        k: v.replace("{IROAS_MDE_B64}", iroas_b64)
            .replace("{TIER_BREAKDOWN_B64}", tier_b64)
            .replace("{SIGMA_MU_B64}", sigma_b64)
        for k, v in NEW_SLIDES.items()
    }

    body_parts = []
    for source, ref in PLAN:
        if source == "new":
            body_parts.append(new_filled[ref])
        elif source == "837":
            if ref >= len(s837):
                raise IndexError(f"ti_837 has {len(s837)} sections; requested {ref}")
            body_parts.append(s837[ref])
        elif source == "884":
            if ref >= len(s884):
                raise IndexError(f"ti_884 has {len(s884)} sections; requested {ref}")
            body_parts.append(s884[ref])
        else:
            raise ValueError(f"unknown source: {source}")

    body = "\n\n".join(body_parts)

    # CDN-linked dev version
    cdn_html = (
        HEADER_TEMPLATE.replace("{REVEAL_CSS_BLOCK}", CDN_CSS).replace("{UNIFIED_CSS}", UNIFIED_CSS)
        + body
        + FOOTER_TEMPLATE.replace("{REVEAL_JS_BLOCK}", CDN_JS)
    )
    OUT_CDN.write_text(cdn_html)
    print(f"[OK] {OUT_CDN.name}  ({len(cdn_html):,} bytes, {len(PLAN)} slides)")

    # Standalone version — inline CDN files
    revcss_path = Path("/tmp/reveal.css")
    whitecss_path = Path("/tmp/reveal_white.css")
    revjs_path = Path("/tmp/reveal.js")
    targets = [
        (revcss_path, "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css"),
        (whitecss_path, "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css"),
        (revjs_path, "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js"),
    ]
    for path, url in targets:
        if not path.exists():
            print(f"[INFO] downloading {url}")
            with urllib.request.urlopen(url) as resp:
                path.write_bytes(resp.read())

    inline_css_block = (
        f"<style>{revcss_path.read_text()}</style>\n"
        f"<style>{whitecss_path.read_text()}</style>"
    )
    inline_js_block = f"<script>{revjs_path.read_text()}</script>"

    standalone_html = (
        HEADER_TEMPLATE.replace("{REVEAL_CSS_BLOCK}", inline_css_block).replace("{UNIFIED_CSS}", UNIFIED_CSS)
        + body
        + FOOTER_TEMPLATE.replace("{REVEAL_JS_BLOCK}", inline_js_block)
    )
    OUT_STANDALONE.write_text(standalone_html)
    print(f"[OK] {OUT_STANDALONE.name}  ({len(standalone_html):,} bytes, {len(PLAN)} slides)")


if __name__ == "__main__":
    main()
