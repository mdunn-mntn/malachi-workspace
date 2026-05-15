"""Build the standalone Power Analysis Workshop deck.

Produces a single self-contained HTML file (RevealJS via CDN, charts inlined
as base64 PNGs). Read by share_deck.sh to publish via githack.

Inputs:
  - charts/ti_xxx_chart_*.png  (from generate_charts.py)

Output:
  - ti_xxx_workshop_deck.html
"""
import base64
from pathlib import Path

HERE = Path(__file__).parent
CHARTS = HERE / "charts"

def b64(name):
    return base64.b64encode((CHARTS / name).read_bytes()).decode()

charts = {
    "spend":    b64("ti_xxx_chart_spend_curve.png"),
    "tier":     b64("ti_xxx_chart_tier_waterfall.png"),
    "noise":    b64("ti_xxx_chart_noise_reveal.png"),
    "states":   b64("ti_xxx_chart_four_states.png"),
    "pool":     b64("ti_xxx_chart_pool_or_nothing.png"),
    "overlap":  b64("ti_xxx_chart_distribution_overlap.png"),
}

CSS = """
:root {
  --navy: #1B2A4A; --blue: #2E5090; --mid: #5A7DB5;
  --light: #A8BDD9; --muted: #C8CDD4; --gray: #888;
  --red: #D63B2F; --green: #2A7A3B; --amber: #B57F00;
  --bg: #FAFAFA; --text: #222; --text-light: #666;
}
.reveal {
  font-size: 30px;
  font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
  color: var(--text); background: var(--bg);
}
.reveal section { text-align: left; }
.reveal h1 { font-size: 1.7em; color: var(--navy); margin-top: 0; text-align: left;
             line-height: 1.15; font-weight: 700; letter-spacing: -0.01em; }
.reveal h2 { font-size: 1.25em; color: var(--navy); margin-top: 0; text-align: left;
             font-weight: 700; line-height: 1.2; letter-spacing: -0.01em; margin-bottom: 0.4em; }
.reveal h3 { font-size: 0.95em; color: var(--text); margin-top: 0; font-weight: 600; }
.reveal p { font-size: 0.85em; line-height: 1.45; color: var(--text); margin: 0.4em 0; }
.reveal ul, .reveal ol { font-size: 0.85em; line-height: 1.5; margin: 0.4em 0 0 1em; padding: 0; }
.reveal li { margin: 0.3em 0; }
.reveal .red   { color: var(--red);   font-weight: 700; }
.reveal .green { color: var(--green); font-weight: 700; }
.reveal .amber { color: var(--amber); font-weight: 700; }
.reveal .navy  { color: var(--navy);  font-weight: 700; }
.reveal .muted { color: var(--text-light); font-weight: 400; }
.reveal .footer { position: absolute; bottom: 12px; left: 40px; right: 40px;
                  font-size: 12px; color: var(--text-light); }
.reveal .powerline {
  font-size: 1.6em; color: var(--navy); font-weight: 700;
  line-height: 1.25; padding: 0.6em 0;
  border-left: 6px solid var(--red); padding-left: 0.7em;
}
.reveal .hero { font-size: 3.2em; color: var(--red); font-weight: 700; line-height: 1; }
.reveal .hero-sub { font-size: 0.85em; color: var(--text-light); margin-top: 0.4em; }
.reveal table { font-size: 0.65em; border-collapse: collapse; width: 100%; margin-top: 0.5em; }
.reveal th, .reveal td { padding: 6px 10px; border-bottom: 1px solid #DDD;
                          text-align: left; vertical-align: top; }
.reveal th { color: var(--navy); font-weight: 700; border-bottom: 2px solid var(--navy); }
.reveal td.r { text-align: right; font-variant-numeric: tabular-nums; }
.reveal img.chart { max-width: 100%; max-height: 78vh; display: block; margin: 0 auto; }
.reveal .lever-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 18px 28px; font-size: 0.85em; }
.reveal .lever-card { padding: 12px 16px; border-left: 4px solid var(--blue); background: white; }
.reveal .lever-card h3 { color: var(--blue); font-size: 0.95em; margin-bottom: 0.3em; }
.reveal .lever-card p { font-size: 0.9em; color: var(--text-light); }
.reveal .quote { font-size: 1.05em; font-style: italic; color: var(--navy);
                 padding: 0.4em 0 0.4em 0.8em; border-left: 4px solid var(--mid); }
.reveal .quote-attr { font-size: 0.7em; font-style: normal; color: var(--text-light); margin-top: 0.4em; }
.reveal .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
.reveal .card { padding: 14px 18px; background: white; border: 1px solid #E5E7EB; border-radius: 4px; }
.reveal .card h3 { color: var(--navy); margin-bottom: 0.3em; }
.reveal .three-q { font-size: 0.95em; }
.reveal .three-q li { margin: 0.6em 0; padding-left: 0.4em; }
.reveal .three-q strong { color: var(--navy); }
.reveal .label-row { display: flex; gap: 16px; align-items: baseline; margin: 0.3em 0; }
.reveal .label-row .lbl { color: var(--text-light); font-size: 0.75em; min-width: 140px; }
.reveal .formula { font-family: 'Courier New', monospace; background: #FFF; padding: 8px 12px;
                   border-left: 3px solid var(--navy); font-size: 0.85em; margin: 0.4em 0; }
"""

# ----- Slides -----

SLIDES = []

def add(html):
    SLIDES.append(html.strip())

# 1. Title
add(f"""
<section>
  <h1>Power Analysis at MNTN</h1>
  <h2 class="muted" style="font-weight:400;margin-top:0.3em;">
    What it is, what it means for incremental lift, and how to screen advertisers before we run a test.
  </h2>
  <p style="margin-top:2em;font-size:0.85em;color:var(--text-light);">
    Workshop · 45–60 min · Targeting Infrastructure
  </p>
  <p style="font-size:0.85em;color:var(--text-light);margin-top:0;">Malachi Dunn</p>
</section>
""")

# 2. Cold open
add("""
<section>
  <h2>One number on a slide. Show of hands.</h2>
  <div style="margin-top:1.2em;display:grid;grid-template-columns:1fr 1fr;gap:36px;align-items:center;">
    <div>
      <div class="hero">+0.72%</div>
      <p class="hero-sub">
        Visit-rate lift reported on a recent MNTN pilot.<br>
        Advertiser: Ownerly. n = 1.49M treated IPs. 95% CI.
      </p>
    </div>
    <div>
      <h3 class="navy">Would you ship this?</h3>
      <p>
        Pause for the room. Hands for "yes ship it." Hands for "no."
        Hands for "I need more info." We come back to this answer in 45 minutes.
      </p>
    </div>
  </div>
</section>
""")

# 3. Power Line
add("""
<section>
  <h2 class="muted">The whole workshop, in one sentence:</h2>
  <p class="powerline">
    Power is the question you have to answer <em>before</em> you run the test &mdash; not after.
  </p>
  <p style="margin-top:1.6em;">
    Today we'll build the math, the calculator, and the screening rule so you can answer it for any advertiser in two minutes.
  </p>
</section>
""")

# 4. What we'll cover
add("""
<section>
  <h2>The plan</h2>
  <ol>
    <li><strong>Act 1 &middot; 8 min.</strong> The cold open and why it should bother you.</li>
    <li><strong>Act 2 &middot; 15 min.</strong> What power is. Four states, four levers, one trap.</li>
    <li><strong>Act 3a &middot; 8 min.</strong> From power to MDE. The spend &rarr; MDE curve.</li>
    <li><strong>Act 3b &middot; 20 min.</strong> <span class="red">Hands-on calculator drills.</span></li>
    <li><strong>Act 3c &middot; 5 min.</strong> The screening rule. Three questions to ask before launch.</li>
    <li><strong>Close &middot; 2 min.</strong> Back to the cold open. Different answer.</li>
  </ol>
</section>
""")

# === Act 2: What power is ===

# 5. Four states
add(f"""
<section>
  <h2>Four possible outcomes from a lift test</h2>
  <img class="chart" src="data:image/png;base64,{charts['states']}">
  <div class="footer">
    α controls how often we cry wolf. β controls how often we miss the real thing. Power = 1 &minus; β.
  </div>
</section>
""")

# 6. Power = 1 - beta with distributions
add(f"""
<section>
  <h2>Power is how cleanly two distributions separate</h2>
  <img class="chart" src="data:image/png;base64,{charts['overlap']}">
  <div class="footer">
    Mouse-diet visual from StatQuest, redrawn on MNTN axes (treated visit rate &minus; holdout visit rate).
  </div>
</section>
""")

# 7. Four levers — overview
add("""
<section>
  <h2>You have four levers</h2>
  <div class="lever-grid">
    <div class="lever-card">
      <h3>Sample size &uarr;</h3>
      <p>More impressions, more IPs, more spend. The lever we usually pull.</p>
    </div>
    <div class="lever-card">
      <h3>Variance &darr;</h3>
      <p>CUPED, ghost-ad conditioning, stratification. Combined ≈ 40% SE reduction.</p>
    </div>
    <div class="lever-card">
      <h3>Effect size &uarr;</h3>
      <p>Retargeting at +21 pp is easy. Pure prospecting at &lt;1 pp is hard.</p>
    </div>
    <div class="lever-card">
      <h3>Alpha &uarr;</h3>
      <p>Loosen the false-positive bar. We don't &mdash; the room ships on this.</p>
    </div>
  </div>
</section>
""")

# 8. The trap
add("""
<section>
  <h2>The trap nobody talks about</h2>
  <p class="quote">
    An underpowered test usually fails to reject the null &mdash; even when there's a real effect.
    A null result from an underpowered test is meaningless.
  </p>
  <p style="margin-top:1.2em;">
    "We saw no significant lift" is <span class="red">not</span> "there is no lift."
    Without power, it's "we didn't have the statistical equipment to see it."
  </p>
  <p class="muted" style="margin-top:1em;">
    This is the bridge to the screening rule. We need to know <em>before</em> the test whether we'd have detected the lift we expect.
  </p>
</section>
""")

# === Act 3a: From power to MDE ===

# 9. Lewis-Rao
add("""
<section>
  <h2>From "I want power" to "what's the smallest lift I can see?"</h2>
  <p>Solve the Lewis-Rao power equation for the effect size:</p>
  <div class="formula">
    MDE<sub>abs</sub> &nbsp;=&nbsp; (z<sub>α/2</sub> + z<sub>power</sub>) &middot; σ &middot; &radic;(1/n<sub>t</sub> + 1/n<sub>c</sub>) &middot; var_reduction
  </div>
  <p>At MNTN defaults: α = 0.05, power = 0.80 &nbsp;&rarr;&nbsp; z<sub>α/2</sub> + z<sub>power</sub> = <strong>2.80</strong>.</p>
  <p>For visits / CVR (binomial): σ = &radic;(p &middot; (1 &minus; p)).</p>
  <p style="margin-top:1em;" class="muted">
    Given the N we <em>have</em> (spend &rarr; impressions &rarr; IPs), we solve for the smallest lift we could reliably detect. That's the MDE.
  </p>
</section>
""")

# 10. Spend curve
add(f"""
<section>
  <h2>Spend &rarr; MDE: the curve</h2>
  <img class="chart" src="data:image/png;base64,{charts['spend']}">
  <div class="footer">
    Hero chart. The whole rest of the workshop is figuring out where on this curve a given advertiser sits.
  </div>
</section>
""")

# 11. MNTN ρ
add("""
<section>
  <h2>The variance stack is real &mdash; and weaker than the papers say</h2>
  <table>
    <thead>
      <tr><th>Advertiser</th><th class="r">CUPED ρ</th><th class="r">SE multiplier &radic;(1 &minus; ρ²)</th></tr>
    </thead>
    <tbody>
      <tr><td>WGU (31357)</td><td class="r">0.461</td><td class="r">0.887</td></tr>
      <tr><td>Ferguson Home (31276)</td><td class="r">0.441</td><td class="r">0.897</td></tr>
      <tr><td>Vivint (30506)</td><td class="r">0.170</td><td class="r">0.985</td></tr>
      <tr><td><strong>Cohort mean</strong></td><td class="r"><strong>0.357</strong></td><td class="r"><strong>0.934</strong></td></tr>
      <tr><td class="muted">Published benchmark (Deng et al.)</td><td class="r muted">≈ 0.5</td><td class="r muted">0.866</td></tr>
    </tbody>
  </table>
  <p style="margin-top:1em;">
    Combined post-stack multiplier:
    <span class="formula" style="display:inline-block;padding:2px 8px;">0.934 &times; 0.75 &times; 0.85 = 0.595</span>
    <span class="muted">&nbsp; (CUPED &times; ghost-ad &times; stratified)</span>
  </p>
</section>
""")

# === Act 3b: Calculator drills ===

# 12. Drill intro
add("""
<section>
  <h2>Open the calculator</h2>
  <p style="font-size:1.0em;">
    <span class="navy" style="font-family:'Courier New',monospace;font-size:0.85em;word-break:break-all;">
      https://gist.githack.com/mdunn-mntn/34c2828f4288d123f5bfaf60f08bc244/raw/ti_xxx_mde_calculator.html
    </span>
  </p>
  <p>We'll run three drills. Inputs come from the screening_examples CSV. The calculator returns MDE for visits and CVR, raw and post-stack, and tells you whether the test would have been runnable.</p>
  <p class="muted" style="margin-top:1em;">
    Pair up. Plug in advertisers as we hit each drill. Numbers should match what's on the slide.
  </p>
</section>
""")

# 13. Drill 1 — visits
add("""
<section>
  <h2>Drill 1 &middot; Visits power</h2>
  <p>Plug each advertiser in, read the visits post-stack MDE.</p>
  <table>
    <thead><tr><th>Advertiser</th><th class="r">Monthly $</th><th class="r">Treated IPs</th><th class="r">Baseline visit %</th><th class="r">Visits MDE (post-stack)</th><th>Verdict</th></tr></thead>
    <tbody>
      <tr><td>WGU</td><td class="r">$3.35M</td><td class="r">15.6M</td><td class="r">9.66%</td><td class="r"><strong>0.41%</strong></td><td><span class="green">well-powered</span></td></tr>
      <tr><td>Ferguson Home</td><td class="r">$812k</td><td class="r">5.1M</td><td class="r">17.77%</td><td class="r"><strong>0.50%</strong></td><td><span class="green">well-powered</span></td></tr>
      <tr><td>Vivint</td><td class="r">$1.76M</td><td class="r">21.2M</td><td class="r">0.39%</td><td class="r"><strong>1.84%</strong></td><td><span class="green">well-powered</span></td></tr>
      <tr><td>Hugo Insurance</td><td class="r">$81k</td><td class="r">0.44M</td><td class="r">0.32%</td><td class="r"><strong>~13%</strong></td><td><span class="red">underpowered</span></td></tr>
    </tbody>
  </table>
  <p style="margin-top:0.8em;"><strong>Takeaway:</strong> Three pass, one fails. Spend buys IPs. IPs buy power.</p>
</section>
""")

# 14. Drill 2 — CVR (Vivint flips)
add("""
<section>
  <h2>Drill 2 &middot; Same advertisers, but CVR</h2>
  <p>Don't change inputs. Just read the CVR post-stack MDE.</p>
  <table>
    <thead><tr><th>Advertiser</th><th class="r">Baseline CVR</th><th class="r">CVR MDE (post-stack)</th><th>Verdict</th></tr></thead>
    <tbody>
      <tr><td>WGU</td><td class="r">0.59%</td><td class="r">1.7%</td><td><span class="green">well-powered</span></td></tr>
      <tr><td>Ferguson Home</td><td class="r">1.81%</td><td class="r">1.5%</td><td><span class="green">well-powered</span></td></tr>
      <tr><td>Vivint</td><td class="r">0.04%</td><td class="r"><span class="red">~18%</span></td><td><span class="red">flips to underpowered</span></td></tr>
      <tr><td>Hugo Insurance</td><td class="r">0.002%</td><td class="r"><span class="red">≫ 100%</span></td><td><span class="red">unrunnable</span></td></tr>
    </tbody>
  </table>
  <p style="margin-top:0.8em;">
    <strong>Vivint flipped.</strong> Same advertiser, same spend, same N &mdash; but the baseline rate is 25&times; smaller, so &radic;(p(1&minus;p))/p explodes. <strong>Power is metric-specific, not advertiser-specific.</strong>
  </p>
</section>
""")

# 15. Drill 3 — Ownerly
add(f"""
<section>
  <h2>Drill 3 &middot; "Should we have run this?"</h2>
  <div class="grid-2">
    <div>
      <h3>Inputs</h3>
      <p class="label-row"><span class="lbl">Advertiser</span> Ownerly (44630)</p>
      <p class="label-row"><span class="lbl">Spend</span> $265k / month</p>
      <p class="label-row"><span class="lbl">Treated IPs</span> 1.49M</p>
      <p class="label-row"><span class="lbl">Baseline visit rate</span> 1.48%</p>
      <p class="label-row"><span class="lbl">Reported lift</span> +0.72% relative</p>
      <h3 style="margin-top:0.8em;">Calculator output</h3>
      <p class="label-row"><span class="lbl">Visits MDE raw</span> <span class="red">5.93%</span></p>
      <p class="label-row"><span class="lbl">Visits MDE post-stack</span> <span class="red">3.53%</span></p>
    </div>
    <div>
      <img class="chart" src="data:image/png;base64,{charts['noise']}">
    </div>
  </div>
  <p style="margin-top:0.8em;">
    Reported 0.72% vs MDE 3.53% &mdash; the reported number is <span class="red"><strong>4.7&times; below</strong></span> what the test could detect.
    Whatever it found, it wasn't lift.
  </p>
</section>
""")

# 16. Pool-or-nothing
add(f"""
<section>
  <h2>What if every advertiser is too small?</h2>
  <img class="chart" src="data:image/png;base64,{charts['pool']}">
  <div class="footer">
    TI-933 Select cohort. 23 individually-underpowered advertisers. Pooled lift +2.055 pp [+2.011, +2.100].
    Pool, extend the window, or larger holdouts &mdash; design choices recover what spend can't.
  </div>
</section>
""")

# 17. Tier waterfall
add(f"""
<section>
  <h2>Across our top-50 advertisers</h2>
  <img class="chart" src="data:image/png;base64,{charts['tier']}">
  <div class="footer">
    Most iROAS lift reporting at MNTN is mathematically undetectable. Visits we can measure for almost everyone. CVR is hit-or-miss. iROAS is a different game.
  </div>
</section>
""")

# === Act 3c: The screening rule ===

# 18. Screening rule
add("""
<section>
  <h2>Three questions, asked <em>before</em> we commit budget</h2>
  <ol class="three-q">
    <li><strong>What's the metric?</strong> Visits, CVR, or iROAS &mdash; each has its own MDE. Don't promise iROAS measurement when only visits will be detectable.</li>
    <li><strong>What's the expected effect size?</strong> Use prior MNTN results as the prior. Retargeting: big. Pure prospecting: small. Awareness-only Select: in between.</li>
    <li><strong>Does this advertiser's spend put MDE below the expected effect?</strong> If yes, run it. If no, pool, extend the window, or don't run.</li>
  </ol>
  <p style="margin-top:1em;" class="muted">
    No question 4. If you can't answer these three, don't launch the test &mdash; you'll learn nothing.
  </p>
</section>
""")

# 19. Close — back to Ownerly
add("""
<section>
  <h2>Back to the slide we opened on</h2>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:36px;margin-top:1em;align-items:center;">
    <div>
      <div class="hero">+0.72%</div>
      <p class="hero-sub">Ownerly visit-rate lift. 1.49M treated IPs.</p>
    </div>
    <div>
      <h3 class="navy">Show of hands again. Would you ship this?</h3>
      <p>The number didn't change. Your reading of it did.</p>
      <p style="margin-top:0.8em;font-size:0.9em;color:var(--text-light);">
        That's what power gives the room: a way to refuse tests that can't tell us what they claim to tell us.
      </p>
    </div>
  </div>
</section>
""")

# 20. Power Line again
add("""
<section>
  <p class="powerline">
    Power is the question you have to answer <em>before</em> you run the test &mdash; not after.
  </p>
  <p style="margin-top:1.2em;">
    The handout has the three questions, the spend-threshold rule of thumb, and the calculator URL.
    Use it the next time someone asks "can we run a lift study on advertiser X?"
  </p>
</section>
""")

# 21. References
add("""
<section>
  <h2>References &amp; tickets</h2>
  <ul style="font-size:0.8em;">
    <li><strong>TI-884</strong> &mdash; Power & sample size analysis. The math, the cohort tiering, the cross-validation of Lauren's 7 pilot tests.</li>
    <li><strong>TI-917</strong> &mdash; Combined Loom: TI-837 v5 lift results + power primer + screening rule. Original 17–20 min version of this material.</li>
    <li><strong>TI-933</strong> &mdash; MNTN Select pooled lift. The 0/23 individuals → pooled +2.055 pp story.</li>
    <li><strong>TI-837</strong> &mdash; Ghost-bidding holdout methodology. +3.12 pp all-campaigns, +0.78 pp prospecting, +21 pp retargeting.</li>
    <li><strong>TI-886</strong> &mdash; Ghost-bidder bidder-level implementation. The unlock for per-advertiser Select readouts.</li>
    <li><code>knowledge/experimentation.md</code> &mdash; Lewis-Rao formula, MNTN variance-reduction stack, MDE rules of thumb.</li>
    <li>Lewis &amp; Rao (2015), <em>QJE</em>; Deng-Xu-Liu-Schmidt (2013), CUPED; Johnson-Lewis-Reiley (2017), ghost-ad conditioning.</li>
  </ul>
</section>
""")

# 22. Appendix — formula in detail
add("""
<section data-state="appendix">
  <h2 class="muted">Appendix &middot; Lewis-Rao derivation</h2>
  <p>Two-arm test of proportions. Want: smallest δ (effect size) that we'd reject H₀ for, with probability ≥ power.</p>
  <p>Under H₀ (no effect), the test statistic is N(0, 1). Under H₁ (δ true effect), it's N(δ/SE, 1) where SE = σ &middot; &radic;(1/n<sub>t</sub> + 1/n<sub>c</sub>).</p>
  <p>Reject when |Z| &gt; z<sub>α/2</sub>. Probability of rejection under H₁ is ≥ power when:</p>
  <div class="formula">δ &nbsp;≥&nbsp; (z<sub>α/2</sub> + z<sub>power</sub>) &middot; SE</div>
  <p>That's the MDE. Plug var_reduction in as a multiplier on SE for the variance stack.</p>
  <p class="muted" style="margin-top:1em;">
    For binomial outcomes, σ = &radic;(p(1&minus;p)). For continuous outcomes (iROAS, revenue per IP), σ is the per-unit SD of the outcome.
  </p>
</section>
""")

# 23. Appendix — Boll & Branch / GLD cross-validation
add("""
<section data-state="appendix">
  <h2 class="muted">Appendix &middot; Lauren's 7 pilot tests, cross-validated</h2>
  <table>
    <thead><tr><th>Test</th><th class="r">Reported lift</th><th class="r">Treated IPs</th><th class="r">MDE raw</th><th class="r">MDE post-stack</th><th>Verdict</th></tr></thead>
    <tbody>
      <tr><td>Ownerly</td><td class="r">0.72%</td><td class="r">1.49M</td><td class="r">5.93%</td><td class="r">3.53%</td><td><span class="red">noise (4.7&times;)</span></td></tr>
      <tr><td>GLD</td><td class="r">0.67%</td><td class="r">2.39M</td><td class="r">3.12%</td><td class="r">1.86%</td><td><span class="red">noise (2.8&times;)</span></td></tr>
      <tr><td>Boll &amp; Branch</td><td class="r">1.00%</td><td class="r">1,462</td><td class="r">88.4%</td><td class="r">52.6%</td><td><span class="red">unrunnable</span></td></tr>
      <tr><td>Bumper</td><td class="r">0.60%</td><td class="r" colspan="3">below top-50 (not in cohort)</td><td><span class="muted">not measured</span></td></tr>
      <tr><td>ReversePhone</td><td class="r">0.89%</td><td class="r" colspan="3">below top-50</td><td><span class="muted">not measured</span></td></tr>
      <tr><td>Grow Therapy</td><td class="r">0.57%</td><td class="r" colspan="3">below top-50</td><td><span class="muted">not measured</span></td></tr>
      <tr><td>Nav.com</td><td class="r">0.74%</td><td class="r" colspan="3">below top-50</td><td><span class="muted">not measured</span></td></tr>
    </tbody>
  </table>
  <p style="margin-top:0.8em;" class="muted">
    All 3 measurable tests reported lifts well below MDE. The other 4 were below the cohort threshold and would have been screened out by question 3 of the rule.
  </p>
</section>
""")

# ----- HTML wrap -----

slides_html = "\n".join(SLIDES)
HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Power Analysis Workshop</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.0.4/dist/reset.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.0.4/dist/reveal.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/reveal.js@5.0.4/dist/theme/white.css">
<style>{CSS}</style>
</head>
<body>
<div class="reveal"><div class="slides">
{slides_html}
</div></div>
<script src="https://cdn.jsdelivr.net/npm/reveal.js@5.0.4/dist/reveal.js"></script>
<script>
Reveal.initialize({{
  hash: true, slideNumber: true, controls: true, progress: true, center: true,
  transition: 'fade', transitionSpeed: 'fast',
  width: 1280, height: 800, margin: 0.04, minScale: 0.2, maxScale: 1.6
}});
</script>
</body>
</html>
"""

(HERE / "ti_xxx_workshop_deck.html").write_text(HTML)
print(f"[OK] deck built: {HERE / 'ti_xxx_workshop_deck.html'}  ({len(HTML):,} bytes, {len(SLIDES)} slides)")
