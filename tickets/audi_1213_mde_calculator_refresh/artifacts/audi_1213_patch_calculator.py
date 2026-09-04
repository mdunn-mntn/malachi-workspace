"""Rebuild the MDE calculator from the TI-1019 shipped file: fresh prefill data + arm-split fixes."""
import json
import re
import sys
from pathlib import Path

TICKET = Path(__file__).resolve().parents[1]
WORKSPACE = TICKET.parents[1]
SRC = (
    WORKSPACE
    / "tickets/ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill"
    / "artifacts/ti_xxx_mde_calculator_prefill.html"
)
PAYLOAD = TICKET / "outputs" / "audi_1213_prefill_compact.json"
OUT = TICKET / "artifacts" / "audi_1213_mde_calculator.html"
RUN_DATE = "2026-09-03"

EDITS = []


def sub(html, old, new, label):
    if html.count(old) != 1:
        raise SystemExit(f"anchor not unique ({html.count(old)}): {label}")
    EDITS.append(label)
    return html.replace(old, new)


def main():
    html = SRC.read_text()
    payload = json.loads(PAYLOAD.read_text())
    cohort = payload["cohort"]

    replacement = (
        "window.ADVERTISERS = "
        + json.dumps(payload["advertisers"], separators=(",", ":"))
        + ";\nwindow.COHORT = "
        + json.dumps(cohort, separators=(",", ":"))
        + ";"
    )
    html, n = re.subn(
        r"^window\.ADVERTISERS = \[.*\];$",
        lambda _: replacement,
        html,
        count=1,
        flags=re.M,
    )
    if n != 1:
        raise SystemExit("ADVERTISERS anchor not found")
    EDITS.append("advertisers + cohort payload")

    html = sub(
        html,
        'window.DATA_PULL_DATE = "2026-06-04";',
        f'window.DATA_PULL_DATE = "{RUN_DATE}";',
        "header data-pull date",
    )

    html = sub(
        html,
        "Generated 2026-06-04 by ti_xxx_advertiser_prefill_metrics.sql. */",
        f"Generated {RUN_DATE} by incr_75_advertiser_metrics.sql (AUDI-1213 refresh).\n"
        "   Spend is advertiser-facing (media + data + platform), not media cost. */",
        "provenance comment",
    )

    html = sub(
        html,
        """  const totalSpend = monthlyBudget * (durationWk / WEEKS_PER_MONTH);
  // Budget covers the full reach pool (treatment + holdout).
  // Holdout is a hash-bucket split of total reach: h% control, (1-h)% treated.
  const totalIps   = (totalSpend / cpm * 1000) / impsPerIp;
  const nTreated   = totalIps * (1 - holdoutFrac);
  const nControl   = totalIps * holdoutFrac;""",
        """  const totalSpend = monthlyBudget * (durationWk / WEEKS_PER_MONTH);
  // The holdout is never served, so the budget buys the treated arm only.
  const nTreated   = (totalSpend / cpm * 1000) / impsPerIp;
  const nControl   = nTreated * (holdoutFrac / (1 - holdoutFrac));
  const totalIps   = nTreated + nControl;""",
        "computeMDE arm split",
    )

    html = sub(
        html,
        "  const totalSpend = nTotal * impsPerIp * cpm / 1000;",
        "  const totalSpend = nTotal * (1 - holdoutFrac) * impsPerIp * cpm / 1000;",
        "spendRequired arm split",
    )

    html = sub(
        html,
        """function setOutcome(o) {
  const rates = { ivr: 2.15, cvr: 0.054 };
  document.getElementById('inp-base').value = rates[o] ?? 2.15;
  S.baselineRate = (rates[o] ?? 2.15) / 100;""",
        """function setOutcome(o) {
  S.currentOutcome = o;
  const adv = S.advertiser;
  const pct = adv
    ? (o === 'cvr' ? adv.pCvr : adv.pVisit) * 100
    : (o === 'cvr' ? window.COHORT.cvr : window.COHORT.ivr);
  document.getElementById('inp-base').value = pct.toFixed(3);
  S.baselineRate = pct / 100;""",
        "setOutcome respects the loaded advertiser",
    )

    html = sub(
        html,
        """  document.getElementById('inp-cpm').value  = '24.84';
  document.getElementById('inp-imps').value = '3.5';
  S.cpm = 24.84; S.impsPerIp = 3.5;""",
        """  document.getElementById('inp-cpm').value  = window.COHORT.cpm.toFixed(2);
  document.getElementById('inp-imps').value = window.COHORT.impsIp.toFixed(2);
  S.cpm = window.COHORT.cpm; S.impsPerIp = window.COHORT.impsIp;""",
        "clearAdvertiser cohort defaults",
    )

    html = sub(
        html,
        """  baselineRate  : 0.0215,
  cpm           : 24.84,
  impsPerIp     : 3.5,""",
        f"""  baselineRate  : {cohort['ivr'] / 100},
  cpm           : {cohort['cpm']},
  impsPerIp     : {cohort['impsIp']},""",
        "initial state defaults",
    )

    html = sub(
        html,
        '<input type="number" class="num-inp" id="inp-base" value="2.15" min="0.001" max="50" step="0.01">',
        f'<input type="number" class="num-inp" id="inp-base" value="{cohort["ivr"]}" min="0.001" max="50" step="0.01">',
        "baseline input default",
    )
    html = sub(
        html,
        '<input type="number" class="num-inp" id="inp-cpm" value="24.84" min="1" step="0.01">',
        f'<input type="number" class="num-inp" id="inp-cpm" value="{cohort["cpm"]}" min="1" step="0.01">',
        "cpm input default",
    )
    html = sub(
        html,
        '<input type="number" class="num-inp" id="inp-imps" value="3.5" min="0.1" step="0.1">',
        f'<input type="number" class="num-inp" id="inp-imps" value="{cohort["impsIp"]}" min="0.1" step="0.1">',
        "imps/IP input default",
    )

    html = sub(
        html,
        '<span class="hd-ref" id="hd-data-ref">INCREMENTALITY \u00b7 LEWIS\u2013RAO \u00b7 TI-884 \u00b7 \u2014 advertisers</span>',
        '<span class="hd-ref" id="hd-data-ref">INCREMENTALITY \u00b7 LEWIS\u2013RAO</span>',
        "header placeholder without ticket id",
    )

    html = sub(
        html,
        "if (ref) ref.textContent = `INCREMENTALITY \u00b7 LEWIS\u2013RAO \u00b7 TI-884 \u00b7 ${window.ADVERTISERS.length} advertisers \u00b7 ${window.DATA_PULL_DATE}`;",
        "if (ref) ref.textContent = `INCREMENTALITY \u00b7 LEWIS\u2013RAO \u00b7 ${window.ADVERTISERS.length.toLocaleString()} advertisers \u00b7 LAST RAN ${window.DATA_PULL_DATE}`;",
        "header shows advertiser count and last-ran date",
    )

    html = sub(
        html,
        "Source: TI-884 / Lewis-Rao (2015 QJE)",
        "Source: AUDI-884 / Lewis-Rao (2015 QJE)",
        "footer ticket id",
    )

    html = sub(
        html,
        "  document.getElementById('req-detail').textContent = `for ${(T*100).toFixed(1)}% target \u00b7 ${W} week${W!==1?'s':''}`;",
        "  const vrLabel = S.vrMode === 'stack' ? 'full stack' : 'no variance reduction';\n"
        "  document.getElementById('req-detail').textContent = `for ${(T*100).toFixed(1)}% target \u00b7 ${W} week${W!==1?'s':''} \u00b7 ${vrLabel}`;",
        "budget tile names the active variance-reduction mode",
    )

    html = sub(
        html,
        "// Required spend \u2192 N_total = (z\u00b7\u03c3\u00b7varR / mde_abs)\u00b2 / (h\u00b7(1\u2212h)); totalSpend covers full N_total reach",
        "// Required spend \u2192 N_total = (z\u00b7\u03c3\u00b7varR / mde_abs)\u00b2 / (h\u00b7(1\u2212h)); only the treated arm is served, so only it is billed",
        "spendRequired comment matches the fixed code",
    )

    html = sub(
        html,
        """// Wrap the existing setOutcome so it pulls advertiser-specific baseline when one is loaded
const _origSetOutcome = setOutcome;
setOutcome = function(o) {
  S.currentOutcome = o;
  if (S.advertiser) {
    const baselinePct = (o === 'cvr' ? S.advertiser.pCvr : S.advertiser.pVisit) * 100;
    document.getElementById('inp-base').value = baselinePct.toFixed(3);
    S.baselineRate = baselinePct / 100;
    document.getElementById('btn-ivr').classList.toggle('on', o === 'ivr');
    document.getElementById('btn-cvr').classList.toggle('on', o === 'cvr');
    update();
  } else {
    _origSetOutcome(o);
  }
};

""",
        "",
        "drop the setOutcome wrapper that shadows the fixed base",
    )

    html = sub(
        html,
        """    `<div class="adv-item" data-id="${a.id}">
       <span class="adv-item-name">${a.name}</span>
       <span class="adv-item-meta">${a.id} \u00b7 ${fmtBudget(a.spend30)}/30d</span>
     </div>`).join('');""",
        """    `<div class="adv-item" data-id="${a.id}">
       <span class="adv-item-name">${a.name}${a.live ? '' : ' \u00b7 LAPSED'}</span>
       <span class="adv-item-meta">${a.id} \u00b7 ${fmtBudget(a.spend30)}${a.live ? '/30d' : ' last active ' + a.lastDay}</span>
     </div>`).join('');""",
        "picker flags lapsed advertisers",
    )

    html = sub(
        html,
        "  document.getElementById('adv-loaded-name').textContent = `${a.name} \u00b7 ${a.id}`;",
        "  document.getElementById('adv-loaded-name').textContent = a.live\n"
        "    ? `${a.name} \u00b7 ${a.id}`\n"
        "    : `${a.name} \u00b7 ${a.id} \u00b7 LAPSED ${a.daysOff}d (last active ${a.lastDay})`;",
        "loaded pane names the lapsed window",
    )

    html = sub(
        html,
        """        <div id="adv-loaded" class="adv-loaded">
          <div class="adv-loaded-name" id="adv-loaded-name">\u2014</div>""",
        """        <div id="adv-loaded" class="adv-loaded">
          <div class="adv-loaded-name" id="adv-loaded-name">\u2014</div>
          <div class="adv-lapsed" id="adv-lapsed" hidden></div>""",
        "lapsed banner element",
    )

    html = sub(
        html,
        "    .adv-loaded.on { display: block; }",
        """    .adv-loaded.on { display: block; }
    .adv-lapsed { display: none; margin: 6px 0 8px; padding: 6px 8px; border-left: 3px solid var(--amber);
                  background: rgba(200, 120, 20, 0.08); font-family: var(--ui); font-size: 10px;
                  letter-spacing: 0.06em; color: var(--amber); line-height: 1.45; }
    .adv-lapsed.on { display: block; }""",
        "lapsed banner style",
    )

    html = sub(
        html,
        """  document.getElementById('adv-loaded-name').textContent = a.live
    ? `${a.name} \u00b7 ${a.id}`
    : `${a.name} \u00b7 ${a.id} \u00b7 LAPSED ${a.daysOff}d (last active ${a.lastDay})`;""",
        """  document.getElementById('adv-loaded-name').textContent = `${a.name} \u00b7 ${a.id}`;
  const lapsed = document.getElementById('adv-lapsed');
  lapsed.textContent = a.live
    ? ''
    : `NOT CURRENTLY ACTIVE \u00b7 last delivered ${a.lastDay}, ${a.daysOff} days ago. Rates below are from that advertiser's final 30 delivering days.`;
  lapsed.classList.toggle('on', !a.live);
  lapsed.hidden = a.live;""",
        "lapsed banner rendering",
    )

    html = sub(
        html,
        """      <!-- Variance reduction -->
      <div class="ctrl-section">
        <div class="ctrl-section-head">VARIANCE REDUCTION</div>
        <div class="toggle-row">
          <button class="tog-btn on" id="btn-raw" onclick="setVR('raw')">NONE (RAW)</button>
          <button class="tog-btn" id="btn-stack" onclick="setVR('stack')">FULL STACK</button>
        </div>
      </div>

""",
        "",
        "remove the variance-reduction control",
    )

    html = sub(
        html,
        """        <div class="v-sep"></div>

        <div class="hero-block">
          <div class="hero-lbl">POST-STACK MDE</div>
          <div class="hero-num-sm" id="h-stk">—</div>
          <div class="tier-pill" id="tp-stk">—</div>
          <div class="hero-ci" id="ci-stk">95% CI ±—</div>
        </div>
""",
        "",
        "remove the post-stack hero",
    )

    html = sub(
        html,
        """          <div class="legend-item"><div class="legend-line dashed"></div>POST-STACK</div>
""",
        "",
        "remove the post-stack legend entry",
    )

    html = sub(
        html,
        """      <div class="stack-note">
        Stack: CUPED(0.934) × ghost-ad(0.75) × stratified(0.85) = 0.595 · Source: AUDI-884 / Lewis-Rao (2015 QJE)
      </div>""",
        """      <div class="stack-note">
        Source: AUDI-884 / Lewis-Rao (2015 QJE)
      </div>""",
        "footer drops the stack line",
    )

    html = sub(
        html,
        "const VR_STACK = 0.595; // CUPED(0.934) × ghost-ad(0.75) × stratified(0.85)\n",
        "",
        "drop VR_STACK",
    )

    html = sub(html, "  vrMode        : 'raw',\n", "", "drop vrMode state")

    html = sub(
        html,
        """  const rawPts = [], stkPts = [], rawCiHi = [], rawCiLo = [], stkCiHi = [], stkCiLo = [];
  for (let i = 0; i <= N; i++) {
    const spend = Math.pow(10, L_MIN + (i / N) * (Math.log10(1e7) - L_MIN));
    const r = computeMDE(spend, S.durationWk, S.holdoutFrac, S.baselineRate, S.alpha, S.power, 1.0,      S.cpm, S.impsPerIp);
    const k = computeMDE(spend, S.durationWk, S.holdoutFrac, S.baselineRate, S.alpha, S.power, VR_STACK, S.cpm, S.impsPerIp);
    const rawMde = Math.min(r.mdeRel * 100, 35);
    const stkMde = Math.min(k.mdeRel * 100, 35);
    rawPts.push({ x: spend, y: rawMde });
    stkPts.push({ x: spend, y: stkMde });
    rawCiHi.push({ x: spend, y: Math.min(rawMde * (1 + cf), 35) });
    rawCiLo.push({ x: spend, y: Math.max(rawMde * (1 - cf), 0) });
    stkCiHi.push({ x: spend, y: Math.min(stkMde * (1 + cf), 35) });
    stkCiLo.push({ x: spend, y: Math.max(stkMde * (1 - cf), 0) });
  }
  return { rawPts, stkPts, rawCiHi, rawCiLo, stkCiHi, stkCiLo };""",
        """  const rawPts = [], rawCiHi = [], rawCiLo = [];
  for (let i = 0; i <= N; i++) {
    const spend = Math.pow(10, L_MIN + (i / N) * (Math.log10(1e7) - L_MIN));
    const r = computeMDE(spend, S.durationWk, S.holdoutFrac, S.baselineRate, S.alpha, S.power, 1.0, S.cpm, S.impsPerIp);
    const rawMde = Math.min(r.mdeRel * 100, 35);
    rawPts.push({ x: spend, y: rawMde });
    rawCiHi.push({ x: spend, y: Math.min(rawMde * (1 + cf), 35) });
    rawCiLo.push({ x: spend, y: Math.max(rawMde * (1 - cf), 0) });
  }
  return { rawPts, rawCiHi, rawCiLo };""",
        "curvePts drops the stack series",
    )

    html = sub(
        html,
        """        // index 3: stack CI upper — fills to index 4
        { label: '_stkCiHi', data: stkCiHi, borderWidth: 0, borderColor: 'transparent', backgroundColor: 'rgba(0,112,168,0.04)', pointRadius: 0, tension: 0.35, fill: '+1' },
        // index 4: stack CI lower
        { label: '_stkCiLo', data: stkCiLo, borderWidth: 0, borderColor: 'transparent', pointRadius: 0, tension: 0.35, fill: false },
        // index 5: stack MDE main line
        { label: 'Post-Stack MDE', data: stkPts, borderColor: 'rgba(0,112,168,0.35)', borderWidth: 2, borderDash: [6,4], pointRadius: 0, tension: 0.35, fill: false },
""",
        "",
        "chart drops the stack datasets",
    )

    html = sub(
        html,
        """  const raw = computeMDE(B, W, H, p, a, pw, 1.0,      cpm, impsPerIp);
  const stk = computeMDE(B, W, H, p, a, pw, VR_STACK,  cpm, impsPerIp);
  const reqVR = S.vrMode === 'stack' ? VR_STACK : 1.0;
  const req = spendRequired(T, H, p, a, pw, reqVR, cpm, impsPerIp, W);

  setHero('h-raw', 'tp-raw', raw.mdeRel, tier(raw.mdeRel), 'hero-num');
  setHero('h-stk', 'tp-stk', stk.mdeRel, tier(stk.mdeRel), 'hero-num-sm');""",
        """  const raw = computeMDE(B, W, H, p, a, pw, 1.0, cpm, impsPerIp);
  const req = spendRequired(T, H, p, a, pw, 1.0, cpm, impsPerIp, W);

  setHero('h-raw', 'tp-raw', raw.mdeRel, tier(raw.mdeRel), 'hero-num');""",
        "update drops the stack hero",
    )

    html = sub(html, "  document.getElementById('ci-stk').textContent = ciPct(stk.mdeRel);\n", "", "drop the stack CI line")

    html = sub(
        html,
        """  const vrLabel = S.vrMode === 'stack' ? 'full stack' : 'no variance reduction';
  document.getElementById('req-detail').textContent = `for ${(T*100).toFixed(1)}% target · ${W} week${W!==1?'s':''} · ${vrLabel}`;""",
        "  document.getElementById('req-detail').textContent = `for ${(T*100).toFixed(1)}% target · ${W} week${W!==1?'s':''}`;",
        "budget tile drops the variance label",
    )

    html = sub(
        html,
        """    const { rawPts, stkPts, rawCiHi, rawCiLo, stkCiHi, stkCiLo } = curvePts();""",
        "    const { rawPts, rawCiHi, rawCiLo } = curvePts();",
        "update drops the stack destructure",
    )

    html = sub(
        html,
        """    chart.data.datasets[3].data = stkCiHi;
    chart.data.datasets[4].data = stkCiLo;
    chart.data.datasets[5].data = stkPts;
""",
        "",
        "chart update drops the stack series",
    )

    html = sub(
        html,
        """function setVR(mode) {
  S.vrMode = mode;
  document.getElementById('btn-raw').classList.toggle('on', mode === 'raw');
  document.getElementById('btn-stack').classList.toggle('on', mode === 'stack');
  update();
}

""",
        "",
        "drop setVR",
    )

    html = sub(
        html,
        """  const ctx = document.getElementById('chart').getContext('2d');
  const { rawPts, stkPts, rawCiHi, rawCiLo, stkCiHi, stkCiLo } = curvePts();""",
        """  const ctx = document.getElementById('chart').getContext('2d');
  const { rawPts, rawCiHi, rawCiLo } = curvePts();""",
        "initChart drops the stack destructure",
    )

    html = sub(
        html,
        """      // Dots + CI whiskers at current budget (raw & stack)
      const rawR = computeMDE(S.monthlyBudget, S.durationWk, S.holdoutFrac, S.baselineRate, S.alpha, S.power, 1.0,      S.cpm, S.impsPerIp);
      const stkR = computeMDE(S.monthlyBudget, S.durationWk, S.holdoutFrac, S.baselineRate, S.alpha, S.power, VR_STACK, S.cpm, S.impsPerIp);
      const cf = ciFactor();
      [[rawR.mdeRel * 100, 'rgba(0,112,168,0.55)', 'rgba(0,112,168,1)'],
       [stkR.mdeRel * 100, 'rgba(0,112,168,0.22)', 'rgba(0,112,168,0.4)']].forEach(([mde100, whiskerCol, dotCol]) => {""",
        """      // Dot + CI whisker at current budget
      const rawR = computeMDE(S.monthlyBudget, S.durationWk, S.holdoutFrac, S.baselineRate, S.alpha, S.power, 1.0, S.cpm, S.impsPerIp);
      const cf = ciFactor();
      [[rawR.mdeRel * 100, 'rgba(0,112,168,0.55)', 'rgba(0,112,168,1)']].forEach(([mde100, whiskerCol, dotCol]) => {""",
        "chart marker drops the stack dot",
    )

    html = sub(
        html,
        "grid-template-columns: auto 1px auto auto;",
        "grid-template-columns: auto auto;",
        "hero grid matches its two remaining blocks",
    )

    html = sub(
        html,
        """    .req-block {
      display: flex;
      flex-direction: column;
      gap: 4px;
      align-self: center;
      padding-left: 4px;
    }""",
        """    .req-block {
      display: flex;
      flex-direction: column;
      gap: 4px;
      align-self: center;
      padding-left: 4px;
      min-width: max-content;
    }
    .req-label, .req-monthly, .req-detail { white-space: nowrap; }""",
        "budget block sizes to its content instead of wrapping",
    )

    html = sub(
        html,
        '<h1 class="hd-title">mde calculator \u00b7 per-advertiser prefill</h1>',
        '<h1 class="hd-title">mde calculator \u00b7 smallest lift a test can detect</h1>',
        "title says what the tool answers",
    )

    OUT.write_text(html)
    print(f"wrote {OUT.relative_to(WORKSPACE)}  {OUT.stat().st_size / 1024:.0f} KB")
    for e in EDITS:
        print(f"  applied: {e}")


if __name__ == "__main__":
    sys.exit(main())
