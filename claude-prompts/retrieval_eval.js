export const meta = {
  name: 'retrieval-eval',
  description: 'Run every cold-start retrieval probe in knowledge/eval_probes.md and report per-probe pass/gaps — the regression suite for the self-optimizing context system',
  phases: [
    { title: 'Probe', detail: 'one cold-start agent per probe, entry limited to START_HERE + _ROUTING + tickets/INDEX' },
    { title: 'Report', detail: 'aggregate pass/fail + named routing gaps' },
  ],
}

// Probes are the machine-readable JSON block in knowledge/eval_probes.md. args.probes overrides (ad-hoc run).
// The list is embedded so the workflow is self-contained; keep it in sync with eval_probes.md ## PROBES.
const DEFAULT_PROBES = [
  { id: 'mm_pre_post', question: "MM-campaign performance pre/post after a given date — where's the context, the right tables, the method, and what did we learn before?",
    must_reach: ["MM-definition (AUDI-1083 / decisions 0001)", "pre/post method (experimentation.md Standard Analysis Protocol)", "perf tables (sum_by_campaign_by_day for long pre-periods)", "agg__daily_sum_by_campaign Sep-2025 gotcha -> sum_by_campaign_by_day"] },
  { id: 'ddp_vendor_valuation', question: "How do we value a 3P data (DDP) vendor and decide keep/drop + willingness-to-pay?",
    must_reach: ["DDP valuation/WTP framework doc", "a worked vendor eval ticket (AUDI-1089 child)", "metered bill source coredw.usage_reporting_data"] },
  { id: 'incrementality_experiment', question: "Did a feature/rollout move visit rate — how do we design and measure it causally?",
    must_reach: ["experimentation.md Standard Analysis Protocol (DiD + CausalImpact)", "a canonical experiment ticket (BER-2250 / TI-961 / TI-933)", "power/MDE up front (TI-884)"] },
  { id: 'availability_gate', question: "Why is an advertiser's audience smaller than the UI size, and what gates bidding availability?",
    must_reach: ["DS14 augmentor 7-day availability gate", "audience-eval ticket (TI-1026 / AUDI-1117)", "HHST intent-gate mechanics"] },
  { id: 'bidstream_features', question: "Which bidstream/log features predict visits and feed the Fangorn feature store?",
    must_reach: ["feature-inventory ticket TI-790 + epic TI-789", "SHAP ranking, pre-visit vs feedback split", "source log tables (augmentor_log, win_logs, ci)"] },
]

let ARGS = args
if (typeof ARGS === 'string') { try { ARGS = JSON.parse(ARGS) } catch (e) { ARGS = {} } }
if (!ARGS || typeof ARGS !== 'object') ARGS = {}
const PROBES = (Array.isArray(ARGS.probes) && ARGS.probes.length) ? ARGS.probes : DEFAULT_PROBES

const EVAL = {
  type: 'object', additionalProperties: false,
  required: ['id', 'opened', 'reached', 'gaps', 'pass'],
  properties: {
    id: { type: 'string' },
    opened: { type: 'array', items: { type: 'string' }, description: 'docs actually opened' },
    reached: { type: 'array', description: 'per must_reach target: reached or not',
      items: { type: 'object', additionalProperties: false, required: ['target', 'reached'],
        properties: { target: { type: 'string' }, reached: { type: 'boolean' }, via: { type: 'string' } } } },
    gaps: { type: 'array', items: { type: 'string' }, description: 'each miss + the exact routing fix' },
    pass: { type: 'boolean', description: 'all targets reached with only a handful of opens' },
  },
}

phase('Probe')
const results = await parallel(PROBES.map(p => () =>
  agent(
    `You are a COLD chat with NO prior context. You may open ONLY these three entry points and docs they name:\n` +
    `  - knowledge/START_HERE.md\n  - knowledge/_ROUTING.md\n  - tickets/INDEX.md\n` +
    `Do not guess ticket paths; only open a doc one of the three (or a doc they link) names. Track every open in "opened".\n\n` +
    `QUESTION: ${p.question}\n\n` +
    `For EACH of these targets, set reached=true only if you actually got there via the retrieval path (not prior knowledge), and say via what:\n` +
    p.must_reach.map((t, i) => `  ${i + 1}. ${t}`).join('\n') + `\n\n` +
    `pass = every target reached AND you did not have to open more than a handful of docs. For any miss, put the EXACT fix in gaps ` +
    `(e.g. "add keyword X to _ROUTING", "add a START_HERE task row for Y", "ticket Z missing from tickets/INDEX"). Be strict and honest. id="${p.id}".`,
    { schema: EVAL, phase: 'Probe', label: `probe:${p.id}`, agentType: 'general-purpose' }
  )
)).then(rs => rs.filter(Boolean))

phase('Report')
const passed = results.filter(r => r.pass)
const failed = results.filter(r => !r.pass)
return {
  total: PROBES.length,
  passed: passed.map(r => r.id),
  failed: failed.map(r => ({ id: r.id, gaps: r.gaps, missed: (r.reached || []).filter(x => !x.reached).map(x => x.target) })),
  suite_pass: failed.length === 0,
}
