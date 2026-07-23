export const meta = {
  name: 'ticket-context-pilot',
  description: 'Pilot: TL;DR cards + delta knowledge extraction + keyword routing for 5 tickets, adversarially verified, retrieval-eval gated',
  phases: [
    { title: 'Extract',  detail: 'one read-only agent per ticket → TL;DR card + delta facts' },
    { title: 'Verify',   detail: '2 adversarial reviewers per card, source-only' },
    { title: 'Land',     detail: 'write cards into summary.md, stage shared-doc facts, rebuild index, commit' },
    { title: 'Eval',     detail: 'fresh-context retrieval eval on the MM pre/post question (pass/fail gate)' },
  ],
}

// Pilot set: an MM/pre-post cluster (so the eval is meaningful) + one 1342-line monster (compression stress test)
const TICKETS = (args && args.tickets) || [
  'audi_1083_mm_classifying_view',   // MM definitions — central to the eval; result:'' (a lint violation to fix)
  'audi_1141_mm_vs_3p_by_vertical',  // MM vs 3P scorecard
  'ti_390_mmv3_performance',         // MM performance (thin)
  'ti_221_pre_post_analysis',        // pre/post method (thin, has queries/)
  'ti_999_interest_segment_sizing',  // 1342-line summary — stress the card compression
]

const CARD = {
  type: 'object', additionalProperties: false,
  required: ['question','answer','how','tables','learned','reuse_when','keywords','delta_facts'],
  properties: {
    question:   { type: 'string', description: 'what was actually asked, 1 line' },
    answer:     { type: 'string', description: 'blessed finding, 1 line (mirror front-matter result:)' },
    how:        { type: 'string', description: 'method + key technique, 1-2 lines' },
    tables:     { type: 'array', items: { type: 'string' }, description: 'BQ tables/sources touched (clean names)' },
    learned:    { type: 'array', items: { type: 'string' }, description: '1-3 durable facts/gotchas' },
    reuse_when: { type: 'array', items: { type: 'string' }, description: 'trigger phrases a future chat would search' },
    keywords:   { type: 'array', items: { type: 'string' }, description: 'lowercase routing keywords for _ROUTING.md' },
    delta_facts:{ type: 'array', description: 'facts NOT already in the knowledge docs — with provenance',
      items: { type: 'object', additionalProperties: false, required: ['fact','home_doc','source_line'],
        properties: {
          fact:        { type: 'string' },
          home_doc:    { type: 'string', enum: ['data_catalog.md','data_knowledge.md','mntn_business.md','experimentation.md','none'] },
          source_line: { type: 'string', description: 'quote/section of summary.md this came from — no invention' },
        } } },
    front_matter_fix: { type: 'object', description: 'only if lint flagged this ticket',
      properties: { summary: { type: 'string' }, result: { type: 'string' } } },
  },
}

const VERDICT = {
  type: 'object', additionalProperties: false, required: ['claims','card_ok'],
  properties: {
    card_ok: { type: 'boolean' },
    claims: { type: 'array', items: { type: 'object', additionalProperties: false,
      required: ['claim','supported'],
      properties: { claim: {type:'string'}, supported: {type:'boolean'}, evidence: {type:'string'} } } },
  },
}

const EVAL = {
  type: 'object', additionalProperties: false, required: ['opened','found','gaps','pass'],
  properties: {
    opened: { type: 'array', items: { type: 'string' } },
    found:  { type: 'array', items: { type: 'string' } },
    gaps:   { type: 'array', items: { type: 'string' } },
    pass:   { type: 'boolean' },
    notes:  { type: 'string' },
  },
}

// ---------- PHASE 1: Extract (read-only, one agent per ticket, parallel) ----------
phase('Extract')
const cards = (await parallel(TICKETS.map(t => () =>
  agent(
    `Read tickets/${t}/summary.md in full (and skim tickets/${t}/outputs/ filenames if present).\n` +
    `Produce the TL;DR card for this ticket. Then produce delta_facts: durable data/business facts stated in ` +
    `this summary that are NOT already captured in knowledge/_ROUTING.md, knowledge/data_catalog.md, or ` +
    `knowledge/data_knowledge.md (grep them to check). For every fact, quote the source line — invent nothing. ` +
    `If lint flagged this ticket (missing/empty front-matter), fill front_matter_fix.`,
    { schema: CARD, phase: 'Extract', label: `extract:${t}`, agentType: 'general-purpose' }
  ).then(r => r && ({ t, ...r }))
))).filter(Boolean)

// ---------- PHASE 2: Verify (2 skeptics per card, source-only, parallel) ----------
phase('Verify')
const verified = (await parallel(cards.map(c => () =>
  parallel([1, 2].map(i => () =>
    agent(
      `Assume this TL;DR card for ${c.t} is WRONG. Verify every field ONLY against tickets/${c.t}/summary.md ` +
      `(read it fresh; do not trust the card). Flag any claim the summary does not support. Card:\n` +
      JSON.stringify({ question:c.question, answer:c.answer, how:c.how, tables:c.tables, learned:c.learned, delta_facts:c.delta_facts }),
      { schema: VERDICT, phase: 'Verify', label: `verify${i}:${c.t}`, agentType: 'reviewer-adversarial' }
    )
  )).then(vs => {
    const ok = vs.filter(Boolean)
    // keep only claims BOTH reviewers support; a card passes if both said card_ok
    const passed = ok.length === 2 && ok.every(v => v.card_ok)
    return { ...c, reviews: ok, passed }
  })
))).filter(Boolean)

// ---------- PHASE 3: Land (write cards; stage shared-doc facts; rebuild index; commit) ----------
phase('Land')
// Cards go straight into each ticket's OWN summary.md (low blast radius, parallel-safe: distinct files).
await parallel(verified.filter(v => v.passed).map(v => () =>
  agent(
    `In tickets/${v.t}/summary.md, insert this "## TL;DR" block immediately AFTER the closing --- of the YAML ` +
    `front-matter and BEFORE the "# ${v.t}" H1. Do not alter anything below it. Also add a ` +
    `\`keywords: [${v.keywords.map(k=>`"${k}"`).join(', ')}]\` line into the front-matter. ` +
    (v.front_matter_fix ? `Apply front_matter_fix: ${JSON.stringify(v.front_matter_fix)}. ` : '') +
    `Card fields:\n${JSON.stringify({question:v.question,answer:v.answer,how:v.how,tables:v.tables,learned:v.learned,reuse_when:v.reuse_when})}`,
    { phase: 'Land', label: `card:${v.t}`, agentType: 'general-purpose' }
  )
))
// Shared-doc facts are NOT auto-merged (high blast radius). Stage them for human review, then commit everything.
await agent(
  `Write tickets/_pilot_extracted_facts.md: a review queue of the verified delta_facts across all pilot tickets, ` +
  `grouped by home_doc, each with its source ticket + source_line. Then run ` +
  `bash .claude/scripts/build_index.sh to regenerate tickets/INDEX.md and _ROUTING.md, run ` +
  `python3 .claude/scripts/lint_tickets.py --check, and git add + commit + push with message ` +
  `"pilot: TL;DR cards + keyword routing for 5 tickets; stage delta facts for review". ` +
  `Verified facts: ${JSON.stringify(verified.filter(v=>v.passed).map(v=>({t:v.t, facts:v.delta_facts})))}`,
  { phase: 'Land', label: 'stage+commit', agentType: 'general-purpose' }
)

// ---------- PHASE 4: Eval (the acceptance gate) ----------
phase('Eval')
const evalResult = await agent(
  `Fresh context, no memory of this run. A teammate asks: "I need MM-campaign performance pre/post after a ` +
  `given date — where do I get the context, the right tables, the method, and anything we learned before?" ` +
  `Using ONLY knowledge/START_HERE.md, knowledge/_ROUTING.md, and tickets/INDEX.md as entry points, list what ` +
  `you would open. Then judge pass/fail: does routing now surface (1) the MM-definition ticket, (2) the ` +
  `pre/post method, (3) the correct perf tables, and (4) the "agg__daily_sum_by_campaign only from Sep 2025" ` +
  `gotcha? Report every gap.`,
  { schema: EVAL, phase: 'Eval', label: 'retrieval-eval', agentType: 'general-purpose' }
)

return {
  cards_written: verified.filter(v => v.passed).map(v => v.t),
  cards_rejected: verified.filter(v => !v.passed).map(v => v.t),
  facts_staged: verified.filter(v=>v.passed).flatMap(v => v.delta_facts.length),
  eval: evalResult,
}
