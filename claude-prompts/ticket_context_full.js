export const meta = {
  name: 'ticket-context-full',
  description: 'Self-driving context pass over all remaining tickets: per-ticket extract -> verify -> fix-and-reverify -> land, commit per batch, keep going past failures, report needs-review + recurring themes',
  phases: [
    { title: 'Extract', detail: 'one read-only agent per ticket -> TL;DR card + delta facts (strict fidelity)' },
    { title: 'Verify',  detail: '2 adversarial reviewers per card, source-only, blocking-only' },
    { title: 'Repair',  detail: 'fixer corrects flagged claims against source; re-verify once' },
    { title: 'Land',    detail: 'per batch: write confirmed cards, queue failures, rebuild index, lint, commit' },
    { title: 'Report',  detail: 'carded vs needs_review + recurring discrepancy themes' },
  ],
}

// Remaining un-carded tickets (relative to tickets/, epic children include a '/'). args.tickets overrides (resume).
const DEFAULT = [
  "audi_1070_yoy_decline_caraway_avon_hexclad","audi_1089_ddp_vendor_evaluations",
  "audi_1089_ddp_vendor_evaluations/ds24_justuno","audi_1089_ddp_vendor_evaluations/ds26_predactiv",
  "audi_1089_ddp_vendor_evaluations/ds28_33across","audi_1089_ddp_vendor_evaluations/ds33_sovrn",
  "audi_1089_ddp_vendor_evaluations/ds36_cybba","audi_1089_ddp_vendor_evaluations/ds39_klickly",
  "audi_1089_ddp_vendor_evaluations/ds40_33across_api","audi_1091_augmentor_full_source",
  "audi_1111_vendor_quality","audi_1111_vendor_quality/audi_1115_wtp_cpm",
  "audi_1111_vendor_quality/audi_1116_rtc_free_logs","audi_1111_vendor_quality/audi_1117_ds14_svs_overlap",
  "ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill","ber_2250_incrementality_overhaul/ti_1039_liftlab_design_review",
  "ber_2250_incrementality_overhaul/ti_831_audience_deciles","ber_2250_incrementality_overhaul/ti_835_control_group_design",
  "ber_2250_incrementality_overhaul/ti_837_implementation_plan","ber_2250_incrementality_overhaul/ti_839_measure_results",
  "ber_2250_incrementality_overhaul/ti_842_present_results","ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis",
  "ber_2250_incrementality_overhaul/ti_885_mid_intent_experiment_setup","ber_2250_incrementality_overhaul/ti_886_uplift_model_implementation",
  "ber_2250_incrementality_overhaul/ti_917_combined_loom","ber_2250_incrementality_overhaul/ti_933_select_lift_analysis",
  "dm_3118_rtc_monitor","dm_3188_comparison_rt_and_non_rt",
  "goal_attainment_customer_goal_map","incr_75_eligible_advertisers",
  "mm_44_ipdsc_hh_discrepancy","tgt_4016_ecomm_classifier_thresholds",
  "tgt_4103_common_crawl_coverage","ti_033_vertical_classification_changes",
  "ti_1003_experiment_archive","ti_1016_memdb_bidder_cache_optimization",
  "ti_1017_autocamp_fangorn_perf_diag","ti_1026_orange_theory_audience_eval",
  "ti_1027_5x5_data_evaluation","ti_1033_experiment_archive_deploy",
  "ti_1037_audience_diagnostic_tool","ti_1044_elevenlabs_ctv_incrementality",
  "ti_1053_elevenlabs_3p_segments","ti_1058_ds13_ds19_pipeline_map",
  "ti_1060_mntn_matched_openai_cost","ti_200_whitelist_blocklist",
  "ti_253_tpa_monitor","ti_254_investigate_low_ntb_percentage",
  "ti_270_pre_post_analysis_ga","ti_310_ntb_investigations",
  "ti_34_identity_sync_freshness","ti_391_audience_intent_scoring",
  "ti_501_jaguar_kpi","ti_502_ip_scoring",
  "ti_504_causal_impact_experimentation","ti_541_ip_scoring_pipeline",
  "ti_542_max_reach_causal_impact","ti_644_root_insurance",
  "ti_650_stage_3_vv_audit","ti_684_missing_ip_from_ipdsc",
  "ti_737_fpa_advertiser_verticals","ti_748_causal_impact_media_plan",
  "ti_780_campaign_ramp_up_research","ti_789_bidstream_feature_extraction",
  "ti_790_bidstream_feature_inventory","ti_797_buk_knowledge_transfer",
  "ti_803_buk_value_analysis","ti_804_keyword_visit_rate_analysis",
  "ti_809_multiday_validation","ti_810_feature_store_pipeline",
  "ti_811_advertiser_features","ti_813_buk_500_advertiser_scale",
  "ti_832_feature_store_roas_cpa","ti_849_fangorn_score_monitoring",
  "ti_896_audience_composition_2025_drop","ti_921_fangorn_lift_dashboard",
  "ti_923_scout_feasibility_review","ti_931_summary_dag_column_drift",
  "ti_956_interest_segment_scoring_schedule","ti_961_fangorn_causal_impact",
  "ti_adhoc_advertiser_scoring_filter","ti_kafka_secret_sweep",
  "ti_xxx_power_analysis_workshop","ti_xxx_ticket_theme_analysis",
]
// The full list is injected via args.tickets at launch; DEFAULT is a resumable fallback subset.
// args may arrive as a real object OR a JSON string (the harness sometimes stringifies it) — normalize both.
let ARGS = args
if (typeof ARGS === 'string') { try { ARGS = JSON.parse(ARGS) } catch (e) { ARGS = {} } }
if (!ARGS || typeof ARGS !== 'object') ARGS = {}
const TICKETS = (Array.isArray(ARGS.tickets) && ARGS.tickets.length) ? ARGS.tickets : DEFAULT
const BATCH = ARGS.batch || 8

function chunk(a, n) { const out = []; for (let i = 0; i < a.length; i += n) out.push(a.slice(i, i + n)); return out }

const CARD = {
  type: 'object', additionalProperties: false,
  required: ['question','answer','how','tables','learned','reuse_when','keywords','delta_facts'],
  properties: {
    question:{type:'string'}, answer:{type:'string'}, how:{type:'string'},
    tables:{type:'array',items:{type:'string'}}, learned:{type:'array',items:{type:'string'}},
    reuse_when:{type:'array',items:{type:'string'}}, keywords:{type:'array',items:{type:'string'}},
    delta_facts:{type:'array',items:{type:'object',additionalProperties:false,
      required:['fact','home_doc','source_line'],
      properties:{fact:{type:'string'},
        home_doc:{type:'string',enum:['data_catalog.md','data_knowledge.md','mntn_business.md','experimentation.md','none']},
        source_line:{type:'string'}}}},
    front_matter_fix:{type:'object',properties:{summary:{type:'string'},result:{type:'string'}}},
  },
}
const VERDICT = {
  type:'object', additionalProperties:false, required:['card_ok','discrepancies'],
  properties:{ card_ok:{type:'boolean'},
    discrepancies:{type:'array',items:{type:'object',additionalProperties:false,
      required:['field','problem'], properties:{field:{type:'string'},problem:{type:'string'}}}} },
}

const FIDELITY =
  `STRICT FIDELITY (adversarial reviewers reject non-blocking-safe cards):\n` +
  `- Tables: use the SAME name the summary uses; do NOT add a dataset/project qualifier the summary omits, and never ` +
  `list a table the summary never names.\n` +
  `- Preserve hedges ('likely X' stays 'likely'); never upgrade a hedge to a fact.\n` +
  `- 'How'/'Answer' report only what the Findings/Results state — never promote a Plan step to done, never state an ` +
  `outcome the summary doesn't conclude.\n` +
  `- Cite no other doc/line unless the summary itself does. Cover every part of the question the answer addresses.\n`

// one reviewer pair; blocking-only gate (Tables qualifier normalization is NOT a reason to fail)
async function verifyPair(t, card, round) {
  const vs = await parallel([1,2].map(i => () =>
    agent(
      `Assume this TL;DR card for ${t} is WRONG. Verify every field ONLY against tickets/${t}/summary.md ` +
      `(read it fresh; do not trust the card).\n` +
      `Set card_ok=false ONLY for a BLOCKING problem: a stated fact/number/outcome the summary does not support, ` +
      `a hedge upgraded to certainty, a planned step reported as done, inflated scope, or a table the summary never ` +
      `names at all. Do NOT fail a card because the Tables field adds/drops a dataset qualifier on a table the ` +
      `summary DOES name — note it but keep it supported.\nCard:\n` +
      JSON.stringify({question:card.question,answer:card.answer,how:card.how,tables:card.tables,learned:card.learned,delta_facts:card.delta_facts}),
      { schema: VERDICT, phase: round === 2 ? 'Repair' : 'Verify', label: `verify${round}.${i}:${t}`, agentType: 'reviewer-adversarial' }
    )
  ))
  const ok = vs.filter(Boolean)
  return { pass: ok.length === 2 && ok.every(v => v.card_ok), discrepancies: ok.flatMap(v => v.discrepancies || []) }
}

// ---- process every batch: converge each ticket independently, then land+commit the batch ----
const carded = [], needsReview = []
for (const [bi, group] of chunk(TICKETS, BATCH).entries()) {
  log(`batch ${bi + 1}/${Math.ceil(TICKETS.length / BATCH)} (${group.length} tickets)`)

  // pipeline: extract -> converge (verify, and on fail: fix + re-verify once)
  const results = await pipeline(group,
    t => agent(
      `Read tickets/${t}/summary.md in full (skim tickets/${t}/outputs/ and queries/ filenames if present). ` +
      `Produce the TL;DR card. Then delta_facts: durable data/business facts in this summary NOT already in ` +
      `knowledge/data_catalog.md, knowledge/data_knowledge.md, knowledge/experimentation.md, knowledge/mntn_business.md ` +
      `(grep to check); quote the source line, invent nothing; empty array if none. If front-matter summary/result is ` +
      `missing or empty, fill front_matter_fix.\n\n` + FIDELITY,
      { schema: CARD, phase: 'Extract', label: `extract:${t}`, agentType: 'general-purpose' }
    ).then(r => r ? ({ t, ...r }) : ({ t, _extract_error: 'extractor returned nothing' }))
     .catch(e => ({ t, _extract_error: String((e && e.message) || e) })),   // retry-cap etc. -> tracked, never a silent drop

    async (c, t) => {
     try {
      if (!c) return { t, ok: false, status: 'extract_failed', discrepancies: [{ field: '-', problem: 'stage returned nothing' }] }
      if (c._extract_error) return { t, ok: false, status: 'extract_error', discrepancies: [{ field: 'extract', problem: c._extract_error }] }
      let verd = await verifyPair(t, c, 1)
      if (!verd.pass) {
        // FIX THE FAILURE AND RE-RUN THIS TICKET: correct only the flagged claims against source
        const fixed = await agent(
          `A TL;DR card for ${t} failed adversarial review. Fix ONLY the flagged problems by re-checking ` +
          `tickets/${t}/summary.md — correct or delete the unsupported claim; do not add anything new. Keep every ` +
          `already-supported field as-is. Return the full corrected card.\n\n` + FIDELITY +
          `\nFlagged problems:\n` + JSON.stringify(verd.discrepancies, null, 2) +
          `\n\nCurrent card:\n` + JSON.stringify(c),
          { schema: CARD, phase: 'Repair', label: `fix:${t}`, agentType: 'fixer' }
        )
        if (fixed) {
          c = { ...c, ...fixed }
          verd = await verifyPair(t, c, 2)
        }
      }
      return { t, card: c, ok: verd.pass, discrepancies: verd.discrepancies }
     } catch (e) {
      return { t, ok: false, status: 'converge_error', discrepancies: [{ field: 'pipeline', problem: String((e && e.message) || e) }] }
     }
    }
  )

  const good = results.filter(Boolean).filter(r => r.ok && r.card)
  const bad  = results.filter(Boolean).filter(r => !r.ok)
  carded.push(...good.map(r => r.t))
  needsReview.push(...bad.map(r => ({ t: r.t, discrepancies: r.discrepancies })))

  // barrier: land THIS batch (single agent = atomic; serialized across batches so git never races)
  await agent(
    `Land this batch of the context pass. Work carefully; never truncate any existing summary content.\n\n` +
    `CONFIRMED CARDS to write (JSON): ${JSON.stringify(good.map(r => ({
      t: r.t, question: r.card.question, answer: r.card.answer, how: r.card.how, tables: r.card.tables,
      learned: r.card.learned, reuse_when: r.card.reuse_when, keywords: r.card.keywords,
      front_matter_fix: r.card.front_matter_fix || null, delta_facts: r.card.delta_facts,
    })))}\n\n` +
    `NEEDS-REVIEW (did NOT pass — do NOT write a card): ${JSON.stringify(bad.map(r => ({ t: r.t, discrepancies: r.discrepancies })))}\n\n` +
    `For each CONFIRMED card, edit tickets/<t>/summary.md:\n` +
    `  1. Build a "## TL;DR" block from the fields (Q/A/How + Tables/Learned/Reuse-when bullets). Insert it AFTER the ` +
    `closing '---' of the YAML front-matter and BEFORE the first '# ' H1. If a '## TL;DR' block already exists there, ` +
    `REPLACE it (never stack two). Leave everything below byte-for-byte unchanged.\n` +
    `  2. Add/replace a front-matter line: keywords: [lowercase, comma-separated].\n` +
    `  3. If front_matter_fix is present: set summary/result ONLY where the current value is empty; never overwrite a ` +
    `non-empty field.\n` +
    `Then, once for the batch:\n` +
    `  4. Append confirmed delta_facts to tickets/_extracted_facts_queue.md (create if missing), grouped by home_doc, ` +
    `each with source ticket + source_line. This is a human-review queue; do NOT merge into knowledge/.\n` +
    `  5. Append/update tickets/_CONTEXT_COVERAGE.md: one row per ticket in this batch — | ticket | state | facts | ` +
    `notes |, state=carded for confirmed, needs_review for the rest (with a one-line reason).\n` +
    `  6. Run: bash .claude/scripts/build_index.sh\n` +
    `  7. Run: python3 .claude/scripts/lint_tickets.py --check  (report result; a pre-existing unrelated violation is OK ` +
    `to leave, but never introduce a new one).\n` +
    `  8. Commit + push: cd /Users/malachi/Developer/work/mntn/workspace && git add -A && git commit -m ` +
    `"context-full: batch ${bi + 1} — card ${good.length}, needs_review ${bad.length}" && git push origin main\n` +
    `Verify with 'git diff --stat' before committing. Report what landed.`,
    { phase: 'Land', label: `land:batch${bi + 1}`, agentType: 'general-purpose' }
  )
  log(`batch ${bi + 1} done: carded ${good.length}, needs_review ${bad.length}`)
}

// ---- Report: recurring failure themes so the prompt can be adjusted between runs ----
phase('Report')
const themeCounts = {}
for (const nr of needsReview) for (const d of (nr.discrepancies || [])) {
  const key = (d.problem || '').toLowerCase().slice(0, 60)
  themeCounts[key] = (themeCounts[key] || 0) + 1
}
const themes = Object.entries(themeCounts).sort((a, b) => b[1] - a[1]).slice(0, 12).map(([k, n]) => ({ theme: k, count: n }))

return {
  total: TICKETS.length,
  carded: carded.length,
  carded_tickets: carded,
  needs_review: needsReview,
  recurring_themes: themes,   // adjust the extractor/fixer prompt for the top themes, then resume with args.tickets = needs_review slugs
}
