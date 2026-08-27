// pr_gauntlet.js — adversarial PR review loop: blind reviewers -> default-refute verify ->
// one fixer, fresh agents every round, until a full round confirms nothing.
// Invoked by /pr_gauntlet with args {repo, base, files, tier?, prNumber?, description?}.
// tier: fast (1 round, bugs only) | medium (2 rounds, default) | thorough (3 rounds, must converge).
// Verdicts: PASS | FIXED_UNVERIFIED | FAIL_MAX_ROUNDS | THRASH | ERROR. Never commits.
export const meta = {
  name: 'pr_gauntlet',
  description: 'Adversarial PR gauntlet: skeptic+stylist review, refute, fix, loop until an empty round',
  whenToUse: 'Dispatched by the /pr_gauntlet skill on a PR, branch, or diff before it ships',
}

const TIERS = {
  fast: { rounds: 1, stylist: false, refuters: 3, effort: 'medium', fixLastRound: true },
  medium: { rounds: 2, stylist: true, refuters: 4, effort: undefined, fixLastRound: true },
  thorough: { rounds: 3, stylist: true, refuters: 6, effort: undefined, fixLastRound: false },
}
const LINE_BUCKET = 10

const FINDINGS_SCHEMA = {
  type: 'object',
  required: ['findings', 'clean_paths'],
  properties: {
    findings: {
      type: 'array',
      items: {
        type: 'object',
        required: ['file', 'line', 'title', 'claim', 'evidence', 'severity'],
        properties: {
          file: { type: 'string', description: 'repo-relative path' },
          line: { type: 'integer' },
          title: { type: 'string', description: 'one-line label' },
          claim: { type: 'string', description: 'the defect or rule breach, stated precisely' },
          evidence: { type: 'string', description: 'skeptic: concrete failure scenario (inputs/state -> wrong outcome); stylist: the rule quoted, or a before/after sketch' },
          severity: { enum: ['blocker', 'major', 'minor'] },
        },
      },
    },
    clean_paths: { type: 'array', items: { type: 'string' }, description: 'on zero findings: the 3 riskiest/cleanest things you tried to break and could not' },
  },
}

const VERDICT_SCHEMA = {
  type: 'object',
  required: ['refuted', 'evidence'],
  properties: {
    refuted: { type: 'boolean' },
    evidence: { type: 'string', description: 'one line of evidence for whichever way you ruled' },
  },
}

const FIX_SCHEMA = {
  type: 'object',
  required: ['fixed', 'rejected', 'new_files', 'mechanical'],
  properties: {
    fixed: { type: 'array', items: { type: 'string' }, description: 'keys of findings fixed' },
    rejected: {
      type: 'array',
      items: {
        type: 'object',
        required: ['key', 'evidence'],
        properties: { key: { type: 'string' }, evidence: { type: 'string' } },
      },
    },
    new_files: { type: 'array', items: { type: 'string' } },
    mechanical: { type: 'string', description: 'result of the post-fix mechanical re-check' },
  },
}

const a = typeof args === 'string' ? JSON.parse(args) : args
// haiku by default: token cost, not review depth, is the binding constraint (user rule 2026-08-27)
const MODEL = a.model || 'haiku'
if (!a || !a.repo || !a.base || !Array.isArray(a.files) || a.files.length === 0) {
  throw new Error('args must be {repo, base, files[], tier?, prNumber?, description?}')
}
const tier = TIERS[(a.tier || 'medium').toLowerCase()]
if (!tier) throw new Error(`tier must be one of ${Object.keys(TIERS).join(', ')}`)
const MAX_ROUNDS = tier.rounds
const MAX_REFUTERS_PER_ROUND = tier.refuters
const fileList = a.files.join('\n  ')
const desc = a.description ? `PR description under review:\n---\n${a.description}\n---` : 'No PR description exists yet.'

const ARBITER_SCHEMA = {
  type: 'object',
  required: ['same', 'evidence'],
  properties: {
    same: { type: 'boolean', description: 'true only if the new finding is the same defect the earlier fix claimed to resolve' },
    evidence: { type: 'string' },
  },
}

const key = f => `${f.file}:${f.class}:${Math.floor(f.line / LINE_BUCKET)}`
const adjudicated = new Map()
const tallies = []

function reviewTask(role) {
  return `Review this change per your role instructions.
Repo: ${a.repo}
Base ref: ${a.base}
Changed files (the review set — the diff is \`git -C ${a.repo} diff ${a.base} -- <files>\`, working tree vs base, restricted to these files):
  ${fileList}
${desc}
Return your findings via the structured output schema. ${role === 'bug' ? 'Every finding needs the concrete failure scenario in `evidence`.' : 'Every finding needs the quoted rule or a before/after sketch in `evidence`.'}`
}

function refuteTask(f) {
  return `Refute this ${f.class === 'bug' ? 'correctness' : 'style'} finding per your role instructions.
Repo: ${a.repo}   Base ref: ${a.base}
Finding at ${f.file}:${f.line} — ${f.title} [${f.severity}]
Claim: ${f.claim}
Evidence given: ${f.evidence}`
}

async function roleAgent(agentType, task, opts) {
  try {
    return await agent(task, { model: MODEL, ...opts, agentType })
  } catch (e) {
    if (!String(e).includes('not found')) throw e
    const fallback = `Read ${a.repo}/.claude/agents/${agentType}.md and adopt it as your complete role instructions (this session's agent registry predates that file). Obey its tools line: Read and read-only Bash only — never edit, never commit.\n\n${task}`
    return await agent(fallback, { model: MODEL, ...opts })
  }
}

async function dispatchReviewer(role, agentType, round) {
  const opts = { schema: FINDINGS_SCHEMA, phase: `Round ${round} review`, label: `${agentType} r${round}`, effort: tier.effort }
  let r = await roleAgent(agentType, reviewTask(role), opts)
  if (r === null) r = await roleAgent(agentType, reviewTask(role), { ...opts, label: `${agentType} r${round} retry` })
  if (r === null) return null
  return r.findings.map(f => ({ ...f, class: role }))
}

for (let round = 1; round <= MAX_ROUNDS; round++) {
  log(`Round ${round}/${MAX_ROUNDS}: dispatching fresh skeptic${tier.stylist ? ' + stylist' : ''}`)
  const [skeptic, stylist] = await parallel([
    () => dispatchReviewer('bug', 'pr-gauntlet-skeptic', round),
    () => (tier.stylist ? dispatchReviewer('style', 'pr-gauntlet-stylist', round) : []),
  ])
  if (skeptic === null || stylist === null) {
    return { verdict: 'ERROR', detail: `round ${round}: a reviewer failed twice; convergence cannot be certified`, tallies }
  }

  const t = { round, skeptic_found: skeptic.length, stylist_found: stylist.length, auto_dropped: 0, refuter_capped: 0, refuted: 0, confirmed: 0, fixed: 0, rejected: 0, errors: [] }
  tallies.push(t)

  const seenThisRound = new Set()
  const fresh = []
  for (const f of [...skeptic, ...stylist]) {
    const k = key(f)
    if (seenThisRound.has(k)) continue
    seenThisRound.add(k)
    const prior = adjudicated.get(k)
    if (prior && (prior.status === 'refuted' || prior.status === 'rejected')) { t.auto_dropped++; continue }
    fresh.push({ ...f, key: k })
  }

  if (fresh.length === 0) {
    log(`Round ${round}: both reviewers empty after adjudication — CONVERGED`)
    return { verdict: 'PASS', rounds_run: round, tallies, clean: true }
  }

  const order = { blocker: 0, major: 1, minor: 2 }
  fresh.sort((x, y) => order[x.severity] - order[y.severity])
  const toRefute = fresh.slice(0, MAX_REFUTERS_PER_ROUND)
  t.refuter_capped = fresh.length - toRefute.length
  if (t.refuter_capped > 0) log(`Round ${round}: refuter cap dropped ${t.refuter_capped} minor finding(s) to next round`)

  const verdicts = await parallel(toRefute.map(f => async () => {
    try {
      const v = await roleAgent('pr-gauntlet-refuter', refuteTask(f), { schema: VERDICT_SCHEMA, phase: `Round ${round} verify`, label: `refute ${f.file}:${f.line}`, effort: tier.effort || 'medium' })
      return { f, v }
    } catch (e) {
      return { f, v: null, err: String(e) }
    }
  }))

  const confirmed = []
  for (const row of verdicts.filter(Boolean)) {
    if (!row.v) { t.errors.push(`refuter failed on ${row.f.key}${row.err ? ': ' + row.err : ''}`); continue }
    if (row.v.refuted) { t.refuted++; adjudicated.set(row.f.key, { status: 'refuted', finding: row.f }); continue }
    const prior = adjudicated.get(row.f.key)
    if (prior && prior.status === 'fixed') {
      const ruling = await agent(`Two review findings share the location bucket ${row.f.key}. The first was confirmed and a fixer claims it is fixed; the second was just confirmed by a fresh round. Read the current code at that location in ${a.repo} and rule: is the new finding the SAME defect (the fix failed or regressed), or a DIFFERENT defect that happens to sit nearby?
Earlier (claimed fixed): ${JSON.stringify(prior.finding)}
New (just confirmed): ${JSON.stringify(row.f)}`,
        { model: MODEL, schema: ARBITER_SCHEMA, phase: `Round ${round} verify`, label: `arbiter ${row.f.key}`, effort: 'high' })
      if (ruling && ruling.same) {
        return { verdict: 'THRASH', detail: `fixed finding recurred confirmed: ${row.f.key} — ${row.f.title}`, arbiter_evidence: ruling.evidence, oscillating: row.f, tallies }
      }
      if (!ruling) t.errors.push(`arbiter failed on ${row.f.key}; treating as a distinct finding`)
    }
    confirmed.push({ ...row.f, refuter_evidence: row.v.evidence })
  }
  t.confirmed = confirmed.length

  if (a.report_only) {
    return { verdict: 'REPORT', findings: confirmed, tallies }
  }
  if (confirmed.length === 0 && t.errors.length === 0 && t.refuter_capped === 0) {
    log(`Round ${round}: every finding refuted — CONVERGED`)
    return { verdict: 'PASS', rounds_run: round, tallies, clean: false }
  }
  if (confirmed.length === 0) {
    log(`Round ${round}: nothing confirmed but ${t.errors.length} refuter error(s) / ${t.refuter_capped} capped — looping to re-adjudicate`)
    continue
  }

  const lastRound = round === MAX_ROUNDS
  if (lastRound && !tier.fixLastRound) {
    return { verdict: 'FAIL_MAX_ROUNDS', open_findings: confirmed, tallies }
  }

  log(`Round ${round}: fixing ${confirmed.length} confirmed finding(s)`)
  const fix = await agent(`Apply these confirmed review findings in ${a.repo}. The source is the oracle; a finding that survived refutation can still be wrong — verify each against the code before editing, and reject it (with one line of evidence) rather than degrade correct code.
Rules: edit ONLY the review-set files listed below (creating a new test file is allowed — report it in new_files); never commit; never run destructive git; follow the workspace clean-code rules (self-documenting, one-line comments max).
Smallest correct fix, always. Never add features, flags, options, or new surface to satisfy a finding — new surface spawns next-round findings and the loop diverges (proven: a 124-line linter grew to 350 across 3 rounds and failed its own gate). If a finding can only be satisfied by new functionality, reject it with evidence 'requires new surface — backlog'. When a finding targets code a previous fix ADDED, prefer deleting that addition over patching it.
After editing, run the mechanical gate on the files you touched and fix what it flags: \`ruff format --force-exclude\` + \`ruff check --fix --force-exclude\` on durable .py (lib/ or .claude/scripts/), \`python3 ${a.repo}/.claude/scripts/lint_comments.py <files>\` if that script exists, and \`grep -nE 'Path\\.home\\(\\)|/Users/|Developer/work|@mountain\\.com|\\.databrickscfg|\\.zshrc' <files>\` (no personal paths; this workflow file itself whitelists nothing).
Review set:
  ${fileList}
Findings (JSON):
${JSON.stringify(confirmed, null, 2)}`,
    { model: MODEL, schema: FIX_SCHEMA, phase: `Round ${round} fix`, label: `fixer r${round}` })

  if (fix === null) {
    return { verdict: 'ERROR', detail: `round ${round}: fixer failed; findings stand unapplied`, open_findings: confirmed, tallies }
  }
  t.fixed = fix.fixed.length
  t.rejected = fix.rejected.length
  const byKey = new Map(confirmed.map(f => [f.key, f]))
  for (const k of fix.fixed) adjudicated.set(k, { status: 'fixed', finding: byKey.get(k) || { key: k } })
  for (const r of fix.rejected) adjudicated.set(r.key, { status: 'rejected', finding: byKey.get(r.key) || { key: r.key } })
  if (fix.new_files.length) log(`Round ${round}: fixer created ${fix.new_files.join(', ')}`)
  if (lastRound) {
    log(`Round ${round}: fixed ${t.fixed}, rejected ${t.rejected} — last round of the ${a.tier || 'medium'} tier, so the fixes are NOT re-reviewed`)
    return {
      verdict: 'FIXED_UNVERIFIED',
      tier: a.tier || 'medium',
      fixed: fix.fixed,
      rejected: fix.rejected,
      unreviewed: t.refuter_capped,
      new_files: fix.new_files,
      mechanical: fix.mechanical,
      tallies,
    }
  }
  log(`Round ${round}: fixed ${t.fixed}, rejected ${t.rejected} — next round re-reviews the fixes with fresh agents`)
}

return { verdict: 'ERROR', detail: 'loop exited without a verdict', tallies }
