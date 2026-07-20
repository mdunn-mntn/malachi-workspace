# Plan — The Scaled-Execution Engine (Orchestration Layer)

> Component plan for the super-structure. Scope: **the engine that transforms the current workspace
> into whatever target the sibling plans define — at a scale that blows past any single context
> window — reliably and at high quality.** Sibling plans define *target structures* (tickets layer,
> analysis ladder, deck structure, BQ velocity). This plan defines the **machine that ports the
> existing corpus onto those targets**: how work is decomposed, dispatched to many agents, kept
> context-bounded, adversarially verified, and merged. Distilled from Anthropic's Bun→Rust rewrite
> (1M+ lines, 11 days, ~64 concurrent Claudes) and mapped onto the kit you **already have**.

---

## 0. Thesis

**A massive AI task never fits in one context window, so stop trying to fit it. Shrink the unit of
work until a single fresh context can do it perfectly; externalize all shared memory to files;
let a tool generate the work queue and measure progress; verify every unit with an adversary that
never wrote it; and fix the process, not the artifact, when a class of mistake recurs.**

The Bun rewrite is proof this works at extreme scale. It is not magic and not a bigger model doing
one giant task — it is **many small, disposable contexts** coordinated by files on disk and a
deterministic loop. Your workspace restructure is the same shape: thousands of files that must be
relocated, renamed, re-documented, and mined for durable knowledge — far more than one chat can
hold. The engine below is how you run it without drowning, and it is ~80% built already.

---

## 1. The five questions, answered directly

You asked five things about how Bun did it. Here are the answers in one place; the rest of the plan
operationalizes them.

| Your question | The one-line answer | Where it's built here |
|---|---|---|
| **How did they structure prompts & workflow?** | ~50 *dynamic workflows*, each a loop over a work queue: `pop task → implement → 2 adversarial reviews → apply fix → commit one file`. Not one prompt — a **pipeline of tiny, identical loops**. | §5 (the loop), §12 (the phase ladder) |
| **How did they not run out of context?** | The unit was one file/one crate/one test. Each agent's context held **only that unit + a shared guide + the target** — never the codebase. Shared memory lived in **files** (`PORTING.md`, `LIFETIMES.tsv`, `errors.txt`), and each worker was a **fresh, disposable context** discarded after one unit. | §4 (the core mechanism) |
| **How did they make the AI perform excellently?** | **Adversarial review with split context** (2 reviewers who see only the diff, told "assume it's wrong"), a **de-risking ladder** (guide → pilot on 3 files → scale), a **model-independent correctness oracle** (the test suite), and **fixing the process** when a mistake recurred. | §5, §9, §11 |
| **Many agents / windows / tasks?** | One **orchestrator** (holds the queue + loop, no heavy content) dispatches many **workers** (fresh contexts, one job each), sharded across **git worktrees** so they never collide. Pipeline the common case; barrier only for merges. | §8 |
| **How do agents come back to the main task?** | They don't "remember" — they **re-read externalized state** (the manifest's progress, the lint count, `git log`). The main task is a file, not a memory. Resuming = reading the index, not recalling the history. | §4.3, §6 |

---

## 2. Requirements → structure traceability

Your brief maps 1:1 onto the design. This table is the contract.

| Requirement (your words) | Where it lives | Why it's AI-native |
|---|---|---|
| "iteratively go through our **entire** workspace folder structure" | **The manifest** (§6) — one row per file/folder, the externalized work queue | Enumerated once by a script; agents pop rows. The corpus is a list on disk, never held in a head. |
| "documentation, re-writing, **and learning**" | **The unit loop** (§5) — each unit both *ports* the artifact to target **and** graduates durable facts to `knowledge/` | Three outputs per unit (relocate, rewrite, learn) are a fixed contract, not an ad-hoc decision. |
| "sheer number of files… we'll hit context limits fast" | **Context bounding** (§4) — small unit + externalized memory + disposable workers | No single context ever holds more than one unit. Scale is unbounded by design. |
| "how to structure it" | **The de-risking ladder** (§9) + **phase ladder** (§12) | A fixed sequence: prep the guide → pilot one slice → scale in shards → lint to green → synthesize. |
| "turn it into a plan to give to an AI prompt" | **The kickoff plan** (§13) — a paste-ready orchestration prompt + a `Workflow` script skeleton | The plan is executable, not just descriptive. |
| "fundamentals of many agents, windows, tasks… back to the main task" | **Parallelism model** (§8) — orchestrator/worker split, worktrees, pipeline vs barrier, Workflow vs Agent vs terminals | The coordination primitives are named and mapped to tools you have. |
| "make sure the AI performed excellently" | **Adversarial verification** (§5) + **process-fixing** (§11) + **oracle** (§6.4) | Quality is enforced by a second agent and a linter, not by hoping the first agent was careful. |

---

## 3. Design principles (the "why")

1. **Shrink the unit until a fresh context nails it.** The single most important lever. One file,
   one folder, one lint violation — never "a subtree." If a unit needs more than its own content +
   the guide + the target to complete, it is too big; split it.
2. **Externalize memory to files.** The project's shared state (the mapping rules, the work list,
   the provenance of every decision) lives in versioned files that any agent reads on demand. Context
   is a scratchpad, never a database.
3. **Disposable workers.** Each worker is spawned fresh, does one unit, commits, and dies. Context
   never accumulates, so it never overflows. Amnesia is a feature.
4. **Content-free orchestrator.** The thing that loops holds the *queue position and the loop logic*,
   not the artifacts. It dispatches heavy content to workers and reads back only pass/fail + a path.
5. **The queue is tool output.** A build, a linter, or a `find` generates the work list and measures
   progress (violation count → 0). Never maintain the todo list by hand — it drifts and it's context you
   don't have to spend.
6. **Adversarial verification, split-context.** The agent that writes never approves. Two reviewers,
   each a fresh context seeing only the diff + source, told to assume it's wrong. Author-bias is
   structurally removed.
7. **Fix the process, not the artifact.** When a mistake recurs across units, edit the guide/prompt
   and re-run — never hand-patch dozens of outputs. Quality compounds; whack-a-mole does not.

---

## 4. The core mechanism — why you never run out of context

This is *the* question. The answer has four parts. Together they make the maximum context any single
agent ever holds a **constant**, independent of corpus size.

### 4.1 The unit is tiny and self-contained

Bun's atomic unit was **one `.zig` file → one `.rs` file**. A single implementer's entire context
was: the one source file, the shared `PORTING.md` guide, and the target file it was writing. Never
the other 1,447 files. Your unit is **one workspace file/folder → its target home + its doc**.

```
What a worker's context holds (a constant, ~one unit):

   ┌─────────────────────────────────────────────┐
   │  ONE source unit   (the .sql / .md / folder) │   ← the only heavy thing
   │  + the GUIDE       (mapping rules, shared)   │   ← cached, small
   │  + the TARGET      (where it's going)        │   ← a path + a template
   └─────────────────────────────────────────────┘
            ▲  never the whole corpus  ▲
```

If a unit doesn't fit, the unit is wrong, not the model. Split folders into files; split files into
sections.

### 4.2 Shared memory lives on disk, not in context

The knowledge a thousand agents must agree on is written **once, to files**, and re-read on demand:

| Bun's externalized memory | Your equivalent (mostly built) |
|---|---|
| `PORTING.md` — Zig→Rust pattern map | `workflows/INGEST_GUIDE.md` + `knowledge/folder_definitions.md` + the sibling target specs → **one `RESTRUCTURE_GUIDE.md`** (§6.1) |
| `LIFETIMES.tsv` — per-field spec every agent reads | `workflows/manifest.tsv` — per-file target spec (§6.2) |
| `errors.txt` — the work queue | lint output / `manifest.tsv` unstarted rows (§6.3) |
| the git repo — accumulated result | same |

No agent has to *remember* the rules — it reads the guide. No agent has to *remember* what's left —
it reads the manifest. The corpus-sized state is on disk; the context-sized state is one unit.

### 4.3 Workers are disposable; the orchestrator is content-free

```
        MAIN SESSION (orchestrator)                    holds: queue index, loop logic
        holds NO file contents                         reads back: {unit, PASS/FAIL, path}
              │
              │ dispatch (fresh context each time)
              ▼
   ┌──────────┬──────────┬──────────┬──────────┐
   │ worker 1 │ worker 2 │ worker 3 │  …  N     │       each holds: ONE unit + guide + target
   │  (dies)  │  (dies)  │  (dies)  │           │       each writes: one file, commits, exits
   └──────────┴──────────┴──────────┴──────────┘
              │
              ▼
        FILES + COMMITS  ◀── the durable result; the "memory" of progress
```

"Coming back to the main task" is not recall — it is **re-reading externalized state**. A resumed
session reads `manifest.tsv` (what's done), the lint count (what's broken), and `git log` (what
landed). This is why the work survives compaction, a killed session, or you closing the laptop: the
state was never in a context window to begin with.

### 4.4 Prompt caching makes "every agent re-reads the guide" cheap

Bun spent 72B **cached** input-token reads against 5.9B uncached. The shared guide + system prompt
are identical across thousands of dispatches, so they hit cache and cost a fraction. This is *why*
you can afford to re-hand the guide to every worker instead of trying to keep it "in memory" — the
cache is the memory. (Your session already runs a 1-hour cache TTL; the same economics hold at your
scale, which is orders of magnitude smaller.)

> **The takeaway in one sentence:** context stays bounded because the unit is small, the shared
> memory is a file, the worker is thrown away after one unit, and progress is measured by a tool —
> so corpus size scales the *number of units*, never the *size of any context*.

---

## 5. The unit loop — implement → 2 adversarial reviews → fix → commit

The atom of the whole engine. Identical for every unit; this is what you paste into a workflow.

```
for each unit in manifest:                        # the queue is on disk
    doc  = implementer(unit, GUIDE, target)       # ports + documents + surfaces learnings
    r1,r2 = review(doc, source), review(doc,source)   # 2 fresh contexts, "assume it's wrong", diff-only
    doc  = fixer(doc, [r1, r2], source)           # applies blockers; rejects wrong findings with evidence
    commit(one_file)                              # git add <path> && git commit — NEVER whole-tree
```

**Role separation is the quality engine — never collapse it:**

| Role | Sees | Produces | Write access | Your agent file |
|---|---|---|---|---|
| **implementer** | source unit + guide + template + any existing doc | the ported/documented artifact | yes | `.claude/agents/implementer.md` |
| **reviewer-adversarial ×2** | **only** the produced artifact + its source | a numbered fault list (blocker/should-fix/nit) | **no** | `.claude/agents/reviewer-adversarial.md` |
| **fixer** | artifact + both reviews + source | the corrected artifact | yes | `.claude/agents/fixer.md` |
| **synthesizer** (barrier, end only) | all per-unit outputs of a type | merged master doc + rebuilt indexes | yes | `.claude/agents/synthesizer.md` |

Why two reviewers who never wrote it: the author wants the work accepted; the reviewer wants it
found wrong. Bun's own writeup: *"The Claude that wrote the code wants the code to get accepted. The
Claude that reviews wants to find issues."* Splitting the contexts removes merge-bias mechanically —
you already ship this exact roster, including reviewers with **no Write/Edit tools** so isolation is
enforced by capability, not just instruction.

---

## 6. The work queue is tool output — the self-regenerating todo list

Bun's most elegant move: **the compiler wrote the todo list.** `cargo check` dumped ~16,000 errors to
a file, grouped by crate; agents popped errors; progress was the error count falling to zero. The
queue regenerated itself every time you re-ran the tool, and "done" was unambiguous and machine-checked.

Your restructure has the same primitive, and you already own the tools:

### 6.1 The Guide (the `PORTING.md` analog) — write this first
A single `workflows/RESTRUCTURE_GUIDE.md` that fuses the rules already scattered across
`folder_definitions.md`, `INGEST_GUIDE.md`, the naming convention, the front-matter schema, and the
sibling target specs. For each **source artifact type** it states: target location, target name,
required front-matter, what to document, and what counts as "durable → graduate to `knowledge/`."
This is the one file every implementer reads. **Adversarially review the guide itself before scaling**
(Bun ran a review pass on `PORTING.md` + `LIFETIMES.tsv` before writing any code).

### 6.2 The Manifest (the `LIFETIMES.tsv` analog) — the enumerated corpus
`workflows/manifest.tsv`, one row per unit: `type ⇥ current_path ⇥ target_path ⇥ shard ⇥ status`.
Built once by a `find`, sharded so concurrent agents write disjoint files (§8). The agent-pass runbook
already specifies exactly this, including the prune rules that stop agents re-documenting the kit into
itself.

### 6.3 The queue = manifest rows + lint violations
Two self-regenerating queues, run in sequence:
- **Port queue:** unstarted `manifest.tsv` rows. Pop → run the loop → mark done.
- **Cleanup queue:** run the lint suite — `lint_coverage.py`, `check_ticket_layout.sh`,
  `build_index.sh` (must run clean), plus a `lint_tickets.py` if the work-structure plan lands one.
  **Every violation is a work item.** Loop the unit-loop over violations until the count is zero.
  This is your `cargo check` — the linter *is* the queue and the progress bar.

### 6.4 The oracle (the `test suite` analog) — how you know a unit is really done
Bun merged only when a **model-independent** oracle (the TypeScript test suite, language-agnostic by
construction) went 100% green — *and* Jarred manually confirmed the tests were actually running, not
skipped. Your oracle is the **lint suite + `build_index.sh` clean + a human spot-check of a random
sample against source**. A unit is "done" only when its lints pass; a *slice* is mergeable only when
its whole shard is green **and** you've read 3–4 outputs against their sources yourself. Never trust
the reviewers' green without spot-checking that the reviewers are actually catching discrepancies —
that is "reviewing the reviewers," and it is non-optional.

---

## 7. The three externalized-memory artifacts (summary)

Everything the engine needs to survive amnesia lives in exactly three places. Learn these; they are
the whole system's memory.

| Artifact | Role | Analog | Status |
|---|---|---|---|
| `workflows/RESTRUCTURE_GUIDE.md` | the rules every worker obeys | `PORTING.md` | **to write** (fuse existing docs) |
| `workflows/manifest.tsv` | the enumerated work queue + target spec | `LIFETIMES.tsv` / `errors.txt` | **to generate** (one `find`) |
| the lint suite + `git` | the oracle + the durable result | the test suite + the repo | **built** |

---

## 8. Parallelism — many agents, windows, tasks, and back to the main task

### 8.1 The orchestrator/worker split
One **orchestrator** (a top-level session, or one `Workflow` run) holds the loop and the queue. It
dispatches **workers** (sub-agents), each a fresh context with one job. **Sub-agents cannot nest** —
the orchestrator must be the top-level session, never another agent.

### 8.2 Three ways to run the fan-out (pick per phase)

| Mechanism | What it is | Best for | Cost/risk |
|---|---|---|---|
| **`Workflow` tool** | a deterministic script (`pipeline`/`parallel`/`agent`/loop-until-dry) that fans out sub-agents and returns structured results — *the exact "dynamic workflow" primitive Bun used* | the bulk pass: hundreds of units, one loop, programmatic control flow | one command; resumable via `resumeFromRunId`; concurrency auto-capped (~10–16) |
| **`Agent` tool from the main session** | dispatch sub-agents by hand, a few at a time, reading each result | the **pilot**, and steering when a class of mistake appears | you drive the loop; full visibility |
| **Multiple terminal windows / sessions** | N Claude Code windows, each on its **own git worktree/shard** | coarse manual parallelism + live monitoring while a Workflow runs | you manage collisions via worktrees |

Recommendation: **pilot with the `Agent` tool** (few units, high visibility), **scale with the
`Workflow` tool** (the bulk pass), and keep **one or two extra terminal windows** open on separate
worktrees for monitoring and hand-steering — this is the direct analog of Jarred watching workflows
and editing the loop while 64 Claudes ran.

### 8.3 Sharding and worktrees (the collision problem Bun hit first)
Bun's very first false start: agents ran `git stash` / `git reset` and clobbered each other; putting
each in its own worktree risked running out of disk. The resolution:
- **Shard so concurrent workers write disjoint files.** Shard by top-level folder (`tickets/`,
  `knowledge/`, `documentation/`, `claude-prompts/`) or by dataset within `knowledge/bq/`. Every unit
  writes a **unique target path**, so "no two agents touch the same file" holds by construction.
- **One git worktree per shard** (not per agent) — cap the count to what disk allows. Bun peaked at 4
  worktrees × 16 Claudes = 64. Yours is far smaller: start with **2–4 shards**.
- Single-file merge targets (a shared glossary, an index) are **never** written in parallel — workers
  write per-unit **fragments** to `_staging/`, and a single **synthesizer** barrier merges them at the
  end. This rule is already in `INGEST_GUIDE.md` (rule 7).

### 8.4 Pipeline vs barrier (the default is pipeline)
Each unit flows `implement → review → fix → commit` **independently** — unit B doesn't wait for unit
A. That's a **pipeline** (the `Workflow` tool's default and cheapest wall-clock). Use a **barrier**
(wait for all units) **only** for synthesis: merging fragments, deduping a glossary, rebuilding
indexes — steps that genuinely need every prior result at once.

---

## 9. The de-risking ladder — prep → pilot → scale (never skip a rung)

Bun did not point 64 Claudes at 1,448 files on day one. It climbed:

1. **Prep the guide (hours, not minutes).** Jarred spent ~3 hours building `PORTING.md` with Claude,
   then a `LIFETIMES.tsv` workflow, then **adversarially reviewed both** and read them himself. →
   *You:* write `RESTRUCTURE_GUIDE.md`, review it adversarially, read it yourself. The guide's quality
   is the ceiling on every downstream unit's quality.
2. **Trial run on 3 units.** Bun ported *3* files through the full loop before scaling. → *You:* run
   the full loop on ONE small slice (~5–15 units — one ticket folder, or one `knowledge/bq` dataset)
   using the `Agent` tool. **Human checkpoint:** read 3–4 outputs against source; are they faithful?
   consistent? does `build_index.sh` pick them up? Tune the guide until the pilot is clean.
3. **Scale only after the pilot is clean.** Run the `Workflow` pass over the full manifest, 2–4 shards
   in worktrees. Monitor; when a class of mistake recurs, stop, fix the guide (§11), resume.

**Measure the pilot's token spend and multiply by (total_units / pilot_units) before launching the
full pass.** Bun cost ~$165k at 64-agent/11-day scale; yours should be *orders of magnitude* smaller —
keep it that way by piloting and sharding, not by running 24/7.

---

## 10. Guardrails — Bun's "false starts" made into hard rules

Every rule below is a scar from a Bun failure. You already encode most of them in
`agent_pass_runbook.md` §Guardrails; this is the consolidated list.

| Rule | The false start it prevents |
|---|---|
| **Commit specific files only** (`git add <path>`); **never** `git stash`/`git reset`/`git checkout -- .`/anything whole-tree | Agents in one tree clobbered each other's uncommitted work |
| **One worktree per shard**, capped to disk | Per-agent worktrees ran the box out of disk |
| **No slow commands in the loop** — no full `bq` scans, no full builds; `bq show`/`--dry_run` only | One slow `grep`/`cargo` froze disk I/O and stalled every agent |
| **No stubs, no essay-justifications** — reviewers reject any "TODO/unknown" section or a paragraph explaining why a gap is "fine" | Claude "completed" crates by stubbing functions and writing long apologetic comments |
| **Resource-isolate destructive/heavy work** (a slice can't exhaust the machine) | Tests that spawned 10k processes / filled the disk crashed the host |
| **Fresh context per unit; reviewers see only the diff** | Author-bias and context bleed let plausible-but-wrong work through |

---

## 11. Fix the process, not the artifact

The highest-leverage habit in the whole method. When a mistake appears in **one** doc, fix the doc.
When the **same** mistake appears across many, **stop and fix the thing that generated them** — the
`RESTRUCTURE_GUIDE.md` or the agent prompt — then re-run the affected units. Bun did this repeatedly:
"stub out the function" → added a reviewer rule rejecting stubs; "paragraph-long workaround comments" →
added *"if you need a paragraph to justify the workaround, the code is wrong."* One prompt edit fixed
thousands of future outputs. Hand-patching N artifacts scales linearly with the corpus; fixing the
process is O(1). This is why the guide is a living file, not a frozen spec.

---

## 12. Application — the whole-workspace restructure as a phase ladder

Bun's ~50 workflows were really **one loop applied to a sequence of queues**: generate the guide →
port every file → fix every compiler error → get each subcommand to run → get every test to pass →
refactor. Your restructure is the same sequence with your queues substituted in. Run these **in order**;
each phase's output is the next phase's input.

| Phase | Queue (what's popped) | Unit loop does | Oracle (done when) | Mechanism |
|---|---|---|---|---|
| **0 · Guide** | the rules, scattered | fuse `folder_definitions` + `INGEST_GUIDE` + naming + front-matter + sibling target specs into `RESTRUCTURE_GUIDE.md` | 2 adversarial reviews of the guide pass; you've read it | `Agent` (few) |
| **1 · Enumerate** | the corpus | `find` → `manifest.tsv` (type, current→target path, shard); prune the kit's own scaffolding | every file has a row + a target | one script |
| **2 · Pilot** | ~5–15 rows, one slice | full unit loop (§5) — relocate + document + surface learnings | you verify 3–4 vs source; `build_index` clean; guide tuned | `Agent` (hand-driven) |
| **3 · Port (scale)** | all unstarted `manifest.tsv` rows | full unit loop per file, 2–4 shards in worktrees | every row `done`; per-shard lints green | `Workflow` (bulk) |
| **4 · Lint-to-green** | every lint violation | fix the flagged unit (naming, front-matter, misplacement, stub) | violation count = 0; `build_index.sh` clean | `Workflow`, loop-until-dry |
| **5 · Synthesize** | all per-unit outputs (barrier) | merge `_staging/` fragments → masters; rebuild all indexes; graduate durable `[learned]` facts → `knowledge/`; update `MEMORY.md` | indexes match disk; random human spot-check passes | `synthesizer` (barrier) |

The **"learning" dimension** you called out is not a separate phase — it's the **third output of every
unit** (relocate, rewrite, *graduate durable facts*) plus the Phase-5 barrier that promotes the
accumulated `[learned]` tags into `knowledge/`. The graduation pathways in the work-structure plan
(§6: findings→`knowledge/`, code→`_lib/`) are the declared seams; this engine is what walks them at scale.

---

## 13. The kickoff plan (paste-ready)

### 13.1 The orchestration prompt (hand to a fresh top-level session)

```
You are the ORCHESTRATOR for a workspace restructure pass. You hold the loop and the queue;
you never hold file contents — you dispatch sub-agents and read back {unit, PASS/FAIL, path}.

SHARED MEMORY (read once, re-hand to every worker):
  - workflows/RESTRUCTURE_GUIDE.md   ← the rules (target path, name, front-matter, what to learn)
  - workflows/manifest.tsv           ← the work queue (type, current→target, shard, status)

THE LOOP (per unstarted manifest row, pipelined — do NOT wait between units):
  1. implementer  → port the unit to its target path + write its doc + tag durable facts [learned];
                    hand it ONLY: the source unit, RESTRUCTURE_GUIDE.md, the target template.
  2. 2× reviewer-adversarial IN PARALLEL → hand each ONLY the produced doc + its source +
                    "assume it's wrong; find every discrepancy vs source and every guide violation."
  3. fixer        → apply blockers/should-fixes; reject wrong findings with source evidence; no new claims.
  4. commit ONE file: git add <target_path> && git commit -m "restructure: <unit>". Mark the row done.

HARD RULES (non-negotiable — these are Bun's scars):
  - Commit specific files only. NEVER git stash / git reset / git checkout -- . / any whole-tree git.
  - No slow commands in the loop (no full bq scans, no full builds; bq show/--dry_run only).
  - No stubs, no essay-justifications — reviewers reject them.
  - When the SAME mistake appears across units, STOP, fix RESTRUCTURE_GUIDE.md, then resume.

START: run the PILOT only — the first shard's ~10 rows. Then stop and report the outputs for my review.
Do not scale until I approve the pilot.
```

### 13.2 The `Workflow` script skeleton (for the bulk pass, Phase 3–4)

You have the `Workflow` tool — the deterministic fan-out primitive. The bulk pass is one `pipeline`
over the manifest; each unit runs implement → 2 reviews (parallel) → fix, independently.

```js
export const meta = {
  name: 'workspace-restructure-port',
  description: 'Port each manifest unit to target: implement → 2 adversarial reviews → fix → commit',
  phases: [{ title: 'Port' }, { title: 'Verify' }],
}
// args = array of manifest rows for ONE shard: [{type, current, target, slug}, ...]
const GUIDE = 'workflows/RESTRUCTURE_GUIDE.md'
const results = await pipeline(
  args,
  u => agent(
    `Port ${u.current} → ${u.target} per ${GUIDE}. Relocate + document + tag durable facts [learned]. `
    + `Commit ONLY ${u.target} (git add <path> && git commit). Never a whole-tree git command.`,
    { label: `impl:${u.slug}`, phase: 'Port', agentType: 'implementer' }
  ).then(() => u),
  u => parallel([1, 2].map(n => () =>            // 2 adversarial reviewers, diff-only, "assume wrong"
    agent(`Adversarially review ${u.target} against ${u.current}. Assume it's wrong; list every `
        + `discrepancy vs source + every ${GUIDE} violation. Do not fix, do not approve.`,
        { label: `rev${n}:${u.slug}`, phase: 'Verify', agentType: 'reviewer-adversarial',
          schema: { type:'object', properties:{ findings:{type:'array',items:{type:'string'}} },
                    required:['findings'] } }))
  ).then(reviews => ({ u, reviews }))
)
// Fixer pass only where reviewers found blockers; then the lint-to-green loop (Phase 4) runs the
// same shape over `check_ticket_layout.sh` / `lint_coverage.py` violations until the count is 0.
return results.filter(Boolean)
```

Run one invocation per shard (or shard inside the script), each shard in its own worktree. Resume a
killed run with `resumeFromRunId` — the unchanged prefix returns from cache, only new units re-run.

---

## 14. Merge / done criteria + cost framing

**Done (per the Bun bar):**
- [ ] Every `manifest.tsv` row is `done`; none stubbed.
- [ ] The full lint suite passes (`check_ticket_layout.sh`, `lint_coverage.py`) and `build_index.sh`
      runs clean — indexes match disk.
- [ ] A **random human spot-check** of scaled outputs against source passes (you confirmed the
      reviewers actually caught discrepancies — "review the reviewers").
- [ ] `_staging/` fragments synthesized into masters; `_staging/` cleared.
- [ ] Durable `[learned]` facts graduated to `knowledge/`; `MEMORY.md` updated.
- [ ] You ran a few things by hand and are confident — **then** you "merge" (adopt the new structure
      as canonical). Merge ≠ release: adopt when the oracle is green *and* you've eyeballed it.

**Cost framing:** cost ≈ `units × ~4 model turns/unit × avg tokens/turn`, heavily discounted by prompt
caching on the repeated guide. **Pilot, measure, multiply** before the full pass. This is a small
fraction of Bun's scale — do not run agents 24/7; run a bounded, sharded, piloted pass and stop when
the oracle is green.

---

## 15. Why it composes into the super-structure

- **It's the engine, not another structure.** The sibling plans define *targets* (ticket layout,
  analysis ladder, deck structure, BQ velocity). This plan is the **transform** that ports the existing
  corpus onto whichever targets the master-planner selects. It consumes the target spec as *input*
  (the guide), so it works regardless of which structure wins — including the two competing ticket
  models (ARCHITECTURE §7 vs work-structure §4); the engine ports to whichever the master-planner
  ratifies, unchanged.
- **Declared seams.** Its inputs are `RESTRUCTURE_GUIDE.md` (fed by every target plan) and
  `manifest.tsv` (the corpus). Its outputs are conforming files + graduated `knowledge/` facts. The
  master-planner wires target-specs → guide; this plan turns the crank.
- **Reuses the built kit.** The loop, roster, guardrails, staging/synthesis, and lint oracle already
  exist (`.claude/agents/`, `workflows/agent_pass_runbook.md`, `INGEST_GUIDE.md`, the linters). The
  only net-new artifacts are the fused `RESTRUCTURE_GUIDE.md` and a corpus-wide `manifest.tsv`.

**One-line summary for the synthesizer:** *the execution engine is one adversarially-verified unit
loop, run over a tool-generated queue, by disposable workers sharded across worktrees, coordinated by
a content-free orchestrator through files on disk — so quality is enforced by a second agent and a
linter, and context stays bounded no matter how large the corpus.*
