# The Agnostic Blueprint — how to run this workflow in any agent, on any machine

The vendor-neutral outline of the system. `README.md` explains the kit as shipped (a Claude Code
repo); this file explains the **pattern underneath it**, so you can rebuild it in Codex, Cursor,
Gemini CLI, a plain terminal, or whatever ships next — and so you can tell, for any piece of it,
whether that piece is portable or is a rented vendor feature.

Nothing here is specific to a company, a stack, or a job. Copy this file anywhere.

**Contents:** §1 thesis · §2 the four loops · §3 the layer model · §4 the primitives · §5 the
portability ladder · §6 harness matrix · §7 Codex adapter · §8 build order · §9 invariants ·
§10 failure modes · §11 what must never travel.

---

## 1. The thesis: why this ports

Most "AI workflow" setups are a pile of prompts in a vendor's config directory. Move machines or
change tools and you lose all of it. This one survives because of one rule:

> **Push every behavior to the lowest layer that can hold it.**
> A git hook beats an agent hook. A generated index beats a remembered convention.
> A script beats an instruction. An instruction beats a hope.

Follow that rule and the vendor-specific surface shrinks to a single instruction file plus a handful
of optional conveniences. Everything load-bearing ends up as **git, shell, python, and markdown** —
which every agent on every machine can already read and run.

Two ideas do the actual work:

1. **Load indexes, not the tree.** Curated docs stay on disk. The agent loads a tiny front door plus
   generated indexes it *greps*, and opens only the one doc a question needs. The corpus can grow
   without bound because it is never ingested whole.
2. **Generate and enforce, don't hand-maintain.** Indexes are regenerated from each doc's
   front-matter. A commit gate blocks malformed files. A periodic audit reviews the whole repo. The
   documents describing the system are themselves generated, so they cannot drift from the system.

Everything below is machinery in service of those two.

---

## 2. The four loops (this is the outline)

The whole system is four loops running at different clock rates. Build them in this order — each
one is useful alone, and each later loop assumes the earlier ones exist.

### Loop A — Orient (every session, seconds)

**Problem:** a fresh session knows nothing, and reading the repo to find out burns the context you
need for the work.

1. An always-loaded instruction file states the operating rules and **where to look**, never the
   content itself.
2. A session-start step prints a short orientation: repo state, what's stale, what's queued, the
   retrieval path.
3. The agent greps a generated keyword index for its topic and opens **one** doc.

**Done when:** a cold session can answer "where does X live?" without opening more than two files.

### Loop B — Do the work (per task, hours to days)

The bookends matter more than the middle.

1. **Frame first.** Before any work starts, agree in writing: the single question, why it matters
   (which decision changes), what "done" looks like (binary), the approach, and what result would
   flip the conclusion. Five lines, written into the work record.
2. **A gate enforces it.** A linter refuses to let the record move to `in_progress` while the framing
   is still a draft. There is an explicit skip hatch for trivial work, and it must state its reason.
3. **Work, recording as you go.** One folder per unit of work, one canonical record file inside it.
   The record is living: a finding lands, an assumption dies, the record is updated in the same beat.
4. **Capture at the end.** A single command sweeps the session for durable facts and routes each to
   its home: the knowledge docs, the work record, cross-session memory. Then it rebuilds indexes,
   lints, commits.

**Done when:** someone who wasn't there can read the record and know what was asked, what was found,
and what is still open.

### Loop C — Keep the record true (every commit, seconds)

Without this loop, Loops A and B rot within a month and the agent starts citing fiction.

1. Every doc carries **front-matter**: type, keywords, domain, a lifecycle state, and dates.
2. One generator reads all front-matter and **regenerates every index** — idempotently, byte-stable.
3. Linters validate the front-matter and the formats. Each exits non-zero on a violation.
4. A **commit gate** runs the linters plus an index-freshness check, scoped to the files *this*
   commit stages. Malformed file, or a changed doc whose regenerated index wasn't re-staged? Blocked.
5. One **doctor** command runs every deterministic check, with a `--fix` mode that repairs and
   re-stages.

**Done when:** it is *impossible* to commit a doc that the index doesn't know about.

### Loop D — Improve the system (weekly, minutes)

1. A read-only aggregator collects every signal: structure conformance, records missing framing,
   knowledge-base health, coverage debt, cost drift, recurring request shapes, whole-repo doctor run.
2. A reasoning pass reads the signals and writes **one ranked action list**.
3. **Propose-only.** The loop may append and suggest. It may never delete, merge, or rewrite the
   instruction files. Deletion is a human motion, always.

**Done when:** the system tells you what's wrong with it before you notice.

```
       ┌──────────── Loop D: audit (weekly, propose-only) ────────────┐
       │                                                              │
   ┌───▼────┐      ┌──────────────────────────┐      ┌────────────┐   │
   │ Loop A │─────▶│ Loop B: frame → work →   │─────▶│  Loop C:   │───┘
   │ orient │      │        capture           │      │ index/gate │
   └────────┘      └──────────────────────────┘      └────────────┘
      reads              writes records                keeps them true
```

---

## 3. The layer model

Five layers. The number in the last column is what you lose when you switch tools.

| # | Layer | Made of | Owns | Vendor coupling |
|---|---|---|---|---|
| L0 | **Repo** | git, folder conventions, naming | history, the shared substrate, the commit gate's teeth | **none** |
| L1 | **Deterministic** | shell + python scripts, git hooks | logging, detection, generation, blocking | **none** |
| L2 | **Knowledge** | markdown + YAML front-matter | meaning: facts, gotchas, runbooks, decisions, memory | **none** |
| L3 | **Instruction** | one always-loaded file (+ triggered procedures) | operating rules, retrieval routes, style caps | **low** — one file to re-point |
| L4 | **Agent features** | event hooks, subagents, native memory, MCP | convenience: automatic firing, parallelism, recall | **high** — replace per harness |

**The rule again:** a behavior belongs in the lowest layer that can hold it. If a check can be a git
hook, it must not be an agent hook. If a fact can be a generated index, it must not be a convention
you remind the model about. L4 should hold *nothing you would miss*.

Test your own setup: **delete L4 in your head.** If the workflow still functions — degraded, more
manual, but functioning — it ports. If it collapses, you have built on rented land.

### The honest scope of "automatic"

A shell hook can **print, log, and block**. It cannot invoke a model. So the genuinely zero-touch
behaviors are *detection* (a new object gets queued for documentation), *logging* (every expensive
operation gets recorded), *enforcement* (an unsafe command is blocked), and *orientation* (the
session-start print). Everything semantic — writing what a field means, routing a fact to its home
doc, judging whether a finding is real — is a **triggered** step a human starts.

Design to that. The system's promise is *nothing is lost or hidden*, not *a model runs unattended*.

---

## 4. The primitives

Every mechanism, stated by function. "Substitute" is what you build when your harness lacks the
feature — in every case there is one, which is the point.

### Retrieval and knowledge

| # | Primitive | Function | Layer | Substitute if absent |
|---|---|---|---|---|
| 1 | **Front door** | One short file mapping *task → the one doc to open*. Curated, hand-written, ~1 page. | L2 | — (just a file) |
| 2 | **Front-matter schema** | Every doc declares type, keywords, domain, lifecycle state, and dates. The only input the index generator reads. | L2 | — |
| 3 | **Index generator** | Reads all front-matter, regenerates keyword→doc, by-domain, by-lifecycle, and coverage indexes. Idempotent and byte-stable. | L1 | — |
| 4 | **Grep-don't-ingest** | The agent greps an index for a term and opens only what it names. Indexes may grow without bound because they are never read whole. | L3 | — (an instruction) |
| 5 | **Hot tier** | A tiny always-loaded memory file holding *only* cross-cutting rules, with a hard character budget. Everything else is grep-on-demand at zero resident cost. | L2/L3 | — |
| 6 | **Per-prompt recall** | Match the incoming prompt against the keyword index; inject pointers to the 1–3 matching notes. | L4 → L1 | Run the matcher as a script the agent calls, or fold the top routes into the front door |
| 7 | **Two-date coverage** | `synced` is stamped by machine; `verified` only when a human confirms against source. A doc is *stale* only when the machine date is newer than the human date. | L2 | — |
| 8 | **Append-only regions** | Explicit START/END markers so a generator can refresh part of a doc while hand-written findings survive untouched. | L2 | — |
| 9 | **Doc-debt queue** | A plain text file listing objects referenced but not yet documented. Append on detection, drain on cadence. | L1 | — |

### Work and records

| # | Primitive | Function | Layer | Substitute if absent |
|---|---|---|---|---|
| 10 | **Scaffolder** | One command creates a conforming work folder: validated name, fixed subfolders, a record file with front-matter that already passes the linter. | L1 | — |
| 11 | **Framing gate** | Five agreed lines (question, why, done-when, approach, kill criteria) written before work starts; a linter blocks the status change while they're draft. | L1+L2 | — |
| 12 | **Living record** | One canonical file per unit of work, updated in the same beat as the finding. A stale record is a bug. | L2 | — |
| 13 | **Anti-sprawl layout** | A whitelist of permitted subfolders; phases are headers, never directories; flat, monotonically numbered artifacts. Enforced by a linter. | L1 | — |
| 14 | **Capture** | A triggered sweep that routes the session's new facts to their home docs, corrects now-false lines, updates the record, rebuilds indexes, commits. | L3/L4 | A numbered checklist in the instruction file + a session-end reminder |

### Safety and cost

| # | Primitive | Function | Layer | Substitute if absent |
|---|---|---|---|---|
| 15 | **The wrapper** | Every expensive or risky external call goes through one script that estimates first, aborts over a threshold, runs, then records real cost and provenance to an append-only log. | L1 | — |
| 16 | **The block** | A pre-execution check that refuses the raw command so the wrapper cannot be bypassed. | L4 → L1 | A shell alias/function that shadows the raw command, plus a loud instruction-file rule and a periodic audit that greps history for bypasses |
| 17 | **Log then digest** | Never reason over raw logs. A deterministic aggregator produces the tables; the model curates only the conclusions, on cadence. | L1 | — |
| 18 | **Provenance card** | Any number that leaves the system can name the query, the run, and the filters that produced it. | L1 | — |
| 19 | **Output linter** | Hard character/word/bullet caps on outward-facing writing, checked by a script before it posts. | L1 | — |

### Enforcement and self-improvement

| # | Primitive | Function | Layer | Substitute if absent |
|---|---|---|---|---|
| 20 | **Commit gate** | Git `pre-commit` runs the doctor on staged files; `commit-msg` lints the message. Staged-scoped, so pre-existing debt never blocks unrelated work. Bypass exists and is loud. | L0/L1 | — (works in every harness, every editor, every CI) |
| 21 | **The doctor** | One command that runs every deterministic check. Modes: full, staged, `--fix`. The single entry point the gate and the audit both reuse. | L1 | — |
| 22 | **Generated inventory** | The component list is generated from the actual files. A diff means someone added a component without updating the docs. | L1 | — |
| 23 | **Role separation** | Author, adversarial reviewer, and fixer are *different contexts*. The reviewer gets the artifact and the source, never the author's reasoning, and is told to assume it's wrong. Reviewers have no write capability. | L4 → L3 | Run the roles sequentially in fresh sessions; capability isolation degrades to a prompt rule plus the commit gate |
| 24 | **Propose-only kernel** | The self-improvement loop may read, append, and suggest. It has no delete or merge authority over knowledge or instructions. | L3 | — |
| 25 | **Request mining** | Log a keyword-only fingerprint of each request (never the raw text); mine for recurring shapes; **propose** a new procedure when one recurs. | L1 | — |
| 26 | **Liveness monitoring** | Every background task is paired with a stall detector, because a *hung* task sends no completion signal — only a finished one does. Poll the task's output mtimes; alert on idle, not on slow. | L1 | — |
| 27 | **Instruction-file budget** | The always-loaded files hold behavioral rules and pointers only. New procedure goes into a triggered file. A rule may leave the hot path only if a real trigger reloads it. | L3 | — |
| 28 | **The barrier** | Packaging applies two ordered scrub maps — secrets, then domain context — and refuses to emit unless both acceptance sweeps come back clean. | L1 | — |

---

## 5. The portability ladder

When a harness lacks a feature, walk down. Each rung is strictly worse than the one above and
strictly better than nothing.

| Rung | Mechanism | Fires | Use when |
|---|---|---|---|
| 1 | **Git hook** | On commit, in every tool, forever | The check can wait until commit — put everything possible here |
| 2 | **Wrapper script + shell shadow** | On the command | The behavior attaches to a specific operation |
| 3 | **Agent event hook** | On the harness event | Only for things git and shell genuinely cannot see (a prompt arriving, a session starting) |
| 4 | **Triggered procedure** | When invoked by name | Multi-step work with judgement in it |
| 5 | **Instruction-file rule** | When the model remembers | Judgement that cannot be scripted |
| 6 | **A periodic audit that greps for violations** | Weekly | The backstop under every rung above — this is what makes rung 5 survivable |

**The pairing rule:** every rung-5 rule that matters should have a rung-1 or rung-6 partner that
notices when it was skipped. An instruction with no detector is a wish.

**Worked example — enforcing "expensive calls go through the wrapper":**
- Best (rung 2+3): shell function shadows the raw command; agent pre-execution hook blocks it too.
- No hooks (rung 2+6): keep the shell shadow; the weekly audit greps shell history and the log for
  calls that never produced a log record, and reports the gap.
- Nothing but a text file (rung 5+1): state the rule loudly; add a `pre-commit` check that any
  committed query file carries the wrapper's provenance header.

---

## 6. Harness matrix

Verified against vendor primary docs on **2026-08-12**. This surface moves fast — re-check before
you rely on a cell.

**The headline: `AGENTS.md` is now a real cross-vendor standard**, stewarded by the Agentic AI
Foundation under the Linux Foundation ([agents.md](https://agents.md/)). Most harnesses read it
natively, so one instruction file covers most of the field.

### Instruction file

| Harness | Reads `AGENTS.md`? | Native file | Nesting | Global/user file |
|---|---|---|---|---|
| **Codex CLI** | **yes** (+ higher-priority `AGENTS.override.md`) | `AGENTS.md` | concatenated root→cwd, later wins | `~/.codex/AGENTS.md` |
| **Cursor** | **yes** | `.cursor/rules/*.mdc` | nested files combine, specific wins | User Rules live in the UI, not a file |
| **Copilot** | **yes** (nearest wins, then `CLAUDE.md`, then `GEMINI.md`) | `.github/copilot-instructions.md` | path-scoped `*.instructions.md`, additive | `~/.copilot/copilot-instructions.md` |
| **Windsurf / Devin** | **yes** (case-insensitive) | `.devin/rules/*.md` | subdirectory file gets an auto glob | `~/.codeium/windsurf/memories/global_rules.md` |
| **Cline** | **yes** | `.clinerules/` (all `.md`/`.txt` combined) | workspace beats global | `~/.agents/AGENTS.md` |
| **Gemini CLI** | **opt-in** — set `context.fileName` in `.gemini/settings.json` | `GEMINI.md` | hierarchical, all concatenated | `~/.gemini/GEMINI.md` |
| **Aider** | **no auto-discovery** — add `read: AGENTS.md` to `.aider.conf.yml` | none | — | `.aider.conf.yml` is discovered from `$HOME` |

Cursor's CLI also reads `CLAUDE.md`, and VS Code Copilot honors it via `chat.useClaudeMdFile`. So the
portable move is **one file, two names**:

```bash
# write the rules once; every harness finds them
ln -s AGENTS.md CLAUDE.md
```

### Everything else

| Capability | Codex CLI | Cursor | Copilot | Gemini CLI | Windsurf | Cline | Aider |
|---|---|---|---|---|---|---|---|
| **Named procedures** | Skills: `.agents/skills/<n>/SKILL.md`, `$name` or `/skills` | Skills: `.cursor/skills/`, `.agents/skills/` — **also reads `.claude/skills/` and `.codex/skills/`** | `*.prompt.md` in `.github/prompts/` | TOML in `.gemini/commands/` | `.windsurf/workflows/*.md` | `.clinerules/workflows/*.md` | `--load` replay only |
| **Event hooks** | 11 events, `.codex/hooks.json` or `[hooks]` in config | 20+ events, `.cursor/hooks.json` | `.github/hooks/*.json` (VS Code also reads `.claude/settings.json`) | `hooks` in `settings.json`, 11 events | 12 events, `.windsurf/hooks.json` | plugin hook stages | **none** (`--lint-cmd`/`--test-cmd` only) |
| **Blocking hook** | `PreToolUse` → `permissionDecision:"deny"`, or exit 2 | pre-hooks block | `preToolUse` | `BeforeTool` | pre-hooks, exit 2 | yes | — |
| **Subagents** | built-in + custom TOML in `.codex/agents/` | yes | custom `.agent.md` | — | — | — | — |
| **MCP** | `[mcp_servers.*]` in config.toml | `.cursor/mcp.json` | repo settings / `.mcp.json` | `mcpServers` in settings.json | `~/.codeium/windsurf/mcp_config.json` | `~/.cline/mcp.json` | **none** |
| **Headless / CI** | `codex exec` (`--json`, `--output-schema`) | `cursor-agent -p` | `copilot -p --no-ask-user` | `gemini -p` | Devin CLI (separate product) | `cline "<task>"` | `aider -m --yes` |

**Read this matrix as confirmation of §3, not as a shopping list.** Every harness above has hooks,
procedures, and MCP in some form — and every one spells them differently, caps them differently, and
will rename them again next year. That is exactly why the load-bearing machinery lives in git.

### Gotchas worth knowing before you port

- **Instruction files are capped.** Codex truncates the combined `AGENTS.md` set at
  `project_doc_max_bytes`, **32 KiB by default**. Windsurf caps global rules at 6,000 characters and
  workspace rules at 12,000 per file. The hot-path budget (§4.27) is not a stylistic preference —
  past the cap your rules are silently cut off.
- **Skills directories overlap on purpose.** `.agents/skills/` is read by both Codex and Cursor, and
  Cursor additionally reads `.claude/skills/` and `.codex/skills/`. Pick `.agents/skills/` for
  anything you want portable.
- **Hook config is per-vendor and unshareable.** The *scripts* port unchanged (JSON on stdin, JSON on
  stdout, exit 2 to block, in every implementation above). Only the registration file differs. Write
  the scripts to be harness-neutral and keep a thin per-harness registration.
- **Hooks are a guardrail, not a boundary.** Codex's own docs say so: hosted tools like web search
  don't go through the local tool-hook path, so `PreToolUse` cannot see them. Anything that must be
  enforced belongs at rung 1 or 2 of the ladder (§5).
- **Vendor-generated memory is not your knowledge base.** Codex's local memories are off by default,
  are written in the background, and its docs explicitly say to treat them as generated state, not to
  hand-edit them, and to keep required guidance in `AGENTS.md` or checked-in docs. That is the same
  conclusion this kit reached: memory belongs in git, as files you write and index.

---

## 7. Codex adapter

Concrete mapping. Every path below is a Codex path; the scripts they point at are the same scripts.

### 7.1 Instructions

```
AGENTS.md                    # the shared rules (symlink CLAUDE.md → this)
~/.codex/AGENTS.md           # your personal cross-project rules
AGENTS.override.md           # optional, higher priority than AGENTS.md at the same level
```

Codex walks from the repo root down to your working directory and **concatenates one file per
directory**, later (deeper) files winning. That is strictly better than a single-file model: put
repo-wide rules at the root and subsystem rules in the subdirectory they govern.

Watch the **32 KiB combined cap** (`project_doc_max_bytes`). Raise it in `~/.codex/config.toml` if
you must, but the better fix is to move procedure behind a skill.

The set is built **once per session launch**, not per turn — restart to reload after an edit.

### 7.2 Procedures → skills

Move each triggered procedure to a skill directory. Codex and Cursor both read `.agents/skills/`, so
this is the portable home:

```
.agents/skills/
  frame/SKILL.md
  capture/SKILL.md
  oncall/SKILL.md
  workflow-audit/SKILL.md
```

`SKILL.md` needs YAML front-matter with `name` and `description`; optional `scripts/`, `references/`,
and `assets/` subdirectories travel with it. Invoke explicitly with `$frame` or the `/skills` picker.

**The description is the trigger.** Codex also selects skills *implicitly* when the task matches the
description, so front-load the real trigger phrases. Only name + description load up front (capped
around 2% of the context window); the body is read on selection. With many skills installed, Codex
shortens descriptions and may omit some — keep the set small and the descriptions sharp.

One loss worth planning for: skills have **no argument passing**. The deprecated
`~/.codex/prompts/<name>.md` mechanism (`/prompts:<name>`, with `$1`–`$9` and `$ARGUMENTS`) is the
only Codex path with parameters, and it is local-only and on its way out. Have the skill *ask* for
its argument, or read it from the session.

### 7.3 Hooks

`.codex/hooks.json` (repo) or `~/.codex/hooks.json` (personal), or inline `[hooks]` in `config.toml`.
Eleven events: `SessionStart`, `SessionEnd`, `SubagentStart`, `SubagentStop`, `PreToolUse`,
`PermissionRequest`, `PostToolUse`, `PreCompact`, `PostCompact`, `UserPromptSubmit`, `Stop`.

Shape is event → matcher group → handlers. Only `type: "command"` executes today. The contract is the
familiar one: **one JSON object on stdin, JSON on stdout** — so hook scripts written for another
harness usually port with no changes.

| This kit's hook | Codex event | Notes |
|---|---|---|
| Session orientation print | `SessionStart` | return `additionalContext` to inject the routing block |
| Per-prompt recall | `UserPromptSubmit` | **matcher is ignored on this event** — filter inside the script |
| Block the raw expensive command | `PreToolUse`, `matcher = "^Bash$"` | `hookSpecificOutput.permissionDecision = "deny"`, or exit 2 with a reason on stderr |
| Flag undocumented objects | `PostToolUse` | |
| Capture-due reminder | `Stop` | matcher ignored here too |

**Four Codex-specific traps:**

1. **The trust gate.** A non-managed hook must be reviewed and trusted via `/hooks`, and trust is
   recorded against the hook definition's **hash**. Edit the configured command and the hook is
   silently marked for review and **skipped until re-trusted** — you get a startup warning and
   nothing else. For automation, `--dangerously-bypass-hook-trust`; for teams, ship hooks as managed
   via `requirements.toml`.
2. **`PreToolUse` can deny a call but cannot abort the turn.** `continue:false`, `stopReason`, and
   `permissionDecision:"ask"` are parsed but unsupported there — return them and the hook is marked
   failed while **the tool call proceeds anyway**. To halt, deny at `PreToolUse` and stop at
   `PostToolUse` (`continue:false`) or `Stop`.
3. **Matching hooks from every layer all run, concurrently.** A higher-precedence layer does not
   replace a lower one, and one hook cannot prevent another from starting.
4. **Hosted tools bypass hooks entirely.** Web search and other hosted tools don't take the local
   function-tool path.

### 7.4 Subagents

Built-ins are `default`, `worker`, `explorer`. Custom agents are **TOML** files in `.codex/agents/`
(project) or `~/.codex/agents/` (personal), requiring `name`, `description`, and
`developer_instructions`, and optionally carrying `model`, `model_reasoning_effort`, `sandbox_mode`,
scoped `[mcp_servers.*]`, and `[[skills.config]]`. The `name` field, not the filename, is the
identity. Tune concurrency with `[agents] max_concurrent_threads_per_session`.

Two differences that matter for the role-separation primitive (§4.23):

- **Dispatch is prompt-driven, not a callable tool.** There is no user-invoked "run this subagent
  with this prompt". Codex delegates when you ask in natural language, or when `AGENTS.md` or a skill
  instructs it to. Write the delegation into the skill imperatively: *"Spawn one agent per finding.
  Wait for all of them."*
- **There is no per-agent tool allowlist**, and the parent's live runtime overrides are reapplied to
  children — so a `sandbox_mode: read-only` agent file does **not** guarantee read-only under a
  parent running `--yolo`. Adversarial isolation therefore has to be a **file boundary**, not a
  capability boundary: review passes write a findings file, and only the fixer pass may touch the
  artifact.

### 7.5 Automation

```bash
codex exec "run the weekly audit and write the signal file"        # headless
codex exec --json ...                                              # JSONL events
codex exec --output-schema schema.json ...                         # structured result
```

Progress goes to stderr and only the final message to stdout, so piping is clean. Three defaults to
know: it **requires a git repo** (`--skip-git-repo-check` to opt out), it **defaults to a read-only
sandbox** (`--sandbox workspace-write` to allow edits), and for auth you set `CODEX_API_KEY` **inline
on the single invocation**, never as a job-level environment variable where repo-controlled code runs.

### 7.6 What does not need porting at all

The commit gate, the doctor, the index generator, every linter, the scaffolder, the wrappers, the
stall detector, the digests, and the packaging gates. They are git, shell, and python. Codex runs
them by running them.

**That is the whole point of the layer model.** Porting this kit to Codex is: write one `AGENTS.md`,
move four procedures into `.agents/skills/`, re-register the same hook scripts in `.codex/hooks.json`,
and re-trust them. Everything load-bearing was never Claude Code's to begin with.

---

## 8. Build order

Do not build this all at once. Each step is useful the day you finish it.

| When | Build | You get |
|---|---|---|
| **Day 1** | A repo. An instruction file. A `knowledge/` folder with a front door. Commit and push after every meaningful change. | A durable record instead of chat scrollback |
| **Day 2** | Front-matter on every doc + the index generator. Run it after every knowledge change. | Grep-don't-ingest; context stops being the bottleneck |
| **Week 1** | The doctor + the commit gate (`pre-commit` → doctor on staged files). | Docs cannot rot silently |
| **Week 1** | The scaffolder + the record template + the framing gate. | Work that starts with an agreed question |
| **Week 2** | The wrapper around your one expensive/risky operation + its append-only log. | Cost and provenance for free |
| **Week 2** | The capture procedure (route facts home, rebuild, lint, commit). | Knowledge accrues instead of evaporating |
| **Week 3** | Session-start orientation + per-prompt recall. | Cold sessions that route correctly |
| **Week 4** | The audit aggregator + the propose-only reasoning pass. | A system that reports its own decay |
| **Later** | Role-separated review passes; the digest over your logs; the packaging barrier. | Verified docs, tuned costs, a portable copy |

**If you only ever build three things:** the index generator, the commit gate, and the capture
procedure. Those three are the difference between a knowledge base and a graveyard.

---

## 9. Invariants (breaking these breaks the system)

1. **No timestamps in generated files.** A date in a header makes every regeneration a diff, and the
   freshness check becomes noise.
2. **Total-order every generated section.** Same inputs must produce byte-identical output. Sort
   order-independent lists; leave meaningful order (an ordinal, a ranking) alone — sorting it is a
   correctness bug.
3. **Fixed default tokens.** Emit an explicit `unknown` / `—`, never an empty string that wobbles.
4. **The gate is staged-scoped.** A gate that fails on pre-existing debt gets bypassed within a week,
   and then you have no gate.
5. **The bypass must exist and be loud.** `--no-verify` is a feature. A gate with no escape hatch
   gets uninstalled.
6. **Contradictions append, never overwrite.** When a new claim conflicts with a recorded one, keep
   both, name each one's evidence, state the hypothesis that reconciles them, and name the check that
   settles it. Only equal-or-better evidence may replace a fact: a person's assertion does not delete
   a line verified against source, and newer is not truer. The disagreement is usually the finding.
7. **Two dates, one state.** Never let a machine stamp the human-verified date. The whole staleness
   signal is the gap between them.
8. **Propose-only for anything self-modifying.** No automated delete of knowledge or instructions.
9. **The hot path is a budget.** Every line in an always-loaded file is paid for on every request.

---

## 10. Failure modes

The expensive lessons, stated so you can skip paying for them.

| Failure | What it looks like | The fix |
|---|---|---|
| **Instruction bloat** | The always-loaded file grows to thousands of lines; the model starts ignoring the middle of it | Hard budget. Behavioral rules and pointers stay; procedure moves behind a trigger. Re-audit quarterly |
| **Doc rot** | Docs describe an intention, not the system; nobody trusts them; everyone re-derives | Generate everything generatable. Gate the rest at commit |
| **Index drift** | A doc exists but no index names it, so retrieval silently misses it | Index-freshness in the commit gate, not in a habit |
| **Enforcement in the wrong layer** | The rule lives only in prose, so compliance depends on the model remembering | Walk down the ladder (§5). Pair every prose rule with a detector |
| **Silent background hangs** | A task neither completes nor errors; passive waiting stalls for hours | Pair every background dispatch with a stall detector. Stall is *idle*, not *slow* — never preempt a job that is still writing output |
| **Blanket staging** | `git add -A` in a shared working tree sweeps another session's in-flight edits into your commit | Stage explicit paths. Check the staged list before committing |
| **Over-automation of a human loop** | The agent silently maintains someone's planning tool / task list, and the human stops planning | Some tools are the human's. Read and write them only on explicit request |
| **Deference to authority over evidence** | A confident but hedged correction from a domain owner overwrites a fact verified against source | Treat it as a hypothesis. Keep the evidenced answer, record both, name the discriminating test |
| **Reviewer capture** | The reviewer sees the author's reasoning and rubber-stamps it | Fresh context, artifact and source only, told to assume it's wrong, no write capability |
| **Cost surprises** | An unbounded query or job runs before anyone estimates it | Estimate-first is in the wrapper, not in the instructions |
| **Skipped framing** | Work starts on a vague ask and ends with a deliverable nobody wanted | The framing gate blocks the status change. The skip hatch must state its reason |
| **Verbosity creep** | Every artifact grows a preamble, a recap, and a "let me know if" | Hard caps, checked by a linter, applied before it posts |
| **The unclearable warning list** | A new rule makes hundreds of existing records print a warning on every run; a wall nobody can clear trains everyone to skip the linter | Give every warning list a grandfather date or an expiry. Collapse legacy violations into one counted line |
| **A check that can only report one outcome** | A detector whose underlying command silently returns nothing reads as "healthy" forever | Validate every monitor against a **positive and a negative control**. If it cannot produce the alarm on demand, it is not a monitor |
| **The test that encodes the answer** | A green test built from a hand-written fixture, not from the real producer's output, hides a live defect | Build fixtures from what the real code emits. Assert in the surface the reader actually sees, not the object behind it |
| **Silent precedence rot** | A first-match-wins rule list grows, and a new entry starts stealing matches from an older one | Add an order-integrity test: run every known case through the full ordered list, assert the expected entry wins |
| **"Done" claimed from the wrong evidence** | A rewritten file, a moved timestamp, and a green run all look like success while the content never changed | Gate every completion claim on the artifact itself: assert contents against the source of truth, and confirm it landed in version control |
| **Invented values** | Asked for a label with no roster supplied, a model produces plausible names that join to nothing — and silently "fixes" real typos that were load-bearing | Pull the real roster from source and pin the output to it as an enumerated choice |
| **Governance heavier than the work** | Every new surface mints its own schema, linter, and index; people route around all of it | Write down the anti-goals. Reuse one mechanism across surfaces — usually a few new fields on files that already exist |
| **The silently unindexed doc** | One stray character truncates a front-matter list, the parser skips the file, and it vanishes from every search path with no warning | After adding a doc or editing front-matter, confirm it appears in the regenerated index **and** is tracked by git |

---

## 11. What must never travel

If you carry this pattern between employers, machines, or clients, the machinery travels and the
content does not. Two independent sweeps, both of which must pass before a copy leaves:

**Sweep 1 — secrets.** Absolute home paths, work emails, API tokens, tracker/account/custom-field
IDs, project and section IDs, hostnames, SSH keys and paths, git remotes, cloud project and region
identifiers.

**Sweep 2 — domain context.** This is the one people forget. Even with every secret stripped, a
bundle full of illustrative table names, pipeline names, incident names, ticket prefixes, and a
topic taxonomy tells a reader exactly what the business is. Scrub illustrative names to neutral
generics and replace the taxonomy.

**Never travels at all:** the knowledge base, real memory facts, real work records, real incident
history, session transcripts, and any live credential.

**Mechanize it.** Two ordered find→replace maps (secrets first, then domain), applied by the
packager, followed by two acceptance greps. The packager **refuses to emit** unless both come back
clean. A checklist you run by hand is a checklist you will eventually skip.

**A fresh copy per context** keeps two contexts isolated. You carry your framework, never your data.
