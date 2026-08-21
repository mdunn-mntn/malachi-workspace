# PART A — Component-by-Component Integration Architecture: dsh as Sidecar/Lab Harness

> Design agent output, 2026-08-21. Companion docs: `ti_xxx_design_b_engine.md`, `ti_xxx_design_c_verification.md`, `ti_xxx_master_plan.md` (reconciled plan; the plan supersedes where they differ — notably Part C's `$WS/dsh-kit/` path is unified into the sibling repo `dsh-lab/`).

Stance: **Claude Code remains the daily interactive driver. dsh is a sidecar harness** — headless automation + Python SDK + Web UI inspection + plugin lab — sharing the same host-agnostic substrate (scripts, skills, git knowledge base). Everything MNTN-specific stays in the workspace repo as shell/python; dsh gets only thin TypeScript adapters.

## 0. Placement decision: sibling repo `dsh-lab`, not a dir in the workspace

**Recommendation: `/Users/malachi/Developer/work/mntn/dsh-lab` — a new sibling git repo (pnpm workspace).** Not `$WS/harness/`. Justification:

1. `$WS/.gitignore` ignores `*.json` globally (with per-file negations like `!knowledge/_MEMORY_RECALL.tsv`). A Node monorepo (`package.json`, `tsconfig.json`, `compatibility.json`, lockfiles) would fight the ignore rules of the system-of-record repo file by file.
2. The pre-commit gate (`verify.sh --staged`, ruff on durable Python) and the Pi's weekly `--ff-only` pull are calibrated for a markdown/python kit. Preview-software churn (lockfile bumps on every rc) doesn't belong in a repo the Pi cron pulls and pushes unattended.
3. The ecosystem-first `dsh-bq` plugin must eventually be publishable (npm + `dsh-plugin` GitHub topic) from a repo containing zero MNTN internals. A sibling repo with a `packages/` split makes extraction trivial; the workspace repo can never be made public.
4. Cordis/dsh profiles physically live in `$DSH_HOME/profiles/<name>` (`~/.dsh`) as pnpm dirs anyway — the repo only needs to hold the *source of truth* for patch files plus an install script; nothing dsh-shaped ever touches `$WS`.

Layout:

```
/Users/malachi/Developer/work/mntn/dsh-lab/
  package.json  pnpm-workspace.yaml  pnpm-lock.yaml     # dsh pinned EXACTLY 0.1.1-rc.1
  bin/dsh-mntn                                          # Keychain→env launcher (see §4)
  packages/dsh-bq/          # @mntn/dsh-bq  (tool + guard; MNTN-agnostic core, config-driven)
  packages/dsh-kit/         # @mntn/dsh-kit (recall inject, orient, commands, advisory bridges)
  profiles/mntn-analyst/    # source-of-truth package.json + cordis.patch.yml per profile
  profiles/mntn-automation/
  profiles/mntn-lab/
  launchd/                  # com.mntn.dsh-audit.plist etc. (Mac-only scheduling)
  scripts/install_profiles.sh   # rsync profiles/ → ~/.dsh/profiles/ (one-way, idempotent)
  tests/parity/             # replays hooks_selftest.sh cases against the dsh guards
  VERSIONS.md               # pin log + upgrade checklist
```

The only permitted touchpoint inside `$WS`: **none on disk**. Skills mount via `skill-filesystem` custom-dir config (rank 300) pointing at `$WS/.claude/skills` — pure profile config, zero symlinks, zero new files in the workspace.

---

## 1. Component mapping table

Legend: **R** = reuse-as-is · **W** = thin-wrap (existing script stays the implementation; dsh gets a <300-line adapter) · **RW** = rewrite (native dsh) · **X** = don't-port.

### Hooks (`$WS/.claude/hooks/`, registered in `$WS/.claude/settings.json`)

| # | Hook | dsh counterpart | Verdict |
|---|---|---|---|
| 1 | `enforce_bq_wrapper.sh` (PreToolUse, exit 2) | `tools/pre-execute` waterfall deny in `@mntn/dsh-bq` returning `{ kind: 'deny', reason }` (`PreToolDecision`); same command-position regex + allowlist (`--dry_run`, `INFORMATION_SCHEMA.`, `bq show/ls`), but the deny message points at the `bq_query` **tool**, not the script | **RW** (small; the regex is copied verbatim, only the envelope is new). The load-bearing gate must be native, not bridged — see §7 |
| 2 | `comms_lint_precheck.sh` (advisory) | `hooks-claude-code` bridge onto dsh interception events, pointed at the existing `settings.json`/hook script; fallback: skip | **W** (bridge), low priority |
| 3 | `flag_net_new_tables.sh` (PostToolUse) | Folded into `@mntn/dsh-bq`: after successful execute, pipe the composed `bq_run.sh` command line into the *same script* as synthetic stdin `{"tool_input":{"command":…}}` → `_UNDOCUMENTED.queue` accrues identically | **W** |
| 4 | `memory_recall.py` (UserPromptSubmit inject) | `@mntn/dsh-kit` pre-step plugin: on prompt interception (`agent/*`), spawn `python3 $WS/.claude/hooks/memory_recall.py` via `ctx.subprocess` with `{"prompt":…}` stdin, inject stdout via `agent.inject()`. The python + `_MEMORY_RECALL.tsv` remain the single implementation for both hosts | **W** (host-agnostic by design) |
| 5 | `log_request.py` | Same spawn pattern, **analyst profile only** (headless machine prompts would pollute `request_digest.py` shapes) | **W** (analyst) / **X** (automation) |
| 6 | `brevity_pointer.py` | — RULE 0 is Claude Code chat ergonomics | **X** |
| 7 | `session_start_routing.sh` | `@mntn/dsh-kit`: run at agent creation, inject stdout (orientation + clean-tree pull); also exposed as `ctx.commands` `/orient` | **W** |
| 8 | `chat_brevity_meter.py` | — pairs with #6 | **X** |
| 9 | `capture_reminder.sh` | Optional turn-end listener in analyst profile; in automation the scheduled jobs *are* the answer | **W** (low priority) |
| 10 | `comms_cap_reminder.sh` | — interactive nudge | **X** |
| 11 | `oncall_triage_reminder.sh` | Superseded in dsh by a scheduled headless check (phase 2); Claude Code keeps the hook | **X** (dsh side) |

### Skills (`$WS/.claude/skills/*/SKILL.md`)

| Skill | dsh counterpart | Verdict |
|---|---|---|
| `/frame` `/capture` `/oncall` `/present` `/transcribe` `/workflow-audit` | Same files, discovered via `skill-filesystem` provider custom dir → `$WS/.claude/skills` (AgentSkills `SKILL.md` standard is shared; kebab-case names already conform) | **R** — verbatim, zero copies. Caveats: `/frame` is interactive-by-design → analyst (Web UI, `ctx.userQuestions`) only, **never** headless. `/capture` headless only under the branch guardrail in §5 step 10 |

### Agents (`$WS/.claude/agents/*.md`, 7)

| Agent | dsh counterpart | Verdict |
|---|---|---|
| `implementer`, `reviewer-adversarial`(×2), `fixer`, `synthesizer` | `@dsh-external/workflow` named workflow using its **adversarial-verification** standard pattern; reviewer read-only enforced by `readOnlyAllowedTools` (capability, matching the current no-Write/Edit discipline) | **RW** (phase 2, flagship dsh win — the QuickJS-sandboxed workflow layer is strictly better isolation than today's disciplinary boundary) |
| `curator` | The `/capture` skill executor — see /capture row | **R** (via skill) |
| `perf-analyst`, `cataloger` | Scheduled headless jobs (Mac launchd → `dsh --profile mntn-automation`), propose-only outputs | **W** (phase 2) |

### Scripts (`$WS/.claude/scripts/`, 31)

All **R** — they are already host-agnostic bash/python and the dsh bash tool calls them exactly as Claude Code does. Specific notes: `bq_run.sh` stays THE implementation (dsh-bq shells to it — never reimplements the dry-run gate, reservation, or perf log); `verify.sh`/`build_index.sh`/linters/`new_ticket.sh`/`airflow_pull.sh`/`transcribe.sh` unchanged; `workflow_audit.sh` unchanged (consumed by automation profile); the three Pi/unattended scripts are **X** for dsh entirely (§6).

### Other components

| Component | dsh counterpart | Verdict |
|---|---|---|
| Knowledge base (213 memories, indexes, `_ROUTING.md`, `MEMORY.md`) | Read/written in place by dsh sessions running in cwd `$WS`; recall via hook-#4 wrap | **R** — git markdown remains system of record |
| `.githooks/` pre-commit + commit-msg | Fire naturally on any dsh-session commit (repo-level) | **R**, no action |
| Slash-command-shaped deterministic ops (`verify.sh full`, orientation, `perf_digest.py`, `workflow_audit.sh`) | `ctx.commands` `/verify` `/orient` `/bq-perf` `/audit-signals` — human dispatch **without a model turn** (new capability Claude Code lacks) | new, cheap (in `@mntn/dsh-kit`) |
| Weekly self-audit reasoning half | `dsh --profile mntn-automation` headless run on the Mac, launchd-scheduled after the Pi's Monday signals commit | **W** (§5 step 10) |
| Memory reverse symlink (`~/.claude/projects/.../memory`) | — Claude Code native-memory specific | **X** |
| OpenViking / MemOS / EverOS | — | **X** (§6) |

---

## 2. `@mntn/dsh-bq` plugin design (first-class deliverable)

Package `dsh-lab/packages/dsh-bq`, npm name `@mntn/dsh-bq` (publishable later as `dsh-bq` with the `dsh-plugin` topic — nothing MNTN-specific is hardcoded; it is generic "governed BigQuery via a wrapper-script contract"). Manifest declares `dsh: { bundle: { patch: "./cordis.patch.yml" } }` so `dsh plugin --profile <p> add` auto-joins the layer stack. Ships `compatibility.json` recording the tested harness snapshot (the `dsh_workflow` precedent).

Two sub-plugins in one bundle (separate `cordis.patch.yml` rows with stable ids `bq-tool` and `bq-guard`, so each is independently `disabled:`-togglable):

**(a) Tool plugin** — `src/tool.ts`:

```ts
import type { Context } from '@deepseek-ai/cordis'
import { defineTool } from '@deepseek-ai/dsh-tools'
export const name = 'dsh-bq-tool'
export const inject = ['tools', 'subprocess']

export function apply(ctx: Context, config: Config) {
  ctx.tools.register(defineTool({
    name: 'bq_query',
    description: 'Run BigQuery SQL through the governed wrapper: dry-run cost gate (sample-first), perf/provenance logging, us-central1 reservation, read-only. ALWAYS use this instead of raw bq.',
    parameters: {
      sql:     { type: 'string', required: true, description: 'Standard SQL. Include partition filters; run a sample before a full pass.' },
      ticket:  { type: 'string', description: 'Ticket id for provenance, e.g. TI-912' },
      label:   { type: 'string', description: 'One-line purpose of the query' },
      phase:   { type: 'string', description: "'sample' or 'full' (est→actual accuracy loop)" },
      dry_run: { type: 'boolean', description: 'Estimate cost only; bills nothing' },
    },
    output: { schema: { type: 'string' }, render: (_a, v) => [{ type: 'text', text: v }] },
    async execute(args, exec) { /* spawn wrapper via ctx.subprocess, honor exec.signal */ },
  }))
}
```

**Config schema** (Schemastery, validated at load):

```ts
export const Config = Schema.object({
  wrapperPath:    Schema.string().required(),        // $WS/.claude/scripts/bq_run.sh
  workspaceRoot:  Schema.string().required(),        // cwd for the spawn; perf log lands in $WS/knowledge/bq_perf_log.jsonl via the script
  postExecHook:   Schema.string(),                   // $WS/.claude/hooks/flag_net_new_tables.sh (optional doc-debt accrual)
  maxOutputChars: Schema.number().default(16000),    // truncate rows returned to the model
  timeoutMs:      Schema.number().default(600000),
  allowForce:     Schema.boolean().default(false),   // when false, '--force' is NEVER passed; gate refusals are final
})
```

**Execution semantics.** `execute()` composes `bash <wrapperPath> [--ticket T] [--label L] [--phase P] [--dry_run] '<sql>'` and spawns through `ctx.subprocess` (so it inherits sandbox/subprocess policy) with `cwd: workspaceRoot`. It never invents flags: no `--force` path exists when `allowForce=false`, so the `BQ_GB_ABORT` refusal is un-overridable by the model — the cost-gate invariant is preserved by construction. On success it (1) truncates stdout to `maxOutputChars`, (2) reads the just-appended tail line of the perf log for `job_id / est_gb / billed / referenced_tables`, (3) if `postExecHook` set, pipes `{"tool_input":{"command":"<composed cmdline>"}}` into `flag_net_new_tables.sh` (hook #3 parity). **Returned to the model:** the row output plus a compact provenance footer — `job_id`, `est_gb → billed_gb`, cost, `phase`, and any wrapper warnings (sample-first nudge; "full with no prior sample"). On wrapper refusal (non-zero exit from the dry-run gate), the tool returns the refusal text verbatim as an error result — the model's only recourse is to sample/narrow, same as today.

**(b) Guard plugin** — `src/guard.ts` (cookbook permission-gate pattern):

```ts
export const name = 'dsh-bq-guard'
export const inject = ['tools']
export function apply(ctx: Context) {
  ctx.on('tools/pre-execute', (call, next) => {
    const cmd = call.name === 'bash' ? String(call.args?.command ?? '') : ''
    if (cmd && RAW_BQ_QUERY_RE.test(cmd) && !ALLOWED_RE.some(r => r.test(cmd)))
      return { kind: 'deny', reason: "BLOCKED: use the bq_query tool (dry-run cost gate + provenance logging), not raw 'bq query'." }
    return next()
  })
}
```

`RAW_BQ_QUERY_RE` / `ALLOWED_RE` are line-for-line ports of `enforce_bq_wrapper.sh` (command-position match after `^|[|&;]`; allow `--dry_run`, `INFORMATION_SCHEMA.`, `bq show/ls`), with vitest cases transliterated from `hooks_selftest.sh` so both hosts are tested against the same corpus. Belt-and-braces option: also `ctx.tools.guard()` monotonic denial once a session is flagged.

---

## 3. Profile design

Three profiles, each a dir in `dsh-lab/profiles/` installed to `~/.dsh/profiles/<name>` by `scripts/install_profiles.sh`. `$DSH_HOME/cordis.patch.yml` (machine-global layer) is kept deliberately **empty** — all composition is named and version-controlled.

**`mntn-analyst`** — interactive Web UI for inspection/experiments (`dsh-mntn --profile mntn-analyst` → :3080).
`package.json`: `dsh.profile.bundles: ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app", "@mntn/dsh-bq", "@mntn/dsh-kit"]`.
`cordis.patch.yml` (indicative — exact row ids/keys confirmed against `dsh --profile mntn-analyst --dump-default-config` at build time; a patch replaces the row's WHOLE config, so each row carries its complete config):

```yaml
- id: llm-anthropic
  name: '@deepseek-ai/dsh-llm-pi-ai'
  config:
    provider: anthropic
    model: claude-opus-4-6            # confirm current id at build time
    apiKeyEnv: ANTHROPIC_API_KEY      # env reference — never a literal
- id: skills-fs
  name: '@deepseek-ai/dsh-skill-filesystem'
  config:
    customDirs: [/Users/malachi/Developer/work/mntn/workspace/.claude/skills]
- id: bq-tool
  config: { wrapperPath: /Users/malachi/Developer/work/mntn/workspace/.claude/scripts/bq_run.sh,
            workspaceRoot: /Users/malachi/Developer/work/mntn/workspace,
            postExecHook: /Users/malachi/Developer/work/mntn/workspace/.claude/hooks/flag_net_new_tables.sh }
- id: kit
  config: { workspaceRoot: /Users/malachi/Developer/work/mntn/workspace,
            recall: true, requestLog: true, orient: true }
```
Permission preset: `workspace-write` + approval `ask` (default). Workspace cwd: `$WS`.

**`mntn-automation`** — headless one-shot runner for scheduled/scripted jobs (`dsh-mntn --profile mntn-automation "task"`; also the target of the Python SDK).
Bundles: `["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-headless", "@mntn/dsh-bq", "@mntn/dsh-kit"]`.
Patch deltas vs analyst: approval policy `never` (deterministic fail-closed reject — documented as "useful for headless/CI"), sandbox `workspace-write` confined to `$WS`, `kit.requestLog: false` (don't pollute `request_digest` shapes), session persistence jsonl under `~/.dsh` (session logs are dsh-internal; durable facts land in git via the skills' own write-backs).

**`mntn-lab`** — creator mode for plugin development only.
Bundles: analyst set + `@deepseek-ai/cordis-plugin-hmr` + `dsh-tool-cordis` (live `cordis_define/run/inspect_*`), optionally `@deepseek-ai/dsh-subagent-claude-code` (A/B: same task via Claude Code subagent vs native) and `github:omdsh-dev/dsh_workflow#<pinned-sha>`. Scratch plugins mounted with `pnpm dsh web --patch ./scratch/cordis.yml`. This is the only profile where third-party or experimental plugins are ever installed (in-process security containment by profile boundary).

---

## 4. Model routing and keys

- **Default everywhere: Anthropic via `llm-pi-ai`** (generic multi-provider adapter over `@earendil-works/pi-ai` — Anthropic as pure config), so the sidecar reasons with the same model family as the daily driver and outputs are comparable.
- **DeepSeek optional, `mntn-lab` only**: an additional `llm-deepseek` row (`deepseek-v4-flash`/`-pro`) behind `DEEPSEEK_API_KEY`, `disabled: true` by default; flip the row to compare.
- **Key handling:** no plaintext anywhere. `dsh-lab/bin/dsh-mntn` launcher: `export ANTHROPIC_API_KEY="$(security find-generic-password -s anthropic -w)"` (login Keychain) then `exec dsh "$@"`. dsh env layering is inherited > project `.env` > user `.env`, so the launcher export wins; config rows reference `apiKeyEnv` (or `!!js process.env.ANTHROPIC_API_KEY`) only. `~/.dsh/.credentials.yaml` stays empty of LLM keys; `.env` files never contain keys. **dsh is never installed on the Pi** — every model-touching runtime (headless jobs included) is Mac + launchd. CI check in dsh-lab: `git grep -E 'sk-|api[_-]?key.*:'` gate in its pre-commit.

---

## 5. Ordered build steps (each ≤ half-day, independently testable, one agent each)

| # | Step | Inputs | Outputs | Test criterion |
|---|---|---|---|---|
| 1 | **Bootstrap `dsh-lab`**: init repo, pnpm workspace, pin `@deepseek-ai/dsh@0.1.1-rc.1` exact, `VERSIONS.md` | — | repo skeleton, lockfile | `npx dsh web` boots UI on :3080; `--dump-default-config` prints |
| 2 | **Profiles skeleton + installer**: `profiles/{mntn-analyst,mntn-automation}` (base bundles only), `scripts/install_profiles.sh` | step 1 | `~/.dsh/profiles/*` populated | `dsh --profile mntn-analyst --dump-config` shows composed rows; home-level patch confirmed empty |
| 3 | **Keychain launcher + Anthropic routing**: `bin/dsh-mntn`, `llm-anthropic` row via llm-pi-ai | step 2 | working model round-trip | `bin/dsh-mntn --profile mntn-automation "Reply exactly OK"` exits 0, prints OK; `grep -r sk- ~/.dsh dsh-lab` finds nothing |
| 4 | **Mount skills**: `skill-filesystem` customDirs → `$WS/.claude/skills` | step 2 | analyst patch row | Web UI catalog lists all 6; `skill` tool loads `/present` body; `/frame` asks questions interactively in Web UI |
| 5 | **`@mntn/dsh-bq` v1 (tool)**: `defineTool` + subprocess spawn of `bq_run.sh` + provenance footer | steps 2–3 | `packages/dsh-bq` | headless task "use bq_query: SELECT 1" → new line in `$WS/knowledge/bq_perf_log.jsonl`; oversized query returns the gate refusal, no `--force` path exists |
| 6 | **dsh-bq v2 (guard)**: `tools/pre-execute` deny + regex port + vitest cases from `hooks_selftest.sh` | step 5 | guard sub-plugin | headless task "run `bq query 'SELECT 1'` in bash" → denied with bq_query pointer; `--dry_run` / `INFORMATION_SCHEMA` / `bq show` all pass |
| 7 | **`@mntn/dsh-kit` recall inject**: prompt-interception spawn of `memory_recall.py`, `agent.inject()` | step 3 | `packages/dsh-kit` | prompt containing a known TSV keyword → session `session/event` jsonl contains the injected memory-pointer block; unknown prompt → no injection |
| 8 | **dsh-kit orient + commands**: agent-creation spawn of `session_start_routing.sh`; `ctx.commands` `/verify` `/orient` `/bq-perf` `/audit-signals` | step 7 | kit v2 | `/verify` runs `verify.sh full` with **no model turn** (`command/run`/`command/done` events only); orientation block present at session start |
| 9 | **`hooks-claude-code` bridge spike (timeboxed)**: point at `$WS/.claude/settings.json`; enable only advisory hooks (#2, #9) if event mapping is clean; document which of the 11 map | steps 2, 5 | bridge config or a written "not worth it" note in VERSIONS.md | synthetic Jira-curl bash call surfaces lint advisory; bq guard from step 6 (not the bridge) still owns the hard block |
| 10 | **First automation job — audit reasoning half**: `launchd/com.mntn.dsh-audit.plist` (Mon 09:00, after Pi 08:00 signals commit) → headless task: run `/workflow-audit` over `signals_latest.md`, write report, commit to branch `audit/auto-<date>`, never push main | steps 3–8 | plist + job doc | manual dry run yields `claude-prompts/workflow_audits/audit_<date>.md` on the branch; `git diff main` shows only that file; propose-only wording intact |
| 11 | **dsh_workflow ingestion pass**: install `github:omdsh-dev/dsh_workflow#<sha>` into `mntn-lab`; port implementer→2×adversarial-reviewer→fixer→synthesizer as a named workflow (adversarial-verification pattern, reviewers via `readOnlyAllowedTools`) | steps 1–8 | `.dsh/workflows/` def in dsh-lab | run against one sample doc set; run capsule under `workflow-runs/<id>/` shows reviewers made zero writes |
| 12 | **Parity + upgrade harness**: `tests/parity/` replaying all `hooks_selftest.sh` guard cases against dsh guards; `VERSIONS.md` pin-bump checklist (`--dump-config` diff → parity suite → smoke steps 3/5/6) | steps 5–8 | CI-runnable test suite | `pnpm test` green; deliberately breaking the guard regex fails the suite |

Steps 1–4 are pure composition (no code); 5–8 are the only real TypeScript (~4 small files); 9–12 are integration. Nothing before step 10 touches automation of judgement-layer writes.

## 6. What NOT to port (ruthless)

- **The Pi cron trio** (`pi_run_workflow_audit.sh`, `oncall_daily_rca.sh`, `oncall_daily_optimizer.sh`): keyless, deterministic, battle-tested, and MNTN policy forbids model runtimes on the Pi anyway. dsh adds only risk. Untouched.
- **`.githooks/` pre-commit + commit-msg, `verify.sh`, all linters, `build_index.sh`, all 11 generated indexes**: repo-level and host-agnostic — they already govern dsh-session commits for free.
- **The brevity pair** (`chat_brevity_meter.py` + `brevity_pointer.py`) and `comms_cap_reminder.sh`: RULE-0 chat ergonomics tuned to Claude Code's surface.
- **The memory reverse symlink**: Claude Code native-memory-tool plumbing; dsh writes memory through `/capture` like everyone else.
- **OpenViking / MemOS / EverOS or any memory plugin**: all maintain parallel processed stores (L0/L1/L2 tiers, auto session capture) — a direct violation of git-as-system-of-record. The deterministic `_MEMORY_RECALL.tsv` recall already beat native recall and is host-agnostic; it is the memory system.
- **`dsh-data-agent` and all `dsh-sql` plugins**: zero BigQuery support, and their `sql-query`/`sql-write` tools would bypass every `bq_run.sh` invariant. `@mntn/dsh-bq` exists precisely because this hole is real (24 data repos surveyed, none fit).
- **`@deepseek-ai/dsh-subagent-claude-code` in daily profiles**: 257MB dependency that returns final-text-only — pointless when Claude Code itself is the daily driver. Lab-profile A/B tool only.
- **Marketplace plugins (`dsh-market` etc.)**: in-process with your permissions, no security review; nothing from the marketplace channel gets installed in any profile.
- **`/frame` automation**: interactive-by-design forever; automating it is a named anti-goal analogue.
- **`/capture` as unattended headless (for now)**: it's the one loop with real write authority over knowledge, guarded today by human presence. If ever automated: branch + human merge, same guardrail as step 10. Not in this plan's committed scope.

## 7. Preview-churn exposure map and isolation

| Churn point | Exposure | Isolation |
|---|---|---|
| dsh 0.1.x breaking changes (promised) | every plugin/profile | **Exact pin** `0.1.1-rc.1` in dsh-lab lockfile + profile `package.json`; upgrades only via the step-12 checklist; `compatibility.json` in each `@mntn` package records last-tested snapshot |
| Event/config key renames (`tools/pre-execute`, `apiKeyEnv`, skill `customDirs`) | guard, llm rows, skills mount | Adapters are ≤300 lines each; parity test suite fails loudly; `--dump-config` diff is a mandatory pre-upgrade step; all MNTN logic lives in workspace shell/python that never churns |
| Patch semantics (whole-row config replace, no deep-merge) | silent config loss on upstream default changes | Every patched row carries its **complete** config in our files; never rely on upstream row defaults |
| `hooks-claude-code` bridge (tracks two moving contracts) | hooks #2/#9 only | Bridge restricted to advisory hooks; the hard gate (bq) is native (step 6), so bridge breakage degrades to "missing nudge", never "missing block" |
| `dsh_workflow` pinned to its own harness snapshot (`compatibility.json` churn) | step 11 only | Pinned github SHA; lab-profile-only; the Claude Code agent-pass runbook remains the production path until parity is proven |
| In-process plugin execution (no sandbox for plugin code) | any third-party install | Only `@deepseek-ai/*`, `@mntn/*`, and the pinned dsh_workflow SHA are ever installed; third-party experiments confined to `mntn-lab` |
| Profile/home-patch drift (hot-reloaded `$DSH_HOME/cordis.patch.yml`) | invisible global state | Home patch kept empty by policy; profiles are rsync'd from version-controlled `dsh-lab/profiles/` and `install_profiles.sh` is the only writer |
| Claude Agent SDK / model-id drift in llm rows | analyst/automation | Model id is one config key per profile; verified in step 3's smoke test |

The structural bet: because every gate, log, and index remains a workspace script and every dsh artifact is a thin adapter in a separately-pinned sibling repo, a total dsh breakage costs exactly the adapters — the kit, knowledge base, Pi loops, and Claude Code daily driver are unaffected by construction.

## Critical files
- `$WS/.claude/scripts/bq_run.sh` — the wrapper `@mntn/dsh-bq` shells to; its flag contract and refusal exit codes define the tool's execute/error semantics
- `$WS/.claude/hooks/enforce_bq_wrapper.sh` — regex + allowlist to port verbatim into the `tools/pre-execute` guard
- `$WS/.claude/hooks/memory_recall.py` — spawned unchanged by the dsh-kit recall plugin (stdin `{"prompt"}` → stdout inject block)
- `$WS/.claude/settings.json` — the hooks registry the `hooks-claude-code` bridge spike consumes, and the parity reference for what each dsh adapter must replicate
- `$WS/.claude/scripts/hooks_selftest.sh` — the synthetic test corpus transliterated into the dsh parity suite (step 12)
