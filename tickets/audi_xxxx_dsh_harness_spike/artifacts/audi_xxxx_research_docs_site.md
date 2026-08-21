# DeepSeek Harness Docs — Research Report

Base: `https://deepseek-harness.github.io/deepseek-harness` (all paths below relative to it). Source repo: `https://github.com/deepseek-ai/deepseek-harness`.

## 1. Full docs sitemap

**Guide** ([/en/guide/quickstart](https://deepseek-harness.github.io/deepseek-harness/en/guide/quickstart))
- `/en/guide/quickstart` — Use the Web UI
- `/en/guide/providers` — Configure models
- `/en/guide/python-sdk` — Python SDK

**Develop** ([/en/develop/basic/](https://deepseek-harness.github.io/deepseek-harness/en/develop/basic/))
- `/en/develop/basic/` (Your first plugin), `/en/develop/basic/tool`, `/en/develop/basic/config`, `/en/develop/basic/publish`
- `/en/develop/framework/` (Plugins and lifecycle), `/en/develop/framework/service`, `/en/develop/framework/events`
- `/en/develop/practice/` (Capability layering), `/en/develop/practice/llm-adapter`
- `/en/develop/cordis-tutorial/` + chapters `01-first-plugin`, `02-lifecycle-and-effects`, `03-services`, `04-events`, `05-config`, `06-composition-and-hmr`, `07-into-the-harness`

**Reference** ([/en/reference/](https://deepseek-harness.github.io/deepseek-harness/en/reference/) = Architecture)
- Concepts: `/en/reference/cordis-primer`, `/en/reference/capability-seams`, `/en/reference/agent-lifecycle`, `/en/reference/tool-execution-pipeline`
- Generated: `/en/reference/config-catalog`, `/en/reference/tool-catalog`, `/en/reference/persistence-catalog`
- Cordis Core API: `/en/reference/cordis-api/{context,events,fiber,registry,service,inherited}`
- Cookbook: `/en/reference/cookbook/{adding-a-package,adding-a-tool,adding-an-llm-adapter,adding-a-settings-card,extension-cookbook,adding-a-conversation-node}`
- Subsystems: `/en/reference/subsystems/` index plus `core, scope, invariants, session, session-query, session-reference, session-title, session-projection, persistence, spill, session-telemetry, llm-streaming, token-meter, system-prompt, compaction, tools, shell, subprocess, terminal, jobs, filesystem, lsp, code-runtime, web, skills, workflow, subagent, approval, permission-presets, sandbox, plan, user-questions, commands, goal, schedule, web-server, typert, client-modules, storage, workspace, settings, credentials`

Off-site references linked from docs: CLI reference `github.com/deepseek-ai/deepseek-harness/blob/master/apps/cli/reference/README.md`; `packages/llm/llm-pi-ai/README.md`; `packages/llm/llm-deepseek/README.md`; ACP package `packages/acp/acp`. Chinese mirror exists under `/deepseek-harness/develop/...` (no `/en/`).

## 2. Plugin model

Source pages: [develop/basic/](https://deepseek-harness.github.io/deepseek-harness/en/develop/basic/), [develop/framework/](https://deepseek-harness.github.io/deepseek-harness/en/develop/framework/), [develop/basic/config](https://deepseek-harness.github.io/deepseek-harness/en/develop/basic/config), [develop/basic/publish](https://deepseek-harness.github.io/deepseek-harness/en/develop/basic/publish), [cordis-tutorial/01](https://deepseek-harness.github.io/deepseek-harness/en/develop/cordis-tutorial/01-first-plugin), [cordis-tutorial/06](https://deepseek-harness.github.io/deepseek-harness/en/develop/cordis-tutorial/06-composition-and-hmr).

**Authoring.** A plugin is a TypeScript module exporting an `apply(ctx)` function; `name` is optional diagnostic metadata:

```typescript
import type { Context } from '@deepseek-ai/cordis'

export const name = 'my-plugin'

export function apply(ctx: Context) {
  // Register capabilities here.
}
```

Three forms — function (recommended default), object, class (`Service` subclass, used when providing a service):

```typescript
// Object form
export default {
  name: 'my-plugin',
  inject: ['tools'],
  apply(ctx: Context) { }
}
// Class form
import { Service, type Context } from '@deepseek-ai/cordis'
export default class MyService extends Service {
  static inject = ['tools']
  constructor(ctx: Context) {
    super(ctx, 'myService')
  }
}
```

Dependencies via `inject` (`export const inject = ['tools']`) — the plugin waits until every required service exists before `apply` runs; if a required service disappears (provider swap) the plugin auto-unloads and reloads when it returns ([framework/](https://deepseek-harness.github.io/deepseek-harness/en/develop/framework/)). Optional deps: `ctx.get('metrics')` ([framework/service](https://deepseek-harness.github.io/deepseek-harness/en/develop/framework/service)). Cleanup: listeners/tools/timers auto-dispose on unload; explicit resources use `ctx.effect()` returning a disposer:

```typescript
ctx.effect(() => {
  const timer = setInterval(() => { console.log('heartbeat') }, 5000)
  return () => clearInterval(timer)
})
```

Lifecycle states (fiber state machine): PENDING → LOADING → ACTIVE → UNLOADING → DISPOSED, with FAILED branch; disposers run in reverse registration order, async disposers concurrently ([framework/](https://deepseek-harness.github.io/deepseek-harness/en/develop/framework/)). Child plugins via `ctx.plugin()`; manual `dispose()` removes all owned registrations recursively.

**Registration/composition.** `cordis.yml` is a YAML array of plugin rows; patch layers use `insert` with stable `id`:

```yaml
- insert:
    - id: hello
      name: '/absolute/path/to/deepseek-harness/scratch-plugin/src/my-plugin.ts'
```

Run against the Web UI: `pnpm dsh web --patch ./scratch-plugin/cordis.yml` (UI at `http://127.0.0.1:3080`). "Entries start concurrently, so list position guarantees nothing about which plugin loads first; ordering comes from service dependencies" ([cordis-tutorial/01](https://deepseek-harness.github.io/deepseek-harness/en/develop/cordis-tutorial/01-first-plugin)). Row metadata: `id` (stable identity, distinguishes edit from remove+add), `disabled: true` (unmounts but keeps the row; toggling remounts plugin and dependents), groups (nested entries load/unload as a unit), `isolate` (independent service instances per group, e.g. `isolate: { shell: true }`) ([cordis-tutorial/06](https://deepseek-harness.github.io/deepseek-harness/en/develop/cordis-tutorial/06-composition-and-hmr), [framework/service](https://deepseek-harness.github.io/deepseek-harness/en/develop/framework/service)).

**Configuration.** Export a `Config` interface plus a same-named Schemastery schema; validated at load, invalid config fails early ([develop/basic/config](https://deepseek-harness.github.io/deepseek-harness/en/develop/basic/config)):

```typescript
import Schema from '@deepseek-ai/schemastery'

export interface Config {
  greeting: string
  maxRetries: number
  verbose?: boolean
}

export const Config: Schema<Config> = Schema.object({
  greeting: Schema.string().default('Hello'),
  maxRetries: Schema.number().default(3),
  verbose: Schema.boolean().default(false),
})

export function apply(ctx: Context, config: Config) {
  console.log(config.greeting)
}
```

```yaml
- insert:
    - id: hello
      name: './src/my-plugin.ts'
      config:
        greeting: 'Hi there'
        maxRetries: 5
```

**Packaging/distribution.** Two concepts, both defined by `package.json` manifests ([develop/basic/publish](https://deepseek-harness.github.io/deepseek-harness/en/develop/basic/publish)): a **bundle** (npm package with `dsh.bundle.patch` pointing at a `cordis.patch.yml` layer — "what does this package contribute?") and a **profile** (`$DSH_HOME/profiles/<name>` with `dsh.profile.bundles` ordered list — "which bundles compose this setup"). "A bundle is what you author and distribute; a profile is what a user boots with `dsh --profile <name>`. Nothing is both." Bundle manifest verbatim:

```json
{
  "name": "dsh-hello-plugin",
  "version": "0.1.0",
  "type": "module",
  "main": "index.js",
  "files": ["index.js", "cordis.patch.yml"],
  "dsh": { "bundle": { "patch": "./cordis.patch.yml" } }
}
```

Install: `dsh plugin --profile demo add ./hello-plugin` (first use seeds the profile with `@deepseek-ai/dsh-base`); remove: `dsh plugin --profile demo remove dsh-hello-plugin`; git installs `dsh plugin --profile demo add github:you/hello-plugin` need a `prepare` script and a pnpm `allowBuilds` allowlist entry (explicit "permission to execute the package's code on your machine at install time"). Layer precedence (later wins per row; a patch replaces the row's whole `config`, no deep-merge): (1) bundle patches in `dsh.profile.bundles` order → (2) profile `cordis.patch.yml` → (3) `$DSH_HOME/cordis.patch.yml` → (4) `--patch <path>` overlays in argv order.

**Hot-swap.** `@deepseek-ai/cordis-plugin-hmr` watches files and on save unloads the old instance (releasing effects) and loads the new one; config edits trigger the same reload cycle; explicit `id`s prevent spurious remounts ([cordis-tutorial/06](https://deepseek-harness.github.io/deepseek-harness/en/develop/cordis-tutorial/06-composition-and-hmr)). Home `cordis.patch.yml` edits reload automatically; profile changes require restart ([CLI reference](https://github.com/deepseek-ai/deepseek-harness/blob/master/apps/cli/reference/README.md)). Live/dynamic plugins also exist via the `cordis_*` tools (`cordis_define`, `cordis_run`, `cordis_stop`, `cordis_undefine`, `cordis_inspect_*`) in `dsh-tool-cordis` ([tool-catalog](https://deepseek-harness.github.io/deepseek-harness/en/reference/tool-catalog)).

## 3. Core concepts and exact terminology

From [reference/](https://deepseek-harness.github.io/deepseek-harness/en/reference/), [cordis-primer](https://deepseek-harness.github.io/deepseek-harness/en/reference/cordis-primer), [agent-lifecycle](https://deepseek-harness.github.io/deepseek-harness/en/reference/agent-lifecycle), [subsystems/session](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/session):

- **Cordis** — "the framework under dsh: plugins contribute services, typed events, and reversible effects to a shared context." Every component is a plugin (model adapters, tool registries, session logs, the agent loop); "there is no privileged core to patch."
- **Context (`ctx`)** — service repository; services claim stable keys (`ctx.tools`, `ctx.llm`, `ctx.sessions`). **Service** — named capability mounted on `ctx`. **Fiber** — execution/lifecycle unit of a loaded plugin. **Registry** — service registration/lookup. **Effect** — reversible registration cleaned up on disposal. **Inject** — declared service dependency.
- **Event dispatch modes** (primer table): `emit` (not awaited, sequential, no return), `waterfall` (sequential, returns value, listeners wrap `next()`), `parallel` (awaited, concurrent), `serial` (awaited, sequential, first non-null result wins). Naming convention `namespace/action` (`agent/step`, `tools/result`, `session/event`) ([framework/events](https://deepseek-harness.github.io/deepseek-harness/en/develop/framework/events)).
- **Profile / bundle** — a running `dsh` instance is "a plugin tree composed at boot from ordered layers"; profile = named composition in the Harness home; bundle = distribution format for Cordis config rows + code. Core bundles: `dsh-base` (adapters, tools, persistence, sandbox+approval policy, settings, credentials, telemetry), `dsh-web-app` (browser app), `dsh-headless` (one-shot runner, no server).
- **Session** — append-only `SessionEvent` log; "the session log is the source of the context the model sees" (runtime invariant: model-visible content must be logged and reconstructable). Event vocabulary `SessionEventMap`: `turn/start`, `turn/end`, `user/message`, `assistant/message`, `tool/result` (surface events), `assistant/chunk` (token replay), log-only events (`compaction/*`, `approval/*`, `plan/mode`, `permission/preset`). Surface ops: `surfaceOp: 'append'` or `{ op: 'replace', start, end }` with `sourceEventSeqs` lineage. `deriveMessages()` projects the log to LLM history. Fork: `ctx.sessions.fork(source, boundary?, childSessionId?)`; `session/end-seed` marks inherited-replay end.
- **Turn / step** — "a step is one model request plus its tool calls; a turn is zero or more steps": turn/start → step/start → model streaming → tool execution → step/end, closing "once nothing is owed." Two parallel streams: durable facts on `session/event`, live control on `agent/*` ("the live coordination API for queue/status, prompt interception, request construction, steering, continuation, and errors") ([agent-lifecycle](https://deepseek-harness.github.io/deepseek-harness/en/reference/agent-lifecycle)).
- **Capability seam** — swappable capability with three roles: **Service Definition**, **Service Provider**, **Consumer**; "one provider swap changes the entire product behavior without forks" ([capability-seams](https://deepseek-harness.github.io/deepseek-harness/en/reference/capability-seams), [develop/practice/](https://deepseek-harness.github.io/deepseek-harness/en/develop/practice/) — e.g. Bash = `dsh-shell` (definition) + `dsh-bash-local` (provider) + `dsh-tool-bash` (consumer)).
- **Tool execution pipeline** ([tool-execution-pipeline](https://deepseek-harness.github.io/deepseek-harness/en/reference/tool-execution-pipeline)): `tools/pre-execute` waterfall → monotonic guards (`ctx.tools.guard()`) → execution (`tools/execute` lifetime wrap, timeouts) → `tools/post-execute` transform → `tools/result` observation; `ctx.approval` resolves asks before monotonic guards; `run_code` transport serializes sub-calls through the same pipeline carrying the parent token.

## 4. CLI

From [CLI reference README](https://github.com/deepseek-ai/deepseek-harness/blob/master/apps/cli/reference/README.md) (linked as authoritative from [develop/basic/publish](https://deepseek-harness.github.io/deepseek-harness/en/develop/basic/publish)):

- `dsh --profile <name>` — boot a named profile from `$DSH_HOME/profiles/<name>`. "Later layers win per row; a patch replaces the targeted row's complete `config` value rather than deep-merging keys."
- `dsh web [--host --port --trusted-host(repeatable) --no-open]` — hardcoded alias for `--profile web`. Dev-tree variant: `pnpm dsh web --patch ./scratch-plugin/cordis.yml`.
- `dsh --profile <name> --dump-config` / `--dump-default-config` — print composed config without booting (latter = bundle layers only).
- `dsh --profile <name> --patch <path>` — repeatable overlay, argv order.
- `dsh plugin --profile <name> <pnpm-args>` — forwards to pnpm in the profile dir (`add`, `remove`, `update`, …). E.g. optional subagent providers: `dsh plugin --profile <name> add @deepseek-ai/dsh-subagent-codex`, `... add @deepseek-ai/dsh-subagent-claude-code`.
- **Headless jobs**: `dsh --profile headless "task description"` — creates a one-shot Agent, submits the task, prints to stdout, exits 0 on completion / 1 on failure (bundle `dsh-headless`, "one-shot runner with no server" per [reference/](https://deepseek-harness.github.io/deepseek-harness/en/reference/)).
- Profile changes need restart; `$DSH_HOME/cordis.patch.yml` edits hot-reload.

## 5. Python SDK

From [/en/guide/python-sdk](https://deepseek-harness.github.io/deepseek-harness/en/guide/python-sdk). Package `deepseek-harness-sdk` (pip); bundles a same-version runtime, "no system Node.js installation" required. Prereqs: Python ≥3.10, Git, Linux x64/arm64 or macOS 14+ arm64, DeepSeek-compatible endpoint + credentials, isolated workspace.

```sh
git clone https://github.com/deepseek-ai/deepseek-harness.git
cd deepseek-harness
python -m venv .venv
. .venv/bin/activate
python -m pip install deepseek-harness-sdk
export DEEPSEEK_API_KEY=sk-your-key-here
# optional: DEEPSEEK_BASE_URL, DSH_MODEL, DSH_SYSTEM_PROMPT
python examples/jsonrpc-agent/minimal.py \
  --workspace /absolute/path/to/workspace \
  --session-root /absolute/path/to/sessions \
  --session-id example-001 \
  "Inspect the repository and fix the failing tests."
```

```python
from pathlib import Path
from deepseek_harness import DeepSeekHarness

config = Path("examples/jsonrpc-agent/minimal.cordis.yml").resolve()
workspace = Path("/absolute/path/to/workspace").resolve()
sessions = Path("/absolute/path/to/sessions").resolve()

with DeepSeekHarness(
    provider="deepseek-official",
    model="deepseek-v4-flash",
    max_tokens=49_152,
    cwd=str(workspace),
    session_root=str(sessions),
    cordis=str(config),
) as harness:
    result = harness.run(
        "Inspect the repository and fix the failing tests.",
        session_id="example-001",
    )

print(result.final_response)
```

Documented surface: `DeepSeekHarness(provider, model, max_tokens, cwd, session_root, cordis)` context manager; `harness.run(prompt, session_id=...)` → result with `.final_response`. Reusing a `session_id` preserves shell state, cwd, variables; fresh IDs for independent tasks. The minimal composition ships `bash` + `str_replace_editor`, 300 s bash timeout, 16,000-char editor output cap, and runs in `danger-full-access` mode — docs warn to run only in disposable containers/checkouts. SDK consumers wanting transcripts read `session/event`; `agent/*` is the live coordination API ([agent-lifecycle](https://deepseek-harness.github.io/deepseek-harness/en/reference/agent-lifecycle)).

## 6. Permission policy model

Three independent layers ([subsystems/approval](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/approval), [subsystems/sandbox](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/sandbox), [subsystems/permission-presets](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/permission-presets)):

**Approval (`ctx.approval`)** — closed fail-closed outcome set: `allowed-once`, `rejected`, `cancelled`, `unavailable` (missing/non-responding answerer = denial). Per-session policies: `ask` (default; delegates to composed answerers via the scope-filtered `approval/request` waterfall — first responder claims the slot, others `next()`) and `never` (deterministic reject, "useful for headless/CI"). Effective policy = latest `approval/policy` event in the session log, else service config default. API: `request(req: ApprovalRequest)` (appends `approval/asked` → outcome → `approval/decided`; requires open turn), `setPolicy(agent, policy)`, `overrideOf(session)`. `ApprovalRequest` = `{agent, toolName, callId?, reason?, signal?}` (arguments deliberately omitted). Audit events are log-only, never in model transcripts. Consumers (`dsh-tools`, `dsh-tool-bash`) "fail closed unless `allowed-once` is returned."

**Sandbox (`ctx.sandbox` + `ctx.sandboxPolicy`)** — modes: `read-only` (denies writes except sinks like /dev/null), `workspace-write` (writes under workspace root + temp), `danger-full-access` (bypasses provider entirely). `ConfinedSandboxMode` = first two. `confine(argv, policy)` → `ConfinedArgv {argv, enforcement, denialSignatures, runnerFailureRules}`; enforcement reported as `full` or `partial`. Backends in `dsh-sandbox-local`: Linux bwrap/Landlock, macOS Seatbelt, Windows ACL restricted-token. No backend → `SandboxUnavailableError` (`SANDBOX_UNAVAILABLE`); "silent unconfined passthrough is never legal for a confined policy."

**Permission presets** — bundle the two knobs into named presets offered as one Permissions selector. Defaults: `workspace-write` (= sandbox `workspace-write` + approval `ask`) and `danger-full-access` (= sandbox `danger-full-access` + approval `never`). Custom presets:

```typescript
interface PresetSpec {
  sandbox: SandboxMode
  approval: ApprovalPolicy
  name?: string
  description?: string
}
interface Config {
  presets?: Record<string, PresetSpec>
  defaultPreset?: string
}
```

`custom` is a reserved, derived-only name for unmatched knob states. Switching appends log-only `permission/preset` then sets each knob via its own setter. Service methods: `current(events)`, `set(session, name)`, `resolve(name)`, `optionOf(name)`, `selectFor(state)`. Plan mode is explicitly separate from both knobs ([subsystems/plan](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/plan)).

## 7. Skills

From [subsystems/skills](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/skills): skills are "optional instructions, not session events" — curated guidance catalogs, distinct from tools (which execute actions) and plugins (which extend the framework through Cordis). `ctx.skills` is a seam (`skill` package; providers `skill-badge`, `skill-filesystem` per [capability-seams](https://deepseek-harness.github.io/deepseek-harness/en/reference/capability-seams)).

- **Format**: directory bundle `<name>/SKILL.md` or flat `<name>.md`; kebab-case names (`^[a-z0-9]+(?:-[a-z0-9]+)*$`); frontmatter flags `disable-model-invocation`, `user-invocable`.
- **Discovery**: layered registry (host layer + per-scope layers; a plugin mounted by an agent preset registers into that preset's layer). Filesystem provider rank order: project DSH skills (100) → project agent skills (200) → custom dirs (300) → user DSH home (400) → user agents home (500) → bundled (600). Rank-based conflict resolution within layers.
- **API**: `registerProvider()`, `register()`, `list()` (invocation-neutral summaries), `snapshot()`, `get()`; providers implement `list()` (async candidates) and `get()` (full body); options are "readonly borrowed"; `cwd`-aware; `AbortSignal` cancellation. `skills/change` broadcasts invalidation without diffs.
- **Invocation**: model-invocable skills appear in session catalogs as name+description only; bodies load on demand via the `skill` tool (`dsh-tool-skill`, "Load full instructions for an available skill" — [tool-catalog](https://deepseek-harness.github.io/deepseek-harness/en/reference/tool-catalog)).

## 8. Extension points (everything replaceable/extendable)

**"Where new behavior goes" table** ([reference/](https://deepseek-harness.github.io/deepseek-harness/en/reference/)): model provider → adapter on `ctx.llm`; model-facing capability → `ctx.tools` (schema joins prompt assembly); per-session capability set → agent preset (+ `isolate` realm for service rows); shell exec → `ctx.shell` backend (local spawns via `ctx.subprocess`); persistent terminal → `ctx.terminals` backend + `dsh-tool-terminal`; human command → `ctx.commands` (dispatch without a model turn; see [subsystems/commands](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/commands): `register/list/find/execute`, `command/run`/`command/done` events, scoped shadowing); background work → `ctx.jobs` (+ `job_list`/`job_output`/`job_kill` tools, [subsystems/jobs](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/jobs)); filesystem access/policy → `ctx.fs` provider or `fs/*` events; process confinement → `ctx.sandbox` backend; intercept request/tool/turn → `agent/*` and `tools/*` events (`agent/turn-stopping` stops a turn); inject model-facing context → `agent.inject()`; UI/editor integration → drive `ctx.agents`, render from `session/event`; Web Client Chat node → `ConversationNodeDefinition` + keyed renderer; durable session state → extend `SessionEventMap`; session titles → sole `ctx.sessionTitle` provider; same-session objective → `ctx.goals`; fork live session → `ctx.sessions.fork()`; agent-scoped registration → `agent.ctx`.

**Swappable seams with shipped providers** ([capability-seams](https://deepseek-harness.github.io/deepseek-harness/en/reference/capability-seams)): `ctx.llm` (`llm-deepseek`, `llm-pi-ai`, `llm-replay`) · `ctx.shell` (`bash-local`, `bash-sandbox`, `pwsh-local`) · `ctx.subprocess` (`subprocess-local`, `subprocess-e2b`) · `ctx.terminals` (`terminal-bash`) · `ctx.sandbox` (`sandbox-local`) · `ctx.fs` (`fs-local`, `fs-sandbox`, `fs-e2b`) · `ctx.codeRuntime` (`code-runtime-worker`) · `ctx.web` (`web-search-exa`, `web-search-perplexity`, `web-search-deepseek`, `web-fetch-http`) · `ctx.skills` (`skill-badge`, `skill-filesystem`) · `ctx.subagents` (multiple coexisting by name: `subagent-spawn-in-process`, `subagent-acp`, `subagent-codex`, fork, plus installable `dsh-subagent-claude-code`) · `ctx.workflowEngine` (`workflow-worker-thread`) · `ctx.jobs` (`jobs-local`) · `ctx.sessionPersistence` (`session-persistence-jsonl`, `-sqlite`) · `ctx.storage` (`storage-json`, `-sqlite`) · `ctx.sessionQuery` (`session-query-sqlite`) · `ctx.sessionTitle` (`session-title-first-prompt-llm`, `-all-prompts-llm`) · `ctx.credentials` (`credentials-local`) · `ctx.settings` (`settings-file`) · `ctx.attachments` (`attachment-local`) · `ctx.fileReferences` (`file-reference-local`) · `ctx.lsp` (`lsp-local`) · `ctx.spillStore` (`spill-local`) · `ctx.sessionTelemetry` (`session-telemetry-otel`) · `ctx.authorization`. Providers are chosen at composition time; swapping is config-only. Non-replaceable core spine: `ctx.sessions`, `ctx.tools`, `ctx.agents`, `ctx.invariants`, `ctx.typert`, `ctx.systemPrompt`, `ctx.tokenMeter`, `ctx.approval`, `ctx.planMode`.

**Hook/recipe patterns** ([cookbook/extension-cookbook](https://deepseek-harness.github.io/deepseek-harness/en/reference/cookbook/extension-cookbook)): tool plugins (`defineTool` or raw JSON-Schema `ToolDefinition`); permission-gate hooks returning `PreToolDecision` from `tools/pre-execute` (`return { kind: 'deny', reason: 'Denied by policy.' }`), plus `ctx.tools.guard()` monotonic denials, `tools/execute` wrapping, `tools/post-execute` transforms, `tools/result` observation; UI plugins consuming `session/event` and feeding back via `agent.followup()`/`agent.steer()`; protocol drivers adapting external peers to `ctx.agents` (ACP JSON-RPC stdio example). LLM adapters: extend `LlmAdapter`, implement `async * stream(options): AsyncIterable<StreamChunk>` (chunks: `block-start/end`, `text-delta`, `tool-call-delta`, `usage`, `finish`; throw `LlmError` with stable codes; `registerAdapter(['my-provider'], new MyAdapter(...))`) ([cookbook/adding-an-llm-adapter](https://deepseek-harness.github.io/deepseek-harness/en/reference/cookbook/adding-an-llm-adapter)). Orchestration: `ctx.subagents.start/startContinuable/followup/reportFrom/interrupt` with `SubagentResult.stopReason ∈ {completed, aborted, error, max-tokens, refusal}` ([subsystems/subagent](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/subagent)); `ctx.workflowEngine` runs model-written JS orchestration scripts (`WorkflowStartRequest {script, meta, args?, subagentProvider?, maxTotalAgents?, parent, signal?}`, events `workflow/start|phase|log|agent-start|agent-end|end`) ([subsystems/workflow](https://deepseek-harness.github.io/deepseek-harness/en/reference/subsystems/workflow)); experimental durable agent teams (`spawn_teammate`, `team_task_*`, `wait_agent` in `dsh-experimental-tool-agent-team`, [tool-catalog](https://deepseek-harness.github.io/deepseek-harness/en/reference/tool-catalog)). Generated catalogs enumerate every configurable plugin ([config-catalog](https://deepseek-harness.github.io/deepseek-harness/en/reference/config-catalog)), all 65 shipped tools across 23 packages ([tool-catalog](https://deepseek-harness.github.io/deepseek-harness/en/reference/tool-catalog)), and persistence ([persistence-catalog](https://deepseek-harness.github.io/deepseek-harness/en/reference/persistence-catalog), not fetched in detail).

Caveat: WebFetch condenses pages through a summarizing model; code blocks above are reproduced as returned and matched across multiple pages, but exact surrounding prose on source pages may be longer than quoted. Pages listed in the sitemap but not individually fetched: `cordis-api/*`, `persistence-catalog`, tutorial chapters 02-05, and ~30 subsystem pages (existence confirmed via the reference index sidebar at https://deepseek-harness.github.io/deepseek-harness/en/reference/).
