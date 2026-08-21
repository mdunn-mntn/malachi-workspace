# deepseek-ai/deepseek-harness — deep-read report

Repo: https://github.com/deepseek-ai/deepseek-harness — "DeepSeek Harness: Everything is a Plugin." TypeScript, MIT, default branch `master`, v0.1.1-rc.1 (developer preview, "THERE WILL BE COMPATIBILITY-BREAKING CHANGES"), 177k stars, created 2026-08-13, ~7,900 files. CLI/product name: `dsh`. Run: `npx @deepseek-ai/dsh web` (Web UI on 127.0.0.1:3080) (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/README.md). Built on **Cordis** (https://github.com/cordiverse/cordis), vendored under `vendor/` and rescoped to `@deepseek-ai/cordis` (private, pinned upstream SHAs; https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/AGENTS.md).

## Monorepo map

Top level: `packages/` (3,963 files), `.agents/` (2,249 — agent workflows + "Agent Notes"), `examples/`, `docs/`, `apps/` (`cli` = the `dsh` bin, `web`), `scripts/` (repo gates/generators), `vendor/` (Cordis), `native/` (`node-addon-landlock-run` Linux sandbox addon), `python/` (Python SDK + pip-distributed bundled JS runtime), `website/` (VitePress). Package groups under `packages/<group>/<pkg>`, every npm package named `@deepseek-ai/dsh-<name>` (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/AGENTS.md):

- **core/** — product spine: `session` (append-only SessionEvent log, `ctx.sessions`), `system-prompt`, `tools` (scoped registry + guarded execution, `ctx.tools`), `agent` (`ctx.agents`), `agent-loop` (default driver), `scope`.
- **llm/** — seam vocabulary + `ctx.llm` adapter registry (`llm/llm`), `llm-deepseek` (official provider; default catalog `deepseek-v4-flash`, `deepseek-v4-pro`, vision-exp), `llm-pi-ai` (generic multi-provider adapter over `@earendil-works/pi-ai` — OpenAI-compatible gateways/self-hosted/any provider as pure config), `llm-retry`.
- **Capability seams**: `shell/` (bash + pwsh providers), `subprocess/`, `terminal/`, `fs/`, `lsp/`, `web/` (search/fetch), `skill/`, `compaction/`, `sandbox/` (+ `e2b/` POC), `workflow/`, `jobs/` (background jobs), `goal/`, `todo/`, `plan/` ("plan mode as logged state"), `mcp/`, `acp/` (Agent Client Protocol server), `hooks/` (**Claude Code/Codex hooks.json bridges**), `subagent/` (below), `extensions/` (self-modification), `guard/`, `credentials/`, `settings/`, `session/` (persistence/projection/titles), `preset/`, `bundle/`, `boot/`, `sdk/` (JSON-RPC + TS client), `code-runtime/` (Code Mode worker), `typert/`, `api/`, `host/` + `client/` (1,178 files — Web UI), `test-support/`, `util/`, `experimental/` (incl. private "Agent Teams" `ctx.agentTeams`).

## Plugin authoring pattern (Cordis)

A plugin is a module exporting `apply(ctx)` (+ optional `name`, `inject`, `Config`); registrations are reversible effects; services claim `ctx.<key>` and consumers declare `inject` (plugin stays PENDING until services exist; load order in config is irrelevant). Docs: https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/cordis-primer.md, tutorial https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/cordis-tutorial/01-first-plugin.md. Minimal verbatim tool plugin (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/cookbook/adding-a-tool.md):

```ts
import type { Context } from '@deepseek-ai/cordis'
import { defineTool } from '@deepseek-ai/dsh-tools'

export const name = 'my-tool'
export const inject = ['tools']

export function apply(ctx: Context) {
  ctx.tools.register(defineTool({
    name: 'read_file',
    description: 'Read a file from disk.',
    parameters: { path: { type: 'string', required: true, description: 'Absolute path' } },
    output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] },
    async execute(args, exec) { /* args typed from schema; honor exec.signal */ },
  }))
}
```

Service provider pattern (tutorial 03): `class GreeterService extends Service { constructor(ctx) { super(ctx, 'greeter') } }` + `declare module '@deepseek-ai/cordis' { interface Context { greeter: GreeterService } }`, mounted with `ctx.plugin(GreeterService)`; optional deps via `ctx.get('name')`. Config schemas use vendored **schemastery**: `export const Config: z<Config> = z.object({...})` with defaults/validation (see `packages/subagent/subagent-claude-code/src/index.ts`, `packages/llm/llm-deepseek/src/index.ts`). Key conventions (AGENTS.md): every registration through `ctx.effect()`/`ctx.on()` with disposers; typed events via declaration merging with dispatch modes `emit|waterfall|parallel|serial`; waterfall listeners must call `next()`; "model-visible ⟺ logged" (any model input must be reconstructable from the session log); branded ids; no hardcoded tunables (deployment choices are Config fields). Model providers: subclass `LlmAdapter`, `ctx.llm.registerAdapter(providers, adapter)` (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/packages/llm/llm/README.md).

## Config format & profiles

- **`cordis.yml`**: YAML list of entries `{ id, name: <module specifier or npm package>, config, disabled, inject }`; `!!js` expressions allowed in `config`/`disabled` (evaluated at mount, e.g. `ANTHROPIC_API_KEY: !!js process.env.ANTHROPIC_API_KEY`). Full runnable example: https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/examples/headless-agent/cordis.yml.
- **`cordis.patch.yml`**: patch layer — id-targeted entry replaces the named row's *whole* config (no deep-merge); `insert:` adds rows. Example: https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/packages/bundle/headless/cordis.patch.yml.
- **Profiles**: directory `$DSH_HOME/profiles/<name>` (`$DSH_HOME` defaults `~/.dsh`) holding `package.json` (out-of-tree plugin `dependencies` + manifest `dsh: { profile: { bundles: [...] } }`) and the user's `cordis.patch.yml`. A **bundle** is an npm package declaring `dsh: { bundle: { patch: "./cordis.patch.yml" } }`. Composition over an empty entry list: each bundle patch in order → profile `cordis.patch.yml` → home-level `$DSH_HOME/cordis.patch.yml` → `--patch` overlays. Shipped bundles: `dsh-base` (first layer everywhere), `dsh-web-app`, `dsh-headless`. Inspect with `dsh --profile web --dump-config`. (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/packages/boot/app-boot/README.md, https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/architecture.md, https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/apps/cli/README.md)
- Also in `$DSH_HOME`: `.env` (layered: inherited > project `.env` > user `.env`), `settings.yaml` (hot-reloaded user settings sections), `.credentials.yaml` (credential store; configs hold `apiKeyEnv` references, never secrets). Patch files are HMR-watched live.
- **Agent presets** (`packages/preset`, `apps/cli/config/agent-presets/{minimal,standard,code,cordis}`): per-session agent composition from `agent.cordis.yml` + `preset.yml`; service rows need `isolate` realms. Preset display names are in Chinese (`name: 标准模式`).

## Headless / CLI entry

`apps/cli` owns the `dsh` bin (`src/bin.ts`, source-run via `node --import tsx/esm`). Modes: `dsh --profile <name>`; `dsh web` (alias); `dsh --profile headless "task"` — one-shot: boots base + headless bundle (no server, no port), creates one persisted Agent, submits the task as a user message, waits for quiescence, prints last non-empty assistant text to stdout, exits 0 on clean `turn/end` else 1 (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/packages/bundle/headless/README.md); `dsh plugin --profile <name> <pnpm args>` for plugin management. Unrecognized args pass through to the booted app (`dsh --profile web --port 8080`).

## Sub-agents — the Claude Code / Codex claim is real code

Seam: `ctx.subagents` (`packages/subagent/subagent`), multiple named providers coexisting; doc: https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/docs/subsystems/subagent.md. Providers: `subagent-spawn-in-process` (fresh child), `subagent-fork-in-process` (child from parent history), `subagent-acp` (out-of-process over ACP), **`subagent-codex`** ("real Codex app-server child", `src/wire.ts`), **`subagent-claude-code`**, `subagent-dsh-sdk` (harness child via own TS SDK). Model-facing consumers: `tool-subagent` (one static tool per provider row, config `{provider, toolName, backgroundMode: one-shot|continuable, maxDepth, persona, toolFilter}`), `tool-subagent-control` (`send_message`/`interrupt_agent`/`list_agents`), `tool-subagent-report` (child→parent report channel). Continuable children: durable child Session + at most one Activation, inbox-FIFO followups, `interrupt()`, parent-authorized.

`subagent-claude-code` (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/packages/subagent/subagent-claude-code/README.md, `src/index.ts`): pins **official `@anthropic-ai/claude-agent-sdk@0.3.220`** (carries Claude Code CLI 2.1.220, ~257MB unpacked darwin-arm64 payload), calls SDK `query()` in the parent session's cwd, routes the SDK's `spawnClaudeCodeProcess` hook through `ctx.subprocess` (credential-scrubbed env; explicit `env` overlay for keys), accepts only `result`/`subtype: "success"`, returns final text only (no reasoning/tool traffic/stderr enters parent context). `permissionMode`: `dontAsk` (default) | `acceptEdits` | `auto` | `plan` (disallows `ExitPlanMode`) | `bypassPermissions`; `AskUserQuestion` disabled, `persistSession: false`, unattended denials. No capabilities advertised (no outputSchema/toolFilter/persona/depth). Install: `dsh plugin --profile <name> add @deepseek-ai/dsh-subagent-claude-code`; registers a dormant provider, tool exposed per-row (`subagent_claude_code`). Codex provider is symmetric.

## Runtime third-party plugin loading

`dsh plugin --profile X add <npm-pkg>` forwards to pnpm in the profile dir, then reconciles installed deps: any dep whose manifest declares `dsh.bundle` auto-joins the profile's bundle layer stack; removal withdraws it (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/apps/cli/src/plugin.ts). Bare specifiers in configs resolve via the Cordis Loader's module loader (installation first, then profile `node_modules`; `healProfilesModuleFallback` maintains a symlink farm). Discovery is social: GitHub topic `dsh-plugin`. Also: `hooks-claude-code`/`hooks-codex` bridge existing external `hooks.json` shell hooks onto harness interception events, and `extensions/tool-cordis` + runners let **the agent inspect and mount plugins into its own live runtime** (`pnpm run demo:cordis`).

## Toolchain

Node `^22.19.0 || >=24.0.0`; **pnpm 11.7.0** workspaces (`vendor/*`, `packages/*/*`, `native/…`, `apps/*`, `examples`, `python/sdk-runtime`, `website`); ESM everywhere; TypeScript 6.0.3 `strict`; tsdown builds, vitest 4, oxlint, knip, jscpd, lefthook (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/package.json).

## Surprising

- **No external PRs accepted** — ecosystem-only contributions (https://raw.githubusercontent.com/deepseek-ai/deepseek-harness/master/CONTRIBUTING.md: "we cannot accept external pull requests at the moment… Into the unknown.").
- The repo is agent-operated: `.agents/` holds 2,249 files of workflows/skills/"Agent Notes" (ADR-like, mandatory in every non-trivial PR); root `CLAUDE.md` symlinks `AGENTS.md`.
- CI gate is **per-file 100% coverage** on `packages/*/*/src`, plus keyless snapshot-replay tests of full transcripts and a wine-based Windows gate.
- Entire framework (Cordis, schemastery, cosmokit) vendored and rescoped; `@deepseek-ai/cordis` is a peerDependency of every package.
- Every package README carries mandated "Model Experience"/"Token effect"/"KV Cache effect"/"Known Limitations" sections, enforced by doc gates; all docs bilingual EN/ZH with pairing/translation gates.
- `llm-pi-ai` makes non-DeepSeek providers (OpenAI-compatible gateways etc.) pure configuration; mandatory app-attribution headers on provider HTTP.
- Code Mode: every registered tool callable as `await tools.<name>(args)` from model-written JS in a worker thread.
- Python SDK ships a bundled Node runtime closure via pip (`python/sdk-runtime`); native Linux Landlock sandbox addon; E2B cloud-sandbox POC swaps fs+subprocess+shell wholesale via the seam design.
