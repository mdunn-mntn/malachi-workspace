---
name: reference_claude_cli_npmrc_os_pin
description: "~/.npmrc pins os=linux, so npm installs the Linux optional dep and the claude CLI dies with 'native binary not installed' — fix with npm i -g --os=darwin --cpu=arm64, and expect the same break in any npm package with platform-native binaries"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [claude native binary not installed, npmrc os=linux, npm platform optional dependency, claude-code-darwin-arm64, claude.exe symlink, npm i -g --os=darwin --cpu=arm64, install.cjs postinstall, claude cli broken, mcp interactive oauth, npm-global prefix]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-19
---

**`~/.npmrc` on this Mac contains `os=linux`.** npm honors it when resolving platform-specific optional
dependencies, so a global install of any package with native binaries pulls the **Linux** artifact and
skips `darwin-arm64`.

Symptom on `@anthropic-ai/claude-code`: `claude` exits with

```
Error: claude native binary not installed.
```

and `~/.npm-global/bin/claude` is a symlink to a **`claude.exe`** stub. Re-running the postinstall does
not help; it reports `Native package "@anthropic-ai/claude-code-darwin-arm64" not found`, because the
optional dep was never downloaded in the first place.

**Fix without editing the file:**
```bash
npm i -g --os=darwin --cpu=arm64 @anthropic-ai/claude-code
```

The `os=linux` pin is presumably deliberate for building Linux artifacts (the Pi, see
[[reference_pi5_server]]), so it was left in place rather than removed. **Expect this to break every
future native npm install on this machine** until it is removed or scoped, and reach for the two flags
rather than debugging the package.

**Why it mattered (2026-08-19, IMP-047):** a remote HTTP MCP server's OAuth handshake can only run in an
**interactive** session. The VSCode extension's `/mcp` prints a one-line summary and nothing else, so
authorizing needed the terminal CLI, which was dead for this reason. Sequence that worked: fix the
install, run `claude` in the workspace, `/mcp`, select the server, Authenticate. Then **reload the other
session** — a session that connected before authorization does not pick up the new token.

Also seen in that session: `claude` prompts *"Detected a custom API key in your environment"* because a
live `ANTHROPIC_API_KEY` is exported from the shell profile. Answer **No** to use the normal login.
That key should be removed; local API keys are the pattern MNTN retired on 2026-06-10
([[reference_pi5_server]]).

Related: [[reference_mntn_public_mcp]].
