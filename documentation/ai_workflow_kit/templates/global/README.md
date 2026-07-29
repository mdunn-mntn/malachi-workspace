# global/ — your personal `~/.claude/` framework (sanitized)

The user-level layer that makes a new laptop behave like the old one, decoupled from any job. `bootstrap.sh`
installs these when run with `--with-global`; each is job-neutral (fill the `<PLACEHOLDER>`s).

| File | Installs to | How bootstrap handles it |
|---|---|---|
| `CLAUDE.md` | `~/.claude/CLAUDE.md` | backs up any existing to `~/.claude/CLAUDE.md.backup-preport`, then copies |
| `settings.json` | `~/.claude/settings.json` | copies only if absent; else writes `~/.claude/settings.json.from-kit` for you to merge |
| `mcp_servers.json` | `~/.claude.json` → `mcpServers` | **never auto-written** (that file holds session state) — merge by hand, fill the token, rebuild the vendored server |

**What is NOT here (deliberately):** the rest of `~/.claude/` — `projects/`, `sessions/`, `history.jsonl`,
`shell-snapshots/`, caches, and any personal notes — is session state and prior-job work. It must not
travel. Custom global skills would live in `~/.claude/skills/`; there were none to port.

**Revert:** restore `~/.claude/CLAUDE.md.backup-preport`; delete the `todoist` block you added to `~/.claude.json`.
